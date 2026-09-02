/* lsa.h - log-structured bump allocator, whole-object admission.
 * The ENTIRE keyspace object (robj word + val_ptr + embedded key + value
 * sds, contiguous, standard object layout preserved) is one log entry.
 * Measurement spike, not mergeable code. Single-threaded: all log mutation
 * happens on the main thread (lazyfree must stay disabled; copy-avoid
 * replies are disabled under LSA; IO-thread free offload is bypassed). */
#ifndef LSA_H
#define LSA_H

#include <stdint.h>
#include <stddef.h>
#include "sds.h"

#define LSA_SEGMENT_SIZE (8ULL * 1024 * 1024) /* 8MB segments */
#define LSA_EMAX (1ULL * 1024 * 1024)         /* value admission cap */

/* Lifecycle. lsaInit reserves size_mb of VA (commit on touch) and installs
 * the sds.c hooks. band_lo/band_hi are the cleaner band percentages:
 * trigger when live < band_lo% of used, clean until live >= band_hi%.
 * Returns 1 on success, 0 on failure. */
int lsaInit(size_t size_mb, int band_lo, int band_hi);
int lsaEnabled(void);

/* Whole-object admission: bump-allocate 'size' payload bytes. The payload
 * start is 8-aligned and is where the caller lays out the robj. Returns
 * NULL when the pool is exhausted (caller falls back to the heap). No
 * inline emergency cleaning: relocation is only safe from cron, never
 * mid-command (commands hold raw robj pointers). */
void *lsaAdmitWhole(size_t size);

/* True if 'p' points anywhere inside the log region (entry payloads and
 * their interiors, e.g. the embedded value sds). */
int lsaIsLogPtr(const void *p);

/* Tombstone the whole entry that owns this payload (the robj pointer as
 * returned by lsaAdmitWhole). Called instead of zfree() on object free. */
void lsaFreeWhole(void *payload);

/* Payload size of the entry owning 'payload' (for MEMORY USAGE etc.). */
size_t lsaWholePayloadSize(const void *payload);

/* Cleaner callbacks, installed by the server (they need robj/keyspace
 * knowledge that lsa.c deliberately lacks so sds.c stays linkable into
 * cli/benchmark binaries). Liveness = look the entry's embedded key up in
 * the keyspace hashtable and pointer-compare; repoint = the same
 * find+replace-in-place seam activedefrag uses
 * (hashtableReplaceReallocatedEntry, preserving cached hash bits). */
typedef struct lsaWholeOps {
    /* 1 iff the object's refcount is exactly 1 (relocation is only safe
     * when the keyspace slot is the sole reference). */
    int (*refcount_is_one)(void *payload);
    /* Keyspace bucket slot holding exactly this object, or NULL. Scans all
     * DBs (the entry stores no db index: SWAPDB/MOVE would stale a stored
     * one; pointer equality makes cross-db false positives impossible). */
    void **(*find_ref)(void *payload);
    /* Replace old->new in the keyspace bucket in place. Returns 1 on
     * success, 0 if the old pointer was not found. */
    int (*replace_ref)(void *old_payload, void *new_payload);
    /* Fix interior pointers (val_ptr) in the relocated copy. */
    void (*fix_interior)(void *new_payload, const char *old_lo, const char *old_hi, ptrdiff_t delta);
} lsaWholeOps;
void lsaSetWholeOps(const lsaWholeOps *ops);

/* Band cleaner slice, called from serverCron only (never mid-command:
 * executing commands hold raw robj pointers that relocation would stale).
 * Caller must skip while a child process is active (COW). */
void lsaCleanerCron(void);

/* Append "field:value\r\n" stats lines to an INFO payload. */
sds lsaInfoAppend(sds info);

#endif
