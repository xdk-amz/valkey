#ifndef __INTERN_H
#define __INTERN_H

#include <stddef.h>
#include <stdint.h>
#include "sds.h"

/* Automatic interning of out-of-line keyspace strings.
 *
 * A shareable string is an sds allocated with an internHdr prefix and marked
 * with the SDS_AUX_SHARED header bit. It is immutable. Every holder owns one
 * reference, and sdsfree() releases one reference. Holders keep pointing at the
 * sds itself, so read paths are unchanged.
 *
 * A sampled write turns its value into a shareable singleton and remembers it in
 * a small probation table. A later write of the same bytes finds it there or in
 * the index, takes a reference and frees its own copy. The first holder never
 * needs to be found again.
 *
 * Thread contract: admission, lookup and the index are main-thread only. A
 * release on any other thread is queued and applied by internDrainDeferred(). */

typedef struct internGate {
    uint32_t tick;   /* calls left before the next sampled call */
    uint32_t period; /* sample one call in `period` */
} internGate;

extern internGate intern_gate;

sds internAdmitSlow(sds s);

/* Offers a freshly owned string that is about to be stored in the keyspace.
 * Takes ownership of `s` and returns the sds the holder must store. The result
 * is `s` itself or a shareable string that carries one reference for the
 * holder. Main thread only. */
static inline sds internAdmit(sds s) {
    if (sdsIsShared(s)) return s;
    if (--intern_gate.tick) return s;
    return internAdmitSlow(s);
}

void internInit(void);

/* RDB loading. While every write is being probed, the loader reads a candidate
 * string into the scratch buffer instead of a fresh allocation, so a string that
 * is already shared costs no allocation:
 *
 *   if (internLoadReady(len)) {
 *       char *buf = internLoadScratch(len);
 *       ... read len bytes into buf ...
 *       sds s = internAdmitLoaded(len);  // owned by the caller
 *   }
 *
 * The returned sds is either shared or a fresh private or singleton copy. A
 * later internAdmit() of it is a no-op. Main thread only. */
#define INTERN_LOAD_MAX (64 * 1024)
extern int intern_load_ready;
#define INTERN_MIN_LEN 16
/* The loader holds exclusive access to the keyspace (main thread, or a module
 * thread that holds the GIL), so no thread check is needed here. */
static inline int internLoadReady(size_t len) {
    return intern_load_ready && len >= INTERN_MIN_LEN && len <= INTERN_LOAD_MAX;
}
char *internLoadScratch(size_t len);
sds internAdmitLoaded(size_t len);

/* Returns a new sds holding buf[0..len) to store in the keyspace, sharing an
 * existing copy when there is one, like internAdmit(sdsnewlen(buf, len)) but
 * without allocating on a hit. Main thread only. */
sds internNewFrom(const char *buf, size_t len);

/* Request parsing. While every write is being probed, a bulk argument equal to
 * a hot shared string becomes a string object that holds a reference on it, so
 * the argument costs no allocation or copy. Returns NULL otherwise, or when not
 * called on the main thread. The argument must be long enough that it would not
 * be embedded. */
struct serverObject;
struct serverObject *internParseArg(const char *buf, size_t len);
extern __thread int intern_is_main;
static inline int internParseReady(void) {
    return intern_is_main && intern_load_ready;
}

/* Bio jobs that free keyspace data run between these two calls, so their
 * releases are coalesced into one message for the main thread. */
void internDeferBegin(void);
void internDeferPublish(void);

/* Applies releases queued by other threads. Main thread only. */
size_t internDrainDeferred(void);

/* Moves a shareable string with a single holder using `defragfn`, keeping the
 * table consistent. Returns the new sds, or NULL if it did not move. The caller
 * updates the holder. */
sds internDefragShared(sds s, void *(*defragfn)(void *));

/* Allocation size of a shareable string, including its prefix. */
size_t internSharedAllocSize(const_sds s);
uint32_t internSharedRefcount(const_sds s);

/* Memory attributed to one holder of `s`: the full allocation for a private
 * string, an equal share of the allocation for a shared one. */
size_t internSdsMemUsage(const_sds s);

typedef struct internInfo {
    size_t shared;        /* strings in the index */
    size_t singletons;    /* strings in probation */
    size_t refs;          /* references held on indexed strings */
    size_t shared_bytes;  /* allocation bytes of indexed strings */
    size_t saved_bytes;   /* bytes not allocated thanks to sharing */
    size_t table_bytes;   /* index and probation table memory */
    size_t hits;          /* writes that took a reference on an indexed string */
    size_t hot_hits;      /* hits found through the hot cache */
    size_t promotions;    /* singletons that gained a second holder */
    size_t sampled;       /* writes that were hashed */
    size_t deferred;      /* releases applied from other threads */
    int level;            /* sampling level, 0 = every write */
    int sel_level;        /* singleton selection level, 0 = every missed value */
} internInfo;

void internGetInfo(internInfo *info);

#endif
