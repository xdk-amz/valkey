#ifndef _INTERN_H_
#define _INTERN_H_

#include "fmacros.h"
#include <stddef.h>
#include <stdint.h>

/* String interning subsystem (spike S1).
 *
 * A slot holds one canonical copy of a duplicated string. Any number of hash
 * entries reference a slot through a pointer; the slot is refcounted and freed
 * inline when the last reference dies. The slot is prefix-compatible with
 * stringRef {const char *buf; size_t len;} so every existing Type-4 read path
 * (entryGetValue, digest, RDB save, replies) works unchanged when a hash entry
 * points its value at a slot instead of at a stringRef. */

#define INTERN_SSO_MAX 23 /* longest string stored inline in the slot */

typedef struct internSlot {
    const char *buf;      /* -> inline_[] (SSO) or -> zmalloc'd bytes (long) */
    size_t len;           /* content length (excludes NUL) */
    uint32_t refcount;    /* main thread only, touched on ref create/destroy */
    uint32_t hash;        /* cached content hash for the dedup index */
    char inline_[INTERN_SSO_MAX + 1]; /* SSO storage, NUL terminated */
} internSlot;

typedef struct internStats {
    size_t slots;      /* live slots */
    size_t sso_slots;  /* slots storing content inline */
    size_t long_slots; /* slots with a separate byte buffer */
    size_t bytes_saved; /* sum (refcount-1) * (len + sdshdr8 overhead) */
    size_t index_mem;  /* memory used by the dedup index */
} internStats;

/* Acquire a slot for the given content. On an index hit, bumps the refcount and
 * returns the existing slot. On a miss, allocates a new slot, copies the bytes,
 * inserts it into the dedup index, and returns it with refcount 1. */
internSlot *internAcquire(const char *buf, size_t len);

/* Release a reference. At refcount 0, removes the slot from the dedup index and
 * frees the slot (and its long buffer, if any) inline. */
void internRelease(internSlot *slot);

/* Fills stats from the current dedup index. */
void internGetStats(internStats *stats);

/* Total allocation size owned by a slot (slot + long buffer, if any). */
size_t internSlotAllocSize(const internSlot *slot);

/* Current reference count of a slot (>= 1 while referenced). */
uint32_t internSlotRefcount(const internSlot *slot);

/* Estimated bytes saved by all interning (headline INFO field). */
size_t internBytesSaved(void);

#endif
