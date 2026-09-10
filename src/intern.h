#ifndef _INTERN_H_
#define _INTERN_H_

#include "fmacros.h"
#include "sds.h"
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

#define INTERN_SSO_MAX 23 /* longest VALUE string stored inline in the slot */

/* When a slot holds a hash FIELD name (S2 / Type-5 entry), inline_[] is laid out
 * as a real sdshdr8 {len, alloc, flags, bytes, NUL} so slot->buf points at a
 * genuine sds and every hashtable-type callback (sdslen, hash, compare) works
 * unchanged. The sdshdr8 header is 3 bytes, so the inline field capacity is
 * INTERN_SSO_MAX + 1 - 3 - 1(NUL) = 20 bytes. Longer field names go to a
 * separately allocated sdshdr8 buffer. */
#define INTERN_FIELD_SSO_MAX (INTERN_SSO_MAX + 1 - 3 - 1) /* = 20 */

typedef struct internSlot {
    const char *buf;      /* VALUE: -> raw bytes. FIELD: -> sds body (past sdshdr8) */
    size_t len;           /* content length (excludes NUL) */
    uint32_t refcount;    /* main thread only, touched on ref create/destroy */
    uint32_t hash;        /* cached content hash for the dedup index */
    char inline_[INTERN_SSO_MAX + 1]; /* SSO storage; sdshdr8+bytes+NUL for fields */
} internSlot;

/* Length histogram buckets for interned strings (feeds HLD O1: is SSO_MAX right?). */
#define INTERN_HIST_BUCKETS 7
/* Bucket upper bounds: <=8, 9-16, 17-20, 21-23, 24-32, 33-64, >64 */

typedef struct internStats {
    size_t slots;       /* live slots (both kinds) */
    size_t value_slots; /* slots holding a hash value */
    size_t field_slots; /* slots holding a hash field name (Type-5) */
    size_t sso_slots;   /* slots storing content inline */
    size_t long_slots;  /* slots with a separate byte buffer */
    size_t bytes_saved; /* estimate: sum (refcount-1) * (len + sdshdr8 overhead) */
    size_t dup_refs;    /* sum (refcount-1) over all slots: references deduplicated away */
    size_t index_mem;   /* memory used by both dedup indexes */
    size_t hist[INTERN_HIST_BUCKETS]; /* interned-string length histogram */
} internStats;

/* Acquire a slot for the given content (hash VALUE). On an index hit, bumps the
 * refcount and returns the existing slot. On a miss, allocates a new slot,
 * copies the bytes, inserts it into the value dedup index, refcount 1. */
internSlot *internAcquire(const char *buf, size_t len);

/* Acquire a slot for a hash FIELD name. Identical contract to internAcquire but
 * uses the field dedup index and lays inline_[] (or a long buffer) out as a real
 * sdshdr8 so slot->buf is a valid sds of length len. */
internSlot *internAcquireField(const char *buf, size_t len);

/* Release a reference to a VALUE slot. At refcount 0, removes it from the value
 * index and frees it inline. */
void internRelease(internSlot *slot);

/* Take an additional reference on a slot the caller already holds one on. */
void internRetain(internSlot *slot);

/* Release a reference to a FIELD slot. At refcount 0, removes it from the field
 * index and frees it inline. */
void internReleaseField(internSlot *slot);

/* Returns the field slot's content as a genuine sds (points into the sdshdr8
 * laid out in the slot). Only valid for slots from internAcquireField. */
sds internSlotFieldSds(const internSlot *slot);

/* Inverse of internSlotFieldSds: recover the slot from its (always inline) sds. */
internSlot *internFieldSlotFromSds(const sds s);

/* Value slots use the same inline-sds layout: sds body <-> slot header. */
sds internSlotSds(const internSlot *slot);
internSlot *internSlotFromSds(const sds s);

/* Value slots use the same inline-sds layout: sds body <-> slot header. */
sds internSlotSds(const internSlot *slot);
internSlot *internSlotFromSds(const sds s);

/* Fills stats from the current dedup index. */
void internGetStats(internStats *stats);

/* Total allocation size owned by a slot (slot + long buffer, if any). */
size_t internSlotAllocSize(const internSlot *slot);

/* Total allocation size owned by a FIELD slot (slot + long sdshdr8, if any). */
size_t internSlotFieldAllocSize(const internSlot *slot);

/* Current reference count of a slot (>= 1 while referenced). */
uint32_t internSlotRefcount(const internSlot *slot);

/* Estimated bytes saved by all interning (headline INFO field). */
size_t internBytesSaved(void);

#endif
