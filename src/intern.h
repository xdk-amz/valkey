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
    uint8_t admitted;     /* if set, the slot stays in the index at refcount 0
                           * (an admission decision) rather than being freed;
                           * see internAdmit / internEvictAdmitted (P2 S4a) */
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
    size_t admitted_slots;        /* slots marked admitted (index-owned) */
    size_t admitted_unreferenced; /* admitted slots currently at refcount 0 */
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

/* Write-time read-through probe (P2 S4a). Lookup-only on the respective index:
 * on a hit bump the refcount and return the existing slot; on a miss return NULL
 * and insert NOTHING. The content length is gated to [4, server.intern_max_len]
 * inside the probe (out-of-band content returns NULL fast, without hashing).
 * A hit hands the caller ONE reference which the constructed/updated entry owns.
 * Probe hit/miss counters are bumped (see internGetProbeCounters). */
internSlot *internProbeValue(const char *buf, size_t len);
internSlot *internProbeField(const char *buf, size_t len);

/* Reads the cumulative probe hit/miss counters (each split value/field). */
void internGetProbeCounters(size_t *value_hits, size_t *value_misses, size_t *field_hits, size_t *field_misses);

/* Admission (P2 S4a). Insert content into the respective index as an "admitted"
 * slot: refcount 0, admitted flag set. If the content is already present the
 * existing slot is marked admitted (refcount untouched). Returns the slot.
 * An admitted slot is NOT freed when its refcount reaches 0 -- it stays in the
 * index as an admission decision until internEvictAdmitted (or, in production,
 * a cap/dead-list policy). */
internSlot *internAdmitValue(const char *buf, size_t len);
internSlot *internAdmitField(const char *buf, size_t len);

/* Frees all admitted slots that are currently at refcount 0 (removes them from
 * their index). Referenced admitted slots are left in place. Test helper for
 * DEBUG INTERN EVICT. */
void internEvictAdmitted(void);

/* Persistence of the admission decision (P2 S4d). The admitted set is written to
 * the RDB as the aux field "intern-admitted" (no refs, no per-key data) so the
 * load-time probe hits from the first key after a restart or replica full sync. */

/* Cap on the serialised admitted-set blob. Not a config -- a hard ceiling; if
 * the encoded set would exceed it, nothing is written (see internEncodeAdmitted). */
#define INTERN_ADMITTED_MAX_BYTES (16 * 1024 * 1024)

/* Callback for internForEachAdmitted. kind is 0 for a VALUE slot, 1 for a FIELD
 * slot; buf/len are the slot's content. Invoked once per admitted slot regardless
 * of refcount. */
typedef void (*internAdmittedCb)(uint8_t kind, const char *buf, size_t len, void *privdata);

/* Enumerate every admitted slot (both indexes), value slots first then field
 * slots. Returns the total number of admitted slots visited. */
size_t internForEachAdmitted(internAdmittedCb cb, void *privdata);

/* Number of admitted slots across both indexes (value + field), any refcount. */
size_t internAdmittedCount(void);

/* Persist/load counters (P2 S4d), surfaced via INFO memory and DEBUG INTERN STATS.
 * *_persisted = strings written at the last RDB save; *_loaded = strings admitted
 * from the aux field at the last RDB load. Set by rdb.c. */
void internSetAdmittedPersisted(size_t n);
void internSetAdmittedLoaded(size_t n);
void internGetPersistCounters(size_t *persisted, size_t *loaded);

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
