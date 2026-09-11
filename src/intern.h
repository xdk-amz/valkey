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
    uint8_t slab_class;   /* P1-1: size-class index into the slab table; the
                           * sentinel INTERN_CLASS_LONG marks a long allocation
                           * that is not slab-managed (a raw zmalloc/zfree). */
    uint32_t idle_since_tick; /* P1-2: serverCron tick at which this slot last
                           * became cold (refcount 0). Written ONLY when refcount
                           * transitions to 0 and reset when a probe/acquire hit
                           * takes it back to >= 1. Meaningful only for admitted
                           * slots (referenced slots are never evicted); its value
                           * is ignored while refcount > 0. Cold-admitted eviction
                           * (internEvictIdle) reclaims a slot idle for longer than
                           * intern-admitted-idle-ms. See intern.c / HLD §18.2. */
    char inline_[INTERN_SSO_MAX + 1]; /* SSO storage; sdshdr8+bytes+NUL for fields */
} internSlot;

/* P1-1 slab allocator size classes (slot byte sizes). A string picks the
 * smallest class whose inline capacity fits sdsReqSize(len, SDS_TYPE_8) after
 * the fixed header (offsetof(internSlot, inline_)). Slots never move; slabs are
 * released back to jemalloc at zero live count, keeping one empty cushion slab
 * per class. The header always precedes inline_ at the same offset in every
 * class, so internSlotFromSds() is class-independent. */
#define INTERN_NUM_CLASSES 6
#define INTERN_CLASS_LONG 0xFF /* sentinel: not slab-managed (raw zmalloc) */

/* Per-class slab accounting, exposed for DEBUG INTERN STATS. */
typedef struct internClassStats {
    size_t slot_bytes;   /* the class slot byte size */
    size_t slots_total;  /* live slots carved from this class's slabs */
    size_t slots_live;   /* slots currently live (refcount>0 or admitted) */
    size_t slabs;        /* slabs currently held by this class */
    size_t slots_per_slab;
} internClassStats;

/* Fills per-class slab stats (INTERN_NUM_CLASSES entries) and, if non-NULL,
 * the count of long (non-slab) allocations. */
void internGetClassStats(internClassStats out[INTERN_NUM_CLASSES], size_t *long_allocs);

/* Total bytes held by all slabs (server-level overhead, HLD §5 / R7). */
size_t internSlabMemory(void);
/* Number of slabs held across all classes. */
size_t internSlabCount(void);
/* Ratio of live slots to carved slots across all slabs, [0,1]; 1.0 when empty. */
double internSlabLiveRatio(void);

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

/*-----------------------------------------------------------------------------
 * P1-2: admission caps and cold-admitted eviction.
 *
 * Caps bound the table so nothing grows it without limit (R6). Two independent
 * ceilings, both MODIFIABLE and enforced on a MISS only:
 *   - intern-max-slots     : total live + admitted slots.
 *   - intern-max-slab-bytes : sum of slab bytes across all classes (P1-1).
 * At either cap, internAcquire / internAdmit / internProbe on a MISS return NULL
 * (the caller stores an owned copy exactly as a probe miss does today). Probe
 * HITS are never refused -- refcount++ allocates nothing. Lowering a cap at
 * runtime does not free anything; it only stops growth (eviction reclaims).
 *
 * Cold-admitted eviction is the ONLY eviction. A slot admitted with refcount 0
 * for longer than intern-admitted-idle-ms is de-admitted and freed. Referenced
 * slots (refcount >= 1) are never evicted -- their holders own them. Eviction
 * runs from serverCron in bounded batches with a persistent cursor.
 *----------------------------------------------------------------------------*/

/* Admission-refusal reasons for the per-reason counters. */
#define INTERN_REFUSE_SLOTS 0
#define INTERN_REFUSE_SLAB_BYTES 1

/* Cumulative admission-refusal counters: total and per reason (slots / slab
 * bytes). Bumped whenever a MISS is refused because a cap is hit. */
void internGetCapCounters(size_t *refusals_total, size_t *refusals_slots, size_t *refusals_slab_bytes);

/* True if a new slot of `content_len` bytes can be admitted without breaching
 * either cap (0-arg cap values are read from server config: 0 == no cap). Used
 * by the acquire/admit/probe MISS paths; a HIT never consults it. */
int internAtCap(size_t content_len, int *reason);

/* Total slots (live + admitted) currently held across both indexes. Cheap:
 * maintained incrementally, not an O(n) walk. */
size_t internSlotCount(void);

/* Cold-admitted eviction driven from serverCron.
 *
 * internEvictIdleBatch: walk up to `batch` slots from the persistent cursor and
 * evict every admitted refcount-0 slot whose idle age (current tick - its
 * idle_since_tick) is > idle_ticks. When `pressure` is non-zero (slab-bytes cap
 * reached) it also evicts cold admitted slots regardless of idle age, oldest
 * idle first within the scanned batch. `now_tick` is the current serverCron
 * tick. Returns the number of slots evicted this call. Persistent cursor state
 * lives inside intern.c. */
size_t internEvictIdleBatch(uint32_t now_tick, uint32_t idle_ticks, size_t batch, int pressure);

/* Force one FULL eviction pass (all slots, ignoring the batch/cursor) evicting
 * every admitted refcount-0 slot idle for > idle_ticks. Test helper behind
 * DEBUG INTERN EVICT [IDLE ms]. Returns slots evicted. */
size_t internEvictIdleFull(uint32_t now_tick, uint32_t idle_ticks);

/* Cumulative count of slots reclaimed by cold-admitted eviction. */
size_t internEvictions(void);

/* Oldest idle admitted slot's age in serverCron ticks (0 if none). For the
 * intern_admitted_idle_max_ms INFO field. */
uint32_t internAdmittedIdleMaxTicks(uint32_t now_tick);

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

/*-----------------------------------------------------------------------------
 * Deferred, coalesced release for off-main entry destruction (O8 / S5).
 *
 * The bio lazy-free thread destroys hash entries that hold interned refs, but
 * refcounts and the dedup index are main-thread-only. Instead of touching them,
 * a bio thread runs its job between internDeferBegin() and internDeferPublish():
 * every internRelease[Field] on that thread is recorded (slot -> pending count)
 * into a thread-local coalescing map rather than applied. At publish, the map
 * becomes ONE message pushed onto a lock-free MPSC stack. Main pops the whole
 * stack in internDrainDeferred() (beforeSleep + serverCron backstop) and applies
 * the counts through the normal release path -- plain integer arithmetic, still
 * only ever written on main. See valkey-intern-o8-lazyfree-design.md.
 *----------------------------------------------------------------------------*/

/* Record the main-thread id so the debug-build guard can assert that the
 * immediate release path and the drain run on main. Called from initServer. */
void internSetMainThread(void);

/* Open thread-local deferral mode: subsequent internRelease[Field] calls on this
 * thread record into a coalescing map instead of applying. Nestable begins are
 * not expected (one per bio job); a second begin without publish is a no-op. */
void internDeferBegin(void);

/* Close deferral mode: build one message from the coalescing map and push it
 * onto the MPSC stack for main to drain. If nothing was recorded, frees the map
 * and publishes nothing. */
void internDeferPublish(void);

/* MAIN THREAD ONLY. Pop the entire pending-message stack and apply every
 * recorded decrement through the normal at-zero reclamation (respecting the
 * admitted flag). Returns the number of individual releases applied. */
size_t internDrainDeferred(void);

/* Deferred-release counters (main-thread readers). msgs = messages drained,
 * releases = individual references applied at drain, pending = messages pushed
 * but not yet drained (atomic). */
void internGetDeferredCounters(size_t *msgs, size_t *releases, size_t *pending);

#endif
