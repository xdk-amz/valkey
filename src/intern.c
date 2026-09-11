#include <stddef.h>
#include "server.h"
#include "serverassert.h"
#include "intern.h"
#include "hashtable.h"
#include "sds.h"
#include "zmalloc.h"
#include <stdatomic.h>
#include <pthread.h>

/*-----------------------------------------------------------------------------
 * String interning subsystem (spike S1 values, S2 hash field names).
 *
 * Two dedup indexes, one per kind: intern_index_value (hash values, S1) and
 * intern_index_field (hash field names, S2). Both are hashtables of internSlot*
 * keyed on content. The hash function reads the cached slot->hash; the compare
 * function compares len then memcmp. Lookups by raw {buf,len} use a probe key
 * whose prefix layout (buf, len, refcount, hash) matches a slot so the same
 * callbacks operate on it.
 *
 * VALUE slots: slot->buf points at raw content bytes (inline_ or a long alloc).
 * FIELD slots: inline_ (or a long alloc) is laid out as a real sdshdr8
 *   {len, alloc, flags=SDS_TYPE_8, bytes, NUL} and slot->buf points at the sds
 *   body (header + 3). This makes slot->buf a genuine sds so the hash's
 *   hashtable type callbacks (sdslen, sdsHashConfigurableSeed, dictSdsKeyCompare)
 *   operate on it with no change. Field aux bits are NOT used on this shared sds;
 *   the Type-5 entry carries the holder's aux bits in its own flags byte.
 *----------------------------------------------------------------------------*/

/* Per-duplicate saving: an un-interned duplicate would have cost an owned
 * sdshdr8 buffer of (len + header + NUL). */
#define INTERN_PER_USE_OVERHEAD(len) ((len) + sdsHdrSize(SDS_TYPE_8) + 1)

static uint64_t internSlotHash(const void *key) {
    const internSlot *slot = key;
    return slot->hash;
}

/* Returns non-zero (true) when the two keys are equal, matching the hashtable
 * keyCompare contract (see dictSdsKeyCompare). */
static int internSlotCompare(const void *key1, const void *key2) {
    const internSlot *a = key1;
    const internSlot *b = key2;
    if (a->len != b->len) return 0;
    return memcmp(a->buf, b->buf, a->len) == 0;
}

static hashtableType internIndexType = {
    .hashFunction = internSlotHash,
    .keyCompare = internSlotCompare,
    .instant_rehashing = 1,
};

/* The two dedup indexes. Lazily created. */
static hashtable *intern_index_value = NULL;
static hashtable *intern_index_field = NULL;

/*-----------------------------------------------------------------------------
 * Slab allocator (P1-1).
 *
 * Slots are carved from per-class slabs instead of an individual zmalloc each.
 * A class is a fixed slot byte size; a string picks the smallest class whose
 * inline capacity fits sdsReqSize(len, SDS_TYPE_8) after the fixed header. Slots
 * never move (a slot address is stable from acquire until its slab is released),
 * and a slab is released only when no slot in it is live -- so the never-move
 * guarantee the entry contract depends on (internSlotFromSds recovers the header
 * at a fixed negative offset) holds unchanged.
 *
 * Free slots are threaded on a per-class intrusive free list (the free pointer
 * overlaps the slot's header bytes -- a free slot carries no live data). Each
 * slab tracks its live count; at zero live it is released back to jemalloc
 * unless it is the class's single retained empty "cushion" slab (avoids
 * alloc/free flapping when a class oscillates around a slab boundary).
 *
 * Admitted refcount-0 slots (P2) count as LIVE for slab purposes: they are an
 * admission decision that must stay resident, so their slab is never released.
 * Long strings (beyond the biggest class's inline capacity -- only reachable if
 * intern-max-len is raised well past 256) fall back to a raw zmalloc tagged
 * INTERN_CLASS_LONG and freed directly; they are not slab-managed and are
 * counted separately.
 *----------------------------------------------------------------------------*/

/* Class slot byte sizes. Chosen so the inline capacity
 * (class_bytes - offsetof(internSlot, inline_) - 8 B owner back-pointer) covers
 * the sdsReqSize bands. The top class is 296 (not the design-doc's 288): with
 * the 26 B header and the 8 B trailing owner pointer this allocator adds per
 * slot, 296 B is the smallest 8-aligned class whose inline capacity (262 B)
 * holds sdsReqSize(256)=260 -- i.e. it covers intern-max-len 256 as the design
 * intends. The small classes pack the schema/field-name lengths densely (R5). */
static const size_t intern_class_bytes[INTERN_NUM_CLASSES] = {48, 64, 96, 128, 192, 296};

/* Max internable content length: the slot stores content as an inline sdshdr8,
 * whose alloc byte tops out at 255. Longer strings are never interned so that
 * internSlotFromSds can recover the header at a fixed sdsHdrSize(SDS_TYPE_8)
 * offset in every size class (the never-move contract). */
#define INTERN_MAX_INLINE_LEN 255 /* sdsTypeMaxSize(SDS_TYPE_8) */

typedef struct internSlab internSlab;

/* One slab: a single zmalloc of `bytes`, carved into fixed-size slots of one
 * class. Live slots are those handed out and not yet returned to the free list. */
struct internSlab {
    internSlab *next;      /* class slab list link */
    internSlab *prev;
    uint32_t live;         /* slots handed out and not on the free list */
    uint32_t capacity;     /* slots this slab can carve */
    uint8_t klass;         /* owning class index */
    char *base;            /* first slot byte (== (char*)(this+1) rounded) */
    size_t bytes;          /* total zmalloc bytes for this slab */
};

/* A slot on the free list borrows its first bytes for the link pointer. The
 * owning slab pointer lives in the LAST sizeof(internSlab*) bytes of every slot
 * (reserved out of the inline capacity), so it survives both the free and the
 * live state and owner recovery on return is O(1) -- no slab-list scan. */
typedef struct internFreeSlot {
    struct internFreeSlot *next;
} internFreeSlot;

#define INTERN_SLOT_OWNER_BYTES ((size_t)sizeof(internSlab *))

typedef struct internClass {
    internSlab *slabs;       /* doubly-linked list of this class's slabs */
    internFreeSlot *freelist; /* intrusive free list of returned slots */
    internSlab *cushion;     /* at most one retained empty slab (may be NULL) */
    size_t slot_bytes;       /* == intern_class_bytes[klass] */
    size_t slots_total;      /* carved slots currently in existence (any state) */
    size_t slots_live;       /* live slots (== sum of slab->live) */
    size_t slab_count;       /* slabs held (including the cushion) */
    size_t slots_per_slab;   /* capacity per slab at the current config */
} internClass;

static internClass intern_classes[INTERN_NUM_CLASSES];
static int intern_classes_ready = 0;
static size_t intern_slab_total_bytes = 0; /* Σ slab bytes across all classes */
static size_t intern_long_allocs = 0;      /* live INTERN_CLASS_LONG slots */

/*-----------------------------------------------------------------------------
 * P1-2: admission caps and cold-admitted eviction state.
 *----------------------------------------------------------------------------*/

/* Total slots (live + admitted) across both indexes, maintained incrementally
 * on every slot add / free so internAtCap is O(1) and never walks the index. */
static size_t intern_slot_count = 0;

/* Cumulative admission-refusal counters (main-thread only). */
static size_t intern_refusals_total = 0;
static size_t intern_refusals_slots = 0;
static size_t intern_refusals_slab_bytes = 0;

/* Cumulative cold-admitted evictions (main-thread only). */
static size_t intern_evictions = 0;

/* Persistent eviction cursors, one per index, advanced across serverCron ticks
 * so every slot gets a bounded coverage guarantee (fair, deterministic -- not a
 * per-pass random offset that resets). See internEvictIdleBatch. */
static size_t intern_evict_cursor_value = 0;
static size_t intern_evict_cursor_field = 0;

size_t internSlotCount(void) {
    return intern_slot_count;
}

void internGetCapCounters(size_t *refusals_total, size_t *refusals_slots, size_t *refusals_slab_bytes) {
    if (refusals_total) *refusals_total = intern_refusals_total;
    if (refusals_slots) *refusals_slots = intern_refusals_slots;
    if (refusals_slab_bytes) *refusals_slab_bytes = intern_refusals_slab_bytes;
}

size_t internEvictions(void) {
    return intern_evictions;
}

/* Cap check for a MISS that wants to allocate a slot of `content_len` bytes.
 * Returns non-zero (refused) when either cap would be breached; sets *reason.
 * A zero cap means "no cap". The slab-bytes cap is checked against the CURRENT
 * slab bytes: an admit that fits in an existing slab's free list will not push
 * bytes up, but the conservative check (already at/over the cap) is what the
 * spec's "at cap" semantics require -- growth stops, existing refs unaffected. */
int internAtCap(size_t content_len, int *reason) {
    (void)content_len;
    size_t max_slots = server.intern_max_slots;
    size_t max_slab = server.intern_max_slab_bytes;
    if (max_slots != 0 && intern_slot_count >= max_slots) {
        if (reason) *reason = INTERN_REFUSE_SLOTS;
        return 1;
    }
    if (max_slab != 0 && intern_slab_total_bytes >= max_slab) {
        if (reason) *reason = INTERN_REFUSE_SLAB_BYTES;
        return 1;
    }
    return 0;
}


/* Header carved out of the front of each slab allocation. Slot 0 begins at an
 * 8-byte-aligned offset just past the internSlab header. */
static inline char *internSlabSlot(internSlab *slab, size_t idx, size_t slot_bytes) {
    return slab->base + idx * slot_bytes;
}

static void internClassesInit(void) {
    if (intern_classes_ready) return;
    for (int k = 0; k < INTERN_NUM_CLASSES; k++) {
        memset(&intern_classes[k], 0, sizeof(intern_classes[k]));
        intern_classes[k].slot_bytes = intern_class_bytes[k];
    }
    intern_classes_ready = 1;
}

/* Slots that fit a class's inline capacity. The last INTERN_SLOT_OWNER_BYTES of
 * every slot hold the owning-slab back-pointer, so they are not available to the
 * sds. */
static inline size_t internClassInlineCap(size_t slot_bytes) {
    return slot_bytes - offsetof(internSlot, inline_) - INTERN_SLOT_OWNER_BYTES;
}

/* Address of a slot's trailing owner back-pointer. */
static inline internSlab **internSlotOwnerRef(internSlot *slot, size_t slot_bytes) {
    return (internSlab **)((char *)slot + slot_bytes - INTERN_SLOT_OWNER_BYTES);
}

/* Smallest class index whose inline capacity fits `need` (sdsReqSize bytes),
 * or INTERN_CLASS_LONG if none does. */
static uint8_t internClassFor(size_t need) {
    for (int k = 0; k < INTERN_NUM_CLASSES; k++) {
        if (need <= internClassInlineCap(intern_class_bytes[k])) return (uint8_t)k;
    }
    return INTERN_CLASS_LONG;
}

/* Number of slots that fit in a slab of `slab_bytes` for a class of `slot_bytes`,
 * after the slab header. At least 1. */
static size_t internSlotsPerSlab(size_t slab_bytes, size_t slot_bytes) {
    size_t hdr = (sizeof(internSlab) + 15) & ~(size_t)15; /* 16-byte aligned base */
    if (slab_bytes <= hdr + slot_bytes) return 1;
    return (slab_bytes - hdr) / slot_bytes;
}

/* Configured slab byte size, clamped to the config bounds. */
static size_t internSlabBytes(void) {
    size_t b = server.intern_slab_bytes;
    if (b < 4096) b = 4096;
    if (b > (8 * 1024 * 1024)) b = 8 * 1024 * 1024;
    return b;
}

/* Allocate one slab for a class, thread all its slots onto the class free list,
 * and link it into the class slab list. */
static internSlab *internSlabNew(internClass *cls, uint8_t klass) {
    size_t slab_bytes = internSlabBytes();
    size_t slot_bytes = cls->slot_bytes;
    size_t cap = internSlotsPerSlab(slab_bytes, slot_bytes);
    internSlab *slab = zmalloc(slab_bytes);
    slab->next = slab->prev = NULL;
    slab->live = 0;
    slab->capacity = (uint32_t)cap;
    slab->klass = klass;
    slab->bytes = zmalloc_usable_size(slab);
    size_t hdr = (sizeof(internSlab) + 15) & ~(size_t)15;
    slab->base = (char *)slab + hdr;

    /* Thread every slot onto the free list (LIFO) and stamp each slot's trailing
     * owner back-pointer. */
    for (size_t i = 0; i < cap; i++) {
        char *p = internSlabSlot(slab, i, slot_bytes);
        *(internSlab **)(p + slot_bytes - INTERN_SLOT_OWNER_BYTES) = slab;
        internFreeSlot *fs = (internFreeSlot *)p;
        fs->next = cls->freelist;
        cls->freelist = fs;
    }
    /* Link into the class slab list. */
    slab->next = cls->slabs;
    if (cls->slabs) cls->slabs->prev = slab;
    cls->slabs = slab;

    cls->slab_count++;
    cls->slots_total += cap;
    cls->slots_per_slab = cap;
    intern_slab_total_bytes += slab->bytes;
    return slab;
}

/* Release an empty slab back to jemalloc, unlinking every one of its slots from
 * the class free list first. Only called when slab->live == 0. */
static void internSlabFree(internClass *cls, internSlab *slab) {
    size_t slot_bytes = cls->slot_bytes;
    /* Filter the class free list, dropping every slot that belongs to `slab`. */
    internFreeSlot **pp = &cls->freelist;
    size_t dropped = 0;
    while (*pp) {
        internFreeSlot *fs = *pp;
        internSlab *owner = *(internSlab **)((char *)fs + slot_bytes - INTERN_SLOT_OWNER_BYTES);
        if (owner == slab) {
            *pp = fs->next;
            dropped++;
        } else {
            pp = &fs->next;
        }
    }
    serverAssert(dropped == slab->capacity);
    /* Unlink from the class slab list. */
    if (slab->prev) slab->prev->next = slab->next; else cls->slabs = slab->next;
    if (slab->next) slab->next->prev = slab->prev;

    cls->slab_count--;
    cls->slots_total -= slab->capacity;
    intern_slab_total_bytes -= slab->bytes;
    zfree(slab);
}

/* Carve a slot of `klass` from the class free list (allocating a slab if the
 * list is empty). The caller fills the header; the trailing owner pointer is
 * already stamped and is preserved. */
static internSlot *internSlabAlloc(uint8_t klass) {
    internClassesInit();
    internClass *cls = &intern_classes[klass];
    if (cls->freelist == NULL) {
        internSlabNew(cls, klass);
    }
    internFreeSlot *fs = cls->freelist;
    cls->freelist = fs->next;
    internSlab *slab = *(internSlab **)((char *)fs + cls->slot_bytes - INTERN_SLOT_OWNER_BYTES);
    slab->live++;
    cls->slots_live++;
    /* If this was the cushion (now non-empty), it is no longer the cushion. */
    if (cls->cushion == slab) cls->cushion = NULL;
    internSlot *slot = (internSlot *)fs;
    slot->slab_class = klass;
    return slot;
}

/* Return a slab-managed slot to its class free list; release the slab at zero
 * live unless it becomes the class cushion. */
static void internSlabReturn(internSlot *slot) {
    uint8_t klass = slot->slab_class;
    serverAssert(klass < INTERN_NUM_CLASSES);
    internClass *cls = &intern_classes[klass];
    /* Owner recovered O(1) from the slot's trailing back-pointer. */
    internSlab *owner = *internSlotOwnerRef(slot, cls->slot_bytes);
    serverAssert(owner != NULL && owner->klass == klass);
    /* The trailing owner back-pointer is preserved across the union with
     * internFreeSlot (it lives past the link), so just re-link onto the list. */
    internFreeSlot *fs = (internFreeSlot *)slot;
    fs->next = cls->freelist;
    cls->freelist = fs;
    serverAssert(owner->live > 0);
    owner->live--;
    cls->slots_live--;

    if (owner->live == 0) {
        /* Keep one empty slab as a cushion; release any further empties. */
        if (cls->cushion == NULL) {
            cls->cushion = owner;
        } else if (cls->cushion != owner) {
            internSlabFree(cls, owner);
        }
    }
}

/* Free one slot: return it to its slab, or zfree a long allocation. */
static void internSlotFree(internSlot *slot) {
    serverAssert(intern_slot_count > 0);
    intern_slot_count--;
    if (slot->slab_class == INTERN_CLASS_LONG) {
        serverAssert(intern_long_allocs > 0);
        intern_long_allocs--;
        zfree(slot);
    } else {
        internSlabReturn(slot);
    }
}

size_t internSlabMemory(void) {
    return intern_slab_total_bytes;
}

size_t internSlabCount(void) {
    size_t n = 0;
    if (!intern_classes_ready) return 0;
    for (int k = 0; k < INTERN_NUM_CLASSES; k++) n += intern_classes[k].slab_count;
    return n;
}

double internSlabLiveRatio(void) {
    size_t live = 0, total = 0;
    if (!intern_classes_ready) return 1.0;
    for (int k = 0; k < INTERN_NUM_CLASSES; k++) {
        live += intern_classes[k].slots_live;
        total += intern_classes[k].slots_total;
    }
    if (total == 0) return 1.0;
    return (double)live / (double)total;
}

void internGetClassStats(internClassStats out[INTERN_NUM_CLASSES], size_t *long_allocs) {
    internClassesInit();
    for (int k = 0; k < INTERN_NUM_CLASSES; k++) {
        out[k].slot_bytes = intern_classes[k].slot_bytes;
        out[k].slots_total = intern_classes[k].slots_total;
        out[k].slots_live = intern_classes[k].slots_live;
        out[k].slabs = intern_classes[k].slab_count;
        out[k].slots_per_slab = intern_classes[k].slots_per_slab;
    }
    if (long_allocs) *long_allocs = intern_long_allocs;
}

static void internInit(void) {
    if (intern_index_value == NULL) intern_index_value = hashtableCreate(&internIndexType);
    if (intern_index_field == NULL) intern_index_field = hashtableCreate(&internIndexType);
    internClassesInit();
}

/* True if the slot stores its content in the fixed inline region (slab-managed),
 * vs a long allocation whose sds lives in a separate zmalloc. */
static inline bool internFieldIsInline(const internSlot *slot) {
    return slot->slab_class != INTERN_CLASS_LONG;
}

internSlot *internSlotFromSds(const sds s) {
    return (internSlot *)((char *)s - sdsHdrSize(SDS_TYPE_8) - offsetof(internSlot, inline_));
}

internSlot *internFieldSlotFromSds(const sds s) {
    return (internSlot *)((char *)s - sdsHdrSize(SDS_TYPE_8) - offsetof(internSlot, inline_));
}

/* Accounting size of a slot. For a slab-managed slot this is the CLASS slot byte
 * size (its exact share of the slab), NOT zmalloc_size of the whole slab. For a
 * long allocation it is the raw zmalloc size. Slab bytes are additionally
 * accounted once as server overhead via internSlabMemory(); this per-slot figure
 * feeds only the amortised entryMemUsage share. */
size_t internSlotAllocSize(const internSlot *slot) {
    if (slot->slab_class == INTERN_CLASS_LONG) return zmalloc_size((void *)slot);
    return intern_class_bytes[slot->slab_class];
}

size_t internSlotFieldAllocSize(const internSlot *slot) {
    if (slot->slab_class == INTERN_CLASS_LONG) return zmalloc_size((void *)slot);
    return intern_class_bytes[slot->slab_class];
}

uint32_t internSlotRefcount(const internSlot *slot) {
    return slot->refcount;
}

sds internSlotFieldSds(const internSlot *slot) {
    return (sds)slot->buf;
}

sds internSlotSds(const internSlot *slot) {
    return (sds)slot->buf;
}

/* Common acquire: probe `index`, on hit bump refcount, on miss build a slot with
 * `builder` and insert. `is_field` selects the storage layout. */
static internSlot *internAcquireImpl(hashtable *index, const char *buf, size_t len, bool is_field) {
    internSlot probe;
    probe.buf = buf;
    probe.len = len;
    probe.hash = (uint32_t)hashtableGenHashFunction(buf, len);

    void *found;
    if (hashtableFind(index, &probe, &found)) {
        internSlot *slot = found;
        slot->refcount++;
        return slot;
    }

    /* MISS: a new slot would grow the table. Enforce the admission caps here --
     * a refused miss returns NULL and the caller keeps its owned copy exactly as
     * a probe miss does (P1-2). Probe/acquire HITS above never reach this and are
     * never refused. */
    {
        int reason = INTERN_REFUSE_SLOTS;
        if (internAtCap(len, &reason)) {
            intern_refusals_total++;
            if (reason == INTERN_REFUSE_SLAB_BYTES) intern_refusals_slab_bytes++;
            else intern_refusals_slots++;
            return NULL;
        }
    }

    (void)is_field;
    /* Both kinds: lay out an sdshdr8 INLINE so slot->buf is a genuine sds. The
     * holder stores that sds pointer directly (read path == plain sds value /
     * plain sds field: one dependent load) and recovers the slot header at a
     * fixed negative offset via internSlotFromSds -- class-independent, so slabs
     * are never-move-safe for that contract. Content is bounded to 255 B (the
     * sdshdr8 alloc-byte limit); callers gate this via internLenEligible /
     * DEBUG_INTERN_MAX_LEN. */
    serverAssert(len <= INTERN_MAX_INLINE_LEN);
    size_t need = sdsReqSize(len, SDS_TYPE_8); /* hdr(3) + len + 1 */
    uint8_t klass = internClassFor(need);

    internSlot *slot;
    if (klass != INTERN_CLASS_LONG) {
        /* Slab-managed: carve a fixed-size slot from the class free list. */
        slot = internSlabAlloc(klass);
    } else {
        /* Long fallback (only reachable if intern-max-len is raised past the
         * biggest class's inline capacity): a raw zmalloc, not slab-managed. */
        size_t alloc = offsetof(internSlot, inline_) + need;
        slot = zmalloc(alloc);
        slot->slab_class = INTERN_CLASS_LONG;
        intern_long_allocs++;
    }
    slot->len = len;
    slot->refcount = 1;
    slot->hash = probe.hash;
    slot->admitted = 0;
    slot->idle_since_tick = 0; /* referenced (refcount 1); idle tick unused */
    slot->buf = sdswrite(slot->inline_, need, SDS_TYPE_8, buf, len);
    serverAssert(hashtableAdd(index, slot));
    intern_slot_count++;
    return slot;
}

internSlot *internAcquire(const char *buf, size_t len) {
    internInit();
    return internAcquireImpl(intern_index_value, buf, len, false);
}

internSlot *internAcquireField(const char *buf, size_t len) {
    internInit();
    return internAcquireImpl(intern_index_field, buf, len, true);
}

/*-----------------------------------------------------------------------------
 * Write-time probe + admission (P2 S4a).
 *----------------------------------------------------------------------------*/

/* Cumulative probe counters, main-thread only. */
static size_t intern_probe_value_hits = 0, intern_probe_value_misses = 0;
static size_t intern_probe_field_hits = 0, intern_probe_field_misses = 0;

/* Content is probe/admission eligible only within [4, min(intern_max_len, 255)].
 * The upper cap is 255 because the slot lays its content out as an inline
 * sdshdr8 (whose alloc byte tops out at 255) so internSlotFromSds can recover
 * the header at a fixed sdsHdrSize(SDS_TYPE_8) offset in every size class. The
 * length gate bounds the hash cost and is checked before hashing. */
static inline bool internLenEligible(size_t len) {
    size_t hi = server.intern_max_len < INTERN_MAX_INLINE_LEN ? server.intern_max_len : INTERN_MAX_INLINE_LEN;
    return len >= 4 && len <= hi;
}

/* Lookup-only probe on `index`. On hit: refcount++ (a reference handed to the
 * caller) and return the slot. On miss: return NULL, insert nothing. */
static internSlot *internProbeImpl(hashtable *index,
                                   const char *buf,
                                   size_t len,
                                   size_t *hits,
                                   size_t *misses) {
    if (index == NULL || !internLenEligible(len)) {
        (*misses)++;
        return NULL;
    }
    internSlot probe;
    probe.buf = buf;
    probe.len = len;
    probe.hash = (uint32_t)hashtableGenHashFunction(buf, len);
    void *found;
    if (hashtableFind(index, &probe, &found)) {
        internSlot *slot = found;
        slot->refcount++;
        (*hits)++;
        return slot;
    }
    (*misses)++;
    return NULL;
}

internSlot *internProbeValue(const char *buf, size_t len) {
    return internProbeImpl(intern_index_value, buf, len, &intern_probe_value_hits, &intern_probe_value_misses);
}

internSlot *internProbeField(const char *buf, size_t len) {
    return internProbeImpl(intern_index_field, buf, len, &intern_probe_field_hits, &intern_probe_field_misses);
}

void internGetProbeCounters(size_t *value_hits, size_t *value_misses, size_t *field_hits, size_t *field_misses) {
    if (value_hits) *value_hits = intern_probe_value_hits;
    if (value_misses) *value_misses = intern_probe_value_misses;
    if (field_hits) *field_hits = intern_probe_field_hits;
    if (field_misses) *field_misses = intern_probe_field_misses;
}

/* Current idle tick: serverCron uses one-second resolution (server.unixtime),
 * which is monotonic enough for the idle-eviction policy and free to read. A
 * freshly cold slot stamped now has age 0. */
static inline uint32_t internNowTick(void) {
    return (uint32_t)server.unixtime;
}

/* Insert content as an admitted slot (refcount 0, admitted). If already present,
 * mark the existing slot admitted without touching its refcount. Returns NULL
 * (nothing admitted) when a MISS would breach an admission cap (P1-2). */
static internSlot *internAdmitImpl(hashtable *index, const char *buf, size_t len, bool is_field) {
    internSlot probe;
    probe.buf = buf;
    probe.len = len;
    probe.hash = (uint32_t)hashtableGenHashFunction(buf, len);
    void *found;
    if (hashtableFind(index, &probe, &found)) {
        internSlot *slot = found;
        slot->admitted = 1;
        /* An already-present cold slot that was not admitted before is now an
         * admission decision; stamp it cold-from-now if it is unreferenced. */
        if (slot->refcount == 0) slot->idle_since_tick = internNowTick();
        return slot;
    }
    /* Build via the acquire path (refcount 1) then hand the reference back:
     * decrement to 0 and mark admitted so the slot stays index-owned. A refused
     * miss (cap reached) returns NULL from internAcquireImpl -- propagate it. */
    internSlot *slot = internAcquireImpl(index, buf, len, is_field);
    if (slot == NULL) return NULL;
    slot->admitted = 1;
    slot->idle_since_tick = internNowTick(); /* admitted cold: age starts now */
    slot->refcount = 0;
    return slot;
}

internSlot *internAdmitValue(const char *buf, size_t len) {
    internInit();
    return internAdmitImpl(intern_index_value, buf, len, false);
}

internSlot *internAdmitField(const char *buf, size_t len) {
    internInit();
    return internAdmitImpl(intern_index_field, buf, len, true);
}

static void internEvictAdmittedIndex(hashtable *index) {
    if (index == NULL) return;
    /* Snapshot the refcount-0 admitted slots, then delete them (deleting during
     * iteration is unsafe). */
    size_t n = hashtableSize(index);
    if (n == 0) return;
    internSlot **dead = zmalloc(sizeof(internSlot *) * n);
    size_t d = 0;
    hashtableIterator iter;
    hashtableInitIterator(&iter, index, 0);
    void *elem;
    while (hashtableNext(&iter, &elem) && d < n) {
        internSlot *slot = elem;
        if (slot->admitted && slot->refcount == 0) dead[d++] = slot;
    }
    hashtableCleanupIterator(&iter);
    for (size_t i = 0; i < d; i++) {
        serverAssert(hashtableDelete(index, dead[i]));
        internSlotFree(dead[i]);
    }
    zfree(dead);
}

void internEvictAdmitted(void) {
    internEvictAdmittedIndex(intern_index_value);
    internEvictAdmittedIndex(intern_index_field);
}

/*-----------------------------------------------------------------------------
 * P1-2: cold-admitted eviction.
 *
 * Only admitted refcount-0 slots are eviction candidates -- referenced slots are
 * never touched (their holders own them; there is no reverse index over holders,
 * so a referenced slot cannot be reclaimed here -- explicitly out of scope, see
 * HLD §13 O5). A candidate's idle age is (now_tick - idle_since_tick).
 *----------------------------------------------------------------------------*/

/* Idle age of an admitted, unreferenced slot in ticks (0 if referenced). */
static inline uint32_t internSlotIdleAge(const internSlot *slot, uint32_t now_tick) {
    if (slot->refcount != 0) return 0;
    /* Unsigned wrap is fine: server.unixtime moves forward; a slot stamped in the
     * future (clock stepped back) reads age 0, which just defers its eviction. */
    return now_tick - slot->idle_since_tick;
}

uint32_t internAdmittedIdleMaxTicks(uint32_t now_tick) {
    uint32_t maxage = 0;
    hashtable *idxs[2] = {intern_index_value, intern_index_field};
    for (int x = 0; x < 2; x++) {
        if (idxs[x] == NULL) continue;
        hashtableIterator iter;
        hashtableInitIterator(&iter, idxs[x], 0);
        void *elem;
        while (hashtableNext(&iter, &elem)) {
            internSlot *slot = elem;
            if (slot->admitted && slot->refcount == 0) {
                uint32_t age = internSlotIdleAge(slot, now_tick);
                if (age > maxage) maxage = age;
            }
        }
        hashtableCleanupIterator(&iter);
    }
    return maxage;
}

/* Delete a batch of candidate slots from `index` and free them, bumping the
 * eviction counter. */
static void internEvictDeleteBatch(hashtable *index, internSlot **dead, size_t d) {
    for (size_t i = 0; i < d; i++) {
        serverAssert(hashtableDelete(index, dead[i]));
        internSlotFree(dead[i]);
        intern_evictions++;
    }
}

/* Full pass: evict every admitted refcount-0 slot idle for > idle_ticks. A
 * idle_ticks of UINT32_MAX means "no eviction" (never); callers gate that. */
static size_t internEvictIdleFullIndex(hashtable *index, uint32_t now_tick, uint32_t idle_ticks) {
    if (index == NULL) return 0;
    size_t n = hashtableSize(index);
    if (n == 0) return 0;
    internSlot **dead = zmalloc(sizeof(internSlot *) * n);
    size_t d = 0;
    hashtableIterator iter;
    hashtableInitIterator(&iter, index, 0);
    void *elem;
    while (hashtableNext(&iter, &elem) && d < n) {
        internSlot *slot = elem;
        if (slot->admitted && slot->refcount == 0 && internSlotIdleAge(slot, now_tick) > idle_ticks)
            dead[d++] = slot;
    }
    hashtableCleanupIterator(&iter);
    internEvictDeleteBatch(index, dead, d);
    zfree(dead);
    return d;
}

size_t internEvictIdleFull(uint32_t now_tick, uint32_t idle_ticks) {
    size_t evicted = 0;
    evicted += internEvictIdleFullIndex(intern_index_value, now_tick, idle_ticks);
    evicted += internEvictIdleFullIndex(intern_index_field, now_tick, idle_ticks);
    return evicted;
}

/* Batch scan state passed through the hashtableScan callback: it only COLLECTS
 * candidates (deleting inside a scan callback is unsafe); the caller deletes
 * after the scan returns. */
typedef struct internEvictScanCtx {
    uint32_t now_tick;
    uint32_t idle_ticks;   /* evict when age > this; ignored under pressure */
    int pressure;          /* evict cold slots regardless of age, oldest first */
    internSlot **dead;     /* candidate array (capacity == cap) */
    size_t d;              /* candidates collected */
    size_t cap;            /* max candidates to collect this batch */
} internEvictScanCtx;

static void internEvictScanCb(void *privdata, void *entry) {
    internEvictScanCtx *ctx = privdata;
    if (ctx->d >= ctx->cap) return;
    internSlot *slot = entry;
    if (!slot->admitted || slot->refcount != 0) return;
    if (!ctx->pressure && internSlotIdleAge(slot, ctx->now_tick) <= ctx->idle_ticks) return;
    ctx->dead[ctx->d++] = slot;
}

/* Bounded incremental eviction from one index using a persistent cursor. Scans
 * buckets until it has looked at ~`budget` candidates or the cursor wraps, then
 * deletes the collected candidates. Under pressure, collected cold slots are
 * sorted oldest-idle-first before eviction so the coldest go first. */
static size_t internEvictIdleBatchIndex(hashtable *index,
                                        size_t *cursor,
                                        uint32_t now_tick,
                                        uint32_t idle_ticks,
                                        size_t budget,
                                        int pressure) {
    if (index == NULL || budget == 0) return 0;
    size_t n = hashtableSize(index);
    if (n == 0) { *cursor = 0; return 0; }

    internSlot **dead = zmalloc(sizeof(internSlot *) * budget);
    internEvictScanCtx ctx = {now_tick, idle_ticks, pressure, dead, 0, budget};

    /* Deleting slots can trigger an auto-shrink, and a shrink is the one thing
     * that invalidates the reverse-binary scan cursor we persist across ticks
     * (recorded hashtable.c behaviour). Pause auto-shrink for the batch so the
     * cursor stays coherent; resume after (a deferred shrink then happens on the
     * next mutation). */
    hashtablePauseAutoShrink(index);

    /* Advance the cursor across buckets, collecting up to `budget` candidates.
     * hashtableScan visits one bucket-run per call and returns the next cursor;
     * a returned 0 means a full cycle completed. Cap the number of scan calls to
     * bound work even when the table is huge and sparse in candidates. */
    size_t scan_calls = 0, max_scan_calls = budget + 16;
    size_t cur = *cursor;
    do {
        cur = hashtableScan(index, cur, internEvictScanCb, &ctx);
        scan_calls++;
    } while (cur != 0 && ctx.d < budget && scan_calls < max_scan_calls);
    *cursor = cur; /* persist for the next tick (0 == wrapped to start) */

    if (pressure && ctx.d > 1) {
        /* Oldest idle first: simple insertion sort (batch is small, <= budget). */
        for (size_t i = 1; i < ctx.d; i++) {
            internSlot *k = dead[i];
            uint32_t ka = internSlotIdleAge(k, now_tick);
            size_t j = i;
            while (j > 0 && internSlotIdleAge(dead[j - 1], now_tick) < ka) {
                dead[j] = dead[j - 1];
                j--;
            }
            dead[j] = k;
        }
    }
    internEvictDeleteBatch(index, dead, ctx.d);
    hashtableResumeAutoShrink(index);
    zfree(dead);
    return ctx.d;
}

size_t internEvictIdleBatch(uint32_t now_tick, uint32_t idle_ticks, size_t batch, int pressure) {
    /* Split the per-tick budget across the two indexes. */
    size_t half = batch / 2;
    size_t evicted = 0;
    evicted += internEvictIdleBatchIndex(intern_index_value, &intern_evict_cursor_value,
                                         now_tick, idle_ticks, half ? half : 1, pressure);
    evicted += internEvictIdleBatchIndex(intern_index_field, &intern_evict_cursor_field,
                                         now_tick, idle_ticks, batch - half ? batch - half : 1, pressure);
    return evicted;
}


/*-----------------------------------------------------------------------------
 * Persistence of the admission decision (P2 S4d).
 *----------------------------------------------------------------------------*/

/* Last-save / last-load counters, main-thread only. */
static size_t intern_admitted_persisted = 0;
static size_t intern_admitted_loaded = 0;

static size_t internForEachAdmittedIndex(hashtable *index, uint8_t kind, internAdmittedCb cb, void *privdata) {
    if (index == NULL) return 0;
    size_t n = 0;
    hashtableIterator iter;
    hashtableInitIterator(&iter, index, 0);
    void *elem;
    while (hashtableNext(&iter, &elem)) {
        internSlot *slot = elem;
        if (!slot->admitted) continue;
        cb(kind, slot->buf, slot->len, privdata);
        n++;
    }
    hashtableCleanupIterator(&iter);
    return n;
}

size_t internForEachAdmitted(internAdmittedCb cb, void *privdata) {
    size_t n = 0;
    n += internForEachAdmittedIndex(intern_index_value, 0, cb, privdata);
    n += internForEachAdmittedIndex(intern_index_field, 1, cb, privdata);
    return n;
}

size_t internAdmittedCount(void) {
    internStats stats;
    internGetStats(&stats);
    return stats.admitted_slots;
}

void internSetAdmittedPersisted(size_t n) {
    intern_admitted_persisted = n;
}

void internSetAdmittedLoaded(size_t n) {
    intern_admitted_loaded = n;
}

void internGetPersistCounters(size_t *persisted, size_t *loaded) {
    if (persisted) *persisted = intern_admitted_persisted;
    if (loaded) *loaded = intern_admitted_loaded;
}

/*-----------------------------------------------------------------------------
 * Deferred, coalesced release (O8 / S5).
 *
 * Bio lazy-free threads never touch refcounts or the index. Between
 * internDeferBegin() and internDeferPublish() a thread records each release into
 * a thread-local pointer-keyed coalescing map; publish turns the map into one
 * message pushed onto a lock-free MPSC stack; main drains and applies.
 *----------------------------------------------------------------------------*/

/* Main-thread id, captured at initServer, for the debug-build guard. */
static pthread_t intern_main_thread;
static int intern_main_thread_set = 0;

void internSetMainThread(void) {
    intern_main_thread = pthread_self();
    intern_main_thread_set = 1;
}

/* True on the main thread (or before capture, e.g. very early init). */
static inline int internOnMainThread(void) {
    return !intern_main_thread_set || pthread_equal(pthread_self(), intern_main_thread);
}

/* One coalescing-map entry: a slot and the number of pending decrements, plus
 * the kind so the drain calls the right release logic. kind 0 = value, 1 = field. */
typedef struct internDeferEntry {
    internSlot *slot; /* NULL == empty bucket */
    uint32_t count;
    uint8_t kind;
} internDeferEntry;

/* Thread-local coalescing map: open addressing, linear probing, pointer-keyed,
 * doubles at 70% load, starts at 64 buckets. */
typedef struct internDeferMap {
    internDeferEntry *tab;
    size_t cap;   /* power of two, number of buckets */
    size_t used;  /* occupied buckets */
} internDeferMap;

static __thread int intern_defer_active = 0;
static __thread internDeferMap intern_defer_map;

#define INTERN_DEFER_INIT_CAP 64u

/* The published message: one per bio job. */
typedef struct internDeferMsg {
    struct internDeferMsg *_Atomic next; /* MPSC stack link */
    size_t n;
    internDeferEntry entries[]; /* flexible array */
} internDeferMsg;

/* MPSC stack head: producers = bio threads, consumer = main. */
static internDeferMsg *_Atomic intern_defer_head = NULL;

/* Counters. msgs/releases are main-thread-only (written in drain). pending is
 * atomic (incremented by producers, decremented by main). */
static size_t intern_deferred_msgs = 0;
static size_t intern_deferred_releases = 0;
static _Atomic size_t intern_deferred_pending = 0;

static inline size_t internDeferHash(const internSlot *slot) {
    /* Pointer-value hash (Fibonacci mix of the pointer bits). */
    uintptr_t x = (uintptr_t)slot;
    x ^= x >> 33;
    x *= (uintptr_t)0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    return (size_t)x;
}

static void internDeferMapAlloc(internDeferMap *m, size_t cap) {
    m->tab = zcalloc(cap * sizeof(internDeferEntry));
    m->cap = cap;
    m->used = 0;
}

/* Insert-or-increment slot in the map. Assumes there is room (grown by caller). */
static void internDeferMapBump(internDeferMap *m, internSlot *slot, uint8_t kind, uint32_t by) {
    size_t mask = m->cap - 1;
    size_t i = internDeferHash(slot) & mask;
    while (m->tab[i].slot != NULL) {
        if (m->tab[i].slot == slot) {
            m->tab[i].count += by;
            return;
        }
        i = (i + 1) & mask;
    }
    m->tab[i].slot = slot;
    m->tab[i].count = by;
    m->tab[i].kind = kind;
    m->used++;
}

static void internDeferMapGrow(internDeferMap *m) {
    size_t newcap = m->cap << 1;
    internDeferMap nm;
    internDeferMapAlloc(&nm, newcap);
    for (size_t i = 0; i < m->cap; i++) {
        if (m->tab[i].slot != NULL)
            internDeferMapBump(&nm, m->tab[i].slot, m->tab[i].kind, m->tab[i].count);
    }
    zfree(m->tab);
    *m = nm;
}

/* Record one deferred release. Called from internReleaseImpl on the deferring
 * thread; never reads slot fields other than the pointer identity. */
static void internDeferRecord(internSlot *slot, uint8_t kind) {
    internDeferMap *m = &intern_defer_map;
    if (m->tab == NULL) internDeferMapAlloc(m, INTERN_DEFER_INIT_CAP);
    /* Grow before insert so a new key always has room (70% load factor). */
    if ((m->used + 1) * 10 >= m->cap * 7) internDeferMapGrow(m);
    internDeferMapBump(m, slot, kind, 1);
}

void internDeferBegin(void) {
    /* A stray second begin without publish is a no-op: keep accumulating into
     * the same map. */
    intern_defer_active = 1;
}

void internDeferPublish(void) {
    intern_defer_active = 0;
    internDeferMap *m = &intern_defer_map;
    if (m->tab == NULL || m->used == 0) {
        /* Nothing recorded: free the map (if any) and publish nothing. */
        if (m->tab != NULL) {
            zfree(m->tab);
            m->tab = NULL;
            m->cap = 0;
            m->used = 0;
        }
        return;
    }

    size_t n = m->used;
    internDeferMsg *msg = zmalloc(sizeof(internDeferMsg) + n * sizeof(internDeferEntry));
    msg->n = n;
    size_t j = 0;
    for (size_t i = 0; i < m->cap; i++) {
        if (m->tab[i].slot != NULL) {
            msg->entries[j].slot = m->tab[i].slot;
            msg->entries[j].count = m->tab[i].count;
            msg->entries[j].kind = m->tab[i].kind;
            j++;
        }
    }
    serverAssert(j == n);

    /* Reset the thread-local map for the next job. */
    zfree(m->tab);
    m->tab = NULL;
    m->cap = 0;
    m->used = 0;

    /* Push onto the MPSC stack: load head, store as our next, CAS head to us.
     * Release semantics so main's acquire-exchange sees the fully-built msg. */
    internDeferMsg *old = atomic_load_explicit(&intern_defer_head, memory_order_relaxed);
    do {
        atomic_store_explicit(&msg->next, old, memory_order_relaxed);
    } while (!atomic_compare_exchange_weak_explicit(&intern_defer_head, &old, msg,
                                                    memory_order_release, memory_order_relaxed));
    atomic_fetch_add_explicit(&intern_deferred_pending, 1, memory_order_relaxed);
}

static void internReleaseImpl(hashtable *index, internSlot *slot, bool is_field) {
    /* Off-main deferral path: record the decrement and return before touching
     * refcount / admitted / the index. */
    if (intern_defer_active) {
        internDeferRecord(slot, is_field ? 1 : 0);
        return;
    }

    /* Immediate path: main thread only (debug-build guard). */
    serverAssert(internOnMainThread());
    serverAssert(slot->refcount > 0);
    if (--slot->refcount > 0) return;

    /* An admitted slot stays in the index at refcount 0 (an admission decision);
     * it is reclaimed only by cold-admitted eviction (internEvictIdle*) once it
     * has been idle for longer than intern-admitted-idle-ms, or by the test
     * helper internEvictAdmitted. Stamp the moment it became cold so the idle
     * clock starts here (write only on the transition to 0, per the design). */
    if (slot->admitted) {
        slot->idle_since_tick = internNowTick();
        return;
    }

    serverAssert(hashtableDelete(index, slot));
    (void)is_field;
    internSlotFree(slot); /* return to slab (or zfree a long alloc) */
}

size_t internDrainDeferred(void) {
    serverAssert(internOnMainThread());
    /* Pop the whole stack with acquire so we see each message fully built. */
    internDeferMsg *msg = atomic_exchange_explicit(&intern_defer_head, NULL, memory_order_acquire);
    size_t applied = 0;
    while (msg != NULL) {
        internDeferMsg *next = atomic_load_explicit(&msg->next, memory_order_relaxed);
        for (size_t i = 0; i < msg->n; i++) {
            internSlot *slot = msg->entries[i].slot;
            uint32_t count = msg->entries[i].count;
            hashtable *index = msg->entries[i].kind == 1 ? intern_index_field : intern_index_value;
            /* Apply all but one decrement as plain subtraction, then run the
             * at-zero reclamation exactly once via the normal release path. */
            serverAssert(slot->refcount >= count);
            slot->refcount -= (count - 1);
            internReleaseImpl(index, slot, msg->entries[i].kind == 1);
            applied += count;
        }
        intern_deferred_msgs++;
        atomic_fetch_sub_explicit(&intern_deferred_pending, 1, memory_order_relaxed);
        zfree(msg);
        msg = next;
    }
    intern_deferred_releases += applied;
    return applied;
}

void internGetDeferredCounters(size_t *msgs, size_t *releases, size_t *pending) {
    if (msgs) *msgs = intern_deferred_msgs;
    if (releases) *releases = intern_deferred_releases;
    if (pending) *pending = atomic_load_explicit(&intern_deferred_pending, memory_order_relaxed);
}

void internRetain(internSlot *slot) {
    serverAssert(slot->refcount > 0);
    slot->refcount++;
}

void internRelease(internSlot *slot) {
    internReleaseImpl(intern_index_value, slot, false);
}

void internReleaseField(internSlot *slot) {
    internReleaseImpl(intern_index_field, slot, true);
}

size_t internBytesSaved(void) {
    internStats stats;
    internGetStats(&stats);
    return stats.bytes_saved;
}

/* Length histogram bucket index: <=8, 9-16, 17-20, 21-23, 24-32, 33-64, >64. */
static int internHistBucket(size_t len) {
    if (len <= 8) return 0;
    if (len <= 16) return 1;
    if (len <= 20) return 2;
    if (len <= 23) return 3;
    if (len <= 32) return 4;
    if (len <= 64) return 5;
    return 6;
}

static void internAccumulate(hashtable *index, bool is_field, internStats *stats) {
    if (index == NULL) return;
    hashtableIterator iter;
    hashtableInitIterator(&iter, index, 0);
    void *elem;
    while (hashtableNext(&iter, &elem)) {
        internSlot *slot = elem;
        stats->slots++;
        if (slot->admitted) {
            stats->admitted_slots++;
            if (slot->refcount == 0) stats->admitted_unreferenced++;
        }
        if (is_field) {
            stats->field_slots++;
            if (internFieldIsInline(slot)) stats->sso_slots++; else stats->long_slots++;
        } else {
            stats->value_slots++;
            if (internFieldIsInline(slot)) stats->sso_slots++; else stats->long_slots++;
        }
        stats->hist[internHistBucket(slot->len)]++;
        if (slot->refcount > 1) {
            stats->bytes_saved += (size_t)(slot->refcount - 1) * INTERN_PER_USE_OVERHEAD(slot->len);
            stats->dup_refs += (slot->refcount - 1);
        }
    }
    hashtableCleanupIterator(&iter);
    stats->index_mem += hashtableMemUsage(index);
}

void internGetStats(internStats *stats) {
    memset(stats, 0, sizeof(*stats));
    internAccumulate(intern_index_value, false, stats);
    internAccumulate(intern_index_field, true, stats);
}
