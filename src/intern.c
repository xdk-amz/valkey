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

static void internInit(void) {
    if (intern_index_value == NULL) intern_index_value = hashtableCreate(&internIndexType);
    if (intern_index_field == NULL) intern_index_field = hashtableCreate(&internIndexType);
}

/* True if the field slot stores its sdshdr8 inline (vs a long allocation). */
/* Field sds is always inline; "SSO" for stats means it fit the fixed 48 B slot. */
static inline bool internFieldIsInline(const internSlot *slot) {
    return sdsReqSize(slot->len, SDS_TYPE_8) <= sizeof(slot->inline_);
}

internSlot *internSlotFromSds(const sds s) {
    return (internSlot *)((char *)s - sdsHdrSize(SDS_TYPE_8) - offsetof(internSlot, inline_));
}

internSlot *internFieldSlotFromSds(const sds s) {
    return (internSlot *)((char *)s - sdsHdrSize(SDS_TYPE_8) - offsetof(internSlot, inline_));
}

size_t internSlotAllocSize(const internSlot *slot) {
    /* VALUE slot: the slot itself, plus a long byte buffer if content did not
     * fit inline. slot->buf == slot->inline_ exactly when stored inline. */
    return zmalloc_size((void *)slot); /* variable-size allocation, sds inside */
}

size_t internSlotFieldAllocSize(const internSlot *slot) {
    /* FIELD slot: the slot itself, plus a long sdshdr8 allocation if the field
     * did not fit inline. The header base is slot->buf - sdsHdrSize(SDS_TYPE_8). */
    return zmalloc_size((void *)slot); /* variable-size allocation, sds inside */
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

    internSlot *slot = zmalloc(sizeof(internSlot));
    slot->len = len;
    slot->refcount = 1;
    slot->hash = probe.hash;
    slot->admitted = 0;

    (void)is_field;
    /* Both kinds: lay out an sdshdr8 INLINE so slot->buf is a genuine sds. The
     * holder stores that sds pointer directly (read path == plain sds value /
     * plain sds field: one dependent load) and recovers the slot header at a
     * fixed negative offset via internSlotFromSds. Variable-size allocation. */
    size_t need = sdsReqSize(len, SDS_TYPE_8); /* hdr(3) + len + 1 */
    if (need > sizeof(slot->inline_)) {
        size_t alloc = offsetof(internSlot, inline_) + need;
        internSlot *big = zmalloc(alloc);
        memcpy(big, slot, offsetof(internSlot, inline_));
        zfree(slot);
        slot = big;
    }
    slot->buf = sdswrite(slot->inline_, need, SDS_TYPE_8, buf, len);
    serverAssert(hashtableAdd(index, slot));
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

/* Content is probe/admission eligible only within [4, server.intern_max_len].
 * The length gate bounds the hash cost and is checked before hashing. */
static inline bool internLenEligible(size_t len) {
    return len >= 4 && len <= server.intern_max_len;
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

/* Insert content as an admitted slot (refcount 0, admitted). If already present,
 * mark the existing slot admitted without touching its refcount. */
static internSlot *internAdmitImpl(hashtable *index, const char *buf, size_t len, bool is_field) {
    internSlot probe;
    probe.buf = buf;
    probe.len = len;
    probe.hash = (uint32_t)hashtableGenHashFunction(buf, len);
    void *found;
    if (hashtableFind(index, &probe, &found)) {
        internSlot *slot = found;
        slot->admitted = 1;
        return slot;
    }
    /* Build via the acquire path (refcount 1) then hand the reference back:
     * decrement to 0 and mark admitted so the slot stays index-owned. */
    internSlot *slot = internAcquireImpl(index, buf, len, is_field);
    slot->admitted = 1;
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
        zfree(dead[i]);
    }
    zfree(dead);
}

void internEvictAdmitted(void) {
    internEvictAdmittedIndex(intern_index_value);
    internEvictAdmittedIndex(intern_index_field);
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
     * it is reclaimed only by internEvictAdmitted / cap policy, never inline. */
    if (slot->admitted) return;

    serverAssert(hashtableDelete(index, slot));
    (void)is_field;
    zfree(slot); /* the sds is always inside the slot allocation */
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
