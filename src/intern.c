/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "server.h"
#include "intern.h"

#include <stdatomic.h>

/* Shareable string layout:
 *
 *   [ internHdr ][ sdshdrN ][ bytes ][ \0 ]
 *                           ^ sds
 *
 * The sds type is at least SDS_TYPE_8 so its flags byte has room for the
 * SDS_AUX_SHARED bit. The alloc field equals len, so the string reports no free
 * space and is never grown. */
typedef struct internHdr {
    uint32_t refcount;
    uint32_t size : 24; /* bytes requested for the whole allocation */
    uint32_t where : 8; /* INTERN_IN_INDEX, INTERN_IN_PROBATION (possibly evicted since) or 0 */
} internHdr;

#define INTERN_IN_INDEX 1
#define INTERN_IN_PROBATION 2
#define INTERN_RETIRED 3 /* dead or revived, freed or released by internEpochTick() */

/* A string at this many references is not shared further. */
#define INTERN_REFS_MAX (UINT32_MAX - 1)

#define INTERN_MAX_LEN (512 * 1024)
#define INTERN_HASH_FULL_MAX 512

#define INTERN_LEVEL_MAX 6
#define INTERN_LEVEL_START 0
#define INTERN_WINDOW 64

#define PROB_WAYS 4
#define PROB_MIN_SETS 256
#define PROB_MAX_SETS 16384

internState intern_state __attribute__((aligned(4096))) = {.gate = {1, 1}};
int intern_load_ready = 0;

/* Set on the main thread at init; a forked child inherits it for its only thread. */
__thread int intern_is_main;

/* Names for the private fields of intern_state. */
#define intern_seed (intern_state.seed)
#define intern_index (intern_state.index)
#define prob (intern_state.prob)
#define prob_nsets (intern_state.prob_nsets)
#define prob_count (intern_state.prob_count)
#define prob_victim (intern_state.prob_victim)
#define intern_level (intern_state.level)
#define intern_sel_level (intern_state.sel_level)
#define intern_sel_mask (intern_state.sel_mask)
#define intern_salt (intern_state.salt)
#define intern_epoch (intern_state.epoch)
#define win_sel (intern_state.win_sel)
#define win_benefit (intern_state.win_benefit)
#define win_evict (intern_state.win_evict)
#define win_created (intern_state.win_created)
#define win_promoted (intern_state.win_promoted)
#define sel_floor_tick (intern_state.sel_floor_tick)
#define memo_key (intern_state.memo_key)
#define memo_hash (intern_state.memo_hash)
#define probe_buf (intern_state.probe_buf)
#define probe_len (intern_state.probe_len)
#define st (intern_state.stats)
#define K0 (intern_state.k0)
#define K1 (intern_state.k1)
#define K2 (intern_state.k2)
#define K3 (intern_state.k3)

typedef struct probSlot {
    uint64_t hash;
    sds s;
} probSlot;

static inline internHdr *internHdrOf(const_sds s) {
    return (internHdr *)((char *)s - sdsHdrSize(sdsType(s)) - sizeof(internHdr));
}

static inline size_t internSize(const_sds s) {
    return internHdrOf(s)->size;
}

/*-----------------------------------------------------------------------------
 * Hashing. A seeded multiply-mix hash over the whole string up to
 * INTERN_HASH_FULL_MAX bytes, and over the head, tail and eight interior windows
 * of longer strings. Equality is always confirmed with memcmp. The multiplier
 * constants are derived from the random seed at startup: with public constants
 * a block equal to a constant zeroes the product, which lets a client build
 * colliding strings without knowing the seed.
 *----------------------------------------------------------------------------*/

static inline uint64_t mix(uint64_t a, uint64_t b) {
    __uint128_t r = (__uint128_t)a * b;
    return (uint64_t)r ^ (uint64_t)(r >> 64);
}

static inline uint64_t rd64(const unsigned char *p) {
    uint64_t v;
    memcpy(&v, p, 8);
    return v;
}

static inline uint64_t rd32(const unsigned char *p) {
    uint32_t v;
    memcpy(&v, p, 4);
    return v;
}

static uint64_t hashBytes(const unsigned char *p, size_t len, uint64_t seed) {
    uint64_t a, b;
    seed ^= mix(seed ^ K0, K1);
    if (len <= 16) {
        if (len >= 4) {
            a = (rd32(p) << 32) | rd32(p + ((len >> 3) << 2));
            b = (rd32(p + len - 4) << 32) | rd32(p + len - 4 - ((len >> 3) << 2));
        } else if (len > 0) {
            a = ((uint64_t)p[0] << 16) | ((uint64_t)p[len >> 1] << 8) | p[len - 1];
            b = 0;
        } else {
            a = b = 0;
        }
    } else {
        size_t i = len;
        if (i > 48) {
            uint64_t s1 = seed, s2 = seed;
            do {
                seed = mix(rd64(p) ^ K1, rd64(p + 8) ^ seed);
                s1 = mix(rd64(p + 16) ^ K2, rd64(p + 24) ^ s1);
                s2 = mix(rd64(p + 32) ^ K3, rd64(p + 40) ^ s2);
                p += 48;
                i -= 48;
            } while (i > 48);
            seed ^= s1 ^ s2;
        }
        while (i > 16) {
            seed = mix(rd64(p) ^ K1, rd64(p + 8) ^ seed);
            i -= 16;
            p += 16;
        }
        a = rd64(p + i - 16);
        b = rd64(p + i - 8);
    }
    a ^= K1;
    b ^= seed;
    __uint128_t r = (__uint128_t)a * b;
    return mix((uint64_t)r ^ K0 ^ len, (uint64_t)(r >> 64) ^ K1);
}

static uint64_t internHash(const char *buf, size_t len) {
    const unsigned char *p = (const unsigned char *)buf;
    if (len <= INTERN_HASH_FULL_MAX) return hashBytes(p, len, intern_seed);
    uint64_t h = hashBytes(p, 256, intern_seed ^ len);
    h = hashBytes(p + len - 256, 256, h);
    size_t step = (len - 512) / 9;
    for (size_t k = 1; k <= 8; k++) {
        const unsigned char *w = p + 256 + step * k - 16;
        h = mix(rd64(w) ^ K1 ^ h, rd64(w + 8) ^ K2) ^ mix(rd64(w + 16) ^ K3, rd64(w + 24) ^ h);
    }
    return mix(h ^ len, K3);
}

/*-----------------------------------------------------------------------------
 * Index of strings that have, or had, more than one holder.
 *----------------------------------------------------------------------------*/

/* Admission already hashed the string it looks up, so the index reuses that
 * value instead of hashing again: memo_key and memo_hash are set right before an
 * index call and cleared after. A lookup of plain bytes passes &probe_key as the
 * key; the bytes are in probe_buf and probe_len. */
static const char probe_key = 0;

static uint64_t indexHash(const void *key) {
    if (key == memo_key) return memo_hash;
    const_sds s = key;
    return internHash(s, sdslen(s));
}

/* Returns non-zero when the keys are equal, as the hashtable expects. */
static int indexCompare(const void *a, const void *b) {
    const char *pa = a, *pb = b;
    size_t la, lb;
    if (pa == &probe_key) {
        pa = probe_buf;
        la = probe_len;
    } else {
        la = sdslen(pa);
    }
    if (pb == &probe_key) {
        pb = probe_buf;
        lb = probe_len;
    } else {
        lb = sdslen(pb);
    }
    return la == lb && memcmp(pa, pb, la) == 0;
}

static hashtableType internIndexType = {
    .hashFunction = indexHash,
    .keyCompare = indexCompare,
};

/*-----------------------------------------------------------------------------
 * Hot cache: a direct-mapped array of indexed strings keyed by a fingerprint of
 * the length and the first and last 8 bytes. A hit skips the full hash. A slot
 * only ever points at a string that is in the index; the slot is cleared when
 * that string leaves the index.
 *----------------------------------------------------------------------------*/

#define HOT_BITS 12
static_assert((1 << HOT_BITS) == INTERN_HOT_SLOTS, "hot cache size");
typedef struct hotSlotEntry {
    uint64_t tag; /* full mix of len + edge bytes; a mismatch skips the memcmp */
    sds s;
} hotSlotEntry;

static hotSlotEntry hot[1 << HOT_BITS];

/* intern_state.hot_used has one bit per slot, set while the slot holds a
 * string, so a lookup of an empty slot does not touch the slot's page. */
#define hot_used (intern_state.hot_used)

static inline int hotUsed(size_t i) {
    return (hot_used[i >> 6] >> (i & 63)) & 1;
}

static inline uint64_t hotMix(const char *p, size_t len) {
    return mix(rd64((const unsigned char *)p) ^ K1 ^ len, rd64((const unsigned char *)p + len - 8) ^ K2);
}

static inline size_t hotSlot(const char *p, size_t len) {
    return hotMix(p, len) >> (64 - HOT_BITS);
}

/* Slots are written only on the main thread. IO threads read them while they
 * parse, so writes are atomic and the string pointer is published last; a
 * reader that pairs a tag with another slot's string fails the compare. */
static inline void hotStore(const char *p, size_t len, sds s) {
    uint64_t m = hotMix(p, len);
    size_t i = m >> (64 - HOT_BITS);
    __atomic_store_n(&hot[i].tag, m, __ATOMIC_RELAXED);
    __atomic_store_n(&hot[i].s, s, __ATOMIC_RELEASE);
    hot_used[i >> 6] |= 1ull << (i & 63);
}

static inline void hotForget(sds s) {
    size_t i = hotSlot(s, sdslen(s));
    if (hotUsed(i) && hot[i].s == s) {
        __atomic_store_n(&hot[i].s, (sds)NULL, __ATOMIC_RELAXED);
        hot_used[i >> 6] &= ~(1ull << (i & 63));
    }
}

/* Returns the hot string equal to buf[0..len) or NULL. A fingerprint mismatch
 * returns without the memcmp, so a value the cache has not seen is cheap. */
static inline sds hotFind(const char *buf, size_t len) {
    uint64_t m = hotMix(buf, len);
    size_t i = m >> (64 - HOT_BITS);
    if (!hotUsed(i)) return NULL;
    hotSlotEntry *e = &hot[i];
    sds c = e->s;
    if (!c || e->tag != m || sdslen(c) != len || memcmp(c, buf, len) != 0) return NULL;
    return c;
}

/*-----------------------------------------------------------------------------
 * Filter of the strings in the index or in probation, by hash. A bucket holds up
 * to seven 8-bit fingerprints in its low bytes and, in its top byte, a count of
 * the strings that did not fit. A lookup whose fingerprint is not in its bucket,
 * when that count is zero, skips both tables and their pages. A full count
 * stays full.
 *----------------------------------------------------------------------------*/

#define FILTER_WAYS 7
#define FILTER_LOW 0x0001010101010101ull

static inline uint64_t *filterBucket(uint64_t h) {
    return &intern_state.filter[(h >> 8) & (INTERN_FILTER_BUCKETS - 1)];
}

static inline uint64_t filterFp(uint64_t h) {
    uint64_t f = (h >> 32) & 0xff;
    return f ? f : 1;
}

static void filterAdd(uint64_t h) {
    uint64_t *b = filterBucket(h);
    for (int i = 0; i < FILTER_WAYS; i++) {
        if (((*b >> (8 * i)) & 0xff) == 0) {
            *b |= filterFp(h) << (8 * i);
            return;
        }
    }
    if ((*b >> 56) != 0xff) *b += 1ull << 56;
}

static void filterRemove(uint64_t h) {
    uint64_t *b = filterBucket(h);
    uint64_t fp = filterFp(h);
    for (int i = 0; i < FILTER_WAYS; i++) {
        if (((*b >> (8 * i)) & 0xff) == fp) {
            *b &= ~(0xffull << (8 * i));
            return;
        }
    }
    uint64_t over = *b >> 56;
    serverAssert(over != 0);
    if (over != 0xff) *b -= 1ull << 56;
}

static inline int filterMayHave(uint64_t h) {
    uint64_t b = *filterBucket(h);
    if (b >> 56) return 1;
    uint64_t x = b ^ (filterFp(h) * FILTER_LOW);
    return ((x - FILTER_LOW) & ~x & (FILTER_LOW << 7)) != 0;
}

/*-----------------------------------------------------------------------------
 * Probation: a set-associative table of singletons waiting for a second holder.
 *----------------------------------------------------------------------------*/

static inline probSlot *probSet(uint64_t h) {
    return prob + ((h >> 20) & (prob_nsets - 1)) * PROB_WAYS;
}

static sds probTake(uint64_t h, const char *buf, size_t len) {
    probSlot *set = probSet(h);
    for (int w = 0; w < PROB_WAYS; w++) {
        sds c = set[w].s;
        if (c && set[w].hash == h && sdslen(c) == len && memcmp(c, buf, len) == 0) {
            set[w].s = NULL;
            prob_count--;
            filterRemove(h);
            internHdrOf(c)->where = 0;
            return c;
        }
    }
    return NULL;
}

static void probInsert(uint64_t h, sds s) {
    probSlot *set = probSet(h);
    int w;
    for (w = 0; w < PROB_WAYS; w++) {
        if (set[w].s == NULL) break;
    }
    if (w == PROB_WAYS) {
        /* The evicted string keeps where == INTERN_IN_PROBATION; its header is
         * cold, and release and defrag look the slot up before using it. */
        w = prob_victim++ & (PROB_WAYS - 1);
        prob_count--;
        filterRemove(set[w].hash);
        win_evict++;
    }
    set[w].hash = h;
    set[w].s = s;
    prob_count++;
    filterAdd(h);
    internHdrOf(s)->where = INTERN_IN_PROBATION;
}

/* Returns the probation slot holding `s`, or NULL if it was evicted. The slot is
 * found by the hash of `content`, which has the bytes of `s`. */
static probSlot *probFind(const_sds content, sds s) {
    probSlot *set = probSet(internHash(content, sdslen(content)));
    for (int w = 0; w < PROB_WAYS; w++) {
        if (set[w].s == s) return &set[w];
    }
    return NULL;
}

static void probRemove(sds s) {
    probSlot *slot = probFind(s, s);
    if (slot == NULL) return;
    slot->s = NULL;
    prob_count--;
    filterRemove(slot->hash);
}

static void probResize(size_t nsets) {
    probSlot *old = prob;
    size_t old_slots = prob_nsets * PROB_WAYS;
    prob = zcalloc(nsets * PROB_WAYS * sizeof(probSlot));
    prob_nsets = nsets;
    prob_count = 0;
    for (size_t i = 0; i < old_slots; i++) {
        if (old[i].s) {
            internHdrOf(old[i].s)->where = 0;
            filterRemove(old[i].hash);
            probInsert(old[i].hash, old[i].s);
        }
    }
    zfree(old);
}

/*-----------------------------------------------------------------------------
 * Sampling. At level L one call in 2^L is hashed, and a hashed value enters
 * probation when L bits of its salted hash are zero. Selecting by hash keeps
 * all sightings of a value in or out of the sample together during a window, so
 * duplicates are found at a low rate. The salt changes every window, so every
 * value is eventually selected. The level moves toward 0 while hashed writes
 * keep finding shared strings and back up when they do not.
 *----------------------------------------------------------------------------*/

/* Singleton selection has its own level. Creating a singleton costs a copy and
 * a probation slot, which only pays off when the value is seen again, so the
 * selection rate follows how many singletons are promoted, not the hit rate of
 * already shared strings. At selection level S a missed value becomes a
 * singleton when S bits of its salted hash are zero. */
#define INTERN_SEL_MAX 8

/* About one missed value per INTERN_SEL_FLOOR writes becomes a singleton
 * whatever its hash, so a value that starts repeating is found even while
 * selection has backed off. Its first repeat after that promotes it. A sampled
 * miss stands for intern_gate.period writes, so the countdown steps by that.
 * The floor only picks values whose shareable copy fits the allocation size
 * class of the private copy, so a singleton that never repeats costs no memory. */
#define INTERN_SEL_FLOOR 1024

static inline size_t allocClass(size_t size) {
#if defined(USE_JEMALLOC)
    return je_nallocx(size, 0);
#else
    return (size + 15) & ~(size_t)15;
#endif
}

static inline size_t shareableSize(size_t len) {
    char type = sdsReqType(len);
    if (type == SDS_TYPE_5) type = SDS_TYPE_8;
    return sizeof(internHdr) + sdsHdrSize(type) + len + 1;
}

/* True if a shareable copy of a len-byte string is no bigger than a private
 * allocation of priv_size bytes. */
static inline int shareableFits(size_t len, size_t priv_size) {
    return allocClass(shareableSize(len)) <= allocClass(priv_size);
}

/* The floor's turn: select the value if its shareable copy fits. */
__attribute__((cold, noinline)) static int selectByFloor(uint64_t h, size_t len, size_t priv_size) {
    sel_floor_tick = INTERN_SEL_FLOOR;
    if (shareableFits(len, priv_size)) return 1;
    return (((h ^ intern_salt) >> 56) & intern_sel_mask) == 0;
}

static inline int selectSingleton(uint64_t h, size_t len, size_t priv_size) {
    uint32_t step = intern_gate.period;
    if (unlikely(sel_floor_tick <= step)) return selectByFloor(h, len, priv_size);
    sel_floor_tick -= step;
    return (((h ^ intern_salt) >> 56) & intern_sel_mask) == 0;
}

static void setLevel(int level) {
    __atomic_store_n(&intern_level, level, __ATOMIC_RELAXED);
    intern_load_ready = (level == 0);
    intern_gate.period = 1u << level;
    if (intern_gate.tick > intern_gate.period) intern_gate.tick = intern_gate.period;
}

static void setSelLevel(int level) {
    intern_sel_level = level;
    intern_sel_mask = (1ull << level) - 1;
}

void internResetSampling(void) {
    /* The check tools load without internInit(); loading must stay plain. */
    if (intern_index == NULL) return;
    setLevel(INTERN_LEVEL_START);
    setSelLevel(INTERN_LEVEL_START);
    win_sel = win_benefit = win_evict = win_created = win_promoted = 0;
    sel_floor_tick = INTERN_SEL_FLOOR;
    intern_gate.tick = intern_gate.period;
}

/* Ends a sampling window: moves the levels and changes the salt. */
__attribute__((cold, noinline)) static void observeWindow(void) {
    if (win_benefit * 10 >= win_sel * 3) {
        if (intern_level > 0) setLevel(intern_level - 1);
        if (win_evict * 2 > win_sel && prob_nsets < PROB_MAX_SETS) probResize(prob_nsets * 2);
    } else if (win_benefit * 100 < win_sel * 3 && intern_level < INTERN_LEVEL_MAX) {
        setLevel(intern_level + 1);
    }
    /* Keep selecting while at least one singleton in four gets promoted; back
     * off when fewer than one in sixteen does. A floor of the sampling level
     * keeps the overall singleton rate at or below one in 2^(2L). */
    if (win_created >= INTERN_WINDOW / 4) {
        if (win_promoted * 4 >= win_created && intern_sel_level > intern_level) {
            setSelLevel(intern_sel_level - 1);
        } else if (win_promoted * 16 < win_created && intern_sel_level < INTERN_SEL_MAX) {
            setSelLevel(intern_sel_level + 1);
        }
        win_created = win_promoted = 0;
    }
    if (intern_sel_level < intern_level) setSelLevel(intern_level);
    win_sel = win_benefit = win_evict = 0;
    intern_salt = mix(++intern_epoch ^ intern_seed, K2);
}

static inline void observe(int benefit) {
    win_sel++;
    win_benefit += benefit;
    if (unlikely(win_sel >= INTERN_WINDOW)) observeWindow();
}

/*-----------------------------------------------------------------------------
 * Admission.
 *----------------------------------------------------------------------------*/

static sds newShareable(const char *buf, size_t len) {
    char type = sdsReqType(len);
    if (type == SDS_TYPE_5) type = SDS_TYPE_8;
    int hdrlen = sdsHdrSize(type);
    size_t size = sizeof(internHdr) + hdrlen + len + 1;
    char *p = zmalloc(size);
    internHdr *h = (internHdr *)p;
    h->refcount = 1;
    h->size = size;
    h->where = 0;
    sds s = sdswrite(p + sizeof(internHdr), hdrlen + len + 1, type, buf, len);
    s[-1] |= (char)(1 << (SDS_TYPE_BITS + SDS_AUX_SHARED));
    return s;
}

static inline sds shareIndexed(sds found, sds s) {
    internHdrOf(found)->refcount++;
    st.refs++;
    st.saved_bytes += internSize(found);
    st.hits++;
    if (s) sdsfree(s);
    observe(1);
    return found;
}

/* Allocation size of a private copy of a len-byte string made by sdsnewlen(). */
static inline size_t privateSize(size_t len) {
    return sdsHdrSize(sdsReqType(len)) + len + 1;
}

/* Promotes a probation singleton equal to buf[0..len) to the index, or returns
 * NULL. */
static sds promote(uint64_t h, const char *buf, size_t len) {
    if (!prob_count) return NULL;
    sds cand = probTake(h, buf, len);
    if (!cand) return NULL;
    internHdr *ch = internHdrOf(cand);
    ch->refcount++;
    memo_key = cand;
    memo_hash = h;
    int added = hashtableAdd(intern_index, cand);
    memo_key = NULL;
    /* An equal string already in the index has reached INTERN_REFS_MAX. */
    if (!added) return cand;
    filterAdd(h);
    ch->where = INTERN_IN_INDEX;
    size_t sz = internSize(cand);
    st.refs += ch->refcount;
    st.shared_bytes += sz;
    st.saved_bytes += (size_t)(ch->refcount - 1) * sz;
    st.promotions++;
    win_promoted++;
    hotStore(buf, len, cand);
    observe(1);
    return cand;
}

/* Looks buf[0..len) up in the index. */
static sds indexFind(const char *buf, size_t len, uint64_t h) {
    sds found = NULL;
    if (hashtableSize(intern_index) == 0) return NULL;
    probe_buf = buf;
    probe_len = len;
    memo_key = &probe_key;
    memo_hash = h;
    hashtableFind(intern_index, &probe_key, (void **)&found);
    memo_key = NULL;
    return found;
}

/* Looks `s` up in the index and probation, for a hash the filter may hold.
 * Returns the string the holder must store, or NULL if there is no match. */
__attribute__((cold, noinline)) static sds admitFromTables(sds s, size_t len, uint64_t h) {
    sds found = indexFind(s, len, h);
    if (found) {
        if (internHdrOf(found)->refcount >= INTERN_REFS_MAX) return s;
        hotStore(s, len, found);
        return shareIndexed(found, s);
    }
    if ((found = promote(h, s, len)) != NULL) {
        sdsfree(s);
        return found;
    }
    return NULL;
}

/* Replaces the private string `s` with a new probation singleton. */
__attribute__((cold, noinline)) static sds makeSingleton(sds s, size_t len, uint64_t h) {
    win_created++;
    sds sh = newShareable(s, len);
    sdsfree(s);
    probInsert(h, sh);
    return sh;
}

sds internAdmitSlow(sds s) {
    intern_gate.tick = intern_gate.period;
    if (intern_index == NULL) return s;
    size_t len = sdslen(s);
    if (len < INTERN_MIN_LEN || len > INTERN_MAX_LEN) return s;
    st.sampled++;

    sds c = hotFind(s, len);
    if (unlikely(c != NULL)) {
        if (internHdrOf(c)->refcount >= INTERN_REFS_MAX) return s;
        st.hot_hits++;
        return shareIndexed(c, s);
    }

    uint64_t h = internHash(s, len);
    if (unlikely(filterMayHave(h))) {
        sds r = admitFromTables(s, len, h);
        if (r) return r;
    }

    observe(0);
    if (unlikely(selectSingleton(h, len, sdsAllocSize(s)))) return makeSingleton(s, len, h);
    return s;
}

static sds internAdmitBufSlow(const char *buf, size_t len);

/* Scratch sds the loader reads candidate strings into. Its length is set to the
 * string being offered so it can be used as an index key. */
static sds load_scratch;
int intern_load_busy;

char *internLoadScratch(size_t len) {
    serverAssert(!intern_load_busy);
    if (load_scratch == NULL) load_scratch = sdsnewlen(SDS_NOINIT, INTERN_LOAD_MAX);
    sdssetlen(load_scratch, len);
    intern_load_busy = 1;
    return load_scratch;
}

sds internAdmitLoaded(size_t len) {
    serverAssert(intern_load_busy && load_scratch && sdslen(load_scratch) == len);
    intern_load_busy = 0;
    return internAdmitBufSlow(load_scratch, len);
}

void internLoadAbort(void) {
    intern_load_busy = 0;
}

sds internNewFrom(const char *buf, size_t len) {
    if (--intern_gate.tick || len < INTERN_MIN_LEN || len > INTERN_MAX_LEN) {
        if (intern_gate.tick == 0) intern_gate.tick = intern_gate.period;
        sds s = sdsnewlen(buf, len);
        intern_offered = s;
        return s;
    }
    intern_gate.tick = intern_gate.period;
    return internAdmitBufSlow(buf, len);
}

sds internDupSds(const_sds s) {
    if (!intern_is_main) return sdsdup(s);
    if (sdsIsShared(s)) {
        internHdr *h = internHdrOf(s);
        if (h->refcount < INTERN_REFS_MAX) {
            h->refcount++;
            if (h->where == INTERN_IN_INDEX) {
                st.refs++;
                st.saved_bytes += h->size;
            }
            return (sds)s;
        }
        return sdsdup(s);
    }
    sds d = internNewFrom(s, sdslen(s));
    /* Already offered; a later internAdmit() of it returns at once. */
    if (!sdsIsShared(d)) intern_offered = d;
    return d;
}

static sds internAdmitBufSlow(const char *buf, size_t len) {
    if (intern_index == NULL) return sdsnewlen(buf, len);
    st.sampled++;
    sds c = hotFind(buf, len);
    if (c && internHdrOf(c)->refcount < INTERN_REFS_MAX) {
        st.hot_hits++;
        return shareIndexed(c, NULL);
    }

    uint64_t h = internHash(buf, len);
    if (filterMayHave(h)) {
        sds found = indexFind(buf, len, h);
        if (found && internHdrOf(found)->refcount < INTERN_REFS_MAX) {
            hotStore(buf, len, found);
            return shareIndexed(found, NULL);
        }
        if ((found = promote(h, buf, len)) != NULL) return found;
    }

    observe(0);
    if (!selectSingleton(h, len, privateSize(len))) {
        sds s = sdsnewlen(buf, len);
        intern_offered = s;
        return s;
    }
    win_created++;
    sds sh = newShareable(buf, len);
    probInsert(h, sh);
    return sh;
}


/*-----------------------------------------------------------------------------
 * Release.
 *----------------------------------------------------------------------------*/

static uint64_t reads_inflight;
static void retire(sds s);

static void releaseOnMain(sds s, uint32_t n) {
    internHdr *h = internHdrOf(s);
    serverAssert(h->refcount >= n);
    if (h->where == INTERN_IN_INDEX) {
        size_t sz = internSize(s);
        uint32_t left = h->refcount - n;
        st.refs -= n;
        st.saved_bytes -= (size_t)(h->refcount - 1) * sz - (left ? (size_t)(left - 1) * sz : 0);
        if (left == 0) st.shared_bytes -= sz;
    }
    h->refcount -= n;
    if (h->refcount) return;
    if (h->where == INTERN_IN_INDEX) {
        hotForget(s);
        uint64_t hash = internHash(s, sdslen(s));
        memo_key = s;
        memo_hash = hash;
        serverAssert(hashtableDelete(intern_index, s));
        memo_key = NULL;
        filterRemove(hash);
        /* An IO thread may have read it from the hot cache. */
        if (reads_inflight) {
            retire(s);
            return;
        }
    } else if (h->where == INTERN_IN_PROBATION) {
        probRemove(s);
    } else if (h->where == INTERN_RETIRED) {
        return; /* freed by internEpochTick() */
    }
    zfree(h);
}

/* Releases made off the main thread are counted per string in a thread-local map
 * and published as one message. The main thread pops all messages and applies
 * them, so the reference count and the tables are only written on main. */
typedef struct deferEntry {
    sds s;
    uint32_t n;
} deferEntry;

typedef struct deferMsg {
    struct deferMsg *next;
    size_t count;
    deferEntry e[];
} deferMsg;

typedef struct deferMap {
    deferEntry *tab;
    size_t cap, used;
    int open;
} deferMap;

static _Atomic(deferMsg *) defer_head = NULL;
static __thread deferMap defer_map;

static inline size_t deferSlot(sds s, size_t mask) {
    uint64_t x = (uint64_t)(uintptr_t)s;
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdull;
    x ^= x >> 33;
    return x & mask;
}

static void deferPut(deferMap *m, sds s, uint32_t n) {
    size_t mask = m->cap - 1, i = deferSlot(s, mask);
    while (m->tab[i].s && m->tab[i].s != s) i = (i + 1) & mask;
    if (m->tab[i].s == NULL) {
        m->tab[i].s = s;
        m->used++;
    }
    m->tab[i].n += n;
}

static void deferGrow(deferMap *m) {
    deferMap nm = {zcalloc(m->cap * 2 * sizeof(deferEntry)), m->cap * 2, 0, m->open};
    for (size_t i = 0; i < m->cap; i++) {
        if (m->tab[i].s) deferPut(&nm, m->tab[i].s, m->tab[i].n);
    }
    zfree(m->tab);
    *m = nm;
}

static void deferPush(deferMsg *msg) {
    deferMsg *head = atomic_load_explicit(&defer_head, memory_order_relaxed);
    do {
        msg->next = head;
    } while (!atomic_compare_exchange_weak_explicit(&defer_head, &head, msg, memory_order_release,
                                                    memory_order_relaxed));
}

void internDeferBegin(void) {
    defer_map.open = 1;
}

/* Releases an IO thread made since its last flush, one entry per string. */
#define DEFER_RUN_MAX 32
typedef struct deferRun {
    deferEntry e[DEFER_RUN_MAX];
    int n;
    int coalesce;
} deferRun;

static __thread deferRun defer_run;

void internDeferCoalesce(void) {
    defer_run.coalesce = 1;
}

void internDeferFlush(void) {
    deferRun *r = &defer_run;
    if (r->n == 0) return;
    deferMsg *msg = zmalloc(sizeof(deferMsg) + r->n * sizeof(deferEntry));
    msg->count = r->n;
    memcpy(msg->e, r->e, r->n * sizeof(deferEntry));
    r->n = 0;
    deferPush(msg);
}

void internDeferPublish(void) {
    deferMap *m = &defer_map;
    m->open = 0;
    if (m->used == 0) return;
    deferMsg *msg = zmalloc(sizeof(deferMsg) + m->used * sizeof(deferEntry));
    msg->count = 0;
    for (size_t i = 0; i < m->cap; i++) {
        if (m->tab[i].s) msg->e[msg->count++] = m->tab[i];
    }
    zfree(m->tab);
    m->tab = NULL;
    m->cap = m->used = 0;
    deferPush(msg);
}

static void deferRecord(sds s) {
    deferMap *m = &defer_map;
    if (!m->open) {
        deferRun *r = &defer_run;
        if (r->coalesce) {
            for (int i = r->n - 1; i >= 0; i--) {
                if (r->e[i].s == s) {
                    r->e[i].n++;
                    return;
                }
            }
            if (r->n == DEFER_RUN_MAX) internDeferFlush();
            r->e[r->n].s = s;
            r->e[r->n].n = 1;
            r->n++;
            return;
        }
        deferMsg *msg = zmalloc(sizeof(deferMsg) + sizeof(deferEntry));
        msg->count = 1;
        msg->e[0].s = s;
        msg->e[0].n = 1;
        deferPush(msg);
        return;
    }
    if (m->tab == NULL) {
        m->cap = 1024;
        m->tab = zcalloc(m->cap * sizeof(deferEntry));
    } else if ((m->used + 1) * 4 > m->cap * 3) {
        deferGrow(m);
    }
    deferPut(m, s, 1);
}

/* Releases are applied a slice at a time, so a large asynchronous free does not
 * hold up the event loop. drain_msg is the message being applied, followed by
 * the rest of the list it came in; drain_pos is its next entry. */
#define INTERN_DRAIN_SLICE 1024
static deferMsg *drain_msg;
static size_t drain_pos;

size_t internDrainDeferred(void) {
    if (drain_msg == NULL) {
        if (atomic_load_explicit(&defer_head, memory_order_relaxed) == NULL) return 0;
        drain_msg = atomic_exchange_explicit(&defer_head, NULL, memory_order_acquire);
        drain_pos = 0;
    }
    size_t applied = 0, budget = INTERN_DRAIN_SLICE;
    while (drain_msg != NULL && budget > 0) {
        deferMsg *msg = drain_msg;
        for (; drain_pos < msg->count && budget > 0; drain_pos++, budget--) {
            releaseOnMain(msg->e[drain_pos].s, msg->e[drain_pos].n);
            applied += msg->e[drain_pos].n;
        }
        if (drain_pos == msg->count) {
            drain_msg = msg->next;
            drain_pos = 0;
            zfree(msg);
        }
    }
    st.deferred += applied;
    return applied;
}

int internDrainPending(void) {
    return drain_msg != NULL || atomic_load_explicit(&defer_head, memory_order_relaxed) != NULL;
}


/*-----------------------------------------------------------------------------
 * Parse-time sharing on IO threads. The IO thread reads the hot cache without
 * taking references and records each shared argument. The main thread takes
 * the references when it collects the read. A shared string that dies while
 * reads are in flight is retired: it stays allocated until every read
 * dispatched up to its death is collected, since one of those reads may still
 * be comparing it or holding it unreferenced.
 *----------------------------------------------------------------------------*/

#define INTERN_PENDING_MAX 4096

/* Arguments an IO thread selected for admission, with the hash and length it
 * computed, so the main thread can admit them without reading their bytes. */
typedef struct internSelected {
    robj *o;
    uint64_t hash;
    size_t len;
} internSelected;

struct internPending {
    uint32_t n, cap;   /* shared arguments waiting for their references */
    uint32_t ns, scap; /* arguments selected for admission */
    uint32_t hits;     /* sampled arguments that were shared */
    robj **o;
    internSelected *sel;
};

static internPending *pendingGet(internPending **pp) {
    if (*pp == NULL) *pp = zcalloc(sizeof(internPending));
    return *pp;
}

/* Makes room for one more entry in an array of `cap` entries holding `n`. */
static int pendingGrow(void **arr, uint32_t n, uint32_t *cap, size_t entry) {
    if (n < *cap) return 1;
    uint32_t ncap = *cap ? *cap * 2 : 16;
    if (ncap > INTERN_PENDING_MAX) return 0;
    *arr = zrealloc(*arr, ncap * entry);
    *cap = ncap;
    return 1;
}

/* Each IO thread selects one eligible argument in 2^level, the main sampling
 * rate, and hashes it, since the main thread does not have its bytes in cache. */
static __thread uint32_t io_sel_tick = 1;

static robj *parseArgIO(internPending **pp, const char *buf, size_t len, int argi) {
    if (len < INTERN_MIN_LEN || len > INTERN_MAX_LEN) return NULL;
    /* The command name and, usually, the key come first; values follow. */
    int sampled = 0;
    if (argi >= 2 && --io_sel_tick == 0) {
        io_sel_tick = 1u << __atomic_load_n(&intern_level, __ATOMIC_RELAXED);
        sampled = 1;
    }
    uint64_t m = hotMix(buf, len);
    hotSlotEntry *e = &hot[m >> (64 - HOT_BITS)];
    sds c = __atomic_load_n(&e->s, __ATOMIC_ACQUIRE);
    if (c && __atomic_load_n(&e->tag, __ATOMIC_RELAXED) == m && sdslen(c) == len && memcmp(c, buf, len) == 0) {
        internPending *p = pendingGet(pp);
        if (!pendingGrow((void **)&p->o, p->n, &p->cap, sizeof(robj *))) return NULL;
        robj *o = createObject(OBJ_STRING, c);
        p->o[p->n++] = o;
        /* A sampled hit, so the sampling level sees what parse-time sharing found. */
        p->hits += sampled;
        return o;
    }
    if (!sampled) return NULL;
    internPending *p = pendingGet(pp);
    if (!pendingGrow((void **)&p->sel, p->ns, &p->scap, sizeof(internSelected))) return NULL;
    robj *o = createStringObject(buf, len);
    p->sel[p->ns++] = (internSelected){o, internHash(buf, len), len};
    return o;
}

robj *internParseArg(internPending **pending, const char *buf, size_t len, int argi) {
    if (!intern_is_main) return parseArgIO(pending, buf, len, argi);
    if (len > INTERN_MAX_LEN) return NULL;
    sds c = hotFind(buf, len);
    if (!c) return NULL;
    internHdr *h = internHdrOf(c);
    if (h->refcount >= INTERN_REFS_MAX) return NULL;
    h->refcount++;
    st.refs++;
    st.saved_bytes += h->size;
    st.hits++;
    st.hot_hits++;
    return createObject(OBJ_STRING, c);
}

void internFreePending(internPending *pending) {
    if (pending == NULL) return;
    serverAssert(pending->n == 0 && pending->ns == 0);
    zfree(pending->o);
    zfree(pending->sel);
    zfree(pending);
}

/* True for commands that store a value argument itself rather than a copy of
 * it. Other commands copy their arguments, so sharing those on an IO thread
 * would only add reference work on the main thread. */
int internIOShares(const char *name, size_t len) {
    static const char *cmds[] = {"set", "setex", "psetex", "setnx", "getset", "mset", "msetnx"};
    if (len < 3 || len > 6) return 0;
    for (size_t i = 0; i < sizeof(cmds) / sizeof(cmds[0]); i++) {
        if (strlen(cmds[i]) == len && strncasecmp(cmds[i], name, len) == 0) return 1;
    }
    return 0;
}

/* Admits a private argument an IO thread selected, using the hash and length
 * it computed. Returns the sds the argument must hold. The bytes are read only
 * on a table hit or to copy a new singleton. */
static sds admitSelected(sds s, size_t len, uint64_t h) {
    st.sampled++;
    if (unlikely(filterMayHave(h))) {
        sds r = admitFromTables(s, len, h);
        if (r) return r;
    }
    observe(0);
    if (unlikely(selectSingleton(h, len, privateSize(len)))) return makeSingleton(s, len, h);
    return s;
}

/* Takes the reference an IO thread left for the main thread. The string may
 * have died since; it was retired, so it is still allocated, and this brings
 * it back as a shared string outside the index. */
static void takePendingRef(robj *o) {
    sds s = objectGetVal(o);
    internHdr *h = internHdrOf(s);
    if (h->refcount >= INTERN_REFS_MAX) {
        objectSetVal(o, sdsnewlen(s, sdslen(s)));
        return;
    }
    h->refcount++;
    if (h->where == INTERN_IN_INDEX) {
        st.refs++;
        st.saved_bytes += h->size;
    }
    st.hits++;
    st.hot_hits++;
}

/* Read jobs in flight, counted by the epoch they were dispatched in. The epoch
 * advances once per event loop iteration, but not onto a slot whose reads are
 * still in flight, so every in-flight read is within the last READ_EPOCHS. */
#define READ_EPOCHS 64
static uint32_t read_inflight[READ_EPOCHS];
static uint64_t read_epoch = 1, read_oldest = 1;

typedef struct retiredString {
    sds s;
    uint64_t epoch;
} retiredString;

static retiredString *retired;
static size_t retired_count, retired_cap;

void internReadDispatched(uint32_t *epoch) {
    uint32_t slot = read_epoch % READ_EPOCHS;
    read_inflight[slot]++;
    reads_inflight++;
    *epoch = slot + 1;
}

void internReadCollected(internPending *pending, uint32_t *epoch) {
    if (pending && pending->n) {
        for (uint32_t i = 0; i < pending->n; i++) takePendingRef(pending->o[i]);
        pending->n = 0;
        for (; pending->hits; pending->hits--) observe(1);
    }
    if (pending && pending->ns) {
        for (uint32_t i = 0; i < pending->ns; i++) {
            internSelected *e = &pending->sel[i];
            objectSetVal(e->o, admitSelected(objectGetVal(e->o), e->len, e->hash));
        }
        pending->ns = 0;
    }
    /* A long pipeline grows the arrays; do not keep them for an idle client. */
    if (pending && (pending->cap > 64 || pending->scap > 64)) {
        zfree(pending->o);
        zfree(pending->sel);
        pending->o = NULL;
        pending->sel = NULL;
        pending->cap = pending->scap = 0;
    }
    if (*epoch) {
        serverAssert(read_inflight[*epoch - 1] > 0);
        read_inflight[*epoch - 1]--;
        reads_inflight--;
        *epoch = 0;
    }
}

static void retire(sds s) {
    if (retired_count == retired_cap) {
        retired_cap = retired_cap ? retired_cap * 2 : 64;
        retired = zrealloc(retired, retired_cap * sizeof(retiredString));
    }
    internHdrOf(s)->where = INTERN_RETIRED;
    retired[retired_count++] = (retiredString){s, read_epoch};
}

void internEpochTick(void) {
    while (read_oldest < read_epoch && read_inflight[read_oldest % READ_EPOCHS] == 0) read_oldest++;
    if (read_inflight[(read_epoch + 1) % READ_EPOCHS] == 0) {
        read_epoch++;
        if (reads_inflight == 0) read_oldest = read_epoch;
    }
    if (retired_count == 0) return;
    size_t kept = 0;
    for (size_t i = 0; i < retired_count; i++) {
        retiredString r = retired[i];
        /* Reads dispatched at r.epoch or later may still hold it. */
        if (reads_inflight && r.epoch >= read_oldest) {
            retired[kept++] = r;
            continue;
        }
        internHdr *h = internHdrOf(r.s);
        if (h->refcount == 0) {
            zfree(h);
        } else {
            h->where = 0;
        }
    }
    retired_count = kept;
}

static void releaseHook(sds s) {
    /* A fork child frees nothing shared: its keyspace is a copy-on-write image
     * that other dumped keys may still reference. */
    serverAssert(!server.in_fork_child);
    if (intern_is_main) {
        releaseOnMain(s, 1);
    } else {
        deferRecord(s);
    }
}

/*-----------------------------------------------------------------------------
 * Defrag. A string with one holder can move: the caller updates the holder and
 * this updates the table slot. A string with more holders stays in place, since
 * the other holders cannot be found. A pending off-main release would make the
 * count higher than the holders, never lower, so a count of 1 is exact.
 *----------------------------------------------------------------------------*/

sds internDefragShared(sds s, void *(*defragfn)(void *)) {
    internHdr *h = internHdrOf(s);
    if (h->refcount != 1) return NULL;
    /* The retire list holds the address, and IO threads may be reading an
     * indexed string: moving it frees the old copy. */
    if (h->where == INTERN_RETIRED) return NULL;
    if (h->where == INTERN_IN_INDEX && server.io_threads_num > 1) return NULL;
    void *np = defragfn(h);
    if (np == NULL) return NULL;
    sds ns = (char *)np + ((char *)s - (char *)h);
    uint8_t where = ((internHdr *)np)->where;
    if (where == INTERN_IN_INDEX) {
        serverAssert(hashtableReplaceReallocatedEntry(intern_index, s, ns));
        size_t i = hotSlot(ns, sdslen(ns));
        if (hot[i].s == s) __atomic_store_n(&hot[i].s, ns, __ATOMIC_RELEASE);
    } else if (where == INTERN_IN_PROBATION) {
        /* The old copy is freed; hash the moved one. */
        probSlot *slot = probFind(ns, s);
        if (slot) slot->s = ns;
    }
    return ns;
}

/*-----------------------------------------------------------------------------
 * Init and introspection.
 *----------------------------------------------------------------------------*/

/* splitmix64 step, used to expand the seed into the hash secrets. */
static uint64_t nextSecret(uint64_t *x) {
    uint64_t z = (*x += 0x9e3779b97f4a7c15ull);
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ull;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebull;
    return (z ^ (z >> 31)) | 1;
}

void internInit(void) {
    uint8_t *seed = hashtableGetHashFunctionSeed();
    uint64_t x;
    memcpy(&x, seed, sizeof(x));
    K0 = nextSecret(&x);
    K1 = nextSecret(&x);
    K2 = nextSecret(&x);
    K3 = nextSecret(&x);
    memcpy(&intern_seed, seed + 8, sizeof(intern_seed));
    intern_salt = mix(intern_seed, K2);
    intern_index = hashtableCreate(&internIndexType);
    prob_nsets = PROB_MIN_SETS;
    prob = zcalloc(prob_nsets * PROB_WAYS * sizeof(probSlot));
    setLevel(INTERN_LEVEL_START);
    setSelLevel(INTERN_LEVEL_START);
    intern_gate.tick = intern_gate.period;
    sel_floor_tick = INTERN_SEL_FLOOR;
    intern_is_main = 1;
    sdsSharedReleaseHook = releaseHook;
}

size_t internSharedAllocSize(const_sds s) {
    return zmalloc_size(internHdrOf(s));
}

uint32_t internSharedRefcount(const_sds s) {
    return internHdrOf(s)->refcount;
}

size_t internSdsMemUsage(const_sds s) {
    if (!sdsIsShared(s)) return sdsAllocSize(s);
    internHdr *h = internHdrOf(s);
    return zmalloc_size(h) / (h->refcount ? h->refcount : 1);
}

void internGetInfo(internInfo *info) {
    info->shared = hashtableSize(intern_index);
    info->singletons = prob_count;
    info->refs = st.refs;
    info->shared_bytes = st.shared_bytes;
    info->saved_bytes = st.saved_bytes;
    info->table_bytes = hashtableMemUsage(intern_index) + prob_nsets * PROB_WAYS * sizeof(probSlot);
    info->hits = st.hits;
    info->hot_hits = st.hot_hits;
    info->promotions = st.promotions;
    info->sampled = st.sampled;
    info->deferred = st.deferred;
    info->level = intern_level;
    info->sel_level = intern_sel_level;
}
