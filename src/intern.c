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
    uint32_t where : 8; /* INTERN_IN_INDEX, INTERN_IN_PROBATION or 0 */
} internHdr;

#define INTERN_IN_INDEX 1
#define INTERN_IN_PROBATION 2

#define INTERN_MAX_LEN (512 * 1024)
#define INTERN_HASH_FULL_MAX 512

#define INTERN_LEVEL_MAX 4
#define INTERN_LEVEL_START 2
#define INTERN_WINDOW 64

#define PROB_WAYS 4
#define PROB_MIN_SETS 256
#define PROB_MAX_SETS 16384

internGate intern_gate = {1, 1};
int intern_load_ready = 0;

static uint64_t intern_seed;
static hashtable *intern_index;

typedef struct probSlot {
    uint64_t hash;
    sds s;
} probSlot;

static probSlot *prob;
static size_t prob_nsets;
static size_t prob_count;
static uint32_t prob_victim;

static int intern_level;
static uint64_t intern_sel_mask;
static uint32_t win_sel, win_benefit, win_evict;

static struct {
    size_t refs, shared_bytes, saved_bytes;
    size_t hits, hot_hits, promotions, sampled, deferred;
} st;

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

static uint64_t K0, K1, K2, K3;

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
 * value instead of hashing again. Set right before an index call, cleared after. */
static const char *memo_key;
static uint64_t memo_hash;

/* A lookup of plain bytes passes &probe_key as the key; the bytes are in
 * probe_buf and probe_len. */
static const char probe_key = 0;
static const char *probe_buf;
static size_t probe_len;

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
static sds hot[1 << HOT_BITS];

static inline size_t hotSlot(const char *p, size_t len) {
    return mix(rd64((const unsigned char *)p) ^ K1 ^ len, rd64((const unsigned char *)p + len - 8) ^ K2) >>
           (64 - HOT_BITS);
}

static inline void hotForget(sds s) {
    size_t i = hotSlot(s, sdslen(s));
    if (hot[i] == s) hot[i] = NULL;
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
        w = prob_victim++ & (PROB_WAYS - 1);
        internHdrOf(set[w].s)->where = 0;
        prob_count--;
        win_evict++;
    }
    set[w].hash = h;
    set[w].s = s;
    prob_count++;
    internHdrOf(s)->where = INTERN_IN_PROBATION;
}

static void probRemove(sds s) {
    probSlot *set = probSet(internHash(s, sdslen(s)));
    for (int w = 0; w < PROB_WAYS; w++) {
        if (set[w].s == s) {
            set[w].s = NULL;
            prob_count--;
            return;
        }
    }
    serverPanic("intern: probation entry not found");
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

static uint64_t intern_salt;
static uint64_t intern_epoch;

/* Singleton selection has its own level. Creating a singleton costs a copy and
 * a probation slot, which only pays off when the value is seen again, so the
 * selection rate follows how many singletons are promoted, not the hit rate of
 * already shared strings. At selection level S a missed value becomes a
 * singleton when S bits of its salted hash are zero. */
#define INTERN_SEL_MAX 8
static int intern_sel_level;
static uint32_t win_created, win_promoted;

static void setLevel(int level) {
    intern_level = level;
    intern_load_ready = (level == 0);
    intern_gate.period = 1u << level;
    if (intern_gate.tick > intern_gate.period) intern_gate.tick = intern_gate.period;
}

static void setSelLevel(int level) {
    intern_sel_level = level;
    intern_sel_mask = (1ull << level) - 1;
}

static void observe(int benefit) {
    win_sel++;
    win_benefit += benefit;
    if (win_sel < INTERN_WINDOW) return;
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

/* The string internAdmitLoaded() last returned as a private copy. It was already
 * offered, so internAdmit() of the same pointer returns at once. A stale value
 * after that string is freed can only make one later call return early. */
static const char *offered_last;

/* Promotes a probation singleton equal to buf[0..len) to the index, or returns
 * NULL. */
static sds promote(uint64_t h, const char *buf, size_t len, size_t hi) {
    if (!prob_count) return NULL;
    sds cand = probTake(h, buf, len);
    if (!cand) return NULL;
    internHdr *ch = internHdrOf(cand);
    ch->where = INTERN_IN_INDEX;
    ch->refcount++;
    memo_key = cand;
    memo_hash = h;
    serverAssert(hashtableAdd(intern_index, cand));
    memo_key = NULL;
    size_t sz = internSize(cand);
    st.refs += ch->refcount;
    st.shared_bytes += sz;
    st.saved_bytes += (size_t)(ch->refcount - 1) * sz;
    st.promotions++;
    win_promoted++;
    hot[hi] = cand;
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

sds internAdmitSlow(sds s) {
    intern_gate.tick = intern_gate.period;
    if (s == offered_last) {
        offered_last = NULL;
        return s;
    }
    if (sdsIsShared(s)) return s;
    size_t len = sdslen(s);
    if (len < INTERN_MIN_LEN || len > INTERN_MAX_LEN) return s;
    st.sampled++;

    size_t hi = hotSlot(s, len);
    sds c = hot[hi];
    if (c && sdslen(c) == len && memcmp(c, s, len) == 0) {
        st.hot_hits++;
        return shareIndexed(c, s);
    }

    uint64_t h = internHash(s, len);
    sds found = indexFind(s, len, h);
    if (found) {
        hot[hi] = found;
        return shareIndexed(found, s);
    }
    if ((found = promote(h, s, len, hi)) != NULL) {
        sdsfree(s);
        return found;
    }

    observe(0);
    if ((((h ^ intern_salt) >> 56) & intern_sel_mask) != 0) return s;
    win_created++;
    sds sh = newShareable(s, len);
    sdsfree(s);
    probInsert(h, sh);
    return sh;
}

static sds internAdmitBufSlow(const char *buf, size_t len);

/* Scratch sds the loader reads candidate strings into. Its length is set to the
 * string being offered so it can be used as an index key. */
static sds load_scratch;

char *internLoadScratch(size_t len) {
    if (load_scratch == NULL) load_scratch = sdsnewlen(SDS_NOINIT, INTERN_LOAD_MAX);
    sdssetlen(load_scratch, len);
    return load_scratch;
}

sds internAdmitLoaded(size_t len) {
    serverAssert(load_scratch && sdslen(load_scratch) == len);
    return internAdmitBufSlow(load_scratch, len);
}

sds internNewFrom(const char *buf, size_t len) {
    if (--intern_gate.tick || len < INTERN_MIN_LEN || len > INTERN_MAX_LEN) {
        if (intern_gate.tick == 0) intern_gate.tick = intern_gate.period;
        return sdsnewlen(buf, len);
    }
    intern_gate.tick = intern_gate.period;
    return internAdmitBufSlow(buf, len);
}

static sds internAdmitBufSlow(const char *buf, size_t len) {
    st.sampled++;
    size_t hi = hotSlot(buf, len);
    sds c = hot[hi];
    if (c && sdslen(c) == len && memcmp(c, buf, len) == 0) {
        st.hot_hits++;
        return shareIndexed(c, NULL);
    }

    uint64_t h = internHash(buf, len);
    sds found = indexFind(buf, len, h);
    if (found) {
        hot[hi] = found;
        return shareIndexed(found, NULL);
    }
    if ((found = promote(h, buf, len, hi)) != NULL) return found;

    observe(0);
    if ((((h ^ intern_salt) >> 56) & intern_sel_mask) != 0) {
        sds s = sdsnewlen(buf, len);
        offered_last = s;
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
        serverAssert(hashtableDelete(intern_index, s));
    } else if (h->where == INTERN_IN_PROBATION) {
        probRemove(s);
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

size_t internDrainDeferred(void) {
    if (atomic_load_explicit(&defer_head, memory_order_relaxed) == NULL) return 0;
    deferMsg *msg = atomic_exchange_explicit(&defer_head, NULL, memory_order_acquire);
    size_t applied = 0;
    while (msg) {
        deferMsg *next = msg->next;
        for (size_t i = 0; i < msg->count; i++) {
            releaseOnMain(msg->e[i].s, msg->e[i].n);
            applied += msg->e[i].n;
        }
        zfree(msg);
        msg = next;
    }
    st.deferred += applied;
    return applied;
}

/* Set on the main thread at init; a forked child inherits it for its only thread. */
static __thread int intern_is_main;

robj *internParseArg(const char *buf, size_t len) {
    if (!intern_is_main || len > INTERN_MAX_LEN) return NULL;
    sds c = hot[hotSlot(buf, len)];
    if (!c || sdslen(c) != len || memcmp(c, buf, len) != 0) return NULL;
    internHdr *h = internHdrOf(c);
    h->refcount++;
    st.refs++;
    st.saved_bytes += h->size;
    st.hits++;
    st.hot_hits++;
    return createObject(OBJ_STRING, c);
}

static void releaseHook(sds s) {
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
    void *np = defragfn(h);
    if (np == NULL) return NULL;
    sds ns = (char *)np + ((char *)s - (char *)h);
    uint8_t where = ((internHdr *)np)->where;
    if (where == INTERN_IN_INDEX) {
        serverAssert(hashtableReplaceReallocatedEntry(intern_index, s, ns));
        size_t i = hotSlot(ns, sdslen(ns));
        if (hot[i] == s) hot[i] = ns;
    } else if (where == INTERN_IN_PROBATION) {
        probSlot *set = probSet(internHash(ns, sdslen(ns)));
        int w;
        for (w = 0; w < PROB_WAYS; w++) {
            if (set[w].s == s) break;
        }
        serverAssert(w < PROB_WAYS);
        set[w].s = ns;
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
