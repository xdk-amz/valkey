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
    uint8_t where; /* INTERN_IN_INDEX, INTERN_IN_PROBATION or 0 */
    uint8_t unused[3];
} internHdr;

#define INTERN_IN_INDEX 1
#define INTERN_IN_PROBATION 2

#define INTERN_MIN_LEN 16
#define INTERN_MAX_LEN (512 * 1024)
#define INTERN_HASH_FULL_MAX 512

#define INTERN_LEVEL_MAX 4
#define INTERN_LEVEL_START 2
#define INTERN_WINDOW 64

#define PROB_WAYS 4
#define PROB_MIN_SETS 256
#define PROB_MAX_SETS 16384

internGate intern_gate = {1, 1};

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
    size_t hits, promotions, sampled, deferred;
} st;

static inline internHdr *internHdrOf(const_sds s) {
    return (internHdr *)((char *)s - sdsHdrSize(sdsType(s)) - sizeof(internHdr));
}

static inline size_t internSize(const_sds s) {
    return sizeof(internHdr) + sdsHdrSize(sdsType(s)) + sdslen(s) + 1;
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

static uint64_t indexHash(const void *key) {
    if (key == memo_key) return memo_hash;
    const_sds s = key;
    return internHash(s, sdslen(s));
}

/* Returns non-zero when the keys are equal, as the hashtable expects. */
static int indexCompare(const void *a, const void *b) {
    size_t la = sdslen(a);
    return la == sdslen(b) && memcmp(a, b, la) == 0;
}

static hashtableType internIndexType = {
    .hashFunction = indexHash,
    .keyCompare = indexCompare,
};

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

static void setLevel(int level) {
    intern_level = level;
    intern_gate.period = 1u << level;
    intern_sel_mask = (1ull << level) - 1;
    if (intern_gate.tick > intern_gate.period) intern_gate.tick = intern_gate.period;
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
    char *p = zmalloc(sizeof(internHdr) + hdrlen + len + 1);
    internHdr *h = (internHdr *)p;
    h->refcount = 1;
    h->where = 0;
    sds s = sdswrite(p + sizeof(internHdr), hdrlen + len + 1, type, buf, len);
    s[-1] |= (char)(1 << (SDS_TYPE_BITS + SDS_AUX_SHARED));
    return s;
}

sds internAdmitSlow(sds s) {
    intern_gate.tick = intern_gate.period;
    if (sdsIsShared(s)) return s;
    size_t len = sdslen(s);
    if (len < INTERN_MIN_LEN || len > INTERN_MAX_LEN) return s;

    uint64_t h = internHash(s, len);
    st.sampled++;

    sds found = NULL;
    if (hashtableSize(intern_index)) {
        memo_key = s;
        memo_hash = h;
        hashtableFind(intern_index, s, (void **)&found);
        memo_key = NULL;
    }
    if (found) {
        internHdrOf(found)->refcount++;
        st.refs++;
        st.saved_bytes += internSize(found);
        st.hits++;
        sdsfree(s);
        observe(1);
        return found;
    }

    if (prob_count) {
        sds cand = probTake(h, s, len);
        if (cand) {
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
            sdsfree(s);
            observe(1);
            return cand;
        }
    }

    observe(0);
    if ((((h ^ intern_salt) >> 56) & intern_sel_mask) != 0) return s;
    sds sh = newShareable(s, len);
    sdsfree(s);
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

static void releaseHook(sds s) {
    if (onServerMainThread()) {
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
    intern_gate.tick = intern_gate.period;
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
    info->promotions = st.promotions;
    info->sampled = st.sampled;
    info->deferred = st.deferred;
    info->level = intern_level;
}
