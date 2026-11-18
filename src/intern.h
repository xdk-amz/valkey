#ifndef __INTERN_H
#define __INTERN_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include "config.h"
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

#define INTERN_HOT_SLOTS 4096

struct hashtable;
struct probSlot;

/* State that a sampled write reads, in one page-aligned object. Every write
 * touches the gate, so the page stays in the TLB for the writes that are
 * sampled. Fields after `offered` are private to intern.c. */
typedef struct internState {
    internGate gate;
    /* Non-zero while interning runs: string-interning-enabled is yes and the
     * tables exist. A hook in a command path tests it before anything else, so
     * with interning off the hook does no other work. */
    int enabled;
    /* Set once the tables are made. From then on shared strings may exist and
     * other threads may queue their releases, so the main thread keeps applying
     * those and advancing read epochs after interning is turned off. */
    int active;
    int config_enabled; /* string-interning-enabled */
    /* The private string internNewFrom(), internDupSds() or the loader last
     * returned. It was already offered, so internAdmit() of it returns at once. */
    const char *offered;

    uint64_t k0, k1, k2, k3; /* hash secrets */
    uint64_t seed, salt, epoch, sel_mask;
    struct hashtable *index;
    struct probSlot *prob;
    size_t prob_nsets, prob_count;
    uint32_t prob_victim, sel_floor_tick;
    int level, sel_level;
    uint32_t win_sel, win_benefit, win_evict, win_created, win_promoted;
    const char *memo_key, *probe_buf;
    uint64_t memo_hash;
    size_t probe_len;
    struct {
        size_t refs, shared_bytes, saved_bytes;
        size_t hits, hot_hits, promotions, sampled, deferred;
    } stats;
    uint64_t hot_used[INTERN_HOT_SLOTS / 64]; /* hot cache slots in use */
    uint32_t hot_count;                       /* hot cache slots in use; written on main only */
    uint64_t *filter;                         /* fingerprints of indexed and probation strings */
    uint64_t filter_mask;                     /* buckets in the filter, minus one */
} internState;

extern internState intern_state;
#define intern_gate (intern_state.gate)
#define intern_offered (intern_state.offered)

sds internAdmitSlow(sds s);

/* Hooks in hot command paths test one condition inline and leave the rest to a
 * function that is marked cold and never inlined, so the caller keeps the code
 * it has without interning, and the interning code is placed away from it. */
struct serverObject;

/* The work of internAdmit() while interning runs. */
sds internOffer(sds s);

/* Offers a freshly owned string that is about to be stored in the keyspace.
 * Takes ownership of `s`, which may be NULL, and returns the sds the holder
 * must store. The result is `s` itself or a shareable string that carries one
 * reference for the holder. Main thread only. */
static inline sds internAdmit(sds s) {
    if (unlikely(intern_state.enabled)) return internOffer(s);
    return s;
}

/* setKey() calls this after it stores a value, once the value has its final
 * encoding: it counts the write against the sampling gate and offers the value
 * when the write is due. Values stored through setKey are not modified in place
 * afterwards, unlike objects some commands create with dbAdd and then write
 * into. Main thread only, while interning runs. */
void internOfferSetKey(struct serverObject *val);

/* Offers the value of a key that RESTORE, COPY or a load just stored. Only an
 * unshared object is changed, so no other reference sees its value pointer
 * move. The caller must not modify the value in place afterwards. Main thread
 * only, while interning runs. */
void internOfferValue(struct serverObject *val);

/* The copy of `value` that a hash field stores by pointer. Main thread only,
 * while interning runs. */
sds internDupHashValue(sds value);

/* The member a hashtable-encoded set stores when it adds `str`: `sdsval` is
 * `str` itself, when it is an sds the caller keeps, or a copy the set takes
 * over. A member shared when the request was parsed is referenced. Main thread
 * only, while interning runs. */
sds internSetMember(sds sdsval, const char *str, size_t len);

/* The copy of a new member when adding it converts a set to a hashtable, or
 * when a set is converted. Main thread only, while interning runs. */
sds internNewMember(const char *str, size_t len, int str_is_sds);

/* Until this runs, admission returns its input and loading stays plain. The
 * check tools load data without it. With string-interning-enabled no it
 * allocates nothing, and interning stays off until internSetEnabled(1). */
void internInit(void);

/* Applies string-interning-enabled at runtime. Turning interning off stops all
 * new sharing at once; strings shared so far keep their holders and are
 * released as before. Turning it on starts sampling from the first write.
 * Main thread only, not while a dataset loads. */
void internSetEnabled(int enabled);
/* The apply function of string-interning-enabled. */
int internApplyConfig(const char **err);

/* Probes every write again. Called when the whole dataset is replaced, since
 * what was learned about the old data does not apply to the new. Main thread
 * only. */
void internResetSampling(void);

/* Called when a dataset load starts and ends. In between, every string is
 * offered and every repeated value is shared, whatever the sampling level would
 * be. Main thread only. */
void internLoadBegin(void);
void internLoadEnd(void);

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
/* Set while a loader holds the scratch buffer. Reading can process events, and
 * a command run there (RESTORE during an async load) must not reuse it. */
extern int intern_load_busy;
#define INTERN_MIN_LEN 16
/* The loader holds exclusive access to the keyspace (main thread, or a module
 * thread that holds the GIL), so no thread check is needed here. */
static inline int internLoadReady(size_t len) {
    return intern_load_ready && !intern_load_busy && len >= INTERN_MIN_LEN && len <= INTERN_LOAD_MAX;
}
char *internLoadScratch(size_t len);
sds internAdmitLoaded(size_t len);
/* Releases the scratch buffer when a read into it fails. */
void internLoadAbort(void);
/* The loader's test for a candidate string of `len` bytes, returned as an sds
 * when `sds` is non-zero and as a string object otherwise. */
int internLoadWants(size_t len, int sds);
/* Reads such a string from `rdb` into the scratch buffer and admits it. Returns
 * the sds or the string object, or NULL if the read fails. */
struct _rio;
void *internLoadString(struct _rio *rdb, size_t len, int sds, size_t *lenptr);
/* True if createStringObject() embeds a string of this length. */
bool stringObjectWouldEmbed(size_t len);

/* Returns a new sds holding buf[0..len) to store in the keyspace, sharing an
 * existing copy when there is one, like internAdmit(sdsnewlen(buf, len)) but
 * without allocating on a hit. Main thread only. */
sds internNewFrom(const char *buf, size_t len);

/* Copy of `s` for a new holder. A shared string gets one more reference. A
 * private string is copied through internNewFrom(), so it is offered while its
 * bytes are in cache. Main thread only. */
sds internDupSds(const_sds s);

/* Request parsing. A bulk argument equal to a hot shared string becomes a
 * string object that points at it, so the argument costs no allocation or copy.
 * Returns NULL otherwise. The argument must be long enough that it would not be
 * embedded; `argi` is its position in the command.
 *
 * On the main thread the object holds a reference. The main thread shares
 * arguments of the commands internIOShares() accepts, and of every command
 * except sorted set ones while every write is being probed. On an IO thread
 * the object is recorded in *pending without one; the main thread takes the
 * references when it collects the read with internReadCollected(). */
typedef struct internPending internPending;
struct serverObject *internParseArg(internPending **pending, const char *buf, size_t len, int argi);
extern __thread int intern_is_main;
/* Arguments are shared only from the hot cache, which stays empty while
 * interning is off, so the parser and the argv release test only this. */
static inline int internParseReady(void) {
    return __atomic_load_n(&intern_state.hot_count, __ATOMIC_RELAXED) != 0;
}
/* parseMultibulk() calls this for argument `argc`, the c->bulklen bytes at
 * c->querybuf + c->qb_pos, once internParseReady(): returns the argument as a
 * shared string object, or as a new string object when it is not shared. */
struct client;
struct serverObject *internParseBulk(struct client *c, struct serverObject **argv, int argc);
/* parseInlineBuffer() calls this for argv[j] once internParseReady(): returns
 * the argument object. It takes over argv[j], and frees it when the argument
 * is shared. */
struct serverObject *internParseInline(struct client *c, sds *argv, int j);
/* freeClientArgv() calls this once internParseReady(), before it frees the
 * arguments or hands them to an IO thread: it releases the shared strings that
 * arguments shared at parse time point at, and clears those arguments. Their
 * headers are in cache here, while an IO thread would have to send each
 * release back to the main thread. */
void internReleaseSharedArgs(int argc, struct serverObject **argv);
void internFreePending(internPending *pending);
/* Below the probing level, request parsing shares only arguments of these
 * commands: SET, SETEX, PSETEX, SETNX, GETSET, MSET, MSETNX, HSET, HSETNX,
 * HMSET and SADD, which store a value argument itself, or a reference to it
 * when it is shared. Other commands copy their arguments, so sharing those on
 * an IO thread would only add reference work on the main thread.
 *
 * It runs for every long argument, so it compares whole words: OR-ing 0x20
 * lower-cases a letter and maps no other byte onto one. */
#define INTERN_W4(a, b, c, d)                                                                                \
    ((uint32_t)(unsigned char)(a) | (uint32_t)(unsigned char)(b) << 8 | (uint32_t)(unsigned char)(c) << 16 | \
     (uint32_t)(unsigned char)(d) << 24)
static inline uint32_t internLower4(const char *p) {
    return INTERN_W4(p[0], p[1], p[2], p[3]) | 0x20202020u;
}
static inline int internIOShares(const char *name, size_t len) {
    if (len < 3 || len > 6) return 0;
    if (len == 3) return (INTERN_W4(name[0], name[1], name[2], 0) | 0x202020u) == INTERN_W4('s', 'e', 't', 0);
    uint32_t w = internLower4(name);
    /* Bitwise | and & keep the compares free of branches. */
    if (len == 4)
        return (w == INTERN_W4('s', 'a', 'd', 'd')) | (w == INTERN_W4('h', 's', 'e', 't')) |
               (w == INTERN_W4('m', 's', 'e', 't'));
    uint32_t t = internLower4(name + len - 4); /* last four bytes, overlapping w */
    if (len == 5)
        return ((w == INTERN_W4('s', 'e', 't', 'e')) & (t == INTERN_W4('e', 't', 'e', 'x'))) |
               ((w == INTERN_W4('s', 'e', 't', 'n')) & (t == INTERN_W4('e', 't', 'n', 'x'))) |
               ((w == INTERN_W4('h', 'm', 's', 'e')) & (t == INTERN_W4('m', 's', 'e', 't')));
    return ((w == INTERN_W4('p', 's', 'e', 't')) & (t == INTERN_W4('e', 't', 'e', 'x'))) |
           ((w == INTERN_W4('g', 'e', 't', 's')) & (t == INTERN_W4('t', 's', 'e', 't'))) |
           ((w == INTERN_W4('m', 's', 'e', 't')) & (t == INTERN_W4('e', 't', 'n', 'x'))) |
           ((w == INTERN_W4('h', 's', 'e', 't')) & (t == INTERN_W4('e', 't', 'n', 'x')));
}

/* Read jobs. An IO thread reads hot shared strings while it parses, and holds
 * unreferenced ones until the read is collected, so a shared string that dies
 * while reads are in flight is freed only once every read dispatched before
 * its death is collected. The main thread reports each read job it dispatches
 * and collects, and calls internEpochTick() once per event loop iteration. */
void internReadDispatched(uint32_t *epoch);
void internReadCollected(internPending *pending, uint32_t *epoch);
void internEpochTick(void);

/* beforeSleep() calls this once interning has been on: applies the releases
 * other threads queued and advances the read epochs. */
void internBeforeSleep(void);

/* Bio jobs that free keyspace data run between these two calls, so their
 * releases are coalesced into one message for the main thread. */
void internDeferBegin(void);
void internDeferPublish(void);

/* IO threads: consecutive releases of the same string are counted and sent as
 * one entry. internDeferFlush() sends what is pending; call it after each batch
 * of jobs and before waiting for work. */
void internDeferCoalesce(void);
extern __thread int intern_defer_held; /* releases recorded and not sent yet */
void internDeferFlushSlow(void);
static inline void internDeferFlush(void) {
    if (unlikely(intern_defer_held)) internDeferFlushSlow();
}

/* Applies releases queued by other threads, at most a fixed number of strings
 * per call. internDrainPending() is non-zero while some are left, so the caller
 * can poll again without waiting for events. Main thread only. */
size_t internDrainDeferred(void);
int internDrainPending(void);

/* Moves a shareable string with a single holder using `defragfn`, keeping the
 * table consistent. Returns the new sds, or NULL if it did not move. The caller
 * updates the holder. */
sds internDefragShared(sds s, void *(*defragfn)(void *));

/* True if `o` is a raw-encoded string object that points at a shared string. */
int internObjectIsShared(const struct serverObject *o);

/* Gives a raw-encoded string object that points at a shared string a private
 * copy instead, so it can be modified in place. */
void internUnshareStringObject(struct serverObject *o);

/* Allocation size of a shareable string, including its prefix. */
size_t internSharedAllocSize(const_sds s);
uint32_t internSharedRefcount(const_sds s);

/* Memory attributed to one holder of `s`: the full allocation for a private
 * string, an equal share of the allocation for a shared one. */
size_t internSdsMemUsage(const_sds s);

typedef struct internInfo {
    size_t shared;       /* strings in the index */
    size_t singletons;   /* strings in probation */
    size_t refs;         /* references held on indexed strings */
    size_t shared_bytes; /* allocation bytes of indexed strings */
    size_t saved_bytes;  /* bytes not allocated thanks to sharing */
    size_t table_bytes;  /* index and probation table memory */
    size_t hits;         /* writes that took a reference on an indexed string */
    size_t hot_hits;     /* hits found through the hot cache */
    size_t promotions;   /* singletons that gained a second holder */
    size_t sampled;      /* writes that were hashed */
    size_t deferred;     /* releases applied from other threads */
    int level;           /* sampling level, 0 = every write */
    int sel_level;       /* singleton selection level, 0 = every missed value */
} internInfo;

void internGetInfo(internInfo *info);
/* Appends the intern_* fields of the INFO memory section to `info`. */
sds internCatInfo(sds info);

#endif
