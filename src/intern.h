#ifndef __INTERN_H
#define __INTERN_H

#include <stddef.h>
#include <stdint.h>
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
#define INTERN_FILTER_BUCKETS 256

struct hashtable;
struct probSlot;

/* State that a sampled write reads, in one page-aligned object. Every write
 * touches the gate, so the page stays in the TLB for the writes that are
 * sampled. Fields after `offered` are private to intern.c. */
typedef struct internState {
    internGate gate;
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
    uint64_t filter[INTERN_FILTER_BUCKETS];   /* fingerprints of indexed and probation strings */
} internState;

extern internState intern_state;
#define intern_gate (intern_state.gate)
#define intern_offered (intern_state.offered)

sds internAdmitSlow(sds s);

/* Offers a freshly owned string that is about to be stored in the keyspace.
 * Takes ownership of `s` and returns the sds the holder must store. The result
 * is `s` itself or a shareable string that carries one reference for the
 * holder. Main thread only. */
static inline sds internAdmit(sds s) {
    if (s == intern_offered) {
        intern_offered = NULL;
        return s;
    }
    if (sdsIsShared(s)) return s;
    if (--intern_gate.tick) return s;
    return internAdmitSlow(s);
}

/* Counts one write for a caller that checks the gate before looking at its
 * value. Returns non-zero for one write in every sampling period; the caller
 * then offers that write's value with internAdmitDue(), or skips it if the
 * value is not eligible. */
static inline int internGateDue(void) {
    if (--intern_gate.tick) return 0;
    intern_gate.tick = intern_gate.period;
    return 1;
}

/* Offers the value of a write that internGateDue() selected. Same contract as
 * internAdmit(). */
static inline sds internAdmitDue(sds s) {
    if (s == intern_offered) {
        intern_offered = NULL;
        return s;
    }
    if (sdsIsShared(s)) return s;
    return internAdmitSlow(s);
}

/* An IO thread selects eligible arguments for admission at the main sampling
 * rate, but at most one in INTERN_IO_MIN_PERIOD, and hashes them, since the
 * main thread does not have their bytes in cache. See internParseArg(). */
#define INTERN_IO_MIN_PERIOD 8

/* Until this runs, admission returns its input and loading stays plain. The
 * check tools load data without it. */
void internInit(void);

/* Probes every write again. Called when the whole dataset is replaced, since
 * what was learned about the old data does not apply to the new. Main thread
 * only. */
void internResetSampling(void);

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
 * On the main thread, which parses only while every write is being probed, the
 * object holds a reference. On an IO thread the object is recorded in *pending
 * without one, and an argument after the key may be selected, hashed and
 * recorded instead; the main thread takes the references and admits the
 * selected arguments when it collects the read with internReadCollected(). */
struct serverObject;
typedef struct internPending internPending;
struct serverObject *internParseArg(internPending **pending, const char *buf, size_t len, int argi);
extern __thread int intern_is_main;
static inline int internParseReady(void) {
    return intern_is_main ? intern_load_ready : 1;
}
void internFreePending(internPending *pending);
/* IO threads share and select only arguments of these commands. */
int internIOShares(const char *name, size_t len);

/* Read jobs. An IO thread reads hot shared strings while it parses, and holds
 * unreferenced ones until the read is collected, so a shared string that dies
 * while reads are in flight is freed only once every read dispatched before
 * its death is collected. The main thread reports each read job it dispatches
 * and collects, and calls internEpochTick() once per event loop iteration. */
void internReadDispatched(uint32_t *epoch);
void internReadCollected(internPending *pending, uint32_t *epoch);
void internEpochTick(void);

/* Bio jobs that free keyspace data run between these two calls, so their
 * releases are coalesced into one message for the main thread. */
void internDeferBegin(void);
void internDeferPublish(void);

/* IO threads: consecutive releases of the same string are counted and sent as
 * one entry. internDeferFlush() sends what is pending; call it after each batch
 * of jobs and before waiting for work. */
void internDeferCoalesce(void);
void internDeferFlush(void);

/* Applies releases queued by other threads. Main thread only. */
size_t internDrainDeferred(void);

/* Moves a shareable string with a single holder using `defragfn`, keeping the
 * table consistent. Returns the new sds, or NULL if it did not move. The caller
 * updates the holder. */
sds internDefragShared(sds s, void *(*defragfn)(void *));

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

#endif
