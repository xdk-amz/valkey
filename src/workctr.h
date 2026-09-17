/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Test-only deterministic work counters.
 *
 * Built only with `make WORK_COUNTERS=yes` (-DWORK_COUNTERS). Without the flag
 * every WC_* macro expands to nothing, so production builds carry no counter
 * branches, atomics or allocations. The counters are plain 64-bit integers
 * updated from the main thread; they are not thread safe and are meant for
 * single-threaded test servers (io-threads 1).
 *
 * Tests drive them through DEBUG WORKCTR (see debug.c):
 *   DEBUG WORKCTR ARM       measure the next top-level command
 *   DEBUG WORKCTR GET       last measured snapshot as a flat name/value array
 *   DEBUG WORKCTR RESET     zero the live counters
 *   DEBUG WORKCTR CURRENT   live counters (for multi-command windows)
 *   DEBUG WORKCTR SEED <n>  reseed rand()/random()/genrand64 deterministically
 *   DEBUG WORKCTR SETINFO <key>  encoding / physical size / volatile count
 *
 * Forked children (AOF rewrite, RDB save) dump their own counters to a file
 * in the server's working directory (wcDumpToFile). */

#ifndef WORKCTR_H
#define WORKCTR_H

#ifdef WORK_COUNTERS

#include <stdint.h>
#include <stddef.h>

/* X-macro list: name, description. Keep grouped; the order is the report order. */
#define WC_COUNTERS(X)                                                                                             \
    /* ---- hashtable lookup / selection ---- */                                                                   \
    X(ht_lookups, "findBucket calls: logical find/pop/add/find-position requests")                                 \
    X(ht_bucket_probes, "buckets examined during findBucket (top-level + chained)")                                \
    X(ht_key_compares, "keyCompare invocations")                                                                   \
    X(ht_hash_calls, "hash function invocations")                                                                  \
    X(ht_hash_bytes, "bytes fed to the sds hash functions")                                                        \
    X(ht_validate_calls, "validateEntry callback invocations (only volatile types have one)")                      \
    X(ht_validate_rejected, "validateEntry returned false (expired entry hidden)")                                 \
    X(ht_insert_positions, "findBucketForInsert calls")                                                            \
    X(ht_inserts, "entries inserted")                                                                              \
    X(ht_pops, "entries popped/deleted")                                                                           \
    X(ht_iter_visits, "filled positions examined by hashtableNext (incl. rejected)")                               \
    X(ht_iter_rejected, "hashtableNext entries hidden by validateEntry")                                           \
    X(ht_iter_buckets, "bucket steps taken by hashtableNext")                                                      \
    X(ht_scan_calls, "hashtableScanDefrag calls")                                                                  \
    X(ht_scan_buckets, "buckets visited by scan (top-level + chained)")                                            \
    X(ht_scan_visits, "filled positions examined by scan (incl. rejected)")                                        \
    X(ht_scan_rejected, "scan entries hidden by validateEntry")                                                    \
    X(ht_random_calls, "hashtableRandomEntry/FairRandomEntry/SampleEntries calls")                                 \
    X(ht_random_scans, "scan invocations issued by the random samplers")                                           \
    X(ht_rehash_steps, "incremental rehash steps")                                                                 \
    X(ht_rehash_entries, "entries moved by rehashing (each also costs one hash + one insert position)")           \
    X(ht_resizes, "table resizes")                                                                                 \
    /* ---- listpack ---- */                                                                                       \
    X(lp_find_calls, "lpFind calls")                                                                               \
    X(lp_find_steps, "entries stepped over by lpFind (incl. metadata)")                                            \
    X(lp_next_steps, "lpNext/lpPrev/lpSkip steps taken by callers")                                                \
    X(lp_random_calls, "lpNextRandom/lpRandomEntries/lpRandomPair(s) calls")                                       \
    X(lp_random_steps, "entries stepped over by the listpack random pickers")                                      \
    X(lp_inserts, "entry insertions/replacements")                                                                 \
    X(lp_deletes, "single-entry / range deletions")                                                                \
    X(lp_batch_deletes, "lpBatchDelete calls")                                                                     \
    X(lp_tail_bytes_moved, "bytes memmoved inside listpacks by insert/delete/batch-delete")                        \
    X(lp_reallocs, "listpack reallocations")                                                                       \
    X(lp_realloc_bytes, "requested sizes of listpack reallocations (sum)")                                         \
    X(lp_blob_bytes_copied, "bytes copied by whole-listpack duplication/merge (lpDup, lpMerge)")                 \
    /* ---- expiry index (vset) ---- */                                                                            \
    X(vset_adds, "vsetAddEntry")                                                                                   \
    X(vset_removes, "vsetRemoveEntry")                                                                             \
    X(vset_updates, "vsetUpdateEntry")                                                                             \
    X(vset_expire_calls, "vsetRemoveExpired calls")                                                                \
    X(vset_bucket_visits, "vset buckets visited (expire scan, iteration, mem usage, defrag)")                      \
    X(vset_entry_visits, "vset entries visited (expire scan, iteration)")                                          \
    /* ---- set type layer ---- */                                                                                 \
    X(set_random_calls, "setTypeRandomElement calls")                                                              \
    X(set_random_expired_seen, "random picks rejected because the member was expired")                             \
    X(set_reservoir_passes, "full-iteration reservoir passes (random live element, SPOP/SRANDMEMBER volatile)")    \
    X(set_iter_next, "setTypeNext returning a member")                                                             \
    X(list_iter_next, "listTypeNext returning an entry")                                                            \
    X(hash_iter_next, "hashTypeNext returning a field")                                                             \
    X(set_lp_skipped_expired, "listpack members skipped by setTypeNext as expired")                                \
    X(set_members_reclaimed, "expired members physically removed (active/lazy reclaim, replace)")                  \
    /* ---- memory (zmalloc) ---- */                                                                               \
    X(mem_allocs, "zmalloc/zcalloc calls")                                                                         \
    X(mem_alloc_bytes, "requested bytes of zmalloc/zcalloc (sum)")                                                 \
    X(mem_reallocs, "zrealloc calls")                                                                              \
    X(mem_realloc_bytes, "requested new sizes of zrealloc (sum)")                                                  \
    X(mem_frees, "zfree calls")                                                                                    \
    X(mem_max_alloc, "largest single allocation/reallocation requested")                                           \
    X(mem_live_delta, "usable bytes allocated minus freed since reset (int64, app class)")                         \
    X(mem_peak_live_delta, "maximum of mem_live_delta since reset (app class)")                                    \
    X(reply_allocs, "client reply buffer blocks allocated")                                                        \
    X(reply_alloc_bytes, "requested bytes of reply buffer blocks")                                                 \
    /* ---- copying ---- */                                                                                        \
    X(sds_copies, "sdsnewlen with a source buffer")                                                                \
    X(sds_copy_bytes, "payload bytes copied by sdsnewlen")                                                         \
    X(str_objs_created, "string robj created (createStringObject / FromLongLong / FromSds)")                       \
    X(str_obj_bytes, "payload bytes of string robj created")                                                       \
    X(smember_created, "smemberCreate calls")                                                                      \
    X(smember_bytes, "payload bytes copied by smemberCreate")                                                      \
    /* ---- propagation ---- */                                                                                    \
    X(prop_cmds, "alsoPropagate calls that queued a command")                                                      \
    X(prop_cmds_dropped, "alsoPropagate calls dropped (no consumer)")                                              \
    X(prop_dropped_args, "argv entries of dropped alsoPropagate calls")                                             \
    X(prop_dropped_arg_bytes, "payload bytes of dropped alsoPropagate calls")                                       \
    X(prop_args, "argv entries queued by alsoPropagate")                                                           \
    X(prop_arg_bytes, "payload bytes represented by queued argv")                                                  \
    X(prop_retained_bytes, "payload bytes currently retained in also_propagate")                                   \
    X(prop_peak_retained_bytes, "peak payload bytes retained in also_propagate")                                   \
    X(prop_now_cmds, "propagateNow calls (commands actually fed to AOF/replication)")                              \
    X(repl_bytes, "bytes fed to the replication backlog/buffer")                                                   \
    X(aof_bytes, "bytes fed to the AOF buffer")                                                                    \
    X(aof_rewrite_cmds, "commands written by the AOF rewriter")                                                    \
    X(aof_rewrite_bytes, "bytes written by the AOF rewriter")                                                      \
    X(rdb_members_saved, "set members written by rdbSaveObject")                                                   \
    X(rdb_members_loaded, "set members read by rdbLoadObject")

typedef struct workCounters {
#define WC_FIELD(name, desc) int64_t name;
    WC_COUNTERS(WC_FIELD)
#undef WC_FIELD
} workCounters;

extern workCounters wc;       /* the measured object's own work */
extern workCounters wc_index; /* work inside an expiry-index (vset) bracket, see WC_INDEX_BEGIN */
extern int wc_index_depth;
extern int wc_alloc_class; /* WC_CLASS_APP or WC_CLASS_REPLY */
extern int wc_armed;       /* DEBUG WORKCTR ARM was issued */
extern uint64_t wc_armed_client; /* id of the client that issued ARM */
extern int wc_measuring;   /* a top-level call() is being measured */

#define WC_CLASS_APP 0
#define WC_CLASS_REPLY 1
#define WC_CLASS_IGNORE 2 /* server bookkeeping allocated lazily on a command's first call */

/* Inside WC_INDEX_BEGIN()/WC_INDEX_END() every counter lands in wc_index, so the
 * set's hashtable/memory work stays separable from the expiry index's internal
 * hashtables and allocations. */
#define WC_TARGET() (wc_index_depth > 0 ? &wc_index : &wc)
#define WC_INDEX_BEGIN() ((void)(wc_index_depth++))
#define WC_INDEX_END() ((void)(wc_index_depth--))
/* For a callback that does object work while an index bracket is open. */
#define WC_INDEX_SUSPEND()                    \
    int wc_saved_depth_ = wc_index_depth; \
    wc_index_depth = 0
#define WC_INDEX_RESUME() ((void)(wc_index_depth = wc_saved_depth_))
#define WC_INC(f) ((void)(WC_TARGET()->f++))
#define WC_ADD(f, n) ((void)(WC_TARGET()->f += (int64_t)(n)))
#define WC_SUB(f, n) ((void)(WC_TARGET()->f -= (int64_t)(n)))
#define WC_MAX(f, v)                                              \
    do {                                                          \
        int64_t wc_v_ = (int64_t)(v);                             \
        if (wc_v_ > WC_TARGET()->f) WC_TARGET()->f = wc_v_;                         \
    } while (0)
#define WC_SET_ALLOC_CLASS(cls) ((void)(wc_alloc_class = (cls)))

/* Called from zmalloc: usable-size accounting for live/peak tracking. */
static inline void wcMemAlloc(size_t requested, size_t usable) {
    workCounters *t = WC_TARGET();
    if (wc_alloc_class == WC_CLASS_IGNORE) return;
    if (wc_alloc_class == WC_CLASS_REPLY) {
        t->reply_allocs++;
        t->reply_alloc_bytes += (int64_t)requested;
        return;
    }
    t->mem_allocs++;
    t->mem_alloc_bytes += (int64_t)requested;
    if ((int64_t)requested > t->mem_max_alloc) t->mem_max_alloc = (int64_t)requested;
    t->mem_live_delta += (int64_t)usable;
    if (t->mem_live_delta > t->mem_peak_live_delta) t->mem_peak_live_delta = t->mem_live_delta;
}

static inline void wcMemRealloc(size_t requested, size_t old_usable, size_t new_usable) {
    workCounters *t = WC_TARGET();
    if (wc_alloc_class == WC_CLASS_IGNORE) return;
    if (wc_alloc_class == WC_CLASS_REPLY) {
        t->reply_allocs++;
        t->reply_alloc_bytes += (int64_t)requested;
        return;
    }
    t->mem_reallocs++;
    t->mem_realloc_bytes += (int64_t)requested;
    if ((int64_t)requested > t->mem_max_alloc) t->mem_max_alloc = (int64_t)requested;
    t->mem_live_delta += (int64_t)new_usable - (int64_t)old_usable;
    if (t->mem_live_delta > t->mem_peak_live_delta) t->mem_peak_live_delta = t->mem_live_delta;
}

/* A free is booked to the class current at free time: reply blocks released
 * while a command runs (rare) and ignored-class blocks are not distinguishable. */
static inline void wcMemFree(size_t usable) {
    workCounters *t = WC_TARGET();
    if (wc_alloc_class != WC_CLASS_APP) return;
    t->mem_frees++;
    t->mem_live_delta -= (int64_t)usable;
}

void wcReset(void);
void wcSnapshot(void);          /* copy live counters into the last-measured snapshot */
const workCounters *wcLast(void);
const workCounters *wcIndexLast(void);
uint64_t wcGeneration(void);
int wcDumpToFile(const char *path); /* "name value\n" lines; returns 0 on success */

#else /* !WORK_COUNTERS */

#define WC_INC(f) ((void)0)
#define WC_ADD(f, n) ((void)0)
#define WC_SUB(f, n) ((void)0)
#define WC_MAX(f, v) ((void)0)
#define WC_SET_ALLOC_CLASS(cls) ((void)0)
#define WC_INDEX_BEGIN() ((void)0)
#define WC_INDEX_END() ((void)0)
#define WC_INDEX_SUSPEND() ((void)0)
#define WC_INDEX_RESUME() ((void)0)

#endif /* WORK_COUNTERS */

#endif /* WORKCTR_H */
