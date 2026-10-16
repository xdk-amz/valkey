/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Parallel keyspace maintenance. See maint.h. */

#include "server.h"
#include "maint.h"
#include "staged_loop.h"
#include "cronstats.h"
#include "kvstore.h"
#include "io_threads.h"

#include <limits.h>
#include <stdatomic.h>

/* One work unit is a keyspace table shared by a db's three kvstores at the same index. */
typedef struct {
    int dbid;
    int didx;
} maintUnit;

/* An expired key's value a lane unlinked; main replays its delete after the phase. In propagate-only mode
 * free_only entries are freed by main without replay (their delete already went to the propagate buffer);
 * the lane keeps them because the value was too expensive to free in the phase. */
typedef struct {
    robj *val;
    int dbid;
    int didx;
    int free_only;
} maintExpired;

typedef struct {
    maintExpired *e;
    int len;
    int cap;
} maintLog;

/* One expired key whose delete main propagates after the phase (propagate-only mode): the lane copied
 * the key name here and freed the value itself, so main feeds DEL/UNLINK to the AOF and replicas without
 * touching the keyspace. Kept in the lane's expiry order so the stream matches the serial path. */
typedef struct {
    sds key;  /* owned copy; main frees it after feeding */
    int dbid;
    int lazy; /* UNLINK when set, DEL otherwise */
} maintPropagate;

typedef struct {
    maintPropagate *e;
    int len;
    int cap;
} maintPropLog;

/* Pending requests for the current iteration. */
static int mt_requested[MAINT_JOB_COUNT];
static uint64_t mt_budget_us[MAINT_JOB_COUNT];

/* Unit list and the atomic claim cursor, built on main before the phase opens. */
static maintUnit *mt_units = NULL;
static int mt_units_len = 0;
static int mt_units_cap = 0;
static int mt_start = 0; /* index in mt_units the claim order starts at */
static _Atomic int mt_cursor = 0;
/* First claimed unit, in claim order, where a timed job ran out of budget; the next phase starts there
 * so units cut off by a budget come first. Kept as a unit key (dbid * tables + didx) across phases. */
static _Atomic int mt_first_cut = INT_MAX;
static long mt_resume_key = 0;
static int mt_nlanes = 0;
static mstime_t mt_now = 0;
/* Monotonic time by which this phase's timed jobs end; 0 when only untimed jobs run. */
static _Atomic uint64_t mt_phase_deadline = 0;

uint64_t maintPhaseDeadline(void) {
    return atomic_load_explicit(&mt_phase_deadline, memory_order_relaxed);
}

/* EXPIRE cost model, per expired key: lane time summed across threads, and main's replay time. The
 * replay is serial and grows with the number of lanes, so each thread's budget is set for the whole
 * phase, lanes plus replay, to fit the cycle's budget. Moving averages over phases that expired keys. */
static double mt_expire_lane_us = 1.0;
static double mt_expire_replay_us = 0.5;
static long mt_replayed = 0;
static uint64_t mt_replay_us = 0;

/* Per-thread, per-job accounting merged on main after the phase. */
static uint64_t mt_lane_usec[IO_THREADS_MAX_NUM][MAINT_JOB_COUNT];
static uint64_t mt_lane_work[IO_THREADS_MAX_NUM][MAINT_JOB_COUNT]; /* units where the job did work */
static maintLog mt_logs[IO_THREADS_MAX_NUM];
static maintPropLog mt_prop_logs[IO_THREADS_MAX_NUM];

/* EXPIRE propagate-only append cost on main, per key: feeding one DEL/UNLINK to the AOF and replicas.
 * Measured on phases that fed, moving-averaged. The shared budget uses this instead of the full replay
 * cost when the only observer is propagation. */
static double mt_expire_feed_us = 0.3;
static long mt_fed = 0;
static uint64_t mt_feed_us = 0;

void maintRequest(int job, uint64_t budget_us) {
    if (job < 0 || job >= MAINT_JOB_COUNT) return;
    mt_requested[job] = 1;
    if (budget_us > mt_budget_us[job]) mt_budget_us[job] = budget_us;
}

int maintPending(void) {
    for (int j = 0; j < MAINT_JOB_COUNT; j++)
        if (mt_requested[j]) return 1;
    return 0;
}

void maintReset(void) {
    for (int j = 0; j < MAINT_JOB_COUNT; j++) {
        mt_requested[j] = 0;
        mt_budget_us[j] = 0;
    }
    mt_units_len = 0;
}

void maintLogExpired(int tid, robj *val, int dbid, int didx) {
    maintLog *l = &mt_logs[tid];
    if (l->len == l->cap) {
        l->cap = l->cap ? l->cap * 2 : 1024;
        l->e = zrealloc(l->e, sizeof(maintExpired) * l->cap);
    }
    l->e[l->len++] = (maintExpired){.val = val, .dbid = dbid, .didx = didx, .free_only = 0};
}

/* Propagate-only mode: the key's delete already went to the propagate buffer, but its value was too
 * expensive to free in the phase, so main frees it after the phase without replaying any effects. */
void maintLogExpiredFreeOnly(int tid, robj *val, int dbid, int didx) {
    maintLog *l = &mt_logs[tid];
    if (l->len == l->cap) {
        l->cap = l->cap ? l->cap * 2 : 1024;
        l->e = zrealloc(l->e, sizeof(maintExpired) * l->cap);
    }
    l->e[l->len++] = (maintExpired){.val = val, .dbid = dbid, .didx = didx, .free_only = 1};
}

/* Propagate-only mode: record an expired key whose DEL/UNLINK main feeds to the AOF and replicas after
 * the phase. The lane owns and passes a key-name copy; main frees it once fed. */
void maintLogPropagate(int tid, sds key_copy, int dbid, int lazy) {
    maintPropLog *l = &mt_prop_logs[tid];
    if (l->len == l->cap) {
        l->cap = l->cap ? l->cap * 2 : 1024;
        l->e = zrealloc(l->e, sizeof(maintPropagate) * l->cap);
    }
    l->e[l->len++] = (maintPropagate){.key = key_copy, .dbid = dbid, .lazy = lazy};
}

/* Return the db's kvstore for slot k (0 keys, 1 expires, 2 keys_with_volatile_items). */
static kvstore *maintKvstore(serverDb *db, int k) {
    switch (k) {
    case 0: return db->keys;
    case 1: return db->expires;
    default: return db->keys_with_volatile_items;
    }
}

static long maintUnitKey(const maintUnit *u) {
    return (long)u->dbid * kvstoreNumHashtables(server.db[u->dbid]->keys) + u->didx;
}

static void maintNoteCut(int claim_index) {
    int cur = atomic_load_explicit(&mt_first_cut, memory_order_relaxed);
    while (claim_index < cur &&
           !atomic_compare_exchange_weak_explicit(&mt_first_cut, &cur, claim_index, memory_order_relaxed,
                                                  memory_order_relaxed));
}

/* Run every requested job on one unit as thread tid, honoring tid's remaining per-job budget. */
static void maintRunUnit(int tid, int claim_index, const maintUnit *u, uint64_t remain_us[MAINT_JOB_COUNT]) {
    serverDb *db = server.db[u->dbid];

    if (mt_requested[MAINT_JOB_EXPIRE] && remain_us[MAINT_JOB_EXPIRE] > 0) {
        int hit_limit = 0;
        uint64_t spent =
            activeExpireTableInLane(tid, db, u->didx, mt_now, remain_us[MAINT_JOB_EXPIRE], &hit_limit);
        mt_lane_usec[tid][MAINT_JOB_EXPIRE] += spent;
        if (spent > 0) mt_lane_work[tid][MAINT_JOB_EXPIRE]++;
        remain_us[MAINT_JOB_EXPIRE] = spent >= remain_us[MAINT_JOB_EXPIRE] ? 0 : remain_us[MAINT_JOB_EXPIRE] - spent;
        if (hit_limit) maintNoteCut(claim_index);
    }

    for (int k = 0; k < 3; k++) {
        kvstore *kvs = maintKvstore(db, k);
        if (u->didx >= kvstoreNumHashtables(kvs)) continue;

        if (mt_requested[MAINT_JOB_RESIZE]) {
            monotime t = getMonotonicUs();
            unsigned long before = kvstoreHashtableBuckets(kvs, u->didx);
            int rehashing_before = kvstoreHashtableRehashingAt(kvs, u->didx);
            kvstoreTryResizeHashtable(kvs, u->didx);
            mt_lane_usec[tid][MAINT_JOB_RESIZE] += getMonotonicUs() - t;
            /* A rightsize either changes the bucket count outright or starts a shrink/grow rehash. */
            if (kvstoreHashtableBuckets(kvs, u->didx) != before ||
                (!rehashing_before && kvstoreHashtableRehashingAt(kvs, u->didx)))
                mt_lane_work[tid][MAINT_JOB_RESIZE]++;
        }
        if (mt_requested[MAINT_JOB_REHASH] && remain_us[MAINT_JOB_REHASH] > 0) {
            uint64_t spent = kvstoreRehashHashtable(kvs, u->didx, remain_us[MAINT_JOB_REHASH]);
            mt_lane_usec[tid][MAINT_JOB_REHASH] += spent;
            if (spent > 0) {
                mt_lane_work[tid][MAINT_JOB_REHASH]++;
                remain_us[MAINT_JOB_REHASH] = spent >= remain_us[MAINT_JOB_REHASH] ? 0 : remain_us[MAINT_JOB_REHASH] - spent;
                if (remain_us[MAINT_JOB_REHASH] == 0 && kvstoreHashtableRehashingAt(kvs, u->didx))
                    maintNoteCut(claim_index);
            }
        }
    }
}

/* Lane body for one thread: claim units until the list is drained, or until every requested job is a
 * timed one and the thread has spent all of their budgets. */
static void maintLane(int tid) {
    uint64_t remain_us[MAINT_JOB_COUNT];
    for (int j = 0; j < MAINT_JOB_COUNT; j++) remain_us[j] = mt_budget_us[j];
    for (;;) {
        int i = atomic_fetch_add_explicit(&mt_cursor, 1, memory_order_relaxed);
        if (i >= mt_units_len) break;
        maintRunUnit(tid, i, &mt_units[(mt_start + i) % mt_units_len], remain_us);
        if (mt_requested[MAINT_JOB_RESIZE]) continue;
        int left = 0;
        for (int j = 0; j < MAINT_JOB_COUNT; j++)
            if (mt_requested[j] && remain_us[j] > 0) left = 1;
        if (!left) break;
    }
}

/* Main: build the unit list for the pending jobs across every db's tables. */
static void maintBuildUnits(void) {
    mt_units_len = 0;
    mt_start = 0;
    for (int dbid = 0; dbid < server.dbnum; dbid++) {
        serverDb *db = server.db[dbid];
        if (!db) continue;
        int n = kvstoreNumHashtables(db->keys);
        for (int didx = 0; didx < n; didx++) {
            /* A cluster node allocates tables only for the slots it holds. */
            if (!kvstoreGetHashtable(db->keys, didx) && !kvstoreGetHashtable(db->expires, didx) &&
                !kvstoreGetHashtable(db->keys_with_volatile_items, didx))
                continue;
            int due = mt_requested[MAINT_JOB_RESIZE];
            if (!due && mt_requested[MAINT_JOB_REHASH])
                due = kvstoreHashtableRehashingAt(db->keys, didx) || kvstoreHashtableRehashingAt(db->expires, didx) ||
                      kvstoreHashtableRehashingAt(db->keys_with_volatile_items, didx);
            if (!due && mt_requested[MAINT_JOB_EXPIRE]) due = kvstoreHashtableSize(db->expires, didx) > 0;
            if (!due) continue;
            if (mt_units_len == mt_units_cap) {
                mt_units_cap = mt_units_cap ? mt_units_cap * 2 : 256;
                mt_units = zrealloc(mt_units, sizeof(maintUnit) * mt_units_cap);
            }
            mt_units[mt_units_len] = (maintUnit){.dbid = dbid, .didx = didx};
            if (maintUnitKey(&mt_units[mt_units_len]) < mt_resume_key) mt_start = mt_units_len + 1;
            mt_units_len++;
        }
    }
    if (mt_start >= mt_units_len) mt_start = 0;
}

int maintKeyspaceSplit(void) {
    return server.cluster_enabled || server.keyspace_shard_bits > 0;
}

int maintParallelReady(void) {
    return server.cron_lanes && maintKeyspaceSplit() && stagedApplyReady() && stagedWorkerCount() > 0;
}

/* Main, after the phase: the effects of every delete the lanes made, thread by thread in log order. A
 * free_only entry (propagate-only mode, expensive value) is just freed; its delete was already fed. */
static void maintReplay(void) {
    monotime t0 = getMonotonicUs();
    long replayed = 0;
    for (int tid = 0; tid < IO_THREADS_MAX_NUM; tid++) {
        maintLog *l = &mt_logs[tid];
        for (int i = 0; i < l->len; i++) {
            maintExpired *e = &l->e[i];
            if (e->free_only) {
                freeValueNeverOnMain(NULL, e->val, e->dbid);
            } else {
                replayExpiredKeyUnlinkedInLane(server.db[e->dbid], e->val, e->didx);
                replayed++;
            }
        }
        l->len = 0;
    }
    mt_replayed = replayed;
    mt_replay_us = replayed ? getMonotonicUs() - t0 : 0;
    if (replayed) cronStatsRecord(CRON_JOB_MAINT_REPLAY, mt_replay_us);
}

/* Main, after the phase (propagate-only mode): feed the DEL/UNLINK of every key the lanes logged to the
 * AOF and the replication stream, in the lanes' expiry order, with the exact framing propagateNow
 * produces -- feedAppendOnlyFile and replicationFeedReplicas each emit their own SELECT and account for
 * offsets, one command per key, no MULTI. Bulk-adds stat_expiredkeys and dirty. */
static void maintFeedPropagation(void) {
    monotime t0 = getMonotonicUs();
    long fed = 0;
    /* Mirror propagateNow's target logic for PROPAGATE_AOF | PROPAGATE_REPL (slot migration is excluded
     * by maintExpirePropagateOnly): AOF when enabled; replicas when AOF forces it (WAITAOF) or a replica
     * or backlog is present. */
    int propagate_to_aof = server.aof_state != AOF_OFF;
    int propagate_to_repl = 1;
    if (!propagate_to_aof)
        propagate_to_repl = server.primary_host == NULL && (server.repl_backlog || listLength(server.replicas) != 0);
    for (int tid = 0; tid < IO_THREADS_MAX_NUM; tid++) {
        maintPropLog *l = &mt_prop_logs[tid];
        for (int i = 0; i < l->len; i++) {
            maintPropagate *p = &l->e[i];
            robj *keyobj = createStringObject(p->key, sdslen(p->key));
            robj *argv[2] = {p->lazy ? shared.unlink : shared.del, keyobj};
            if (propagate_to_aof) feedAppendOnlyFile(p->dbid, argv, 2);
            if (propagate_to_repl) replicationFeedReplicas(p->dbid, argv, 2);
            decrRefCount(keyobj);
            sdsfree(p->key);
            fed++;
        }
        l->len = 0;
    }
    if (fed) {
        server.stat_expiredkeys += fed;
        server.dirty += fed;
    }
    mt_fed = fed;
    mt_feed_us = fed ? getMonotonicUs() - t0 : 0;
    if (fed) cronStatsRecord(CRON_JOB_MAINT_FEED, mt_feed_us);
}

/* Main: each thread's share of the EXPIRE budget. Threads running for t each expire about
 * n * t / lane_us keys, which main then replays at replay_us each, so t = budget / (1 + n * replay_us /
 * lane_us) keeps lanes plus replay within the budget. */
static uint64_t maintExpireThreadBudget(uint64_t budget_us, int nlanes) {
    if (nlanes <= 1) return budget_us;
    if (server.cron_lanes_split_budget) return budget_us / nlanes;
    double t = budget_us / (1.0 + nlanes * mt_expire_replay_us / mt_expire_lane_us);
    double floor = (double)budget_us / (nlanes + 1);
    if (t < floor) t = floor;
    return t < 1 ? 1 : (uint64_t)t;
}

static void maintExpireUpdateCosts(uint64_t lane_us) {
    if (mt_fed >= 256) {
        double feed = (double)mt_feed_us / mt_fed;
        mt_expire_feed_us = 0.8 * mt_expire_feed_us + 0.2 * feed;
    }
    if (mt_replayed < 256) return;
    double lane = (double)lane_us / mt_replayed, replay = (double)mt_replay_us / mt_replayed;
    mt_expire_lane_us = 0.8 * mt_expire_lane_us + 0.2 * lane;
    mt_expire_replay_us = 0.8 * mt_expire_replay_us + 0.2 * replay;
}

void maintRun(void) {
    if (!maintPending()) return;
    /* The EXPIRE gate is rechecked: serverCron can start a forkless save after databasesCron posted. */
    if (mt_requested[MAINT_JOB_EXPIRE] && !activeExpireInLanes()) {
        mt_requested[MAINT_JOB_EXPIRE] = 0;
        mt_budget_us[MAINT_JOB_EXPIRE] = 0;
        if (!maintPending()) return;
    }

    maintBuildUnits();
    if (mt_units_len == 0) {
        maintReset();
        return;
    }

    for (int t = 0; t < IO_THREADS_MAX_NUM; t++)
        for (int j = 0; j < MAINT_JOB_COUNT; j++) mt_lane_usec[t][j] = mt_lane_work[t][j] = 0;
    atomic_store_explicit(&mt_cursor, 0, memory_order_relaxed);
    atomic_store_explicit(&mt_first_cut, INT_MAX, memory_order_relaxed);
    mt_now = server.mstime;

    int parallel = maintParallelReady();
    mt_nlanes = parallel ? stagedWorkerCount() + stagedMainTakesLane() : 1;
    if (mt_requested[MAINT_JOB_EXPIRE]) {
        /* Decide once, before the exclusive phase, whether expired keys need main's per-key replay or a
         * lane can free the value itself. The state it reads is global or db-wide and cannot change in
         * the phase. */
        int replay_needed = maintExpireReplayNeeded();
        int propagate_only = replay_needed && maintExpirePropagateOnly();
        activeExpireSetFreeInline(!replay_needed);
        activeExpireSetPropagateOnly(propagate_only);
        uint64_t phase_budget = mt_budget_us[MAINT_JOB_EXPIRE];
        /* The shared bound charges the per-key cost main pays after the phase: a full replay normally, or
         * just the propagation feed in propagate-only mode. In free-inline mode nothing is charged. */
        double per_key_us = propagate_only ? mt_expire_feed_us : mt_expire_replay_us;
        if (server.cron_lanes_split_budget) {
            /* Override: each thread gets a plain slice of the budget, replay bound off. */
            mt_budget_us[MAINT_JOB_EXPIRE] = maintExpireThreadBudget(phase_budget, mt_nlanes);
            activeExpireBudgetBegin(0, 0.0);
        } else {
            /* Shared replay-bounded budget: lanes get the whole-phase budget as a per-thread safety
             * ceiling and stop on the shared bound (elapsed + logged keys * per-key cost). */
            activeExpireBudgetBegin(phase_budget, per_key_us);
        }
        activeExpireLaneBegin();
    }

    monotime phase_t0 = getMonotonicUs();
    uint64_t longest = 0;
    for (int j = 0; j < MAINT_JOB_COUNT; j++)
        if (mt_requested[j] && mt_budget_us[j] > longest) longest = mt_budget_us[j];
    atomic_store_explicit(&mt_phase_deadline, longest ? phase_t0 + longest : 0, memory_order_relaxed);
    dbLanePhaseBegin();
    if (parallel) {
        stagedRunLanePhase(STAGED_PHASE_MAINT, mt_nlanes > 1 || !stagedMainTakesLane());
    } else {
        maintLane(0);
    }
    uint64_t lanes_us = getMonotonicUs() - phase_t0;

    monotime rec_t0 = getMonotonicUs();
    dbLanePhaseEnd();
    cronStatsRecord(CRON_JOB_SC_RECONCILE, getMonotonicUs() - rec_t0);

    maintReplay();
    maintFeedPropagation();
    cronStatsRecord(CRON_JOB_SC_MAINT, getMonotonicUs() - phase_t0);

    /* Per-job lane time summed across threads, recorded only when a job actually did work so a
     * due-free iteration leaves the counters still. */
    uint64_t usec[MAINT_JOB_COUNT] = {0}, work[MAINT_JOB_COUNT] = {0};
    for (int t = 0; t < IO_THREADS_MAX_NUM; t++) {
        for (int j = 0; j < MAINT_JOB_COUNT; j++) {
            usec[j] += mt_lane_usec[t][j];
            work[j] += mt_lane_work[t][j];
        }
    }
    if (work[MAINT_JOB_REHASH]) cronStatsRecord(CRON_JOB_MAINT_REHASH, usec[MAINT_JOB_REHASH]);
    if (work[MAINT_JOB_RESIZE]) cronStatsRecord(CRON_JOB_MAINT_RESIZE, usec[MAINT_JOB_RESIZE]);
    if (mt_requested[MAINT_JOB_EXPIRE]) {
        if (work[MAINT_JOB_EXPIRE]) cronStatsRecord(CRON_JOB_MAINT_EXPIRE, usec[MAINT_JOB_EXPIRE]);
        maintExpireUpdateCosts(usec[MAINT_JOB_EXPIRE]);
        activeExpireLaneEnd(lanes_us, getMonotonicUs() - phase_t0, usec[MAINT_JOB_EXPIRE]);
    }

    /* Start the next phase at the first unit a budget cut off. */
    int cut = atomic_load_explicit(&mt_first_cut, memory_order_relaxed);
    if (cut < mt_units_len) mt_resume_key = maintUnitKey(&mt_units[(mt_start + cut) % mt_units_len]);

    maintReset();
}

void maintInit(void) {
    stagedSetLaneRunner(STAGED_PHASE_MAINT, maintLane);
}
