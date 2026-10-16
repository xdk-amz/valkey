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

#include <limits.h>
#include <stdatomic.h>

/* One work unit is a keyspace table shared by a db's three kvstores at the same index. */
typedef struct {
    int dbid;
    int didx;
} maintUnit;

/* An expired key's value a lane unlinked; main replays its delete after the phase. */
typedef struct {
    robj *val;
    int dbid;
    int didx;
} maintExpired;

typedef struct {
    maintExpired *e;
    int len;
    int cap;
} maintLog;

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
    l->e[l->len++] = (maintExpired){.val = val, .dbid = dbid, .didx = didx};
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

int maintParallelReady(void) {
    return server.cron_lanes && server.keyspace_shard_bits > 0 && stagedApplyReady() && stagedWorkerCount() > 0;
}

/* Main, after the phase: the effects of every delete the lanes made, thread by thread in log order. */
static void maintReplay(void) {
    monotime t0 = getMonotonicUs();
    long replayed = 0;
    for (int tid = 0; tid < IO_THREADS_MAX_NUM; tid++) {
        maintLog *l = &mt_logs[tid];
        for (int i = 0; i < l->len; i++) {
            maintExpired *e = &l->e[i];
            replayExpiredKeyUnlinkedInLane(server.db[e->dbid], e->val, e->didx);
        }
        replayed += l->len;
        l->len = 0;
    }
    mt_replayed = replayed;
    mt_replay_us = replayed ? getMonotonicUs() - t0 : 0;
    if (replayed) cronStatsRecord(CRON_JOB_MAINT_REPLAY, mt_replay_us);
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
        mt_budget_us[MAINT_JOB_EXPIRE] = maintExpireThreadBudget(mt_budget_us[MAINT_JOB_EXPIRE], mt_nlanes);
        activeExpireLaneBegin();
    }

    monotime phase_t0 = getMonotonicUs();
    kvstoreSetConcurrent(1);
    if (parallel) {
        stagedRunLanePhase(STAGED_PHASE_MAINT, mt_nlanes > 1 || !stagedMainTakesLane());
    } else {
        maintLane(0);
    }
    kvstoreSetConcurrent(0);
    uint64_t lanes_us = getMonotonicUs() - phase_t0;

    monotime rec_t0 = getMonotonicUs();
    for (int dbid = 0; dbid < server.dbnum; dbid++) {
        serverDb *db = server.db[dbid];
        if (!db) continue;
        kvstoreReconcile(db->keys);
        kvstoreReconcile(db->expires);
        kvstoreReconcile(db->keys_with_volatile_items);
    }
    cronStatsRecord(CRON_JOB_SC_RECONCILE, getMonotonicUs() - rec_t0);

    maintReplay();
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
