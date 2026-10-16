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

#include <stdatomic.h>

/* One work unit is a keyspace table shared by a db's three kvstores at the same index. */
typedef struct {
    int dbid;
    int didx;
} maintUnit;

/* Pending requests for the current iteration. */
static int mt_requested[MAINT_JOB_COUNT];
static uint64_t mt_budget_us[MAINT_JOB_COUNT];

/* Unit list and the atomic claim cursor, built on main before the phase opens. */
static maintUnit *mt_units = NULL;
static int mt_units_len = 0;
static int mt_units_cap = 0;
static _Atomic int mt_cursor = 0;
static int mt_rotate = 0; /* rotating start so budget-cut units come first next time */
static int mt_nlanes = 0;

/* Per-thread, per-job accounting merged on main after the phase. */
static uint64_t mt_lane_usec[IO_THREADS_MAX_NUM][MAINT_JOB_COUNT];
static uint64_t mt_lane_work[IO_THREADS_MAX_NUM][MAINT_JOB_COUNT]; /* units where the job did work */

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

/* Return the db's kvstore for slot k (0 keys, 1 expires, 2 keys_with_volatile_items). */
static kvstore *maintKvstore(serverDb *db, int k) {
    switch (k) {
    case 0: return db->keys;
    case 1: return db->expires;
    default: return db->keys_with_volatile_items;
    }
}

/* Run every requested job on one unit as thread tid, honoring tid's remaining per-job budget. */
static void maintRunUnit(int tid, const maintUnit *u, uint64_t remain_us[MAINT_JOB_COUNT]) {
    serverDb *db = server.db[u->dbid];
    for (int k = 0; k < 3; k++) {
        kvstore *kvs = maintKvstore(db, k);
        if (u->didx >= kvstoreNumHashtables(kvs)) continue;

        if (mt_requested[MAINT_JOB_RESIZE]) {
            monotime t = getMonotonicUs();
            unsigned long before = kvstoreHashtableBuckets(kvs, u->didx);
            kvstoreTryResizeHashtable(kvs, u->didx);
            mt_lane_usec[tid][MAINT_JOB_RESIZE] += getMonotonicUs() - t;
            if (kvstoreHashtableBuckets(kvs, u->didx) != before) mt_lane_work[tid][MAINT_JOB_RESIZE]++;
        }
        if (mt_requested[MAINT_JOB_REHASH] && remain_us[MAINT_JOB_REHASH] > 0) {
            uint64_t spent = kvstoreRehashHashtable(kvs, u->didx, remain_us[MAINT_JOB_REHASH]);
            mt_lane_usec[tid][MAINT_JOB_REHASH] += spent;
            if (spent > 0) {
                mt_lane_work[tid][MAINT_JOB_REHASH]++;
                remain_us[MAINT_JOB_REHASH] = spent >= remain_us[MAINT_JOB_REHASH] ? 0 : remain_us[MAINT_JOB_REHASH] - spent;
            }
        }
    }
}

/* Lane body for one thread: claim units until the list is drained or the thread's budget is spent. */
static void maintLane(int tid) {
    uint64_t remain_us[MAINT_JOB_COUNT];
    for (int j = 0; j < MAINT_JOB_COUNT; j++) remain_us[j] = mt_budget_us[j];
    for (;;) {
        int i = atomic_fetch_add_explicit(&mt_cursor, 1, memory_order_relaxed);
        if (i >= mt_units_len) break;
        int idx = (mt_rotate + i) % mt_units_len;
        maintRunUnit(tid, &mt_units[idx], remain_us);
        /* Stop early only when every timed job has spent its budget. */
        if (!mt_requested[MAINT_JOB_RESIZE] && mt_requested[MAINT_JOB_REHASH] && remain_us[MAINT_JOB_REHASH] == 0) break;
    }
}

/* Main: build the unit list for the pending jobs across every db's tables. */
static void maintBuildUnits(void) {
    mt_units_len = 0;
    for (int dbid = 0; dbid < server.dbnum; dbid++) {
        serverDb *db = server.db[dbid];
        if (!db) continue;
        int n = kvstoreNumHashtables(db->keys);
        for (int didx = 0; didx < n; didx++) {
            /* REHASH-only requests skip tables with nothing rehashing in any of the three kvstores. */
            if (!mt_requested[MAINT_JOB_RESIZE]) {
                int any = kvstoreHashtableRehashingAt(db->keys, didx) ||
                          kvstoreHashtableRehashingAt(db->expires, didx) ||
                          kvstoreHashtableRehashingAt(db->keys_with_volatile_items, didx);
                if (!any) continue;
            }
            if (mt_units_len == mt_units_cap) {
                mt_units_cap = mt_units_cap ? mt_units_cap * 2 : 256;
                mt_units = zrealloc(mt_units, sizeof(maintUnit) * mt_units_cap);
            }
            mt_units[mt_units_len++] = (maintUnit){.dbid = dbid, .didx = didx};
        }
    }
}

/* Main: whether the MAINT phase can fan out to the staged workers this iteration. */
static int maintCanParallel(void) {
    return server.cron_lanes && server.keyspace_shard_bits > 0 && stagedApplyReady() && stagedWorkerCount() > 0;
}

void maintRun(void) {
    if (!maintPending()) return;

    maintBuildUnits();
    if (mt_units_len == 0) {
        maintReset();
        return;
    }

    for (int t = 0; t < IO_THREADS_MAX_NUM; t++)
        for (int j = 0; j < MAINT_JOB_COUNT; j++) mt_lane_usec[t][j] = mt_lane_work[t][j] = 0;
    atomic_store_explicit(&mt_cursor, 0, memory_order_relaxed);

    int parallel = maintCanParallel();
    mt_nlanes = parallel ? stagedWorkerCount() + stagedMainTakesLane() : 1;

    monotime phase_t0 = getMonotonicUs();
    kvstoreSetConcurrent(1);
    if (parallel) {
        stagedRunLanePhase(STAGED_PHASE_MAINT, mt_nlanes > 1 || !stagedMainTakesLane());
    } else {
        maintLane(0);
    }
    kvstoreSetConcurrent(0);
    cronStatsRecord(CRON_JOB_SC_MAINT, getMonotonicUs() - phase_t0);

    monotime rec_t0 = getMonotonicUs();
    for (int dbid = 0; dbid < server.dbnum; dbid++) {
        serverDb *db = server.db[dbid];
        if (!db) continue;
        kvstoreReconcile(db->keys);
        kvstoreReconcile(db->expires);
        kvstoreReconcile(db->keys_with_volatile_items);
    }
    cronStatsRecord(CRON_JOB_SC_RECONCILE, getMonotonicUs() - rec_t0);

    /* Per-job lane time summed across threads, recorded only when a job actually did work so a
     * due-free iteration leaves the counters still. */
    uint64_t rehash_us = 0, resize_us = 0, rehash_work = 0, resize_work = 0;
    for (int t = 0; t < IO_THREADS_MAX_NUM; t++) {
        rehash_us += mt_lane_usec[t][MAINT_JOB_REHASH];
        resize_us += mt_lane_usec[t][MAINT_JOB_RESIZE];
        rehash_work += mt_lane_work[t][MAINT_JOB_REHASH];
        resize_work += mt_lane_work[t][MAINT_JOB_RESIZE];
    }
    if (rehash_work) cronStatsRecord(CRON_JOB_MAINT_REHASH, rehash_us);
    if (resize_work) cronStatsRecord(CRON_JOB_MAINT_RESIZE, resize_us);

    /* Advance the rotating start past the units served under budget. */
    int served = atomic_load_explicit(&mt_cursor, memory_order_relaxed);
    if (served > mt_units_len) served = mt_units_len;
    mt_rotate = mt_units_len ? (mt_rotate + served) % mt_units_len : 0;

    maintReset();
}

void maintInit(void) {
    stagedSetLaneRunner(STAGED_PHASE_MAINT, maintLane);
}
