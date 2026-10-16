/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef CRONSTATS_H
#define CRONSTATS_H

#include <stdint.h>

#include "sds.h"

/*
 * Per-job wall-time accounting for the cron stage. Each tracked job owns a
 * counter set (calls, total us, max us, and a log2 duration histogram) updated
 * with cronStatsRecord(). The same job ids are reused by the parallel cron
 * build so serial and parallel are measured identically. Recording is
 * allocation-free and the counter array is fixed size.
 */

/* Histogram bucket i covers [2^i, 2^(i+1)) microseconds. */
#define CRON_STATS_HIST_BUCKETS 25

typedef enum {
    /* Staged loop phases (whole stage, main's wall time). */
    CRON_JOB_SL_BEFORE_SLEEP = 0,
    CRON_JOB_SL_POLL,
    CRON_JOB_SL_READ,
    CRON_JOB_SL_WRITE,
    CRON_JOB_SL_AOF_ALWAYS,
    CRON_JOB_SL_REPLY,
    CRON_JOB_SL_CRONS,

    /* beforeSleep sub-jobs (both loops). */
    CRON_JOB_BS_WHOLE,
    CRON_JOB_BS_EXPIRE_FAST,
    CRON_JOB_BS_EVICT_BATCH,
    CRON_JOB_BS_BLOCKED,
    CRON_JOB_BS_TRACKING_BCAST,
    CRON_JOB_BS_AOF_FLUSH,
    CRON_JOB_BS_ASYNC_FREE,
    CRON_JOB_BS_EVICT_CLIENTS,

    /* serverCron sub-jobs. */
    CRON_JOB_SC_WHOLE,
    CRON_JOB_SC_EXPIRE_SLOW,
    CRON_JOB_SC_RESIZE,
    CRON_JOB_SC_REHASH,
    CRON_JOB_SC_DEFRAG_MONITOR,
    CRON_JOB_SC_MEM_STATS,
    CRON_JOB_SC_REPLICATION_CRON,
    CRON_JOB_SC_CLUSTER_CRON,
    CRON_JOB_SC_MODULES_CRON,
    CRON_JOB_SC_PERSISTENCE,
    CRON_JOB_SC_REST,

    /* Other time events and feeds. */
    CRON_JOB_CLIENTS_TIMEPROC,
    CRON_JOB_DEFRAG_TIMEPROC,
    CRON_JOB_EVICTION_TIMEPROC,
    CRON_JOB_BGITERATION_FEED,
    CRON_JOB_MODULE_TIMERS,
    CRON_JOB_OTHER_TIMERS,

    CRON_JOB_COUNT
} cronJob;

/* Record one invocation of job of duration us microseconds. Lock-free, no
 * allocation; safe to call only from the main thread. */
void cronStatsRecord(int job, uint64_t us);

/* Record one invocation measured in per-thread CPU time (us). Separate from the
 * wall-time counters so a lane later reports both. */
void cronStatsRecordCpu(int job, uint64_t us);

/* Reset all counters (CONFIG RESETSTAT). */
void cronStatsReset(void);

/* Sum of accumulated usec over the job id range [lo, hi]. */
uint64_t cronStatsUsecRange(int lo, int hi);

/* Append the cronstats INFO section lines. */
sds cronStatsInfoString(sds info);

#endif /* CRONSTATS_H */
