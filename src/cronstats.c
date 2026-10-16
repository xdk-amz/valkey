/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "server.h"
#include "cronstats.h"

/* One counter set per tracked cron job. */
typedef struct cronJobStats {
    uint64_t calls;
    uint64_t usec;
    uint64_t max_usec;
    uint64_t cpu_calls;
    uint64_t cpu_usec;
    uint64_t cpu_max_usec;
    uint64_t hist[CRON_STATS_HIST_BUCKETS];
} cronJobStats;

static cronJobStats cron_stats[CRON_JOB_COUNT];

/* Stable INFO names, indexed by cronJob. */
static const char *cron_job_name[CRON_JOB_COUNT] = {
    [CRON_JOB_SL_BEFORE_SLEEP] = "sl_before_sleep",
    [CRON_JOB_SL_POLL] = "sl_poll",
    [CRON_JOB_SL_READ] = "sl_read",
    [CRON_JOB_SL_WRITE] = "sl_write",
    [CRON_JOB_SL_AOF_ALWAYS] = "sl_aof_always",
    [CRON_JOB_SL_REPLY] = "sl_reply",
    [CRON_JOB_SL_CRONS] = "sl_crons",
    [CRON_JOB_BS_WHOLE] = "before_sleep",
    [CRON_JOB_BS_EXPIRE_FAST] = "expire_fast",
    [CRON_JOB_BS_EVICT_BATCH] = "evict_batch",
    [CRON_JOB_BS_BLOCKED] = "blocked",
    [CRON_JOB_BS_TRACKING_BCAST] = "tracking_bcast",
    [CRON_JOB_BS_AOF_FLUSH] = "aof_flush",
    [CRON_JOB_BS_ASYNC_FREE] = "async_free",
    [CRON_JOB_BS_EVICT_CLIENTS] = "evict_clients",
    [CRON_JOB_SC_WHOLE] = "server_cron",
    [CRON_JOB_SC_EXPIRE_SLOW] = "expire_slow",
    [CRON_JOB_SC_RESIZE] = "resize",
    [CRON_JOB_SC_REHASH] = "rehash",
    [CRON_JOB_SC_MAINT] = "maint",
    [CRON_JOB_SC_RECONCILE] = "maint_reconcile",
    [CRON_JOB_MAINT_REHASH] = "maint_rehash",
    [CRON_JOB_MAINT_RESIZE] = "maint_resize",
    [CRON_JOB_MAINT_EXPIRE] = "maint_expire",
    [CRON_JOB_MAINT_REPLAY] = "maint_replay",
    [CRON_JOB_MAINT_FEED] = "maint_feed",
    [CRON_JOB_SC_DEFRAG_MONITOR] = "defrag_monitor",
    [CRON_JOB_SC_MEM_STATS] = "mem_stats",
    [CRON_JOB_SC_REPLICATION_CRON] = "replication_cron",
    [CRON_JOB_SC_CLUSTER_CRON] = "cluster_cron",
    [CRON_JOB_SC_MODULES_CRON] = "modules_cron",
    [CRON_JOB_SC_PERSISTENCE] = "persistence",
    [CRON_JOB_SC_REST] = "server_cron_rest",
    [CRON_JOB_CLIENTS_TIMEPROC] = "clients_timeproc",
    [CRON_JOB_DEFRAG_TIMEPROC] = "defrag_timeproc",
    [CRON_JOB_EVICTION_TIMEPROC] = "eviction_timeproc",
    [CRON_JOB_BGITERATION_FEED] = "bgiteration_feed",
    [CRON_JOB_MODULE_TIMERS] = "module_timers",
    [CRON_JOB_OTHER_TIMERS] = "other_timers",
};

/* floor(log2(us)) clamped to the top bucket; us 0 falls in bucket 0. */
static inline int cronStatsBucket(uint64_t us) {
    int b = us ? 63 - __builtin_clzll(us) : 0;
    if (b >= CRON_STATS_HIST_BUCKETS) b = CRON_STATS_HIST_BUCKETS - 1;
    return b;
}

void cronStatsRecord(int job, uint64_t us) {
    if (job < 0 || job >= CRON_JOB_COUNT) return;
    cronJobStats *s = &cron_stats[job];
    s->calls++;
    s->usec += us;
    if (us > s->max_usec) s->max_usec = us;
    s->hist[cronStatsBucket(us)]++;
}

void cronStatsRecordCpu(int job, uint64_t us) {
    if (job < 0 || job >= CRON_JOB_COUNT) return;
    cronJobStats *s = &cron_stats[job];
    s->cpu_calls++;
    s->cpu_usec += us;
    if (us > s->cpu_max_usec) s->cpu_max_usec = us;
}

void cronStatsReset(void) {
    memset(cron_stats, 0, sizeof(cron_stats));
}

/* Sum of accumulated usec over a contiguous job id range [lo, hi]. Used to
 * derive a residual (e.g. serverCron "rest") without threading locals. */
uint64_t cronStatsUsecRange(int lo, int hi) {
    uint64_t sum = 0;
    if (lo < 0) lo = 0;
    if (hi >= CRON_JOB_COUNT) hi = CRON_JOB_COUNT - 1;
    for (int j = lo; j <= hi; j++) sum += cron_stats[j].usec;
    return sum;
}

sds cronStatsInfoString(sds info) {
    info = sdscat(info, "# Cronstats\r\n");
    for (int j = 0; j < CRON_JOB_COUNT; j++) {
        cronJobStats *s = &cron_stats[j];
        info = sdscatprintf(info, "cron_%s:calls=%llu,usec=%llu,max_usec=%llu", cron_job_name[j],
                            (unsigned long long)s->calls, (unsigned long long)s->usec,
                            (unsigned long long)s->max_usec);
        if (s->cpu_calls) {
            info = sdscatprintf(info, ",cpu_calls=%llu,cpu_usec=%llu,cpu_max_usec=%llu",
                                (unsigned long long)s->cpu_calls, (unsigned long long)s->cpu_usec,
                                (unsigned long long)s->cpu_max_usec);
        }
        info = sdscat(info, ",hist=");
        for (int b = 0; b < CRON_STATS_HIST_BUCKETS; b++) {
            info = sdscatprintf(info, "%s%llu", b ? "/" : "", (unsigned long long)s->hist[b]);
        }
        info = sdscat(info, "\r\n");
    }
    return info;
}
