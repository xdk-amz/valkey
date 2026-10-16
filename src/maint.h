/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef MAINT_H
#define MAINT_H

/* Parallel keyspace maintenance (MAINT phase of the staged event loop).
 *
 * Crons post requests (maintRequest) for table-local jobs instead of walking the keyspace inline.
 * When the MAINT phase can run in parallel, main opens it after crons(); every staged thread claims
 * whole keyspace tables with an atomic cursor and runs the due jobs on them with no shared state,
 * each table accumulating kvstore deltas. Main then reconciles the touched kvstores and merges the
 * per-thread stat deltas. With no parallel thread, one table per kvstore, or MAINT off, the same
 * per-unit code runs inline on main. */

#include <stdint.h>

/* Jobs a cron can request for the next MAINT phase. */
#define MAINT_JOB_REHASH 0
#define MAINT_JOB_RESIZE 1
#define MAINT_JOB_EXPIRE 2 /* active expire of keys; hash fields stay on main */
#define MAINT_JOB_COUNT 3

struct serverObject;

/* Main, from a cron: request job for the next MAINT phase with a per-thread time ceiling of
 * budget_us (0 for an untimed job such as resize). Idempotent within one iteration: the largest
 * budget requested for a job wins. */
void maintRequest(int job, uint64_t budget_us);

/* Each kvstore has more than one table: cluster mode (one per slot) or keyspace-shard-bits > 0. */
int maintKeyspaceSplit(void);

/* Main: the MAINT phase can fan out to the staged workers now (staged on, not nested, workers
 * enrolled, cron-lanes on, more than one table per kvstore). */
int maintParallelReady(void);

/* Lane: record a value the lane unlinked as expired; main replays its delete after the phase. */
void maintLogExpired(int tid, struct serverObject *val, int dbid, int didx);

/* Main: a MAINT job is pending for this iteration. */
int maintPending(void);

/* Main, after crons(): run the pending jobs. In parallel when MAINT can run (staged on, workers
 * enrolled, more than one table per kvstore) and inline on main otherwise; then reconcile the
 * touched kvstores and merge the per-thread stats. Clears the pending requests. */
void maintRun(void);

/* IO thread, in a MAINT phase: the monotonic time by which the phase's timed jobs must be done, or 0
 * when only untimed jobs run. Spare lane time before it may drain the thread's job inboxes. */
uint64_t maintPhaseDeadline(void);

/* Clear pending requests without running them (iteration teardown, config change). */
void maintReset(void);

/* Bind the MAINT lane runner. Call once at startup. */
void maintInit(void);

#endif /* MAINT_H */
