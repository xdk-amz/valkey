/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef PHASE_BARRIER_H
#define PHASE_BARRIER_H

/* Phase barrier between main and a set of worker threads.
 *
 * Main opens a phase for workers [0, n) and waits until each of them reports done. A worker
 * blocks until a phase that includes its index opens, runs it, and reports done. Waiters spin
 * briefly, unless the caller says not to, and then sleep on a condition variable, so a worker and
 * main sharing one CPU cannot livelock. Workers outside the participant count park until the count
 * grows or the barrier shuts down. */

#include <pthread.h>
#ifndef __cplusplus
#include <stdatomic.h> /* C++ unit tests map _Atomic(T) in wrappers.h */
#endif
#include <stdint.h>

#define PHASE_BARRIER_EXIT (-1) /* returned by phaseBarrierAwaitPhase after shutdown */
#define PHASE_BARRIER_MAX_ID 0xffff
#define PHASE_BARRIER_MAX_PARTICIPANTS 0xffff

typedef struct phaseBarrier {
    _Atomic(uint64_t) state; /* seq << 32 | active_n << 16 | phase_id, one word so a reader never sees a torn phase */
    _Atomic(int) pending;    /* participants of the open phase not yet done */
    _Atomic(int) shutdown;
    int nparticipants; /* applied at the next open; guarded by lock */
    int open_sleepers; /* guarded by lock */
    int park_sleepers; /* guarded by lock */
    int done_sleeper;  /* guarded by lock */
    pthread_mutex_t lock;
    pthread_cond_t open_cv; /* participants waiting for the next phase */
    pthread_cond_t park_cv; /* workers outside the participant count */
    pthread_cond_t done_cv; /* main waiting for the last done */
} phaseBarrier;

/* Returns 0, or -1 if nparticipants is out of range or pthread init fails. */
int phaseBarrierInit(phaseBarrier *b, int nparticipants);
void phaseBarrierDestroy(phaseBarrier *b);

/* Main. Change the participant count (0..PHASE_BARRIER_MAX_PARTICIPANTS) for phases opened after
 * this call. Returns -1 if a phase is still in progress or the count is out of range. */
int phaseBarrierSetParticipants(phaseBarrier *b, int nparticipants);

/* Main. Open phase_id (0..PHASE_BARRIER_MAX_ID) for the current participants. Returns -1 if the previous phase
 * has not completed, phase_id is out of range, or the barrier is shut down. Stores made by main
 * before the call are visible to every participant that returns this phase. */
int phaseBarrierOpen(phaseBarrier *b, int phase_id);

/* Main. Block until every participant of the open phase has called phaseBarrierDone. Stores
 * made by participants before phaseBarrierDone are visible to main on return. spin 0 sleeps at
 * once, for a main that shares its CPU with a participant. */
void phaseBarrierWaitAllDone(phaseBarrier *b, int spin);

/* Main. Wake every worker; each subsequent phaseBarrierAwaitPhase returns PHASE_BARRIER_EXIT.
 * Call only after phaseBarrierWaitAllDone for any open phase. */
void phaseBarrierShutdown(phaseBarrier *b);

/* Worker. Block until a phase newer than *last_seen opens with worker_id among its participants
 * and return its id, or return PHASE_BARRIER_EXIT after shutdown. *last_seen starts at 0. spin 0
 * sleeps at once, for a worker that shares its CPU with main. */
int phaseBarrierAwaitPhase(phaseBarrier *b, int worker_id, uint64_t *last_seen, int spin);

/* Worker. Report the phase returned by phaseBarrierAwaitPhase complete. Not called on exit. */
void phaseBarrierDone(phaseBarrier *b);

#endif /* PHASE_BARRIER_H */
