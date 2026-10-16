/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef STAGED_LOOP_H
#define STAGED_LOOP_H

/* Staged main event loop for the IO-thread fast path.
 *
 * With io-threads-fast-path on (and io-threads-strict-offload, io-threads >= 2, no cluster), one
 * main iteration runs collect -> read -> write -> reply -> crons, and IO threads work only inside
 * the read and reply phases. Otherwise the loop is stock ae. Non-epoll builds are always stock.
 *
 * Invariants:
 * - IO threads read the keyspace only in the read phase; main mutates it only in the write phase
 *   and crons. The barrier between phases is the only synchronization.
 * - Within one iteration exactly one IO thread reads a fast-path client, owns its batch entries and
 *   writes their replies, so a client's replies stay in order. Main touches the client itself only
 *   at a handoff, between phases.
 * - A client's leading GETs run in the read phase; its first other command and everything behind it
 *   run on main in the write phase, so a read never overtakes an earlier write of the same client.
 * - A fast-path client is freed only after main took it back following the reply phase of every
 *   iteration that produced entries for it. */

#include "fastpath_no_epoll.h"
#include <stdint.h>

#define STAGED_MAX_DEPTH 2 /* outer iteration, plus one nested iteration from processEventsWhileBlocked */

/* Phase ids opened on the staged barrier. */
#define STAGED_PHASE_READ 1
#define STAGED_PHASE_REPLY 2
#define STAGED_PHASE_SYNC 3  /* workers leave the staged loop and recheck their enrollment */
#define STAGED_PHASE_DRAIN 4 /* workers only drain their job inboxes */
#define STAGED_PHASE_APPLY 5 /* workers apply their lane of held replicated writes */

struct aeEventLoop;
struct poller;
struct phaseBarrier;
struct client;

/* Main loop: staged iterations while staged mode is wanted, stock ae iterations otherwise. */
void stagedMain(struct aeEventLoop *el);
/* Main thread: staged mode is on for the current iteration. */
int stagedLoopActive(void);
/* Main thread: IO threads enrolled in the staged loop. */
int stagedWorkerCount(void);
/* Main, from processEventsWhileBlocked: one nested read/write/reply pass so fast-path clients get
 * their BUSY/LOADING replies while main is blocked. A no-op outside staged mode or when nested. */
void stagedNestedIteration(void);
/* Main: have every staged worker drain its job inboxes now (drainIOThreadsQueue in staged mode). */
void stagedDrainWorkers(void);
/* Main, process exit: wake every staged worker so it can stop. */
void stagedStopWorkers(void);

/* Main: staged workers can take an APPLY phase now (staged on, not nested, workers enrolled). */
int stagedApplyReady(void);
/* Main: run the primary link's read in the next read phase on a worker, which reads and parses it;
 * its commands run at the start of that iteration's write phase. 1 when deferred. */
int stagedDeferRead(struct client *c);
/* Main: main takes a lane in APPLY phases (it is not pinned onto a worker's CPUs). */
int stagedMainTakesLane(void);
/* IO thread tid: spin before sleeping at the barrier. 0 while it is pinned to main's one CPU,
 * where spinning would hold the CPU main needs. */
int stagedWorkerSpins(int tid);
/* Main: run an APPLY phase; with_workers 0 runs lane 0 on main alone. */
void stagedRunApplyPhase(int with_workers);

/* Main: grow the poller with maxclients, as ae grows; 0 on success. */
int stagedResize(int setsize);

struct poller *stagedPoller(void);
struct phaseBarrier *stagedBarrier(void);
/* IO thread tid: returns 1 when main enrolled it in the staged loop, adopting the barrier sequence to
 * wait from on a new enrollment. *seen and *gen are the thread's own, kept across calls. */
int stagedWorkerEnrolled(int tid, uint64_t *seen, uint32_t *gen);

/* Unit tests: turn staged fast-path admission on and off without enrolling workers; 0 on success. */
int testOnlyStagedStart(void);
void testOnlyStagedStop(void);

#endif /* STAGED_LOOP_H */
