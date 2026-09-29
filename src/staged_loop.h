#ifndef STAGED_LOOP_H
#define STAGED_LOOP_H

/* Staged main event loop for the IO-thread fast path. See docs/staged-event-loop.md.
 *
 * With io-threads-fast-path on (and io-threads-strict-offload, io-threads >= 2, no cluster), one
 * main iteration runs collect -> read -> write -> reply -> crons, and IO threads work only inside
 * the read and reply phases. Otherwise the loop is stock ae. Non-epoll builds are always stock. */

#include "fastpath_no_epoll.h"
#include <stdint.h>

#define STAGED_MAX_DEPTH 2 /* outer iteration, plus one nested iteration from processEventsWhileBlocked */

/* Phase ids opened on the staged barrier. */
#define STAGED_PHASE_READ 1
#define STAGED_PHASE_REPLY 2
#define STAGED_PHASE_SYNC 3  /* workers leave the staged loop and recheck their enrollment */
#define STAGED_PHASE_DRAIN 4 /* workers only drain their job inboxes */

struct aeEventLoop;
struct poller;
struct phaseBarrier;

/* Main loop: staged iterations while staged mode is wanted, stock ae iterations otherwise. */
void stagedMain(struct aeEventLoop *el);
/* Main thread: staged mode is on for the current iteration. */
int stagedLoopActive(void);
/* Main, from processEventsWhileBlocked: one nested read/write/reply pass so fast-path clients get
 * their BUSY/LOADING replies while main is blocked. A no-op outside staged mode or when nested. */
void stagedNestedIteration(void);
/* Main: have every staged worker drain its job inboxes now (drainIOThreadsQueue in staged mode). */
void stagedDrainWorkers(void);
/* Main, process exit: wake every staged worker so it can stop. */
void stagedStopWorkers(void);

/* Main: grow the poller with maxclients, as ae grows; 0 on success. */
int stagedResize(int setsize);

struct poller *stagedPoller(void);
struct phaseBarrier *stagedBarrier(void);
/* IO thread tid: returns 1 when main enrolled it in the staged loop, adopting the barrier sequence to
 * wait from on a new enrollment. *seen and *gen are the thread's own, kept across calls. */
int stagedWorkerEnrolled(int tid, uint64_t *seen, uint32_t *gen);

#endif /* STAGED_LOOP_H */
