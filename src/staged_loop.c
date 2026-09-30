/* Staged main event loop. See staged_loop.h and docs/staged-event-loop.md. */

#include "server.h"
#include "staged_loop.h"
#include "fastpath.h"
#include "io_threads.h"
#include "repl_apply.h"

#ifdef HAVE_FASTPATH_EPOLL

#include "poller.h"
#include "phase_barrier.h"
#include <sched.h>

static poller *sl_poller = NULL;
static phaseBarrier sl_barrier;
static int sl_barrier_ready = 0;
static int sl_on = 0;        /* staged mode is on for this iteration */
static int sl_want = 0;      /* staged mode is wanted: stock IO offload stops so it can start */
static int sl_workers = 0;   /* IO threads 1..sl_workers are enrolled; the barrier's participant count */
static int sl_depth = 0;     /* 1 while a nested iteration runs */
static uint64_t sl_opens = 0; /* phases opened so far; equals the barrier's sequence number */
static int sl_ae_fd = -1;    /* ae's own epoll fd, the single main-affinity entry */
static int sl_warned = 0;

/* Enrollment: main writes the sequence a newly enrolled worker waits from, then bumps its generation,
 * then sets enrolled, all between phases. A worker adopts the sequence once per generation. */
static _Atomic int sl_enrolled[IO_THREADS_MAX_NUM];
static _Atomic uint32_t sl_gen[IO_THREADS_MAX_NUM];
static uint64_t sl_enroll_seq[IO_THREADS_MAX_NUM];

int stagedLoopActive(void) {
    return sl_on || sl_want;
}

int stagedWorkerCount(void) {
    return sl_workers;
}

poller *stagedPoller(void) {
    return sl_poller;
}

phaseBarrier *stagedBarrier(void) {
    return &sl_barrier;
}

int stagedWorkerEnrolled(int tid, uint64_t *seen, uint32_t *gen) {
    if (!atomic_load_explicit(&sl_enrolled[tid], memory_order_acquire)) return 0;
    uint32_t g = atomic_load_explicit(&sl_gen[tid], memory_order_acquire);
    if (g != *gen) {
        *gen = g;
        *seen = sl_enroll_seq[tid];
    }
    return 1;
}

/* Open one phase for the enrolled workers and wait until all of them are done. */
static void stagedRunPhase(int phase) {
    int rc = phaseBarrierOpen(&sl_barrier, phase);
    serverAssert(rc == 0);
    sl_opens++;
    phaseBarrierWaitAllDone(&sl_barrier);
}

/* Main-affinity handler. Never called: main checks whether ae's fd fired and runs ae itself. */
static void stagedAeReady(int fd, int mask, void *privdata, pollerAffinity affinity) {
    UNUSED(fd);
    UNUSED(mask);
    UNUSED(privdata);
    UNUSED(affinity);
}

static int stagedInit(void) {
    if (!sl_barrier_ready) {
        if (phaseBarrierInit(&sl_barrier, 0) != 0) return 0;
        sl_barrier_ready = 1;
    }
    int setsize = server.maxclients + CONFIG_FDSET_INCR;
    if (!sl_poller) {
        sl_poller = pollerCreate(setsize);
        if (!sl_poller) return 0;
    } else if (pollerSetSize(sl_poller) != setsize && pollerResize(sl_poller, setsize) != 0) {
        return 0;
    }
    return 1;
}

/* Staged mode needs the fast path, strict offload, no cluster, and at least one IO thread besides
 * main. With io-threads 1 there is none, so staged mode stays off (logged once) instead of starting
 * a thread the configuration did not ask for. */
static int stagedWanted(void) {
    if (!server.io_threads_fast_path || !server.io_threads_strict_offload || server.cluster_enabled) return 0;
    if (server.io_threads_num < 2 || ioThreadsReadyNum() < 2) {
        if (server.io_threads_num < 2 && !sl_warned) {
            serverLog(LL_NOTICE, "io-threads-fast-path needs io-threads >= 2; the staged event loop stays off");
            sl_warned = 1;
        }
        return 0;
    }
    return 1;
}

/* Main: place the epoll thread on the union of the enrolled IO threads' CPUs, minus main's own when
 * main is pinned to fewer CPUs than that union. */
static void stagedPlaceEpollThread(void) {
    cpu_set_t all, mine;
    CPU_ZERO(&all);
    for (int tid = 1; tid <= sl_workers; tid++) {
        cpu_set_t one;
        CPU_ZERO(&one);
        if (ioThreadGetAffinity(tid, &one, sizeof(one)) == 0) CPU_OR(&all, &all, &one);
    }
    if (CPU_COUNT(&all) == 0) return;
    CPU_ZERO(&mine);
    if (pthread_getaffinity_np(pthread_self(), sizeof(mine), &mine) == 0 && CPU_COUNT(&mine) < CPU_COUNT(&all)) {
        cpu_set_t others;
        CPU_ZERO(&others);
        for (int cpu = 0; cpu < CPU_SETSIZE; cpu++)
            if (CPU_ISSET(cpu, &all) && !CPU_ISSET(cpu, &mine)) CPU_SET(cpu, &others);
        if (CPU_COUNT(&others) > 0) all = others;
    }
    if (pollerSetThreadAffinity(sl_poller, &all, sizeof(all)) != 0)
        serverLog(LL_WARNING, "Staged event loop: cannot set the epoll thread's CPU affinity");
}

/* Main, between phases: enroll IO threads 1..want. Leavers pass one SYNC phase so none is left
 * waiting on the barrier; newcomers wait from the current sequence. */
static void stagedSetWorkers(int want) {
    if (want == sl_workers) return;
    if (want < sl_workers) {
        for (int tid = want + 1; tid <= sl_workers; tid++) atomic_store_explicit(&sl_enrolled[tid], 0, memory_order_release);
        stagedRunPhase(STAGED_PHASE_SYNC);
    }
    int rc = phaseBarrierSetParticipants(&sl_barrier, want);
    serverAssert(rc == 0);
    for (int tid = sl_workers + 1; tid <= want; tid++) {
        sl_enroll_seq[tid] = (uint32_t)sl_opens;
        atomic_fetch_add_explicit(&sl_gen[tid], 1, memory_order_release);
        atomic_store_explicit(&sl_enrolled[tid], 1, memory_order_release);
    }
    sl_workers = want;
    if (want) {
        ioThreadsStagedUnpark();
        stagedPlaceEpollThread();
    }
}

/* Main, at the iteration boundary: follow the configuration and the live IO thread count. */
static void stagedReconcile(aeEventLoop *el) {
    int want = stagedWanted();
    if (want && !sl_on && !stagedInit()) want = 0;
    if (sl_on && !want) {
        fastpathStagedReleaseAll();
        fastpathStagedSet(0);
        pollerUnregister(sl_poller, sl_ae_fd);
        pollerStop(sl_poller);
        sl_ae_fd = -1;
        sl_on = 0;
        sl_want = 0;
        stagedSetWorkers(0);
        serverLog(LL_NOTICE, "Staged event loop stopped");
        return;
    }
    sl_want = want;
    if (!want) return;
    if (!sl_on) {
        /* Stock offload is refused from here on; start once what it had in flight has landed. The
         * wait is bounded so a stuck job leaves main serving on the stock loop meanwhile. */
        unpartitionAllClients();
        ioThreadsStagedUnpark();
        commitIOJobs();
        monotime deadline = getMonotonicUs() + 100000;
        while (!ioThreadsStagedIdle() && getMonotonicUs() < deadline) {
            if (processIOThreadsResponses() == 0) atomic_thread_fence(memory_order_acquire);
        }
        if (!ioThreadsStagedIdle()) return;
        int fd = aeGetPollFd(el);
        if (fd < 0 || pollerRegister(sl_poller, fd, POLLER_READABLE, stagedAeReady, NULL, POLLER_AFF_MAIN) != 0) {
            serverLog(LL_WARNING, "Staged event loop: cannot watch the ae poll fd; staying on the stock loop");
            sl_want = 0;
            return;
        }
        sl_ae_fd = fd;
    }
    stagedSetWorkers(ioThreadsReadyNum() - 1);
    if (!sl_on) {
        if (pollerStart(sl_poller) != 0)
            serverLog(LL_WARNING, "Staged event loop: cannot start the epoll thread; main waits on epoll itself");
        sl_on = 1;
        fastpathStagedSet(1);
        fastpathStagedAdoptClients();
        serverLog(LL_NOTICE, "Staged event loop started with %d IO threads", sl_workers);
    }
}

/* Collect: the pre-sleep hook, the next ready batch from the epoll thread (waiting only when none
 * accumulated, bounded like aeProcessEvents), the post-sleep hook. */
static void collectFDs(aeEventLoop *el) {
    if (el->beforesleep) el->beforesleep(el);
    /* Write jobs queued by the pre-sleep hook run in the next phase; do not sleep on them. */
    long long timeout = ((el->flags & AE_DONT_WAIT) || ioThreadsInboxPending()) ? 0 : aeUsUntilEarliestTimer(el);
    int n = pollerGetNextBatch(sl_poller, timeout);
    if (n < 0) {
        serverLog(LL_WARNING, "Staged event loop: poll failed: %s", strerror(errno));
        n = 0;
    }
    if (el->aftersleep) el->aftersleep(el, n);
}

/* Read: IO threads read and parse ready clients, answer the leading reads, and batch the rest. */
static void readPhase(void) {
    if (pollerIOCount(sl_poller) == 0 && !ioThreadsInboxPending()) return;
    stagedRunPhase(STAGED_PHASE_READ);
}

/* Write: main executes every batch, then serves main-affinity fds and all non-fast-path clients. */
static void writePhase(aeEventLoop *el) {
    int ae_ready = pollerMainCount(sl_poller) > 0; /* read before a nested iteration takes the next batch */
    fastpathStagedExecute();
    if (ae_ready) aeProcessEvents(el, AE_FILE_EVENTS | AE_DONT_WAIT);
}

/* Reply: IO threads write the replies of their own batches and any main-path write jobs; the barrier
 * closes the phase, then main takes back the clients that left and unmaps the ones it closed. */
static void replyPhase(void) {
    if (fastpathStagedRepliesPending() || ioThreadsInboxPending()) stagedRunPhase(STAGED_PHASE_REPLY);
    fastpathStagedFinish();
}

/* Crons: timers, main only. beforeSleep runs at the head of the next collect, as in ae. */
static void crons(aeEventLoop *el) {
    aeProcessTimeEvents(el);
}

static void stagedLoopIteration(aeEventLoop *el) {
    collectFDs(el);
    readPhase();
    writePhase(el);
    replyPhase();
    crons(el);
}

void stagedMain(aeEventLoop *el) {
    el->stop = 0;
    while (!el->stop) {
        stagedReconcile(el);
        if (sl_on)
            stagedLoopIteration(el);
        else
            aeProcessEvents(el, AE_ALL_EVENTS | AE_CALL_BEFORE_SLEEP | AE_CALL_AFTER_SLEEP);
    }
}

void stagedNestedIteration(void) {
    if (!sl_on || sl_depth) return;
    sl_depth = 1;
    fastpathStagedSetDepth(1);
    if ((pollerGetNextBatch(sl_poller, 0) > 0 && pollerIOCount(sl_poller) > 0) || ioThreadsInboxPending())
        stagedRunPhase(STAGED_PHASE_READ);
    fastpathStagedExecute();
    if (fastpathStagedRepliesPending() || ioThreadsInboxPending()) stagedRunPhase(STAGED_PHASE_REPLY);
    fastpathStagedSetDepth(0);
    sl_depth = 0;
}

void stagedDrainWorkers(void) {
    if (sl_on) stagedRunPhase(STAGED_PHASE_DRAIN);
}

int stagedApplyReady(void) {
    return sl_on && !sl_depth && sl_workers > 0;
}

void stagedRunApplyPhase(int with_workers) {
    if (!with_workers) {
        replApplyRunLane(0);
        return;
    }
    int rc = phaseBarrierOpen(&sl_barrier, STAGED_PHASE_APPLY);
    serverAssert(rc == 0);
    sl_opens++;
    replApplyRunLane(0);
    phaseBarrierWaitAllDone(&sl_barrier);
}

void stagedStopWorkers(void) {
    if (sl_barrier_ready) phaseBarrierShutdown(&sl_barrier);
    if (sl_poller) pollerStop(sl_poller);
}

int stagedResize(int setsize) {
    if (!sl_poller || pollerSetSize(sl_poller) >= setsize) return 0;
    return pollerResize(sl_poller, setsize);
}

/* Unit tests run the staged phases inline on one thread; no worker is enrolled. */
int testOnlyStagedStart(void) {
    if (!stagedInit()) return -1;
    fastpathStagedSet(1);
    return 0;
}

void testOnlyStagedStop(void) {
    fastpathStagedReleaseAll();
    fastpathStagedSet(0);
}

#else /* !HAVE_FASTPATH_EPOLL */

int stagedResize(int setsize) {
    UNUSED(setsize);
    return 0;
}

int testOnlyStagedStart(void) {
    return -1;
}
void testOnlyStagedStop(void) {
}

void stagedMain(struct aeEventLoop *el) {
    aeMain(el);
}
int stagedLoopActive(void) {
    return 0;
}
int stagedWorkerCount(void) {
    return 0;
}
void stagedNestedIteration(void) {
}
void stagedDrainWorkers(void) {
}
int stagedApplyReady(void) {
    return 0;
}
void stagedRunApplyPhase(int with_workers) {
    UNUSED(with_workers);
    replApplyRunLane(0);
}
void stagedStopWorkers(void) {
}
struct poller *stagedPoller(void) {
    return NULL;
}
struct phaseBarrier *stagedBarrier(void) {
    return NULL;
}
int stagedWorkerEnrolled(int tid, uint64_t *seen, uint32_t *gen) {
    UNUSED(tid);
    UNUSED(seen);
    UNUSED(gen);
    return 0;
}

#endif /* HAVE_FASTPATH_EPOLL */
