/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Staged main event loop. See staged_loop.h. */

#include "server.h"
#include "staged_loop.h"
#include "fastpath.h"
#include "io_threads.h"
#include "repl_apply.h"
#include "cronstats.h"

#ifdef HAVE_FASTPATH_EPOLL

#include "poller.h"
#include "phase_barrier.h"
#include <sched.h>

extern int ProcessingEventsWhileBlocked; /* networking.c */

static poller *sl_poller = NULL;
static phaseBarrier sl_barrier;
static int sl_barrier_ready = 0;
static int sl_on = 0;         /* staged mode is on for this iteration */
static int sl_want = 0;       /* staged mode is wanted: stock IO offload stops so it can start */
static int sl_workers = 0;    /* IO threads 1..sl_workers are enrolled; the barrier's participant count */
static int sl_depth = 0;      /* 1 while a nested iteration runs */
static uint64_t sl_opens = 0; /* phases opened so far; equals the barrier's sequence number */
static int sl_ae_fd = -1;     /* ae's own epoll fd, the single main-affinity entry */
static int sl_warned = 0;
static int sl_main_lane = 1;        /* main has a CPU no worker is pinned to, so it works a share of each phase */
static uint64_t sl_read_client = 0; /* client id whose read waits for the next read phase */
static int sl_read_queued = 0;      /* that read went to the workers in this iteration's read phase */

/* Main and the workers pinned to main's one CPU sleep at the barrier instead of spinning. Pinning
 * can change after start (taskset), so it is reread every STAGED_PINNING_READ_US. */
#define STAGED_PINNING_READ_US 1000000

/* With few IO threads main waits on epoll itself: handing each ready batch over from an epoll thread
 * costs a wakeup per iteration that a small pool does not win back. */
#define STAGED_EPOLL_THREAD_MIN_WORKERS 4
static int sl_main_spins = 1;     /* no enrolled worker is pinned to main's CPU */
static int sl_pinned_workers = 0; /* enrolled workers pinned to main's one CPU, as last read */
static int sl_pinned_cpu = -1;    /* main's one CPU, as last read; -1 when main may run on several */
static monotime sl_pinning_read = 0;

/* Enrollment: main writes the sequence a newly enrolled worker waits from, then bumps its generation,
 * then sets enrolled, all between phases. A worker adopts the sequence once per generation. */
static _Atomic int sl_enrolled[IO_THREADS_MAX_NUM];
static _Atomic uint32_t sl_gen[IO_THREADS_MAX_NUM];
static uint64_t sl_enroll_seq[IO_THREADS_MAX_NUM];
static _Atomic int sl_on_main_cpu[IO_THREADS_MAX_NUM]; /* worker is pinned to main's one CPU */

/* Also read by IO threads deciding whether they may sleep; main writes both at iteration boundaries. */
int stagedLoopActive(void) {
    return __atomic_load_n(&sl_on, __ATOMIC_RELAXED) || __atomic_load_n(&sl_want, __ATOMIC_RELAXED);
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
    /* Main works its own share of the phase instead of waiting idle for the IO threads. */
    if (sl_main_lane && phase == STAGED_PHASE_READ)
        fastpathStagedReadPhase(0);
    else if (sl_main_lane && phase == STAGED_PHASE_REPLY)
        fastpathStagedReplyPhase(0);
    phaseBarrierWaitAllDone(&sl_barrier, sl_main_spins);
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

/* Staged mode needs the fast path, strict offload, and at least one IO thread besides
 * main. With io-threads 1 there is none, so staged mode stays off (logged once) instead of starting
 * a thread the configuration did not ask for. */
static int stagedWanted(void) {
    if (!server.io_threads_fast_path || !server.io_threads_strict_offload || server.sentinel_mode) return 0;
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

/* Main is pinned to CPUs a worker also runs on, so a lane on main would only take turns with it. */
static int stagedMainSharesWorkerCpu(void) {
    cpu_set_t mine;
    CPU_ZERO(&mine);
    if (pthread_getaffinity_np(pthread_self(), sizeof(mine), &mine) != 0) return 0;
    long ncpu = sysconf(_SC_NPROCESSORS_ONLN);
    if (ncpu <= 0 || CPU_COUNT(&mine) >= ncpu) return 0;
    for (int tid = 1; tid <= sl_workers; tid++) {
        cpu_set_t one, both;
        CPU_ZERO(&one);
        if (ioThreadGetAffinity(tid, &one, sizeof(one)) != 0) continue;
        CPU_AND(&both, &one, &mine);
        if (CPU_COUNT(&both) > 0) return 1;
    }
    return 0;
}

/* Main: a worker pinned to the one CPU main is pinned to only takes turns with main, so neither
 * of them spins at the barrier, where a spinning waiter holds the CPU the other needs. */
static void stagedReadCpuPinning(void) {
    cpu_set_t mine;
    CPU_ZERO(&mine);
    int cpu = -1;
    if (pthread_getaffinity_np(pthread_self(), sizeof(mine), &mine) == 0 && CPU_COUNT(&mine) == 1) {
        for (int i = 0; i < CPU_SETSIZE && cpu < 0; i++)
            if (CPU_ISSET(i, &mine)) cpu = i;
    }
    int pinned = 0;
    for (int tid = 1; tid < IO_THREADS_MAX_NUM; tid++) {
        int on = 0;
        if (cpu >= 0 && tid <= sl_workers) {
            cpu_set_t one;
            CPU_ZERO(&one);
            on = ioThreadGetAffinity(tid, &one, sizeof(one)) == 0 && CPU_EQUAL(&one, &mine);
        }
        atomic_store_explicit(&sl_on_main_cpu[tid], on, memory_order_relaxed);
        pinned += on;
    }
    if (sl_workers > 0 && (pinned != sl_pinned_workers || (pinned && cpu != sl_pinned_cpu))) {
        if (pinned)
            serverLog(LL_NOTICE, "Staged event loop: %d IO thread%s pinned to main's CPU %d; they and main sleep at the barrier instead of spinning",
                      pinned, pinned == 1 ? " is" : "s are", cpu);
        else
            serverLog(LL_NOTICE, "Staged event loop: no IO thread is pinned to main's CPU; the barrier spins again");
    }
    sl_pinned_workers = pinned;
    sl_pinned_cpu = cpu;
    sl_main_spins = pinned == 0;
    int lane = !stagedMainSharesWorkerCpu();
    if (sl_workers > 0 && lane != sl_main_lane)
        serverLog(LL_NOTICE, lane ? "Staged event loop: main works a share of each phase"
                                  : "Staged event loop: main shares a CPU with an IO thread and leaves the phases to the IO threads");
    sl_main_lane = lane;
    sl_pinning_read = getMonotonicUs();
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
    stagedReadCpuPinning();
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
        __atomic_store_n(&sl_on, 0, __ATOMIC_RELAXED);
        __atomic_store_n(&sl_want, 0, __ATOMIC_RELAXED);
        sl_read_client = 0; /* its fd is still readable, so ae serves it on main */
        stagedSetWorkers(0);
        serverLog(LL_NOTICE, "Staged event loop stopped");
        return;
    }
    __atomic_store_n(&sl_want, want, __ATOMIC_RELAXED);
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
            __atomic_store_n(&sl_want, 0, __ATOMIC_RELAXED);
            return;
        }
        sl_ae_fd = fd;
    }
    stagedSetWorkers(ioThreadsReadyNum() - 1);
    if (!sl_on) {
        if (server.io_threads_num - 1 >= STAGED_EPOLL_THREAD_MIN_WORKERS && pollerStart(sl_poller) != 0)
            serverLog(LL_WARNING, "Staged event loop: cannot start the epoll thread; main waits on epoll itself");
        __atomic_store_n(&sl_on, 1, __ATOMIC_RELAXED);
        fastpathStagedSet(1);
        fastpathStagedAdoptClients();
        serverLog(LL_NOTICE, "Staged event loop started with %d IO threads", sl_workers);
    }
    if (getMonotonicUs() - sl_pinning_read >= STAGED_PINNING_READ_US) stagedReadCpuPinning();
}

/* Collect: the pre-sleep hook, the next ready batch from the epoll thread (waiting only when none
 * accumulated, bounded like aeProcessEvents), the post-sleep hook. */
static void collectFDs(aeEventLoop *el) {
    if (el->beforesleep) {
        monotime bs = getMonotonicUs();
        el->beforesleep(el);
        cronStatsRecord(CRON_JOB_SL_BEFORE_SLEEP, getMonotonicUs() - bs);
    }
    /* Write jobs queued by the pre-sleep hook run in the next phase; do not sleep on them. */
    long long timeout = ((el->flags & AE_DONT_WAIT) || ioThreadsInboxPending() || sl_read_client) ? 0 : aeUsUntilEarliestTimer(el);
    monotime pl = getMonotonicUs();
    int n = pollerGetNextBatch(sl_poller, timeout);
    cronStatsRecord(CRON_JOB_SL_POLL, getMonotonicUs() - pl); /* includes sleep when idle */
    if (n < 0) {
        serverLog(LL_WARNING, "Staged event loop: poll failed: %s", strerror(errno));
        n = 0;
    }
    if (el->aftersleep) el->aftersleep(el, n);
}

/* Main, at the start of a read phase: hand the waiting read to whichever worker drains first, which
 * then claims fewer ready fds. */
static void stagedQueueDeferredRead(void) {
    if (!sl_read_client) return;
    client *c = lookupClientByID(sl_read_client);
    sl_read_client = 0;
    if (!c || c->io_read_state != CLIENT_IDLE) return;
    if (ioThreadsSendStagedRead(c) == C_OK && c->io_read_state == CLIENT_PENDING_IO) sl_read_queued = 1;
}

/* Read: IO threads read and parse ready clients, answer the leading reads, and batch the rest. */
static void readPhase(void) {
    stagedQueueDeferredRead();
    if (pollerIOCount(sl_poller) == 0 && !ioThreadsInboxPending()) return;
    stagedRunPhase(STAGED_PHASE_READ);
}

/* Write: main executes every batch, then serves main-affinity fds and all non-fast-path clients. */
static void writePhase(aeEventLoop *el) {
    int ae_ready = pollerMainCount(sl_poller) > 0; /* read before a nested iteration takes the next batch */
    if (sl_read_queued) {
        sl_read_queued = 0;
        processIOThreadsResponses(); /* runs the primary's commands parsed in the read phase */
    }
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
    monotime t;
    collectFDs(el);
    t = getMonotonicUs();
    readPhase();
    cronStatsRecord(CRON_JOB_SL_READ, getMonotonicUs() - t);
    t = getMonotonicUs();
    writePhase(el);
    cronStatsRecord(CRON_JOB_SL_WRITE, getMonotonicUs() - t);
    /* With appendfsync always a write is acknowledged only once it is on disk: the write phase's
     * commands reach the AOF before the reply phase sends their replies. */
    t = getMonotonicUs();
    if (server.aof_state == AOF_ON && server.aof_fsync == AOF_FSYNC_ALWAYS) flushAppendOnlyFile(0);
    cronStatsRecord(CRON_JOB_SL_AOF_ALWAYS, getMonotonicUs() - t);
    t = getMonotonicUs();
    replyPhase();
    cronStatsRecord(CRON_JOB_SL_REPLY, getMonotonicUs() - t);
    t = getMonotonicUs();
    crons(el);
    cronStatsRecord(CRON_JOB_SL_CRONS, getMonotonicUs() - t);
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
    /* Clients that left for a command only main runs, such as SCRIPT KILL behind a busy script, are
     * handed back now: no outer reply phase finishes until the blocking command ends. */
    fastpathStagedFinish();
}

void stagedDrainWorkers(void) {
    if (sl_on) stagedRunPhase(STAGED_PHASE_DRAIN);
}

int stagedApplyReady(void) {
    return sl_on && !sl_depth && sl_workers > 0;
}

int stagedDeferRead(client *c) {
    if (!sl_on || sl_depth || !sl_workers || ProcessingEventsWhileBlocked) return 0;
    if (!c->flag.primary || !c->conn || server.repl_stream_reader) return 0;
    if (c->flag.blocked || c->flag.unblocked || c->flag.close_asap || c->flag.lua_debug) return 0;
    if (c->io_read_state != CLIENT_IDLE || c->io_write_state == CLIENT_PENDING_IO) return 0;
    if (sl_read_client && sl_read_client != c->id) return 0;
    sl_read_client = c->id;
    return 1;
}

int stagedMainTakesLane(void) {
    return sl_main_lane;
}

int stagedWorkerSpins(int tid) {
    return !atomic_load_explicit(&sl_on_main_cpu[tid], memory_order_relaxed);
}

/* Lane body per phase, bound once at startup; indexed by phase id. */
static stagedLaneFn sl_lane_fn[STAGED_PHASE_MAINT + 1];

void stagedSetLaneRunner(int phase, stagedLaneFn fn) {
    serverAssert(phase >= 0 && phase <= STAGED_PHASE_MAINT);
    sl_lane_fn[phase] = fn;
}

void stagedRunLane(int phase, int tid) {
    stagedLaneFn fn = sl_lane_fn[phase];
    if (fn) fn(tid);
}

/* Open phase for the enrolled workers, run main's lane when it takes one, and wait. with_workers 0,
 * or no enrolled worker, runs lane 0 on main alone with no barrier. */
void stagedRunLanePhase(int phase, int with_workers) {
    if (!with_workers) {
        stagedRunLane(phase, 0);
        return;
    }
    int rc = phaseBarrierOpen(&sl_barrier, phase);
    serverAssert(rc == 0);
    sl_opens++;
    if (sl_main_lane) stagedRunLane(phase, 0);
    phaseBarrierWaitAllDone(&sl_barrier, sl_main_spins);
}

void stagedRunApplyPhase(int with_workers) {
    stagedRunLanePhase(STAGED_PHASE_APPLY, with_workers);
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
int stagedDeferRead(client *c) {
    UNUSED(c);
    return 0;
}
int stagedMainTakesLane(void) {
    return 1;
}
int stagedWorkerSpins(int tid) {
    UNUSED(tid);
    return 1;
}
void stagedRunApplyPhase(int with_workers) {
    UNUSED(with_workers);
    replApplyRunLane(0);
}
void stagedSetLaneRunner(int phase, stagedLaneFn fn) {
    UNUSED(phase);
    UNUSED(fn);
}
void stagedRunLanePhase(int phase, int with_workers) {
    UNUSED(phase);
    UNUSED(with_workers);
    replApplyRunLane(0);
}
void stagedRunLane(int phase, int tid) {
    UNUSED(phase);
    UNUSED(tid);
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
