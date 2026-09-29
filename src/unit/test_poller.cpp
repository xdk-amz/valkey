/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "generated_wrappers.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <pthread.h>
#include <sched.h>
#include <thread>
#include <unistd.h>
#include <vector>

extern "C" {
#include "poller.h"
}

#ifdef HAVE_FASTPATH_EPOLL

using std::chrono::milliseconds;
using std::chrono::steady_clock;

static void noopHandler(int, int, void *, pollerAffinity) {
}

class PollerTest : public ::testing::Test {
  protected:
    poller *p = nullptr;
    std::vector<int> fds;

    void SetUp() override {
        p = pollerCreate(4096);
        ASSERT_NE(p, nullptr);
    }

    void TearDown() override {
        pollerFree(p);
        for (int fd : fds) close(fd);
    }

    /* Returns {read end, write end}. */
    std::pair<int, int> makePipe() {
        int pfd[2];
        EXPECT_EQ(pipe2(pfd, O_NONBLOCK | O_CLOEXEC), 0);
        fds.push_back(pfd[0]);
        fds.push_back(pfd[1]);
        return {pfd[0], pfd[1]};
    }

    static void poke(int wfd) {
        char c = 'x';
        ASSERT_EQ(write(wfd, &c, 1), 1);
    }

    static void drain(int rfd) {
        char buf[4096];
        while (read(rfd, buf, sizeof(buf)) > 0) {
        }
    }

    /* Claims every active PRIORITY/IO entry, in claim order. */
    std::vector<pollerEvent> claimAll() {
        std::vector<pollerEvent> out;
        pollerEvent e;
        while (pollerClaimIO(p, &e)) out.push_back(e);
        return out;
    }

    /* Thread mode: take batches until one carries at least want fds, up to a deadline. */
    int batchWithAtLeast(int want) {
        int got = 0;
        auto deadline = steady_clock::now() + milliseconds(2000);
        while (steady_clock::now() < deadline) {
            got += pollerGetNextBatch(p, 100000);
            if (got >= want) break;
        }
        return got;
    }
};

TEST_F(PollerTest, RegisterModifyUnregister) {
    auto [r, w] = makePipe();
    EXPECT_EQ(pollerRegister(p, r, POLLER_READABLE, noopHandler, nullptr, POLLER_AFF_IO), 0);
    EXPECT_EQ(pollerCount(p), 1);
    EXPECT_EQ(pollerModify(p, r, POLLER_NONE), 0);
    poke(w);
    EXPECT_EQ(pollerGetNextBatch(p, 0), 0);
    /* Changing interest re-reports current readiness, even edge-triggered. */
    EXPECT_EQ(pollerModify(p, r, POLLER_READABLE), 0);
    EXPECT_EQ(pollerGetNextBatch(p, 0), 1);
    EXPECT_EQ(pollerUnregister(p, r), 0);
    EXPECT_EQ(pollerCount(p), 0);
    EXPECT_EQ(pollerGetNextBatch(p, 0), 0);
    EXPECT_EQ(pollerUnregister(p, r), 0);
    EXPECT_EQ(pollerModify(p, r, POLLER_READABLE), -1);
    /* A dropped fd can be registered again. */
    EXPECT_EQ(pollerRegister(p, r, POLLER_READABLE, noopHandler, nullptr, POLLER_AFF_MAIN), 0);
    EXPECT_EQ(pollerGetNextBatch(p, 0), 1);
    EXPECT_EQ(pollerMainCount(p), 1);
}

TEST_F(PollerTest, RejectsDuplicateAndOutOfRange) {
    auto [r, w] = makePipe();
    (void)w;
    EXPECT_EQ(pollerRegister(p, r, POLLER_READABLE, noopHandler, nullptr, POLLER_AFF_IO), 0);
    EXPECT_EQ(pollerRegister(p, r, POLLER_READABLE, noopHandler, nullptr, POLLER_AFF_IO), -1);
    EXPECT_EQ(pollerCount(p), 1);
    EXPECT_EQ(pollerRegister(p, -1, POLLER_READABLE, noopHandler, nullptr, POLLER_AFF_IO), -1);
    EXPECT_EQ(pollerRegister(p, 4096, POLLER_READABLE, noopHandler, nullptr, POLLER_AFF_IO), -1);
    EXPECT_EQ(pollerModify(p, 4096, POLLER_READABLE), -1);
    EXPECT_EQ(pollerUnregister(p, -1), -1);
    EXPECT_EQ(pollerCount(p), 1);
    EXPECT_EQ(pollerCreate(0), nullptr);
}

TEST_F(PollerTest, AffinitySelectsClassEdgeAndOneShot) {
    auto [rio, wio] = makePipe();
    auto [rmain, wmain] = makePipe();
    int tag_io = 1, tag_main = 2;
    ASSERT_EQ(pollerRegister(p, rio, POLLER_READABLE, noopHandler, &tag_io, POLLER_AFF_IO), 0);
    ASSERT_EQ(pollerRegister(p, rmain, POLLER_READABLE, noopHandler, &tag_main, POLLER_AFF_MAIN), 0);
    poke(wio);
    poke(wmain);
    ASSERT_EQ(pollerGetNextBatch(p, 1000000), 2);
    ASSERT_EQ(pollerIOCount(p), 1);
    ASSERT_EQ(pollerMainCount(p), 1);
    pollerEvent e;
    ASSERT_EQ(pollerClaimIO(p, &e), 1);
    EXPECT_EQ(e.fd, rio);
    EXPECT_EQ(e.privdata, &tag_io);
    EXPECT_EQ(e.affinity, POLLER_AFF_IO);
    EXPECT_TRUE(e.mask & POLLER_READABLE);
    EXPECT_EQ(pollerClaimIO(p, &e), 0);
    pollerEvent m;
    ASSERT_EQ(pollerMainAt(p, 0, &m), 1);
    EXPECT_EQ(m.fd, rmain);
    EXPECT_EQ(m.privdata, &tag_main);
    EXPECT_EQ(m.handler, noopHandler);
    EXPECT_EQ(pollerMainAt(p, 1, &m), 0);
    /* Neither fd was read. The IO fd saw no new edge; the one-shot MAIN fd is re-armed when its
     * batch is retired and, still readable, is reported again. */
    ASSERT_EQ(pollerGetNextBatch(p, 0), 1);
    EXPECT_EQ(pollerIOCount(p), 0);
    EXPECT_EQ(pollerMainCount(p), 1);
    /* A new write is a new edge. */
    poke(wio);
    ASSERT_EQ(pollerGetNextBatch(p, 0), 2);
    EXPECT_EQ(pollerIOCount(p), 1);
    /* Once drained, the MAIN fd stays quiet. */
    drain(rmain);
    drain(rio);
    EXPECT_EQ(pollerGetNextBatch(p, 0), 0);
}

TEST_F(PollerTest, HangupReportsErrorAndBothDirections) {
    auto [r, w] = makePipe();
    ASSERT_EQ(pollerRegister(p, r, POLLER_READABLE, noopHandler, nullptr, POLLER_AFF_IO), 0);
    close(w);
    fds.erase(std::find(fds.begin(), fds.end(), w));
    ASSERT_EQ(pollerGetNextBatch(p, 0), 1);
    pollerEvent e;
    ASSERT_EQ(pollerClaimIO(p, &e), 1);
    EXPECT_EQ(e.mask, POLLER_READABLE | POLLER_WRITABLE | POLLER_ERROR);
}

static void timeoutSemantics(poller *p, int w) {
    auto t0 = steady_clock::now();
    EXPECT_EQ(pollerGetNextBatch(p, 0), 0);
    EXPECT_LT(steady_clock::now() - t0, milliseconds(50));

    t0 = steady_clock::now();
    EXPECT_EQ(pollerGetNextBatch(p, 20000), 0);
    EXPECT_GE(steady_clock::now() - t0, milliseconds(19));

    /* Sub-millisecond positive timeouts still wait rather than degrading to a non-blocking poll. */
    t0 = steady_clock::now();
    EXPECT_EQ(pollerGetNextBatch(p, 1), 0);
    EXPECT_GE(steady_clock::now() - t0, std::chrono::microseconds(1));

    /* Negative blocks until an fd fires. */
    std::thread writer([w] {
        std::this_thread::sleep_for(milliseconds(20));
        char c = 'x';
        (void)!write(w, &c, 1);
    });
    t0 = steady_clock::now();
    EXPECT_EQ(pollerGetNextBatch(p, -1), 1);
    EXPECT_GE(steady_clock::now() - t0, milliseconds(15));
    writer.join();
}

TEST_F(PollerTest, TimeoutSemanticsInline) {
    auto [r, w] = makePipe();
    ASSERT_EQ(pollerRegister(p, r, POLLER_READABLE, noopHandler, nullptr, POLLER_AFF_IO), 0);
    timeoutSemantics(p, w);
}

TEST_F(PollerTest, TimeoutSemanticsThread) {
    auto [r, w] = makePipe();
    ASSERT_EQ(pollerRegister(p, r, POLLER_READABLE, noopHandler, nullptr, POLLER_AFF_IO), 0);
    ASSERT_EQ(pollerStart(p), 0);
    EXPECT_TRUE(pollerThreadRunning(p));
    timeoutSemantics(p, w);
}

TEST_F(PollerTest, ThreadAccumulatesAndSwaps) {
    auto [ra, wa] = makePipe();
    auto [rb, wb] = makePipe();
    auto [rc, wc] = makePipe();
    ASSERT_EQ(pollerRegister(p, ra, POLLER_READABLE, noopHandler, nullptr, POLLER_AFF_IO), 0);
    ASSERT_EQ(pollerRegister(p, rb, POLLER_READABLE, noopHandler, nullptr, POLLER_AFF_IO), 0);
    ASSERT_EQ(pollerRegister(p, rc, POLLER_READABLE, noopHandler, nullptr, POLLER_AFF_IO), 0);
    ASSERT_EQ(pollerStart(p), 0);
    /* Everything that fired while main was away is one batch. */
    poke(wa);
    poke(wb);
    poke(wc);
    std::this_thread::sleep_for(milliseconds(50));
    ASSERT_EQ(pollerGetNextBatch(p, 0), 3);
    /* While the active batch is being consumed, new edges go to next, not to active. */
    poke(wa);
    std::this_thread::sleep_for(milliseconds(50));
    EXPECT_EQ(pollerIOCount(p), 3);
    EXPECT_EQ(claimAll().size(), 3u);
    auto t0 = steady_clock::now();
    ASSERT_EQ(pollerGetNextBatch(p, 1000000), 1);
    EXPECT_LT(steady_clock::now() - t0, milliseconds(500)); /* work was pending: no wait */
    auto got = claimAll();
    ASSERT_EQ(got.size(), 1u);
    EXPECT_EQ(got[0].fd, ra);
    EXPECT_EQ(pollerGetNextBatch(p, 0), 0);
}

TEST_F(PollerTest, DedupeAndMaskMerge) {
    auto [r, w] = makePipe();
    ASSERT_EQ(pollerRegister(p, r, POLLER_READABLE, noopHandler, nullptr, POLLER_AFF_IO), 0);
    poke(w);
    pollerRequeue(p, r, POLLER_READABLE);
    pollerRequeue(p, r, POLLER_WRITABLE);
    pollerRequeue(p, r, POLLER_READABLE);
    ASSERT_EQ(pollerGetNextBatch(p, 0), 1);
    auto got = claimAll();
    ASSERT_EQ(got.size(), 1u);
    EXPECT_EQ(got[0].mask, POLLER_READABLE | POLLER_WRITABLE);
}

TEST_F(PollerTest, RequeueServesWithoutNewEdge) {
    auto [r, w] = makePipe();
    ASSERT_EQ(pollerRegister(p, r, POLLER_READABLE, noopHandler, nullptr, POLLER_AFF_IO), 0);
    ASSERT_EQ(pollerStart(p), 0);
    poke(w);
    ASSERT_EQ(batchWithAtLeast(1), 1);
    ASSERT_EQ(claimAll().size(), 1u);
    /* Unread, but edge-triggered: nothing more until requeued. */
    EXPECT_EQ(pollerGetNextBatch(p, 20000), 0);
    ASSERT_EQ(claimAll().size(), 0u);
    std::thread req([&] { pollerRequeue(p, r, POLLER_READABLE); });
    req.join();
    ASSERT_EQ(pollerGetNextBatch(p, 0), 1);
    auto got = claimAll();
    ASSERT_EQ(got.size(), 1u);
    EXPECT_EQ(got[0].fd, r);
    /* A requeue wakes a waiting main. */
    std::thread late([&] {
        std::this_thread::sleep_for(milliseconds(20));
        pollerRequeue(p, r, POLLER_READABLE);
    });
    auto t0 = steady_clock::now();
    EXPECT_EQ(pollerGetNextBatch(p, -1), 1);
    EXPECT_GE(steady_clock::now() - t0, milliseconds(15));
    late.join();
}

TEST_F(PollerTest, StaleGenerationDropped) {
    auto [r, w] = makePipe();
    int tag_old = 1, tag_new = 2;
    ASSERT_EQ(pollerRegister(p, r, POLLER_READABLE, noopHandler, &tag_old, POLLER_AFF_IO), 0);
    /* Queued, then the fd is dropped and registered again before the batch is claimed. */
    pollerRequeue(p, r, POLLER_READABLE);
    ASSERT_EQ(pollerUnregister(p, r), 0);
    ASSERT_EQ(pollerGetNextBatch(p, 0), 1);
    EXPECT_EQ(claimAll().size(), 0u);

    /* Reported by the epoll thread, unregistered before main takes the batch. */
    ASSERT_EQ(pollerRegister(p, r, POLLER_READABLE, noopHandler, &tag_old, POLLER_AFF_IO), 0);
    ASSERT_EQ(pollerStart(p), 0);
    poke(w);
    std::this_thread::sleep_for(milliseconds(50));
    ASSERT_EQ(pollerUnregister(p, r), 0);
    ASSERT_EQ(pollerRegister(p, r, POLLER_READABLE, noopHandler, &tag_new, POLLER_AFF_IO), 0);
    /* The new registration reports the pending byte itself; only it is claimed. */
    std::this_thread::sleep_for(milliseconds(50));
    ASSERT_EQ(pollerGetNextBatch(p, 0), 1);
    auto got = claimAll();
    ASSERT_EQ(got.size(), 1u);
    EXPECT_EQ(got[0].privdata, &tag_new);

    /* Stale MAIN entries are skipped too. */
    auto [rm, wm] = makePipe();
    ASSERT_EQ(pollerRegister(p, rm, POLLER_READABLE, noopHandler, nullptr, POLLER_AFF_MAIN), 0);
    poke(wm);
    std::this_thread::sleep_for(milliseconds(50));
    ASSERT_EQ(pollerUnregister(p, rm), 0);
    ASSERT_EQ(pollerGetNextBatch(p, 0), 1);
    pollerEvent m;
    EXPECT_EQ(pollerMainAt(p, 0, &m), 0);
}

TEST_F(PollerTest, PriorityClaimedBeforeIO) {
    std::vector<int> io, prio;
    for (int i = 0; i < 4; i++) {
        auto [r, w] = makePipe();
        ASSERT_EQ(pollerRegister(p, r, POLLER_READABLE, noopHandler, nullptr, POLLER_AFF_IO), 0);
        poke(w);
        io.push_back(r);
    }
    for (int i = 0; i < 2; i++) {
        auto [r, w] = makePipe();
        ASSERT_EQ(pollerRegister(p, r, POLLER_READABLE, noopHandler, nullptr, POLLER_AFF_PRIORITY), 0);
        poke(w);
        prio.push_back(r);
    }
    ASSERT_EQ(pollerGetNextBatch(p, 0), 6);
    EXPECT_EQ(pollerIOCount(p), 6);
    auto got = claimAll();
    ASSERT_EQ(got.size(), 6u);
    for (int i = 0; i < 2; i++) {
        EXPECT_EQ(got[i].affinity, POLLER_AFF_PRIORITY);
        EXPECT_NE(std::find(prio.begin(), prio.end(), got[i].fd), prio.end());
    }
    for (int i = 2; i < 6; i++) EXPECT_EQ(got[i].affinity, POLLER_AFF_IO);
}

static void concurrentClaims(poller *p, std::vector<int> &readers, std::vector<int> &writers, int rounds) {
    const int nthreads = 8;
    int npipes = (int)readers.size();
    for (int round = 0; round < rounds; round++) {
        for (int w : writers) {
            char c = 'x';
            ASSERT_EQ(write(w, &c, 1), 1);
        }
        int got = 0;
        auto deadline = steady_clock::now() + milliseconds(5000);
        std::vector<std::atomic<int>> hits(4096);
        std::atomic<int> total{0};
        while (got < npipes && steady_clock::now() < deadline) {
            got = pollerGetNextBatch(p, 100000);
            if (got == 0) continue;
            std::atomic<bool> go{false};
            std::vector<std::thread> threads;
            for (int t = 0; t < nthreads; t++) {
                threads.emplace_back([&] {
                    while (!go.load(std::memory_order_acquire)) {
                    }
                    pollerEvent e;
                    while (pollerClaimIO(p, &e)) {
                        hits[e.fd].fetch_add(1, std::memory_order_relaxed);
                        total.fetch_add(1, std::memory_order_relaxed);
                    }
                });
            }
            go.store(true, std::memory_order_release);
            for (auto &t : threads) t.join();
            got = total.load();
        }
        ASSERT_EQ(total.load(), npipes);
        for (int r : readers) ASSERT_EQ(hits[r].load(), 1) << "fd " << r;
    }
}

TEST_F(PollerTest, ConcurrentClaimTakesEachEntryOnce) {
    const int npipes = 1000;
    std::vector<int> readers, writers;
    for (int i = 0; i < npipes; i++) {
        auto [r, w] = makePipe();
        ASSERT_EQ(pollerRegister(p, r, POLLER_READABLE, noopHandler, nullptr, POLLER_AFF_IO), 0);
        readers.push_back(r);
        writers.push_back(w);
    }
    concurrentClaims(p, readers, writers, 10);
    ASSERT_EQ(pollerStart(p), 0);
    concurrentClaims(p, readers, writers, 10);
}

TEST_F(PollerTest, StopEmptiesBuffersAndRestarts) {
    auto [r, w] = makePipe();
    ASSERT_EQ(pollerRegister(p, r, POLLER_READABLE, noopHandler, nullptr, POLLER_AFF_IO), 0);
    for (int round = 0; round < 20; round++) {
        ASSERT_EQ(pollerStart(p), 0);
        ASSERT_EQ(pollerStart(p), 0); /* idempotent */
        poke(w);
        std::this_thread::sleep_for(milliseconds(2));
        pollerRequeue(p, r, POLLER_READABLE);
        pollerStop(p);
        EXPECT_FALSE(pollerThreadRunning(p));
        /* Nothing queued survives a stop. */
        EXPECT_EQ(pollerGetNextBatch(p, 0), 0);
        drain(r);
    }
    pollerStop(p); /* idempotent */
    ASSERT_EQ(pollerStart(p), 0);
    poke(w);
    EXPECT_EQ(batchWithAtLeast(1), 1);
}

TEST_F(PollerTest, ResizeWhileThreadRuns) {
    std::vector<int> readers, writers;
    for (int i = 0; i < 64; i++) {
        auto [r, w] = makePipe();
        ASSERT_EQ(pollerRegister(p, r, POLLER_READABLE, noopHandler, nullptr, POLLER_AFF_IO), 0);
        readers.push_back(r);
        writers.push_back(w);
    }
    ASSERT_EQ(pollerStart(p), 0);
    std::atomic<bool> stop{false};
    std::thread writer([&] {
        while (!stop.load(std::memory_order_relaxed)) {
            for (int w : writers) {
                char c = 'x';
                (void)!write(w, &c, 1);
            }
            std::this_thread::sleep_for(std::chrono::microseconds(200));
        }
    });
    int maxfd = *std::max_element(readers.begin(), readers.end());
    for (int i = 0; i < 200; i++) {
        ASSERT_EQ(pollerResize(p, (i & 1) ? 8192 : maxfd + 1 + (i % 7)), 0);
        pollerGetNextBatch(p, 1000);
        for (auto &e : claimAll()) {
            ASSERT_NE(std::find(readers.begin(), readers.end(), e.fd), readers.end());
            drain(e.fd);
        }
    }
    stop.store(true);
    writer.join();
    /* A registered fd past the new size blocks shrinking. */
    EXPECT_EQ(pollerResize(p, maxfd), -1);
    /* Entries queued before a resize are kept. */
    for (int r : readers) drain(r);
    while (pollerGetNextBatch(p, 20000) > 0) claimAll();
    poke(writers[0]);
    std::this_thread::sleep_for(milliseconds(50));
    ASSERT_EQ(pollerResize(p, 4096), 0);
    ASSERT_EQ(pollerGetNextBatch(p, 0), 1);
    auto got = claimAll();
    ASSERT_EQ(got.size(), 1u);
    EXPECT_EQ(got[0].fd, readers[0]);
}

/* Main, the epoll thread and a writer all on one CPU: every edge is still delivered. */
TEST_F(PollerTest, SingleCpuNoLivelock) {
    cpu_set_t saved, one;
    ASSERT_EQ(pthread_getaffinity_np(pthread_self(), sizeof(saved), &saved), 0);
    int cpu = 0;
    while (cpu < CPU_SETSIZE && !CPU_ISSET(cpu, &saved)) cpu++;
    CPU_ZERO(&one);
    CPU_SET(cpu, &one);
    ASSERT_EQ(pthread_setaffinity_np(pthread_self(), sizeof(one), &one), 0);
    ASSERT_EQ(pollerSetThreadAffinity(p, &one, sizeof(one)), 0);

    std::vector<int> readers, writers;
    for (int i = 0; i < 32; i++) {
        auto [r, w] = makePipe();
        ASSERT_EQ(pollerRegister(p, r, POLLER_READABLE, noopHandler, nullptr, POLLER_AFF_IO), 0);
        readers.push_back(r);
        writers.push_back(w);
    }
    ASSERT_EQ(pollerStart(p), 0);
    const int rounds = 300;
    std::thread writer([&] {
        pthread_setaffinity_np(pthread_self(), sizeof(one), &one);
        for (int round = 0; round < rounds; round++) {
            char c = 'x';
            (void)!write(writers[round % writers.size()], &c, 1);
            if (round % 16 == 0) std::this_thread::yield();
        }
    });
    int seen = 0;
    auto deadline = steady_clock::now() + milliseconds(10000);
    std::vector<char> buf(4096);
    long long bytes = 0;
    while (bytes < rounds && steady_clock::now() < deadline) {
        pollerGetNextBatch(p, 50000);
        for (auto &e : claimAll()) {
            seen++;
            ssize_t n;
            while ((n = read(e.fd, buf.data(), buf.size())) > 0) bytes += n;
        }
    }
    writer.join();
    EXPECT_EQ(bytes, rounds);
    EXPECT_GT(seen, 0);
    pollerStop(p);
    ASSERT_EQ(pthread_setaffinity_np(pthread_self(), sizeof(saved), &saved), 0);
}

TEST_F(PollerTest, ThreadAffinityApplied) {
    cpu_set_t saved;
    ASSERT_EQ(pthread_getaffinity_np(pthread_self(), sizeof(saved), &saved), 0);
    int cpu = 0;
    while (cpu < CPU_SETSIZE && !CPU_ISSET(cpu, &saved)) cpu++;
    cpu_set_t one;
    CPU_ZERO(&one);
    CPU_SET(cpu, &one);
    ASSERT_EQ(pollerSetThreadAffinity(p, &one, sizeof(one)), 0);
    ASSERT_EQ(pollerStart(p), 0);
    /* The thread is found by name and its mask checked through /proc. */
    char path[64], name[32] = {0};
    bool found = false;
    for (int tries = 0; tries < 100 && !found; tries++) {
        std::vector<long> tids;
        DIR *d = opendir("/proc/self/task");
        ASSERT_NE(d, nullptr);
        while (struct dirent *de = readdir(d))
            if (de->d_name[0] != '.') tids.push_back(atol(de->d_name));
        closedir(d);
        for (long t : tids) {
            snprintf(path, sizeof(path), "/proc/self/task/%ld/comm", t);
            FILE *f = fopen(path, "r");
            if (!f) continue;
            if (fgets(name, sizeof(name), f) && strncmp(name, "io_epoll", 8) == 0) {
                cpu_set_t got;
                CPU_ZERO(&got);
                ASSERT_EQ(sched_getaffinity((pid_t)t, sizeof(got), &got), 0);
                EXPECT_TRUE(CPU_EQUAL(&got, &one));
                found = true;
            }
            fclose(f);
        }
        if (!found) std::this_thread::sleep_for(milliseconds(10));
    }
    EXPECT_TRUE(found);
}

#endif /* HAVE_FASTPATH_EPOLL */
