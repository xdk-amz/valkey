/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "generated_wrappers.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <fcntl.h>
#include <thread>
#include <unistd.h>
#include <vector>

extern "C" {
#include "poller.h"
}

#ifdef HAVE_FASTPATH_EPOLL

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
};

TEST_F(PollerTest, RegisterModifyUnregister) {
    auto [r, w] = makePipe();
    EXPECT_EQ(pollerRegister(p, r, POLLER_READABLE, noopHandler, nullptr, POLLER_AFF_IO), 0);
    EXPECT_EQ(pollerCount(p), 1);
    EXPECT_EQ(pollerModify(p, r, POLLER_NONE), 0);
    poke(w);
    EXPECT_EQ(pollerGetFDs(p, 0), 0);
    EXPECT_EQ(pollerModify(p, r, POLLER_READABLE), 0);
    EXPECT_EQ(pollerGetFDs(p, 0), 1);
    EXPECT_EQ(pollerUnregister(p, r), 0);
    EXPECT_EQ(pollerCount(p), 0);
    EXPECT_EQ(pollerGetFDs(p, 0), 0);
    EXPECT_EQ(pollerUnregister(p, r), 0);
    EXPECT_EQ(pollerModify(p, r, POLLER_READABLE), -1);
    /* A dropped fd can be registered again. */
    EXPECT_EQ(pollerRegister(p, r, POLLER_READABLE, noopHandler, nullptr, POLLER_AFF_MAIN), 0);
    EXPECT_EQ(pollerGetFDs(p, 0), 1);
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

TEST_F(PollerTest, AffinitySelectsReadySet) {
    auto [rio, wio] = makePipe();
    auto [rmain, wmain] = makePipe();
    int tag_io = 1, tag_main = 2;
    ASSERT_EQ(pollerRegister(p, rio, POLLER_READABLE, noopHandler, &tag_io, POLLER_AFF_IO), 0);
    ASSERT_EQ(pollerRegister(p, rmain, POLLER_READABLE, noopHandler, &tag_main, POLLER_AFF_MAIN), 0);
    poke(wio);
    poke(wmain);
    ASSERT_EQ(pollerGetFDs(p, 1000000), 2);
    ASSERT_EQ(pollerIOCount(p), 1);
    ASSERT_EQ(pollerMainCount(p), 1);
    pollerEvent e;
    ASSERT_EQ(pollerClaimIO(p, &e), 1);
    EXPECT_EQ(e.fd, rio);
    EXPECT_EQ(e.privdata, &tag_io);
    EXPECT_EQ(e.affinity, POLLER_AFF_IO);
    EXPECT_TRUE(e.mask & POLLER_READABLE);
    EXPECT_EQ(pollerClaimIO(p, &e), 0);
    const pollerEvent *m = pollerMainAt(p, 0);
    ASSERT_NE(m, nullptr);
    EXPECT_EQ(m->fd, rmain);
    EXPECT_EQ(m->privdata, &tag_main);
    EXPECT_EQ(m->handler, noopHandler);
    EXPECT_EQ(pollerMainAt(p, 1), nullptr);
    /* The next call resets both sets; level-triggered fds fire again. */
    ASSERT_EQ(pollerGetFDs(p, 0), 2);
    ASSERT_EQ(pollerClaimIO(p, &e), 1);
}

TEST_F(PollerTest, HangupReportsErrorAndBothDirections) {
    auto [r, w] = makePipe();
    ASSERT_EQ(pollerRegister(p, r, POLLER_READABLE, noopHandler, nullptr, POLLER_AFF_IO), 0);
    close(w);
    fds.erase(std::find(fds.begin(), fds.end(), w));
    ASSERT_EQ(pollerGetFDs(p, 0), 1);
    pollerEvent e;
    ASSERT_EQ(pollerClaimIO(p, &e), 1);
    EXPECT_EQ(e.mask, POLLER_READABLE | POLLER_WRITABLE | POLLER_ERROR);
}

TEST_F(PollerTest, TimeoutSemantics) {
    auto [r, w] = makePipe();
    (void)w;
    ASSERT_EQ(pollerRegister(p, r, POLLER_READABLE, noopHandler, nullptr, POLLER_AFF_IO), 0);
    auto t0 = std::chrono::steady_clock::now();
    EXPECT_EQ(pollerGetFDs(p, 0), 0);
    auto zero = std::chrono::steady_clock::now() - t0;
    EXPECT_LT(zero, std::chrono::milliseconds(50));

    t0 = std::chrono::steady_clock::now();
    EXPECT_EQ(pollerGetFDs(p, 20000), 0);
    EXPECT_GE(std::chrono::steady_clock::now() - t0, std::chrono::milliseconds(19));

    /* Sub-millisecond positive timeouts round up rather than degrading to a non-blocking poll. */
    t0 = std::chrono::steady_clock::now();
    EXPECT_EQ(pollerGetFDs(p, 1), 0);
    EXPECT_GE(std::chrono::steady_clock::now() - t0, std::chrono::microseconds(900));

    /* Negative blocks until an fd fires. */
    std::thread writer([w = w] {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        char c = 'x';
        (void)!write(w, &c, 1);
    });
    t0 = std::chrono::steady_clock::now();
    EXPECT_EQ(pollerGetFDs(p, -1), 1);
    EXPECT_GE(std::chrono::steady_clock::now() - t0, std::chrono::milliseconds(15));
    writer.join();
}

TEST_F(PollerTest, ConcurrentClaimTakesEachEntryOnce) {
    const int npipes = 1000, nthreads = 8, rounds = 20;
    std::vector<int> readers;
    for (int i = 0; i < npipes; i++) {
        auto [r, w] = makePipe();
        ASSERT_EQ(pollerRegister(p, r, POLLER_READABLE, noopHandler, nullptr, POLLER_AFF_IO), 0);
        poke(w);
        readers.push_back(r);
    }
    for (int round = 0; round < rounds; round++) {
        ASSERT_EQ(pollerGetFDs(p, 1000000), npipes);
        ASSERT_EQ(pollerIOCount(p), npipes);
        std::vector<std::atomic<int>> hits(4096);
        std::atomic<int> total{0};
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
        ASSERT_EQ(total.load(), npipes);
        for (int r : readers) ASSERT_EQ(hits[r].load(), 1) << "fd " << r;
    }
}

#endif /* HAVE_FASTPATH_EPOLL */
