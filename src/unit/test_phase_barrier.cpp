/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "generated_wrappers.hpp"

#include <atomic>
#include <chrono>
#include <pthread.h>
#include <sched.h>
#include <thread>
#include <vector>

extern "C" {
#include "monotonic.h"
#include "phase_barrier.h"
}

namespace {

/* The barrier times its spins with the server's monotonic clock. */
struct ClockInit {
    ClockInit() {
        monotonicInit();
    }
} clock_init;

/* Main publishes payload before each open; workers publish results before each done. */
struct Harness {
    phaseBarrier b;
    int payload = 0;              /* written by main only between phases */
    std::vector<int> result;      /* result[w] written by worker w only inside a phase */
    std::vector<int> ran;         /* phases each worker ran, read by main only between phases */
    std::vector<int> spin;        /* spin[w]: worker w spins before sleeping; set before start */
    int main_spin = 1;            /* main spins before sleeping */
    std::atomic<int> in_phase{0}; /* workers currently inside a phase */
    std::atomic<int> errors{0};
    std::vector<std::thread> threads;

    explicit Harness(int nworkers, int nparticipants) :
        result(nworkers, -1), ran(nworkers, 0), spin(nworkers, 1) {
        EXPECT_EQ(phaseBarrierInit(&b, nparticipants), 0);
    }

    void start(int nworkers) {
        for (int w = 0; w < nworkers; w++) {
            threads.emplace_back([this, w] {
                uint64_t seen = 0;
                for (;;) {
                    int ph = phaseBarrierAwaitPhase(&b, w, &seen, spin[w]);
                    if (ph == PHASE_BARRIER_EXIT) return;
                    in_phase.fetch_add(1, std::memory_order_relaxed);
                    if (ph != (payload & PHASE_BARRIER_MAX_ID)) errors.fetch_add(1);
                    result[w] = payload;
                    ran[w]++;
                    in_phase.fetch_sub(1, std::memory_order_relaxed);
                    phaseBarrierDone(&b);
                }
            });
        }
    }

    void stop() {
        phaseBarrierShutdown(&b);
        for (auto &t : threads) t.join();
        threads.clear();
        phaseBarrierDestroy(&b);
    }

    /* Run one phase and verify that exactly workers [0, n) ran it and all had finished. */
    void runPhase(int k, int n, int nworkers) {
        payload = k;
        ASSERT_EQ(phaseBarrierOpen(&b, k & PHASE_BARRIER_MAX_ID), 0);
        phaseBarrierWaitAllDone(&b, main_spin);
        ASSERT_EQ(in_phase.load(), 0) << "WaitAllDone returned while a worker was in phase " << k;
        for (int w = 0; w < nworkers; w++) {
            if (w < n)
                ASSERT_EQ(result[w], k) << "worker " << w << " phase " << k;
            else
                ASSERT_NE(result[w], k) << "non-participant " << w << " ran phase " << k;
        }
    }
};

} // namespace

TEST(PhaseBarrierTest, ManyPhasesManyWorkers) {
    const int nworkers = 8, phases = 20000;
    Harness h(nworkers, nworkers);
    h.start(nworkers);
    for (int k = 1; k <= phases; k++) {
        h.runPhase(k, nworkers, nworkers);
        if (HasFatalFailure()) break;
    }
    h.stop();
    EXPECT_EQ(h.errors.load(), 0);
    for (int w = 0; w < nworkers; w++) EXPECT_EQ(h.ran[w], phases);
}

TEST(PhaseBarrierTest, RejectsMisuse) {
    phaseBarrier b;
    ASSERT_EQ(phaseBarrierInit(&b, 1), 0);
    EXPECT_EQ(phaseBarrierOpen(&b, -1), -1);
    EXPECT_EQ(phaseBarrierOpen(&b, PHASE_BARRIER_MAX_ID + 1), -1);
    EXPECT_EQ(phaseBarrierSetParticipants(&b, -1), -1);
    ASSERT_EQ(phaseBarrierOpen(&b, 3), 0);
    /* The single participant has not reported done yet. */
    EXPECT_EQ(phaseBarrierOpen(&b, 4), -1);
    EXPECT_EQ(phaseBarrierSetParticipants(&b, 2), -1);
    uint64_t seen = 0;
    EXPECT_EQ(phaseBarrierAwaitPhase(&b, 0, &seen, 1), 3);
    phaseBarrierDone(&b);
    phaseBarrierWaitAllDone(&b, 1);
    EXPECT_EQ(phaseBarrierSetParticipants(&b, 0), 0);
    ASSERT_EQ(phaseBarrierOpen(&b, 5), 0);
    phaseBarrierWaitAllDone(&b, 0); /* zero participants: complete at once */
    phaseBarrierShutdown(&b);
    EXPECT_EQ(phaseBarrierOpen(&b, 6), -1);
    EXPECT_EQ(phaseBarrierAwaitPhase(&b, 0, &seen, 0), PHASE_BARRIER_EXIT);
    phaseBarrierDestroy(&b);
}

/* Main and one worker take turns on a single CPU for many phases. */
static void runOnOneCpu(int main_spin, int worker_spin) {
    cpu_set_t orig, one;
    ASSERT_EQ(pthread_getaffinity_np(pthread_self(), sizeof(orig), &orig), 0);
    int cpu = -1;
    for (int i = 0; i < CPU_SETSIZE; i++) {
        if (CPU_ISSET(i, &orig)) {
            cpu = i;
            break;
        }
    }
    ASSERT_GE(cpu, 0);
    CPU_ZERO(&one);
    CPU_SET(cpu, &one);
    ASSERT_EQ(pthread_setaffinity_np(pthread_self(), sizeof(one), &one), 0);

    const int phases = 20000;
    Harness h(1, 1);
    h.main_spin = main_spin;
    h.spin[0] = worker_spin;
    h.start(1); /* the worker inherits the single-CPU mask */
    auto t0 = std::chrono::steady_clock::now();
    for (int k = 1; k <= phases; k++) {
        h.runPhase(k, 1, 1);
        if (::testing::Test::HasFatalFailure()) break;
    }
    auto elapsed = std::chrono::steady_clock::now() - t0;
    h.stop();
    pthread_setaffinity_np(pthread_self(), sizeof(orig), &orig);
    EXPECT_EQ(h.errors.load(), 0);
    EXPECT_EQ(h.ran[0], phases);
    EXPECT_LT(elapsed, std::chrono::seconds(30));
}

TEST(PhaseBarrierTest, SingleCoreNoLivelock) {
    runOnOneCpu(1, 1);
}

TEST(PhaseBarrierTest, SingleCoreWithoutSpinning) {
    runOnOneCpu(0, 0);
}

TEST(PhaseBarrierTest, ParticipantCountChanges) {
    const int nworkers = 8;
    const int counts[] = {8, 3, 0, 5, 1, 8, 2};
    Harness h(nworkers, nworkers);
    h.start(nworkers);
    int k = 0;
    for (int rep = 0; rep < 200; rep++) {
        for (int n : counts) {
            ASSERT_EQ(phaseBarrierSetParticipants(&h.b, n), 0);
            for (int i = 0; i < 5; i++) {
                h.runPhase(++k, n, nworkers);
                if (HasFatalFailure()) {
                    h.stop();
                    return;
                }
            }
        }
    }
    h.stop();
    EXPECT_EQ(h.errors.load(), 0);
}

TEST(PhaseBarrierTest, SomeWaitersSleepAtOnce) {
    const int nworkers = 8;
    const int counts[] = {8, 3, 0, 5, 1, 8, 2};
    Harness h(nworkers, nworkers);
    h.main_spin = 0;
    for (int w = 0; w < nworkers; w += 2) h.spin[w] = 0;
    h.start(nworkers);
    int k = 0;
    for (int rep = 0; rep < 200; rep++) {
        for (int n : counts) {
            ASSERT_EQ(phaseBarrierSetParticipants(&h.b, n), 0);
            for (int i = 0; i < 5; i++) {
                h.runPhase(++k, n, nworkers);
                if (HasFatalFailure()) {
                    h.stop();
                    return;
                }
            }
        }
    }
    h.stop();
    EXPECT_EQ(h.errors.load(), 0);
}

TEST(PhaseBarrierTest, ShutdownWakesParkedAndWaitingWorkers) {
    const int nworkers = 6;
    Harness h(nworkers, 2);
    h.start(nworkers);
    h.runPhase(1, 2, nworkers);
    /* Let every worker reach its sleeping wait before shutdown. */
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    h.stop(); /* joins all six: two waiting on the next phase, four parked */
    EXPECT_EQ(h.errors.load(), 0);
    EXPECT_EQ(h.ran[0], 1);
    EXPECT_EQ(h.ran[5], 0);
}
