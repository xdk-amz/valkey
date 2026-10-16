/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "generated_wrappers.hpp"

extern "C" {
#include "kvstore.h"
}

uint64_t hashTestCallback(const void *key) {
    return hashtableGenHashFunction((const char *)key, strlen((const char *)key));
}

uint64_t hashConflictTestCallback(const void *key) {
    UNUSED(key);
    return 0;
}

int cmpTestCallback(const void *k1, const void *k2) {
    return strcmp((const char *)k1, (const char *)k2) == 0;
}

void freeTestCallback(void *val) {
    zfree(val);
}

/* Hashtable types used for tests - initialized in SetUpTestSuite */
static hashtableType KvstoreHashtableTestType;
static hashtableType KvstoreConflictHashtableTestType;

char *stringFromInt(int value) {
    char buf[32];
    int len;
    char *s;

    len = snprintf(buf, sizeof(buf), "%d", value);
    s = (char *)zmalloc(len + 1);
    memcpy(s, buf, len);
    s[len] = '\0';
    return s;
}

class KvstoreTest : public ::testing::Test {
  protected:
    static void SetUpTestSuite() {
        monotonicInit();

        /* Initialize KvstoreHashtableTestType explicitly by field name to avoid
         * dependency on field order (designated initializers require C++20). */
        memset(&KvstoreHashtableTestType, 0, sizeof(KvstoreHashtableTestType));
        KvstoreHashtableTestType.hashFunction = hashTestCallback;
        KvstoreHashtableTestType.keyCompare = cmpTestCallback;
        KvstoreHashtableTestType.entryDestructor = freeTestCallback;
        KvstoreHashtableTestType.rehashingStarted = kvstoreHashtableRehashingStarted;
        KvstoreHashtableTestType.rehashingCompleted = kvstoreHashtableRehashingCompleted;
        KvstoreHashtableTestType.trackMemUsage = kvstoreHashtableTrackMemUsage;
        KvstoreHashtableTestType.getMetadataSize = kvstoreHashtableMetadataSize;

        /* Initialize KvstoreConflictHashtableTestType */
        memset(&KvstoreConflictHashtableTestType, 0, sizeof(KvstoreConflictHashtableTestType));
        KvstoreConflictHashtableTestType.hashFunction = hashConflictTestCallback;
        KvstoreConflictHashtableTestType.keyCompare = cmpTestCallback;
        KvstoreConflictHashtableTestType.entryDestructor = freeTestCallback;
        KvstoreConflictHashtableTestType.rehashingStarted = kvstoreHashtableRehashingStarted;
        KvstoreConflictHashtableTestType.rehashingCompleted = kvstoreHashtableRehashingCompleted;
        KvstoreConflictHashtableTestType.trackMemUsage = kvstoreHashtableTrackMemUsage;
        KvstoreConflictHashtableTestType.getMetadataSize = kvstoreHashtableMetadataSize;
    }
};

TEST_F(KvstoreTest, kvstoreAdd16Keys) {
    int i;

    int didx = 0;
    kvstore *kvs0 = kvstoreCreate(&KvstoreHashtableTestType, 0, 0);
    kvstore *kvs1 = kvstoreCreate(&KvstoreHashtableTestType, 0, KVSTORE_ALLOCATE_HASHTABLES_ON_DEMAND);
    kvstore *kvs2 = kvstoreCreate(&KvstoreHashtableTestType, 0, KVSTORE_ALLOCATE_HASHTABLES_ON_DEMAND | KVSTORE_FREE_EMPTY_HASHTABLES);

    for (i = 0; i < 16; i++) {
        ASSERT_TRUE(kvstoreHashtableAdd(kvs0, didx, stringFromInt(i)));
        ASSERT_TRUE(kvstoreHashtableAdd(kvs1, didx, stringFromInt(i)));
        ASSERT_TRUE(kvstoreHashtableAdd(kvs2, didx, stringFromInt(i)));
    }
    ASSERT_EQ(kvstoreHashtableSize(kvs0, didx), 16u);
    ASSERT_EQ(kvstoreSize(kvs0), 16u);
    ASSERT_EQ(kvstoreHashtableSize(kvs1, didx), 16u);
    ASSERT_EQ(kvstoreSize(kvs1), 16u);
    ASSERT_EQ(kvstoreHashtableSize(kvs2, didx), 16u);
    ASSERT_EQ(kvstoreSize(kvs2), 16u);

    kvstoreRelease(kvs0);
    kvstoreRelease(kvs1);
    kvstoreRelease(kvs2);
}

TEST_F(KvstoreTest, kvstoreIteratorRemoveAllKeysNoDeleteEmptyHashtable) {
    hashtableType *type[] = {
        &KvstoreHashtableTestType,
        &KvstoreConflictHashtableTestType,
        nullptr,
    };

    for (int t = 0; type[t] != nullptr; t++) {
        hashtableType *testType = type[t];
        printf("Testing %d hashtableType\n", t);

        int i;
        void *key;
        kvstoreIterator *kvs_it;

        int didx = 0;
        int curr_slot = 0;
        kvstore *kvs1 = kvstoreCreate(testType, 0, KVSTORE_ALLOCATE_HASHTABLES_ON_DEMAND);

        for (i = 0; i < 16; i++) {
            ASSERT_TRUE(kvstoreHashtableAdd(kvs1, didx, stringFromInt(i)));
        }

        kvs_it = kvstoreIteratorInit(kvs1, HASHTABLE_ITER_SAFE);
        while (kvstoreIteratorNext(kvs_it, &key)) {
            curr_slot = kvstoreIteratorGetCurrentHashtableIndex(kvs_it);
            ASSERT_TRUE(kvstoreHashtableDelete(kvs1, curr_slot, key));
        }
        kvstoreIteratorRelease(kvs_it);

        hashtable *ht = kvstoreGetHashtable(kvs1, didx);
        ASSERT_NE(ht, nullptr);
        ASSERT_EQ(kvstoreHashtableSize(kvs1, didx), 0u);
        ASSERT_EQ(kvstoreSize(kvs1), 0u);

        kvstoreRelease(kvs1);
    }
}

TEST_F(KvstoreTest, kvstoreIteratorRemoveAllKeysDeleteEmptyHashtable) {
    int i;
    void *key;
    kvstoreIterator *kvs_it;

    int didx = 0;
    int curr_slot = 0;
    kvstore *kvs2 = kvstoreCreate(&KvstoreHashtableTestType, 0, KVSTORE_ALLOCATE_HASHTABLES_ON_DEMAND | KVSTORE_FREE_EMPTY_HASHTABLES);

    for (i = 0; i < 16; i++) {
        ASSERT_TRUE(kvstoreHashtableAdd(kvs2, didx, stringFromInt(i)));
    }

    kvs_it = kvstoreIteratorInit(kvs2, HASHTABLE_ITER_SAFE);
    while (kvstoreIteratorNext(kvs_it, &key)) {
        curr_slot = kvstoreIteratorGetCurrentHashtableIndex(kvs_it);
        ASSERT_TRUE(kvstoreHashtableDelete(kvs2, curr_slot, key));
    }
    kvstoreIteratorRelease(kvs_it);

    /* Make sure the hashtable was removed from the rehashing list. */
    while (kvstoreIncrementallyRehash(kvs2, 1000)) {
    }

    hashtable *ht = kvstoreGetHashtable(kvs2, didx);
    ASSERT_EQ(ht, nullptr);
    ASSERT_EQ(kvstoreHashtableSize(kvs2, didx), 0u);
    ASSERT_EQ(kvstoreSize(kvs2), 0u);

    kvstoreRelease(kvs2);
}

TEST_F(KvstoreTest, kvstoreHashtableIteratorRemoveAllKeysNoDeleteEmptyHashtable) {
    int i;
    void *key;
    kvstoreHashtableIterator *kvs_di;

    int didx = 0;
    kvstore *kvs1 = kvstoreCreate(&KvstoreHashtableTestType, 0, KVSTORE_ALLOCATE_HASHTABLES_ON_DEMAND);

    for (i = 0; i < 16; i++) {
        ASSERT_TRUE(kvstoreHashtableAdd(kvs1, didx, stringFromInt(i)));
    }

    kvs_di = kvstoreGetHashtableIterator(kvs1, didx, HASHTABLE_ITER_SAFE);
    while (kvstoreHashtableIteratorNext(kvs_di, &key)) {
        ASSERT_TRUE(kvstoreHashtableDelete(kvs1, didx, key));
    }
    kvstoreReleaseHashtableIterator(kvs_di);

    hashtable *ht = kvstoreGetHashtable(kvs1, didx);
    ASSERT_NE(ht, nullptr);
    ASSERT_EQ(kvstoreHashtableSize(kvs1, didx), 0u);
    ASSERT_EQ(kvstoreSize(kvs1), 0u);

    kvstoreRelease(kvs1);
}

TEST_F(KvstoreTest, kvstoreHashtableIteratorRemoveAllKeysDeleteEmptyHashtable) {
    int i;
    void *key;
    kvstoreHashtableIterator *kvs_di;

    int didx = 0;
    kvstore *kvs2 = kvstoreCreate(&KvstoreHashtableTestType, 0, KVSTORE_ALLOCATE_HASHTABLES_ON_DEMAND | KVSTORE_FREE_EMPTY_HASHTABLES);

    for (i = 0; i < 16; i++) {
        ASSERT_TRUE(kvstoreHashtableAdd(kvs2, didx, stringFromInt(i)));
    }

    kvs_di = kvstoreGetHashtableIterator(kvs2, didx, HASHTABLE_ITER_SAFE);
    while (kvstoreHashtableIteratorNext(kvs_di, &key)) {
        ASSERT_TRUE(kvstoreHashtableDelete(kvs2, didx, key));
    }
    kvstoreReleaseHashtableIterator(kvs_di);

    hashtable *ht = kvstoreGetHashtable(kvs2, didx);
    ASSERT_EQ(ht, nullptr);
    ASSERT_EQ(kvstoreHashtableSize(kvs2, didx), 0u);
    ASSERT_EQ(kvstoreSize(kvs2), 0u);

    kvstoreRelease(kvs2);
}

TEST_F(KvstoreTest, kvstoreHashtableExpand) {
    kvstore *kvs = kvstoreCreate(&KvstoreHashtableTestType, 0, KVSTORE_ALLOCATE_HASHTABLES_ON_DEMAND | KVSTORE_FREE_EMPTY_HASHTABLES);

    ASSERT_EQ(kvstoreGetHashtable(kvs, 0), nullptr);
    ASSERT_TRUE(kvstoreHashtableExpand(kvs, 0, 10000));
    ASSERT_NE(kvstoreGetHashtable(kvs, 0), nullptr);
    ASSERT_GT(kvstoreBuckets(kvs), 0u);
    ASSERT_EQ(kvstoreBuckets(kvs), kvstoreHashtableBuckets(kvs, 0));

    kvstoreRelease(kvs);
}

/* Apply the same per-table operations serially (reference) and concurrently (N threads over
 * disjoint tables, then kvstoreReconcile), and assert every kvstore-wide aggregate matches. */
extern "C" {
#include <pthread.h>
}

namespace {
struct LaneArg {
    kvstore *kvs;
    int first_didx;
    int last_didx;
    int keys_per_table;
};

/* Each key is "<didx>:<n>"; adding by didx keeps a thread on its own tables. */
char *tableKey(int didx, int n) {
    char buf[48];
    int len = snprintf(buf, sizeof(buf), "%d:%d", didx, n);
    char *s = (char *)zmalloc(len + 1);
    memcpy(s, buf, len + 1);
    return s;
}

/* Insert keys_per_table keys into each owned table, delete every third, then rehash to completion. */
void runTableWork(kvstore *kvs, int first, int last, int kpt) {
    for (int didx = first; didx <= last; didx++) {
        for (int n = 0; n < kpt; n++) kvstoreHashtableAdd(kvs, didx, tableKey(didx, n));
        for (int n = 0; n < kpt; n += 3) {
            void *popped = nullptr;
            char *k = tableKey(didx, n);
            if (kvstoreHashtablePop(kvs, didx, k, &popped)) zfree(popped);
            zfree(k);
        }
        kvstoreTryResizeHashtable(kvs, didx);
        while (kvstoreRehashHashtable(kvs, didx, 1000) > 0) {
        }
    }
}

void *laneThread(void *arg) {
    LaneArg *a = (LaneArg *)arg;
    runTableWork(a->kvs, a->first_didx, a->last_didx, a->keys_per_table);
    return nullptr;
}
} /* namespace */

TEST_F(KvstoreTest, kvstoreConcurrentReconcileMatchesSerial) {
    const int bits = 3; /* 8 tables */
    const int ntables = 1 << bits;
    const int nthreads = 4;
    const int kpt = 500;

    kvstore *serial = kvstoreCreate(&KvstoreHashtableTestType, bits, KVSTORE_FREE_EMPTY_HASHTABLES);
    kvstore *conc = kvstoreCreate(&KvstoreHashtableTestType, bits, KVSTORE_FREE_EMPTY_HASHTABLES);

    /* Reference: do all tables serially on main. */
    runTableWork(serial, 0, ntables - 1, kpt);

    /* Concurrent: nthreads each own a contiguous, disjoint band of tables. */
    kvstoreSetConcurrent(conc, 1);
    pthread_t th[nthreads];
    LaneArg args[nthreads];
    int per = ntables / nthreads;
    for (int t = 0; t < nthreads; t++) {
        args[t] = LaneArg{conc, t * per, (t + 1) * per - 1, kpt};
        ASSERT_EQ(pthread_create(&th[t], nullptr, laneThread, &args[t]), 0);
    }
    for (int t = 0; t < nthreads; t++) pthread_join(th[t], nullptr);
    kvstoreSetConcurrent(conc, 0);
    kvstoreReconcile(conc);

    /* Kvstore-wide aggregates must be identical. */
    ASSERT_EQ(kvstoreSize(conc), kvstoreSize(serial));
    ASSERT_EQ(kvstoreBuckets(conc), kvstoreBuckets(serial));
    ASSERT_EQ(kvstoreNumNonEmptyHashtables(conc), kvstoreNumNonEmptyHashtables(serial));
    ASSERT_EQ(kvstoreNumAllocatedHashtables(conc), kvstoreNumAllocatedHashtables(serial));
    ASSERT_EQ(kvstoreHashtableRehashingCount(conc), kvstoreHashtableRehashingCount(serial));
    ASSERT_EQ(kvstoreOverheadHashtableLut(conc), kvstoreOverheadHashtableLut(serial));
    ASSERT_EQ(kvstoreOverheadHashtableRehashing(conc), kvstoreOverheadHashtableRehashing(serial));

    /* Fenwick index: every cumulative lookup resolves to the same table in both. */
    for (unsigned long target = 1; target <= kvstoreSize(serial); target += 7) {
        ASSERT_EQ(kvstoreFindHashtableIndexByKeyIndex(conc, target),
                  kvstoreFindHashtableIndexByKeyIndex(serial, target));
    }
    /* Per-table sizes match. */
    for (int didx = 0; didx < ntables; didx++) {
        ASSERT_EQ(kvstoreHashtableSize(conc, didx), kvstoreHashtableSize(serial, didx));
    }

    kvstoreRelease(serial);
    kvstoreRelease(conc);
}

/* A table emptied entirely in concurrent mode is deferred-freed by reconcile, not by the lane. */
TEST_F(KvstoreTest, kvstoreConcurrentDeferredEmptyFree) {
    const int bits = 2; /* 4 tables */
    kvstore *kvs = kvstoreCreate(&KvstoreHashtableTestType, bits, KVSTORE_FREE_EMPTY_HASHTABLES);
    for (int didx = 0; didx < 4; didx++)
        for (int n = 0; n < 50; n++) kvstoreHashtableAdd(kvs, didx, tableKey(didx, n));

    kvstoreSetConcurrent(kvs, 1);
    for (int n = 0; n < 50; n++) {
        void *popped = nullptr;
        char *k = tableKey(0, n);
        if (kvstoreHashtablePop(kvs, 0, k, &popped)) zfree(popped);
        zfree(k);
    }
    /* The lane did not free table 0: it still exists until reconcile. */
    ASSERT_NE(kvstoreGetHashtable(kvs, 0), nullptr);
    ASSERT_EQ(kvstoreHashtableSize(kvs, 0), 0u);
    kvstoreSetConcurrent(kvs, 0);
    kvstoreReconcile(kvs);

    ASSERT_EQ(kvstoreGetHashtable(kvs, 0), nullptr);
    ASSERT_EQ(kvstoreNumNonEmptyHashtables(kvs), 3);
    ASSERT_EQ(kvstoreSize(kvs), 150u);

    kvstoreRelease(kvs);
}

/* A table that finishes one rehash and starts the next inside one concurrent phase stays on the
 * rehashing list, so the serial cron driver still finds it. */
TEST_F(KvstoreTest, kvstoreConcurrentRehashRestartKeepsListNode) {
    kvstore *kvs = kvstoreCreate(&KvstoreHashtableTestType, 1, 0);
    int n = 0;
    while (!kvstoreHashtableRehashingAt(kvs, 0)) ASSERT_TRUE(kvstoreHashtableAdd(kvs, 0, stringFromInt(n++)));
    ASSERT_EQ(kvstoreHashtableRehashingCount(kvs), 1u);

    kvstoreSetConcurrent(kvs, 1);
    kvstoreRehashHashtable(kvs, 0, 1000000);
    ASSERT_FALSE(kvstoreHashtableRehashingAt(kvs, 0));
    while (!kvstoreHashtableRehashingAt(kvs, 0)) ASSERT_TRUE(kvstoreHashtableAdd(kvs, 0, stringFromInt(n++)));
    kvstoreSetConcurrent(kvs, 0);
    kvstoreReconcile(kvs);

    ASSERT_EQ(kvstoreHashtableRehashingCount(kvs), 1u);
    ASSERT_EQ(kvstoreSize(kvs), (unsigned long long)n);
    while (kvstoreHashtableRehashingCount(kvs)) kvstoreIncrementallyRehash(kvs, 1000000);
    ASSERT_FALSE(kvstoreHashtableRehashingAt(kvs, 0));
    kvstoreRelease(kvs);
}

/* Cluster-sized kvstore (one table per slot, tables created and freed on demand): threads work on a few
 * spread-out tables over many phases, and reconcile keeps every aggregate equal to a serial run. Also
 * prints the time main spends reconciling a phase that touched one table. */
TEST_F(KvstoreTest, kvstoreConcurrentReconcileSlotSized) {
    const int bits = 14;
    const int flags = KVSTORE_ALLOCATE_HASHTABLES_ON_DEMAND | KVSTORE_FREE_EMPTY_HASHTABLES;
    const int nthreads = 4;
    const int kpt = 300;
    const int stride = (1 << bits) / nthreads;

    kvstore *serial = kvstoreCreate(&KvstoreHashtableTestType, bits, flags);
    kvstore *conc = kvstoreCreate(&KvstoreHashtableTestType, bits, flags);

    for (int phase = 0; phase < 3; phase++) {
        /* Each thread owns two tables at the start of its stride, shifted per phase. */
        for (int t = 0; t < nthreads; t++) runTableWork(serial, t * stride + phase, t * stride + phase + 1, kpt);

        kvstoreSetConcurrent(conc, 1);
        pthread_t th[nthreads];
        LaneArg args[nthreads];
        for (int t = 0; t < nthreads; t++) {
            args[t] = LaneArg{conc, t * stride + phase, t * stride + phase + 1, kpt};
            ASSERT_EQ(pthread_create(&th[t], nullptr, laneThread, &args[t]), 0);
        }
        for (int t = 0; t < nthreads; t++) pthread_join(th[t], nullptr);
        kvstoreSetConcurrent(conc, 0);
        kvstoreReconcile(conc);

        ASSERT_EQ(kvstoreSize(conc), kvstoreSize(serial));
        ASSERT_EQ(kvstoreBuckets(conc), kvstoreBuckets(serial));
        ASSERT_EQ(kvstoreNumNonEmptyHashtables(conc), kvstoreNumNonEmptyHashtables(serial));
        ASSERT_EQ(kvstoreNumAllocatedHashtables(conc), kvstoreNumAllocatedHashtables(serial));
        ASSERT_EQ(kvstoreOverheadHashtableLut(conc), kvstoreOverheadHashtableLut(serial));
        ASSERT_EQ(kvstoreOverheadHashtableRehashing(conc), kvstoreOverheadHashtableRehashing(serial));
        for (unsigned long target = 1; target <= kvstoreSize(serial); target += 11) {
            ASSERT_EQ(kvstoreFindHashtableIndexByKeyIndex(conc, target),
                      kvstoreFindHashtableIndexByKeyIndex(serial, target));
        }
    }

    /* A table emptied in a phase is freed by reconcile. */
    kvstoreSetConcurrent(conc, 1);
    for (int n = 0; n < kpt; n++) {
        void *popped = nullptr;
        char *k = tableKey(0, n);
        if (kvstoreHashtablePop(conc, 0, k, &popped)) zfree(popped);
        zfree(k);
    }
    kvstoreSetConcurrent(conc, 0);
    kvstoreReconcile(conc);
    ASSERT_EQ(kvstoreGetHashtable(conc, 0), nullptr);

    /* Main's cost per phase that touched one table. */
    const int rounds = 2000;
    monotime start = getMonotonicUs();
    for (int r = 0; r < rounds; r++) {
        kvstoreSetConcurrent(conc, 1);
        char *k = tableKey(stride, 100000 + r);
        kvstoreHashtableAdd(conc, stride, k);
        kvstoreSetConcurrent(conc, 0);
        kvstoreReconcile(conc);
    }
    uint64_t us = getMonotonicUs() - start;
    printf("[ reconcile ] %d-table kvstore, one table touched per phase: %.3f us per phase\n", 1 << bits,
           (double)us / rounds);
    ASSERT_EQ(kvstoreHashtableSize(conc, stride), (unsigned long)(kpt - (kpt + 2) / 3 + rounds));

    kvstoreRelease(serial);
    kvstoreRelease(conc);
}

/* A kvstore outside the lane phase (a database a bio thread is releasing) keeps updating its own
 * fields while another kvstore is in concurrent mode, so releasing it does not leave unreconciled
 * deltas behind. */
TEST_F(KvstoreTest, kvstoreConcurrentModeIsPerKvstore) {
    kvstore *live = kvstoreCreate(&KvstoreHashtableTestType, 2, KVSTORE_FREE_EMPTY_HASHTABLES);
    kvstore *old = kvstoreCreate(&KvstoreHashtableTestType, 2, KVSTORE_FREE_EMPTY_HASHTABLES);
    for (int didx = 0; didx < 4; didx++)
        for (int n = 0; n < 200; n++) kvstoreHashtableAdd(old, didx, tableKey(didx, n));
    ASSERT_EQ(kvstoreSize(old), 800u);

    kvstoreSetConcurrent(live, 1);
    for (int n = 0; n < 200; n++) {
        void *popped = nullptr;
        char *k = tableKey(1, n);
        if (kvstoreHashtablePop(old, 1, k, &popped)) zfree(popped);
        zfree(k);
    }
    ASSERT_EQ(kvstoreSize(old), 600u);
    ASSERT_EQ(kvstoreGetHashtable(old, 1), nullptr);
    kvstoreHashtableAdd(live, 3, tableKey(3, 0));
    kvstoreRelease(old);
    kvstoreSetConcurrent(live, 0);
    kvstoreReconcile(live);
    ASSERT_EQ(kvstoreSize(live), 1u);
    kvstoreRelease(live);
}
