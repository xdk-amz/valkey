/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* D+ speculative readers never wait for main, and main frees memory a reader can reach
 * at once only inside an exclusive window main has drained itself. */

#include "generated_wrappers.hpp"

#include <atomic>
#include <chrono>
#include <cstring>
#include <functional>
#include <string>
#include <thread>
#include <vector>

extern "C" {
#include "module.h"
#include "server.h"
#include "dplus.h"
extern hashtableType commandSetType;
extern int dplus_exclusive_mode; /* _Atomic(int) in C */
void createSharedObjects(void);
}

static const char *dplus_logfile = "/dev/null";

class DplusTest : public ::testing::Test {
  protected:
    static void SetUpTestSuite() {
        monotonicInit();
        memset(&server, 0, sizeof(server));
        server.hz = CONFIG_DEFAULT_HZ;
        server.logfile = const_cast<char *>(dplus_logfile);
        server.verbosity = LL_WARNING;
        server.main_thread_id = pthread_self();
        server.el = aeCreateEventLoop(1024);
        createSharedObjects();
        moduleInitModulesSystem();
        server.commands = hashtableCreate(&commandSetType);
        server.orig_commands = hashtableCreate(&commandSetType);
        populateCommandTable();
        ACLInit();
        connTypeInitialize();
        initSharedQueryBuf();

        server.dbnum = 1;
        server.db = static_cast<serverDb **>(zcalloc(sizeof(serverDb *)));
        server.db[0] = static_cast<serverDb *>(zcalloc(sizeof(serverDb)));
        server.db[0]->id = 0;
        server.db[0]->keys = kvstoreCreate(&kvstoreKeysHashtableType, 0, 0);
        server.db[0]->expires = kvstoreCreate(&kvstoreExpiresHashtableType, 0, 0);
        server.db[0]->watched_keys = dictCreate(&keylistDictType);

        server.clients = listCreate();
        server.clients_to_close = listCreate();
        server.clients_pending_write = listCreate();
        server.unblocked_clients = listCreate();
        server.ready_keys = listCreate();
        server.tracking_pending_keys = listCreate();
        server.monitors = listCreate();
        server.replicas = listCreate();
        server.pending_push_messages = listCreate();
        server.postponed_clients = listCreate();
        server.clients_waiting_acks = listCreate();
        server.clients_index = raxNew();
        server.clients_timeout_table = raxNew();
        server.errors = raxNew();

        server.io_threads_num = 2;
        dplusEnableSpeculativeReads();
    }

    static void TearDownTestSuite() {
        aeDeleteEventLoop(server.el);
        server.el = NULL;
    }

    static robj *str(const std::string &s) { return createStringObject(s.data(), s.size()); }

    static hashtable *keys() { return kvstoreGetHashtable(server.db[0]->keys, 0); }

    static unsigned shardOf(const std::string &key) {
        sds k = sdsnewlen(key.data(), key.size());
        unsigned shard = DPLUS_SHARD_INDEX(hashtableHashKey(keys(), k));
        sdsfree(k);
        return shard;
    }

    static void set(const std::string &key, const std::string &val) {
        robj *k = str(key);
        robj *v = str(val);
        setKey(NULL, server.db[0], k, &v, 0);
        decrRefCount(k);
    }

    static robj **getArgv(const std::string &key) {
        robj **argv = static_cast<robj **>(zmalloc(sizeof(robj *) * 2));
        argv[0] = str("GET");
        argv[1] = str(key);
        return argv;
    }

    /* A client whose read parsed GET first, then the queued GETs. */
    static client *readerClient(const std::string &first, const std::vector<std::string> &queued) {
        client *c = createClient(NULL);
        struct serverCommand *get = lookupCommandByCString("get");
        c->argc = 2;
        c->argv_len = 2;
        c->argv = getArgv(first);
        c->parsed_cmd = get;
        c->cmd_queue.cmds = static_cast<parsedCommand *>(zcalloc(sizeof(parsedCommand) * queued.size()));
        c->cmd_queue.cap = queued.size();
        c->cmd_queue.len = queued.size();
        for (size_t i = 0; i < queued.size(); i++) {
            parsedCommand *p = &c->cmd_queue.cmds[i];
            p->argc = 2;
            p->argv_len = 2;
            p->argv = getArgv(queued[i]);
            p->cmd = get;
        }
        __atomic_store_n(&c->spec_acl_ok, 1, __ATOMIC_RELEASE);
        return c;
    }

    /* Main's side of a shard bracket: odd while a write is in progress. */
    static void bumpShard(dplusVersionArray *va, unsigned shard) {
        __atomic_fetch_add(&va->lines[shard / 8].v[shard % 8], 1, __ATOMIC_SEQ_CST);
    }

    static int exclusiveDepth() { return __atomic_load_n(&dplus_exclusive_mode, __ATOMIC_SEQ_CST); }

    static bool waitFor(const std::function<bool()> &done, std::chrono::milliseconds limit) {
        auto deadline = std::chrono::steady_clock::now() + limit;
        while (!done()) {
            if (std::chrono::steady_clock::now() >= deadline) return false;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return true;
    }
};

/* Main holds the later GET's shard odd. The reader must end the prefix there instead of waiting:
 * main can be draining readers while it holds shards odd, as rehash completion does. */
TEST_F(DplusTest, LaterGetOnOddShardEndsThePrefix) {
    set("a", "va");
    std::string b = "b";
    for (int i = 0; shardOf(b) == shardOf("a"); i++) b = "b" + std::to_string(i);
    set(b, "vb");

    client *c = readerClient("a", {b});
    dplus_write_tax[1].history = ~(uint64_t)0; /* recent batches speculated */
    dplusVersionArray *va = hashtableGetVersionArray(keys());
    ASSERT_NE(va, nullptr);
    bumpShard(va, shardOf(b)); /* odd */

    std::atomic<int> speculated{-1};
    std::thread reader([&] { speculated = dplusSpeculateBatch(c, 1); });
    bool returned_while_odd = waitFor([&] { return speculated.load() >= 0; }, std::chrono::milliseconds(2000));
    bumpShard(va, shardOf(b)); /* even again: lets a waiting reader finish */
    reader.join();

    EXPECT_TRUE(returned_while_odd);
    EXPECT_EQ(speculated.load(), 1);
    EXPECT_TRUE(c->read_flags & READ_FLAGS_DPLUS_SPECULATED);
    EXPECT_FALSE(c->cmd_queue.cmds[0].read_flags & READ_FLAGS_DPLUS_SPECULATED);
    EXPECT_EQ(std::string(c->buf, c->bufpos), "$2\r\nva\r\n");
    freeClient(c);
}

/* A thread other than main (BIO freeing a table) must not open an exclusive window: main would
 * take it as proof that readers drained and free retired memory an active reader can still reach. */
TEST_F(DplusTest, DrainOffMainLeavesMainRetirementsDeferred) {
    uint64_t epoch;
    ASSERT_EQ(dplusDebugPinReader(&epoch), C_OK); /* an active reader */

    std::atomic<bool> left{false};
    std::thread bio([&] {
        dplusExclusiveEnter();
        dplusExclusiveLeave();
        left = true;
    });
    waitFor([&] { return left.load() || exclusiveDepth() > 0; }, std::chrono::milliseconds(1000));
    bool bio_waited_for_reader = !left.load();

    void *p = zmalloc(64);
    int deferred = dplusDeferFreeRaw(p);
    ASSERT_EQ(dplusDebugUnpinReader(), C_OK);
    bio.join();
    if (!deferred) zfree(p);

    EXPECT_FALSE(bio_waited_for_reader);
    EXPECT_EQ(deferred, 1);
    dplusReclaimRetired();
    uint64_t stats[8];
    dplusDebugEpochStats(stats);
    EXPECT_EQ(stats[1], 0u); /* nothing left retired */
}

/* Inside main's own window every reader has drained, so a retirement frees at once. */
TEST_F(DplusTest, MainExclusiveWindowFreesAtOnce) {
    dplusExclusiveEnter();
    EXPECT_EQ(exclusiveDepth(), 1);
    void *p = zmalloc(64);
    int deferred = dplusDeferFreeRaw(p);
    if (!deferred) zfree(p);
    dplusExclusiveLeave();
    EXPECT_EQ(deferred, 0);
    EXPECT_EQ(exclusiveDepth(), 0);
}

/* A module thread holding the GIL writes the keyspace like main, so its exclusive window must
 * drain readers like main's. */
TEST_F(DplusTest, ModuleThreadHoldingTheGilDrainsReaders) {
    uint64_t epoch;
    ASSERT_EQ(dplusDebugPinReader(&epoch), C_OK); /* an active reader */

    std::atomic<bool> entered{false};
    std::thread module_thread([&] {
        dplusGilWriterBegin();
        dplusExclusiveEnter();
        entered = true;
        dplusExclusiveLeave();
        dplusGilWriterEnd();
    });
    bool entered_before_drain = waitFor([&] { return entered.load(); }, std::chrono::milliseconds(200));
    ASSERT_EQ(dplusDebugUnpinReader(), C_OK);
    module_thread.join();

    EXPECT_FALSE(entered_before_drain);
    EXPECT_TRUE(entered.load());
    EXPECT_EQ(exclusiveDepth(), 0);
}

/* A unit gives each held shard one bracket however many writes land in it, and leaves the
 * other shards to their own brackets. */
TEST_F(DplusTest, UnitBracketsAHeldShardOnceAcrossWrites) {
    set("u", "0");
    std::string other = "o";
    for (int i = 0; shardOf(other) == shardOf("u"); i++) other = "o" + std::to_string(i);
    dplusVersionArray *va = hashtableGetVersionArray(keys());
    auto version = [&](const std::string &k) {
        unsigned s = shardOf(k);
        return __atomic_load_n(&va->lines[s / 8].v[s % 8], __ATOMIC_SEQ_CST);
    };
    uint64_t u0 = version("u"), o0 = version(other);

    ASSERT_EQ(dplusUnitHoldShard(va, shardOf("u")), 1);
    EXPECT_EQ(version("u"), u0 + 1); /* odd while held */
    set("u", "1");
    set("u", "2");
    set(other, "x");
    EXPECT_EQ(version("u"), u0 + 1); /* the writes inside add nothing */
    dplusUnitRelease();

    EXPECT_EQ(version("u"), u0 + 2);
    EXPECT_EQ(version(other), o0 + 2); /* bracketed by its own write */
}

/* Every pointer a walk handed to entryGetKey, to check it only ever saw entries. */
static std::vector<const void *> seen_by_getkey;

static const void *recordingGetKey(const void *entry) {
    seen_by_getkey.push_back(entry);
    return entry;
}

/* Entries that are aligned like robj pointers, with distinct high hash bits from 'target'. */
static std::vector<void *> entriesAvoidingHashOf(hashtable *ht, void *target, size_t n) {
    std::vector<void *> out;
    uint8_t h2 = hashtableHashKey(ht, target) >> 56;
    for (uintptr_t v = 64; out.size() < n; v += 64) {
        void *e = reinterpret_cast<void *>(v);
        if (e != target && (uint8_t)(hashtableHashKey(ht, e) >> 56) != h2) out.push_back(e);
    }
    return out;
}

/* A walk that already chose a bucket's last slot as its candidate reads that slot after main
 * chains the bucket, which puts the child pointer there. It must not take the child for an
 * entry, and must still find the moved entry in the child. */
TEST_F(DplusTest, WalkRacingChainConversionNeverTakesTheChildForAnEntry) {
    hashtableSetResizePolicy(HASHTABLE_RESIZE_AVOID);
    hashtableType type = {};
    type.entryGetKey = recordingGetKey;
    type.speculative_reads = 1;
    hashtable *ht = hashtableCreate(&type);
    ASSERT_NE(hashtableGetVersionArray(ht), nullptr);

    void *target = reinterpret_cast<void *>(uintptr_t(1) << 20);
    std::vector<void *> others = entriesAvoidingHashOf(ht, target, 7);
    for (int i = 0; i < 6; i++) ASSERT_TRUE(hashtableAdd(ht, others[i]));
    ASSERT_TRUE(hashtableAdd(ht, target)); /* the last slot of the only bucket */
    ASSERT_EQ(hashtableBuckets(ht), 1u);

    hashtableIncrementalFindState st;
    hashtableIncrementalFindInit(&st, ht, target);
    ASSERT_TRUE(hashtableIncrementalFindStep(&st)); /* bucket chosen */
    ASSERT_TRUE(hashtableIncrementalFindStep(&st)); /* the last slot is the only candidate */

    ASSERT_TRUE(hashtableAdd(ht, others[6])); /* main chains the full bucket */

    seen_by_getkey.clear();
    while (hashtableIncrementalFindStep(&st));
    void *found = NULL;
    EXPECT_TRUE(hashtableIncrementalFindGetResult(&st, &found));
    EXPECT_EQ(found, target);
    for (const void *p : seen_by_getkey) {
        bool known = p == target;
        for (void *o : others) known = known || p == o;
        EXPECT_TRUE(known) << "entryGetKey was given " << p << ", which is no entry";
    }

    hashtableRelease(ht);
    hashtableSetResizePolicy(HASHTABLE_RESIZE_ALLOW);
}

/* Removing an emptied middle bucket from a chain must not free it while a walk may be in it. */
TEST_F(DplusTest, ChainCompactionDefersTheRemovedBucket) {
    hashtableSetResizePolicy(HASHTABLE_RESIZE_AVOID);
    hashtableType type = {};
    type.speculative_reads = 1;
    hashtable *ht = hashtableCreate(&type);
    ASSERT_NE(hashtableGetVersionArray(ht), nullptr);

    /* One chain: 6 entries in the first bucket, 6 in the middle one, 2 in the last. */
    std::vector<void *> e;
    for (uintptr_t i = 1; i <= 14; i++) e.push_back(reinterpret_cast<void *>(i * 64));
    for (void *p : e) ASSERT_TRUE(hashtableAdd(ht, p));
    ASSERT_EQ(hashtableBuckets(ht), 1u);
    ASSERT_EQ(hashtableChainedBuckets(ht, 0), 2u);

    uint64_t stats[8];
    dplusDebugEpochStats(stats);
    uint64_t retired_before = stats[1] + stats[2];

    /* Empty the middle bucket while a safe iterator pauses compaction; leaving the chain compacts it. */
    hashtableIterator iter;
    hashtableInitIterator(&iter, ht, HASHTABLE_ITER_SAFE);
    void *entry;
    while (hashtableNext(&iter, &entry)) {
        for (int i = 6; i < 12; i++) {
            if (entry == e[i]) {
                ASSERT_TRUE(hashtableDelete(ht, entry));
            }
        }
    }
    hashtableCleanupIterator(&iter);
    EXPECT_EQ(hashtableChainedBuckets(ht, 0), 1u);

    dplusDebugEpochStats(stats);
    EXPECT_EQ(stats[1] + stats[2], retired_before + 1); /* the removed bucket waits for readers */
    for (int i = 0; i < 14; i++) {
        void *found = NULL;
        EXPECT_EQ(hashtableFind(ht, e[i], &found), i < 6 || i >= 12);
    }

    hashtableRelease(ht);
    dplusReclaimRetired();
    hashtableSetResizePolicy(HASHTABLE_RESIZE_ALLOW);
}

/* The reader reads a walk's result after the walk ended. If main reused the matched slot in
 * between, the result must still be the entry that matched, not whatever the slot holds now. */
TEST_F(DplusTest, WalkResultIsTheEntryThatMatched) {
    hashtableSetResizePolicy(HASHTABLE_RESIZE_AVOID);
    hashtableType type = {};
    type.speculative_reads = 1;
    hashtable *ht = hashtableCreate(&type);

    void *target = reinterpret_cast<void *>(uintptr_t(1) << 20);
    std::vector<void *> others = entriesAvoidingHashOf(ht, target, 7);
    for (int i = 0; i < 6; i++) ASSERT_TRUE(hashtableAdd(ht, others[i]));
    ASSERT_TRUE(hashtableAdd(ht, target));

    hashtableIncrementalFindState st;
    hashtableIncrementalFindInit(&st, ht, target);
    while (hashtableIncrementalFindStep(&st));

    ASSERT_TRUE(hashtableDelete(ht, target));
    ASSERT_TRUE(hashtableAdd(ht, others[6])); /* takes the freed slot */

    void *found = NULL;
    EXPECT_TRUE(hashtableIncrementalFindGetResult(&st, &found));
    EXPECT_EQ(found, target);

    hashtableRelease(ht);
    hashtableSetResizePolicy(HASHTABLE_RESIZE_ALLOW);
}
