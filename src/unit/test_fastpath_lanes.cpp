/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Write lanes driven inline on one thread: thread 1's read phase classifies entries onto lanes, main's
 * write phase runs a lane phase (every lane runs here, lane by lane, through testOnlyStagedSetWorkers),
 * then thread 1's reply phase assembles split replies. Covers the atomicity of a split MSET and a split
 * MGET (fpTrySplit / fpSplitAssemble): a reader never sees half of an MSET, replies come back in key
 * order, the serial fallback of a small segment gives the same answers as the lanes, every multi-key lane
 * command with keys in two shards runs as a barrier, and a client's own ACL change orders its later SET.
 *
 * Ported from the audit stack on the original superset. The tests of that stack's generation guards
 * (the ACL epoch re-check, the key-observer record and the primary's window re-check) are not here:
 * this branch has no such guards; its loop keeps the window between classification and the lanes empty
 * on a primary, and the replica re-checks observed keys right before each planned segment. */

#include "generated_wrappers.hpp"
#include "test_server_init.hpp"

#include <cstdarg>
#include <cstring>
#include <string>
#include <vector>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

extern "C" {
#include "cluster.h"
#include "connection.h"
#include "fastpath.h"
#include "io_threads.h"
#include "module.h"
#include "poller.h"
#include "server.h"
#include "staged_loop.h"
extern hashtableType commandSetType;
extern dictType keylistDictType;
extern dictType objToHashtableDictType;
extern dictType objectKeyPointerValueDictType;
void createSharedObjects(void);
}

static const char *ln_logfile = "/dev/null";

/* Keys and their lane with keyspace-shard-bits 4 and two lanes (lane = shard % 2), chosen at suite start
 * from this build's table hash (the hash of short keys differs between branches): two keys on lane 0 and
 * three on lane 1, all in distinct shards. The fixture asserts the lanes after picking them. */
#define LANE_BITS 4
static char ln_key_store[5][16];
static const char *K_L0A = ln_key_store[0], *K_L0B = ln_key_store[1]; /* lane 0 */
static const char *K_L1A = ln_key_store[2], *K_L1B = ln_key_store[3], *K_L1C = ln_key_store[4]; /* lane 1 */

class FastpathLanesTest : public ::testing::Test {
  protected:
    static void SetUpTestSuite() {
        monotonicInit();
        memset(&server, 0, sizeof(server));
        server.hz = CONFIG_DEFAULT_HZ;
        server.logfile = const_cast<char *>(ln_logfile);
        server.verbosity = LL_WARNING;
        server.main_thread_id = pthread_self();
        server.client_max_querybuf_len = 1024ll * 1024 * 1024;
        server.proto_max_bulk_len = 512ll * 1024 * 1024;
        server.maxclients = 1000;
        server.el = aeCreateEventLoop(1024);
        createSharedObjects();
        unitTestInitModulesSystem();
        server.commands = hashtableCreate(&commandSetType);
        server.orig_commands = hashtableCreate(&commandSetType);
        unitTestPopulateCommandTable();
        unitTestACLInit();

        server.keyspace_shard_bits = LANE_BITS;
        server.dbnum = 1;
        server.db = static_cast<serverDb **>(zcalloc(sizeof(serverDb *)));
        server.db[0] = static_cast<serverDb *>(zcalloc(sizeof(serverDb)));
        server.db[0]->id = 0;
        server.db[0]->keys = kvstoreCreate(&kvstoreKeysHashtableType, LANE_BITS, KVSTORE_ALLOCATE_HASHTABLES_ON_DEMAND);
        server.db[0]->expires = kvstoreCreate(&kvstoreExpiresHashtableType, LANE_BITS, KVSTORE_ALLOCATE_HASHTABLES_ON_DEMAND);
        server.db[0]->keys_with_volatile_items =
            kvstoreCreate(&kvstoreExpiresHashtableType, LANE_BITS, KVSTORE_ALLOCATE_HASHTABLES_ON_DEMAND);
        server.db[0]->watched_keys = dictCreate(&keylistDictType);
        server.db[0]->blocking_keys = dictCreate(&keylistDictType);
        server.db[0]->blocking_keys_unblock_on_nokey = dictCreate(&objectKeyPointerValueDictType);
        server.db[0]->ready_keys = dictCreate(&objectKeyPointerValueDictType);
        /* ACL SETUSER looks at the pubsub tables to decide whether to kill subscribers. */
        server.pubsub_channels = kvstoreCreate(&kvstoreChannelHashtableType, 0, KVSTORE_ALLOCATE_HASHTABLES_ON_DEMAND);
        server.pubsub_patterns = dictCreate(&objToHashtableDictType);
        server.pubsubshard_channels = kvstoreCreate(&kvstoreChannelHashtableType, 0, KVSTORE_ALLOCATE_HASHTABLES_ON_DEMAND);

        server.clients = listCreate();
        server.clients_to_close = listCreate();
        server.clients_pending_write = listCreate();
        server.clients_pending_read = listCreate();
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
        for (int i = 0; i < COMMANDLOG_TYPE_NUM; i++) server.commandlog[i].threshold = -1;

        server.io_threads_num = 2;
        server.io_threads_fast_path = 1;
        server.io_threads_strict_offload = 1;
        server.io_threads_parallel_writes = 1;
        server.io_threads_parallel_writes_min = 1; /* every segment runs on the lanes */
        server.io_batch_commands = 16;
        unitTestConnTypeInitialize();
        initSharedQueryBuf();
        testOnlyInitIOThreadQueues();
        fastpathInitThread(1);
        testOnlySetIOThreadReady(1, epoll_create1(EPOLL_CLOEXEC));
    }

    static void TearDownTestSuite() {
        fastpathFreeThread(1);
        testOnlyClearIOThreadReady(1);
        testOnlyFreeIOThreadQueues();
        unitTestDeleteEventLoop();
    }

    void SetUp() override {
        testOnlyInitIOThreadQueues();
        ASSERT_EQ(testOnlyStagedStart(), 0);
        testOnlyStagedSetWorkers(1, 1); /* main lane 0, IO thread 1 lane 1 */
        testOnlyFastpathSetLanes(2); /* the lane gate counts lanes from the lane executor, which does not run here */
        server.io_threads_parallel_writes_min = 1;
        pickKeys();
        ASSERT_EQ(laneOf(K_L0A), 0);
        ASSERT_EQ(laneOf(K_L0B), 0);
        ASSERT_EQ(laneOf(K_L1A), 1);
        ASSERT_EQ(laneOf(K_L1B), 1);
        ASSERT_EQ(laneOf(K_L1C), 1);
        emptyDbStructure(server.db, -1, 0, NULL);
    }

    void TearDown() override {
        testOnlyFastpathSetLanes(0);
        testOnlyStagedStop();
        freeClientsInAsyncFreeQueue();
    }

    static int shardOf(const char *key) {
        sds k = sdsnew(key);
        int shard = getKVStoreIndexForKey(k);
        sdsfree(k);
        return shard;
    }

    static int laneOf(const char *key) { return shardOf(key) % 2; }

    /* Fill the key slots with the first short keys "k0", "k1", ... that give two keys on lane 0 and three
     * on lane 1, in distinct shards. Deterministic for one build of the hash. */
    static void pickKeys(void) {
        static const int slots[2][3] = {{0, 1, -1}, {2, 3, 4}};
        static const int want[2] = {2, 3};
        int found[2] = {0, 0};
        bool used[1 << LANE_BITS] = {false};
        for (int i = 0; (found[0] < want[0] || found[1] < want[1]) && i < 1000; i++) {
            char name[16];
            snprintf(name, sizeof(name), "k%d", i);
            int shard = shardOf(name);
            int lane = shard % 2;
            if (found[lane] == want[lane] || used[shard]) continue;
            used[shard] = true;
            snprintf(ln_key_store[slots[lane][found[lane]]], sizeof(ln_key_store[0]), "%s", name);
            found[lane]++;
        }
        ASSERT_EQ(found[0], want[0]);
        ASSERT_EQ(found[1], want[1]);
    }

    static client *newStagedClient(int *peer) {
        int sv[2];
        EXPECT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
        connection *conn = connCreateAccepted(connectionByType(CONN_TYPE_SOCKET), sv[0], NULL);
        conn->state = CONN_STATE_CONNECTED;
        client *c = createClient(conn);
        struct sockaddr_in sa;
        memset(&sa, 0, sizeof(sa));
        sa.sin_family = AF_INET;
        sa.sin_port = htons(40000);
        sa.sin_addr.s_addr = htonl(0x7f000001);
        EXPECT_EQ(peerIdentityFromSockaddr(&c->fp_peer, (struct sockaddr *)&sa, sizeof(sa)), C_OK);
        c->fp_local = c->fp_peer;
        EXPECT_EQ(fastpathAttach(c), C_OK);
        EXPECT_EQ(c->flag.fastpath, 1u);
        *peer = sv[1];
        return c;
    }

    static void send(int peer, const std::string &req) {
        EXPECT_EQ(write(peer, req.data(), req.size()), (ssize_t)req.size());
    }

    static std::string recv(int peer) {
        std::string out;
        char buf[65536];
        ssize_t n;
        while ((n = ::recv(peer, buf, sizeof(buf), MSG_DONTWAIT)) > 0) out.append(buf, n);
        return out;
    }

    /* One staged iteration: main plans the lanes, thread 1 reads and classifies, main executes (lane
     * phases inline), thread 1 writes the replies. */
    static void iteration() {
        fastpathStagedPrepareLanes();
        pollerGetNextBatch(stagedPoller(), 0);
        fastpathStagedReadPhase(1);
        fastpathStagedExecute();
        if (fastpathStagedRepliesPending()) fastpathStagedReplyPhase(1);
        fastpathStagedFinish();
    }

    static long long infoStat(const char *name) {
        sds info = sdsempty();
        fastpathInfo(&info);
        const char *p = strstr(info, name);
        long long v = -1;
        if (p) v = atoll(p + strlen(name) + 1);
        sdsfree(info);
        return v;
    }

    static std::string bulk(const std::string &s) {
        return "$" + std::to_string(s.size()) + "\r\n" + s + "\r\n";
    }

    static std::string cmd(int argc, ...) {
        std::string out = "*" + std::to_string(argc) + "\r\n";
        va_list ap;
        va_start(ap, argc);
        for (int i = 0; i < argc; i++) out += bulk(va_arg(ap, const char *));
        va_end(ap);
        return out;
    }

    static void freeStaged(client *c, int peer) {
        close(peer);
        freeClient(c);
        iteration();
        freeClientsInAsyncFreeQueue();
    }
};

/* One pipeline: split MSET, split MGET, split MSET, split MGET in the other key order, a lane GET. Each
 * reply reflects every earlier command of the pipeline, although the parts ran on two lanes. */
TEST_F(FastpathLanesTest, SplitMsetAndMgetKeepPipelineOrderAcrossLanes) {
    int peer;
    client *c = newStagedClient(&peer);
    long long phases = infoStat("fastpath_write_lane_phases");
    long long barriers = infoStat("fastpath_write_barriers");
    send(peer, cmd(5, "MSET", K_L0A, "a", K_L1A, "a") + cmd(3, "MGET", K_L0A, K_L1A) + cmd(5, "MSET", K_L0A, "b", K_L1A, "b") +
                   cmd(3, "MGET", K_L1A, K_L0A) + cmd(2, "GET", K_L1A));
    iteration();
    EXPECT_EQ(recv(peer), "+OK\r\n*2\r\n$1\r\na\r\n$1\r\na\r\n+OK\r\n*2\r\n$1\r\nb\r\n$1\r\nb\r\n$1\r\nb\r\n");
    EXPECT_GT(infoStat("fastpath_write_lane_phases"), phases); /* the entries ran on lanes, not serially */
    EXPECT_EQ(infoStat("fastpath_write_barriers"), barriers); /* nothing ran as a barrier on main */
    freeStaged(c, peer);
}

/* A split MGET's reply is assembled in the command's key order: lane 1 key, missing key, lane 0 key
 * whose value spills past the lane arena, lane 1 key again, missing key on lane 0. */
TEST_F(FastpathLanesTest, SplitMgetAssemblesKeyOrderWithNilsAndSpilledValue) {
    int peer;
    client *c = newStagedClient(&peer);
    std::string big(70 * 1024, 'x');
    send(peer, cmd(7, "MSET", K_L0A, big.c_str(), K_L1B, "three", K_L1A, "two") +
                   cmd(6, "MGET", K_L1B, K_L1C, K_L0A, K_L1A, K_L0B));
    std::string out;
    for (int i = 0; i < 16 && out.empty(); i++) { /* the request spans several reads */
        iteration();
        out = recv(peer);
    }
    EXPECT_EQ(out, "+OK\r\n*5\r\n" + bulk("three") + "$-1\r\n" + bulk(big) + bulk("two") + "$-1\r\n");
    EXPECT_GT(infoStat("fastpath_write_lane_entries"), 0);
    freeStaged(c, peer);
}

/* Two clients in one iteration: a split MSET on one, a split MGET on the other. The reader sees both old
 * values or both new ones, whichever client the read phase harvested first; never one of each. */
TEST_F(FastpathLanesTest, ReaderNeverSeesHalfOfASplitMset) {
    int pw, pr;
    client *w = newStagedClient(&pw);
    client *r = newStagedClient(&pr);
    send(pw, cmd(5, "MSET", K_L0A, "old", K_L1A, "old"));
    iteration();
    EXPECT_EQ(recv(pw), "+OK\r\n");
    for (int i = 0; i < 20; i++) {
        std::string nv = "new" + std::to_string(i);
        /* alternate which socket is written first so both harvest orders are exercised */
        if (i % 2) {
            send(pr, cmd(3, "MGET", K_L1A, K_L0A));
            send(pw, cmd(5, "MSET", K_L0A, nv.c_str(), K_L1A, nv.c_str()));
        } else {
            send(pw, cmd(5, "MSET", K_L0A, nv.c_str(), K_L1A, nv.c_str()));
            send(pr, cmd(3, "MGET", K_L1A, K_L0A));
        }
        iteration();
        EXPECT_EQ(recv(pw), "+OK\r\n");
        std::string prev = i ? "new" + std::to_string(i - 1) : "old";
        std::string got = recv(pr);
        EXPECT_TRUE(got == "*2\r\n" + bulk(prev) + bulk(prev) || got == "*2\r\n" + bulk(nv) + bulk(nv)) << got;
    }
    EXPECT_GT(infoStat("fastpath_write_lane_phases"), 0);
    freeStaged(w, pw);
    freeStaged(r, pr);
}

/* The same pipeline gives the same replies when the segment is too small for a lane phase and main
 * runs the split entries whole (fpSerialSegment), and when it runs on the lanes. */
TEST_F(FastpathLanesTest, SerialFallbackMatchesLanes) {
    std::string program = cmd(5, "MSET", K_L0A, "1", K_L1A, "1") + cmd(3, "MGET", K_L0A, K_L1A) + cmd(3, "DEL", K_L0A, K_L1A) +
                          cmd(3, "MGET", K_L1A, K_L0A) + cmd(5, "MSET", K_L1A, "2", K_L0A, "2") + cmd(4, "MGET", K_L0A, K_L1A, K_L1C);
    std::string expect = "+OK\r\n*2\r\n$1\r\n1\r\n$1\r\n1\r\n:2\r\n*2\r\n$-1\r\n$-1\r\n+OK\r\n*3\r\n$1\r\n2\r\n$1\r\n2\r\n$-1\r\n";
    int peer;
    client *c = newStagedClient(&peer);
    long long serial = infoStat("fastpath_write_serial_segments");
    long long phases = infoStat("fastpath_write_lane_phases");
    long long barriers = infoStat("fastpath_write_barriers");
    send(peer, program);
    iteration();
    EXPECT_EQ(recv(peer), expect);
    EXPECT_GT(infoStat("fastpath_write_lane_phases"), phases);
    EXPECT_EQ(infoStat("fastpath_write_barriers"), barriers + 1); /* the cross-lane DEL runs alone on main */

    server.io_threads_parallel_writes_min = 1000;
    phases = infoStat("fastpath_write_lane_phases");
    send(peer, program);
    iteration();
    EXPECT_EQ(recv(peer), expect);
    EXPECT_EQ(infoStat("fastpath_write_lane_phases"), phases);
    EXPECT_GT(infoStat("fastpath_write_serial_segments"), serial);
    server.io_threads_parallel_writes_min = 1;
    freeStaged(c, peer);
}

/* Every multi-key lane command with its keys in two shards runs as one barrier on main, and gives the
 * same replies and the same final state as the one-shard form (both keys on lane 0). The one-shard
 * form runs on a lane with no barrier. Key names of equal length so the reply bytes compare directly
 * after the destination key name is normalised. (Plan item 17: CB-08, STR-16, CLU-20.) */
struct LaneMultiKeyCase {
    const char *name;
    /* setup(a, b), command(a, b), dump(a, b) as RESP programs; %A and %B are replaced by the key names */
    const char *setup;
    const char *command;
    const char *dump;
};

static std::string lmkSubst(const std::string &tmpl, const char *a, const char *b) {
    std::string out;
    for (size_t i = 0; i < tmpl.size(); i++) {
        if (tmpl[i] == '%' && i + 1 < tmpl.size() && (tmpl[i + 1] == 'A' || tmpl[i + 1] == 'B')) {
            out += (tmpl[i + 1] == 'A') ? a : b;
            i++;
        } else {
            out += tmpl[i];
        }
    }
    return out;
}

/* A RESP program written as a space separated list of words; '|' separates commands. */
static std::string lmkProgram(const std::string &text) {
    std::string out;
    size_t pos = 0;
    while (pos <= text.size()) {
        size_t end = text.find('|', pos);
        if (end == std::string::npos) end = text.size();
        std::string line = text.substr(pos, end - pos);
        std::vector<std::string> words;
        size_t p = 0;
        while (p < line.size()) {
            while (p < line.size() && line[p] == ' ') p++;
            size_t q = p;
            while (q < line.size() && line[q] != ' ') q++;
            if (q > p) words.push_back(line.substr(p, q - p));
            p = q;
        }
        if (!words.empty()) {
            out += "*" + std::to_string(words.size()) + "\r\n";
            for (size_t i = 0; i < words.size(); i++) out += "$" + std::to_string(words[i].size()) + "\r\n" + words[i] + "\r\n";
        }
        pos = end + 1;
    }
    return out;
}

static const LaneMultiKeyCase lmk_cases[] = {
    {"SMOVE", "SADD %A x y z | SADD %B q", "SMOVE %A %B y", "SMEMBERS %A | SMEMBERS %B | SCARD %A | SCARD %B"},
    {"SINTERSTORE", "SADD %A x y z | SADD %B y z w", "SINTERSTORE %B %A %B", "SMEMBERS %B | SCARD %A"},
    {"SUNIONSTORE", "SADD %A x | SADD %B y", "SUNIONSTORE %B %A %B", "SMEMBERS %B | SCARD %A"},
    {"SDIFFSTORE", "SADD %A x y z | SADD %B y", "SDIFFSTORE %B %A %B", "SMEMBERS %B | SCARD %A"},
    {"SINTERCARD", "SADD %A x y z | SADD %B y z", "SINTERCARD 2 %A %B", "SCARD %A | SCARD %B"},
    {"LMOVE", "RPUSH %A 1 2 3 | RPUSH %B 9", "LMOVE %A %B LEFT RIGHT", "LRANGE %A 0 -1 | LRANGE %B 0 -1"},
    {"RPOPLPUSH", "RPUSH %A 1 2 3 | RPUSH %B 9", "RPOPLPUSH %A %B", "LRANGE %A 0 -1 | LRANGE %B 0 -1"},
    {"LMPOP", "RPUSH %B 1 2 3", "LMPOP 2 %A %B LEFT COUNT 2", "LRANGE %A 0 -1 | LRANGE %B 0 -1"},
    {"ZMPOP", "ZADD %B 1 a 2 b 3 c", "ZMPOP 2 %A %B MIN COUNT 2", "ZRANGE %A 0 -1 WITHSCORES | ZRANGE %B 0 -1 WITHSCORES"},
    {"ZRANGESTORE", "ZADD %A 1 a 2 b 3 c", "ZRANGESTORE %B %A 0 1", "ZRANGE %B 0 -1 WITHSCORES | ZCARD %A"},
    {"ZUNIONSTORE", "ZADD %A 1 a | ZADD %B 2 b 3 a", "ZUNIONSTORE %B 2 %A %B", "ZRANGE %B 0 -1 WITHSCORES | ZCARD %A"},
    {"ZINTERSTORE", "ZADD %A 1 a 5 z | ZADD %B 2 b 3 a", "ZINTERSTORE %B 2 %A %B", "ZRANGE %B 0 -1 WITHSCORES | ZCARD %A"},
    {"ZDIFFSTORE", "ZADD %A 1 a 5 z | ZADD %B 3 a", "ZDIFFSTORE %B 2 %A %B", "ZRANGE %B 0 -1 WITHSCORES | ZCARD %A"},
    {"ZINTERCARD", "ZADD %A 1 a 5 z | ZADD %B 3 a", "ZINTERCARD 2 %A %B", "ZCARD %A | ZCARD %B"},
    {"DEL", "SET %A 1 | SET %B 2", "DEL %A %B", "EXISTS %A | EXISTS %B"},
    {"UNLINK", "SET %A 1 | SET %B 2", "UNLINK %A %B", "EXISTS %A | EXISTS %B"},
    {"EXISTS", "SET %A 1", "EXISTS %A %B %A", "GET %A | GET %B"},
    {"TOUCH", "SET %A 1 | SET %B 2", "TOUCH %A %B", "GET %A | GET %B"},
};

TEST_F(FastpathLanesTest, MultiKeyLaneCommandsAcrossTwoShardsRunAsBarriersAndMatchOneShard) {
    for (size_t ci = 0; ci < sizeof(lmk_cases) / sizeof(lmk_cases[0]); ci++) {
        const LaneMultiKeyCase &tc = lmk_cases[ci];
        std::string reply_two, reply_one, dump_two, dump_one;
        /* kb_two: a two-letter key on the other lane than K_L0A; kb_one: one in K_L0A's shard (same kvstore index) */
        static char kb_two[3], kb_one[3];
        if (kb_two[0] == 0) {
            int s0 = shardOf(K_L0A);
            const char *alphabet = "123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
            for (char x = 'a'; x <= 'z'; x++) {
                for (const char *y = alphabet; *y; y++) {
                    char cand[3] = {x, *y, 0};
                    sds k = sdsnew(cand);
                    int sh = getKVStoreIndexForKey(k);
                    sdsfree(k);
                    if (kb_one[0] == 0 && sh == s0 && strcmp(cand, K_L0A) != 0) memcpy(kb_one, cand, 3);
                    if (kb_two[0] == 0 && laneOf(cand) != laneOf(K_L0A)) memcpy(kb_two, cand, 3);
                }
            }
            ASSERT_NE(kb_one[0], 0) << "no two-letter key shares the shard of " << K_L0A;
            ASSERT_NE(kb_two[0], 0);
        }
        for (int form = 0; form < 2; form++) {
            /* form 0: the keys in two shards on two lanes; form 1: both keys in the shard of K_L0A */
            const char *a = K_L0A;
            const char *b = form == 0 ? kb_two : kb_one;
            emptyDbStructure(server.db, -1, 0, NULL);
            int peer;
            client *c = newStagedClient(&peer);
            send(peer, lmkProgram(lmkSubst(tc.setup, a, b)));
            iteration();
            recv(peer);
            long long barriers = infoStat("fastpath_write_barriers");
            long long entries = infoStat("fastpath_write_lane_entries");
            long long phases = infoStat("fastpath_write_lane_phases");
            send(peer, lmkProgram(lmkSubst(tc.command, a, b)));
            iteration();
            std::string reply = recv(peer);
            long long barriers_after = infoStat("fastpath_write_barriers");
            if (form == 0) {
                /* keys in two shards: the command runs alone on main as a barrier */
                EXPECT_EQ(barriers_after, barriers + 1) << tc.name << " two-shard form did not run as a barrier";
            } else {
                EXPECT_EQ(barriers_after, barriers) << tc.name << " one-shard form ran a barrier";
                EXPECT_GT(infoStat("fastpath_write_lane_entries"), entries) << tc.name << " one-shard form did not run on a lane";
                EXPECT_GT(infoStat("fastpath_write_lane_phases"), phases) << tc.name << " one-shard form ran no lane phase";
            }
            send(peer, lmkProgram(lmkSubst(tc.dump, a, b)));
            iteration();
            std::string dump = recv(peer);
            freeStaged(c, peer);
            /* normalise the destination key name (same length) so the two forms compare byte for byte */
            if (form == 0) {
                size_t at;
                while ((at = reply.find(kb_two)) != std::string::npos) reply.replace(at, 2, kb_one);
                while ((at = dump.find(kb_two)) != std::string::npos) dump.replace(at, 2, kb_one);
                reply_two = reply;
                dump_two = dump;
            } else {
                reply_one = reply;
                dump_one = dump;
            }
        }
        EXPECT_EQ(reply_two, reply_one) << tc.name << " replies differ between the two-shard and one-shard forms";
        EXPECT_EQ(dump_two, dump_one) << tc.name << " final state differs between the two-shard and one-shard forms";
        EXPECT_FALSE(reply_two.empty()) << tc.name;
        EXPECT_FALSE(dump_two.empty()) << tc.name;
    }
}

/* ACL epoch: a lane entry's ALLOW verdict is computed in the read phase and the lane runs cmd->proc
 * with no check at execution time. A rule change on main between the two (a main-path command before
 * the write phase, or a barrier entry earlier in the plan) must make the entry run through the full
 * check again, so the plan's own order (the change, then the entry) is what the client observes. */
static void aclLaneKeyAbsent(const char *name) {
    robj *k = createStringObject(name, strlen(name));
    EXPECT_EQ(lookupKeyRead(server.db[0], k), nullptr) << name << " was written by a stale lane entry";
    decrRefCount(k);
}

/* The user's own pipeline revokes SET for itself, then sends a SET. ACL is not a fast-path command, so
 * the client leaves for main at the ACL entry and the rest of its pipeline (the SET) is requeued and
 * runs on main after the write phase, through the full check. The plan never holds a lane SET behind a
 * rule change of the same client; this pins that order, with acl-offload off and on. */
TEST_F(FastpathLanesTest, PipelineAclRevokeThenSetRefusesTheSet) {
    const char *unames[2] = {"aclu0", "aclu1"};
    const char *keys[2] = {K_L1A, K_L1B};
    for (int acl_offload = 0; acl_offload <= 1; acl_offload++) {
        const char *uname = unames[acl_offload], *key = keys[acl_offload];
        server.acl_offload = acl_offload;
        int pa, pu;
        client *admin = newStagedClient(&pa);
        send(pa, cmd(9, "ACL", "SETUSER", uname, "on", "nopass", "~*", "&*", "+@all", "alldbs"));
        iteration();
        EXPECT_EQ(recv(pa) + std::string(admin->buf, admin->bufpos), "+OK\r\n");
        admin->bufpos = 0;
        user *usr = ACLGetUserByName(uname, strlen(uname));
        ASSERT_NE(usr, nullptr);
        EXPECT_TRUE(ACLUserUnrestricted(usr));

        client *u = newStagedClient(&pu);
        clientSetUser(u, usr, 1);
        send(pu, cmd(4, "ACL", "SETUSER", uname, "-set") + cmd(3, "SET", key, "v"));
        iteration();
        std::string got = recv(pu) + std::string(u->buf, u->bufpos);
        EXPECT_EQ(got.substr(0, 12), "+OK\r\n-NOPERM") << "acl_offload=" << acl_offload << " replies: " << got;
        aclLaneKeyAbsent(key);

        close(pu);
        freeClient(u);
        close(pa);
        freeClient(admin);
        iteration();
        freeClientsInAsyncFreeQueue();
        server.acl_offload = 0;
    }
}

