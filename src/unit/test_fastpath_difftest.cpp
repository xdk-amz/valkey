/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Seeded differential test of the write lanes against the serial executor.
 *
 * A seeded generator builds random pipelines of lane-eligible commands (strings, hashes, sets, sorted
 * sets, lists, expiry, multi-key commands with keys in one, two or three shards, the same key twice in
 * one command, plus RENAME which is a barrier) for one to three clients. The same program is run four
 * times on an empty database: with io-threads-parallel-writes-min 1 (every segment runs on the lanes,
 * inline through testOnlyStagedSetWorkers, lane by lane), once with the lane key prefetch off and once
 * with it on (io-threads-lane-prefetch: the lane collects its entries of the phase, prefetches their keys
 * in batches and runs them in the same order); with the minimum set so high that fpSerialSegment runs
 * every entry whole on main between the same barriers; and with lane planning off (the plain staged
 * executor: every batch in stream order, no segments). Every reply byte of every client and a dump of
 * the final dataset must be identical across the four. A difference prints the seed and the program so
 * the seed replays with DIFFTEST_SEED=<n>.
 *
 * The six keys are chosen at suite start from this build's table hash (the hash of short keys differs
 * between branches): three keys per lane, six distinct shards, so a multi-key command may span one, two
 * or three shards and two keys of one lane may still be in different shards (a barrier).
 *
 * Environment: DIFFTEST_SEEDS (default 2000) seeds per test; DIFFTEST_SEED replays one seed and prints
 * the program; DIFFTEST_FIRST shifts the seed range; DIFFTEST_MAXFAIL stops after that many differing
 * seeds (default 3). */

#include "generated_wrappers.hpp"
#include "test_server_init.hpp"

#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

extern "C" {
#include "connection.h"
#include "fastpath.h"
#include "io_threads.h"
#include "module.h"
#include "poller.h"
#include "server.h"
#include "staged_loop.h"
extern hashtableType commandSetType;
extern dictType keylistDictType;
void createSharedObjects(void);
}

static const char *dt_logfile = "/dev/null";

#define LANE_BITS 4

/* Six short keys, three per lane (lane = shard % 2 with keyspace-shard-bits 4), six distinct shards;
 * dt_keys[0], [1] and [4] are in lane 0, [2], [3] and [5] in lane 1. Filled by FastpathDifftest::pickKeys
 * from this build's hash, so a hash change moves the keys instead of failing a false precondition. */
static char dt_key_store[6][16];
static const char *dt_keys[] = {dt_key_store[0], dt_key_store[1], dt_key_store[2],
                                dt_key_store[3], dt_key_store[4], dt_key_store[5]};
static const int dt_nkeys = 6;
static const char *dt_members[] = {"m1", "m2", "m3", "m4"};
static const char *dt_fields[] = {"f1", "f2", "f3"};
static const char *dt_values[] = {"1", "2", "10", "a", "bb", "-3", "x7"};
static const char *dt_far_pxat = "4102444800000"; /* 2100-01-01, never expires during the test */

typedef std::vector<std::string> Cmd;
typedef std::vector<Cmd> Program;

struct DtRng {
    uint64_t s;
    explicit DtRng(uint64_t seed) : s(seed * 0x9E3779B97F4A7C15ull + 0x1234567ull) {
        if (!s) s = 1;
    }
    uint64_t next() {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        return s;
    }
    int below(int n) { return (int)(next() % (uint64_t)n); }
    const char *key() { return dt_keys[below(dt_nkeys)]; }
    const char *member() { return dt_members[below(4)]; }
    const char *field() { return dt_fields[below(3)]; }
    const char *value() { return dt_values[below(7)]; }
};

class FastpathDifftest : public ::testing::Test {
  protected:
    static void SetUpTestSuite() {
        monotonicInit();
        memset(&server, 0, sizeof(server));
        server.hz = CONFIG_DEFAULT_HZ;
        server.logfile = const_cast<char *>(dt_logfile);
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
        server.db[0]->blocking_keys_unblock_on_nokey = dictCreate(&keylistDictType);

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
        server.io_threads_parallel_writes_min = 1;
        server.io_batch_commands = 16;
        server.prefetch_batch_max_size = 16; /* the lane key prefetch batch size when a run turns it on */
        server.io_threads_lane_prefetch = 0;
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
        server.io_threads_lane_prefetch = 0;
        pickKeys();
        ASSERT_EQ(laneOf(dt_keys[0]), 0);
        ASSERT_EQ(laneOf(dt_keys[1]), 0);
        ASSERT_EQ(laneOf(dt_keys[4]), 0);
        ASSERT_EQ(laneOf(dt_keys[2]), 1);
        ASSERT_EQ(laneOf(dt_keys[3]), 1);
        ASSERT_EQ(laneOf(dt_keys[5]), 1);
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

    /* Fill dt_keys with the first short keys "k0", "k1", ... that give three keys per lane in six
     * distinct shards, in the slot order the comment on dt_keys describes. Deterministic for one build. */
    static void pickKeys(void) {
        static const int slots[2][3] = {{0, 1, 4}, {2, 3, 5}};
        int found[2] = {0, 0};
        bool used[1 << LANE_BITS] = {false};
        for (int i = 0; (found[0] < 3 || found[1] < 3) && i < 1000; i++) {
            char name[16];
            snprintf(name, sizeof(name), "k%d", i);
            int shard = shardOf(name);
            int lane = shard % 2;
            if (found[lane] == 3 || used[shard]) continue;
            used[shard] = true;
            snprintf(dt_key_store[slots[lane][found[lane]]], sizeof(dt_key_store[0]), "%s", name);
            found[lane]++;
        }
        ASSERT_EQ(found[0], 3);
        ASSERT_EQ(found[1], 3);
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

    static void recvInto(int peer, std::string &out) {
        char buf[65536];
        ssize_t n;
        while ((n = ::recv(peer, buf, sizeof(buf), MSG_DONTWAIT)) > 0) out.append(buf, n);
    }

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

    static std::string resp(const Cmd &c) {
        std::string out = "*" + std::to_string(c.size()) + "\r\n";
        for (size_t i = 0; i < c.size(); i++) out += "$" + std::to_string(c[i].size()) + "\r\n" + c[i] + "\r\n";
        return out;
    }

    static std::string respProgram(const Program &p) {
        std::string out;
        for (size_t i = 0; i < p.size(); i++) out += resp(p[i]);
        return out;
    }

    /* Count complete RESP replies in s (simple, bulk, integer, error, null, arrays). Returns -1 when the
     * tail is a partial reply. */
    static long replyLen(const std::string &s, size_t at) {
        if (at >= s.size()) return -1;
        size_t eol = s.find("\r\n", at);
        if (eol == std::string::npos) return -1;
        char t = s[at];
        if (t == '+' || t == '-' || t == ':' || t == '_') return (long)(eol + 2 - at);
        long n = atol(s.c_str() + at + 1);
        if (t == '$') {
            if (n < 0) return (long)(eol + 2 - at);
            size_t end = eol + 2 + (size_t)n + 2;
            return end <= s.size() ? (long)(end - at) : -1;
        }
        if (t == '*') {
            size_t pos = eol + 2;
            if (n < 0) return (long)(pos - at);
            for (long i = 0; i < n; i++) {
                long l = replyLen(s, pos);
                if (l < 0) return -1;
                pos += (size_t)l;
            }
            return (long)(pos - at);
        }
        ADD_FAILURE() << "unknown RESP type byte " << (int)t;
        return (long)(eol + 2 - at);
    }

    static int countReplies(const std::string &s) {
        int n = 0;
        size_t pos = 0;
        while (pos < s.size()) {
            long l = replyLen(s, pos);
            if (l < 0) break;
            pos += (size_t)l;
            n++;
        }
        return n;
    }

    static Cmd mk(const char *a, const char *b = NULL, const char *c = NULL, const char *d = NULL, const char *e = NULL,
                  const char *f = NULL, const char *g = NULL) {
        Cmd out;
        const char *w[] = {a, b, c, d, e, f, g};
        for (int i = 0; i < 7; i++)
            if (w[i]) out.push_back(w[i]);
        return out;
    }

    /* One random lane-eligible command (RENAME included as the one barrier command). Multi-key commands
     * draw each key independently so the same key appears twice in one command about one time in six. */
    static Cmd randomCmd(DtRng &r) {
        switch (r.below(40)) {
        case 0: return mk("SET", r.key(), r.value());
        case 1: return mk("GET", r.key());
        case 2: return mk("MSET", r.key(), r.value(), r.key(), r.value());
        case 3: return mk("MSET", r.key(), r.value(), r.key(), r.value(), r.key(), r.value());
        case 4: return mk("MGET", r.key(), r.key());
        case 5: return mk("MGET", r.key(), r.key(), r.key());
        case 6: return mk("MSETNX", r.key(), r.value(), r.key(), r.value());
        case 7: return mk("INCR", r.key());
        case 8: return mk("INCRBY", r.key(), r.value());
        case 9: return mk("APPEND", r.key(), r.value());
        case 10: return mk("DEL", r.key(), r.key());
        case 11: return mk("UNLINK", r.key());
        case 12: return mk("EXISTS", r.key(), r.key(), r.key());
        case 13: return mk("PEXPIREAT", r.key(), dt_far_pxat);
        case 14: return mk("EXPIRE", r.key(), r.below(2) ? "0" : "-5"); /* both delete; a relative positive TTL would differ by the clock */
        case 15: return mk("PERSIST", r.key());
        case 16: return mk("RENAME", r.key(), r.key());
        case 17: return mk("SMOVE", r.key(), r.key(), r.member());
        case 18: return mk("SADD", r.key(), r.member(), r.member());
        case 19: return mk("SREM", r.key(), r.member());
        case 20: return mk("SINTERSTORE", r.key(), r.key(), r.key());
        case 21: return mk("SUNIONSTORE", r.key(), r.key(), r.key());
        case 22: return mk("LPUSH", r.key(), r.value(), r.value());
        case 23: return mk("RPUSH", r.key(), r.value());
        case 24: return mk("LPOP", r.key());
        case 25: return mk("LMOVE", r.key(), r.key(), r.below(2) ? "LEFT" : "RIGHT", r.below(2) ? "LEFT" : "RIGHT");
        case 26: return mk("LRANGE", r.key(), "0", "-1");
        case 27: return mk("ZADD", r.key(), r.value(), r.member());
        case 28: return mk("ZINCRBY", r.key(), "1", r.member());
        case 29: return mk("ZUNIONSTORE", r.key(), "2", r.key(), r.key());
        case 30: return mk("ZSCORE", r.key(), r.member());
        case 31: return mk("HSET", r.key(), r.field(), r.value());
        case 32: return mk("HINCRBY", r.key(), r.field(), "2");
        case 33: return mk("HGET", r.key(), r.field());
        case 34: return mk("GETEX", r.key(), "PXAT", dt_far_pxat);
        case 35: return mk("GETDEL", r.key());
        case 36: return mk("SETRANGE", r.key(), r.below(2) ? "0" : "3", r.value());
        case 37: return mk("GETRANGE", r.key(), "0", "-1");
        case 38: return mk("TYPE", r.key());
        default: return mk("STRLEN", r.key());
        }
    }

    /* The dataset dump: every key through type-independent readers whose output does not depend on
     * iteration order (member tests instead of SMEMBERS), plus the absolute expire time. */
    static Program dumpProgram() {
        Program p;
        for (int i = 0; i < dt_nkeys; i++) {
            const char *k = dt_keys[i];
            p.push_back(mk("TYPE", k));
            p.push_back(mk("GET", k));
            p.push_back(mk("LRANGE", k, "0", "-1"));
            p.push_back(mk("SMISMEMBER", k, "m1", "m2", "m3", "m4"));
            p.push_back(mk("ZMSCORE", k, "m1", "m2", "m3", "m4"));
            p.push_back(mk("HMGET", k, "f1", "f2", "f3"));
            p.push_back(mk("PEXPIRETIME", k));
        }
        return p;
    }

    struct Scenario {
        int nclients;
        int nrounds;
        std::vector<std::vector<Program> > rounds; /* rounds[r][c] */
        std::vector<std::vector<int> > order;      /* order[r]: client write order in round r */
    };

    static Scenario makeScenario(uint64_t seed) {
        DtRng r(seed);
        Scenario s;
        s.nclients = 1 + r.below(3);
        s.nrounds = 1 + r.below(3);
        for (int ro = 0; ro < s.nrounds; ro++) {
            std::vector<Program> progs;
            for (int c = 0; c < s.nclients; c++) {
                Program p;
                int n = 1 + r.below(12); /* at most io_batch_commands (16) so one iteration harvests it */
                for (int i = 0; i < n; i++) p.push_back(randomCmd(r));
                progs.push_back(p);
            }
            s.rounds.push_back(progs);
            std::vector<int> ord;
            for (int c = 0; c < s.nclients; c++) ord.push_back(c);
            for (int c = s.nclients - 1; c > 0; c--) {
                int j = r.below(c + 1);
                int t = ord[c];
                ord[c] = ord[j];
                ord[j] = t;
            }
            s.order.push_back(ord);
        }
        return s;
    }

    struct RunResult {
        std::vector<std::string> replies; /* per client, all rounds */
        std::string dump;
        long long lane_phases, lane_entries, serial_segments, barriers, lane_prefetches;
    };

    /* Four executors for one stream: MODE_LANES runs every segment on the lanes with the lane key prefetch
     * off, MODE_LANES_PREFETCH the same with it on (the lane collects its entries, prefetches their keys in
     * batches of prefetch-batch-max-size and runs them in the collected order); MODE_SERIAL plans the same
     * segments and barriers but fpSerialSegment runs each segment's entries whole on main; MODE_PLAIN turns
     * lane planning off (io-threads-parallel-writes 0), so fastpathStagedExecute runs every batch in stream
     * order with no classification, segments or barriers: the executor the design had before the lanes. */
    enum { MODE_LANES = 0, MODE_SERIAL = 1, MODE_PLAIN = 2, MODE_LANES_PREFETCH = 3 };

    /* Run the scenario on an empty database with the given executor. */
    static RunResult run(const Scenario &s, int mode) {
        RunResult res;
        emptyDbStructure(server.db, -1, 0, NULL);
        server.io_threads_parallel_writes = mode == MODE_PLAIN ? 0 : 1;
        server.io_threads_parallel_writes_min = (mode == MODE_LANES || mode == MODE_LANES_PREFETCH) ? 1 : 1000000;
        server.io_threads_lane_prefetch = mode == MODE_LANES_PREFETCH ? 1 : 0;
        long long prefetches0 = infoStat("fastpath_write_lane_prefetches");
        long long phases0 = infoStat("fastpath_write_lane_phases");
        long long entries0 = infoStat("fastpath_write_lane_entries");
        long long serial0 = infoStat("fastpath_write_serial_segments");
        long long barriers0 = infoStat("fastpath_write_barriers");
        std::vector<client *> cs(s.nclients);
        std::vector<int> peers(s.nclients);
        res.replies.resize(s.nclients);
        for (int c = 0; c < s.nclients; c++) cs[c] = newStagedClient(&peers[c]);
        int expected = 0; /* cumulative over rounds: replies are collected per client across rounds */
        for (int ro = 0; ro < s.nrounds; ro++) {
            for (int c = 0; c < s.nclients; c++) {
                int cl = s.order[ro][c];
                send(peers[cl], respProgram(s.rounds[ro][cl]));
                expected += (int)s.rounds[ro][cl].size();
            }
            int got = 0;
            for (int it = 0; it < 32 && got < expected; it++) {
                iteration();
                got = 0;
                for (int c = 0; c < s.nclients; c++) {
                    recvInto(peers[c], res.replies[c]);
                }
                for (int c = 0; c < s.nclients; c++) got += countReplies(res.replies[c]);
            }
            EXPECT_EQ(got, expected) << "round " << ro << " did not complete";
        }
        int dpeer;
        client *dc = newStagedClient(&dpeer);
        Program dp = dumpProgram();
        send(dpeer, respProgram(dp));
        for (int it = 0; it < 32 && countReplies(res.dump) < (int)dp.size(); it++) {
            iteration();
            recvInto(dpeer, res.dump);
        }
        EXPECT_EQ(countReplies(res.dump), (int)dp.size());
        res.lane_phases = infoStat("fastpath_write_lane_phases") - phases0;
        res.lane_entries = infoStat("fastpath_write_lane_entries") - entries0;
        res.serial_segments = infoStat("fastpath_write_serial_segments") - serial0;
        res.barriers = infoStat("fastpath_write_barriers") - barriers0;
        res.lane_prefetches = infoStat("fastpath_write_lane_prefetches") - prefetches0;
        close(dpeer);
        freeClient(dc);
        for (int c = 0; c < s.nclients; c++) {
            close(peers[c]);
            freeClient(cs[c]);
        }
        iteration();
        freeClientsInAsyncFreeQueue();
        server.io_threads_parallel_writes = 1;
        server.io_threads_parallel_writes_min = 1;
        server.io_threads_lane_prefetch = 0;
        return res;
    }

    static std::string show(const Program &p) {
        std::string out;
        for (size_t i = 0; i < p.size(); i++) {
            for (size_t j = 0; j < p[i].size(); j++) out += (j ? " " : "  ") + p[i][j];
            out += "\n";
        }
        return out;
    }

    static std::string showScenario(uint64_t seed, const Scenario &s) {
        std::string out = "seed " + std::to_string(seed) + ": " + std::to_string(s.nclients) + " clients, " +
                          std::to_string(s.nrounds) + " rounds\n keys (lane/shard):";
        for (int i = 0; i < dt_nkeys; i++)
            out += " " + std::string(dt_keys[i]) + " (" + std::to_string(laneOf(dt_keys[i])) + "/" +
                   std::to_string(shardOf(dt_keys[i])) + ")";
        out += "\n";
        for (int ro = 0; ro < s.nrounds; ro++) {
            out += " round " + std::to_string(ro) + " write order:";
            for (int c = 0; c < s.nclients; c++) out += " c" + std::to_string(s.order[ro][c]);
            out += "\n";
            for (int c = 0; c < s.nclients; c++) out += " c" + std::to_string(c) + ":\n" + show(s.rounds[ro][c]);
        }
        return out;
    }

    static std::string printable(const std::string &s) {
        std::string out;
        for (size_t i = 0; i < s.size(); i++) {
            if (s[i] == '\r') continue;
            if (s[i] == '\n') out += " | ";
            else out += s[i];
        }
        return out;
    }

    /* One seed: lanes (prefetch off and on) versus serial versus plain. Returns false and prints the
     * program on a difference. */
    static bool checkSeed(uint64_t seed, bool verbose) {
        Scenario s = makeScenario(seed);
        RunResult lane = run(s, MODE_LANES);
        RunResult lanepf = run(s, MODE_LANES_PREFETCH);
        RunResult serial = run(s, MODE_SERIAL);
        RunResult plain = run(s, MODE_PLAIN);
        bool ok = lane.dump == serial.dump && lane.dump == plain.dump && lane.dump == lanepf.dump;
        for (int c = 0; c < s.nclients && ok; c++)
            ok = lane.replies[c] == serial.replies[c] && lane.replies[c] == plain.replies[c] &&
                 lane.replies[c] == lanepf.replies[c];
        if (!ok || verbose) {
            std::string msg = showScenario(seed, s);
            for (int c = 0; c < s.nclients; c++) {
                msg += " c" + std::to_string(c) + " lanes : " + printable(lane.replies[c]) + "\n";
                msg += " c" + std::to_string(c) + " lanepf: " + printable(lanepf.replies[c]) + "\n";
                msg += " c" + std::to_string(c) + " serial: " + printable(serial.replies[c]) + "\n";
                msg += " c" + std::to_string(c) + " plain : " + printable(plain.replies[c]) + "\n";
            }
            msg += " dump lanes : " + printable(lane.dump) + "\n";
            msg += " dump lanepf: " + printable(lanepf.dump) + "\n";
            msg += " dump serial: " + printable(serial.dump) + "\n";
            msg += " dump plain : " + printable(plain.dump) + "\n";
            msg += " lane run: phases " + std::to_string(lane.lane_phases) + " entries " + std::to_string(lane.lane_entries) +
                   " barriers " + std::to_string(lane.barriers) + "; prefetch run: prefetches " +
                   std::to_string(lanepf.lane_prefetches) + "; serial run: segments " +
                   std::to_string(serial.serial_segments) + " phases " + std::to_string(serial.lane_phases) + "\n";
            if (!ok) ADD_FAILURE() << "the lane, serial-segment and plain executors differ\n" << msg;
            else printf("%s", msg.c_str());
        }
        /* engagement: the lane runs must have used lanes (and the prefetch stage), the serial run must not */
        EXPECT_GT(lane.lane_phases, 0) << "seed " << seed << ": the lane run ran no lane phase";
        EXPECT_GT(lane.lane_entries, 0) << "seed " << seed;
        EXPECT_EQ(lane.lane_prefetches, 0) << "seed " << seed << ": the lane run prefetched with the stage off";
        EXPECT_GT(lanepf.lane_phases, 0) << "seed " << seed << ": the prefetch run ran no lane phase";
        if (lanepf.lane_entries > 1) {
            EXPECT_GT(lanepf.lane_prefetches, 0) << "seed " << seed << ": the prefetch run ran no prefetch batch";
        }
        EXPECT_EQ(serial.lane_phases, 0) << "seed " << seed << ": the serial run ran a lane phase";
        EXPECT_GT(serial.serial_segments, 0) << "seed " << seed;
        EXPECT_EQ(plain.lane_phases, 0) << "seed " << seed;
        EXPECT_EQ(plain.barriers, 0) << "seed " << seed << ": the plain run planned barriers";
        return ok;
    }

    static uint64_t envNum(const char *name, uint64_t dflt) {
        const char *v = getenv(name);
        return v ? strtoull(v, NULL, 10) : dflt;
    }
};

/* Many seeds: every reply and the final dataset agree between the lanes and the serial executor. */
TEST_F(FastpathDifftest, LanesMatchSerialExecutorOverRandomPrograms) {
    uint64_t one = envNum("DIFFTEST_SEED", 0);
    if (one) {
        EXPECT_TRUE(checkSeed(one, true));
        return;
    }
    uint64_t first = envNum("DIFFTEST_FIRST", 1);
    uint64_t n = envNum("DIFFTEST_SEEDS", 2000);
    int failed = 0;
    for (uint64_t seed = first; seed < first + n; seed++) {
        if (!checkSeed(seed, false)) {
            failed++;
            if (failed >= (int)envNum("DIFFTEST_MAXFAIL", 3)) break; /* enough to replay; stop flooding the log */
        }
    }
    EXPECT_EQ(failed, 0) << "replay with DIFFTEST_SEED=<seed> --gtest_filter=FastpathDifftest.*";
}

/* The serial executor is deterministic: the same program twice gives the same bytes. Guards the oracle
 * against a hidden dependency on harvest order or time. */
TEST_F(FastpathDifftest, SerialExecutorIsDeterministic) {
    uint64_t n = envNum("DIFFTEST_SEEDS", 2000) / 10 + 1;
    for (uint64_t seed = 1; seed <= n; seed++) {
        Scenario s = makeScenario(seed);
        RunResult a = run(s, MODE_SERIAL);
        RunResult b = run(s, MODE_SERIAL);
        ASSERT_EQ(a.dump, b.dump) << "seed " << seed;
        for (int c = 0; c < s.nclients; c++) ASSERT_EQ(a.replies[c], b.replies[c]) << "seed " << seed << " client " << c;
    }
}
