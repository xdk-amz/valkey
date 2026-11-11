/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Staged fast path driven inline on one thread: thread 1's read phase, main's write phase and
 * thread 1's reply phase run back to back, then main finishes the iteration. Covers admission,
 * per-client reply order, spilled replies, leaving for the main path, and closes applied after
 * the reply barrier. */

#include "generated_wrappers.hpp"
#include "test_server_init.hpp"

#include <cstring>
#include <string>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <utility>
#include <vector>

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
void fastGetRecomputeSpecAclOk(client *c); /* from fastget.h, declared here to avoid <stdatomic.h> in C++ */
void fastGetRefreshMainGate(void);     /* main folds the pause, yield and failover states into one gate word */
}

static const char *st_logfile = "/dev/null";

class FastpathStagedTest : public ::testing::Test {
  protected:
    static void SetUpTestSuite() {
        monotonicInit();
        memset(&server, 0, sizeof(server));
        server.hz = CONFIG_DEFAULT_HZ;
        server.logfile = const_cast<char *>(st_logfile);
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
    }

    void TearDown() override {
        testOnlyStagedStop();
        freeClientsInAsyncFreeQueue();
    }

    /* A connected client admitted to the staged fast path, with the peer end of its socket in *peer. */
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

    /* Whatever the socket holds right now; never blocks. */
    static std::string recv(int peer) {
        std::string out;
        char buf[65536];
        ssize_t n;
        while ((n = ::recv(peer, buf, sizeof(buf), MSG_DONTWAIT)) > 0) out.append(buf, n);
        return out;
    }

    static void readPhase() {
        pollerGetNextBatch(stagedPoller(), 0);
        fastpathStagedReadPhase(1);
    }

    static void writeAndReplyPhases() {
        fastpathStagedExecute();
        if (fastpathStagedRepliesPending()) fastpathStagedReplyPhase(1);
        fastpathStagedFinish();
    }

    static void iteration() {
        readPhase();
        writeAndReplyPhases();
    }

    static std::string mainBuf(client *c) {
        return std::string(c->buf, c->bufpos);
    }

    /* Run iterations until the client is freed (it leaves server.clients) or the bound is reached. */
    static bool iterateUntilFreed(client *c, int bound) {
        for (int i = 0; i < bound; i++) {
            iteration();
            freeClientsInAsyncFreeQueue();
            if (!listSearchKey(server.clients, c)) return true;
        }
        return false;
    }

    static void freeStaged(client *c, int peer) {
        close(peer);
        freeClient(c);
        iteration();
        freeClientsInAsyncFreeQueue();
    }
};

/* Admission gives the connection one control; batched commands run on main and their replies are
 * written by the IO thread in the reply phase, in order. */
TEST_F(FastpathStagedTest, PipelinedCommandsRepliedInOrderAfterReplyPhase) {
    int peer;
    client *c = newStagedClient(&peer);
    ASSERT_NE(c->control, nullptr);
    ClientControl *cc = c->control;
    EXPECT_EQ(cc->client_id, c->id);
    EXPECT_EQ(cc->lifecycle, FP_ACTIVE);
    EXPECT_EQ(fastpathControlEnsure(c), C_OK);
    EXPECT_EQ(c->control, cc);

    send(peer, "*2\r\n$4\r\nINCR\r\n$4\r\nst:a\r\n*2\r\n$4\r\nINCR\r\n$4\r\nst:a\r\n");
    readPhase();
    EXPECT_EQ(c->fp_inflight, 2u);
    EXPECT_TRUE(fastpathStagedRepliesPending());
    EXPECT_EQ(recv(peer), ""); /* nothing is written before the reply phase */
    writeAndReplyPhases();
    EXPECT_EQ(c->fp_inflight, 0u);
    EXPECT_FALSE(fastpathStagedRepliesPending());
    EXPECT_EQ(recv(peer), ":1\r\n:2\r\n");
    EXPECT_EQ(c->flag.fastpath, 1u);

    freeStaged(c, peer);
}

/* Two clients in one batch each get exactly their own replies. */
TEST_F(FastpathStagedTest, ClientsSharingABatchGetTheirOwnReplies) {
    int pa, pb;
    client *a = newStagedClient(&pa);
    client *b = newStagedClient(&pb);
    send(pa, "*2\r\n$4\r\nINCR\r\n$4\r\nst:b\r\n");
    send(pb, "*2\r\n$4\r\nINCR\r\n$4\r\nst:b\r\n*2\r\n$4\r\nINCR\r\n$4\r\nst:c\r\n");
    iteration();
    std::string ra = recv(pa), rb = recv(pb);
    /* Execution order across clients follows fd readiness; each client's own order is kept. */
    EXPECT_TRUE((ra == ":1\r\n" && rb == ":2\r\n:1\r\n") || (ra == ":2\r\n" && rb == ":1\r\n:1\r\n"));
    freeStaged(a, pa);
    freeStaged(b, pb);
}

/* A reply larger than the batch arena spills to its own block and is delivered whole. */
TEST_F(FastpathStagedTest, SpilledReplyDeliveredWhole) {
    int peer;
    client *c = newStagedClient(&peer);
    std::string big(32 * 1024, 'x');
    send(peer, "*3\r\n$3\r\nSET\r\n$5\r\nbigky\r\n$" + std::to_string(big.size()) + "\r\n" + big + "\r\n");
    std::string out;
    for (int i = 0; i < 16 && out.empty(); i++) {
        iteration();
        out = recv(peer);
    }
    EXPECT_EQ(out, "+OK\r\n");

    send(peer, "*2\r\n$3\r\nGET\r\n$5\r\nbigky\r\n");
    std::string expect = "$" + std::to_string(big.size()) + "\r\n" + big + "\r\n";
    out.clear();
    for (int i = 0; i < 16 && out.size() < expect.size(); i++) {
        iteration();
        out += recv(peer);
    }
    EXPECT_EQ(out, expect);
    freeStaged(c, peer);
}

/* A command the fast path does not run makes the client leave: what it batched before is replied by
 * the IO thread, then main takes the client back after the reply phase and runs the rest itself. */
TEST_F(FastpathStagedTest, UnsupportedCommandHandsClientBackAfterReplyPhase) {
    int peer;
    client *c = newStagedClient(&peer);
    send(peer, "*2\r\n$4\r\nINCR\r\n$4\r\nst:d\r\n*2\r\n$4\r\nPING\r\n$1\r\nx\r\n*3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n$1\r\nn\r\n");
    readPhase();
    EXPECT_EQ(c->control->lifecycle, FP_LEAVING);
    EXPECT_EQ(c->flag.fastpath, 1u); /* main takes it back only after the reply phase */
    writeAndReplyPhases();
    EXPECT_EQ(c->flag.fastpath, 0u);
    EXPECT_EQ(c->control->lifecycle, FP_DETACHED);
    /* The batched replies come first; CLIENT SETNAME ran on main, which may already have written it. */
    std::string out = recv(peer) + mainBuf(c);
    EXPECT_EQ(out, ":1\r\n$1\r\nx\r\n+OK\r\n");
    ASSERT_NE(c->name, nullptr);
    EXPECT_STREQ((const char *)objectGetVal(c->name), "n");
    close(peer);
    freeClient(c);
}

/* freeClient on a staged client only records the close: its batched entries are skipped, and the
 * client stays allocated until main unmaps it after the reply barrier. */
TEST_F(FastpathStagedTest, CloseSkipsEntriesAndFreesAfterReplyBarrier) {
    int peer;
    client *c = newStagedClient(&peer);
    send(peer, "*3\r\n$3\r\nSET\r\n$4\r\nst:e\r\n$1\r\n1\r\n");
    readPhase();
    EXPECT_EQ(c->fp_inflight, 1u);

    EXPECT_EQ(freeClient(c), 0); /* main, write phase: recorded, not freed */
    EXPECT_EQ(c->flag.fastpath, 1u);
    EXPECT_EQ(freeClientsInAsyncFreeQueue(), 0);
    fastpathStagedExecute();
    robj *key = createStringObject("st:e", 4);
    EXPECT_EQ(lookupKeyRead(server.db[0], key), nullptr); /* the closing client's entry did not run */
    decrRefCount(key);
    EXPECT_EQ(freeClientsInAsyncFreeQueue(), 0);
    if (fastpathStagedRepliesPending()) fastpathStagedReplyPhase(1);
    EXPECT_EQ(recv(peer), "");
    fastpathStagedFinish();
    EXPECT_EQ(c->flag.fastpath, 0u);
    EXPECT_EQ(freeClientsInAsyncFreeQueue(), 1);
    close(peer);
}

/* freeClientAsync on a staged client records the same close as freeClient. */
TEST_F(FastpathStagedTest, AsyncFreeRecordsCloseAndFreesAfterReplyBarrier) {
    int peer;
    client *c = newStagedClient(&peer);
    send(peer, "*3\r\n$3\r\nSET\r\n$4\r\nst:f\r\n$1\r\n1\r\n");
    readPhase();
    EXPECT_EQ(c->fp_inflight, 1u);

    freeClientAsync(c);
    EXPECT_EQ(c->flag.fp_detach_sent, 1u);
    EXPECT_EQ(c->control->closing, 1u);
    EXPECT_EQ(freeClientsInAsyncFreeQueue(), 0);
    fastpathStagedExecute();
    robj *key = createStringObject("st:f", 4);
    EXPECT_EQ(lookupKeyRead(server.db[0], key), nullptr);
    decrRefCount(key);
    if (fastpathStagedRepliesPending()) fastpathStagedReplyPhase(1);
    fastpathStagedFinish();
    EXPECT_EQ(c->flag.fastpath, 0u);
    EXPECT_EQ(freeClientsInAsyncFreeQueue(), 1);
    close(peer);
}

/* A client idle on the fast path is released by freeClientAsync at the next iteration. */
TEST_F(FastpathStagedTest, AsyncFreeOfIdleClientFreesNextIteration) {
    int peer;
    client *c = newStagedClient(&peer);
    freeClientAsync(c);
    freeClientAsync(c); /* repeated requests record one close */
    EXPECT_EQ(freeClientsInAsyncFreeQueue(), 0);
    iteration();
    EXPECT_EQ(freeClientsInAsyncFreeQueue(), 1);
    EXPECT_EQ(fastpathClientCount(), 0u);
    close(peer);
}

/* Unwritten replies past the normal output buffer limit hand the client back, and main closes it. */
TEST_F(FastpathStagedTest, OutputBufferLimitClosesClient) {
    clientBufferLimitsConfig saved = server.client_obuf_limits[CLIENT_TYPE_NORMAL];
    server.client_obuf_limits[CLIENT_TYPE_NORMAL].hard_limit_bytes = 64 * 1024;
    long long closed = server.stat_client_outbuf_limit_disconnections;
    int peer;
    client *c = newStagedClient(&peer);
    int sz = 4096;
    setsockopt(peer, SOL_SOCKET, SO_RCVBUF, &sz, sizeof(sz));
    setsockopt(c->conn->fd, SOL_SOCKET, SO_SNDBUF, &sz, sizeof(sz));
    fcntl(c->conn->fd, F_SETFL, fcntl(c->conn->fd, F_GETFL) | O_NONBLOCK); /* as accepted sockets are */
    std::string big(32 * 1024, 'y');
    send(peer, "*3\r\n$3\r\nSET\r\n$5\r\nobufk\r\n$" + std::to_string(big.size()) + "\r\n" + big + "\r\n");
    for (int i = 0; i < 16 && recv(peer).empty(); i++) iteration();

    std::string get = "*2\r\n$3\r\nGET\r\n$5\r\nobufk\r\n";
    for (int i = 0; i < 64 && c->flag.fastpath; i++) {
        send(peer, get);
        iteration();
    }
    EXPECT_EQ(c->flag.fastpath, 0u);
    EXPECT_EQ(c->flag.close_asap, 1u);
    EXPECT_EQ(server.stat_client_outbuf_limit_disconnections, closed + 1);
    EXPECT_EQ(freeClientsInAsyncFreeQueue(), 1);
    close(peer);
    server.client_obuf_limits[CLIENT_TYPE_NORMAL] = saved;
}

/* Turning staged mode off hands every staged client back to main. */
TEST_F(FastpathStagedTest, ReleaseAllHandsEveryClientBack) {
    int pa, pb;
    client *a = newStagedClient(&pa);
    client *b = newStagedClient(&pb);
    EXPECT_EQ(fastpathClientCount(), 2u);
    testOnlyStagedStop();
    EXPECT_EQ(fastpathClientCount(), 0u);
    EXPECT_EQ(a->flag.fastpath, 0u);
    EXPECT_EQ(b->flag.fastpath, 0u);
    EXPECT_EQ(a->control->lifecycle, FP_DETACHED);
    close(pa);
    close(pb);
    freeClient(a);
    freeClient(b);
    ASSERT_EQ(testOnlyStagedStart(), 0);
}

/* A client that never entered the fast path has no control, and reclaiming it is a no-op. */
TEST_F(FastpathStagedTest, ReclaimWithoutControlIsSafe) {
    client *fake = createClient(NULL);
    EXPECT_EQ(fake->control, nullptr);
    fastpathControlReclaim(fake);
    EXPECT_EQ(fake->control, nullptr);
    freeClient(fake);
}

/* The write phase opens a lane phase only for a run of lane entries between barriers that reaches
 * io-threads-parallel-writes-min, so the promise that IO threads will run lanes (which keeps replies
 * from streaming) holds only when such a run exists, however many lane entries the iteration has. */
TEST_F(FastpathStagedTest, LanesWillRunCountsEachSegment) {
    int saved_bits = server.keyspace_shard_bits;
    int saved_min = server.io_threads_parallel_writes_min;
    int saved_pw = server.io_threads_parallel_writes;
    server.io_threads_parallel_writes = 1;
    int saved_batch = server.io_batch_commands;
    server.keyspace_shard_bits = 1; /* lanes need keyspace shards outside cluster mode */
    server.io_threads_parallel_writes_min = 32;
    server.io_batch_commands = 1024;
    int peer;
    client *c = newStagedClient(&peer);
    const std::string ping = "*1\r\n$4\r\nPING\r\n";
    /* A command that is never a lane command (not in fp_lane_commands), so it is a barrier whether or
     * not an earlier suite in this process marked the lane commands (fpMarkLaneCommands runs once per
     * process). SETBIT that leaves a clear bit clear replies :0 every time. */
    const std::string barrier = "*4\r\n$6\r\nSETBIT\r\n$4\r\nst:w\r\n$1\r\n0\r\n$1\r\n0\r\n";

    /* 50 lane entries, every run between barriers 5 long. */
    std::string req, expect;
    for (int i = 1; i <= 10; i++) {
        req += barrier;
        expect += ":0\r\n";
        for (int j = 0; j < 5; j++) {
            req += ping;
            expect += "+PONG\r\n";
        }
    }
    send(peer, req);
    testOnlyFastpathSetLanes(2);
    readPhase();
    EXPECT_EQ(fastpathStagedLanesWillRun(), 0);
    testOnlyFastpathSetLanes(0);
    writeAndReplyPhases();
    EXPECT_EQ(recv(peer), expect);

    /* One run as long as the minimum. */
    req.clear();
    expect.clear();
    req += barrier;
    expect += ":0\r\n";
    for (int j = 0; j < 32; j++) {
        req += ping;
        expect += "+PONG\r\n";
    }
    req += barrier;
    expect += ":0\r\n";
    send(peer, req);
    testOnlyFastpathSetLanes(2);
    readPhase();
    EXPECT_EQ(fastpathStagedLanesWillRun(), 1);
    testOnlyFastpathSetLanes(0);
    writeAndReplyPhases();
    EXPECT_EQ(recv(peer), expect);

    freeStaged(c, peer);
    server.keyspace_shard_bits = saved_bits;
    server.io_threads_parallel_writes_min = saved_min;
    server.io_threads_parallel_writes = saved_pw;
    server.io_batch_commands = saved_batch;
}

/* STR-12: a reply larger than 4 GiB (MGET of a 512 MiB value 9 times) is sent by the fast path
 * from cmdEntry.reply_big with length cmdEntry.reply_big_len. The length is computed as a size_t
 * (bufpos plus every reply block) and must survive the store into the entry; a 32-bit field keeps
 * only total mod 2^32 and the client receives 536871042 of 4831838338 bytes. No allocation here:
 * only the arithmetic the executor performs. */
TEST(FastpathEntryTest, BigReplyLengthSurvivesThe4GiBBoundary) {
    cmdEntry e;
    memset(&e, 0, sizeof(e));
    /* Computed at run time (volatile), as the executor computes it, so the compiler cannot fold the
     * store and reject the test at compile time: the production store is of a run-time size_t. */
    volatile size_t blocks = 9, bulk = 512ull << 20;
    size_t total = blocks * (1 + 9 + 2 + bulk + 2) + 4; /* MGET x9 of a 512 MiB value: 4831838338 */
    ASSERT_EQ(total, 4831838338ull);
    e.reply_big_len = total; /* the store src/fastpath.c performs after assembling the reply list */
    EXPECT_EQ((size_t)e.reply_big_len, total) << "reply_big_len cannot hold a reply above 4 GiB";
    EXPECT_GE(sizeof(e.reply_big_len), sizeof(size_t));
    volatile size_t one = 1;
    size_t just_over = (one << 32) + 1;
    e.reply_big_len = just_over;
    EXPECT_EQ((size_t)e.reply_big_len, just_over);
}

/* Fast GET answers a pure-GET read on the IO thread during the read phase, before the write and
 * reply phases run. This proves the speculate path is exercised, so the gates tested below are
 * real gates and not tests that pass because fast GET never engaged. */
TEST_F(FastpathStagedTest, FastGetServesPureGetInReadPhase) {
    int peer;
    client *c = newStagedClient(&peer);
    c->flag.authenticated = 1;
    fastGetRecomputeSpecAclOk(c); /* default (no) user is unrestricted: gate opens */

    send(peer, "*3\r\n$3\r\nSET\r\n$3\r\nfgk\r\n$1\r\nv\r\n");
    iteration();
    EXPECT_EQ(recv(peer), "+OK\r\n");

    /* A pure-GET read: the reply is written straight to the socket in the read phase. */
    send(peer, "*2\r\n$3\r\nGET\r\n$3\r\nfgk\r\n");
    readPhase();
    EXPECT_EQ(recv(peer), "$1\r\nv\r\n");
    writeAndReplyPhases();
    freeStaged(c, peer);
}

/* SPT-14 / SPT-15 (audit F11): main wrote into c->buf (a tracking push to a REDIRECT target) and
 * owns that write. A fast GET must not answer this read: sending c->buf from the IO thread too
 * would send main's bytes twice (97 pushes seen for 50 invalidations). The GET goes to main. */
TEST_F(FastpathStagedTest, FastGetPuntsWhenMainWroteIntoClientBuffer) {
    int peer;
    client *c = newStagedClient(&peer);
    c->flag.authenticated = 1;
    fastGetRecomputeSpecAclOk(c);
    send(peer, "*3\r\n$3\r\nSET\r\n$7\r\nst:spt1\r\n$1\r\nv\r\n");
    iteration();
    ASSERT_EQ(recv(peer), "+OK\r\n");

    const std::string push = ">2\r\n$10\r\ninvalidate\r\n*1\r\n$3\r\nfoo\r\n";
    ASSERT_LT(push.size(), (size_t)c->buf_usable_size);
    memcpy(c->buf, push.data(), push.size());
    c->bufpos = (int)push.size();
    c->flag.pending_write = 1; /* main queued its own write of these bytes */

    send(peer, "*2\r\n$3\r\nGET\r\n$7\r\nst:spt1\r\n");
    readPhase();
    EXPECT_EQ(c->fp_inflight, 1u) << "the GET must be harvested for main, not answered by fast GET";
    EXPECT_EQ(recv(peer), "") << "the IO thread sent bytes that main owns";
    EXPECT_EQ(mainBuf(c), push) << "main's bytes were consumed or moved by the IO thread";

    /* Hand main's bytes back so freeStaged sees a clean client. */
    c->bufpos = 0;
    c->flag.pending_write = 0;
    writeAndReplyPhases();
    EXPECT_EQ(recv(peer), "$1\r\nv\r\n");
    freeStaged(c, peer);
}

/* Two clients leave the fast path in the same iteration, on different IO threads. Stock Valkey runs
 * commands in the order the server read them; here A's transaction was read first (by thread 2) and
 * B's second (by thread 1), so A's LPUSH must run before B's and B ends up at the head of the list.
 * SCRIPT KILL behind the EVAL it targets is the user-visible case of this order. */
TEST_F(FastpathStagedTest, LeavingClientsHandedBackInArrivalOrderAcrossThreads) {
    fastpathInitThread(2);
    testOnlySetIOThreadReady(2, epoll_create1(EPOLL_CLOEXEC));
    int pa, pb, po;
    client *a = newStagedClient(&pa);
    client *b = newStagedClient(&pb);
    client *o = newStagedClient(&po);
    /* A arrives first and is read by thread 2. */
    send(pa, "*1\r\n$5\r\nMULTI\r\n*3\r\n$5\r\nLPUSH\r\n$4\r\nst:o\r\n$1\r\nA\r\n*1\r\n$4\r\nEXEC\r\n");
    pollerGetNextBatch(stagedPoller(), 0);
    fastpathStagedReadPhase(2);
    EXPECT_EQ(a->control->lifecycle, FP_LEAVING);
    /* B arrives second and is read by thread 1, in the same iteration. */
    send(pb, "*1\r\n$5\r\nMULTI\r\n*3\r\n$5\r\nLPUSH\r\n$4\r\nst:o\r\n$1\r\nB\r\n*1\r\n$4\r\nEXEC\r\n");
    pollerGetNextBatch(stagedPoller(), 0);
    fastpathStagedReadPhase(1);
    EXPECT_EQ(b->control->lifecycle, FP_LEAVING);
    fastpathStagedExecute();
    if (fastpathStagedRepliesPending()) {
        fastpathStagedReplyPhase(1);
        fastpathStagedReplyPhase(2);
    }
    fastpathStagedFinish();
    EXPECT_EQ(a->flag.multi, 0u);
    EXPECT_EQ(b->flag.multi, 0u);
    /* The observer reads the list: A pushed first, so B is the head. */
    send(po, "*4\r\n$6\r\nLRANGE\r\n$4\r\nst:o\r\n$1\r\n0\r\n$2\r\n-1\r\n");
    iteration();
    std::string out = recv(po);
    EXPECT_EQ(out, "*2\r\n$1\r\nB\r\n$1\r\nA\r\n") << "handoff order must follow arrival order, not thread id";
    freeStaged(o, po);
    close(pa);
    freeClient(a);
    close(pb);
    freeClient(b);
    iteration();
    freeClientsInAsyncFreeQueue();
    fastpathFreeThread(2);
    testOnlyClearIOThreadReady(2);
}

/* F14 (review1 gap 1): a client that leaves in the STREAM or REPLY phase (main handed its entries
 * back unexecuted) must be ordered by the read that produced them, not by whatever its IO thread
 * claimed last. Arrival order here: A (thread 2), B (thread 1), R (thread 1, leaves in the READ
 * phase: MULTI), X (thread 2). A failover in progress makes main requeue A, B and X; thread 2
 * delivers in a STREAM phase, thread 1 in the REPLY phase. Main must run the four LPUSHes as
 * A, B, R, X. With the stamp taken from the thread's last claim, A is stamped like X (thread 2's
 * last claim) and runs after B and R. */
TEST_F(FastpathStagedTest, LeaversInStreamAndReplyPhasesKeepArrivalOrder) {
    fastpathInitThread(2);
    testOnlySetIOThreadReady(2, epoll_create1(EPOLL_CLOEXEC));
    int pa, pb, pr, px, po;
    client *a = newStagedClient(&pa);
    client *b = newStagedClient(&pb);
    client *r = newStagedClient(&pr);
    client *x = newStagedClient(&px);
    client *o = newStagedClient(&po);
    auto lpush = [](const char *v) { return std::string("*3\r\n$5\r\nLPUSH\r\n$4\r\nst:q\r\n$1\r\n") + v + "\r\n"; };
    send(pa, lpush("A"));
    pollerGetNextBatch(stagedPoller(), 0);
    fastpathStagedReadPhase(2);
    send(pb, lpush("B"));
    pollerGetNextBatch(stagedPoller(), 0);
    fastpathStagedReadPhase(1);
    send(pr, "*1\r\n$5\r\nMULTI\r\n" + lpush("R") + "*1\r\n$4\r\nEXEC\r\n");
    pollerGetNextBatch(stagedPoller(), 0);
    fastpathStagedReadPhase(1);
    send(px, lpush("X"));
    pollerGetNextBatch(stagedPoller(), 0);
    fastpathStagedReadPhase(2);
    EXPECT_EQ(a->fp_inflight, 1u);
    EXPECT_EQ(b->fp_inflight, 1u);
    EXPECT_EQ(r->control->lifecycle, FP_LEAVING) << "MULTI leaves in the read phase";
    EXPECT_EQ(x->fp_inflight, 1u);

    server.failover_state = FAILOVER_WAIT_FOR_SYNC; /* fpMustRunOnMain: every entry is requeued */
    fastpathStagedStreamBegin();
    fastpathStagedExecuteStreaming();
    fastpathStagedStreamPhase(2); /* A and X leave in the STREAM phase */
    EXPECT_EQ(a->control->lifecycle, FP_LEAVING);
    EXPECT_EQ(x->control->lifecycle, FP_LEAVING);
    fastpathStagedReplyPhase(1); /* B leaves in the REPLY phase */
    EXPECT_EQ(b->control->lifecycle, FP_LEAVING);
    fastpathStagedReplyPhase(2);
    fastpathStagedFinish(); /* hands back and runs the LPUSHes on main */
    server.failover_state = NO_FAILOVER;

    send(po, "*4\r\n$6\r\nLRANGE\r\n$4\r\nst:q\r\n$1\r\n0\r\n$2\r\n-1\r\n");
    iteration();
    EXPECT_EQ(recv(po), "*4\r\n$1\r\nX\r\n$1\r\nR\r\n$1\r\nB\r\n$1\r\nA\r\n")
        << "STREAM/REPLY-phase leavers must be handed back in arrival order (A, B, R, X)";
    freeStaged(o, po);
    for (auto p : {std::make_pair(a, pa), std::make_pair(b, pb), std::make_pair(r, pr), std::make_pair(x, px)}) {
        close(p.second);
        freeClient(p.first);
    }
    iteration();
    freeClientsInAsyncFreeQueue();
    fastpathFreeThread(2);
    testOnlyClearIOThreadReady(2);
}

/* F14 (review1 gap 2): handing back N leavers must cost one pass over the leaving lists plus one
 * sort, not a rescan after every handoff (N^2/2 node visits: a CLIENT PAUSE or a forkless save
 * requeues every client with a batched command in one iteration). The work is counted in list nodes
 * visited by fastpathStagedFinish, not in time. 160 clients, each with one PING batched for main and
 * requeued by a failover in progress: the merge may visit each node a small constant number of
 * times; the rescanning merge visits 160 * 161 / 2 = 12880. */
TEST_F(FastpathStagedTest, ManyLeaversAreMergedInOneSortedPass) {
    const int n = 160;
    std::vector<client *> cs(n);
    std::vector<int> peers(n);
    for (int i = 0; i < n; i++) {
        cs[i] = newStagedClient(&peers[i]);
        send(peers[i], "*2\r\n$4\r\nPING\r\n$1\r\np\r\n");
    }
    readPhase();
    for (int i = 0; i < n; i++) ASSERT_EQ(cs[i]->fp_inflight, 1u) << "client " << i;
    server.failover_state = FAILOVER_WAIT_FOR_SYNC;
    fastpathStagedExecute();
    fastpathStagedReplyPhase(1); /* every client is requeued and leaves */
    for (int i = 0; i < n; i++) ASSERT_EQ(cs[i]->control->lifecycle, FP_LEAVING) << "client " << i;
    uint64_t visits_before = testOnlyFastpathLeaveVisits();
    fastpathStagedFinish();
    server.failover_state = NO_FAILOVER;
    uint64_t visits = testOnlyFastpathLeaveVisits() - visits_before;
    EXPECT_LE(visits, (uint64_t)4 * n) << "the leaving lists were rescanned after handoffs (quadratic merge)";
    EXPECT_GE(visits, (uint64_t)n) << "every leaver is visited at least once";
    for (int i = 0; i < n; i++)
        EXPECT_EQ(recv(peers[i]) + mainBuf(cs[i]), "$1\r\np\r\n") << "client " << i << " was not handed back";
    for (int i = 0; i < n; i++) {
        close(peers[i]);
        freeClient(cs[i]);
    }
    iteration();
    freeClientsInAsyncFreeQueue();
}


/* CLIENT PAUSE ALL (and FAILOVER, which pauses ALL) must hold a fast-path client's reads too. A
 * client admitted before the pause keeps its fd on the staged poller, so without a pause gate in
 * fastGetSpeculateBatch its GET is answered on the IO thread during the pause. Stock holds every
 * read until unpause. This test fails on the superset binary before the fix (the read phase writes
 * the reply) and passes after it. A read is not held by PAUSE WRITE, which the last block checks. */
TEST_F(FastpathStagedTest, ClientPauseAllHoldsFastGet) {
    int peer;
    client *c = newStagedClient(&peer);
    c->flag.authenticated = 1;
    fastGetRecomputeSpecAclOk(c);

    send(peer, "*3\r\n$3\r\nSET\r\n$3\r\npgk\r\n$1\r\nv\r\n");
    iteration();
    EXPECT_EQ(recv(peer), "+OK\r\n");

    /* Pause all client actions (isPausedActions reads server.paused_actions directly). */
    server.paused_actions = PAUSE_ACTION_CLIENT_ALL;
    fastGetRefreshMainGate();
    send(peer, "*2\r\n$3\r\nGET\r\n$3\r\npgk\r\n");
    readPhase();
    EXPECT_EQ(recv(peer), ""); /* held: nothing served in the read phase while paused ALL */

    /* Unpause and finish the iteration: the held GET was harvested for main and now replies. This
     * also drains the client so the next block starts with nothing in flight (a precondition for
     * fast GET to speculate again). */
    server.paused_actions = 0;
    fastGetRefreshMainGate();
    writeAndReplyPhases();
    EXPECT_EQ(recv(peer), "$1\r\nv\r\n");

    /* A read is not held by PAUSE WRITE: fast GET serves it in the read phase. */
    server.paused_actions = PAUSE_ACTION_CLIENT_WRITE;
    fastGetRefreshMainGate();
    send(peer, "*2\r\n$3\r\nGET\r\n$3\r\npgk\r\n");
    readPhase();
    EXPECT_EQ(recv(peer), "$1\r\nv\r\n");

    server.paused_actions = 0;
    fastGetRefreshMainGate();
    writeAndReplyPhases();
    freeStaged(c, peer);
}

/* REPL-15: on a replica whose link is down, with replica-serve-stale-data no, a GET must be
 * refused with -MASTERDOWN, as processCommand does on main. Fast GET must not answer it. */
TEST_F(FastpathStagedTest, FastGetRefusesStaleDataWhenReplicaLinkIsDown) {
    int peer;
    client *c = newStagedClient(&peer);
    send(peer, "*3\r\n$3\r\nSET\r\n$7\r\nst:rpl1\r\n$1\r\nv\r\n");
    iteration();
    ASSERT_EQ(recv(peer), "+OK\r\n");

    sds saved_host = server.primary_host;
    int saved_state = server.repl_state, saved_stale = server.repl_serve_stale_data;
    server.primary_host = sdsnew("primary.example");
    server.primary_port = 1;
    server.repl_state = REPL_STATE_CONNECT;
    server.repl_serve_stale_data = 0;

    send(peer, "*2\r\n$3\r\nGET\r\n$7\r\nst:rpl1\r\n");
    readPhase();
    EXPECT_EQ(c->fp_inflight, 1u) << "the GET must reach main while the link is down";
    EXPECT_EQ(recv(peer), "") << "fast GET served stale data";
    writeAndReplyPhases();
    std::string got = recv(peer);
    EXPECT_EQ(got.compare(0, 11, "-MASTERDOWN"), 0) << "reply was: " << got;

    sdsfree(server.primary_host);
    server.primary_host = saved_host;
    server.repl_state = saved_state;
    server.repl_serve_stale_data = saved_stale;
    freeStaged(c, peer);
}

/* A transaction or a WATCH sends the client to main (MULTI, WATCH, EXEC and UNWATCH are not fast-path
 * commands). Once the transaction is over and nothing is watched, the client is as eligible as it was
 * before, so main readmits it at handoff, exactly as it does after SELECT. The control case runs first
 * to prove the harness readmits. */
TEST_F(FastpathStagedTest, ClientRejoinsAfterSelect) {
    int peer;
    client *c = newStagedClient(&peer);
    send(peer, "*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n");
    iteration();
    EXPECT_EQ(c->flag.fastpath, 1u) << "control: a SELECT client is readmitted at handoff";
    EXPECT_EQ(c->control->lifecycle, FP_ACTIVE);
    freeStaged(c, peer);
}

TEST_F(FastpathStagedTest, ClientRejoinsAfterTransactionEnds) {
    int peer;
    client *c = newStagedClient(&peer);
    send(peer, "*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$4\r\nst:t\r\n$1\r\n1\r\n*1\r\n$4\r\nEXEC\r\n");
    iteration();
    EXPECT_EQ(c->flag.multi, 0u);
    EXPECT_EQ(c->flag.fastpath, 1u) << "a client whose EXEC finished must rejoin the fast path";
    freeStaged(c, peer);
}

TEST_F(FastpathStagedTest, ClientRejoinsAfterUnwatch) {
    int peer;
    client *c = newStagedClient(&peer);
    send(peer, "*2\r\n$5\r\nWATCH\r\n$4\r\nst:w\r\n*1\r\n$7\r\nUNWATCH\r\n");
    iteration();
    EXPECT_EQ(c->flag.fastpath, 1u) << "a client that watches nothing any more must rejoin the fast path";
    freeStaged(c, peer);
}

/* While a key is watched or the transaction is open the client must stay on main: WATCH needs
 * touchWatchedKey from main and MULTI queues commands there. */
TEST_F(FastpathStagedTest, ClientStaysOnMainWhileWatchingOrInMulti) {
    int peer;
    client *c = newStagedClient(&peer);
    send(peer, "*2\r\n$5\r\nWATCH\r\n$4\r\nst:w\r\n");
    iteration();
    EXPECT_EQ(c->flag.fastpath, 0u) << "a client with a watched key is not readmitted";
    EXPECT_EQ(fastpathEligible(c), 0);
    unwatchAllKeys(c);
    EXPECT_EQ(fastpathEligible(c), 1) << "nothing watched any more: eligible again";
    c->flag.multi = 1;
    EXPECT_EQ(fastpathEligible(c), 0) << "an open MULTI keeps the client on main";
    c->flag.multi = 0;
    close(peer);
    freeClient(c);
}

/* N1: main hands batched entries back (fpMustRunOnMain: a forkless save, a pause, a failover, a
 * throttler ...) while the client has a trailing partial command whose "*N" header is parsed but no
 * argument yet (argc 0, multibulklen > 0). The partial's argv must survive the requeue: the parser
 * continues into c->argv without allocating it again. Here a failover in progress makes main return
 * the entries; on S1 the partial's argv is dropped and the next parse writes through a NULL argv. */
TEST_F(FastpathStagedTest, RequeueKeepsTrailingPartialWithNoArgumentYet) {
    int peer;
    client *c = newStagedClient(&peer);
    send(peer, "*2\r\n$4\r\nPING\r\n$1\r\na\r\n*2\r\n$4\r\nPING\r\n$1\r\nb\r\n*2\r\n$4\r\n");
    readPhase();
    /* The trailing partial: header and first bulk length parsed, no argument yet. */
    EXPECT_EQ(c->control->lifecycle, FP_ACTIVE);
    EXPECT_EQ(c->argc, 0);
    EXPECT_EQ(c->multibulklen, 2);
    EXPECT_EQ(c->bulklen, 4);
    ASSERT_NE(c->argv, nullptr);

    server.failover_state = FAILOVER_WAIT_FOR_SYNC; /* fpMustRunOnMain: every entry is requeued */
    writeAndReplyPhases();
    server.failover_state = NO_FAILOVER;
    std::string out = recv(peer) + mainBuf(c);
    EXPECT_EQ(out, "$1\r\na\r\n$1\r\nb\r\n");
    /* The parser state and its argv still go together, wherever the client is now. */
    EXPECT_EQ(c->multibulklen, 2);
    ASSERT_TRUE(c->multibulklen == 0 || c->argv != nullptr)
        << "partial parse state with a NULL argv: the next read writes through it";

    c->bufpos = 0;
    send(peer, "PING\r\n$1\r\nc\r\n");
    if (c->flag.fastpath) {
        iteration();
    } else {
        readQueryFromClient(c->conn);
    }
    out = recv(peer) + mainBuf(c);
    EXPECT_EQ(out, "$1\r\nc\r\n");
    EXPECT_EQ(c->multibulklen, 0);
    if (c->flag.fastpath) {
        freeStaged(c, peer);
    } else {
        close(peer);
        freeClient(c);
    }
}

/* A client that closes while its last bytes are still unread: the peer's data and its close arrive
 * before the poller reports the fd, so one edge-triggered report carries both. The read phase must
 * read the bytes and still notice the close, or the client stays registered forever: no new edge
 * ever comes for a socket whose state no longer changes. */
TEST_F(FastpathStagedTest, TrailingBytesAndCloseInOneEdgeFreeTheClient) {
    int peer;
    client *c = newStagedClient(&peer);
    send(peer, "*3\r\n$3\r\nSET\r\n$4\r\nlkk1\r\n$1\r\n1\r\n");
    iteration();
    EXPECT_EQ(recv(peer), "+OK\r\n");

    /* The header of a second command with no argument yet, then the close, with no read phase
     * between them: the fd reports readable and hung up in one edge. */
    send(peer, "*3\r\n$3\r\nSET\r\n$4\r\nlkk1\r\n");
    close(peer);
    bool freed = iterateUntilFreed(c, 4);
    EXPECT_TRUE(freed) << "closed client with a trailing partial was never freed; fastpath_clients="
                       << fastpathClientCount();
    EXPECT_EQ(fastpathClientCount(), 0u);
    if (!freed) freeClient(c);
}

/* The same close after the client went to main under CLIENT PAUSE and came back: the paused command is
 * postponed on main, runs at unpause, and the quiescent client is readmitted to the fast path with
 * a fresh poller registration. The close must still be noticed. */
TEST_F(FastpathStagedTest, ClientReadmittedAfterPauseIsFreedOnClose) {
    int peer;
    client *c = newStagedClient(&peer);
    c->flag.authenticated = 1;
    send(peer, "*3\r\n$3\r\nSET\r\n$4\r\nlkk2\r\n$1\r\n1\r\n");
    readPhase();

    /* Pause all actions: the batch is requeued and the client leaves for main, where the postponed
     * SET blocks it (BLOCKED_POSTPONE). */
    server.paused_actions = PAUSE_ACTION_CLIENT_ALL;
    fastGetRefreshMainGate();
    writeAndReplyPhases();
    EXPECT_EQ(c->flag.fastpath, 0u) << "client should have been handed back to main under pause";
    EXPECT_EQ(c->flag.blocked, 1u);

    /* Unpause and run the deferred re-processing, as beforeSleep does: the SET runs on main and the
     * quiescent client rejoins the fast path. */
    server.paused_actions = 0;
    fastGetRefreshMainGate();
    unblockPostponedClients();
    processUnblockedClients();
    EXPECT_EQ(c->flag.blocked, 0u);
    ASSERT_EQ(c->flag.fastpath, 1u) << "control: a quiescent client is readmitted after unpause";
    EXPECT_EQ(recv(peer) + mainBuf(c), "+OK\r\n");

    send(peer, "*3\r\n$3\r\nSET\r\n$4\r\nlkk2\r\n");
    close(peer);
    bool freed = iterateUntilFreed(c, 4);
    EXPECT_TRUE(freed) << "readmitted client that closed was never freed; fastpath_clients="
                       << fastpathClientCount();
    EXPECT_EQ(fastpathClientCount(), 0u);
    if (!freed) freeClient(c);
}

