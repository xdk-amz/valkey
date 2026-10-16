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

#include <cstring>
#include <string>
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
        moduleInitModulesSystem();
        server.commands = hashtableCreate(&commandSetType);
        server.orig_commands = hashtableCreate(&commandSetType);
        populateCommandTable();
        ACLInit();

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
        connTypeInitialize();
        initSharedQueryBuf();
        testOnlyInitIOThreadQueues();
        fastpathInitThread(1);
        testOnlySetIOThreadReady(1, epoll_create1(EPOLL_CLOEXEC));
    }

    static void TearDownTestSuite() {
        fastpathFreeThread(1);
        testOnlyFreeIOThreadQueues();
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
