/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Drives one fast-path IO thread inline through its lifecycle: admission via the
 * return ring, quiesce with batches in flight and unpublished, detach consumption,
 * and the emptiness preconditions for destroying the thread. */

#include "generated_wrappers.hpp"

#include <cstring>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

extern "C" {
#include "connection.h"
#include "fastpath.h"
#include "io_threads.h"
#include "module.h"
#include "server.h"
extern hashtableType commandSetType;
extern dictType keylistDictType;
void createSharedObjects(void);
}

static const char *lc_logfile = "/dev/null";

class FastpathLifecycleTest : public ::testing::Test {
  protected:
    static void SetUpTestSuite() {
        monotonicInit();
        memset(&server, 0, sizeof(server));
        server.hz = CONFIG_DEFAULT_HZ;
        server.logfile = const_cast<char *>(lc_logfile);
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
        server.io_batch_inflight = 16;
        server.io_batch_hold_us = 0;
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
        testOnlyInitIOThreadQueues(); /* forget hand-off messages of the previous test */
    }

    /* A connected client admitted to thread 1, with the peer end of its socket in *peer. */
    static client *newFastpathClient(int *peer) {
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
        EXPECT_EQ(c->io_tid, 1);
        EXPECT_EQ(fastpathWorkerOwnedClients(1), 1u);
        *peer = sv[1];
        return c;
    }

    static void send(int peer, const char *req) {
        EXPECT_EQ(write(peer, req, strlen(req)), (ssize_t)strlen(req));
    }

    static std::string recv(int peer) {
        char buf[4096];
        ssize_t n = read(peer, buf, sizeof(buf));
        return n > 0 ? std::string(buf, n) : std::string();
    }

    static std::string mainBuf(client *c) {
        return std::string(c->buf, c->bufpos);
    }

    static void freeHandedOff(client *c, int peer) {
        close(peer);
        freeClient(c);
    }
};

static const char *INCR_A = "*2\r\n$4\r\nINCR\r\n$4\r\nlc:a\r\n";
static const char *INCR_B = "*2\r\n$4\r\nINCR\r\n$4\r\nlc:b\r\n";

/* Commands the thread parsed but never published are cancelled, not published, once it quiesces:
 * they run on main after the hand-off, in order, and the thread ends up drained. */
TEST_F(FastpathLifecycleTest, QuiesceCancelsUnpublishedBatchAndHandsOffInOrder) {
    int peer;
    client *c = newFastpathClient(&peer);
    EXPECT_EQ(fastpathProcessReturns(1), 1); /* the attach request: thread takes ownership */
    EXPECT_EQ(c->fp_state, FP_ACTIVE);

    std::string req = std::string(INCR_A) + INCR_A + INCR_A;
    send(peer, req.c_str());
    fastpathClientReadable(1, c);
    EXPECT_EQ(c->fp_inflight, 3u);
    EXPECT_EQ(fastpathDrain(), 0); /* below the batch size, nothing published yet */

    fastpathWorkerQuiesce(1);
    EXPECT_EQ(fastpathWorkerRole(1), FP_ROLE_QUIESCING);
    fastpathProcessReturns(1);
    EXPECT_EQ(fastpathDrain(), 0); /* nothing was published after the quiesce */
    EXPECT_EQ(c->fp_state, FP_LEAVING);
    EXPECT_EQ(c->fp_inflight, 0u);
    EXPECT_EQ(c->argc, 2);
    EXPECT_EQ(c->cmd_queue.len - c->cmd_queue.off, 2);
    EXPECT_EQ(fastpathWorkerRole(1), FP_ROLE_DRAINED);
    EXPECT_FALSE(fastpathWorkerDrained(1)); /* main has not taken the client back yet */
    EXPECT_EQ(fastpathWorkerReopen(1), 0);

    fastpathHandoffDone(c, 0);
    EXPECT_EQ(c->flag.fastpath, 0u);
    EXPECT_EQ(mainBuf(c), ":1\r\n:2\r\n:3\r\n");
    EXPECT_TRUE(fastpathWorkerDrained(1));
    EXPECT_EQ(fastpathWorkerReopen(1), 1);
    EXPECT_EQ(fastpathWorkerRole(1), FP_ROLE_OPEN);
    freeHandedOff(c, peer);
}

/* A batch already on main returns and its replies are written before the unpublished
 * remainder is cancelled; the client sees every reply exactly once, in order. */
TEST_F(FastpathLifecycleTest, QuiesceWaitsForInflightBatchThenCancelsTheRest) {
    int peer;
    client *c = newFastpathClient(&peer);
    fastpathProcessReturns(1);

    std::string two = std::string(INCR_B) + INCR_B;
    send(peer, two.c_str());
    fastpathClientReadable(1, c);
    fastpathSubmitPending(1); /* published: one batch of two */
    send(peer, two.c_str());
    fastpathClientReadable(1, c); /* two more, unpublished */
    EXPECT_EQ(c->fp_inflight, 4u);

    fastpathWorkerQuiesce(1);
    fastpathProcessReturns(1); /* observed; a batch is still out, so nothing is cancelled */
    EXPECT_EQ(c->fp_state, FP_LEAVING);
    EXPECT_EQ(fastpathWorkerRole(1), FP_ROLE_QUIESCING);
    EXPECT_EQ(c->fp_inflight, 4u);

    EXPECT_EQ(fastpathDrain(), 2); /* main runs the published batch only */
    fastpathProcessReturns(1);     /* replies written, remainder requeued, client handed off */
    EXPECT_EQ(recv(peer), ":1\r\n:2\r\n");
    EXPECT_EQ(c->fp_inflight, 0u);
    EXPECT_EQ(fastpathWorkerRole(1), FP_ROLE_DRAINED);
    EXPECT_EQ(fastpathDrain(), 0);

    fastpathHandoffDone(c, 0);
    EXPECT_EQ(mainBuf(c), ":3\r\n:4\r\n");
    EXPECT_TRUE(fastpathWorkerDrained(1));
    EXPECT_EQ(fastpathWorkerReopen(1), 1);
    freeHandedOff(c, peer);
}

TEST_F(FastpathLifecycleTest, QuiesceIsIdempotentAndReopenWaitsForDrain) {
    fastpathWorkerQuiesce(1);
    fastpathWorkerQuiesce(1);
    EXPECT_EQ(fastpathWorkerRole(1), FP_ROLE_QUIESCING);
    EXPECT_EQ(fastpathWorkerReopen(1), 0);
    fastpathProcessReturns(1); /* nothing owned: drained at once */
    EXPECT_EQ(fastpathWorkerRole(1), FP_ROLE_DRAINED);
    fastpathWorkerQuiesce(1); /* no effect on a drained thread */
    EXPECT_EQ(fastpathWorkerRole(1), FP_ROLE_DRAINED);
    EXPECT_TRUE(fastpathWorkerDrained(1));
    EXPECT_EQ(fastpathWorkerReopen(1), 1);
    EXPECT_EQ(fastpathWorkerReopen(1), 1);
    EXPECT_EQ(fastpathWorkerRole(1), FP_ROLE_OPEN);
}

/* Admission racing a quiesce: the thread registers the client and hands it straight back. */
TEST_F(FastpathLifecycleTest, AttachDuringQuiesceIsHandedBack) {
    int peer;
    client *c = newFastpathClient(&peer);
    fastpathWorkerQuiesce(1); /* before the thread saw the attach */
    fastpathProcessReturns(1);
    EXPECT_EQ(c->fp_state, FP_LEAVING);
    EXPECT_EQ(fastpathWorkerRole(1), FP_ROLE_DRAINED);
    fastpathHandoffDone(c, 0);
    EXPECT_TRUE(fastpathWorkerDrained(1));
    EXPECT_EQ(fastpathWorkerReopen(1), 1);
    freeHandedOff(c, peer);
}

/* Main may free a detached client only after the thread consumed the request, whether the
 * thread still owned the client (it closes it) or had already handed it back (it ignores it). */
TEST_F(FastpathLifecycleTest, DetachIsConsumedBeforeMainMayFree) {
    int peer;
    client *c = newFastpathClient(&peer);
    fastpathProcessReturns(1);
    fastpathRequestDetach(c);
    EXPECT_EQ(c->flag.fp_detach_sent, 1u);
    EXPECT_FALSE(fastpathDetachConsumed(c));
    EXPECT_FALSE(fastpathWorkerDrained(1));
    fastpathProcessReturns(1); /* owned: closes, hands back FP_CLOSE */
    EXPECT_EQ(c->fp_state, FP_CLOSING);
    EXPECT_TRUE(fastpathDetachConsumed(c));
    EXPECT_EQ(c->flag.fp_detach_sent, 0u);
    fastpathHandoffDone(c, 1); /* frees it */
    close(peer);
    EXPECT_TRUE(fastpathWorkerDrained(1) || fastpathWorkerRole(1) == FP_ROLE_OPEN);

    /* Handed back first, detach second: the stale request is ignored and still gates the free. */
    c = newFastpathClient(&peer);
    fastpathProcessReturns(1);
    fastpathWorkerQuiesce(1);
    fastpathProcessReturns(1); /* FP_HANDOFF sent, thread no longer owns c */
    EXPECT_EQ(fastpathWorkerRole(1), FP_ROLE_DRAINED);
    fastpathRequestDetach(c); /* main has not processed the hand-off yet */
    fastpathHandoffDone(c, 0);
    EXPECT_EQ(c->flag.fastpath, 0u);
    EXPECT_EQ(c->flag.fp_detach_sent, 1u); /* not freed: the request is still in the ring */
    EXPECT_FALSE(fastpathDetachConsumed(c));
    EXPECT_FALSE(fastpathWorkerDrained(1));
    fastpathProcessReturns(1); /* registry miss: ignored */
    EXPECT_TRUE(fastpathDetachConsumed(c));
    EXPECT_TRUE(fastpathWorkerDrained(1));
    EXPECT_EQ(fastpathWorkerReopen(1), 1);
    freeHandedOff(c, peer);
}

/* Destroying a thread that still owns a client is a precondition failure. */
TEST_F(FastpathLifecycleTest, FreeThreadRequiresEmptyRegistry) {
    fastpathInitThread(2);
    testOnlySetIOThreadReady(2, epoll_create1(EPOLL_CLOEXEC));
    int peer_a, peer_b;
    client *a = newFastpathClient(&peer_a);
    client *b = nullptr;
    if (a->io_tid != 2) {
        int sv[2];
        ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
        connection *conn = connCreateAccepted(connectionByType(CONN_TYPE_SOCKET), sv[0], NULL);
        conn->state = CONN_STATE_CONNECTED;
        b = createClient(conn);
        b->fp_peer = a->fp_peer;
        b->fp_local = a->fp_local;
        ASSERT_EQ(fastpathAttach(b), C_OK);
        peer_b = sv[1];
        ASSERT_EQ(b->io_tid, 2);
    }
    fastpathProcessReturns(1);
    fastpathProcessReturns(2);
    EXPECT_DEATH(fastpathFreeThread(2), "");

    fastpathWorkerQuiesce(1);
    fastpathWorkerQuiesce(2);
    fastpathProcessReturns(1);
    fastpathProcessReturns(2);
    fastpathHandoffDone(a, 0);
    freeHandedOff(a, peer_a);
    if (b) {
        fastpathHandoffDone(b, 0);
        freeHandedOff(b, peer_b);
    }
    EXPECT_TRUE(fastpathWorkerDrained(2));
    fastpathFreeThread(2);
    EXPECT_EQ(fastpathWorkerReopen(1), 1);
}
