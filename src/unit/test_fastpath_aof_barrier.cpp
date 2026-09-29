/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* The appendfsync-always reply-publication barrier: main runs a fast-path batch but, when a
 * non-requeued write ran under appendfsync always, holds the whole batch off the owner return
 * ring until fastpathReleaseDurableReplies() runs after the beforeSleep AOF fsync. These tests
 * drive one IO thread inline and prove a held write reply never reaches the socket before its
 * release, that ordering is preserved, that other AOF modes deliver immediately, and that a
 * terminal request against a held client discards its batch instead of writing it. */

#include "generated_wrappers.hpp"

#include <cstring>
#include <fcntl.h>
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

static const char *ab_logfile = "/dev/null";

/* RESP payloads used across the tests. */
static const char *SET_FOO_BAR = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
static const char *GET_FOO = "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
static const char *GET_NOKEY = "*2\r\n$3\r\nGET\r\n$5\r\nnokey\r\n";

class FastpathAofBarrierTest : public ::testing::Test {
  protected:
    static void SetUpTestSuite() {
        monotonicInit();
        memset(&server, 0, sizeof(server));
        server.hz = CONFIG_DEFAULT_HZ;
        server.logfile = const_cast<char *>(ab_logfile);
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
        /* replication_allowed stays 0 (memset), so command propagation is skipped: the hold decision reads
         * only aof_state/aof_fsync, so a write batch is held without needing a real AOF file or repl backlog. */
        connTypeInitialize();
        initSharedQueryBuf();
        testOnlyInitIOThreadQueues();
        fastpathInitThread(1);
        testOnlySetIOThreadReady(1, epoll_create1(EPOLL_CLOEXEC));
    }

    static void TearDownTestSuite() {
        fastpathFreeThread(1);
        testOnlyFreeIOThreadQueues();
        aeDeleteEventLoop(server.el);
        server.el = NULL;
    }

    void SetUp() override {
        testOnlyInitIOThreadQueues(); /* forget hand-off messages of the previous test */
        server.aof_state = AOF_OFF;
        server.aof_fsync = AOF_FSYNC_NO;
        if (server.aof_buf) sdsfree(server.aof_buf);
        server.aof_buf = sdsempty(); /* nothing written and not yet fsynced */
        fastpathWorkerReopen(1); /* the previous test drained the thread; bring it back to OPEN */
    }

    /* A connected client admitted to thread 1, its read end non-blocking so an undelivered reply reads as empty. */
    static client *newFpClient(int *peer) {
        int sv[2];
        EXPECT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
        EXPECT_EQ(fcntl(sv[1], F_SETFL, O_NONBLOCK), 0);
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
        EXPECT_EQ(fastpathProcessReturns(1), 1); /* the attach request: the thread takes ownership */
        *peer = sv[1];
        return c;
    }

    static void send(int peer, const char *req) {
        EXPECT_EQ(write(peer, req, strlen(req)), (ssize_t)strlen(req));
    }

    /* Non-blocking drain of whatever is on the socket right now; "" when nothing was written. */
    static std::string recvNow(int peer) {
        char buf[4096];
        ssize_t n = read(peer, buf, sizeof(buf));
        return n > 0 ? std::string(buf, n) : std::string();
    }

    /* One command becomes one submitted batch: feed it, parse it, publish it. */
    static void submitOne(client *c, int peer, const char *req) {
        send(peer, req);
        fastpathClientReadable(1, c);
        fastpathSubmitPending(1);
    }

    /* Quiesce, deliver, hand back and free a still-owned client, leaving the thread OPEN for the next test. */
    static void cleanupOpen(client *c, int peer) {
        fastpathWorkerQuiesce(1);
        fastpathReleaseDurableReplies(); /* release any straggler so the held batch cannot strand the drain */
        fastpathProcessReturns(1);
        if (c->flag.fastpath) fastpathHandoffDone(c, 0);
        close(peer);
        freeClient(c);
    }
};

/* A write executed under appendfsync always is held: nothing returns to the socket until the
 * post-fsync release publishes it, and only then is the client's in-flight charge cleared. */
TEST_F(FastpathAofBarrierTest, WriteBatchHeldUntilReleaseUnderAofAlways) {
    server.aof_state = AOF_ON;
    server.aof_fsync = AOF_FSYNC_ALWAYS;
    int peer;
    client *c = newFpClient(&peer);

    submitOne(c, peer, SET_FOO_BAR);
    EXPECT_EQ(fastpathDrain(), 1);            /* executed on main... */
    EXPECT_EQ(fastpathPendingBatches(1), 1u); /* ...but held for durability, not returned */
    EXPECT_EQ(fastpathProcessReturns(1), 0);  /* the IO thread has nothing to deliver */
    EXPECT_EQ(c->fp_inflight, 1u);            /* still charged and pinned while held */
    EXPECT_EQ(recvNow(peer), "");             /* the reply has not reached the socket */

    fastpathReleaseDurableReplies();          /* stands in for the beforeSleep post-fsync release */
    EXPECT_EQ(fastpathPendingBatches(1), 0u);
    EXPECT_EQ(fastpathProcessReturns(1), 1);  /* now published to the owner ring and delivered */
    EXPECT_EQ(c->fp_inflight, 0u);
    EXPECT_EQ(recvNow(peer), "+OK\r\n");

    cleanupOpen(c, peer);
}

/* Initial AOF rewrite uses the same appendfsync-always durability barrier as steady AOF_ON. */
TEST_F(FastpathAofBarrierTest, WriteBatchHeldDuringAofWaitRewrite) {
    server.aof_state = AOF_WAIT_REWRITE;
    server.aof_fsync = AOF_FSYNC_ALWAYS;
    int peer;
    client *c = newFpClient(&peer);

    submitOne(c, peer, SET_FOO_BAR);
    EXPECT_EQ(fastpathDrain(), 1);
    EXPECT_EQ(fastpathPendingBatches(1), 1u);
    EXPECT_EQ(fastpathProcessReturns(1), 0);
    EXPECT_EQ(recvNow(peer), "");

    fastpathReleaseDurableReplies();
    EXPECT_EQ(fastpathProcessReturns(1), 1);
    EXPECT_EQ(recvNow(peer), "+OK\r\n");

    cleanupOpen(c, peer);
}

/* A read-only batch under appendfsync always is never held; it returns in the same drain. */
TEST_F(FastpathAofBarrierTest, ReadOnlyBatchReturnsImmediatelyUnderAofAlways) {
    server.aof_state = AOF_ON;
    server.aof_fsync = AOF_FSYNC_ALWAYS;
    int peer;
    client *c = newFpClient(&peer);

    submitOne(c, peer, GET_NOKEY);
    EXPECT_EQ(fastpathDrain(), 1);
    EXPECT_EQ(fastpathPendingBatches(1), 0u); /* not a durable write: not held */
    EXPECT_EQ(fastpathProcessReturns(1), 1);
    EXPECT_EQ(c->fp_inflight, 0u);
    EXPECT_EQ(recvNow(peer), "$-1\r\n");

    cleanupOpen(c, peer);
}

/* A read may observe a write another client made this loop that is not yet fsynced: its reply waits too. */
TEST_F(FastpathAofBarrierTest, ReadHeldWhileUnfsyncedWritePending) {
    server.aof_state = AOF_ON;
    server.aof_fsync = AOF_FSYNC_ALWAYS;
    server.aof_buf = sdscat(server.aof_buf, "*1\r\n$4\r\nPING\r\n"); /* another client's write, not yet fsynced */
    int peer;
    client *c = newFpClient(&peer);

    submitOne(c, peer, GET_NOKEY);
    EXPECT_EQ(fastpathDrain(), 1);
    EXPECT_EQ(fastpathPendingBatches(1), 1u);
    EXPECT_EQ(fastpathProcessReturns(1), 0);
    EXPECT_EQ(recvNow(peer), "");

    sdsclear(server.aof_buf); /* the beforeSleep flush wrote and fsynced it */
    fastpathReleaseDurableReplies();
    EXPECT_EQ(fastpathProcessReturns(1), 1);
    EXPECT_EQ(recvNow(peer), "$-1\r\n");

    cleanupOpen(c, peer);
}

/* With AOF off a write returns immediately: the barrier adds nothing outside appendfsync always. */
TEST_F(FastpathAofBarrierTest, WriteBatchImmediateWhenAofOff) {
    server.aof_state = AOF_OFF;
    int peer;
    client *c = newFpClient(&peer);

    submitOne(c, peer, SET_FOO_BAR);
    EXPECT_EQ(fastpathDrain(), 1);
    EXPECT_EQ(fastpathPendingBatches(1), 0u);
    EXPECT_EQ(fastpathProcessReturns(1), 1);
    EXPECT_EQ(recvNow(peer), "+OK\r\n");

    cleanupOpen(c, peer);
}

/* appendfsync everysec is not the always contract, so a write returns immediately too. */
TEST_F(FastpathAofBarrierTest, WriteBatchImmediateWhenAofEverysec) {
    server.aof_state = AOF_ON;
    server.aof_fsync = AOF_FSYNC_EVERYSEC;
    int peer;
    client *c = newFpClient(&peer);

    submitOne(c, peer, SET_FOO_BAR);
    EXPECT_EQ(fastpathDrain(), 1);
    EXPECT_EQ(fastpathPendingBatches(1), 0u);
    EXPECT_EQ(fastpathProcessReturns(1), 1);
    EXPECT_EQ(recvNow(peer), "+OK\r\n");

    cleanupOpen(c, peer);
}

/* A batch mixing a read and a write is held as one unit, so the client's replies keep their order. */
TEST_F(FastpathAofBarrierTest, MixedReadWriteBatchHeldWhole) {
    server.aof_state = AOF_ON;
    server.aof_fsync = AOF_FSYNC_ALWAYS;
    int peer;
    client *c = newFpClient(&peer);

    /* One read of two pipelined commands harvests both into a single batch. */
    send(peer, GET_NOKEY);
    send(peer, SET_FOO_BAR);
    fastpathClientReadable(1, c);
    EXPECT_EQ(c->fp_inflight, 2u);
    fastpathSubmitPending(1);

    EXPECT_EQ(fastpathDrain(), 2);
    EXPECT_EQ(fastpathPendingBatches(1), 1u); /* the whole mixed batch is held on the write */
    EXPECT_EQ(fastpathProcessReturns(1), 0);
    EXPECT_EQ(recvNow(peer), "");

    fastpathReleaseDurableReplies();
    EXPECT_EQ(fastpathProcessReturns(1), 1);
    EXPECT_EQ(c->fp_inflight, 0u);
    EXPECT_EQ(recvNow(peer), "$-1\r\n+OK\r\n"); /* read reply then write reply, in send order */

    cleanupOpen(c, peer);
}

/* Once a thread holds a batch every later batch is held behind it, so a read submitted after a
 * held write does not overtake it: the owner ring stays per-thread FIFO and the read sees the write. */
TEST_F(FastpathAofBarrierTest, HeldBatchesReleaseInFifoOrder) {
    server.aof_state = AOF_ON;
    server.aof_fsync = AOF_FSYNC_ALWAYS;
    int peer;
    client *c = newFpClient(&peer);

    submitOne(c, peer, SET_FOO_BAR); /* batch 1: a durable write, held */
    EXPECT_EQ(fastpathDrain(), 1);
    EXPECT_EQ(fastpathPendingBatches(1), 1u);
    EXPECT_EQ(fastpathProcessReturns(1), 0);

    submitOne(c, peer, GET_FOO); /* batch 2: read-only, but held behind the write for FIFO */
    EXPECT_EQ(fastpathDrain(), 1);
    EXPECT_EQ(fastpathPendingBatches(1), 2u);
    EXPECT_EQ(fastpathProcessReturns(1), 0);
    EXPECT_EQ(recvNow(peer), "");

    fastpathReleaseDurableReplies();
    EXPECT_EQ(fastpathPendingBatches(1), 0u);
    EXPECT_EQ(fastpathProcessReturns(1), 2);   /* both batches published in held order */
    EXPECT_EQ(c->fp_inflight, 0u);
    EXPECT_EQ(recvNow(peer), "+OK\r\n$3\r\nbar\r\n"); /* write reply first; the later read sees "bar" */

    cleanupOpen(c, peer);
}

/* A terminal request while a batch is held: on release the batch is discarded, not written, its bytes
 * released so accounting balances, its in-flight charge cleared, and the client closes without deadlock. */
TEST_F(FastpathAofBarrierTest, TerminalRequestWhileHeldDiscardsOnRelease) {
    server.aof_state = AOF_ON;
    server.aof_fsync = AOF_FSYNC_ALWAYS;
    int peer;
    client *c = newFpClient(&peer);
    ClientControl *cc = c->control;

    submitOne(c, peer, SET_FOO_BAR);
    EXPECT_EQ(fastpathDrain(), 1);
    EXPECT_EQ(fastpathPendingBatches(1), 1u);
    EXPECT_GT(fastpathReplyOutstanding(cc), 0u); /* the reply bytes are charged while held */

    fastpathRequestDetach(c);           /* main asks to terminally close the held client */
    EXPECT_EQ(fastpathProcessReturns(1), 1);     /* the owner consumes the detach and marks it closing */
    EXPECT_EQ(c->control->lifecycle, FP_CLOSING);
    EXPECT_TRUE(fastpathDetachConsumed(c));
    EXPECT_EQ(c->fp_inflight, 1u);               /* the held batch still pins it */

    fastpathReleaseDurableReplies();             /* released to the ring, then discarded on delivery */
    EXPECT_EQ(fastpathProcessReturns(1), 1);
    EXPECT_EQ(c->fp_inflight, 0u);               /* charge cleared without a write */
    EXPECT_EQ(recvNow(peer), "");                /* nothing was written to the closing client */
    EXPECT_EQ(fastpathReplyOutstanding(cc), 0u); /* released bytes balance the produced charge */

    fastpathHandoffDone(c, 1); /* the FP_CLOSE hand-off frees the client */
    close(peer);
    EXPECT_EQ(fastpathWorkerOwnedClients(1), 0u);
}

/* A held batch keeps the thread from draining through a quiesce until the post-fsync release lets the
 * batch return; only then does the owner drain, which is the thread-destruction precondition. */
TEST_F(FastpathAofBarrierTest, QuiesceBlockedByHeldBatchUntilRelease) {
    server.aof_state = AOF_ON;
    server.aof_fsync = AOF_FSYNC_ALWAYS;
    int peer;
    client *c = newFpClient(&peer);

    submitOne(c, peer, SET_FOO_BAR);
    EXPECT_EQ(fastpathDrain(), 1);
    EXPECT_EQ(fastpathPendingBatches(1), 1u);

    fastpathWorkerQuiesce(1);
    fastpathProcessReturns(1);            /* the held batch keeps in-flight > 0, so the client cannot hand off */
    EXPECT_EQ(fastpathWorkerRole(1), FP_ROLE_QUIESCING);
    EXPECT_FALSE(fastpathWorkerDrained(1));
    EXPECT_EQ(c->fp_inflight, 1u);

    fastpathReleaseDurableReplies();
    EXPECT_EQ(fastpathPendingBatches(1), 0u);
    fastpathProcessReturns(1);            /* delivered, in-flight cleared, client handed off, thread drained */
    EXPECT_EQ(c->fp_inflight, 0u);
    EXPECT_EQ(recvNow(peer), "+OK\r\n");
    EXPECT_EQ(fastpathWorkerRole(1), FP_ROLE_DRAINED);

    fastpathHandoffDone(c, 0);
    EXPECT_TRUE(fastpathWorkerDrained(1));
    close(peer);
    freeClient(c);
}
