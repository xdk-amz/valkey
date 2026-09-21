/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Contract tests for the shared ClientControl and its generation-checked ClientHandle:
 * a detached entry carries a handle, not a connection pointer; main resolves control state
 * without dereferencing the connection; a reused slot is caught by generation without touching
 * freed storage; lifecycle requests are published by anyone but executed only by the owner, with
 * a deterministic precedence; and reply-memory accounting produces and releases the same unit so
 * outstanding stays exact across partial writes, discards, batch recycling, and reclamation. */

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
#include "server.h"
extern hashtableType commandSetType;
extern dictType keylistDictType;
void createSharedObjects(void);
}

static const char *cc_logfile = "/dev/null";

class FastpathClientControlTest : public ::testing::Test {
  protected:
    static void SetUpTestSuite() {
        monotonicInit();
        memset(&server, 0, sizeof(server));
        server.hz = CONFIG_DEFAULT_HZ;
        server.logfile = const_cast<char *>(cc_logfile);
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
        EXPECT_EQ(fastpathProcessReturns(1), 1); /* the attach request: thread takes ownership */
        *peer = sv[1];
        return c;
    }

    static void send(int peer, const char *req) {
        EXPECT_EQ(write(peer, req, strlen(req)), (ssize_t)strlen(req));
    }

    static std::string recv(int peer) {
        char buf[65536];
        ssize_t n = read(peer, buf, sizeof(buf));
        return n > 0 ? std::string(buf, n) : std::string();
    }

    /* Read exactly want bytes (a large reply can arrive across several reads on a socketpair). */
    static std::string recvExact(int peer, size_t want) {
        std::string out;
        char buf[65536];
        while (out.size() < want) {
            ssize_t n = read(peer, buf, sizeof(buf));
            if (n <= 0) break;
            out.append(buf, n);
        }
        return out;
    }

    /* Quiesce the thread so it hands the client back and unregisters it, then let main free it. */
    static void quiesceAndFree(client *c, int peer) {
        fastpathWorkerQuiesce(1);
        fastpathProcessReturns(1);
        if (c->flag.fastpath) fastpathHandoffDone(c, 0);
        EXPECT_EQ(fastpathWorkerOwnedClients(1), 0u);
        fastpathWorkerReopen(1);
        close(peer);
        freeClient(c);
    }
};

/* Layout and alignment: the same facts the source static_asserts pin, checked at runtime so the file
 * documents the ABI the entries and the two reply counters depend on. */
TEST_F(FastpathClientControlTest, HandleAndControlLayoutIsCompact) {
    /* A handle is exactly {control ref, generation}: two pointer-sized words, pointer aligned. */
    EXPECT_EQ(sizeof(ClientHandle), 2 * sizeof(void *));
    EXPECT_EQ(alignof(ClientHandle), alignof(void *));

    /* The detached entry leads with the handle, not a connection pointer, and field packing keeps the
     * 16-byte handle inside the base branch's 160-byte command-entry budget. */
    EXPECT_EQ(offsetof(cmdEntry, handle), 0u);
    EXPECT_EQ(sizeof(cmdEntry), 160u);
    EXPECT_EQ(sizeof(cmdBatch), offsetof(cmdBatch, e) + IO_BATCH_MAX * sizeof(cmdEntry));

    /* The control aligns to a cache line, and its two reply counters sit on their own lines so the
     * main producer and the IO consumer never share a line with each other or with identity. */
    EXPECT_EQ(alignof(ClientControl), (size_t)CACHE_LINE_SIZE);
    EXPECT_EQ(offsetof(ClientControl, reply_bytes_produced), (size_t)CACHE_LINE_SIZE);
    EXPECT_EQ(offsetof(ClientControl, reply_bytes_released), (size_t)(2 * CACHE_LINE_SIZE));
    EXPECT_GE(offsetof(ClientControl, reply_bytes_released) - offsetof(ClientControl, reply_bytes_produced),
              (size_t)CACHE_LINE_SIZE);
    EXPECT_EQ(sizeof(ClientControl), (size_t)(3 * CACHE_LINE_SIZE));
}

/* A control is allocated once, on first admission, and reused idempotently: the same connection keeps
 * its stable id and one control across a re-ensure. The entry an IO thread appends carries a handle
 * that resolves to that control, never a raw connection pointer. */
TEST_F(FastpathClientControlTest, ControlEnsuredOncePerConnectionAndEntriesCarryAHandle) {
    int peer;
    client *c = newFastpathClient(&peer);

    ASSERT_NE(c->control, nullptr);
    ClientControl *cc = c->control;
    EXPECT_EQ(cc->client_id, c->id);
    EXPECT_EQ(cc->owner_domain, CC_OWNER_IO); /* the IO thread owns it after attach */
    EXPECT_EQ(cc->lifecycle, FP_ACTIVE);

    /* Idempotent: a second ensure keeps the same control, does not reallocate. */
    EXPECT_EQ(fastpathControlEnsure(c), C_OK);
    EXPECT_EQ(c->control, cc);

    /* A handle built for the client points at the control with the current generation and is not stale. */
    ClientHandle h = fastpathHandleFor(c);
    EXPECT_EQ(h.control, cc);
    EXPECT_EQ(h.generation, cc->generation);
    EXPECT_EQ(h.owner_slot, c->fp_owner_slot);
    EXPECT_FALSE(fastpathHandleStale(&h));

    quiesceAndFree(c, peer);
}

/* Stale-generation detection reads only the control, never the connection: after the control's slot is
 * conceptually reused (generation bumped), a handle captured earlier is detected as stale without
 * touching any connection storage, and a NULL control reads as stale. */
TEST_F(FastpathClientControlTest, StaleHandleDetectedByGenerationWithoutTouchingConnection) {
    int peer;
    client *c = newFastpathClient(&peer);
    ClientControl *cc = c->control;

    ClientHandle h = fastpathHandleFor(c);
    EXPECT_FALSE(fastpathHandleStale(&h));

    /* Simulate slot reuse: the control's generation moves on. The old handle must now read stale,
     * decided purely from control->generation, not from any connection field. */
    cc->generation++;
    EXPECT_TRUE(fastpathHandleStale(&h));

    /* A rebuilt handle matches again. */
    ClientHandle h2 = fastpathHandleFor(c);
    EXPECT_EQ(h2.generation, cc->generation);
    EXPECT_FALSE(fastpathHandleStale(&h2));

    /* A NULL-control handle is stale, never dereferenced. */
    ClientHandle empty = {NULL, 0, 0};
    EXPECT_TRUE(fastpathHandleStale(&empty));

    cc->generation--; /* restore so the still-owned client reconciles cleanly on teardown */
    quiesceAndFree(c, peer);
}

/* Request publication vs owner execution: publishing a request only sets the bit on the control; the
 * transition happens on the owning IO thread's pass. QUIESCE/HANDOFF return the client to main
 * (FP_LEAVING); the bit is cleared once executed and re-publishing it after execution is a no-op. */
TEST_F(FastpathClientControlTest, RequestPublishedByAnyoneExecutedOnlyByOwner) {
    int peer;
    client *c = newFastpathClient(&peer);
    ClientControl *cc = c->control;

    /* Publish HANDOFF. Merely publishing does not move the lifecycle; the owner has not run yet. */
    fastpathControlRequest(cc, CC_REQ_HANDOFF);
    EXPECT_EQ(cc->lifecycle, FP_ACTIVE);

    /* The owner's pass executes the request: HANDOFF returns the client to main via FP_LEAVING and the
     * request bit is cleared once acted on. */
    fastpathProcessReturns(1);
    EXPECT_EQ(cc->lifecycle, FP_LEAVING);
    EXPECT_EQ(cc->requests & CC_REQ_HANDOFF, 0u);

    /* Re-publishing HANDOFF after the client left FP_ACTIVE is a no-op transition (idempotent). */
    fastpathControlRequest(cc, CC_REQ_HANDOFF);
    fastpathProcessReturns(1);
    EXPECT_EQ(cc->lifecycle, FP_LEAVING);

    fastpathHandoffDone(c, 0);
    EXPECT_EQ(fastpathWorkerOwnedClients(1), 0u);
    fastpathWorkerReopen(1);
    close(peer);
    freeClient(c);
}

/* Request precedence, idempotence, and coalescing on the control word itself: CLOSE is terminal and
 * supersedes the rest, and setting a bit already present does not change the word. */
TEST_F(FastpathClientControlTest, RequestPrecedenceAndIdempotence) {
    int peer;
    client *c = newFastpathClient(&peer);
    ClientControl *cc = c->control;

    /* Compatible non-terminal requests coalesce into one word. */
    fastpathControlRequest(cc, CC_REQ_QUIESCE);
    fastpathControlRequest(cc, CC_REQ_HANDOFF);
    uint32_t after_two = cc->requests;
    EXPECT_EQ(after_two & CC_REQ_QUIESCE, (uint32_t)CC_REQ_QUIESCE);
    EXPECT_EQ(after_two & CC_REQ_HANDOFF, (uint32_t)CC_REQ_HANDOFF);

    /* Idempotent: re-publishing a present bit leaves the word unchanged. */
    fastpathControlRequest(cc, CC_REQ_QUIESCE);
    EXPECT_EQ(cc->requests, after_two);

    /* CLOSE is terminal: it supersedes QUIESCE/HANDOFF/EVICT, leaving only the CLOSE bit set. */
    fastpathControlRequest(cc, CC_REQ_CLOSE);
    EXPECT_EQ(cc->requests, (uint32_t)CC_REQ_CLOSE);

    /* Once closing, nothing outranks it: a later lesser request does not clear or add to CLOSE. */
    fastpathControlRequest(cc, CC_REQ_HANDOFF);
    EXPECT_EQ(cc->requests, (uint32_t)CC_REQ_CLOSE);

    /* The owner executes CLOSE: the client goes to FP_CLOSING and is freed on handoff. */
    fastpathProcessReturns(1);
    EXPECT_EQ(cc->lifecycle, FP_CLOSING);
    fastpathHandoffDone(c, 1); /* frees it */
    EXPECT_EQ(fastpathWorkerOwnedClients(1), 0u);
    fastpathWorkerReopen(1);
    close(peer);
}

/* Reply accounting, exact produce/release: a batch of plain data commands charges reply_bytes_produced
 * on execution and releases the same bytes when the IO thread reclaims the storage after sending, so
 * outstanding returns to zero. The unit is the entry's logical reply bytes, counted once. */
TEST_F(FastpathClientControlTest, ReplyAccountingProducesAndReleasesExactly) {
    int peer;
    client *c = newFastpathClient(&peer);
    ClientControl *cc = c->control;
    EXPECT_EQ(fastpathReplyOutstanding(cc), 0u);

    /* Two pipelined INCRs: each returns a small integer reply retained in the batch arena. */
    const char *req = "*2\r\n$4\r\nINCR\r\n$4\r\nrc:a\r\n*2\r\n$4\r\nINCR\r\n$4\r\nrc:a\r\n";
    send(peer, req);
    fastpathClientReadable(1, c);
    EXPECT_EQ(c->fp_inflight, 2u);
    fastpathSubmitPending(1);

    /* Main executes the batch: replies become retained storage, so produced advances by the reply
     * bytes and outstanding is now positive. Nothing is released until the IO thread reclaims it. */
    EXPECT_EQ(fastpathDrain(), 2);
    size_t outstanding_after_exec = fastpathReplyOutstanding(cc);
    EXPECT_GT(outstanding_after_exec, 0u);
    EXPECT_EQ(cc->reply_bytes_released, 0u);

    /* The IO thread delivers and reclaims the batch: released catches up to produced exactly, so the
     * client owes no outstanding reply memory. Bytes counted once, not double-counted per pass. */
    EXPECT_EQ(fastpathProcessReturns(1), 1);
    EXPECT_EQ(recv(peer), ":1\r\n:2\r\n");
    EXPECT_EQ(fastpathReplyOutstanding(cc), 0u);
    EXPECT_EQ(cc->reply_bytes_produced, cc->reply_bytes_released);

    quiesceAndFree(c, peer);
}

/* A large reply that spills to a heap block (reply_big) is charged and released by its own logical
 * length, still once, so a spilled entry does not double-count against the arena and outstanding
 * returns to zero after delivery. */
TEST_F(FastpathClientControlTest, ReplyAccountingHandlesSpilledBigReply) {
    int peer;
    client *c = newFastpathClient(&peer);
    ClientControl *cc = c->control;

    /* Build a value larger than the batch arena (FP_ARENA_SIZE is 16 KiB) so its GET reply spills to a
     * heap block: SET a 32 KiB value, then GET it back on the fast path. */
    std::string big(32 * 1024, 'x');
    std::string set = "*3\r\n$3\r\nSET\r\n$5\r\nbigky\r\n$" + std::to_string(big.size()) + "\r\n" + big + "\r\n";
    send(peer, set.c_str());
    fastpathClientReadable(1, c);
    fastpathSubmitPending(1);
    EXPECT_EQ(fastpathDrain(), 1);
    EXPECT_EQ(fastpathProcessReturns(1), 1);
    EXPECT_EQ(recv(peer), "+OK\r\n");
    EXPECT_EQ(fastpathReplyOutstanding(cc), 0u); /* +OK sent and reclaimed */

    const char *get = "*2\r\n$3\r\nGET\r\n$5\r\nbigky\r\n";
    send(peer, get);
    fastpathClientReadable(1, c);
    fastpathSubmitPending(1);
    EXPECT_EQ(fastpathDrain(), 1);
    /* The big reply is retained (spilled to reply_big): outstanding covers at least the value bytes. */
    EXPECT_GE(fastpathReplyOutstanding(cc), big.size());
    EXPECT_EQ(cc->reply_bytes_released, 0u);

    /* Delivered and reclaimed exactly once: outstanding returns to zero, produced == released. The
     * bulk reply is "$<len>\r\n<value>\r\n": one '$', the ascii length, two CRLFs, and the value. */
    EXPECT_EQ(fastpathProcessReturns(1), 1);
    size_t expect = 1 + std::to_string(big.size()).size() + 2 + big.size() + 2;
    std::string reply = recvExact(peer, expect);
    EXPECT_EQ(reply.size(), expect);
    EXPECT_EQ(fastpathReplyOutstanding(cc), 0u);
    EXPECT_EQ(cc->reply_bytes_produced, cc->reply_bytes_released);

    quiesceAndFree(c, peer);
}

/* Two clients in one published batch keep separate accounting: each control is charged only for its
 * own entries, so a batch that mixes owners never cross-charges, and each returns to zero after
 * delivery. This is the per-control isolation a recycled, shared batch buffer must preserve. */
TEST_F(FastpathClientControlTest, PerControlAccountingIsolatedAcrossClientsInABatch) {
    int pa, pb;
    client *a = newFastpathClient(&pa);
    client *b = newFastpathClient(&pb);
    ClientControl *ca = a->control;
    ClientControl *cb = b->control;
    EXPECT_NE(ca, cb);

    send(pa, "*2\r\n$4\r\nINCR\r\n$4\r\nis:a\r\n");
    fastpathClientReadable(1, a);
    send(pb, "*2\r\n$4\r\nINCR\r\n$4\r\nis:b\r\n");
    fastpathClientReadable(1, b);
    fastpathSubmitPending(1);

    EXPECT_EQ(fastpathDrain(), 2);
    /* Each control was charged for exactly one reply; the two totals are independent and positive. */
    EXPECT_GT(fastpathReplyOutstanding(ca), 0u);
    EXPECT_GT(fastpathReplyOutstanding(cb), 0u);

    EXPECT_EQ(fastpathProcessReturns(1), 1);
    EXPECT_EQ(recv(pa), ":1\r\n");
    EXPECT_EQ(recv(pb), ":1\r\n");
    EXPECT_EQ(fastpathReplyOutstanding(ca), 0u);
    EXPECT_EQ(fastpathReplyOutstanding(cb), 0u);

    /* Hand both back and free them. */
    fastpathWorkerQuiesce(1);
    fastpathProcessReturns(1);
    if (a->flag.fastpath) fastpathHandoffDone(a, 0);
    if (b->flag.fastpath) fastpathHandoffDone(b, 0);
    EXPECT_EQ(fastpathWorkerOwnedClients(1), 0u);
    fastpathWorkerReopen(1);
    close(pa);
    close(pb);
    freeClient(a);
    freeClient(b);
}

/* Reclamation gates: a control is reclaimable only once nothing pins it and no reply memory is
 * outstanding. A createClient(NULL)-style client that never entered the fast path has a NULL control,
 * so its teardown reclaim is a safe no-op (the field is NULL-initialized in createClient). */
TEST_F(FastpathClientControlTest, ControlReclaimedOnlyAfterGatesClearAndNullIsSafe) {
    /* A never-admitted fake client (no connection) has control == NULL, and reclaim tolerates it. */
    client *fake = createClient(NULL);
    EXPECT_EQ(fake->control, nullptr);
    fastpathControlReclaim(fake); /* no-op, must not touch garbage */
    EXPECT_EQ(fake->control, nullptr);
    freeClient(fake);

    /* An admitted client holds the owner pin until handoff and cannot be freed while owned; after the
     * owner hands it back the pin drops, replies are released, and freeClient reclaims the control. */
    int peer;
    client *c = newFastpathClient(&peer);
    ClientControl *cc = c->control;
    EXPECT_GT(cc->pin_refs, 0u); /* CC_PIN_OWNER held while the IO thread owns the connection */

    fastpathWorkerQuiesce(1);
    fastpathProcessReturns(1);
    fastpathHandoffDone(c, 0);
    EXPECT_EQ(cc->pin_refs, 0u);                  /* ownership pin dropped as main took the client back */
    EXPECT_EQ(fastpathReplyOutstanding(cc), 0u);  /* no charged replies outstanding */
    EXPECT_EQ(fastpathWorkerOwnedClients(1), 0u);
    fastpathWorkerReopen(1);
    close(peer);
    freeClient(c); /* reclaims the control; asserts the reclaimable invariant internally */
}
