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

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <signal.h>
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

/* A fatal write to a closed peer must return EPIPE rather than raise SIGPIPE. */
static struct sigaction cc_prev_sigpipe;
static int cc_sigpipe_saved = 0;

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

        struct sigaction ign;
        memset(&ign, 0, sizeof(ign));
        ign.sa_handler = SIG_IGN;
        sigemptyset(&ign.sa_mask);
        cc_sigpipe_saved = (sigaction(SIGPIPE, &ign, &cc_prev_sigpipe) == 0);
    }

    static void TearDownTestSuite() {
        if (cc_sigpipe_saved) sigaction(SIGPIPE, &cc_prev_sigpipe, NULL);
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

namespace {
void fpSetNonBlock(int fd) {
    int fl = fcntl(fd, F_GETFL, 0);
    if (fl != -1) fcntl(fd, F_SETFL, fl | O_NONBLOCK);
}

/* AF_UNIX flow control keys off the receiver's SO_RCVBUF, so clamp both ends below a multi-KiB reply to force a partial write. */
void fpThrottleSocket(int send_fd, int recv_fd) {
    int one = 1;
    setsockopt(send_fd, SOL_SOCKET, SO_SNDBUF, &one, sizeof(one));
    setsockopt(recv_fd, SOL_SOCKET, SO_RCVBUF, &one, sizeof(one));
    fpSetNonBlock(send_fd);
    fpSetNonBlock(recv_fd);
}

/* Append everything currently readable on fd to acc without blocking; fd must be non-blocking so a drained socket returns instead of waiting. */
sds fpDrainInto(int fd, sds acc) {
    char buf[16384];
    for (;;) {
        ssize_t n = read(fd, buf, sizeof(buf));
        if (n <= 0) break;
        acc = sdscatlen(acc, buf, (size_t)n);
    }
    return acc;
}

/* RESP bulk string "$<len>\r\n<bytes>\r\n". */
sds fpBulk(const char *v, size_t vlen) {
    sds s = sdscatprintf(sdsempty(), "$%zu\r\n", vlen);
    s = sdscatlen(s, v, vlen);
    return sdscatlen(s, "\r\n", 2);
}

sds fpMakeSet(const char *key, const char *v, size_t vlen) {
    sds s = sdscatprintf(sdsempty(), "*3\r\n$3\r\nSET\r\n$%zu\r\n%s\r\n", strlen(key), key);
    sds bulk = fpBulk(v, vlen);
    s = sdscatlen(s, bulk, sdslen(bulk));
    sdsfree(bulk);
    return s;
}

sds fpMakeValue(size_t vlen) {
    sds v = sdsnewlen(NULL, vlen);
    memset(v, 'x', vlen);
    return v;
}

/* fastpathClientReadable reads once, so a request split across reads needs repeated non-blocking reads and the finite guard stops a stalled parse from spinning forever. */
void fpReadUntilInflight(client *c, unsigned int want) {
    fpSetNonBlock(c->conn->fd);
    for (int guard = 0; guard < 8192 && c->fp_inflight < want; guard++) fastpathClientReadable(1, c);
}

/* Feed a large request in chunks, driving the reader between writes so a bounded socket buffer can neither deadlock nor truncate setup, returning only once every byte is sent and want commands are in flight, with a finite guard bounding a stalled write or parse. */
void fpFeedRequest(client *c, int fd, const char *buf, size_t len, unsigned int want) {
    fpSetNonBlock(fd);
    fpSetNonBlock(c->conn->fd);
    size_t sent = 0;
    int guard = 0;
    while ((sent < len || c->fp_inflight < want) && guard++ < 100000) {
        if (sent < len) {
            ssize_t n = write(fd, buf + sent, len - sent);
            if (n > 0) {
                sent += (size_t)n;
            } else if (n < 0 && errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK) {
                break;
            }
        }
        fastpathClientReadable(1, c);
    }
    EXPECT_EQ(sent, len);
    EXPECT_GE(c->fp_inflight, want);
}

/* True when nothing is readable on the peer: a non-blocking read finds no bytes (EAGAIN) or EOF, so no reply reached it. */
int fpPeerHasNoReply(int peer) {
    fpSetNonBlock(peer);
    char buf[64];
    ssize_t n = read(peer, buf, sizeof(buf));
    return n <= 0;
}

/* Attach a fresh DB-0 client to thread 1 but leave its attach request unconsumed in the return ring, so a request can be published against a control the owner has not yet registered. */
client *fpAttachPending(int *peer) {
    int sv[2];
    EXPECT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
    connection *conn = connCreateAccepted(connectionByType(CONN_TYPE_SOCKET), sv[0], NULL);
    conn->state = CONN_STATE_CONNECTED;
    client *c = createClient(conn);
    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_port = htons(40001);
    sa.sin_addr.s_addr = htonl(0x7f000001);
    EXPECT_EQ(peerIdentityFromSockaddr(&c->fp_peer, (struct sockaddr *)&sa, sizeof(sa)), C_OK);
    c->fp_local = c->fp_peer;
    EXPECT_EQ(fastpathAttach(c), C_OK);
    EXPECT_EQ(c->io_tid, 1);
    *peer = sv[1];
    return c;
}
}  // namespace

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

    /* A value larger than the 16 KiB batch arena makes its GET reply spill to a heap block. */
    size_t vlen = 32 * 1024;
    sds val = fpMakeValue(vlen);
    sds set = fpMakeSet("bigky", val, vlen);
    fpFeedRequest(c, peer, set, sdslen(set), 1);
    sdsfree(set);
    EXPECT_EQ(c->fp_inflight, 1u);
    fastpathSubmitPending(1);
    EXPECT_EQ(fastpathDrain(), 1);
    EXPECT_EQ(fastpathProcessReturns(1), 1);
    EXPECT_EQ(recv(peer), "+OK\r\n");
    EXPECT_EQ(fastpathReplyOutstanding(cc), 0u); /* +OK sent and reclaimed */
    size_t released_before_get = cc->reply_bytes_released;

    send(peer, "*2\r\n$3\r\nGET\r\n$5\r\nbigky\r\n");
    fpReadUntilInflight(c, 1);
    EXPECT_EQ(c->fp_inflight, 1u);
    fastpathSubmitPending(1);
    EXPECT_EQ(fastpathDrain(), 1);
    /* The big reply is retained (spilled to reply_big): outstanding covers at least the value bytes. */
    EXPECT_GE(fastpathReplyOutstanding(cc), vlen);
    EXPECT_EQ(cc->reply_bytes_released, released_before_get);

    /* Delivered and reclaimed once: outstanding returns to zero and produced == released; the reply fits the receive buffer so one drain reads it whole. */
    EXPECT_EQ(fastpathProcessReturns(1), 1);
    fpSetNonBlock(peer);
    sds expect = fpBulk(val, vlen);
    sds got = fpDrainInto(peer, sdsempty());
    EXPECT_EQ(sdslen(got), sdslen(expect));
    EXPECT_EQ(sdscmp(got, expect), 0);
    EXPECT_EQ(fastpathReplyOutstanding(cc), 0u);
    EXPECT_EQ(cc->reply_bytes_produced, cc->reply_bytes_released);
    sdsfree(got);
    sdsfree(expect);
    sdsfree(val);

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

/* A full socket write releases every produced byte and buffers nothing, so outstanding returns to zero with no residue charged. */
TEST_F(FastpathClientControlTest, FullWriteLeavesNoResidueAndOutstandingZero) {
    int peer;
    client *c = newFastpathClient(&peer);
    ClientControl *cc = c->control;

    send(peer, "*2\r\n$4\r\nINCR\r\n$4\r\nfw:a\r\n");
    fastpathClientReadable(1, c);
    fastpathSubmitPending(1);
    EXPECT_EQ(fastpathDrain(), 1);
    EXPECT_GT(fastpathReplyOutstanding(cc), 0u);

    EXPECT_EQ(fastpathProcessReturns(1), 1);
    EXPECT_EQ(recv(peer), ":1\r\n");
    EXPECT_TRUE(c->fp_out == nullptr || sdslen(c->fp_out) == 0);
    EXPECT_EQ(fastpathReplyOutstanding(cc), 0u);
    EXPECT_EQ(cc->reply_bytes_produced, cc->reply_bytes_released);

    quiesceAndFree(c, peer);
}

/* A partial write releases only what reached the wire; the fp_out residue stays outstanding and equal to fp_out until drained, then balances. */
TEST_F(FastpathClientControlTest, PartialWriteKeepsResidueOutstandingUntilDrained) {
    int peer;
    client *c = newFastpathClient(&peer);
    ClientControl *cc = c->control;

    size_t vlen = 12 * 1024; /* fits the 16 KiB batch arena: exercises the arena reply path */
    sds val = fpMakeValue(vlen);
    sds set = fpMakeSet("pw:ky", val, vlen);
    fpFeedRequest(c, peer, set, sdslen(set), 1);
    sdsfree(set);
    EXPECT_EQ(c->fp_inflight, 1u);
    fastpathSubmitPending(1);
    EXPECT_EQ(fastpathDrain(), 1);
    EXPECT_EQ(fastpathProcessReturns(1), 1);
    EXPECT_EQ(recv(peer), "+OK\r\n");
    EXPECT_EQ(fastpathReplyOutstanding(cc), 0u);

    fpThrottleSocket(c->conn->fd, peer);

    send(peer, "*2\r\n$3\r\nGET\r\n$5\r\npw:ky\r\n");
    fpReadUntilInflight(c, 1);
    EXPECT_EQ(c->fp_inflight, 1u);
    fastpathSubmitPending(1);
    EXPECT_EQ(fastpathDrain(), 1);
    size_t produced = cc->reply_bytes_produced;
    EXPECT_GT(produced, vlen);

    EXPECT_EQ(fastpathProcessReturns(1), 1);
    EXPECT_TRUE(c->fp_out != nullptr); /* the clamped receiver must have forced a partial write */
    size_t residue = c->fp_out ? sdslen(c->fp_out) : 0;
    EXPECT_GT(residue, 0u);
    EXPECT_GT(cc->reply_bytes_released, 0u);
    EXPECT_LT(cc->reply_bytes_released, produced);
    EXPECT_EQ(fastpathReplyOutstanding(cc), residue);

    sds got = fpDrainInto(peer, sdsempty());
    for (int guard = 0; guard < 100000 && c->fp_out && sdslen(c->fp_out) > 0; guard++) {
        fastpathClientWritable(1, c);
        got = fpDrainInto(peer, got);
    }
    got = fpDrainInto(peer, got);
    sds expect = fpBulk(val, vlen);
    EXPECT_EQ(sdscmp(got, expect), 0);
    EXPECT_EQ(fastpathReplyOutstanding(cc), 0u);
    EXPECT_EQ(cc->reply_bytes_produced, cc->reply_bytes_released);
    sdsfree(got);
    sdsfree(expect);
    sdsfree(val);

    quiesceAndFree(c, peer);
}

/* A batch appended behind existing fp_out releases nothing: outstanding grows by the whole appended reply and stays equal to fp_out. */
TEST_F(FastpathClientControlTest, ReturnedBatchAppendedToExistingBufferStaysOutstanding) {
    int peer;
    client *c = newFastpathClient(&peer);
    ClientControl *cc = c->control;

    size_t vlen = 12 * 1024;
    sds val = fpMakeValue(vlen);
    sds set = fpMakeSet("rb:ky", val, vlen);
    fpFeedRequest(c, peer, set, sdslen(set), 1);
    sdsfree(set);
    EXPECT_EQ(c->fp_inflight, 1u);
    fastpathSubmitPending(1);
    EXPECT_EQ(fastpathDrain(), 1);
    EXPECT_EQ(fastpathProcessReturns(1), 1);
    EXPECT_EQ(recv(peer), "+OK\r\n");

    fpThrottleSocket(c->conn->fd, peer);

    send(peer, "*2\r\n$3\r\nGET\r\n$5\r\nrb:ky\r\n");
    fpReadUntilInflight(c, 1);
    EXPECT_EQ(c->fp_inflight, 1u);
    fastpathSubmitPending(1);
    EXPECT_EQ(fastpathDrain(), 1);
    EXPECT_EQ(fastpathProcessReturns(1), 1);
    EXPECT_TRUE(c->fp_out != nullptr); /* the clamped receiver must have forced a partial write */
    size_t residue = c->fp_out ? sdslen(c->fp_out) : 0;
    EXPECT_GT(residue, 0u);
    size_t released_after_partial = cc->reply_bytes_released;
    size_t outstanding_after_partial = fastpathReplyOutstanding(cc);

    /* A second command returning while the buffer is occupied is appended to fp_out and stays fully outstanding, releasing nothing. */
    send(peer, "*2\r\n$4\r\nINCR\r\n$4\r\nrb:n\r\n");
    fpReadUntilInflight(c, 1);
    EXPECT_EQ(c->fp_inflight, 1u);
    fastpathSubmitPending(1);
    EXPECT_EQ(fastpathDrain(), 1);
    EXPECT_EQ(fastpathProcessReturns(1), 1);
    EXPECT_EQ(cc->reply_bytes_released, released_after_partial);
    EXPECT_EQ(fastpathReplyOutstanding(cc), c->fp_out ? sdslen(c->fp_out) : 0);
    EXPECT_GT(fastpathReplyOutstanding(cc), outstanding_after_partial);

    sds expect = fpBulk(val, vlen);
    expect = sdscatlen(expect, ":1\r\n", 4);
    sds got = fpDrainInto(peer, sdsempty());
    for (int guard = 0; guard < 100000 && c->fp_out && sdslen(c->fp_out) > 0; guard++) {
        fastpathClientWritable(1, c);
        got = fpDrainInto(peer, got);
    }
    got = fpDrainInto(peer, got);
    EXPECT_EQ(sdscmp(got, expect), 0);
    EXPECT_EQ(fastpathReplyOutstanding(cc), 0u);
    EXPECT_EQ(cc->reply_bytes_produced, cc->reply_bytes_released);
    sdsfree(got);
    sdsfree(expect);
    sdsfree(val);

    quiesceAndFree(c, peer);
}

/* A fatal write discards the current reply and releases its logical bytes exactly once, so a client whose peer vanished balances to zero before it closes. */
TEST_F(FastpathClientControlTest, FatalWriteDiscardsReplyAndBalances) {
    int peer;
    client *c = newFastpathClient(&peer);
    ClientControl *cc = c->control;

    send(peer, "*2\r\n$4\r\nINCR\r\n$4\r\nfd:a\r\n");
    fastpathClientReadable(1, c);
    fastpathSubmitPending(1);
    EXPECT_EQ(fastpathDrain(), 1);
    EXPECT_GT(fastpathReplyOutstanding(cc), 0u);

    close(peer); /* the next writev fails fatally */
    EXPECT_EQ(fastpathProcessReturns(1), 1);
    EXPECT_EQ(c->control->lifecycle, FP_CLOSING);
    EXPECT_EQ(fastpathReplyOutstanding(cc), 0u); /* discarded bytes released once */
    EXPECT_EQ(cc->reply_bytes_produced, cc->reply_bytes_released);

    ASSERT_EQ(c->flag.fastpath, 1u);
    fastpathHandoffDone(c, 1); /* frees it; reclaim asserts the gate is clear */
    EXPECT_EQ(fastpathWorkerOwnedClients(1), 0u);
    fastpathWorkerReopen(1);
}

/* A fatal flush while an open owner drains a leaving client's fp_out residue upgrades it to FP_CLOSING, releasing the residue once instead of looping. */
TEST_F(FastpathClientControlTest, FatalFlushDuringOpenLeavingDrainUpgradesToClosing) {
    int peer;
    client *c = newFastpathClient(&peer);
    ClientControl *cc = c->control;

    size_t vlen = 12 * 1024;
    sds val = fpMakeValue(vlen);
    sds set = fpMakeSet("fl:ky", val, vlen);
    sdsfree(val);
    fpFeedRequest(c, peer, set, sdslen(set), 1);
    sdsfree(set);
    EXPECT_EQ(c->fp_inflight, 1u);
    fastpathSubmitPending(1);
    EXPECT_EQ(fastpathDrain(), 1);
    EXPECT_EQ(fastpathProcessReturns(1), 1);
    EXPECT_EQ(recv(peer), "+OK\r\n");

    fpThrottleSocket(c->conn->fd, peer);

    send(peer, "*2\r\n$3\r\nGET\r\n$5\r\nfl:ky\r\n");
    fpReadUntilInflight(c, 1);
    EXPECT_EQ(c->fp_inflight, 1u);
    fastpathSubmitPending(1);
    EXPECT_EQ(fastpathDrain(), 1);
    EXPECT_EQ(fastpathProcessReturns(1), 1);
    EXPECT_TRUE(c->fp_out != nullptr); /* the clamped receiver must have forced a partial write */
    size_t residue = c->fp_out ? sdslen(c->fp_out) : 0;
    EXPECT_GT(residue, 0u);
    EXPECT_EQ(c->control->lifecycle, FP_ACTIVE);
    EXPECT_EQ(fastpathReplyOutstanding(cc), residue);

    /* HANDOFF marks the client FP_LEAVING while the owner role stays open, so fpFinishLeaving drives the residue drain; the closed peer makes that drain write fail fatally. */
    fastpathControlRequest(cc, CC_REQ_HANDOFF);
    close(peer);
    fastpathProcessReturns(1);

    EXPECT_EQ(c->control->lifecycle, FP_CLOSING); /* upgraded, not stranded at FP_LEAVING */
    EXPECT_EQ(fastpathReplyOutstanding(cc), 0u);
    EXPECT_EQ(cc->reply_bytes_produced, cc->reply_bytes_released);

    ASSERT_EQ(c->flag.fastpath, 1u);
    fastpathHandoffDone(c, 1);
    EXPECT_EQ(fastpathWorkerOwnedClients(1), 0u);
    fastpathWorkerReopen(1);
}

/* A fatal delivery to an already-leaving client must upgrade it to FP_CLOSING so a live connection is never handed off; here the fatal write is the delivery itself, with nothing buffered in fp_out. */
TEST_F(FastpathClientControlTest, FatalDeliveryToAlreadyLeavingClientUpgradesToClosing) {
    int peer;
    client *c = newFastpathClient(&peer);
    ClientControl *cc = c->control;

    send(peer, "*2\r\n$4\r\nINCR\r\n$4\r\nld:a\r\n");
    fastpathClientReadable(1, c);
    fastpathSubmitPending(1); /* reply in flight in main's inbox, not yet produced or delivered */

    /* Reach FP_LEAVING while the reply is still in flight: fp_inflight > 0 keeps the client registered so its reply still resolves at delivery. */
    fastpathControlRequest(cc, CC_REQ_HANDOFF);
    fastpathProcessReturns(1);
    EXPECT_EQ(c->control->lifecycle, FP_LEAVING);

    close(peer); /* the pending delivery's writev will fail fatally */
    EXPECT_EQ(fastpathDrain(), 1);
    EXPECT_GT(cc->reply_bytes_produced, 0u);

    fastpathProcessReturns(1); /* fpSend delivers the reply and fails fatally while already FP_LEAVING */
    EXPECT_EQ(c->control->lifecycle, FP_CLOSING); /* upgraded past FP_LEAVING, never handed off alive */
    EXPECT_EQ(fastpathReplyOutstanding(cc), 0u);  /* discarded group released exactly once */
    EXPECT_EQ(cc->reply_bytes_produced, cc->reply_bytes_released);

    ASSERT_EQ(c->flag.fastpath, 1u);
    fastpathHandoffDone(c, 1); /* closes and frees; reclaim asserts the gate is clear */
    EXPECT_EQ(fastpathWorkerOwnedClients(1), 0u);
    fastpathWorkerReopen(1);
}

/* Handing a client back with fp_out residue releases the fast-path charge at the transfer and moves the bytes to the classic path, preserving reply order. */
TEST_F(FastpathClientControlTest, HandoffWithResidueTransfersBytesAndPreservesOrder) {
    int peer;
    client *c = newFastpathClient(&peer);
    ClientControl *cc = c->control;

    size_t vlen = 12 * 1024; /* residue stays under a reply chunk so the transfer lands in c->buf */
    sds val = fpMakeValue(vlen);
    sds set = fpMakeSet("ho:ky", val, vlen);
    fpFeedRequest(c, peer, set, sdslen(set), 1);
    sdsfree(set);
    EXPECT_EQ(c->fp_inflight, 1u);
    fastpathSubmitPending(1);
    EXPECT_EQ(fastpathDrain(), 1);
    EXPECT_EQ(fastpathProcessReturns(1), 1);
    EXPECT_EQ(recv(peer), "+OK\r\n");

    fpThrottleSocket(c->conn->fd, peer);

    send(peer, "*2\r\n$3\r\nGET\r\n$5\r\nho:ky\r\n");
    fpReadUntilInflight(c, 1);
    EXPECT_EQ(c->fp_inflight, 1u);
    fastpathSubmitPending(1);
    EXPECT_EQ(fastpathDrain(), 1);
    EXPECT_EQ(fastpathProcessReturns(1), 1);
    EXPECT_TRUE(c->fp_out != nullptr); /* the clamped receiver must have forced a partial write */
    size_t residue = c->fp_out ? sdslen(c->fp_out) : 0;
    EXPECT_GT(residue, 0u);
    sds prefix = fpDrainInto(peer, sdsempty()); /* bytes already on the wire, read without flushing more */
    EXPECT_EQ(fastpathReplyOutstanding(cc), residue);

    /* Quiesce: a non-open owner hands the residue over instead of draining it to the slow reader. */
    fastpathWorkerQuiesce(1);
    fastpathProcessReturns(1);
    EXPECT_EQ(c->control->lifecycle, FP_LEAVING);
    EXPECT_EQ(fastpathReplyOutstanding(cc), 0u); /* residue released at the ownership transfer */
    EXPECT_EQ(cc->reply_bytes_produced, cc->reply_bytes_released);

    ASSERT_EQ(c->flag.fastpath, 1u);
    fastpathHandoffDone(c, 0); /* moves the residue into main's output buffer */
    EXPECT_EQ(c->flag.fastpath, 0u);
    sds full = sdscatlen(sdsdup(prefix), c->buf, c->bufpos); /* wire bytes then the bytes moved to main's buffer */
    sds expect = fpBulk(val, vlen);
    EXPECT_EQ(sdscmp(full, expect), 0);
    sdsfree(full);
    sdsfree(prefix);
    sdsfree(expect);
    sdsfree(val);

    EXPECT_EQ(fastpathWorkerOwnedClients(1), 0u);
    fastpathWorkerReopen(1);
    close(peer);
    freeClient(c);
}

/* A terminal close with fp_out residue discards it and releases those bytes exactly once, so accounting balances before reclaim. */
TEST_F(FastpathClientControlTest, TerminalCloseWithResidueBalancesBeforeReclaim) {
    int peer;
    client *c = newFastpathClient(&peer);
    ClientControl *cc = c->control;

    size_t vlen = 12 * 1024;
    sds val = fpMakeValue(vlen);
    sds set = fpMakeSet("tc:ky", val, vlen);
    sdsfree(val);
    fpFeedRequest(c, peer, set, sdslen(set), 1);
    sdsfree(set);
    EXPECT_EQ(c->fp_inflight, 1u);
    fastpathSubmitPending(1);
    EXPECT_EQ(fastpathDrain(), 1);
    EXPECT_EQ(fastpathProcessReturns(1), 1);
    EXPECT_EQ(recv(peer), "+OK\r\n");

    fpThrottleSocket(c->conn->fd, peer);

    send(peer, "*2\r\n$3\r\nGET\r\n$5\r\ntc:ky\r\n");
    fpReadUntilInflight(c, 1);
    EXPECT_EQ(c->fp_inflight, 1u);
    fastpathSubmitPending(1);
    EXPECT_EQ(fastpathDrain(), 1);
    EXPECT_EQ(fastpathProcessReturns(1), 1);
    EXPECT_TRUE(c->fp_out != nullptr); /* the clamped receiver must have forced a partial write */
    size_t residue = c->fp_out ? sdslen(c->fp_out) : 0;
    EXPECT_GT(residue, 0u);
    EXPECT_EQ(fastpathReplyOutstanding(cc), residue);

    fastpathRequestDetach(c); /* terminal close while residue is still buffered */
    fastpathProcessReturns(1);
    EXPECT_EQ(c->control->lifecycle, FP_CLOSING);
    EXPECT_EQ(fastpathReplyOutstanding(cc), 0u); /* residue discarded and released once */
    EXPECT_EQ(cc->reply_bytes_produced, cc->reply_bytes_released);
    EXPECT_TRUE(fastpathDetachConsumed(c));

    fastpathHandoffDone(c, 1); /* frees it; reclaim asserts outstanding == 0 */
    EXPECT_EQ(fastpathWorkerOwnedClients(1), 0u);
    fastpathWorkerReopen(1);
    close(peer);
}

/* A reply spilled to a heap block (reply_big) accounts identically under a partial write: outstanding tracks fp_out residue and drains to zero. */
TEST_F(FastpathClientControlTest, LargeSpilledReplyPartialDeliveryBehavesIdentically) {
    int peer;
    client *c = newFastpathClient(&peer);
    ClientControl *cc = c->control;

    size_t vlen = 64 * 1024; /* > 16 KiB arena and > 32 KiB big-arg: the GET reply spills to reply_big */
    sds val = fpMakeValue(vlen);
    sds set = fpMakeSet("bg:ky", val, vlen);
    fpFeedRequest(c, peer, set, sdslen(set), 1);
    sdsfree(set);
    EXPECT_EQ(c->fp_inflight, 1u);
    fastpathSubmitPending(1);
    EXPECT_EQ(fastpathDrain(), 1);
    EXPECT_EQ(fastpathProcessReturns(1), 1);
    EXPECT_EQ(recv(peer), "+OK\r\n");

    fpThrottleSocket(c->conn->fd, peer);

    send(peer, "*2\r\n$3\r\nGET\r\n$5\r\nbg:ky\r\n");
    fpReadUntilInflight(c, 1);
    EXPECT_EQ(c->fp_inflight, 1u);
    fastpathSubmitPending(1);
    EXPECT_EQ(fastpathDrain(), 1);
    size_t produced = cc->reply_bytes_produced;
    EXPECT_GT(produced, vlen);

    EXPECT_EQ(fastpathProcessReturns(1), 1);
    EXPECT_TRUE(c->fp_out != nullptr); /* the clamped receiver must have forced a partial write */
    size_t residue = c->fp_out ? sdslen(c->fp_out) : 0;
    EXPECT_GT(residue, 0u);
    EXPECT_GT(cc->reply_bytes_released, 0u);
    EXPECT_LT(cc->reply_bytes_released, produced);
    EXPECT_EQ(fastpathReplyOutstanding(cc), residue);

    sds got = fpDrainInto(peer, sdsempty());
    for (int guard = 0; guard < 100000 && c->fp_out && sdslen(c->fp_out) > 0; guard++) {
        fastpathClientWritable(1, c);
        got = fpDrainInto(peer, got);
    }
    got = fpDrainInto(peer, got);
    sds expect = fpBulk(val, vlen);
    EXPECT_EQ(sdscmp(got, expect), 0);
    EXPECT_EQ(fastpathReplyOutstanding(cc), 0u);
    EXPECT_EQ(cc->reply_bytes_produced, cc->reply_bytes_released);
    sdsfree(got);
    sdsfree(expect);
    sdsfree(val);

    quiesceAndFree(c, peer);
}

/* A control's generation and slot cannot move while a batch naming it is in flight, so the charge's handle still resolves at release; once handed off and reused, the old handle reads stale. */
TEST_F(FastpathClientControlTest, ControlGenerationFrozenWhileBatchInFlight) {
    int peer;
    client *c = newFastpathClient(&peer);
    ClientControl *cc = c->control;

    send(peer, "*2\r\n$4\r\nINCR\r\n$4\r\ngf:a\r\n*2\r\n$4\r\nINCR\r\n$4\r\ngf:a\r\n");
    fastpathClientReadable(1, c);
    EXPECT_EQ(c->fp_inflight, 2u);
    fastpathSubmitPending(1); /* one batch now in flight */

    uint32_t gen = cc->generation;
    uint32_t slot = c->fp_owner_slot;
    ClientHandle h = fastpathHandleFor(c);
    EXPECT_FALSE(fastpathHandleStale(&h));

    /* With a batch out, quiesce marks the client leaving but cannot unregister it, so neither its generation nor its slot may move. */
    fastpathWorkerQuiesce(1);
    fastpathProcessReturns(1);
    EXPECT_EQ(cc->generation, gen);
    EXPECT_EQ(c->fp_owner_slot, slot);
    EXPECT_FALSE(fastpathHandleStale(&h));
    EXPECT_EQ(c->control->lifecycle, FP_LEAVING);

    /* Main executes and the owner delivers under the same generation the charge used. */
    EXPECT_EQ(fastpathDrain(), 2);
    EXPECT_EQ(fastpathProcessReturns(1), 1);
    EXPECT_EQ(recv(peer), ":1\r\n:2\r\n");
    EXPECT_EQ(fastpathReplyOutstanding(cc), 0u);
    EXPECT_EQ(cc->generation, gen);

    fastpathHandoffDone(c, 0);
    cc->generation++; /* slot-reuse epoch: the earlier handle must now read stale */
    EXPECT_TRUE(fastpathHandleStale(&h));
    cc->generation--; /* restore so the still-live control reconciles cleanly on teardown */

    EXPECT_EQ(fastpathWorkerOwnedClients(1), 0u);
    fastpathWorkerReopen(1);
    close(peer);
    freeClient(c);
}

/* Reclamation stays blocked while retained reply bytes exist: the gate is pin_refs == 0 and outstanding == 0, so fp_out residue blocks reclaim until drained. */
TEST_F(FastpathClientControlTest, FinalReclamationBlockedWhileReplyBytesRetained) {
    int peer;
    client *c = newFastpathClient(&peer);
    ClientControl *cc = c->control;

    size_t vlen = 12 * 1024;
    sds val = fpMakeValue(vlen);
    sds set = fpMakeSet("rc:ky", val, vlen);
    fpFeedRequest(c, peer, set, sdslen(set), 1);
    sdsfree(set);
    EXPECT_EQ(c->fp_inflight, 1u);
    fastpathSubmitPending(1);
    EXPECT_EQ(fastpathDrain(), 1);
    EXPECT_EQ(fastpathProcessReturns(1), 1);
    EXPECT_EQ(recv(peer), "+OK\r\n");

    fpThrottleSocket(c->conn->fd, peer);

    send(peer, "*2\r\n$3\r\nGET\r\n$5\r\nrc:ky\r\n");
    fpReadUntilInflight(c, 1);
    EXPECT_EQ(c->fp_inflight, 1u);
    fastpathSubmitPending(1);
    EXPECT_EQ(fastpathDrain(), 1);
    EXPECT_EQ(fastpathProcessReturns(1), 1);
    EXPECT_TRUE(c->fp_out != nullptr); /* the clamped receiver must have forced a partial write */
    EXPECT_GT(c->fp_out ? sdslen(c->fp_out) : 0, 0u);

    /* Retained bytes hold the reply-memory gate shut: outstanding is positive, so reclaim is blocked. */
    EXPECT_GT(fastpathReplyOutstanding(cc), 0u);
    EXPECT_GT(cc->pin_refs, 0u);

    sds got = fpDrainInto(peer, sdsempty());
    for (int guard = 0; guard < 100000 && c->fp_out && sdslen(c->fp_out) > 0; guard++) {
        fastpathClientWritable(1, c);
        got = fpDrainInto(peer, got);
    }
    got = fpDrainInto(peer, got);
    sds expect = fpBulk(val, vlen);
    EXPECT_EQ(sdscmp(got, expect), 0);
    EXPECT_EQ(fastpathReplyOutstanding(cc), 0u);
    sdsfree(got);
    sdsfree(expect);
    sdsfree(val);

    quiesceAndFree(c, peer); /* handoff + freeClient; reclaim asserts pin_refs == 0 && outstanding == 0 */
}

/* An immediate EVICT published after main charged and returned a batch, but before the owner delivers
 * it, closes the client before delivery: the reply is discarded, released exactly once, never written
 * to the peer, and outstanding returns to zero. */
TEST_F(FastpathClientControlTest, EvictAfterChargedBatchReturnedDiscardsReplyBeforeDelivery) {
    int peer;
    client *c = newFastpathClient(&peer);
    ClientControl *cc = c->control;

    send(peer, "*2\r\n$4\r\nINCR\r\n$4\r\nev:a\r\n");
    fastpathClientReadable(1, c);
    EXPECT_EQ(c->fp_inflight, 1u);
    fastpathSubmitPending(1);

    /* Main charges and returns the batch; the reply is retained but not yet delivered. */
    EXPECT_EQ(fastpathDrain(), 1);
    EXPECT_GT(fastpathReplyOutstanding(cc), 0u);

    /* EVICT lands after the charge and before the owner delivers: it must win immediately. */
    fastpathControlRequest(cc, CC_REQ_EVICT);

    EXPECT_EQ(fastpathProcessReturns(1), 1);
    EXPECT_EQ(c->control->lifecycle, FP_CLOSING);  /* evicted before delivery, never handed off alive */
    EXPECT_TRUE(fpPeerHasNoReply(peer));           /* the discarded reply never reached the peer */
    EXPECT_EQ(fastpathReplyOutstanding(cc), 0u);   /* batch bytes released exactly once on discard */
    EXPECT_EQ(cc->reply_bytes_produced, cc->reply_bytes_released);

    ASSERT_EQ(c->flag.fastpath, 1u);
    fastpathHandoffDone(c, 1); /* frees it; reclaim asserts outstanding == 0 */
    EXPECT_EQ(fastpathWorkerOwnedClients(1), 0u);
    fastpathWorkerReopen(1);
    close(peer);
}

/* An EVICT with fp_out residue from a partial write to a stuck (throttled, unread) peer discards the
 * residue and releases it once, reaching FP_CLOSING in a single owner pass with no wait on the reader. */
TEST_F(FastpathClientControlTest, EvictDiscardsBufferedResidueWithoutStuckReaderWait) {
    int peer;
    client *c = newFastpathClient(&peer);
    ClientControl *cc = c->control;

    size_t vlen = 12 * 1024;
    sds val = fpMakeValue(vlen);
    sds set = fpMakeSet("ev:r", val, vlen);
    sdsfree(val);
    fpFeedRequest(c, peer, set, sdslen(set), 1);
    sdsfree(set);
    EXPECT_EQ(c->fp_inflight, 1u);
    fastpathSubmitPending(1);
    EXPECT_EQ(fastpathDrain(), 1);
    EXPECT_EQ(fastpathProcessReturns(1), 1);
    EXPECT_EQ(recv(peer), "+OK\r\n");

    fpThrottleSocket(c->conn->fd, peer);

    send(peer, "*2\r\n$3\r\nGET\r\n$4\r\nev:r\r\n");
    fpReadUntilInflight(c, 1);
    EXPECT_EQ(c->fp_inflight, 1u);
    fastpathSubmitPending(1);
    EXPECT_EQ(fastpathDrain(), 1);
    EXPECT_EQ(fastpathProcessReturns(1), 1);
    EXPECT_TRUE(c->fp_out != nullptr); /* the clamped receiver must have forced a partial write */
    size_t residue = c->fp_out ? sdslen(c->fp_out) : 0;
    EXPECT_GT(residue, 0u);
    EXPECT_EQ(fastpathReplyOutstanding(cc), residue);

    /* EVICT while the peer is not reading: one owner pass discards the residue and balances. */
    fastpathControlRequest(cc, CC_REQ_EVICT);
    fastpathProcessReturns(1);
    EXPECT_EQ(c->control->lifecycle, FP_CLOSING);
    EXPECT_EQ(fastpathReplyOutstanding(cc), 0u); /* residue discarded and released once */
    EXPECT_EQ(cc->reply_bytes_produced, cc->reply_bytes_released);

    ASSERT_EQ(c->flag.fastpath, 1u);
    fastpathHandoffDone(c, 1);
    EXPECT_EQ(fastpathWorkerOwnedClients(1), 0u);
    fastpathWorkerReopen(1);
    close(peer);
}

/* An EVICT arriving after HANDOFF already moved the client to FP_LEAVING must scan the leaving list and
 * upgrade it to FP_CLOSING, never handing off a live connection; the in-flight reply is then discarded. */
TEST_F(FastpathClientControlTest, EvictUpgradesLeavingClientToClosingViaLeavingScan) {
    int peer;
    client *c = newFastpathClient(&peer);
    ClientControl *cc = c->control;

    send(peer, "*2\r\n$4\r\nINCR\r\n$4\r\nlv:a\r\n");
    fastpathClientReadable(1, c);
    EXPECT_EQ(c->fp_inflight, 1u);
    fastpathSubmitPending(1); /* the reply is in main's inbox, not yet drained */

    /* HANDOFF moves the client to FP_LEAVING; fp_inflight > 0 keeps it owned, not yet handed back. */
    fastpathControlRequest(cc, CC_REQ_HANDOFF);
    fastpathProcessReturns(1);
    EXPECT_EQ(c->control->lifecycle, FP_LEAVING);

    /* EVICT now targets a client already in the leaving list, with no batch delivered this pass: the
     * owner's drain must scan leaving and upgrade FP_LEAVING to FP_CLOSING rather than strand it. */
    fastpathControlRequest(cc, CC_REQ_EVICT);
    fastpathProcessReturns(1);
    EXPECT_EQ(c->control->lifecycle, FP_CLOSING);

    /* The in-flight reply returns to a closing client: discarded, released once, never written. */
    EXPECT_EQ(fastpathDrain(), 1);
    EXPECT_EQ(fastpathProcessReturns(1), 1);
    EXPECT_TRUE(fpPeerHasNoReply(peer));
    EXPECT_EQ(fastpathReplyOutstanding(cc), 0u);
    EXPECT_EQ(cc->reply_bytes_produced, cc->reply_bytes_released);

    ASSERT_EQ(c->flag.fastpath, 1u);
    fastpathHandoffDone(c, 1);
    EXPECT_EQ(fastpathWorkerOwnedClients(1), 0u);
    fastpathWorkerReopen(1);
    close(peer);
}

/* Request precedence: EVICT coalesces with lesser bits but wins over HANDOFF/QUIESCE at execution
 * (terminal FP_CLOSING, not FP_LEAVING); CLOSE supersedes EVICT in the word; repeated requests are
 * idempotent. */
TEST_F(FastpathClientControlTest, EvictPrecedenceOverHandoffQuiesceAndCloseOverEvict) {
    int p1;
    client *c1 = newFastpathClient(&p1);
    ClientControl *cc1 = c1->control;

    /* EVICT coalesces with the lesser bits rather than clearing them; only CLOSE clears. */
    fastpathControlRequest(cc1, CC_REQ_QUIESCE);
    fastpathControlRequest(cc1, CC_REQ_HANDOFF);
    fastpathControlRequest(cc1, CC_REQ_EVICT);
    EXPECT_EQ(cc1->requests & CC_REQ_QUIESCE, (uint32_t)CC_REQ_QUIESCE);
    EXPECT_EQ(cc1->requests & CC_REQ_HANDOFF, (uint32_t)CC_REQ_HANDOFF);
    EXPECT_EQ(cc1->requests & CC_REQ_EVICT, (uint32_t)CC_REQ_EVICT);

    /* Idempotent: re-publishing EVICT leaves the word unchanged. */
    uint32_t before = cc1->requests;
    fastpathControlRequest(cc1, CC_REQ_EVICT);
    EXPECT_EQ(cc1->requests, before);

    /* At execution the terminal EVICT wins over the pending HANDOFF and QUIESCE. */
    fastpathProcessReturns(1);
    EXPECT_EQ(c1->control->lifecycle, FP_CLOSING);
    ASSERT_EQ(c1->flag.fastpath, 1u);
    fastpathHandoffDone(c1, 1);
    EXPECT_EQ(fastpathWorkerOwnedClients(1), 0u);
    fastpathWorkerReopen(1);
    close(p1);

    /* CLOSE supersedes EVICT in the word, leaving only CLOSE; a later lesser request is ignored. */
    int p2;
    client *c2 = newFastpathClient(&p2);
    ClientControl *cc2 = c2->control;
    fastpathControlRequest(cc2, CC_REQ_EVICT);
    fastpathControlRequest(cc2, CC_REQ_CLOSE);
    EXPECT_EQ(cc2->requests, (uint32_t)CC_REQ_CLOSE);
    fastpathControlRequest(cc2, CC_REQ_EVICT);
    EXPECT_EQ(cc2->requests, (uint32_t)CC_REQ_CLOSE);

    fastpathProcessReturns(1);
    EXPECT_EQ(c2->control->lifecycle, FP_CLOSING);
    ASSERT_EQ(c2->flag.fastpath, 1u);
    fastpathHandoffDone(c2, 1);
    EXPECT_EQ(fastpathWorkerOwnedClients(1), 0u);
    fastpathWorkerReopen(1);
    close(p2);
}

/* Attach/request ordering: with a charged batch for one client ahead of a second client's unconsumed
 * attach in the return ring, delivering the batch first must not lose an EVICT published against the
 * not-yet-registered control. The peeked signal is not cleared during delivery, so the post-loop drain
 * catches it once the attach is consumed. */
TEST_F(FastpathClientControlTest, EvictSurvivesUnconsumedAttachWhileBatchProcessedFirst) {
    int pa;
    client *a = newFastpathClient(&pa);
    ClientControl *ca = a->control;
    send(pa, "*2\r\n$4\r\nINCR\r\n$4\r\nat:a\r\n");
    fastpathClientReadable(1, a);
    EXPECT_EQ(a->fp_inflight, 1u);
    fastpathSubmitPending(1);
    EXPECT_EQ(fastpathDrain(), 1); /* batchA now sits in the return ring, undelivered */

    /* Second client: its attach request stays unconsumed in the ring behind batchA, then EVICT is
     * published against it before the owner registers it. */
    int pb;
    client *b = fpAttachPending(&pb);
    ClientControl *cb = b->control;
    fastpathControlRequest(cb, CC_REQ_EVICT);

    /* One owner pass: batchA delivers first, then the attach is consumed; the EVICT must survive. */
    EXPECT_EQ(fastpathProcessReturns(1), 2);
    EXPECT_EQ(recv(pa), ":1\r\n");                 /* cA, with no request, delivered normally */
    EXPECT_EQ(b->control->lifecycle, FP_CLOSING);  /* the EVICT survived the attach/batch ordering */
    EXPECT_EQ(fastpathReplyOutstanding(cb), 0u);   /* the freshly attached client owed no replies */
    EXPECT_EQ(fastpathReplyOutstanding(ca), 0u);

    ASSERT_EQ(b->flag.fastpath, 1u);
    fastpathHandoffDone(b, 1); /* the evicted client was closed and handed back in that pass */
    close(pb);

    quiesceAndFree(a, pa); /* the untouched client hands back and frees cleanly */
}

/* No pending request: a returned batch delivers normally, the client stays FP_ACTIVE, and accounting
 * balances, confirming the pre-delivery request check is a no-op when nothing is pending. */
TEST_F(FastpathClientControlTest, NoPendingRequestPreservesNormalDelivery) {
    int peer;
    client *c = newFastpathClient(&peer);
    ClientControl *cc = c->control;

    send(peer, "*2\r\n$4\r\nINCR\r\n$4\r\nnp:a\r\n");
    fastpathClientReadable(1, c);
    fastpathSubmitPending(1);
    EXPECT_EQ(fastpathDrain(), 1);
    EXPECT_EQ(fastpathProcessReturns(1), 1);
    EXPECT_EQ(recv(peer), ":1\r\n");              /* delivered normally, nothing discarded */
    EXPECT_EQ(c->control->lifecycle, FP_ACTIVE);  /* no request: the client stays active */
    EXPECT_EQ(fastpathReplyOutstanding(cc), 0u);
    EXPECT_EQ(cc->reply_bytes_produced, cc->reply_bytes_released);

    quiesceAndFree(c, peer);
}
