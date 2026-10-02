/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Runs a fast-path batch through the main-thread executor while the
 * originating client's memory is unmapped: main resolves every entry through
 * its ClientHandle + CommandOrigin and never dereferences the connection, so
 * the unmapped client cannot fault, and origin data must still be intact. */

#include "generated_wrappers.hpp"

#include <arpa/inet.h>
#include <cstring>
#include <fcntl.h>
#include <string>
#include <sys/epoll.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

extern "C" {
#include "connection.h"
#include "fastpath.h"
#include "io_threads.h"
#include "module.h"
#include "server.h"
extern hashtableType commandSetType;
extern dictType keylistDictType;
void createSharedObjects(void);
void ACLFreeUser(user *u);
int modulePopulateClientInfoStructure(void *ci, client *client, int structver);
}

static const char *fp_logfile = "/dev/null";

class FastpathExecutorTest : public ::testing::Test {
  protected:
    static void SetUpTestSuite() {
        monotonicInit();
        memset(&server, 0, sizeof(server));
        server.hz = CONFIG_DEFAULT_HZ;
        server.logfile = const_cast<char *>(fp_logfile);
        server.verbosity = LL_WARNING;
        server.main_thread_id = pthread_self();
        server.client_max_querybuf_len = 1024ll * 1024 * 1024;
        server.proto_max_bulk_len = 512ll * 1024 * 1024;
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
    }

    /* Read whatever the executor produced for the client from the other end of the pair. */
    static std::string readReply(int fd) {
        char buf[256];
        ssize_t n = read(fd, buf, sizeof(buf));
        return n > 0 ? std::string(buf, n) : std::string();
    }
};

TEST_F(FastpathExecutorTest, MainExecutesWithOriginOnlyWhileClientIsUnmapped) {
    int sv[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
    connection *conn = connCreateAccepted(connectionByType(CONN_TYPE_SOCKET), sv[0], NULL);
    conn->state = CONN_STATE_CONNECTED;

    /* The client lives alone in its own pages so it can be unmapped while its batch runs. */
    size_t pagesz = sysconf(_SC_PAGESIZE);
    size_t maplen = (sizeof(client) + pagesz - 1) / pagesz * pagesz;
    client *c = static_cast<client *>(mmap(NULL, maplen, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
    ASSERT_NE(c, MAP_FAILED);
    client *tmp = createClient(NULL);
    memcpy(c, tmp, sizeof(client));
    zfree(tmp);
    c->conn = conn;
    connSetPrivateData(conn, c);
    c->id = 424242;
    c->fp_out = NULL;
    struct sockaddr_in6 sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin6_family = AF_INET6;
    sa.sin6_port = htons(31337);
    ASSERT_EQ(inet_pton(AF_INET6, "2001:db8::7", &sa.sin6_addr), 1);
    ASSERT_EQ(peerIdentityFromSockaddr(&c->fp_peer, (struct sockaddr *)&sa, sizeof(sa)), C_OK);
    c->fp_local = c->fp_peer;

    /* Admit through the real flow so the connection gets a ClientControl and the IO thread owns it;
     * its control is a separate allocation, reachable while the client's own pages are unmapped. */
    ASSERT_EQ(fastpathAttach(c), C_OK);
    ASSERT_EQ(c->io_tid, 1);
    ASSERT_EQ(fastpathProcessReturns(1), 1); /* the attach request: thread takes ownership */

    /* Two pipelined commands: GET on a missing key and SET, both plain data commands. */
    const char *req = "*2\r\n$3\r\nGET\r\n$5\r\nnokey\r\n*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    ASSERT_EQ(write(sv[1], req, strlen(req)), (ssize_t)strlen(req));
    fastpathClientReadable(1, c);
    ASSERT_EQ(c->fp_inflight, 2u);
    fastpathSubmitPending(1);

    /* Main runs the batch while the client is unreachable: it must resolve via handle + origin only. */
    ASSERT_EQ(mprotect(c, maplen, PROT_NONE), 0);
    EXPECT_EQ(fastpathDrain(), 2);
    ASSERT_EQ(mprotect(c, maplen, PROT_READ | PROT_WRITE), 0);

    /* Back on the IO thread the handle resolves to the client again: replies go out, entries released. */
    EXPECT_EQ(fastpathProcessReturns(1), 1);
    EXPECT_EQ(c->fp_inflight, 0u);
    EXPECT_EQ(readReply(sv[1]), "$-1\r\n+OK\r\n");
    robj *key = createStringObject("foo", 3);
    EXPECT_NE(lookupKeyRead(server.db[0], key), nullptr);
    decrRefCount(key);

    /* Quiesce so the thread hands the client back and unregisters it (no freeClient on mmap'd pages);
     * main completes the hand-off, then reclaim its separately allocated control so nothing is owned. */
    fastpathWorkerQuiesce(1);
    fastpathProcessReturns(1);
    fastpathHandoffDone(c, 0); /* main takes the client back: drops the owner pin and the routed count */
    EXPECT_EQ(fastpathWorkerOwnedClients(1), 0u);
    fastpathControlReclaim(c);

    close(sv[1]);
    close(sv[0]);
    conn->fd = -1;
    munmap(c, maplen);
}

/* A retired user struct is kept until the last fast-path client naming it hands off,
 * and every hop of a retire chain drops one reference per returning client. */
TEST_F(FastpathExecutorTest, RetiredPrincipalResolvesAndFreesAtHandoff) {
    user *u1 = ACLCreateUnlinkedUser();
    user *u2 = ACLCreateUnlinkedUser();
    user *u3 = ACLCreateUnlinkedUser();
    EXPECT_EQ(ACLResolveUser(u1), u1);

    u1->flags |= USER_FLAG_RETIRED;
    u1->successor = u2;
    u1->fp_refs = 1;
    EXPECT_EQ(ACLResolveUser(u1), u2);
    u2->flags |= USER_FLAG_RETIRED;
    u2->successor = u3;
    u2->fp_refs = 2; /* one client behind u1, one pointing at u2 directly */
    EXPECT_EQ(ACLResolveUser(u1), u3);
    EXPECT_EQ(ACLResolveUser(u2), u3);

    client a;
    memset(&a, 0, sizeof(a));
    a.user = u1;
    a.flag.authenticated = 1;
    ACLFastpathClientReturned(&a);
    EXPECT_EQ(a.user, u3);
    EXPECT_EQ(a.flag.authenticated, 1u); /* u1 is gone now; u2 still has one client */
    EXPECT_EQ(u2->fp_refs, 1u);

    /* The successor was deleted meanwhile: the returning client is demoted, as unstable does before closing it. */
    u2->successor = NULL;
    client b;
    memset(&b, 0, sizeof(b));
    b.user = u2;
    b.flag.authenticated = 1;
    ACLFastpathClientReturned(&b);
    EXPECT_EQ(b.user, DefaultUser);
    EXPECT_EQ(b.flag.authenticated, 0u);

    /* A live principal is left alone. */
    client d;
    memset(&d, 0, sizeof(d));
    d.user = u3;
    ACLFastpathClientReturned(&d);
    EXPECT_EQ(d.user, u3);
    ACLFreeUser(u3);
}

/* A module asking about a fast-path client by id gets the admission-time peer, not a
 * getpeername() on the IO-owned socket. */
TEST_F(FastpathExecutorTest, ClientInfoOfFastpathClientComesFromAdmissionPeer) {
    int sv[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
    connection *conn = connCreateAccepted(connectionByType(CONN_TYPE_SOCKET), sv[0], NULL);
    conn->state = CONN_STATE_CONNECTED;
    client *c = createClient(NULL);
    c->conn = conn;
    connSetPrivateData(conn, c);
    c->id = 515151;
    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_port = htons(40001);
    ASSERT_EQ(inet_pton(AF_INET, "192.0.2.77", &sa.sin_addr), 1);
    ASSERT_EQ(peerIdentityFromSockaddr(&c->fp_peer, (struct sockaddr *)&sa, sizeof(sa)), C_OK);

    ValkeyModuleClientInfoV1 ci;
    memset(&ci, 0, sizeof(ci));
    ci.version = 1;
    c->flag.fastpath = 1;
    ASSERT_EQ(modulePopulateClientInfoStructure(&ci, c, 1), VALKEYMODULE_OK);
    EXPECT_STREQ(ci.addr, "192.0.2.77");
    EXPECT_EQ(ci.port, 40001);
    EXPECT_EQ(ci.id, 515151u);
    EXPECT_EQ(ci.db, 0);

    /* Off the fast path the socket is main's again and is asked directly. */
    c->flag.fastpath = 0;
    ASSERT_EQ(modulePopulateClientInfoStructure(&ci, c, 1), VALKEYMODULE_OK);
    EXPECT_STRNE(ci.addr, "192.0.2.77");

    close(sv[1]);
    close(sv[0]);
    conn->fd = -1;
}

/* An offloaded write advances the global replication offset on main; the IO owner must copy that
 * offset onto the origin client's woff when the batch returns, so a WAIT/WAITAOF the client pipelines
 * afterwards (run on the main path) waits on the right offset. A read-only command leaves woff. */
TEST_F(FastpathExecutorTest, OffloadedWritePropagatesWoffToOriginClient) {
    int sv[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
    connection *conn = connCreateAccepted(connectionByType(CONN_TYPE_SOCKET), sv[0], NULL);
    conn->state = CONN_STATE_CONNECTED;
    client *c = createClient(NULL);
    c->conn = conn;
    connSetPrivateData(conn, c);
    c->id = 626262;
    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_port = htons(40002);
    ASSERT_EQ(inet_pton(AF_INET, "192.0.2.90", &sa.sin_addr), 1);
    ASSERT_EQ(peerIdentityFromSockaddr(&c->fp_peer, (struct sockaddr *)&sa, sizeof(sa)), C_OK);
    c->fp_local = c->fp_peer;
    c->woff = 0;

    /* A top-level primary with no backlog/replicas still advances primary_repl_offset by 1 per
     * propagated write (used for AOF fsync tracking); that is the offset the write "reaches".
     * With AOF on, propagateNow feeds replicas even with none attached (the WAITAOF path), so the
     * offset advances here exactly as it would for a real WAITAOF-relevant write. */
    server.replication_allowed = 1;
    server.primary_host = NULL;
    server.aof_state = AOF_ON;
    server.aof_buf = sdsempty();
    server.aof_selected_db = 0;

    ASSERT_EQ(fastpathAttach(c), C_OK);
    ASSERT_EQ(c->io_tid, 1);
    ASSERT_EQ(fastpathProcessReturns(1), 1); /* attach: the thread takes ownership */

    /* A read-only GET must not move woff. */
    long long off_before = server.primary_repl_offset;
    const char *getreq = "*2\r\n$3\r\nGET\r\n$5\r\nnokey\r\n";
    ASSERT_EQ(write(sv[1], getreq, strlen(getreq)), (ssize_t)strlen(getreq));
    fastpathClientReadable(1, c);
    fastpathSubmitPending(1);
    EXPECT_EQ(fastpathDrain(), 1);
    EXPECT_EQ(fastpathProcessReturns(1), 1);
    EXPECT_EQ(c->woff, 0);                             /* read did not propagate */
    EXPECT_EQ(server.primary_repl_offset, off_before); /* and did not advance the global offset */

    /* A SET propagates: primary_repl_offset advances (even with no backlog/replicas) and the IO
     * owner must carry that offset onto the origin client's woff. */
    const char *setreq = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    ASSERT_EQ(write(sv[1], setreq, strlen(setreq)), (ssize_t)strlen(setreq));
    fastpathClientReadable(1, c);
    fastpathSubmitPending(1);
    EXPECT_EQ(fastpathDrain(), 1);
    EXPECT_EQ(fastpathProcessReturns(1), 1);
    EXPECT_GT(server.primary_repl_offset, off_before); /* the write advanced the global offset */
    EXPECT_EQ(c->woff, server.primary_repl_offset);    /* and it was applied to the origin client */

    /* woff is monotonic: a later read must not lower it. */
    long long woff_after_set = c->woff;
    ASSERT_EQ(write(sv[1], getreq, strlen(getreq)), (ssize_t)strlen(getreq));
    fastpathClientReadable(1, c);
    fastpathSubmitPending(1);
    EXPECT_EQ(fastpathDrain(), 1);
    EXPECT_EQ(fastpathProcessReturns(1), 1);
    EXPECT_EQ(c->woff, woff_after_set);

    fastpathWorkerQuiesce(1);
    fastpathProcessReturns(1);
    fastpathHandoffDone(c, 0);
    fastpathControlReclaim(c);
    freeClient(c);
    close(sv[1]);
}

/* The blocker case: a write is in flight when the client transitions to FP_LEAVING (the WAIT/WAITAOF
 * requeue moves it off the fast path via a handoff request). The write batch returns while the client
 * is LEAVING, so the woff must be applied to a LEAVING (not only ACTIVE) client, or the requeued WAIT
 * on the main path reads a stale woff. CC_REQ_HANDOFF drives the same ACTIVE->LEAVING transition the
 * WAIT requeue does, deterministically: it is applied during the return pass, before delivery. */
TEST_F(FastpathExecutorTest, WoffAppliedToLeavingClientBeforeHandoff) {
    int sv[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
    connection *conn = connCreateAccepted(connectionByType(CONN_TYPE_SOCKET), sv[0], NULL);
    conn->state = CONN_STATE_CONNECTED;
    client *c = createClient(NULL);
    c->conn = conn;
    connSetPrivateData(conn, c);
    c->id = 727272;
    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_port = htons(40003);
    ASSERT_EQ(inet_pton(AF_INET, "192.0.2.91", &sa.sin_addr), 1);
    ASSERT_EQ(peerIdentityFromSockaddr(&c->fp_peer, (struct sockaddr *)&sa, sizeof(sa)), C_OK);
    c->fp_local = c->fp_peer;
    c->woff = 0;

    server.replication_allowed = 1;
    server.primary_host = NULL;
    server.aof_state = AOF_ON;
    server.aof_buf = sdsempty();
    server.aof_selected_db = 0;

    ASSERT_EQ(fastpathAttach(c), C_OK);
    ASSERT_EQ(c->io_tid, 1);
    ASSERT_EQ(fastpathProcessReturns(1), 1);

    long long off_before = server.primary_repl_offset;
    /* Publish a SET; it is in flight (not yet returned). */
    const char *setreq = "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n";
    ASSERT_EQ(write(sv[1], setreq, strlen(setreq)), (ssize_t)strlen(setreq));
    fastpathClientReadable(1, c);
    fastpathSubmitPending(1);
    ASSERT_EQ(c->fp_inflight, 1u);
    ASSERT_EQ(c->control->lifecycle, FP_ACTIVE);

    /* Execute the SET on main (advances the global offset) but do NOT process the return yet. */
    EXPECT_EQ(fastpathDrain(), 1);
    EXPECT_GT(server.primary_repl_offset, off_before);

    /* Request handoff: the same non-terminal transition a queued WAIT/WAITAOF causes. It moves the
     * client ACTIVE->LEAVING during the return pass, before the SET's reply and woff are delivered. */
    fastpathControlRequest(c->control, CC_REQ_HANDOFF);

    fastpathProcessReturns(1);
    /* woff applied to the LEAVING client, before handoff completes: this is the fix under test. */
    EXPECT_EQ(c->woff, server.primary_repl_offset);
    EXPECT_EQ(c->control->lifecycle, FP_LEAVING);

    fastpathWorkerQuiesce(1);
    fastpathProcessReturns(1);
    fastpathHandoffDone(c, 0);
    fastpathControlReclaim(c);
    freeClient(c);
    close(sv[1]);
}

/* The dynamic capability gate is authoritative at execution: a write admitted while the fast path
 * was capable must be requeued to main, not executed on the scratch executor, once a global gate
 * closes (here an in-progress coordinated failover). This pins the "gates change while clients are
 * active -> conservatively requeue/hand off, not run under stale eligibility" invariant. */
TEST_F(FastpathExecutorTest, FailoverGateRequeuesAdmittedWrite) {
    int sv[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
    connection *conn = connCreateAccepted(connectionByType(CONN_TYPE_SOCKET), sv[0], NULL);
    conn->state = CONN_STATE_CONNECTED;
    client *c = createClient(NULL);
    c->conn = conn;
    connSetPrivateData(conn, c);
    c->id = 727272;
    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_port = htons(40003);
    ASSERT_EQ(inet_pton(AF_INET, "192.0.2.91", &sa.sin_addr), 1);
    ASSERT_EQ(peerIdentityFromSockaddr(&c->fp_peer, (struct sockaddr *)&sa, sizeof(sa)), C_OK);
    c->fp_local = c->fp_peer;
    c->woff = 0;

    server.replication_allowed = 1;
    server.primary_host = NULL;
    server.aof_state = AOF_ON;
    server.aof_buf = sdsempty();
    server.aof_selected_db = 0;
    server.failover_state = NO_FAILOVER;

    ASSERT_EQ(fastpathAttach(c), C_OK);
    ASSERT_EQ(c->io_tid, 1);
    ASSERT_EQ(fastpathProcessReturns(1), 1);

    robj *key = createStringObject("gatekey", 7);
    long long off_before = server.primary_repl_offset;

    /* Admit a SET while the fast path is capable, then close the gate before draining: the executor
     * must requeue it, so the global offset does not advance and the key is not written here. */
    const char *setreq = "*3\r\n$3\r\nSET\r\n$7\r\ngatekey\r\n$3\r\nbar\r\n";
    ASSERT_EQ(write(sv[1], setreq, strlen(setreq)), (ssize_t)strlen(setreq));
    fastpathClientReadable(1, c);
    fastpathSubmitPending(1);
    server.failover_state = FAILOVER_IN_PROGRESS; /* gate closes after admission, before execution */
    EXPECT_EQ(fastpathDrain(), 1);
    EXPECT_EQ(fastpathProcessReturns(1), 1);
    EXPECT_EQ(server.primary_repl_offset, off_before);    /* requeued: nothing executed on main here */
    EXPECT_EQ(lookupKeyRead(server.db[0], key), nullptr); /* the write did not run under the closed gate */
    EXPECT_EQ(c->control->lifecycle, FP_LEAVING);         /* and the client was handed off to main */

    server.failover_state = NO_FAILOVER;
    decrRefCount(key);
    fastpathWorkerQuiesce(1);
    fastpathProcessReturns(1);
    fastpathHandoffDone(c, 0);
    fastpathControlReclaim(c);
    freeClient(c);
    close(sv[1]);
}

/* Parse a "name:value\r\n" counter out of the fastpathInfo() block. Returns -1 if absent. */
static long long infoCounter(const char *field) {
    sds info = sdsempty();
    fastpathInfo(&info);
    long long val = -1;
    char needle[64];
    snprintf(needle, sizeof(needle), "%s:", field);
    char *p = strstr(info, needle);
    if (p) val = strtoll(p + strlen(needle), NULL, 10);
    sdsfree(info);
    return val;
}

/* The transport captured at admission on the IO owner must ride into the entry's CommandOrigin. A
 * unix-socket origin executes correctly through a real batch even though the executor's own conn is
 * NULL and it never carries the origin's unix_socket flag; the origin->conn_type predicate that
 * MONITOR/tracing read is pinned separately in the CommandOrigin unit tests. */
TEST_F(FastpathExecutorTest, OriginConnTypeFlowsFromAdmissionToExecutor) {
    int sv[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
    connection *conn = connCreateAccepted(connectionByType(CONN_TYPE_SOCKET), sv[0], NULL);
    conn->state = CONN_STATE_CONNECTED;
    client *c = createClient(NULL);
    c->conn = conn;
    connSetPrivateData(conn, c);
    c->id = 828282;
    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_port = htons(40010);
    ASSERT_EQ(inet_pton(AF_INET, "192.0.2.100", &sa.sin_addr), 1);
    ASSERT_EQ(peerIdentityFromSockaddr(&c->fp_peer, (struct sockaddr *)&sa, sizeof(sa)), C_OK);
    c->fp_local = c->fp_peer;
    c->fp_conn_type = CONN_TYPE_UNIX; /* pretend this origin arrived over a unix socket */

    ASSERT_EQ(fastpathAttach(c), C_OK);
    ASSERT_EQ(c->io_tid, 1);
    ASSERT_EQ(fastpathProcessReturns(1), 1);

    const char *getreq = "*2\r\n$3\r\nGET\r\n$5\r\nnokey\r\n";
    ASSERT_EQ(write(sv[1], getreq, strlen(getreq)), (ssize_t)strlen(getreq));
    fastpathClientReadable(1, c);
    fastpathSubmitPending(1);
    EXPECT_EQ(fastpathDrain(), 1);
    EXPECT_EQ(fastpathProcessReturns(1), 1);
    EXPECT_EQ(readReply(sv[1]), "$-1\r\n"); /* the unix-origin command executed correctly on main */

    fastpathWorkerQuiesce(1);
    fastpathProcessReturns(1);
    fastpathHandoffDone(c, 0);
    fastpathControlReclaim(c);
    freeClient(c);
    close(sv[1]);
}

/* A gate that closes after admission makes main requeue the admitted entry; the IO owner counts that
 * as a requeue-gate fallback exactly once (no double-count across the return pass). */
TEST_F(FastpathExecutorTest, RequeueGateCounterIncrementsOncePerRequeue) {
    long long rq_before = infoCounter("fastpath_requeue_gate");
    ASSERT_GE(rq_before, 0);

    int sv[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
    connection *conn = connCreateAccepted(connectionByType(CONN_TYPE_SOCKET), sv[0], NULL);
    conn->state = CONN_STATE_CONNECTED;
    client *c = createClient(NULL);
    c->conn = conn;
    connSetPrivateData(conn, c);
    c->id = 939393;
    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_port = htons(40011);
    ASSERT_EQ(inet_pton(AF_INET, "192.0.2.101", &sa.sin_addr), 1);
    ASSERT_EQ(peerIdentityFromSockaddr(&c->fp_peer, (struct sockaddr *)&sa, sizeof(sa)), C_OK);
    c->fp_local = c->fp_peer;
    c->woff = 0;

    server.replication_allowed = 1;
    server.primary_host = NULL;
    server.aof_state = AOF_ON;
    if (!server.aof_buf) server.aof_buf = sdsempty();
    server.aof_selected_db = 0;
    server.failover_state = NO_FAILOVER;

    ASSERT_EQ(fastpathAttach(c), C_OK);
    ASSERT_EQ(c->io_tid, 1);
    ASSERT_EQ(fastpathProcessReturns(1), 1);

    const char *setreq = "*3\r\n$3\r\nSET\r\n$7\r\ngatekey\r\n$3\r\nbar\r\n";
    ASSERT_EQ(write(sv[1], setreq, strlen(setreq)), (ssize_t)strlen(setreq));
    fastpathClientReadable(1, c);
    fastpathSubmitPending(1);
    server.failover_state = FAILOVER_IN_PROGRESS; /* close the gate after admission */
    EXPECT_EQ(fastpathDrain(), 1);
    EXPECT_EQ(fastpathProcessReturns(1), 1);

    long long rq_after = infoCounter("fastpath_requeue_gate");
    EXPECT_EQ(rq_after, rq_before + 1); /* exactly one requeued entry, counted once */

    server.failover_state = NO_FAILOVER;
    fastpathWorkerQuiesce(1);
    fastpathProcessReturns(1);
    fastpathHandoffDone(c, 0);
    fastpathControlReclaim(c);
    freeClient(c);
    close(sv[1]);
}

/* An attached fast-path client on one end of a socketpair; the test owns the other end. */
static client *fpCopyAvoidClient(int sv[2], uint64_t id) {
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) != 0) return NULL;
    connection *conn = connCreateAccepted(connectionByType(CONN_TYPE_SOCKET), sv[0], NULL);
    conn->state = CONN_STATE_CONNECTED;
    client *c = createClient(NULL);
    c->conn = conn;
    connSetPrivateData(conn, c);
    c->id = id;
    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_port = htons(40020);
    inet_pton(AF_INET, "192.0.2.120", &sa.sin_addr);
    peerIdentityFromSockaddr(&c->fp_peer, (struct sockaddr *)&sa, sizeof(sa));
    c->fp_local = c->fp_peer;
    c->woff = 0;
    server.failover_state = NO_FAILOVER;
    if (fastpathAttach(c) != C_OK) return NULL;
    fastpathProcessReturns(1); /* attach: the thread takes ownership */
    return c;
}

static robj *fpCopyAvoidStore(const char *key, size_t vlen, char seed) {
    sds v = sdsnewlen(NULL, vlen);
    for (size_t i = 0; i < vlen; i++) v[i] = (char)(seed + i % 23);
    robj *k = createStringObject(key, strlen(key));
    robj *o = createObject(OBJ_STRING, v);
    setKey(NULL, server.db[0], k, &o, 0);
    robj *stored = lookupKeyRead(server.db[0], k);
    decrRefCount(k);
    return stored;
}

static void fpCopyAvoidDelete(const char *key) {
    robj *k = createStringObject(key, strlen(key));
    dbDelete(server.db[0], k);
    decrRefCount(k);
}

static std::string fpBulk(robj *o) {
    size_t len = sdslen((sds)objectGetVal(o));
    return "$" + std::to_string(len) + "\r\n" + std::string((char *)objectGetVal(o), len) + "\r\n";
}

/* Reads until want bytes arrived, letting the IO owner flush what the socket refused earlier. */
static std::string fpReadAll(int fd, size_t want, client *c) {
    std::string got;
    std::vector<char> buf(65536);
    for (int guard = 0; guard < 200000 && got.size() < want; guard++) {
        ssize_t n = recv(fd, buf.data(), buf.size(), MSG_DONTWAIT);
        if (n > 0) {
            got.append(buf.data(), n);
            continue;
        }
        fastpathClientWritable(1, c);
    }
    return got;
}

static void fpCopyAvoidFinish(client *c, int peer) {
    fastpathWorkerQuiesce(1);
    fastpathProcessReturns(1);
    fastpathHandoffDone(c, 0);
    fastpathControlReclaim(c);
    freeClient(c);
    close(peer);
}

/* A large GET goes out by reference: main counts it as copy-avoided, the value stays referenced while
 * the IO owner sends it, and only main drops that reference, on the batch's extra trip. */
TEST_F(FastpathExecutorTest, CopyAvoidedReplyIsSentByReferenceAndReleasedOnMain) {
    int saved_min_threads = server.min_io_threads_copy_avoid;
    server.min_io_threads_copy_avoid = 1;
    robj *val = fpCopyAvoidStore("ca:big", 64 * 1024, 'a');
    ASSERT_EQ(val->refcount, 1u);

    int sv[2];
    client *c = fpCopyAvoidClient(sv, 727001);
    ASSERT_NE(c, nullptr);

    long long avoided = server.stat_reply_copy_avoided;
    const char *req = "*2\r\n$3\r\nGET\r\n$6\r\nca:big\r\n";
    ASSERT_EQ(write(sv[1], req, strlen(req)), (ssize_t)strlen(req));
    fastpathClientReadable(1, c);
    fastpathSubmitPending(1);
    EXPECT_EQ(fastpathDrain(), 1);
    EXPECT_EQ(server.stat_reply_copy_avoided, avoided + 1);
    EXPECT_EQ(val->refcount, 2u); /* the reply holds the value */

    EXPECT_EQ(fastpathProcessReturns(1), 1); /* delivered; the batch goes back for the release */
    EXPECT_EQ(val->refcount, 2u);            /* the IO owner never drops it */
    EXPECT_EQ(fastpathDrain(), 0);           /* the release trip runs no command */
    EXPECT_EQ(val->refcount, 1u);
    EXPECT_EQ(fastpathProcessReturns(1), 1); /* the released batch is recycled */

    std::string want = fpBulk(val);
    EXPECT_EQ(fpReadAll(sv[1], want.size(), c), want);
    EXPECT_EQ(fastpathReplyOutstanding(c->control), 0u);

    fpCopyAvoidFinish(c, sv[1]);
    fpCopyAvoidDelete("ca:big");
    server.min_io_threads_copy_avoid = saved_min_threads;
}

/* When the socket takes only part of a referenced reply, the rest is copied to the client's own output
 * before the release trip, so the value can be released while the reply is still being sent. */
TEST_F(FastpathExecutorTest, CopyAvoidedReplySurvivesPartialWriteAfterRelease) {
    int saved_min_threads = server.min_io_threads_copy_avoid;
    server.min_io_threads_copy_avoid = 1;
    robj *val = fpCopyAvoidStore("ca:huge", 1024 * 1024, 'k');

    int sv[2];
    client *c = fpCopyAvoidClient(sv, 727002);
    ASSERT_NE(c, nullptr);
    int small = 4096;
    ASSERT_EQ(setsockopt(sv[0], SOL_SOCKET, SO_SNDBUF, &small, sizeof(small)), 0);
    ASSERT_EQ(fcntl(sv[0], F_SETFL, fcntl(sv[0], F_GETFL) | O_NONBLOCK), 0);

    const char *req = "*2\r\n$3\r\nGET\r\n$7\r\nca:huge\r\n";
    ASSERT_EQ(write(sv[1], req, strlen(req)), (ssize_t)strlen(req));
    fastpathClientReadable(1, c);
    fastpathSubmitPending(1);
    EXPECT_EQ(fastpathDrain(), 1);
    EXPECT_EQ(fastpathProcessReturns(1), 1);
    EXPECT_GT(sdslen(c->fp_out), 0u); /* the socket refused most of it */
    EXPECT_EQ(fastpathDrain(), 0);
    EXPECT_EQ(val->refcount, 1u); /* released although the reply is still being sent */
    EXPECT_EQ(fastpathProcessReturns(1), 1);

    std::string want = fpBulk(val);
    fpCopyAvoidDelete("ca:huge"); /* the residue is the client's own copy */
    EXPECT_EQ(fpReadAll(sv[1], want.size(), c), want);
    EXPECT_EQ(fastpathReplyOutstanding(c->control), 0u);

    fpCopyAvoidFinish(c, sv[1]);
    server.min_io_threads_copy_avoid = saved_min_threads;
}

/* An MGET whose reply spills out of the arena mixes plain blocks and referenced elements; the flattened
 * region keeps their order and every reference is released. */
TEST_F(FastpathExecutorTest, CopyAvoidedSpillKeepsOrderAcrossPlainAndReferencedParts) {
    int saved_min_threads = server.min_io_threads_copy_avoid;
    int saved_min_size = server.min_string_size_copy_avoid_threaded;
    server.min_io_threads_copy_avoid = 0;
    server.min_string_size_copy_avoid_threaded = 32 * 1024;
    robj *plain = fpCopyAvoidStore("ca:mid", 20 * 1024, 'p'); /* below the size gate: copied */
    robj *big1 = fpCopyAvoidStore("ca:b1", 64 * 1024, 'q');
    robj *big2 = fpCopyAvoidStore("ca:b2", 96 * 1024, 'r');

    int sv[2];
    client *c = fpCopyAvoidClient(sv, 727003);
    ASSERT_NE(c, nullptr);

    long long avoided = server.stat_reply_copy_avoided;
    const char *req = "*6\r\n$4\r\nMGET\r\n$6\r\nca:mid\r\n$5\r\nca:b1\r\n$4\r\nnope\r\n$5\r\nca:b2\r\n$6\r\nca:mid\r\n";
    ASSERT_EQ(write(sv[1], req, strlen(req)), (ssize_t)strlen(req));
    fastpathClientReadable(1, c);
    fastpathSubmitPending(1);
    EXPECT_EQ(fastpathDrain(), 1);
    EXPECT_EQ(server.stat_reply_copy_avoided, avoided + 2);
    EXPECT_EQ(big1->refcount, 2u);
    EXPECT_EQ(big2->refcount, 2u);
    EXPECT_EQ(plain->refcount, 1u); /* copied, never referenced */
    EXPECT_EQ(fastpathProcessReturns(1), 1);
    EXPECT_EQ(fastpathDrain(), 0);
    EXPECT_EQ(big1->refcount, 1u);
    EXPECT_EQ(big2->refcount, 1u);
    EXPECT_EQ(fastpathProcessReturns(1), 1);

    std::string want = "*5\r\n" + fpBulk(plain) + fpBulk(big1) + "$-1\r\n" + fpBulk(big2) + fpBulk(plain);
    EXPECT_EQ(fpReadAll(sv[1], want.size(), c), want);
    EXPECT_EQ(fastpathReplyOutstanding(c->control), 0u);

    fpCopyAvoidFinish(c, sv[1]);
    fpCopyAvoidDelete("ca:mid");
    fpCopyAvoidDelete("ca:b1");
    fpCopyAvoidDelete("ca:b2");
    server.min_io_threads_copy_avoid = saved_min_threads;
    server.min_string_size_copy_avoid_threaded = saved_min_size;
}
