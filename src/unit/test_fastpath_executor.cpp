/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Runs a fast-path batch through the main-thread executor while the
 * originating client's memory is unmapped: any dereference of the
 * io_client cookie on main faults, and origin data must still be intact. */

#include "generated_wrappers.hpp"

#include <arpa/inet.h>
#include <cstring>
#include <sys/mman.h>
#include <sys/socket.h>
#include <unistd.h>

extern "C" {
#include "connection.h"
#include "fastpath.h"
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
        fastpathInitThread(1);
    }

    static void TearDownTestSuite() {
        fastpathFreeThread(1);
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
    c->flag.fastpath = 1;
    c->fp_state = FP_ACTIVE;
    c->io_tid = 1;
    c->fp_inflight = 0;
    c->fp_out = NULL;
    struct sockaddr_in6 sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin6_family = AF_INET6;
    sa.sin6_port = htons(31337);
    ASSERT_EQ(inet_pton(AF_INET6, "2001:db8::7", &sa.sin6_addr), 1);
    ASSERT_EQ(peerIdentityFromSockaddr(&c->fp_peer, (struct sockaddr *)&sa, sizeof(sa)), C_OK);

    /* Two pipelined commands: GET on a missing key and SET, both plain data commands. */
    const char *req = "*2\r\n$3\r\nGET\r\n$5\r\nnokey\r\n*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    ASSERT_EQ(write(sv[1], req, strlen(req)), (ssize_t)strlen(req));
    fastpathClientReadable(1, c);
    ASSERT_EQ(c->fp_inflight, 2u);
    fastpathSubmitPending(1);

    /* Main runs the batch while the client is unreachable. */
    ASSERT_EQ(mprotect(c, maplen, PROT_NONE), 0);
    EXPECT_EQ(fastpathDrain(), 2);
    ASSERT_EQ(mprotect(c, maplen, PROT_READ | PROT_WRITE), 0);

    /* Back on the IO thread the cookie is a client again: replies go out and the entries are released. */
    EXPECT_EQ(fastpathProcessReturns(1), 1);
    EXPECT_EQ(c->fp_inflight, 0u);
    EXPECT_EQ(readReply(sv[1]), "$-1\r\n+OK\r\n");
    robj *key = createStringObject("foo", 3);
    EXPECT_NE(lookupKeyRead(server.db[0], key), nullptr);
    decrRefCount(key);

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
