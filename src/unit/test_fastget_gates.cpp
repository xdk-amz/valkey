/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Gates that main enforces in processCommand but that a GET answered on an IO
 * thread (fastGetSpeculateBatch) must enforce itself: CLIENT PAUSE ALL, a replica
 * with a broken primary link and replica-serve-stale-data no, bytes main wrote
 * into the client's buffer or queued for its own write (F11), and an ACL rule
 * change made through a role (ACL SETROLE) that must recompute every client's
 * spec_acl_ok gate. On this branch main folds the pause, yield and failover
 * states into one gate word (fastGetRefreshMainGate) before the read phase; the
 * tests refresh it as afterSleep does. */

#include "generated_wrappers.hpp"
#include "test_server_init.hpp"

#include <cstring>

extern "C" {
#include "module.h"
#include "server.h"
/* fastget.h uses <stdatomic.h>, which C++ cannot include; declare what the test calls. */
int fastGetSpeculateBatch(struct client *c, int tid);
void fastGetRecomputeSpecAclOk(struct client *c);
void fastGetRefreshMainGate(void);
extern hashtableType commandSetType;
extern dictType keylistDictType;
extern dictType objToHashtableDictType;
extern hashtableType kvstoreChannelHashtableType;
void createSharedObjects(void);
void aclCommand(client *c);
}

class FastGetGatesTest : public ::testing::Test {
  protected:
    static void SetUpTestSuite() {
        monotonicInit();
        memset(&server, 0, sizeof(server));
        server.hz = CONFIG_DEFAULT_HZ;
        server.logfile = const_cast<char *>("/dev/null");
        server.verbosity = LL_WARNING;
        server.main_thread_id = pthread_self();
        server.maxclients = 1000;
        server.el = aeCreateEventLoop(64);
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
        server.unblocked_clients = listCreate();
        server.monitors = listCreate();
        server.clients_index = raxNew();
        server.errors = raxNew();
        for (int i = 0; i < COMMANDLOG_TYPE_NUM; i++) server.commandlog[i].threshold = -1;
        server.io_threads_num = 4;
        server.io_threads_fast_path = 1;
        server.repl_serve_stale_data = 1;
        server.repl_state = REPL_STATE_NONE;
        server.primary_host = NULL;
        server.paused_actions = 0;
        fastGetRefreshMainGate();
        server.acl_filename = const_cast<char *>(""); /* aclCommand reads acl_filename[0] */
        /* ACL SETROLE counts pubsub subscriptions to decide whether to scan for revoked channels. */
        server.pubsub_channels = kvstoreCreate(&kvstoreChannelHashtableType, 0, KVSTORE_ALLOCATE_HASHTABLES_ON_DEMAND);
        server.pubsub_patterns = dictCreate(&objToHashtableDictType);
        server.pubsubshard_channels = kvstoreCreate(&kvstoreChannelHashtableType, 0, KVSTORE_ALLOCATE_HASHTABLES_ON_DEMAND);

        /* One small string key the speculated GET can answer. */
        robj *key = createStringObject("k", 1);
        robj *val = createStringObject("v", 1);
        dbAdd(server.db[0], key, &val);
        decrRefCount(key);
    }

    static void TearDownTestSuite() {
        unitTestDeleteEventLoop();
    }

    /* A fake client (no connection) with "GET k" parsed, as the IO thread leaves it. */
    static client *makeGetClient(void) {
        client *c = createClient(NULL);
        c->flag.authenticated = 1;
        c->user = NULL;
        c->argc = 2;
        c->argv_len = 2;
        c->argv = static_cast<robj **>(zcalloc(sizeof(robj *) * 2));
        c->argv[0] = createStringObject("get", 3);
        c->argv[1] = createStringObject("k", 1);
        c->parsed_cmd = lookupCommandByCString("get");
        c->cmd = c->parsed_cmd;
        c->slot = -1;
        c->cmd_queue.off = 0;
        c->cmd_queue.len = 0;
        fastGetRecomputeSpecAclOk(c);
        return c;
    }

    static void freeFakeClient(client *c) {
        for (int i = 0; i < c->argc; i++) decrRefCount(c->argv[i]);
        zfree(c->argv);
        c->argv = NULL;
        c->argc = 0;
        freeClient(c);
    }
};

/* Sanity: with nothing in the way the GET is answered into c->buf. */
TEST_F(FastGetGatesTest, SpeculatesWhenNoGateIsUp) {
    client *c = makeGetClient();
    EXPECT_EQ(fastGetSpeculateBatch(c, 1), 1);
    EXPECT_EQ(c->bufpos, 7);
    EXPECT_EQ(memcmp(c->buf, "$1\r\nv\r\n", 7), 0);
    freeFakeClient(c);
}

/* CLIENT PAUSE ALL: stock holds every command of every client until unpause
 * (processCommand -> blockPostponeClient). A GET answered on the IO thread
 * must be held too. */
TEST_F(FastGetGatesTest, ClientPauseAllHoldsSpeculatedGet) {
    client *c = makeGetClient();
    server.paused_actions = PAUSE_ACTION_CLIENT_ALL;
    fastGetRefreshMainGate();
    EXPECT_EQ(fastGetSpeculateBatch(c, 1), 0);
    EXPECT_EQ(c->bufpos, 0);
    server.paused_actions = 0;
    fastGetRefreshMainGate();
    EXPECT_EQ(fastGetSpeculateBatch(c, 1), 1) << "after unpause the GET is answered here again";
    freeFakeClient(c);
}

/* CLIENT PAUSE WRITE must not hold a GET. */
TEST_F(FastGetGatesTest, ClientPauseWriteStillSpeculatesGet) {
    client *c = makeGetClient();
    server.paused_actions = PAUSE_ACTION_CLIENT_WRITE;
    fastGetRefreshMainGate();
    EXPECT_EQ(fastGetSpeculateBatch(c, 1), 1);
    server.paused_actions = 0;
    fastGetRefreshMainGate();
    freeFakeClient(c);
}

/* A replica whose primary link is down with replica-serve-stale-data no answers
 * -MASTERDOWN in processCommand. The IO-thread GET must punt to main so the
 * same error is produced. */
TEST_F(FastGetGatesTest, StaleReplicaWithServeStaleDataNoPuntsGet) {
    client *c = makeGetClient();
    char host[] = "127.0.0.1";
    server.primary_host = host;
    server.repl_state = REPL_STATE_CONNECT;
    server.repl_serve_stale_data = 0;
    EXPECT_EQ(fastGetSpeculateBatch(c, 1), 0);
    EXPECT_EQ(c->bufpos, 0);
    /* With a connected link the replica serves reads. */
    server.repl_state = REPL_STATE_CONNECTED;
    EXPECT_EQ(fastGetSpeculateBatch(c, 1), 1);
    server.primary_host = NULL;
    server.repl_state = REPL_STATE_NONE;
    server.repl_serve_stale_data = 1;
    freeFakeClient(c);
}

/* F11, first half: main queued a write of this client's buffers (pending_write) and owns it. The
 * buffer is empty here (main's bytes may sit in the reply list, or its write already drained them
 * but the queue entry remains until beforeSleep). A GET answered on the IO thread would send c->buf
 * from both threads. The bit is tested with the MULTI / blocked / buf_encoded flags in one mask;
 * this test fails when the bit is dropped from that mask. */
TEST_F(FastGetGatesTest, PendingWriteWithEmptyBufferPuntsGet) {
    client *c = makeGetClient();
    c->flag.pending_write = 1;
    EXPECT_EQ(fastGetSpeculateBatch(c, 1), 0);
    EXPECT_EQ(c->bufpos, 0) << "the IO thread wrote a reply into a buffer main is about to send";
    c->flag.pending_write = 0;
    EXPECT_EQ(fastGetSpeculateBatch(c, 1), 1) << "with the write queue entry gone the GET is answered here again";
    freeFakeClient(c);
}

/* F11, second half: main's bytes are in c->buf but no write is queued (main wrote part of them and
 * installed a write handler for the rest, which clears pending_write). The GET must still go to
 * main, which appends its reply after those bytes. This test fails when the bufpos check is
 * dropped: the IO thread would append the reply to main's bytes and send both. */
TEST_F(FastGetGatesTest, MainBytesInBufferWithoutPendingWritePuntsGet) {
    client *c = makeGetClient();
    const char push[] = ">2\r\n$10\r\ninvalidate\r\n*1\r\n$3\r\nfoo\r\n";
    memcpy(c->buf, push, sizeof(push) - 1);
    c->bufpos = sizeof(push) - 1;
    c->flag.pending_write = 0;
    EXPECT_EQ(fastGetSpeculateBatch(c, 1), 0);
    EXPECT_EQ(c->bufpos, (size_t)(sizeof(push) - 1)) << "the IO thread appended a reply behind main's bytes";
    EXPECT_EQ(memcmp(c->buf, push, sizeof(push) - 1), 0) << "main's bytes were changed";
    c->bufpos = 0;
    EXPECT_EQ(fastGetSpeculateBatch(c, 1), 1) << "with the buffer drained the GET is answered here again";
    freeFakeClient(c);
}

/* Run "ACL <args...>" on a fake admin client. */
static void runAcl(const char **args, int argc) {
    client *a = createClient(NULL);
    a->flag.fake = 1; /* no connection: replies are dropped */
    a->argc = argc;
    a->argv_len = argc;
    a->argv = static_cast<robj **>(zcalloc(sizeof(robj *) * argc));
    for (int i = 0; i < argc; i++) a->argv[i] = createStringObject(args[i], strlen(args[i]));
    a->cmd = a->lastcmd = lookupCommandByCString("acl");
    aclCommand(a);
    for (int i = 0; i < argc; i++) decrRefCount(a->argv[i]);
    zfree(a->argv);
    a->argv = NULL;
    a->argc = 0;
    freeClient(a);
}

/* ACL SETROLE changes the effective rules of every member user. A connected
 * member whose GET right comes from the role must lose the IO-thread GET gate
 * (spec_acl_ok) at once, as ACL SETUSER / DELUSER / LOAD already do through
 * fastGetOnAclRulesChanged. */
TEST_F(FastGetGatesTest, AclSetRoleRevokeRecomputesSpecAclOk) {
    const char *mkrole[] = {"ACL", "SETROLE", "gaterole", "~*", "&*", "+get"};
    runAcl(mkrole, 6);
    const char *mkuser[] = {"ACL", "SETUSER", "gateuser", "on", "nopass", "role=gaterole"};
    runAcl(mkuser, 6);
    user *u = ACLGetUserByName("gateuser", 8);
    ASSERT_NE(u, nullptr);

    client *c = makeGetClient();
    c->user = u;
    listAddNodeTail(server.clients, c); /* fastGetOnAclRulesChanged walks server.clients */
    fastGetRecomputeSpecAclOk(c);
    /* One bit per fast GET kind; the role grants only GET, so the byte is exactly bit 0 (GET). */
    ASSERT_EQ(__atomic_load_n(reinterpret_cast<uint8_t *>(&c->spec_acl_ok), __ATOMIC_ACQUIRE), 1);
    EXPECT_EQ(fastGetSpeculateBatch(c, 1), 1);
    c->bufpos = 0;

    const char *revoke[] = {"ACL", "SETROLE", "gaterole", "-get"};
    runAcl(revoke, 4);
    EXPECT_EQ(__atomic_load_n(reinterpret_cast<uint8_t *>(&c->spec_acl_ok), __ATOMIC_ACQUIRE), 0) << "role member keeps the IO-thread GET gate after ACL SETROLE -get";
    EXPECT_EQ(fastGetSpeculateBatch(c, 1), 0);

    /* Granting it back re-enables the gate. */
    const char *grant[] = {"ACL", "SETROLE", "gaterole", "+get"};
    runAcl(grant, 4);
    EXPECT_EQ(__atomic_load_n(reinterpret_cast<uint8_t *>(&c->spec_acl_ok), __ATOMIC_ACQUIRE), 1);

    /* Losing key access through the role must also close the gate. */
    const char *keys[] = {"ACL", "SETROLE", "gaterole", "resetkeys", "~x:*"};
    runAcl(keys, 5);
    EXPECT_EQ(__atomic_load_n(reinterpret_cast<uint8_t *>(&c->spec_acl_ok), __ATOMIC_ACQUIRE), 0) << "role member keeps the gate after the role lost allkeys";

    listDelNode(server.clients, listSearchKey(server.clients, c));
    c->user = NULL;
    freeFakeClient(c);
}
