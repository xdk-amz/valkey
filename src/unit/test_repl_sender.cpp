/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* repl-stream-sender: disabling the sender must hand every owned replica back to
 * the main path. Before the fix, replStreamSenderStop freed the member list but left
 * stream_via_sender set on the replicas, so putClientInPendingWriteQueue skipped them
 * forever and the replica stopped receiving the stream (audit item REPL-01; the same
 * state makes the final flush in finishShutdown skip them, REPL-02). */

#include "generated_wrappers.hpp"

#include "fake_connection.hpp"

#include <cstring>

extern "C" {
#include "server.h"
void replStreamSenderInit(void);
void replStreamSenderStop(void);
void replStreamSenderAdd(client *replica);
void replStreamSenderAdoptSettled(client *replica);
void replStreamSenderTick(void);
int replStreamSenderActive(void);
unsigned long replStreamSenderOwnedReplicas(void);
}

class ReplSenderTest : public ::testing::Test {
  protected:
    void SetUp() override {
        server.logfile = const_cast<char *>("/dev/null");
        server.verbosity = LL_WARNING;
        server.commandlog[COMMANDLOG_TYPE_LARGE_REPLY].threshold = -1;
        if (!server.replicas) server.replicas = listCreate();
        if (!server.clients_pending_write) server.clients_pending_write = listCreate();
    }
};

static client *makeOnlineReplica(fakeConnection *conn) {
    client *c = (client *)zcalloc(sizeof(client));
    initClientReplicationData(c);
    c->reply = listCreate();
    c->conn = (connection *)conn;
    c->flag.replica = 1;
    c->repl_data->repl_state = REPLICA_STATE_ONLINE;
    return c;
}

static void freeReplica(client *c) {
    listRelease(c->reply);
    zfree(c->repl_data);
    zfree(c);
}

TEST_F(ReplSenderTest, DisableHandsOwnedReplicaBackToMain) {
    fakeConnection *conn = connCreateFake(1024);
    client *c = makeOnlineReplica(conn);

    server.repl_stream_sender = REPL_STREAM_SENDER_APPEND;
    replStreamSenderInit();
    ASSERT_TRUE(replStreamSenderActive());
    replStreamSenderAdd(c);
    ASSERT_EQ(c->repl_data->stream_via_sender, 1);
    ASSERT_EQ(replStreamSenderOwnedReplicas(), 1u);

    /* CONFIG SET repl-stream-sender no: the next tick stops the thread. */
    server.repl_stream_sender = REPL_STREAM_SENDER_NO;
    replStreamSenderTick();
    ASSERT_FALSE(replStreamSenderActive());

    /* The replica must belong to the main path again. */
    EXPECT_EQ(c->repl_data->stream_via_sender, 0);
    EXPECT_TRUE(c->repl_data->send_node == NULL);

    /* And main must be able to queue a write for it. */
    putClientInPendingWriteQueue(c);
    EXPECT_TRUE(c->flag.pending_write);
    if (c->flag.pending_write) {
        listUnlinkNode(server.clients_pending_write, &c->clients_pending_write_node);
        c->flag.pending_write = 0;
    }

    connFreeFake(conn);
    freeReplica(c);
}

/* Re-enable must adopt the ONLINE replicas by itself. replicaPutOnline runs once per
 * connection, so production never calls replStreamSenderAdd again for a replica that
 * was handed back: replStreamSenderInit has to walk server.replicas. */
TEST_F(ReplSenderTest, ReenableAfterDisableOwnsReplicaAgain) {
    fakeConnection *conn = connCreateFake(1024);
    client *c = makeOnlineReplica(conn);
    listAddNodeTail(server.replicas, c);

    server.repl_stream_sender = REPL_STREAM_SENDER_APPEND;
    replStreamSenderInit();
    replStreamSenderAdd(c);
    ASSERT_EQ(replStreamSenderOwnedReplicas(), 1u);
    server.repl_stream_sender = REPL_STREAM_SENDER_NO;
    replStreamSenderTick();
    ASSERT_EQ(c->repl_data->stream_via_sender, 0);
    /* Main queued a write for the handed-back replica; the adopt must take it out. */
    EXPECT_TRUE(c->flag.pending_write);

    /* Re-enable through the config path only: no manual Add. */
    server.repl_stream_sender = REPL_STREAM_SENDER_APPEND;
    replStreamSenderTick();
    EXPECT_EQ(replStreamSenderOwnedReplicas(), 1u);
    EXPECT_EQ(c->repl_data->stream_via_sender, 1);
    EXPECT_FALSE(c->flag.pending_write);
    EXPECT_EQ(listLength(server.clients_pending_write), 0u);

    server.repl_stream_sender = REPL_STREAM_SENDER_NO;
    replStreamSenderStop();
    if (c->flag.pending_write) {
        listUnlinkNode(server.clients_pending_write, &c->clients_pending_write_node);
        c->flag.pending_write = 0;
    }
    listDelNode(server.replicas, listSearchKey(server.replicas, c));
    connFreeFake(conn);
    freeReplica(c);
}

/* A replica whose main-path write is still offloaded to an IO thread when the sender
 * starts must not be taken while the IO thread owns the cursor: it is marked and
 * adopted by replStreamSenderAdoptSettled once the write has completed. */
TEST_F(ReplSenderTest, ReenableDefersAReplicaWithAWriteInFlight) {
    fakeConnection *conn = connCreateFake(1024);
    client *c = makeOnlineReplica(conn);
    listAddNodeTail(server.replicas, c);
    c->io_write_state = CLIENT_PENDING_IO;

    server.repl_stream_sender = REPL_STREAM_SENDER_APPEND;
    replStreamSenderInit();
    EXPECT_EQ(replStreamSenderOwnedReplicas(), 0u);
    EXPECT_EQ(c->repl_data->stream_via_sender, 0);
    EXPECT_EQ(c->repl_data->sender_adopt_pending, 1);

    /* Still in flight: the settle hook must not take it. */
    replStreamSenderAdoptSettled(c);
    EXPECT_EQ(replStreamSenderOwnedReplicas(), 0u);
    EXPECT_EQ(c->repl_data->sender_adopt_pending, 0);

    /* The write completed: processClientIOWriteDone sets IDLE, then calls the hook. */
    c->repl_data->sender_adopt_pending = 1;
    c->io_write_state = CLIENT_IDLE;
    replStreamSenderAdoptSettled(c);
    EXPECT_EQ(replStreamSenderOwnedReplicas(), 1u);
    EXPECT_EQ(c->repl_data->stream_via_sender, 1);
    EXPECT_EQ(c->repl_data->sender_adopt_pending, 0);

    server.repl_stream_sender = REPL_STREAM_SENDER_NO;
    replStreamSenderStop();
    if (c->flag.pending_write) {
        listUnlinkNode(server.clients_pending_write, &c->clients_pending_write_node);
        c->flag.pending_write = 0;
    }
    listDelNode(server.replicas, listSearchKey(server.replicas, c));
    connFreeFake(conn);
    freeReplica(c);
}
