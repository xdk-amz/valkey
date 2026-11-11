/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* finishShutdown accounts the replica write an IO thread has in flight, so the final flush
 * includes the bytes behind it (audit finding N3). While it does so it sets
 * server.shutdown_no_write_offload, so that accounting a completed write does not hand the
 * rest of the replica's backlog to an IO thread again: without the flag the drain loop in
 * finishShutdown repeats one IO write at a time until the whole backlog is on the wire, and
 * SHUTDOWN blocks for as long as a slow replica takes.
 *
 * The integration test tests/integration/wave2-shutdown-bound.tcl drives the same path end
 * to end, but whether a write is in flight at the shutdown instant depends on how much of
 * the backlog the kernel send buffer absorbed, so it cannot catch the mutant every run.
 * This suite drives the two decision points directly, with a fake connection that accepts a
 * fixed number of bytes per write and no IO thread, so every run takes the same path:
 *
 *   trySendWriteToIOThreads  must refuse while the flag is set;
 *   processClientIOWriteDone must account the completed write and leave the replica in
 *                            clients_pending_write, so the loop ends after one completion.
 *
 * The only completion that re-offloads is a partial write of a single replication block with
 * no WRITE_ERROR (a multi-block writev that stops short sets WRITE_ERROR and installs the
 * write handler instead), so the backlog here is one large block and the fake connection
 * takes a slice of it per write, like a replica whose socket buffer is nearly full. */

#include "generated_wrappers.hpp"

#include "fake_connection.hpp"

#include <cstring>
#include <vector>

extern "C" {
#include "io_threads.h"
#include "server.h"
#include "staged_loop.h"
void testOnlyWriteToReplica(client *c);
}

namespace {

/* One replication block of this many bytes is the replica's whole backlog. */
const size_t kBacklogBytes = 64 * 1024;
/* The fake replica accepts this many bytes per write: a slow, nearly full socket. */
const size_t kSliceBytes = 1024;
/* More completions than the mutant can need to drain the block; stops a broken loop. */
const int kLoopLimit = 2 * (int)(kBacklogBytes / kSliceBytes) + 8;

} // namespace

class ShutdownBoundTest : public ::testing::Test {
  protected:
    /* Every server field the test reads or writes is saved here and restored in TearDown,
     * so the suite leaves no state behind for the other suites in the binary. */
    list *saved_replicas;
    list *saved_pending_write;
    list *saved_repl_buffer_blocks;
    replBacklog *saved_repl_backlog;
    int saved_active_io_threads_num;
    int saved_io_threads_num;
    int saved_no_offload;
    long long saved_stat_io_writes_pending;
    unsigned long long saved_maxmemory_clients;
    long long saved_repl_backlog_size;
    int saved_processing_events_while_blocked;
    char *saved_logfile;
    int saved_verbosity;
    long long saved_large_reply_threshold;

    fakeConnection *conn;
    client *replica;
    replBufBlock *block;
    /* Everything the fake replica received, in order, across all writes. */
    std::vector<char> received;

    void SetUp() override {
        saved_logfile = server.logfile;
        saved_verbosity = server.verbosity;
        saved_large_reply_threshold = server.commandlog[COMMANDLOG_TYPE_LARGE_REPLY].threshold;
        /* A failed serverAssert() logs before it aborts; with no logfile that crashes instead. */
        if (server.logfile == NULL) server.logfile = const_cast<char *>("/dev/null");
        server.verbosity = LL_WARNING;
        server.commandlog[COMMANDLOG_TYPE_LARGE_REPLY].threshold = -1;

        saved_replicas = server.replicas;
        saved_pending_write = server.clients_pending_write;
        saved_repl_buffer_blocks = server.repl_buffer_blocks;
        saved_repl_backlog = server.repl_backlog;
        saved_active_io_threads_num = server.active_io_threads_num;
        saved_io_threads_num = server.io_threads_num;
        saved_no_offload = server.shutdown_no_write_offload;
        saved_stat_io_writes_pending = server.stat_io_writes_pending;
        saved_maxmemory_clients = server.maxmemory_clients;
        saved_repl_backlog_size = server.repl_backlog_size;
        saved_processing_events_while_blocked = ProcessingEventsWhileBlocked;

        server.replicas = listCreate();
        server.clients_pending_write = listCreate();
        server.repl_buffer_blocks = listCreate();
        server.repl_backlog = NULL;
        createReplicationBacklog();
        server.repl_backlog_size = (long long)kBacklogBytes * 4; /* no trimming during the test */
        server.maxmemory_clients = 0;                            /* no client eviction buckets */
        server.stat_io_writes_pending = 0;
        server.shutdown_no_write_offload = 0;
        /* An IO pool with one worker: offload is possible unless something refuses it. */
        server.io_threads_num = 2;
        server.active_io_threads_num = 2;
        /* The staged loop and a blocked main are the other two reasons trySendWriteToIOThreads
         * refuses; neither is part of this test. */
        ProcessingEventsWhileBlocked = 0;
        testOnlyStagedSetOn(0);
        testOnlySetCurTid(0);

        /* The backlog: one block, fully used, byte i holds a pattern so a received prefix
         * can be checked for lost or duplicated bytes. */
        block = (replBufBlock *)zmalloc(sizeof(replBufBlock) + kBacklogBytes);
        block->size = kBacklogBytes;
        block->used = kBacklogBytes;
        block->repl_offset = 1;
        for (size_t i = 0; i < kBacklogBytes; i++) block->buf[i] = (char)('a' + (i * 7 + i / 251) % 26);
        listAddNodeTail(server.repl_buffer_blocks, block);
        /* The backlog itself holds the first block (createReplicationBacklog has none yet). */
        block->refcount = 1;
        server.repl_backlog->ref_repl_buf_node = listFirst(server.repl_buffer_blocks);

        /* An ONLINE replica at the start of that block, on a fake connection. */
        conn = connCreateFake(kSliceBytes);
        conn->conn.state = CONN_STATE_CONNECTED;
        replica = (client *)zcalloc(sizeof(client));
        replica->id = 1;
        replica->conn = (connection *)conn;
        replica->reply = listCreate();
        replica->flag.replica = 1;
        initClientReplicationData(replica);
        replica->repl_data->repl_state = REPLICA_STATE_ONLINE;
        replica->repl_data->ref_repl_buf_node = listFirst(server.repl_buffer_blocks);
        replica->repl_data->ref_block_pos = 0;
        block->refcount++;
        listAddNodeTail(server.replicas, replica);
        received.clear();
    }

    void TearDown() override {
        /* Anything still staged for an IO thread is unwound to the pending write queue once
         * the pool is gone; then the queue entry is dropped. */
        server.active_io_threads_num = 1;
        flushWriteSlab();
        testOnlySetCurTid(0);
        if (replica->flag.pending_write) {
            listUnlinkNode(server.clients_pending_write, &replica->clients_pending_write_node);
            replica->flag.pending_write = 0;
        }
        replica->io_write_state = CLIENT_IDLE;
        replica->io_read_state = CLIENT_IDLE;
        /* Not freeClientReplicationData: it fires a module server event, and this suite does
         * not bring up the module system. The block is freed below in any case. */
        listDelNode(server.replicas, listSearchKey(server.replicas, replica));
        zfree(replica->repl_data);
        listRelease(replica->reply);
        zfree(replica);
        connFreeFake(conn);

        listRelease(server.repl_buffer_blocks);
        zfree(block);
        raxFree(server.repl_backlog->blocks_index);
        zfree(server.repl_backlog);
        listRelease(server.replicas);
        listRelease(server.clients_pending_write);

        server.repl_backlog = saved_repl_backlog;
        server.repl_buffer_blocks = saved_repl_buffer_blocks;
        server.clients_pending_write = saved_pending_write;
        server.replicas = saved_replicas;
        server.active_io_threads_num = saved_active_io_threads_num;
        server.io_threads_num = saved_io_threads_num;
        server.shutdown_no_write_offload = saved_no_offload;
        server.stat_io_writes_pending = saved_stat_io_writes_pending;
        server.maxmemory_clients = saved_maxmemory_clients;
        server.repl_backlog_size = saved_repl_backlog_size;
        ProcessingEventsWhileBlocked = saved_processing_events_while_blocked;
        server.logfile = saved_logfile;
        server.verbosity = saved_verbosity;
        server.commandlog[COMMANDLOG_TYPE_LARGE_REPLY].threshold = saved_large_reply_threshold;
    }

    /* Bytes of the block the replica still has to receive. */
    size_t pendingBytes() const { return kBacklogBytes - replica->repl_data->ref_block_pos; }

    /* Collect what the fake connection took in the last write and make room for the next. */
    void drainFakeConnection() {
        received.insert(received.end(), conn->buffer, conn->buffer + conn->written);
        conn->written = 0;
    }

    /* Play the IO thread for a write that trySendWriteToIOThreads handed over: write from the
     * snapshot it took, on a non-main thread id, and publish the completion. The fake
     * connection takes at most kSliceBytes, so the write is partial and, being a single
     * block, carries no WRITE_ERROR: the completion is one that may re-offload. */
    void ioThreadCompletesWrite() {
        ASSERT_EQ(replica->io_write_state, CLIENT_PENDING_IO);
        ASSERT_TRUE(replica->write_flags & WRITE_FLAGS_IS_REPLICA);
        replica->nwritten = 0;
        testOnlySetCurTid(1);
        testOnlyWriteToReplica(replica);
        testOnlySetCurTid(0);
        ASSERT_EQ(replica->write_flags & WRITE_FLAGS_WRITE_ERROR, 0);
        ASSERT_EQ(replica->nwritten, (ssize_t)kSliceBytes);
        drainFakeConnection();
        __atomic_thread_fence(__ATOMIC_RELEASE);
        replica->io_write_state = CLIENT_COMPLETED_IO;
    }

    /* The state finishShutdown finds: an IO thread finished a partial write of the block
     * before the shutdown started, and main has not accounted it yet. */
    void putOneWriteInFlightAndComplete() {
        ASSERT_EQ(server.shutdown_no_write_offload, 0);
        ASSERT_EQ(trySendWriteToIOThreads(replica), C_OK);
        ASSERT_EQ(server.stat_io_writes_pending, 1);
        ioThreadCompletesWrite();
    }

    /* The drain loop of finishShutdown for one replica, with the IO thread played by
     * ioThreadCompletesWrite whenever a completion re-offloaded the rest of the block.
     * Returns the number of completions main accounted. */
    int runFinishShutdownDrainLoop() {
        int completions = 0;
        while (replica->io_write_state != CLIENT_IDLE && completions < kLoopLimit) {
            if (replica->io_write_state == CLIENT_PENDING_IO) {
                ioThreadCompletesWrite();
                if (HasFatalFailure()) return completions;
            }
            if (replica->io_write_state == CLIENT_COMPLETED_IO) {
                __atomic_thread_fence(__ATOMIC_ACQUIRE);
                processClientIOWriteDone(replica);
                completions++;
            }
        }
        return completions;
    }
};

/* The first decision point: with the flag set, an ONLINE replica with a pending backlog and
 * an active IO pool is not handed to an IO thread. */
TEST_F(ShutdownBoundTest, FlagRefusesReplicaWriteOffload) {
    ASSERT_TRUE(clientHasPendingReplies(replica));
    server.shutdown_no_write_offload = 1;

    EXPECT_EQ(trySendWriteToIOThreads(replica), C_ERR);
    EXPECT_EQ(replica->io_write_state, CLIENT_IDLE);
    EXPECT_EQ(server.stat_io_writes_pending, 0);
    EXPECT_EQ(replica->io_last_reply_block, (listNode *)NULL);
}

/* The same replica is offloaded as soon as the flag is clear: the refusal above comes from
 * the flag and from nothing else in the setup. */
TEST_F(ShutdownBoundTest, WithoutFlagTheSameReplicaIsOffloaded) {
    ASSERT_TRUE(clientHasPendingReplies(replica));
    server.shutdown_no_write_offload = 0;

    EXPECT_EQ(trySendWriteToIOThreads(replica), C_OK);
    EXPECT_EQ(replica->io_write_state, CLIENT_PENDING_IO);
    EXPECT_EQ(server.stat_io_writes_pending, 1);
    EXPECT_EQ(replica->io_last_reply_block, listLast(server.repl_buffer_blocks));
    EXPECT_EQ(replica->io_last_bufpos, kBacklogBytes);
}

/* The second decision point: accounting the completed write while the flag is set leaves
 * the replica in clients_pending_write with the rest of the block still pending, instead of
 * handing that rest to an IO thread. */
TEST_F(ShutdownBoundTest, CompletionUnderFlagIsNotReoffloaded) {
    putOneWriteInFlightAndComplete();
    ASSERT_EQ(pendingBytes(), kBacklogBytes); /* the cursor moves only when main accounts */

    server.shutdown_no_write_offload = 1;
    __atomic_thread_fence(__ATOMIC_ACQUIRE);
    processClientIOWriteDone(replica);

    EXPECT_EQ(replica->io_write_state, CLIENT_IDLE);
    EXPECT_EQ(pendingBytes(), kBacklogBytes - kSliceBytes);
    EXPECT_TRUE(clientHasPendingReplies(replica));
    EXPECT_TRUE(replica->flag.pending_write);
    EXPECT_EQ(listLength(server.clients_pending_write), 1u);
    /* No new write job: the count stays at the one job the IO thread already finished. */
    EXPECT_EQ(server.stat_io_writes_pending, 1);
    EXPECT_FALSE(connHasWriteHandler(replica->conn));
}

/* The loop finishShutdown runs over server.replicas ends after exactly one completion when
 * the flag is set. Without the flag (the mutant) each completion re-offloads the rest of the
 * block and the loop only ends once the fake replica has taken the whole block, one slice
 * per IO write: kBacklogBytes / kSliceBytes completions, every run. */
TEST_F(ShutdownBoundTest, DrainLoopEndsAfterOneCompletion) {
    putOneWriteInFlightAndComplete();

    server.shutdown_no_write_offload = 1;
    int completions = runFinishShutdownDrainLoop();

    EXPECT_EQ(completions, 1);
    EXPECT_EQ(replica->io_write_state, CLIENT_IDLE);
    EXPECT_EQ(received.size(), kSliceBytes);
    EXPECT_EQ(pendingBytes(), kBacklogBytes - kSliceBytes);
    EXPECT_TRUE(replica->flag.pending_write);
    EXPECT_EQ(server.stat_io_writes_pending, 1);
}

/* After the drain, the one best-effort flushReplicasOutputBuffers writes to the replica on
 * the main thread: one more slice goes out, still under the flag, and the bytes the replica
 * received are an exact prefix of the backlog. The flag never costs an acknowledged byte;
 * it only stops the loop from waiting for the rest. */
TEST_F(ShutdownBoundTest, FinalFlushSendsOnceAndKeepsOrder) {
    putOneWriteInFlightAndComplete();

    server.shutdown_no_write_offload = 1;
    ASSERT_EQ(runFinishShutdownDrainLoop(), 1);
    ASSERT_TRUE(replica->flag.pending_write);

    flushReplicasOutputBuffers();
    drainFakeConnection();

    EXPECT_EQ(replica->io_write_state, CLIENT_IDLE);
    EXPECT_EQ(server.stat_io_writes_pending, 1);
    EXPECT_EQ(received.size(), 2 * kSliceBytes);
    EXPECT_EQ(pendingBytes(), kBacklogBytes - 2 * kSliceBytes);
    ASSERT_LE(received.size(), kBacklogBytes);
    EXPECT_EQ(memcmp(received.data(), block->buf, received.size()), 0);
}

/* The mutant's shape, written out so a reader can see what the flag prevents: with the flag
 * clear, the same loop re-offloads after every completion until the block is drained. This
 * is the behaviour the fix removes from finishShutdown; it documents the bound, it does not
 * test the fix. */
TEST_F(ShutdownBoundTest, WithoutFlagTheLoopDrainsTheWholeBlock) {
    putOneWriteInFlightAndComplete();

    server.shutdown_no_write_offload = 0;
    int completions = runFinishShutdownDrainLoop();

    EXPECT_EQ(completions, (int)(kBacklogBytes / kSliceBytes));
    EXPECT_EQ(replica->io_write_state, CLIENT_IDLE);
    EXPECT_EQ(received.size(), kBacklogBytes);
    EXPECT_EQ(pendingBytes(), 0u);
    EXPECT_FALSE(clientHasPendingReplies(replica));
    EXPECT_EQ(memcmp(received.data(), block->buf, kBacklogBytes), 0);
}
