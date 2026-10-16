/* Replication stream sender.
 *
 * A dedicated thread that streams the replication backlog to ONLINE plaintext
 * replicas, off the staged event loop, behind the hidden config
 * `repl-stream-sender` (default off). See docs and
 * ../parallel-replies-notes/repl-stream-design.md.
 *
 * Division of labour (the whole safety argument): the sender SENDS, main
 * ACCOUNTS. The sender only reads block bytes in [send_pos .. stable_offset]
 * and release-publishes each replica's sent_offset; it never writes a refcount,
 * never advances main's cursor (ref_repl_buf_node/ref_block_pos), never frees a
 * block or a client, and never touches server.replicas. Main, in cron, reads
 * sent_offset and does all cursor/refcount/trim/COB/disconnect work exactly as
 * before. A single mutex serialises the sender's per-replica send against main's
 * add/remove/account for that replica, so a replica the sender is sending is
 * always still in the set and refcount-pinned.
 *
 * This is a prototype: plaintext, ONLINE, non-RDB-channel, uncompressed replicas
 * only. Everything else (TLS, compression, pre-ONLINE, dual-channel) stays on the
 * existing main/ae path. */

#include "server.h"
#include "fastpath_no_epoll.h"

#ifdef HAVE_FASTPATH_EPOLL

#include <pthread.h>

/* The streamed replica set and the sender thread state. */
static pthread_t repl_sender_thread;
static pthread_mutex_t repl_sender_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t repl_sender_cond = PTHREAD_COND_INITIALIZER;
static list *repl_sender_replicas = NULL; /* client* of streamed replicas */
static int repl_sender_running = 0;
static int repl_sender_stop = 0;
static _Atomic(long long) repl_sender_wake_offset; /* stable offset last published */

/* True if this replica can be streamed by the sender thread (prototype subset). */
int replStreamSenderEligible(client *replica) {
    if (!server.repl_stream_sender) return 0;
    if (!replica->conn) return 0;
    if (replica->repl_data->stream_ineligible) return 0; /* diskless/socket transfer: main path */
    if (getClientType(replica) != CLIENT_TYPE_REPLICA) return 0;
    if (replica->repl_data->repl_state != REPLICA_STATE_ONLINE) return 0;
    if (replica->repl_data->repl_compression != NULL) return 0; /* compressed: main path */
    if (replica->flag.repl_rdb_channel || replica->flag.protected_rdb_channel) return 0;
    /* TLS replicas keep the main path: connWrite may need main-thread TLS state. */
    if (connGetType(replica->conn) == CONN_TYPE_TLS) return 0;
    return 1;
}

/* Send as much of [send_node:send_pos .. stable] as the socket accepts for one
 * replica. Runs on the sender thread; reads only pinned, below-stable bytes.
 * Returns C_ERR on write error (main will disconnect), C_OK otherwise. */
static int replStreamSendOne(client *replica, long long stable) {
    ClientReplicationData *rd = replica->repl_data;

    /* The sender uses only its own send_node/send_pos, which main seeds (and re-seeds
     * while still NULL) under repl_sender_mutex in the account pass. The sender never
     * reads main-owned ref_repl_buf_node/ref_block_pos, so there is no race on them. */
    if (rd->send_node == NULL) return C_OK; /* nothing attached yet */

    long long sent = atomic_load_explicit(&rd->sent_offset, memory_order_relaxed);
    if (stable <= sent) return C_OK; /* nothing new to send */

    /* Build an iovec of [send_node:send_pos .. offset `stable`]. */
    struct iovec iov_arr[IOV_MAX];
    int iovcnt = 0;
    long long span_end = stable;
    listNode *ln = rd->send_node;
    size_t pos = rd->send_pos;
    for (; ln != NULL && iovcnt < IOV_MAX; ln = listNextNode(ln)) {
        replBufBlock *b = listNodeValue(ln);
        /* b->repl_offset is 1-based (the primary_repl_offset of this block's first byte),
         * while stable/sent are 0-based byte counts. Byte index i in b->buf is at 0-based
         * absolute offset (b->repl_offset - 1) + i. */
        long long block_start = b->repl_offset - 1;
        size_t start = (ln == rd->send_node) ? pos : 0;
        /* End within this block: either the block's used, or where stable falls. */
        long long block_end_off = block_start + (long long)b->used;
        size_t end = b->used;
        if (span_end < block_end_off) end = (size_t)(span_end - block_start);
        if (end <= start) {
            if (span_end <= block_end_off) break;
            continue;
        }
        iov_arr[iovcnt].iov_base = b->buf + start;
        iov_arr[iovcnt].iov_len = end - start;
        iovcnt++;
        if (span_end <= block_end_off) break;
    }
    if (iovcnt == 0) return C_OK;

    ssize_t nwritten = connWritev(replica->conn, iov_arr, iovcnt);
    if (nwritten <= 0) {
        /* EAGAIN is not an error; retry on the next wake. */
        if (nwritten < 0 && connGetState(replica->conn) == CONN_STATE_CONNECTED) return C_OK;
        if (nwritten == 0) return C_OK;
        return C_ERR;
    }

    /* Advance the sender position by nwritten and publish sent_offset. */
    size_t remaining = (size_t)nwritten;
    ln = rd->send_node;
    pos = rd->send_pos;
    while (remaining > 0 && ln != NULL) {
        replBufBlock *b = listNodeValue(ln);
        size_t avail = b->used - pos;
        if (remaining < avail) {
            pos += remaining;
            remaining = 0;
            break;
        }
        remaining -= avail;
        listNode *next = listNextNode(ln);
        if (next == NULL) {
            pos = b->used;
            break;
        }
        ln = next;
        pos = 0;
    }
    rd->send_node = ln;
    rd->send_pos = pos;
    long long new_sent = sent + nwritten;
    atomic_store_explicit(&rd->sent_offset, new_sent, memory_order_release);
    return C_OK;
}

static void *replStreamSenderMain(void *arg) {
    UNUSED(arg);
    valkey_set_thread_title("repl_sender");
    pthread_mutex_lock(&repl_sender_mutex);
    while (!repl_sender_stop) {
        long long stable = atomic_load_explicit(&server.repl_stable_offset, memory_order_acquire);
        listIter li;
        listNode *ln;
        listRewind(repl_sender_replicas, &li);
        while ((ln = listNext(&li))) {
            client *replica = listNodeValue(ln);
            if (replStreamSendOne(replica, stable) == C_ERR)
                atomic_store_explicit(&replica->repl_data->send_error, 1, memory_order_release);
        }
        /* Wait for main to publish a newer stable offset, or a stop. */
        long long wake = atomic_load_explicit(&repl_sender_wake_offset, memory_order_acquire);
        if (!repl_sender_stop && wake <= stable) {
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_nsec += 1000000; /* 1ms backstop so partial writes retry */
            if (ts.tv_nsec >= 1000000000) { ts.tv_sec++; ts.tv_nsec -= 1000000000; }
            pthread_cond_timedwait(&repl_sender_cond, &repl_sender_mutex, &ts);
        }
    }
    pthread_mutex_unlock(&repl_sender_mutex);
    return NULL;
}

/* Main: publish a newer stable offset and wake the sender. Called from
 * replStreamSenderTick (beforeSleep and the staged loop iteration). */
void replStreamSenderPublishStable(long long offset) {
    if (!repl_sender_running) return;
    atomic_store_explicit(&server.repl_stable_offset, offset, memory_order_release);
    atomic_store_explicit(&repl_sender_wake_offset, offset, memory_order_release);
    pthread_cond_signal(&repl_sender_cond);
}

/* Main: reclaim a replica from the sender (state change, close, or disable).
 * After this returns the sender holds no reference to the client. */
void replStreamSenderRemove(client *replica) {
    if (!repl_sender_running) return;
    pthread_mutex_lock(&repl_sender_mutex);
    if (replica->repl_data->stream_via_sender) {
        listNode *ln = listSearchKey(repl_sender_replicas, replica);
        if (ln) listDelNode(repl_sender_replicas, ln);
        replica->repl_data->stream_via_sender = 0;
        replica->repl_data->send_node = NULL;
        replica->repl_data->send_pos = 0;
    }
    pthread_mutex_unlock(&repl_sender_mutex);
}

/* Main: hand an ONLINE plaintext replica to the sender, synchronously at the state
 * transition (replicaPutOnline), BEFORE any incremental data is produced for it. This
 * is the only place a replica joins the sender set: registering here (not lazily in the
 * account pass) guarantees there is never a window in which the main/ae path and the
 * sender both own the same replica's send, which would double-send or skip bytes and
 * diverge the replica. If the sender is off or the replica is not eligible this is a
 * no-op and the replica stays on the main path. */
void replStreamSenderAdd(client *replica) {
    if (!repl_sender_running) return;
    pthread_mutex_lock(&repl_sender_mutex);
    ClientReplicationData *rd = replica->repl_data;
    if (!rd->stream_via_sender && replStreamSenderEligible(replica)) {
        /* Seed the sender from the replica's current cursor so it resumes exactly where
         * the main path left off: no gap and no double-send. The cursor's absolute
         * offset is the next byte to send. ref_repl_buf_node may be NULL here (no
         * incremental data yet); in that case send_node stays NULL and main seeds it
         * from the cursor in the account pass once feedReplicationBuffer attaches one. */
        long long cursor_off = server.primary_repl_offset;
        if (rd->ref_repl_buf_node) {
            replBufBlock *b = listNodeValue(rd->ref_repl_buf_node);
            /* repl_offset is 1-based; cursor_off is a 0-based byte count. */
            cursor_off = (b->repl_offset - 1) + (long long)rd->ref_block_pos;
        }
        rd->stream_via_sender = 1;
        rd->send_node = rd->ref_repl_buf_node;
        rd->send_pos = rd->ref_block_pos;
        atomic_store_explicit(&rd->sent_offset, cursor_off, memory_order_relaxed);
        atomic_store_explicit(&rd->send_error, 0, memory_order_relaxed);
        listAddNodeTail(repl_sender_replicas, replica);
    }
    pthread_mutex_unlock(&repl_sender_mutex);
}

/* Main, in cron: account for what the sender has sent. For each streamed replica
 * advance main's cursor/refcounts up to sent_offset, enforce COB, and disconnect
 * on a send error. Holds the mutex so it never races the sender for one replica. */
void replStreamSenderAccount(void) {
    if (!repl_sender_running) return;
    pthread_mutex_lock(&repl_sender_mutex);

    /* Reconcile membership: drop streamed replicas that are no longer eligible. New
     * replicas join only through replStreamSenderAdd at the ONLINE transition, never
     * here, so the main path and the sender never both own one replica. */
    listIter li;
    listNode *ln;
    listRewind(repl_sender_replicas, &li);
    while ((ln = listNext(&li))) {
        client *replica = listNodeValue(ln);
        if (!replStreamSenderEligible(replica)) {
            replica->repl_data->stream_via_sender = 0;
            replica->repl_data->send_node = NULL;
            replica->repl_data->send_pos = 0;
            listDelNode(repl_sender_replicas, ln);
        }
    }

    client *to_close[64];
    int nclose = 0;
    listRewind(repl_sender_replicas, &li);
    while ((ln = listNext(&li))) {
        client *replica = listNodeValue(ln);
        ClientReplicationData *rd = replica->repl_data;
        if (atomic_load_explicit(&rd->send_error, memory_order_acquire)) {
            if (nclose < 64) to_close[nclose++] = replica;
            continue;
        }
        long long sent = atomic_load_explicit(&rd->sent_offset, memory_order_acquire);
        /* Advance main's cursor/refcounts to `sent` using the shared helper. */
        replStreamAccountCursor(replica, sent);
        /* Seed the sender's own position from main's cursor the first time a cursor
         * exists (main owns ref_repl_buf_node; reading it here is safe because this runs
         * on main). After this the sender advances send_node itself and never reads the
         * main-owned cursor, so there is no cross-thread race on it. */
        if (rd->send_node == NULL && rd->ref_repl_buf_node) {
            rd->send_node = rd->ref_repl_buf_node;
            rd->send_pos = rd->ref_block_pos;
        }
    }
    pthread_mutex_unlock(&repl_sender_mutex);
    /* Disconnect outside the lock: freeClient path calls replStreamSenderRemove itself. */
    for (int i = 0; i < nclose; i++) {
        serverLog(LL_WARNING, "Replication sender write error on replica %s, disconnecting",
                  replicationGetReplicaName(to_close[i]));
        freeClientAsync(to_close[i]);
    }
    if (server.repl_backlog) incrementalTrimReplicationBacklog(REPL_BACKLOG_TRIM_BLOCKS_PER_CALL);
}

int replStreamSenderActive(void) {
    return repl_sender_running;
}

/* Main: one service step of the sender from the event loop. Starts/stops the thread to
 * follow the config, accounts for what the sender streamed (advance cursors, drop
 * refcounts, enforce COB/disconnect, trim), then publishes the next stable offset. Called
 * from beforeSleep AND from the staged loop iteration, which does not run beforeSleep. */
void replStreamSenderTick(void) {
    if (server.repl_stream_sender && !replStreamSenderActive())
        replStreamSenderInit();
    else if (!server.repl_stream_sender && replStreamSenderActive())
        replStreamSenderStop();
    if (!replStreamSenderActive()) return;
    replStreamSenderAccount();
    long long stable = server.primary_repl_offset;
    /* Under appendfsync always the bound is the fsynced offset, so no byte is streamed
     * (and no WAITAOF/WAIT waiter can observe it) before it is on disk. */
    if (server.aof_state == AOF_ON && server.aof_fsync == AOF_FSYNC_ALWAYS &&
        server.fsynced_reploff != -1 && server.fsynced_reploff < stable)
        stable = server.fsynced_reploff;
    replStreamSenderPublishStable(stable);
}

/* Main: start the sender thread (idempotent). */
void replStreamSenderInit(void) {
    if (repl_sender_running) return;
    repl_sender_replicas = listCreate();
    repl_sender_stop = 0;
    if (pthread_create(&repl_sender_thread, NULL, replStreamSenderMain, NULL) != 0) {
        serverLog(LL_WARNING, "repl-stream-sender: cannot start the sender thread; staying on the main path");
        listRelease(repl_sender_replicas);
        repl_sender_replicas = NULL;
        return;
    }
    repl_sender_running = 1;
    serverLog(LL_NOTICE, "Replication stream sender thread started");
}

/* Main: stop and join the sender thread. */
void replStreamSenderStop(void) {
    if (!repl_sender_running) return;
    pthread_mutex_lock(&repl_sender_mutex);
    repl_sender_stop = 1;
    pthread_cond_signal(&repl_sender_cond);
    pthread_mutex_unlock(&repl_sender_mutex);
    pthread_join(repl_sender_thread, NULL);
    repl_sender_running = 0;
    if (repl_sender_replicas) {
        listRelease(repl_sender_replicas);
        repl_sender_replicas = NULL;
    }
    serverLog(LL_NOTICE, "Replication stream sender thread stopped");
}

#else /* !HAVE_FASTPATH_EPOLL */

int replStreamSenderEligible(client *replica) { UNUSED(replica); return 0; }
void replStreamSenderPublishStable(long long offset) { UNUSED(offset); }
void replStreamSenderAdd(client *replica) { UNUSED(replica); }
void replStreamSenderRemove(client *replica) { UNUSED(replica); }
void replStreamSenderAccount(void) {}
int replStreamSenderActive(void) { return 0; }
void replStreamSenderTick(void) {}
void replStreamSenderInit(void) {}
void replStreamSenderStop(void) {}

#endif /* HAVE_FASTPATH_EPOLL */
