/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "io_threads.h"
#include "ae.h"
#include "cluster.h"
#include "cluster_legacy.h"
#include "connhelpers.h"
#include "fastpath.h"

extern int ProcessingEventsWhileBlocked; /* networking.c */

/* One spin-wait step; a hint to the core on x86 and arm64, a no-op elsewhere. */
static inline void cpuRelax(void) {
#if defined(__x86_64__) || defined(__i386__)
    __builtin_ia32_pause();
#elif defined(__aarch64__)
    __asm__ __volatile__("isb" ::: "memory");
#endif
}
#include "cluster_migrateslots.h"
#include "connection.h"
#include "queues.h"
#include "server.h"
#include <sys/resource.h>
#include <sys/epoll.h>

#define IO_MPSC_QUEUE_SIZE 16384
#define IO_SPMC_QUEUE_SIZE 4096
#define IO_SPSC_QUEUE_SIZE 4096
#define IO_EPOLL_BATCH 64
/* Upper bound on outbox batches (of JOB_BATCH_SIZE) one processIOThreadsResponses
 * call handles, so the main thread returns to its event loop under load. */
#define IO_RESPONSE_BATCHES_PER_CALL 256
/* Cap on partitioned read completions handled per call, for the same reason. */
#define IO_PARTITION_COMPLETIONS_PER_CALL 4096

/* Per-IO-thread epoll set watching the sockets of the clients partitioned to
 * that thread, and a sequence number the thread bumps after every pass over
 * its events so the main thread can wait out a pass before freeing a client. */
static int io_epfd[IO_THREADS_MAX_NUM];
int ioThreadEpollFd(int tid) {
    return io_epfd[tid];
}
static _Atomic uint64_t io_epoll_seq[IO_THREADS_MAX_NUM];
static unsigned partition_rr = 0;      /* main-thread only */
static size_t partitioned_clients = 0; /* main-thread only */

/* QoS Swim Lanes for I/O threads
 * High priority queues: reserved for critical internal communication such as
 * cluster bus messages, slot migration, and replication streams
 * Normal priority queues: used for normal client connections
 */
typedef enum {
    /* Normal priority jobs - used for normal client connections */
    JOB_PRIORITY_NORMAL = 0,
    /* High priority jobs - used for critical internal communication such as
     * cluster bus messages, slot migration, and replication streams */
    JOB_PRIORITY_HIGH,
    /* Number of priority levels */
    JOB_PRIORITY_COUNT
} jobPriority;

static _Thread_local int thread_id = 0;
static _Thread_local mpscTicket io_thread_ticket[JOB_PRIORITY_COUNT] = {0};
/* Backlog of responses when io_shared_outbox is full. Should be rare. */
static _Thread_local list *pending_io_responses[JOB_PRIORITY_COUNT] = {NULL, NULL};
static pthread_t io_threads[IO_THREADS_MAX_NUM] = {0};
static pthread_mutex_t io_threads_mutex[IO_THREADS_MAX_NUM];
static int cur_epoll_thread = 0;
// Main -> IO: Shared Queue (Single Producer Multi Consumer) where all IO threads pull jobs from
static spmcQueue io_shared_inbox[JOB_PRIORITY_COUNT] = {0};
// IO -> Main: Response Channel (Multi Producer Single Consumer) used by IO threads to send results back to main-thread
static mpscQueue io_shared_outbox[JOB_PRIORITY_COUNT] = {0};
// Main -> IO (Thread-Specific) for tasks that must run on specific IO thread where IO threads check their private inbox before the shared queue
static spscQueue io_private_inbox[IO_THREADS_MAX_NUM] = {0};
/* Command ring: one per IO thread (producer), drained by the main thread
 * (consumer). An IO thread that has read and parsed a partitioned client's
 * input appends one RING_CMD entry per complete command and one RING_END
 * entry for the read itself. Main executes commands straight from the ring in
 * fixed-size batches and runs the per-client epilogue only on RING_END. */
static spscQueue io_cmd_ring[IO_THREADS_MAX_NUM] = {0};
/* Entries per IO thread (8 bytes each). The fast path carries the bulk of the
 * traffic now; a full ring falls back to the per-client completion path. */
#define IO_CMD_RING_SIZE 16384
#define RING_CMD ((uintptr_t)1)
#define RING_END ((uintptr_t)2)
#define RING_TAGS ((uintptr_t)3)
#define RING_BATCH 64
#define JOB_BATCH_SIZE (16)
static void handleReadJobs(client **read_jobs, int read_count);
static size_t io_jobs_submitted;
static _Atomic(size_t) io_jobs_finished;
static size_t cluster_io_pending_responses;
static int io_threads_initialized = 0;
/* Set during process exit to stop worker response retries. */
static _Atomic int io_threads_exiting = 0;
_Atomic long long used_active_time_io_thread[IO_THREADS_MAX_NUM] = {0};

/* Job Types for Tagged Pointers
 * We use the lower 3 bits of the pointer to store the job type.
 * Requires data pointers to be 8-byte aligned (standard for zmalloc/ptrs). */
#define JOB_TAG_MASK 0x7
#define JOB_PTR_MASK (~(uintptr_t)JOB_TAG_MASK)

static inline jobPriority getJobPriority(const client *c) {
    return (c && connIsPriority(c->conn)) ? JOB_PRIORITY_HIGH : JOB_PRIORITY_NORMAL;
}

static inline void *tagJob(void *ptr, int type) {
    return (void *)((uintptr_t)ptr | type);
}

static inline void untagJob(void *tagged_ptr, void **ptr, int *type) {
    *type = (int)((uintptr_t)tagged_ptr & JOB_TAG_MASK);
    *ptr = (void *)((uintptr_t)tagged_ptr & JOB_PTR_MASK);
}

/* Handler prototypes */
void ioThreadReadQueryFromClient(client *c);
void ioThreadWriteToClient(client *c);
int ioThreadWriteClientNoSignal(client *c, int publish);
void ioThreadFreeArgv(robj **argv);
void ioThreadPoll(aeEventLoop *el);
static void ioThreadAccept(client *c);

int inMainThread(void) {
    return thread_id == 0;
}

/* ===================== W5b: never-free-on-main ==========================
 * Charter directive: the main thread must never perform a terminal free of a
 * customer-data object or a hot-path heap buffer. All such frees are routed to
 * an IO thread (JOB_REQ_FREE_OBJ / JOB_REQ_FREE_PTR) or, for shapes IO threads
 * cannot run (module VM_Free, streams), to the bio lazyfree threads.
 *
 * Enqueue-failure discipline: if the SPMC inbox is momentarily full we must NOT
 * free inline. The object/pointer is parked on a main-thread-private pending
 * list and re-attempted from beforeSleep (drainPendingMainFrees). This list
 * only grows under SUSTAINED queue-full pressure; under normal load it drains
 * to empty every event-loop iteration. Everything parked here is either a
 * sole-reference (refcount == 1) FREE_OBJ-admitted robj or a raw pointer, so
 * re-enqueue is always valid and never requires a synchronous free.
 *
 * Debug enforcement (behind DEBUG_NEVER_FREE_ON_MAIN, sibling of T6's
 * DEBUG_REFCOUNT_DISCIPLINE): assertNoMainThreadFree() fires if the main thread
 * reaches a terminal free at a converted routing site outside the pending-list
 * enqueue. Release builds compile it to a no-op. */

/* ---- Slab free hand-off -------------------------------------------------
 * Terminal frees are NOT enqueued per object: main appends the pointer to the
 * current slab and hands the whole slab to the IO threads as ONE
 * JOB_SPSC_FREE_SLAB job per event-loop drain (or sooner, when a slab fills).
 * This amortizes the contended SPMC enqueue from one-per-object (several per
 * command under a mixed workload) to one-per-batch, and removes queue-full
 * spill at per-object granularity: a slab the ring rejects is parked whole on
 * a pending-slab list and re-attempted from beforeSleep (drainPendingMainFrees).
 * Nothing is ever freed synchronously on main while IO threads are live.
 *
 * Entry encoding: bit 0 marks a robj (decrRefCount) vs a raw heap buffer
 * (zfree). Slabs are zmalloc'd on main and zfree'd by the IO thread that
 * drains them: one cross-arena allocation per slab, amortized over up to
 * FREE_SLAB_CAPACITY frees. */
#define FREE_SLAB_CAPACITY 1022 /* header + entries ~= one 8KB allocation */
#define FREE_ENTRY_OBJ_BIT ((uintptr_t)1)

typedef struct freeSlab {
    size_t count;
    void *entries[];
} freeSlab;

static freeSlab *cur_free_slab = NULL;       /* accumulating; main-thread only */
static mstime_t cur_free_slab_ms = 0; /* server.mstime when the current slab was opened */
static freeSlab **pending_free_slabs = NULL; /* full slabs the ring rejected */
static size_t pending_free_slabs_len = 0;
static size_t pending_free_slabs_cap = 0;

size_t pendingMainFreesLen(void) {
    size_t n = cur_free_slab ? cur_free_slab->count : 0;
    for (size_t i = 0; i < pending_free_slabs_len; i++) n += pending_free_slabs[i]->count;
    return n;
}

/* ---- W5f: off-main pending-free byte accounting ------------------------
 * W5b routes every terminal value/buffer free off the main thread, so the
 * physical reclamation is asynchronous: an evict/delete enqueues the free but
 * used_memory does not drop until an IO thread runs it. The eviction and
 * client-eviction logic reads used_memory to decide how much to free, so
 * without this it over-evicts (drains the DB / evicts keys instead of clients)
 * while the frees are still in flight.
 *
 * We track the bytes committed to be freed off-main but not yet freed:
 * incremented at enqueue, decremented by the freeing thread. getMaxmemoryState
 * subtracts it so eviction sees the effective (post-drain) memory immediately.
 * This mirrors the intent of upstream's lazyfree-lazy-eviction handling (which
 * re-polls real memory as bio threads free); the counter makes the same
 * correction non-blocking and also covers reply-block frees. */
static _Atomic size_t offload_pending_free_bytes = 0;

/* Conservative estimate of the bytes a terminal free of o will release. Must
 * never exceed the real amount (so getMaxmemoryState never under-evicts) and
 * must be computed identically at enqueue and at free (o is sole-referenced and
 * unmodified in between, so the two calls agree). The top allocation already
 * includes any embedded key/expire; a RAW string's sds payload is the one
 * common separate allocation and is added explicitly. Aggregate element memory
 * is intentionally not walked here (cost): the estimate is then a floor, and
 * the eviction re-poll of real memory corrects any residual. */
static inline size_t offloadObjFreeBytes(robj *o) {
    size_t sz = zmalloc_size(o);
    if (o->type == OBJ_STRING && o->encoding == OBJ_ENCODING_RAW && !o->hasembval)
        sz += sdsAllocSize(objectGetVal(o));
    return sz;
}

static inline void offloadFreeAccountAdd(size_t bytes) {
    atomic_fetch_add_explicit(&offload_pending_free_bytes, bytes, memory_order_relaxed);
}

static inline void offloadFreeAccountSub(size_t bytes) {
    atomic_fetch_sub_explicit(&offload_pending_free_bytes, bytes, memory_order_relaxed);
}

/* Emergency inline reclaim: while a client is being evicted for memory
 * pressure, freeing its buffers synchronously is the point of the eviction.
 * Within this window the never-free-on-main routing is bypassed so the
 * reclaimed bytes land in used_memory before the next eviction decision.
 * Scoped strictly to the client-eviction kill path. */
static int inline_reclaim_depth = 0;
void beginInlineReclaim(void) {
    inline_reclaim_depth++;
}
void endInlineReclaim(void) {
    serverAssert(inline_reclaim_depth > 0);
    inline_reclaim_depth--;
}

size_t offloadPendingFreeBytes(void) {
    return atomic_load_explicit(&offload_pending_free_bytes, memory_order_relaxed);
}

#ifdef DEBUG_NEVER_FREE_ON_MAIN
/* Armed only inside converted routing sites; a fire means our routing fell
 * through to a synchronous main-thread free instead of an off-main hand-off. */
static _Thread_local int never_free_armed = 0;
void armNoMainThreadFree(void) { never_free_armed = 1; }
void disarmNoMainThreadFree(void) { never_free_armed = 0; }
int noMainThreadFreeArmed(void) { return never_free_armed; }
#else
void armNoMainThreadFree(void) {}
void disarmNoMainThreadFree(void) {}
int noMainThreadFreeArmed(void) { return 0; }
#endif

/* Hand one slab to the IO threads as a single job. On a full ring (or IO
 * threads inactive) the slab is parked whole on the pending-slab list and
 * re-attempted from beforeSleep. Never frees on main. */
/* Slab jobs travel on a private SPSC inbox (the shared tag space is full):
 * any active IO thread can run them, chosen round-robin. Returns 0 when the
 * chosen inbox is full or no IO thread is active. */
static int submitSlabJob(void *slab, int spsc_type) {
    static unsigned slab_rr = 0;
    if (server.active_io_threads_num <= 1) return 0;
    int tid = 1 + (int)(slab_rr++ % (unsigned)(server.active_io_threads_num - 1));
    if (spscIsFull(&io_private_inbox[tid])) return 0;
    spscEnqueue(&io_private_inbox[tid], tagJob(slab, spsc_type), true);
    io_jobs_submitted++;
    return 1;
}

static void submitFreeSlab(freeSlab *slab) {
    if (submitSlabJob(slab, JOB_SPSC_FREE_SLAB)) return;
    if (pending_free_slabs_len == pending_free_slabs_cap) {
        size_t newcap = pending_free_slabs_cap ? pending_free_slabs_cap * 2 : 8;
        pending_free_slabs = zrealloc(pending_free_slabs, newcap * sizeof(freeSlab *));
        pending_free_slabs_cap = newcap;
    }
    pending_free_slabs[pending_free_slabs_len++] = slab;
}

/* Append one terminal free to the current slab; hands the slab off when it
 * fills. The common flush point is drainPendingMainFrees in beforeSleep, once
 * per event-loop iteration. */
static void slabAppendFree(void *ptr, int is_obj) {
    if (cur_free_slab == NULL) {
        cur_free_slab = zmalloc(sizeof(freeSlab) + FREE_SLAB_CAPACITY * sizeof(void *));
        cur_free_slab->count = 0;
        cur_free_slab_ms = server.mstime;
    }
    freeSlab *s = cur_free_slab;
    s->entries[s->count++] = (void *)((uintptr_t)ptr | (is_obj ? FREE_ENTRY_OBJ_BIT : 0));
    if (s->count == FREE_SLAB_CAPACITY) {
        cur_free_slab = NULL;
        submitFreeSlab(s);
    }
}

/* Client slab: per-client work the main thread hands to the IO threads as ONE
 * job per event loop drain (or sooner, when the slab fills), instead of one job
 * per client. Each entry is a client pointer tagged with what to do:
 *   SLAB_WRITE  write the client's fenced replies
 *   SLAB_REARM  re-arm the client's socket in its IO thread's epoll set so the
 *               next read starts there (partitioned clients, W6a)
 *   SLAB_LAZY   the write is a plain buffer with nothing to release on main:
 *               if it goes out whole, the IO thread keeps the completion to
 *               itself and main reconciles the client's buffer the next time
 *               it touches the client (next command, next reply, cron, free)
 * The IO thread handles every entry and returns the slab as one response only
 * when some entry needs the main thread (SLAB_NOTIFY); otherwise it frees it. Each client's
 * fence (io_last_reply_block / io_last_bufpos), read flags and IO states are
 * set by the main thread before the slab is published; the ring enqueue is the
 * release, the dequeue the acquire. */
#define WRITE_SLAB_CAPACITY 1022
#define SLAB_FLUSH_THRESHOLD 128
#define SLAB_WRITE ((uintptr_t)1)
#define SLAB_REARM ((uintptr_t)2)
#define SLAB_LAZY ((uintptr_t)4)  /* completion needs no response unless something went wrong */
#define SLAB_NOTIFY ((uintptr_t)8) /* set by the IO thread: this entry does need main */
#define SLAB_TAGS (SLAB_WRITE | SLAB_REARM | SLAB_LAZY | SLAB_NOTIFY)
typedef struct writeSlab {
    size_t count;
    uintptr_t entries[];
} writeSlab;

static writeSlab *cur_write_slab = NULL;   /* accumulating; main-thread only */
static writeSlab *spare_write_slab = NULL; /* recycled after a round trip; main-thread only */

static writeSlab *allocWriteSlab(void) {
    writeSlab *s = spare_write_slab;
    if (s) {
        spare_write_slab = NULL;
    } else {
        s = zmalloc(sizeof(writeSlab) + WRITE_SLAB_CAPACITY * sizeof(uintptr_t));
    }
    s->count = 0;
    return s;
}

static void recycleWriteSlab(writeSlab *s) {
    if (spare_write_slab == NULL) {
        spare_write_slab = s;
    } else {
        zfree(s);
    }
}

/* Undo the staging of a client whose slab could not be handed off. */
static void unstageWriteClient(client *c, int lazy) {
    c->io_write_state = CLIENT_IDLE;
    connSetPostponeUpdateState(c->conn, 0);
    c->write_flags = 0;
    c->io_last_reply_block = NULL;
    c->io_last_bufpos = 0;
    if (!lazy) server.stat_io_writes_pending--;
}

static void unstageRearmClient(client *c) {
    c->flag.pending_read = 0;
    server.stat_io_reads_pending--;
    c->io_read_state = CLIENT_IDLE;
}

/* Hand the accumulated slab to an IO thread. When the ring is full the
 * clients are unstaged: writes go back to the pending write queue for the
 * next iteration and re-arms are retried from clientsCron; nothing is ever
 * written on the main thread here. */
void flushWriteSlab(void) {
    writeSlab *s = cur_write_slab;
    if (s == NULL) return;
    cur_write_slab = NULL;
    if (s->count == 0) {
        recycleWriteSlab(s);
        return;
    }
    if (likely(submitSlabJob(s, JOB_SPSC_WRITE_SLAB))) return;
    for (size_t i = 0; i < s->count; i++) {
        client *c = (client *)(s->entries[i] & ~SLAB_TAGS);
        if (s->entries[i] & SLAB_WRITE) {
            unstageWriteClient(c, s->entries[i] & SLAB_LAZY);
            putClientInPendingWriteQueue(c);
        }
        if (s->entries[i] & SLAB_REARM) unstageRearmClient(c);
    }
    recycleWriteSlab(s);
}

static void stageSlabEntry(client *c, uintptr_t tag) {
    if (cur_write_slab == NULL) cur_write_slab = allocWriteSlab();
    /* Merge with the previous entry when it is the same client: a write and a
     * re-arm for one client in the same pass become one entry, write first. */
    if (cur_write_slab->count > 0 &&
        (cur_write_slab->entries[cur_write_slab->count - 1] & ~SLAB_TAGS) == (uintptr_t)c) {
        cur_write_slab->entries[cur_write_slab->count - 1] |= tag;
        return;
    }
    cur_write_slab->entries[cur_write_slab->count++] = (uintptr_t)c | tag;
    /* Flush well before the slab is full: handing work to the IO threads
     * several times per loop keeps clients out of lockstep with main, so
     * completions arrive while main is still busy instead of after it idles. */
    if (cur_write_slab->count >= SLAB_FLUSH_THRESHOLD) flushWriteSlab();
}

static void stageWriteClient(client *c) {
    stageSlabEntry(c, SLAB_WRITE);
}

/* ---- Client partitioning (W6a) ------------------------------------------ */

static int clientIsPartitionable(client *c) {
    if (!c->conn || c->flag.fake) return 0;
    if (!strictOffloadActive()) return 0;
    if (server.io_threads_num < 2) return 0;
    if (c->conn->type != connectionByType(CONN_TYPE_SOCKET)) return 0;
    if (c->flag.lua_debug || c->slot_migration_job) return 0;
    if (getClientType(c) == CLIENT_TYPE_REPLICA || isReplicatedClient(c)) return 0;
    return 1;
}

static void setClientReadFlagsForOffload(client *c) {
    c->read_flags = canParseCommand(c) ? 0 : READ_FLAGS_DONT_PARSE;
    c->read_flags |= authRequired(c) ? READ_FLAGS_AUTH_REQUIRED : 0;
    c->read_flags |= isReplicatedClient(c) ? READ_FLAGS_REPLICATED : 0;
}

/* Watch a new regular TCP client's socket from one IO thread's epoll set.
 * The main thread never installs a read handler for such a client: the IO
 * thread reads and parses on readiness, main only consumes the parsed
 * commands and re-arms the socket once it has drained them. One-shot
 * readiness is what serializes reads against main's drain. */
int tryPartitionClient(client *c) {
    if (!clientIsPartitionable(c)) return C_ERR;
    int tid = 1 + (int)(partition_rr++ % (unsigned)(server.io_threads_num - 1));
    if (io_epfd[tid] <= 0) return C_ERR; /* 0 is a never-initialized slot */
    setClientReadFlagsForOffload(c);
    struct epoll_event ev = {.events = EPOLLIN | EPOLLONESHOT, .data.ptr = c};
    if (epoll_ctl(io_epfd[tid], EPOLL_CTL_ADD, c->conn->fd, &ev) != 0) {
        c->read_flags = 0;
        return C_ERR;
    }
    c->io_tid = tid;
    c->flag.partitioned = 1;
    c->flag.pending_read = 1;
    server.stat_io_reads_pending++;
    partitioned_clients++;
    return C_OK;
}

/* Re-arm a partitioned client's socket once the main thread is done with it:
 * its previous read has been consumed, its parsed commands executed, and it is
 * neither blocked nor closing. Staged into the slab, so the epoll_ctl runs on
 * an IO thread. */
void armPartitionedClientRead(client *c) {
    if (!c->flag.partitioned) return;
    if (c->flag.pending_read) return; /* armed, or a read is in flight */
    if (c->io_read_state != CLIENT_IDLE) return;
    if (c->flag.close_asap || c->flag.protected || c->flag.close_after_reply) return;
    if (c->flag.unblocked) return; /* main is about to resume it and re-arms then */
    if (!c->flag.blocked) {
        /* A blocked client keeps reading (without parsing: DONT_PARSE) so the
         * bytes completing a partial command arrive while it waits; its queued
         * commands run on unblock. Otherwise nothing may be left to execute. */
        if (c->cmd_queue.off < c->cmd_queue.len) return; /* commands still to execute */
        if (c->flag.pending_command) return;            /* a complete command still in argv */
    }
    if (server.active_io_threads_num <= 1) return;   /* threads mid-scale; clientsCron retries */
    setClientReadFlagsForOffload(c);
    c->flag.pending_read = 1;
    server.stat_io_reads_pending++;
    /* Busy to main until the IO thread has touched the socket: a client with
     * a staged re-arm cannot be freed, held or unpartitioned underneath it. */
    c->io_read_state = CLIENT_ARMING_IO;
    stageSlabEntry(c, SLAB_REARM);
}

/* Wait until the client's IO thread has finished the pass it may be in with a
 * stale reference to this client (the event array of one epoll_wait). */
static void waitPartitionPass(int tid) {
    if (tid <= 0 || io_threads[tid] == 0) return;
    /* An inactive thread is parked on its mutex and polls nothing: there is
     * no pass to wait for (io-threads is being changed). */
    if (tid >= server.active_io_threads_num) return;
    uint64_t seq = atomic_load_explicit(&io_epoll_seq[tid], memory_order_acquire);
    while (atomic_load_explicit(&io_epoll_seq[tid], memory_order_acquire) == seq) {
        if (io_threads[tid] == 0) break;
    }
}

/* Claim a partitioned client for teardown. Removes the socket from its IO
 * thread's epoll set and moves IDLE to CLOSING so that thread starts no read.
 * A read in flight is left alone: the caller sees clientHasPendingIO and frees
 * asynchronously, and the retry gets here again once the read has landed. */
void partitionedClientDetach(client *c) {
    if (!c->flag.partitioned) return;
    int tid = c->io_tid;
    if (io_epfd[tid] > 0 && c->conn) epoll_ctl(io_epfd[tid], EPOLL_CTL_DEL, c->conn->fd, NULL);
    /* A staged re-arm lands soon (the slab is published or being published);
     * wait for it rather than racing the IO thread's epoll_ctl. */
    if (c->io_read_state == CLIENT_ARMING_IO) {
        flushWriteSlab();
        while (c->io_read_state == CLIENT_ARMING_IO) atomic_thread_fence(memory_order_acquire);
    }
    uint8_t expected = CLIENT_IDLE;
    int claimed = __atomic_compare_exchange_n(&c->io_read_state, &expected, CLIENT_CLOSING_IO, 0,
                                              __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE);
    if (!claimed && expected != CLIENT_CLOSING_IO) return; /* read in flight or landed, not ours yet */
    waitPartitionPass(tid);
    if (c->flag.pending_read) {
        /* Armed but no readiness ever fired: the expected completion will not come. */
        c->flag.pending_read = 0;
        server.stat_io_reads_pending--;
    }
    c->flag.partitioned = 0;
    c->io_tid = 0;
    partitioned_clients--;
}

/* Return a partitioned client to the main event loop, for roles the IO
 * threads do not own (replica, monitor, throttled, slot migration). */
void unpartitionClient(client *c) {
    if (!c->flag.partitioned) return;
    int tid = c->io_tid;
    if (io_epfd[tid] > 0 && c->conn) epoll_ctl(io_epfd[tid], EPOLL_CTL_DEL, c->conn->fd, NULL);
    if (c->io_read_state == CLIENT_ARMING_IO) flushWriteSlab();
    waitPartitionPass(tid);
    /* A staged re-arm lands, then a read in flight completes through the normal response path. */
    while (c->io_read_state == CLIENT_ARMING_IO || c->io_read_state == CLIENT_PENDING_IO)
        atomic_thread_fence(memory_order_acquire);
    if (c->flag.pending_read && c->io_read_state == CLIENT_IDLE) {
        c->flag.pending_read = 0;
        server.stat_io_reads_pending--;
    }
    c->flag.partitioned = 0;
    c->io_tid = 0;
    partitioned_clients--;
    if (c->conn && !c->flag.close_asap) connSetReadHandler(c->conn, readQueryFromClient);
}

void unpartitionAllClients(void) {
    if (partitioned_clients == 0) return;
    listIter li;
    listNode *ln;
    listRewind(server.clients, &li);
    while ((ln = listNext(&li))) unpartitionClient((client *)listNodeValue(ln));
}

/* Briefly take an armed partitioned client's socket away from its IO thread so
 * the main thread can touch the read-side buffers (clientsCron). Returns 0 when
 * a read is in flight, in which case the caller skips this round. Release
 * re-arms the socket directly: a readiness event consumed while held was
 * skipped by the IO thread and would otherwise be lost. */
int partitionedClientHold(client *c) {
    if (!c->flag.partitioned || !c->flag.pending_read) return 1; /* main already owns it */
    uint8_t expected = CLIENT_IDLE;
    return __atomic_compare_exchange_n(&c->io_read_state, &expected, CLIENT_HELD_IO, 0,
                                       __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE);
}

void partitionedClientRelease(client *c) {
    if (!c->flag.partitioned || c->io_read_state != CLIENT_HELD_IO) return;
    __atomic_store_n(&c->io_read_state, CLIENT_IDLE, __ATOMIC_RELEASE);
    struct epoll_event ev = {.events = EPOLLIN | EPOLLONESHOT, .data.ptr = c};
    if (io_epfd[c->io_tid] > 0 && c->conn) epoll_ctl(io_epfd[c->io_tid], EPOLL_CTL_MOD, c->conn->fd, &ev);
}

/* IO-thread side: one pass over the sockets partitioned to this thread. A
 * client whose state is not IDLE (main is draining it, or is closing it) is
 * skipped; one-shot readiness will not fire again until main re-arms it.
 * Completions are queued on this thread's own ring and published once at the
 * end of the pass. */
static int ioThreadPollPartition(int id) {
    /* An empty poll is a wasted syscall, and a spinning thread makes several
     * per command it eventually serves. After an empty poll the set is left
     * alone for a few microseconds (the thread keeps serving its inbox), which
     * bounds the added read latency and cuts the poll rate to what readiness
     * actually arrives at. */
    static _Thread_local monotime next_poll_at = 0;
    if (next_poll_at) {
        if (getMonotonicUs() < next_poll_at) return 0;
        next_poll_at = 0;
    }
    struct epoll_event evs[IO_EPOLL_BATCH];
    int n = epoll_wait(io_epfd[id], evs, IO_EPOLL_BATCH, 0);
    if (n == 0 && server.io_poll_backoff_us > 0) next_poll_at = getMonotonicUs() + server.io_poll_backoff_us;
    int processed = 0;
    for (int i = 0; i < n; i++) {
        client *c = (client *)evs[i].data.ptr;
        if (c->flag.fastpath) {
            /* Fast-path client: level triggered, owned here for life. */
            if (evs[i].events & EPOLLOUT) fastpathClientWritable(id, c);
            if (evs[i].events & (EPOLLIN | EPOLLHUP | EPOLLERR)) fastpathClientReadable(id, c);
            processed++;
            continue;
        }
        uint8_t expected = CLIENT_IDLE;
        if (!__atomic_compare_exchange_n(&c->io_read_state, &expected, CLIENT_PENDING_IO, 0,
                                         __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE))
            continue;
        ioThreadReadQueryFromClient(c);
        processed++;
    }
    if (processed) spscCommit(&io_cmd_ring[id]);
    atomic_fetch_add_explicit(&io_epoll_seq[id], 1, memory_order_release);
    return processed;
}

/* IO-thread side. After a read has been parsed, publish the client's work on
 * this thread's ring: one RING_CMD per complete command, then RING_END. When
 * the ring cannot take the whole read, only RING_END is queued and the main
 * thread runs the read through the per-client path instead; when it cannot
 * take even that, the completion goes through the shared outbox. Commands are
 * only ringed for a clean read (data, parse succeeded so far, first command
 * complete); anything else is the per-client path's business. */
void ioThreadQueueReadCompletion(client *c) {
    int id = thread_id;
    spscQueue *q = &io_cmd_ring[id];
    if (id <= 0 || q->buffer == NULL) {
        sendToMainThread(c, JOB_RES_READ_CLIENT);
        return;
    }
    size_t ncmd = 0;
    if (c->nread > 0 && !(c->read_flags & (READ_FLAGS_DONT_PARSE | READ_FLAGS_QB_LIMIT_REACHED | READ_FLAGS_REPLICATED)) &&
        !isParsingError(c) && (c->read_flags & READ_FLAGS_PARSING_COMPLETED) && c->argc > 0) {
        ncmd = 1;
        for (int i = c->cmd_queue.off; i < c->cmd_queue.len; i++) {
            /* Stop at the first incomplete command: it and anything after it
             * are re-parsed by main. Error entries are complete and are ringed;
             * main replies to them in order. */
            if (!(c->cmd_queue.cmds[i].read_flags & READ_FLAGS_PARSING_COMPLETED) &&
                !(c->cmd_queue.cmds[i].read_flags & READ_FLAGS_ERROR_MASK))
                break;
            ncmd++;
        }
    }
    size_t free_slots = spscFreeSlots(q);
    if (free_slots == 0) {
        sendToMainThread(c, JOB_RES_READ_CLIENT);
        return;
    }
    if (free_slots < ncmd + 1) ncmd = 0;
    for (size_t i = 0; i < ncmd; i++) spscEnqueue(q, (void *)((uintptr_t)c | RING_CMD), false);
    spscEnqueue(q, (void *)((uintptr_t)c | RING_END), false);
}

/* Accounting a read shares with handleReadResult's success branch. */
static void accountRingRead(client *c) {
    server.stat_total_reads_processed++;
    c->last_interaction = server.unixtime;
    c->net_input_bytes += c->nread;
    server.stat_net_input_bytes += c->nread;
}

/* Main-thread side: the per-client epilogue of one read. On the fast path all
 * commands were executed from the ring, so what remains is to account the
 * read, release the client's IO state and hand it back to its IO thread
 * (write staged, socket re-armed). Anything unusual takes the existing
 * per-client completion path. */
static void ringReadEnd(client *c) {
    serverAssert(c->io_read_state == CLIENT_COMPLETED_IO);
    server.stat_io_reads_pending--;
    server.stat_io_reads_processed++;
    if (c->flag.close_after_reply) {
        /* A protocol error replied from the ring, or an earlier close request:
         * nothing more is executed, the reply goes out, the socket stays disarmed. */
        c->flag.pending_read = 0;
        c->io_read_state = CLIENT_IDLE;
        if (c->flag.protected) return;
        accountRingRead(c);
        beforeNextClient(c); /* frees a client marked close_asap, as the per-client path does */
        return;
    }
    /* The fast path requires a read whose first command was complete and ran
     * from the ring. A parse error, an empty or negative multibulk, a partial
     * command, no data, a query buffer limit or a read taken without parsing
     * all need handleParseResults/handleReadResult and take the per-client path. */
    if (c->argc > 0 || c->nread <= 0 || !(c->read_flags & READ_FLAGS_PARSING_COMPLETED) ||
        (c->read_flags & (READ_FLAGS_DONT_PARSE | READ_FLAGS_QB_LIMIT_REACHED | READ_FLAGS_ERROR_MASK))) {
        processClientIOReadsDone(c);
        return;
    }
    c->flag.pending_read = 0;
    c->io_read_state = CLIENT_IDLE;
    if (c->flag.protected) return;
    accountRingRead(c);
    /* A partial command, or commands left by a client that could not execute
     * them (blocked), follow the per-client drain; it re-parses partials. A
     * client marked close_asap (output buffer limit, CLIENT KILL) is freed by
     * beforeNextClient right away, as on the per-client path, so its memory
     * is gone before the next command's eviction check. */
    if (!c->flag.close_asap &&
        (c->cmd_queue.off < c->cmd_queue.len || (c->querybuf && c->qb_pos < sdslen(c->querybuf)))) {
        if (processPendingCommandAndInputBuffer(c) != C_OK) return;
    }
    if (c->flag.close_asap) {
        beforeNextClient(c); /* frees the client */
        return;
    }
    c->flag.ring_epilogue = 1;
    beforeNextClient(c);
    c->flag.ring_epilogue = 0;
}

/* Main-thread side: drain the command rings. A batch is formed from one
 * thread's ring in small chunks until its keys fill the prefetch budget (so
 * every command in the batch has its keys prefetched, as the per-client batch
 * did) or RING_BATCH entries are in hand; then the entries run in order.
 * Bounded so main keeps returning to its event loop under load. */
#define RING_CHUNK 8
static size_t ringAddChunkToPrefetch(uintptr_t *ents, size_t from, size_t to, getKeysResult *result, int *room) {
    /* The k-th command of a client in this batch is its current argv (k == 0
     * with a command pending) or the k-th queued one. With a stride above 1
     * only one command in N has its keys prefetched: coverage is a trade
     * between key extraction cost and the misses it hides, and the right
     * setting depends on how much of the keyspace is cache resident. */
    static unsigned stride_pos = 0;
    int stride = server.prefetch_ring_stride;
    for (size_t i = from; i < to && *room; i++) {
        if (!(ents[i] & RING_CMD)) continue;
        client *c = (client *)(ents[i] & ~RING_TAGS);
        if (c->io_read_state != CLIENT_COMPLETED_IO) continue;
        int k = c->ring_seen++;
        if (stride > 1 && (stride_pos++ % (unsigned)stride) != 0) continue;
        struct serverCommand *cmd;
        robj **argv;
        int argc, slot, flags;
        if (c->argc > 0 && k == 0) {
            cmd = c->parsed_cmd, argv = c->argv, argc = c->argc, slot = c->slot, flags = c->read_flags;
        } else {
            int idx = c->cmd_queue.off + k - (c->argc > 0 ? 1 : 0);
            if (idx >= c->cmd_queue.len) continue;
            parsedCommand *p = &c->cmd_queue.cmds[idx];
            cmd = p->cmd, argv = p->argv, argc = p->argc, slot = p->slot, flags = p->read_flags;
            p->read_flags |= READ_FLAGS_PREFETCHED;
        }
        if (!cmd || (flags & READ_FLAGS_BAD_ARITY)) continue;
        *room = prefetchBatchAddCommand(cmd, argv, argc, c->db, slot, result);
    }
    return to;
}

/* Warm the lines main touches first for each client of a chunk: the head of
 * the client struct (flags, IO states, argv), the command queue, and the head
 * of the reply buffer the first reply lands in. */
static void ringPrefetchClients(uintptr_t *ents, size_t from, size_t to) {
    for (size_t i = from; i < to; i++) {
        client *c = (client *)(ents[i] & ~RING_TAGS);
        valkey_prefetch(c);
        valkey_prefetch(&c->cmd_queue);
        valkey_prefetch((const void *)&c->io_read_state);
    }
}

/* With one command per client in flight (no pipelining) the rings receive
 * entries at the rate main completes them, so a drain that runs every event
 * loop iteration finds one or two entries and the whole loop's fixed cost
 * (beforeSleep, epoll_wait, ring scan) is paid per command. When the backlog
 * across the rings is thin, spin here for a bounded time until a batch's worth
 * has arrived: main is the bottleneck of the closed loop, so cycles saved per
 * command return as throughput, and the added wait is bounded and small
 * against the queueing latency of thousands of clients. */
static void ringCoalesce(void) {
    int budget_us = server.io_ring_coalesce_us;
    if (budget_us <= 0 || partitioned_clients == 0) return;
    size_t backlog = 0;
    for (int t = 1; t < server.io_threads_num; t++)
        if (io_cmd_ring[t].buffer) backlog += spscBacklog(&io_cmd_ring[t]);
    if (backlog == 0 || backlog >= RING_BATCH / 2) return;
    monotime deadline = getMonotonicUs() + budget_us;
    do {
        for (int i = 0; i < 64; i++) cpuRelax();
        backlog = 0;
        for (int t = 1; t < server.io_threads_num; t++)
            if (io_cmd_ring[t].buffer) backlog += spscBacklog(&io_cmd_ring[t]);
        if (backlog >= RING_BATCH / 2) return;
    } while (getMonotonicUs() < deadline);
}

static int processCommandRing(void) {
    uintptr_t ents[RING_BATCH];
    int total = 0;
    /* Inside processEventsWhileBlocked the interrupted command's client batch
     * may still hold prefetch state; leave it alone. */
    int use_prefetch = prefetchBatchEnabled() && !ProcessingEventsWhileBlocked;
    ringCoalesce();
    while (total < IO_PARTITION_COMPLETIONS_PER_CALL) {
        int got_any = 0;
        for (int t = 1; t < server.io_threads_num; t++) {
            spscQueue *q = &io_cmd_ring[t];
            if (q->buffer == NULL) continue;
            size_t n = 0;
            if (use_prefetch) {
                /* Each chunk's client structs are prefetched before its keys are
                 * extracted, so the misses of eight remote clients overlap
                 * instead of being taken one at a time; the batch still stops
                 * at the key budget so every command in it has its keys warm. */
                getKeysResult result;
                initGetKeysResult(&result);
                int room = 1;
                while (room && n < RING_BATCH) {
                    size_t want = RING_BATCH - n < RING_CHUNK ? RING_BATCH - n : RING_CHUNK;
                    size_t got = spscDequeueBatch(q, (void **)(ents + n), want);
                    if (got == 0) break;
                    ringPrefetchClients(ents, n, n + got);
                    ringAddChunkToPrefetch(ents, n, n + got, &result, &room);
                    n += got;
                }
                getKeysFreeResult(&result);
                prefetchBatchRun();
                /* The key arrays are spent once the prefetch is issued; a read
                 * taking the per-client path below starts its client batch clean. */
                prefetchBatchReset();
            } else {
                n = spscDequeueBatch(q, (void **)ents, RING_BATCH);
            }
            if (n == 0) continue;
            got_any = 1;
            total += (int)n;

            for (size_t i = 0; i < n; i++) {
                client *c = (client *)(ents[i] & ~RING_TAGS);
                c->ring_seen = 0;
                if (ents[i] & RING_CMD) {
                    ringExecuteOne(c);
                } else {
                    ringReadEnd(c);
                }
            }
            /* Reads that took the per-client path may have left a client batch. */
            processClientsCommandsBatch();
        }
        if (!got_any) break;
    }
    return total;
}

int getCurTid(void) {
    return thread_id;
}

void commitIOJobs(void) {
    for (int i = 1; i < server.active_io_threads_num; i++) {
        spscCommit(&io_private_inbox[i]);
    }
}

/* Jobs sent but not yet processed by IO threads. */
static size_t getPendingIOThreadsJobs(void) {
    return io_jobs_submitted - atomic_load_explicit(&io_jobs_finished, memory_order_acquire);
}

/* Read/write jobs awaiting response from IO threads. */
static size_t getPendingIOResponsesCount(void) {
    return server.stat_io_writes_pending + server.stat_io_reads_pending + cluster_io_pending_responses;
}

/* Drains the I/O threads queue by waiting for all jobs to be processed.
 * This function must be called from the main thread. */
void drainIOThreadsQueue(void) {
    serverAssert(inMainThread());
    flushWriteSlab();
    commitIOJobs();
    while (getPendingIOThreadsJobs()) {
        atomic_thread_fence(memory_order_acquire);
    }
}

/* Finish a lazily completed write on the main thread: the IO thread wrote the
 * fenced buffer and moved on without a response. Cheap when there is nothing
 * to do; called wherever main is about to depend on the write state. */
void reconcileLazyWrite(client *c) {
    if (c->io_write_state != CLIENT_COMPLETED_IO || !(c->write_flags & WRITE_FLAGS_LAZY)) return;
    atomic_thread_fence(memory_order_acquire);
    server.stat_io_writes_processed++;
    processClientIOWriteDone(c);
}

/* Returns if there is an IO operation in progress for the given client. */
int clientHasPendingIO(client *c) {
    /* CLOSING is the main thread's own claim on a partitioned client, not IO in flight. */
    return (c->io_read_state != CLIENT_IDLE && c->io_read_state != CLIENT_CLOSING_IO) ||
           c->io_write_state != CLIENT_IDLE;
}

/* Wait until the IO-thread is done with the client */
void waitForClientIO(client *c) {
    /* No need to wait if the client was not offloaded to the IO thread. */
    if (c->io_read_state == CLIENT_IDLE && c->io_write_state == CLIENT_IDLE) return;

    /* A staged write only reaches an IO thread once its slab is published. */
    if (inMainThread()) flushWriteSlab();

    /* Wait for read operation to complete if pending. */
    while (c->io_read_state == CLIENT_PENDING_IO) {
        atomic_thread_fence(memory_order_acquire);
    }

    /* Wait for write operation to complete if pending. */
    while (c->io_write_state == CLIENT_PENDING_IO) {
        atomic_thread_fence(memory_order_acquire);
    }

    /* Final memory barrier to ensure all changes are visible */
    atomic_thread_fence(memory_order_acquire);
}

void IOThreadsBeforeSleep(long long current_time) {
    if (server.io_threads_num == 1) return;
    serverAssert(inMainThread());

    flushWriteSlab();
    commitIOJobs();

    if (server.io_threads_always_active) {
        /* active_all_io_threads state is for debug purposes: deactivate all threads before sleep if no pending jobs,
         * and reactivate all after sleep. We can't leave it active all the time as it will consume much CPU that will interfere with tests */
        if (server.active_io_threads_num > 1 && getPendingIOThreadsJobs() == 0 && !strictOffloadActive()) {
            for (int i = 1; i < server.active_io_threads_num; i++) {
                pthread_mutex_lock(&io_threads_mutex[i]);
            }
            server.active_io_threads_num = 1;
        }
    }

    /* If threads are not active, track main-thread active time for ignition decision */
    if (server.active_io_threads_num == 1) {
        static long long last_measurement_time = 0;
        if (current_time - last_measurement_time < 50000) return; /* Sample once in 50ms */
        last_measurement_time = current_time;
        trackInstantaneousMetric(STATS_METRIC_MAIN_THREAD_ACTIVE_TIME, server.stat_active_time, current_time, 1000000);
    }
}

#define IO_COOLDOWN_MS 1000
#define IO_SAMPLE_RATE_MS 10
#define IO_IGNITION_EVENTS 4
/* Start using I/O threads when the main thread is active for more than the below
 * defined percentage of the time. This number is picked somewhat arbitrarily but
 * needed to be low enough to make sure we start the next thread quickly while not
 * starting too many threads unnecessarily to avoid contention. */
#define IO_IGNITION_MAIN_THREAD_ACTIVE_PERCENT 30
#define BATCH_SIZE 32

void IOThreadsAfterSleep(int numevents) {
    if (server.io_threads_num == 1) return;
    serverAssert(inMainThread());
    /* Always Active Policy */
    if (server.io_threads_always_active) {
        /* Strict offload defers every regular client write until a worker is
         * active, so a scale-up that reset the active count must not wait for
         * a socket event: the deferred write is the event that would arrive. */
        if ((numevents > 0 || strictOffloadActive()) && server.active_io_threads_num < server.io_threads_num) {
            for (int i = server.active_io_threads_num; i < server.io_threads_num; i++) {
                pthread_mutex_unlock(&io_threads_mutex[i]);
            }
            server.active_io_threads_num = server.io_threads_num;
        }
        return;
    }

    mstime_t now = server.mstime;
    static long long last_scale_time = 0;

    /* Strict offload with client partitioning: every IO thread owns sockets and
     * polls them itself, so a parked thread would stall its clients. Keep all
     * configured threads active; the load-based policy below is for the
     * non-strict mode. */
    if (strictOffloadActive()) {
        if (server.active_io_threads_num < server.io_threads_num) {
            for (int i = server.active_io_threads_num; i < server.io_threads_num; i++) {
                pthread_mutex_unlock(&io_threads_mutex[i]);
            }
            server.active_io_threads_num = server.io_threads_num;
            last_scale_time = now;
        }
        return;
    }

    /* Ignition Policy */
    if (server.active_io_threads_num == 1) {
        int should_ignite = 0;
        float main_thread_active_time = (float)getInstantaneousMetric(STATS_METRIC_MAIN_THREAD_ACTIVE_TIME) / 10000.0;
        /* Ignite IO threads when main-thread active time exceeds the threshold (30%) */
        should_ignite = (main_thread_active_time > (float)IO_IGNITION_MAIN_THREAD_ACTIVE_PERCENT);
        /* Strict offload requires a worker to be active at all times so that no
         * regular client socket op ever falls back to the main thread. Ignite
         * unconditionally, regardless of load. */
        if (strictOffloadActive()) should_ignite = 1;
        if (should_ignite) {
            pthread_mutex_unlock(&io_threads_mutex[1]);
            server.active_io_threads_num++;
            last_scale_time = now;
            serverLog(LL_DEBUG, "IO threads ignition: increased to %d", server.active_io_threads_num);
        }
        return;
    }

    static mstime_t last_sample_time = 0;
    static size_t spmc_size_sum = 0;
    static size_t sample_count = 0;

    /* Scaling Up/Down Policy */
    if (now - last_sample_time < IO_SAMPLE_RATE_MS) return;
    last_sample_time = now;

    spmc_size_sum += spmcSize(&io_shared_inbox[JOB_PRIORITY_NORMAL]);
    spmc_size_sum += spmcSize(&io_shared_inbox[JOB_PRIORITY_HIGH]);
    sample_count++;

    trackInstantaneousMetric(STATS_METRIC_IO_WAIT, spmc_size_sum, sample_count, 1);

    /* Decision (Every STATS_METRIC_SAMPLES Samples) */
    if (sample_count % STATS_METRIC_SAMPLES != 0) return;

    size_t avg_q_size = getInstantaneousMetric(STATS_METRIC_IO_WAIT);
    size_t active = server.active_io_threads_num;
    size_t target = active;

    /* Calculate Target */
    if (avg_q_size > 1 && active < (size_t)server.io_threads_num) {
        target++;
    } else if (avg_q_size == 0 && (now - last_scale_time > IO_COOLDOWN_MS)) {
        /* Strict offload keeps at least one worker (2 total incl. main) active
         * so deferred socket ops always have a thread to dispatch to. */
        size_t min_active = strictOffloadActive() ? 2 : 1;
        if (target > min_active) target--;
    }

    /* Scale Up */
    if (target > active) {
        for (size_t i = active; i < target; i++) {
            pthread_mutex_unlock(&io_threads_mutex[i]);
        }
        last_scale_time = now;
        server.active_io_threads_num = target;
        serverLog(LL_DEBUG, "IO threads increased from %zu to %zu", active, target);
    }
    /* Scale Down*/
    else if (target < active) {
        int tid = active - 1;

        /* Don't suspend if work remains in the specific thread's queue... */
        if (!spscIsEmpty(&io_private_inbox[tid])) return;
        /* ...or if we are dropping to 1 thread but the global queue still has work */
        if (target == 1 && (!spmcIsEmpty(&io_shared_inbox[JOB_PRIORITY_NORMAL]) || !spmcIsEmpty(&io_shared_inbox[JOB_PRIORITY_HIGH]))) return;

        pthread_mutex_lock(&io_threads_mutex[tid]);
        server.active_io_threads_num--;
        serverLog(LL_DEBUG, "IO threads decreased from %zu to %d", active, server.active_io_threads_num);
    }
}

/* This function performs polling on the given event loop and updates the server's
 * IO fired events count and poll state. */
void ioThreadPoll(aeEventLoop *el) {
    struct timeval tvp = {0, 0};
    int num_events = aePoll(el, &tvp);
    server.io_ae_fired_events = num_events;
    atomic_store_explicit(&server.io_poll_state, AE_IO_STATE_DONE, memory_order_release);
}

static void flushPendingIOResponsesList(list **pending_list, mpscQueue *outbox, mpscTicket *ticket, int blocking) {
    if (*pending_list == NULL) return;
    listIter li;
    listNode *ln;
    listRewind(*pending_list, &li);

    while ((ln = listNext(&li))) {
        void *job = listNodeValue(ln);
        int pushed = 0;

        /* Try to enqueue. If blocking is set, retry until success. */
        do {
            pushed = mpscEnqueue(outbox, job, ticket);
            /* The main thread does not consume responses during crash or exit. */
            if (pushed || !blocking || server.crashed ||
                atomic_load_explicit(&io_threads_exiting, memory_order_acquire))
                break;
            atomic_thread_fence(memory_order_acquire);
        } while (true);

        if (pushed) {
            listDelNode(*pending_list, ln);
        } else {
            return;
        }
    }

    /* List is fully drained */
    listRelease(*pending_list);
    *pending_list = NULL;
}

static void flushPendingIOResponses(int blocking) {
    flushPendingIOResponsesList(&pending_io_responses[JOB_PRIORITY_HIGH], &io_shared_outbox[JOB_PRIORITY_HIGH], &io_thread_ticket[JOB_PRIORITY_HIGH], blocking);
    flushPendingIOResponsesList(&pending_io_responses[JOB_PRIORITY_NORMAL], &io_shared_outbox[JOB_PRIORITY_NORMAL], &io_thread_ticket[JOB_PRIORITY_NORMAL], blocking);
}

/* Define a cleanup function that will clean all thread resources */
void cleanupThreadResources(void *dummy) {
    UNUSED(dummy);

    /* Blocking flush: ensure all pending jobs are sent before thread dies */
    flushPendingIOResponses(1);

    /* Release response lists left when the flush stops early. */
    for (int i = 0; i < JOB_PRIORITY_COUNT; i++) {
        if (pending_io_responses[i]) {
            listRelease(pending_io_responses[i]);
            pending_io_responses[i] = NULL;
        }
    }

    /* Free the shared query buffer */
    freeSharedQueryBuf();
}

/* A write slab: handle every entry, then return the slab as one response
 * instead of one response per client. The states stored here become visible
 * to the main thread through the outbox enqueue. */
static void ioThreadWriteSlab(writeSlab *slab) {
    int notify = 0;
    for (size_t j = 0; j < slab->count; j++) {
        uintptr_t e = slab->entries[j];
        client *c = (client *)(e & ~SLAB_TAGS);
        /* Order matters: the client is ours until the last state we publish.
         * The write goes out first, the socket is re-armed (read state
         * ARMING -> IDLE), and only then is the write state published, after
         * which main may free the client. */
        int needs_main = 0;
        if (e & SLAB_WRITE) needs_main = ioThreadWriteClientNoSignal(c, 0);
        if (e & SLAB_REARM) {
            struct epoll_event ev = {.events = EPOLLIN | EPOLLONESHOT, .data.ptr = c};
            epoll_ctl(io_epfd[c->io_tid], EPOLL_CTL_MOD, c->conn->fd, &ev);
            atomic_thread_fence(memory_order_release);
            c->io_read_state = CLIENT_IDLE;
        }
        if (e & SLAB_WRITE) {
            atomic_thread_fence(memory_order_release);
            c->io_write_state = CLIENT_COMPLETED_IO;
            if (needs_main) {
                if (e & SLAB_LAZY) slab->entries[j] = e | SLAB_NOTIFY;
                notify = 1;
            }
        }
    }
    if (notify) {
        sendToMainThread(slab, JOB_RES_WRITE_SLAB);
    } else {
        zfree(slab);
    }
}

/* A batch of terminal frees accumulated by main and handed off as one job:
 * walk the entries, then free the slab itself. */
static void ioThreadFreeSlab(freeSlab *slab) {
    for (size_t j = 0; j < slab->count; j++) {
        uintptr_t e = (uintptr_t)slab->entries[j];
        void *p = (void *)(e & ~FREE_ENTRY_OBJ_BIT);
        if (e & FREE_ENTRY_OBJ_BIT) {
            offloadFreeAccountSub(offloadObjFreeBytes((robj *)p));
            decrRefCount(p);
        } else {
            offloadFreeAccountSub(zmalloc_size(p));
            zfree(p);
        }
    }
    zfree(slab);
}

static inline void processTaggedSPMCJob(void *tagged_job) {
    void *data;
    int type;
    untagJob(tagged_job, &data, &type);

    switch (type) {
    case JOB_REQ_READ_CLIENT:
        ioThreadReadQueryFromClient((client *)data);
        break;
    case JOB_REQ_WRITE_CLIENT:
        ioThreadWriteToClient((client *)data);
        break;
    case JOB_REQ_FREE_OBJ:
        offloadFreeAccountSub(offloadObjFreeBytes((robj *)data));
        decrRefCount(data);
        break;
    case JOB_REQ_ACCEPT:
        ioThreadAccept((client *)data);
        break;
    case JOB_REQ_POLL:
        ioThreadPoll((aeEventLoop *)data);
        break;
    case JOB_REQ_CLUSTER_READ:
        clusterReadJob((clusterLink *)data);
        break;
    case JOB_REQ_CLUSTER_WRITE:
        clusterWriteJob((clusterLink *)data);
        break;
    case JOB_REQ_CLUSTER_ACCEPT:
        clusterAcceptJob((connection *)data);
        break;
    default:
        serverPanic("Invalid SPMC job type: %d", type);
    }
}

static void *IOThreadMain(void *myid) {
    /* The ID is the thread ID number (from 1 to server.io_threads_num-1). ID 0 is the main thread. */
    long id = (long)myid;
    char thdname[32];

    snprintf(thdname, sizeof(thdname), "io_thd_%ld", id);
    valkey_set_thread_title(thdname);
    serverSetCpuAffinity(server.server_cpulist);
    initSharedQueryBuf();
    pthread_cleanup_push(cleanupThreadResources, NULL);

    thread_id = (int)id;
    void *batch_jobs[BATCH_SIZE];
    int processed = 0;
    monotime work_start_time = 0;
    while (1) {
        /* Cancellation point so that pthread_cancel() from main thread is honored. */
        pthread_testcancel();
        size_t batch_count = 0;
        monotime prev_work_start_time = work_start_time;
        work_start_time = getMonotonicUs();
        if (processed != 0) {
            atomic_fetch_add_explicit(&used_active_time_io_thread[id],
                                      work_start_time - prev_work_start_time,
                                      memory_order_relaxed);
        }
        processed = 0;
        /* PRIORITY 1: Drain Private SPSC Queue (Batch Processing) */
        while ((batch_count = spscDequeueBatch(&io_private_inbox[id], batch_jobs, BATCH_SIZE)) > 0) {
            for (size_t i = 0; i < batch_count; i++) {
                void *data;
                int type;
                untagJob(batch_jobs[i], &data, &type);

                switch (type) {
                case JOB_SPSC_FREE_ARGV:
                    ioThreadFreeArgv((robj **)data);
                    break;
                case JOB_SPSC_POLL:
                    ioThreadPoll((aeEventLoop *)data);
                    break;
                case JOB_SPSC_WRITE_SLAB:
                    ioThreadWriteSlab((writeSlab *)data);
                    break;
                case JOB_SPSC_FREE_SLAB:
                    ioThreadFreeSlab((freeSlab *)data);
                    break;
                default:
                    serverPanic("Invalid SPSC job type: %d", type);
                }
            }
            processed += batch_count;
        }

        /* PRIORITY 2: Shared Global Queues (SPMC), high priority first.
         * Only checked after SPSC is drained. A bounded run of jobs per
         * iteration: with the fast path an iteration serves many sockets, so
         * one job per iteration would let the shared queues fill. */
        for (int shared_n = 0; shared_n < 64; shared_n++) {
            void *tagged_job = spmcDequeue(&io_shared_inbox[JOB_PRIORITY_HIGH]);
            if (!tagged_job) tagged_job = spmcDequeue(&io_shared_inbox[JOB_PRIORITY_NORMAL]);
            if (!tagged_job) break;
            processTaggedSPMCJob(tagged_job);
            processed++;
        }

        if (processed) {
            atomic_fetch_add_explicit(&io_jobs_finished, processed, memory_order_release);
        }

        /* PRIORITY 3: sockets partitioned to this thread. These reads are not
         * submitted jobs, so they count as work done but not as jobs finished. */
        if (io_epfd[id] > 0) processed += ioThreadPollPartition(id);
        /* Batches main has executed: write the replies, recycle the batches;
         * then a partial batch goes out if main has nothing of ours left. */
        processed += fastpathProcessReturns(id);
        fastpathSubmitPending(id);

        /* If both queues were empty (no processing done), wait for signal. */
        if (processed == 0) {
            int has_pending = 0;
            for (int p = 0; p < JOB_PRIORITY_COUNT; p++) {
                if (pending_io_responses[p]) has_pending = 1;
            }
            if (unlikely(has_pending)) {
                flushPendingIOResponses(0);
            } else {
                /* If it is locked. We should block until main thread unlocks it. */
                pthread_mutex_lock(&io_threads_mutex[id]);
                pthread_mutex_unlock(&io_threads_mutex[id]);
            }
        }
    }
    pthread_cleanup_pop(0);
    return NULL;
}

long long getIOThreadActiveTimeMicroseconds(int id) {
    return atomic_load_explicit(&used_active_time_io_thread[id], memory_order_relaxed);
}

static void createIOThread(int id) {
    serverAssert(server.io_threads_num > 0);
    serverAssert(id > 0 && id < server.io_threads_num);

    /* Initialize the private SPSC queue for this thread */
    spscInit(&io_private_inbox[id], IO_SPSC_QUEUE_SIZE);
    spscInit(&io_cmd_ring[id], IO_CMD_RING_SIZE);
    fastpathInitThread(id);

    /* Epoll set for the sockets partitioned to this thread. */
    io_epfd[id] = epoll_create1(EPOLL_CLOEXEC);
    if (io_epfd[id] < 0) serverLog(LL_WARNING, "IO thread %d: epoll_create1 failed (%s); no clients will be partitioned to it", id, strerror(errno));

    pthread_t tid;
    pthread_mutex_init(&io_threads_mutex[id], NULL);
    pthread_mutex_lock(&io_threads_mutex[id]); /* Thread will be stopped. */

    pthread_attr_t attr;
    serverInitThreadAttribute(&attr);

    int err = pthread_create(&tid, &attr, IOThreadMain, (void *)(long)id);
    pthread_attr_destroy(&attr);
    if (err) {
        serverLog(LL_WARNING, "Fatal: Can't initialize IO thread, pthread_create failed with: %s", strerror(err));
        exit(1);
    }
    io_threads[id] = tid;
}

/* Terminates the IO thread specified by id. */
static void shutdownIOThread(int id) {
    int err;
    pthread_t tid = io_threads[id];
    if (tid == pthread_self()) return;
    if (tid == 0) return;

    /* Only unlock mutex for inactive threads. Active threads are already unlocked. */
    if (id >= server.active_io_threads_num) {
        pthread_mutex_unlock(&io_threads_mutex[id]);
    }
    pthread_cancel(tid);

    if ((err = pthread_join(tid, NULL)) != 0) {
        serverLog(LL_WARNING, "IO thread(tid:%lu) can not be joined: %s", (unsigned long)tid, strerror(err));
    } else {
        serverLog(LL_NOTICE, "IO thread(tid:%lu) terminated", (unsigned long)tid);
    }
    pthread_mutex_destroy(&io_threads_mutex[id]);

    /* The joined worker's inbox may contain uncommitted JOB_SPSC_FREE_ARGV jobs. */
    if (!server.crashed) {
        spscCommit(&io_private_inbox[id]);
        void *batch[BATCH_SIZE];
        size_t batch_count;
        size_t drained = 0;
        while ((batch_count = spscDequeueBatch(&io_private_inbox[id], batch, BATCH_SIZE)) > 0) {
            for (size_t i = 0; i < batch_count; i++) {
                void *data;
                int type;
                untagJob(batch[i], &data, &type);
                switch (type) {
                case JOB_SPSC_FREE_ARGV:
                    ioThreadFreeArgv((robj **)data);
                    break;
                case JOB_SPSC_POLL:
                    serverAssert(atomic_load_explicit(&io_threads_exiting, memory_order_acquire));
                    break;
                default:
                    serverPanic("Invalid SPSC job type: %d", type);
                }
            }
            drained += batch_count;
        }
        if (drained) atomic_fetch_add_explicit(&io_jobs_finished, drained, memory_order_release);
    }

    spscFree(&io_private_inbox[id]);
    spscFree(&io_cmd_ring[id]);
    fastpathFreeThread(id);
    if (io_epfd[id] >= 0) {
        close(io_epfd[id]);
        io_epfd[id] = -1;
    }
}

void killIOThreads(void) {
    /* The main thread does not consume responses while joining workers. */
    atomic_store_explicit(&io_threads_exiting, 1, memory_order_release);
    for (int j = 1; j < server.io_threads_num; j++) { /* We don't kill thread 0, which is the main thread. */
        shutdownIOThread(j);
    }
    server.active_io_threads_num = 1;
}

int updateIOThreads(const char **err) {
    serverAssert(inMainThread());

    int prev_threads_num = 1;
    for (int i = IO_THREADS_MAX_NUM - 1; i > 0; i--) {
        if (io_threads[i]) {
            prev_threads_num = i + 1;
            break;
        }
    }
    if (prev_threads_num == server.io_threads_num) return 1;

    /* DEADLOCK PREVENTION:
     * Check if the pending workload fits in the return queue.
     * If the number of pending jobs is greater than the capacity of the Global MPSC queue,
     * the worker threads might fill the queue and block. If we enter drainIOThreadsQueue
     * in that state, we will deadlock (Main thread waits for worker, Worker waits for queue space). */
    size_t pending = getPendingIOResponsesCount();

    /* Since pending is the sum of all in-flight read/write jobs, in the worst-case scenario where
     * 100% of the traffic happens to be on one priority lane, that outbox will receive at most pending
     * responses. If pending fits within each queue's capacity, neither queue can ever overflow or cause
     * workers to block while draining*/
    if (pending > io_shared_outbox[JOB_PRIORITY_NORMAL].queue_size ||
        pending > io_shared_outbox[JOB_PRIORITY_HIGH].queue_size) {
        if (err) *err = "Can't update IO threads under load, try again later";
        return 0;
    }

    serverLog(LL_NOTICE, "Changing number of IO threads from %d to %d.", prev_threads_num, server.io_threads_num);
    /* Partitioned clients belong to a specific thread's epoll set; return them
     * to the main loop before the thread set changes. New clients partition
     * across the new count. */
    unpartitionAllClients();
    drainIOThreadsQueue();

    /* Set active threads to 1, will be adjusted based on workload later. */
    for (int i = 1; i < server.active_io_threads_num; i++) {
        pthread_mutex_lock(&io_threads_mutex[i]);
    }
    server.active_io_threads_num = 1;

    if (server.io_threads_num > prev_threads_num) {
        initIOThreads(prev_threads_num);
    } else {
        for (int i = prev_threads_num - 1; i >= server.io_threads_num; i--) {
            /* Unblock inactive thread. */
            pthread_mutex_unlock(&io_threads_mutex[i]);
            shutdownIOThread(i);
            io_threads[i] = 0;
        }
    }
    return 1;
}

/* Initialize the data structures needed for I/O threads. */
void initIOThreads(int prev_threads_num) {
    /* Don't spawn any thread if the user selected a single thread:
     * we'll handle I/O directly from the main thread. */
    if (server.io_threads_num == 1) return;

    serverAssert(server.io_threads_num <= IO_THREADS_MAX_NUM);

    if (!io_threads_initialized) {
        server.active_io_threads_num = 1; /* We start with threads not active. */
        server.io_poll_state = AE_IO_STATE_NONE;
        server.io_ae_fired_events = 0;
        for (int i = 0; i < IO_THREADS_MAX_NUM; i++) io_epfd[i] = -1;
        for (int p = 0; p < JOB_PRIORITY_COUNT; p++) {
            spmcInit(&io_shared_inbox[p], IO_SPMC_QUEUE_SIZE);
            mpscInit(&io_shared_outbox[p], IO_MPSC_QUEUE_SIZE);
        }
        io_jobs_submitted = 0;
        atomic_init(&io_jobs_finished, 0);
        cluster_io_pending_responses = 0;
        prefetchCommandsBatchInit();
        io_threads_initialized = 1;
    }

    /* Spawn and initialize the I/O threads. */
    for (int i = prev_threads_num; i < server.io_threads_num; i++) {
        createIOThread(i);
    }
}

void testOnlyInitIOThreadQueues(void) {
    for (int p = 0; p < JOB_PRIORITY_COUNT; p++) {
        if (io_shared_inbox[p].buffer) spmcFree(&io_shared_inbox[p]);
        if (io_shared_outbox[p].buffer) mpscFree(&io_shared_outbox[p]);
        if (pending_io_responses[p]) {
            listRelease(pending_io_responses[p]);
            pending_io_responses[p] = NULL;
        }
        spmcInit(&io_shared_inbox[p], IO_SPMC_QUEUE_SIZE);
        mpscInit(&io_shared_outbox[p], IO_MPSC_QUEUE_SIZE);
        io_thread_ticket[p] = (mpscTicket){0};
    }
    io_jobs_submitted = 0;
    atomic_store_explicit(&io_jobs_finished, 0, memory_order_relaxed);
    cluster_io_pending_responses = 0;
}

void testOnlyFreeIOThreadQueues(void) {
    for (int p = 0; p < JOB_PRIORITY_COUNT; p++) {
        if (pending_io_responses[p]) {
            listRelease(pending_io_responses[p]);
            pending_io_responses[p] = NULL;
        }
        spmcFree(&io_shared_inbox[p]);
        mpscFree(&io_shared_outbox[p]);
        io_thread_ticket[p] = (mpscTicket){0};
    }
    io_jobs_submitted = 0;
    atomic_store_explicit(&io_jobs_finished, 0, memory_order_relaxed);
    cluster_io_pending_responses = 0;
}

/* Fill the shared inbox so the next dispatch has to take its enqueue-failure
 * path. The queue is file-static, so tests cannot do this themselves. */
void testOnlyFillIOThreadInbox(void) {
    for (int p = 0; p < JOB_PRIORITY_COUNT; p++) {
        while (spmcEnqueue(&io_shared_inbox[p], (void *)-1)) {
            /* Keep going until the queue rejects the push. */
        }
    }
}

/* Expose the cluster pending-response count so tests can assert that a failed
 * or completed dispatch leaves no response outstanding. */
size_t testOnlyGetClusterIOPendingResponses(void) {
    return cluster_io_pending_responses;
}

int trySendReadToIOThreads(client *c) {
    if (server.active_io_threads_num <= 1) return C_ERR;
    /* Fake/teardown clients may have no connection; never offload those. */
    if (!c->conn) return C_ERR;
    /* If IO thread is still reading, return C_OK so the main thread does not race it. */
    if (c->io_read_state == CLIENT_PENDING_IO) return C_OK;
    /* A completed read must be finished by processClientIOReadsDone on the main thread
     * before we try to offload another read; do not treat it like PENDING_IO. */
    if (c->io_read_state == CLIENT_COMPLETED_IO) return C_ERR;
    if (c->io_write_state == CLIENT_PENDING_IO) return C_OK;
    /* For simplicity, don't offload replica clients reads as read traffic from replica is negligible */
    if (getClientType(c) == CLIENT_TYPE_REPLICA) return C_ERR;
    /* A live replication stream reader must run on the main thread; the IO-thread
     * read path does not decode. Destroyed once the probe resolves to plaintext. */
    if (c->flag.primary && server.repl_stream_reader) return C_ERR;
    /* With Lua debug client we may call connWrite directly in the main thread */
    if (c->flag.lua_debug) return C_ERR;
    /* For simplicity let the main-thread handle the blocked clients */
    if (c->flag.blocked || c->flag.unblocked) return C_ERR;
    if (c->flag.close_asap) return C_ERR;
    /* Avoid offloading reads to IO thread for the slot migration export job while snapshotting.
     * During this phase, the main thread writes snapshot data directly via connWrite(). */
    if (c->slot_migration_job && !clusterSlotMigrationShouldInstallWriteHandler(c)) return C_ERR;

    c->read_flags = canParseCommand(c) ? 0 : READ_FLAGS_DONT_PARSE;
    c->read_flags |= authRequired(c) ? READ_FLAGS_AUTH_REQUIRED : 0;
    c->read_flags |= isReplicatedClient(c) ? READ_FLAGS_REPLICATED : 0;

    c->io_read_state = CLIENT_PENDING_IO;
    connSetPostponeUpdateState(c->conn, clientConnPostponeMaskFromIOState(c));

    jobPriority qidx = getJobPriority(c);
    if (unlikely(spmcEnqueue(&io_shared_inbox[qidx], tagJob(c, JOB_REQ_READ_CLIENT)) == false)) {
        c->read_flags = 0;
        c->io_read_state = CLIENT_IDLE;
        connSetPostponeUpdateState(c->conn, 0);
        return C_ERR;
    }

    io_jobs_submitted++;
    server.stat_io_reads_pending++;
    c->flag.pending_read = 1;
    return C_OK;
}

/* This function attempts to offload the client's write to an I/O thread.
 * Returns C_OK if the client's writes were successfully offloaded to an I/O thread,
 * or C_ERR if the client is not eligible for offloading. */
int trySendWriteToIOThreads(client *c) {
    if (server.active_io_threads_num <= 1) return C_ERR;
    if (!c->conn) return C_ERR;
    /* A lazily completed write is finished here first; it may stage the next. */
    reconcileLazyWrite(c);
    if (c->flag.close_asap) return C_ERR;
    /* The I/O thread is already writing for this client. */
    if (c->io_write_state != CLIENT_IDLE) return C_OK;
    if (c->io_read_state == CLIENT_PENDING_IO) return C_ERR;
    /* Nothing to write */
    if (!clientHasPendingReplies(c)) return C_ERR;
    /* For simplicity, avoid offloading non-online replicas */
    if (getClientType(c) == CLIENT_TYPE_REPLICA && c->repl_data->repl_state != REPLICA_STATE_ONLINE) return C_ERR;
    /* We can't offload debugged clients as the main-thread may read at the same time  */
    if (c->flag.lua_debug) return C_ERR;
    /* Avoid offloading writes to IO thread for the slot migration export job while snapshotting.
     * During this phase, replies accumulate in the output buffer but must not be flushed
     * as concurrent IO thread writes would race with the main thread processing incoming
     * ACKs on the same client's query buffer. */
    if (c->slot_migration_job && !clusterSlotMigrationShouldInstallWriteHandler(c)) return C_ERR;

    int is_replica = getClientType(c) == CLIENT_TYPE_REPLICA;
    clientReplyBlock *block = NULL;
    if (is_replica) {
        c->io_last_reply_block = listLast(server.repl_buffer_blocks);
        replBufBlock *o = listNodeValue(c->io_last_reply_block);
        c->io_last_bufpos = o->used;
    } else {
        /* Save the last block of the reply list to io_last_reply_block and the used
         * position to io_last_bufpos. The I/O thread will write only up to
         * io_last_bufpos, regardless of the c->bufpos value. This is to prevent I/O
         * threads from reading data that might be invalid in their local CPU cache. */
        c->io_last_reply_block = listLast(c->reply);
        if (c->io_last_reply_block) {
            block = (clientReplyBlock *)listNodeValue(c->io_last_reply_block);
            c->io_last_bufpos = block->used;
        } else {
            c->io_last_bufpos = (size_t)c->bufpos;
        }
    }

    serverAssert(c->bufpos > 0 || c->io_last_bufpos > 0 || is_replica);

    /* The main-thread will update the client state after the I/O thread completes the write. */
    c->write_flags = is_replica ? WRITE_FLAGS_IS_REPLICA : 0;
    c->io_write_state = CLIENT_PENDING_IO;
    connSetPostponeUpdateState(c->conn, clientConnPostponeMaskFromIOState(c));
    /* Priority connections (replication, cluster, slot migration) keep a job
     * of their own on the high-priority queue; regular clients are staged in
     * the write slab below. */
    jobPriority qidx = getJobPriority(c);
    if (qidx == JOB_PRIORITY_HIGH) {
        void *job = tagJob(c, JOB_REQ_WRITE_CLIENT);
        if (unlikely(spmcEnqueue(&io_shared_inbox[qidx], job) == false)) {
            c->io_write_state = CLIENT_IDLE;
            connSetPostponeUpdateState(c->conn, 0);
            c->write_flags = 0;
            c->io_last_reply_block = NULL;
            c->io_last_bufpos = 0;
            return C_ERR;
        }
    }
    /* Force a new header so the main thread never extends a header the IO
     * thread will be reading once the job or slab is published. */
    if (!is_replica) {
        if (block) {
            if (block->flag.buf_encoded) block->last_header = NULL;
        } else {
            if (c->flag.buf_encoded) c->last_header = NULL;
        }
    }
    if (c->flag.pending_write) {
        listUnlinkNode(server.clients_pending_write, &c->clients_pending_write_node);
        c->flag.pending_write = 0;
    }
    if (qidx == JOB_PRIORITY_HIGH) {
        io_jobs_submitted++;
        server.stat_io_writes_pending++;
        return C_OK;
    }

    /* Lazy completion: a plain, unencoded buffer write of a partitioned client
     * has nothing for main to do on success but reset bufpos and account bytes,
     * and that can wait until main next touches the client. Such writes are
     * not counted as pending responses: none is expected. */
    int lazy = c->flag.partitioned && !is_replica && block == NULL && !c->flag.buf_encoded &&
               !c->flag.close_after_reply;
    if (lazy) {
        c->write_flags |= WRITE_FLAGS_LAZY;
        stageSlabEntry(c, SLAB_WRITE | SLAB_LAZY);
    } else {
        server.stat_io_writes_pending++;
        stageWriteClient(c);
    }
    return C_OK;
}

/* Try to offload a cluster link read to an I/O thread.
 * Enqueues a tagged job onto io_shared_inbox (SPMC queue).
 * Returns C_OK if offloaded or if a job is already pending (to prevent
 *   the caller from falling back to synchronous I/O on a connection
 *   with an in-flight worker job).
 * Returns C_ERR if fallback is needed (pool inactive or spmcEnqueue fails). */
int trySendClusterReadToIOThreads(struct clusterLink *link) {
    /* If any I/O job is already in flight for this link, return C_OK
     * so the caller does NOT fall back to synchronous I/O. */
    if (link->io_read_state != CLUSTER_LINK_IO_IDLE) return C_OK;
    if (link->io_write_state != CLUSTER_LINK_IO_IDLE) {
        link->io_read_deferred = 1;
        return C_OK;
    }
    link->io_read_deferred = 0;

    /* Invariant: io_refs must be 0 when both states are IDLE. */
    serverAssert(link->io_refs == 0);

    /* clusterReadHandler() drains any queued complete packets before
     * attempting a new dispatch. */
    serverAssert(link->io_complete_bytes == 0);
    serverAssert(link->io_complete_packets == 0);

    /* The connection is not established yet. See the equivalent guard in
     * trySendClusterWriteToIOThreads() for why we return C_OK here. */
    if (connGetState(link->conn) != CONN_STATE_CONNECTED) return C_OK;

    /* No I/O thread pool available — synchronous fallback. */
    if (server.active_io_threads_num <= 1) {
        server.stat_cluster_io_main_thread_fallbacks++;
        return C_ERR;
    }

    /* Postpone connection state updates while the I/O thread operates. */
    connSetPostponeUpdateState(link->conn, 1);

    /* Transition link to pending-read state. */
    link->io_read_state = CLUSTER_LINK_IO_PENDING;
    link->io_refs++;
    link->rcvbuf_alloc_at_dispatch = link->rcvbuf_alloc;

    /* Enqueue the read job. */
    if (unlikely(spmcEnqueue(&io_shared_inbox[JOB_PRIORITY_HIGH], tagJob(link, JOB_REQ_CLUSTER_READ)) == false)) {
        /* Rollback on enqueue failure. */
        link->io_read_state = CLUSTER_LINK_IO_IDLE;
        link->io_refs--;
        connSetPostponeUpdateState(link->conn, 0);
        server.stat_cluster_io_main_thread_fallbacks++;
        return C_ERR;
    }

    io_jobs_submitted++;
    cluster_io_pending_responses++;
    return C_OK;
}

/* Try to offload a cluster link write to an I/O thread.
 * Enqueues a tagged job onto io_shared_inbox after snapshotting the current
 * head offset and the last queue node visible to the worker. New messages
 * appended by clusterSendMessage during the write stay queued on the main
 * thread and are picked up by a later dispatch.
 * Returns C_OK if offloaded or if a job is already pending (to prevent
 *   the caller from falling back to synchronous I/O on a connection
 *   with an in-flight worker job).
 * Returns C_ERR if fallback is needed (pool inactive or spmcEnqueue fails). */
int trySendClusterWriteToIOThreads(struct clusterLink *link) {
    listNode *last_send_block;

    /* If any I/O job is already in flight for this link, return C_OK
     * so the caller does NOT fall back to synchronous I/O. */
    if (link->io_write_state != CLUSTER_LINK_IO_IDLE) return C_OK;
    if (link->io_read_state != CLUSTER_LINK_IO_IDLE) return C_OK;

    /* Invariant: io_refs must be 0 when both states are IDLE. */
    serverAssert(link->io_refs == 0);

    /* Nothing to write. */
    if (listLength(link->send_msg_queue) == 0) return C_OK;

    /* The connection is still being established (TCP connect or TLS handshake
     * in progress). Don't dispatch: connWrite() fails with a non-EAGAIN error
     * on a connection that isn't connected yet, the worker reports
     * CLUSTER_IO_WRITE_ERROR and the completion handler turns that into a link
     * teardown, so a slow handshake would kill the link. Nothing is stranded:
     * the caller installed the write handler and the connection layer drives it
     * once the handshake completes. Return C_OK so the caller neither retries
     * synchronously (which fails the same way, see clusterWriteHandler) nor
     * records a main-thread fallback. */
    if (connGetState(link->conn) != CONN_STATE_CONNECTED) return C_OK;

    /* No I/O thread pool available — synchronous fallback. */
    if (server.active_io_threads_num <= 1) {
        server.stat_cluster_io_main_thread_fallbacks++;
        return C_ERR;
    }

    /* Yield one dispatch to a read skipped earlier: WRITE_BARRIER fires writable
     * first, so a never-empty send queue would re-claim the link and never read. */
    if (link->io_read_deferred) {
        link->io_read_deferred = 0;
        return C_OK;
    }

    last_send_block = listLast(link->send_msg_queue);
    serverAssert(last_send_block != NULL);

    /* Postpone connection state updates while the I/O thread operates. */
    connSetPostponeUpdateState(link->conn, 1);

    /* Snapshot the canonical queue for one write job. */
    link->io_last_send_block = last_send_block;
    link->io_head_offset = link->head_msg_send_offset;
    link->io_nodes_sent = 0;

    /* Transition link to pending-write state. */
    link->io_write_state = CLUSTER_LINK_IO_PENDING;
    link->io_refs++;

    /* Enqueue the write job. */
    if (unlikely(spmcEnqueue(&io_shared_inbox[JOB_PRIORITY_HIGH], tagJob(link, JOB_REQ_CLUSTER_WRITE)) == false)) {
        link->io_write_state = CLUSTER_LINK_IO_IDLE;
        link->io_refs--;
        link->io_last_send_block = NULL;
        link->io_head_offset = 0;
        link->io_nodes_sent = 0;
        connSetPostponeUpdateState(link->conn, 0);
        server.stat_cluster_io_main_thread_fallbacks++;
        return C_ERR;
    }

    io_jobs_submitted++;
    cluster_io_pending_responses++;
    return C_OK;
}

/* Try to offload a cluster TLS accept to an I/O thread.
 * Called from clusterAcceptHandler BEFORE any clusterLink exists.
 * Returns C_OK if offloaded, C_ERR if fallback is needed. */
int trySendClusterAcceptToIOThreads(connection *conn) {
    if (!(conn->flags & CONN_FLAG_ALLOW_ACCEPT_OFFLOAD)) return C_ERR;
    /* A cluster accept job is already in flight for this connection. */
    if (conn->flags & CONN_FLAG_ACCEPT_OFFLOAD_PENDING) return C_OK;
    if (server.active_io_threads_num <= 1) {
        server.stat_cluster_io_main_thread_fallbacks++;
        return C_ERR;
    }

    conn->flags |= CONN_FLAG_ACCEPT_OFFLOAD_PENDING;
    connSetPostponeUpdateState(conn, 1);
    connIncrRefs(conn);

    if (unlikely(spmcEnqueue(&io_shared_inbox[JOB_PRIORITY_HIGH], tagJob(conn, JOB_REQ_CLUSTER_ACCEPT)) == false)) {
        connDecrRefs(conn);
        connSetPostponeUpdateState(conn, 0);
        conn->flags &= ~CONN_FLAG_ACCEPT_OFFLOAD_PENDING;
        server.stat_cluster_io_main_thread_fallbacks++;
        return C_ERR;
    }

    io_jobs_submitted++;
    cluster_io_pending_responses++;
    return C_OK;
}

/* Internal function to free the client's argv in an IO thread. */
void ioThreadFreeArgv(robj **argv) {
    int last_arg = 0;
    for (int i = 0;; i++) {
        robj *o = argv[i];
        if (o == NULL) {
            continue;
        }

        /* The main-thread set the refcount to 0 to indicate that this is the last argument to free */
        if (objectGetRefcount(o) == 0) {
            last_arg = 1;
            o->refcount = 1;
        }

        decrRefCount(o);

        if (last_arg) {
            break;
        }
    }

    zfree(argv);
}

/* This function attempts to offload the client's argv to an IO thread.
 * Returns C_OK if the client's argv were successfully offloaded to an IO thread,
 * C_ERR otherwise. */
int tryOffloadFreeArgvToIOThreads(client *c, int argc, robj **argv) {
    if (server.active_io_threads_num <= 1 || argc == 0) {
        return C_ERR;
    }

    int target_id = c->cur_tid;
    if (target_id < 1 || target_id >= server.active_io_threads_num) {
        target_id = (c->id % (server.active_io_threads_num - 1)) + 1;
    }

    if (spscIsFull(&io_private_inbox[target_id])) {
        return C_ERR;
    }

    int last_arg_to_free = -1;

    /* Prepare the argv */
    for (int j = 0; j < argc; j++) {
        if (argv[j]->refcount > 1) {
            decrRefCount(argv[j]);
            /* Set argv[j] to NULL to avoid double free */
            argv[j] = NULL;
        } else {
            last_arg_to_free = j;
        }
    }

    /* If no argv to free, free the argv array at the main thread */
    if (last_arg_to_free == -1) {
        zfree(argv);
        return C_OK;
    }

    /* We set the refcount of the last arg to free to 0 to indicate that
     * this is the last argument to free. With this approach, we don't need to
     * send the argc to the IO thread and we can send just the argv ptr. */
    argv[last_arg_to_free]->refcount = 0;
    void *job = tagJob(argv, JOB_SPSC_FREE_ARGV);
    /* We pass false to enqueue the job without committing the queue index immediately.
     * This allows us to batch multiple free jobs together and
     * commit them in a single operation later in the event loop. This reduces the overhead
     * of memory barriers and cache line bouncing associated
     * with updating the queue's write pointer per job. */
    spscEnqueue(&io_private_inbox[target_id], job, false);
    io_jobs_submitted++;

    return C_OK;
}

/* This function attempts to offload the free of an object to an IO thread.
 * Returns C_OK if the object was successfully offloaded to an IO thread,
 * C_ERR otherwise.
 *
 * Off-main safety: the IO thread runs a bare decrRefCount, which for a
 * sole-reference (refcount == 1) object is exactly the same teardown that
 * lazyfree's bio thread already performs today (see lazyfreeFreeObject). Any
 * object shape lazyfree already frees off the main thread is therefore proven
 * safe to free on an IO thread. We admit:
 *   - OBJ_STRING RAW: leaf sdsfree + zfree (the original v1 shape).
 *   - OBJ_LIST / OBJ_SET / OBJ_ZSET / OBJ_HASH, all encodings: pure
 *     object-graph teardown. No shared server/db state is touched inside the
 *     free. In particular a hash with field TTLs is un-tracked from the
 *     db-level keys_with_volatile_items kvstore on the main thread in
 *     dbGenericDeleteWithDictIndex BEFORE this call; freeHashObject then only
 *     releases the per-object volatile-set bucket (vsetRelease) and the
 *     hashtable, both object-local.
 * We deliberately exclude:
 *   - OBJ_MODULE: the value free runs a module VM_Free callback which may call
 *     back into the module API and must stay on the main thread.
 *   - OBJ_STREAM: rax + consumer-group / PEL teardown, not audited here.
 * These fall through to lazyfree / inline decref unchanged.
 *
 * Effort gate: strings gate on payload size (io-threads-free-min-size);
 * aggregates gate on lazyfreeGetFreeEffort() (io-threads-free-min-effort) so
 * only aggregates expensive enough to matter leave the main thread. A small
 * listpack/intset aggregate reports effort 1 and frees inline, just like a
 * short string. */
int tryOffloadFreeObjToIOThreads(robj *obj) {
    if (server.active_io_threads_num <= 1) {
        return C_ERR;
    }

    /* refcount > 1 is NOT a terminal free: decrRefCount merely decrements the
     * shared counter and does not free. That is cheap and safe on main, so we
     * decline here and let the caller decrRefCount inline (no free happens). */
    if (obj->refcount > 1) return C_ERR;

    switch (obj->type) {
    case OBJ_STRING:
        /* All string encodings are leaf frees safe off-main:
         *   RAW    -> sdsfree(payload) + zfree(robj)
         *   EMBSTR -> single zfree of the combined robj+sds allocation
         *   INT    -> single zfree of the robj (shared ints have refcount
         *             OBJ_SHARED_REFCOUNT > 1 and were already declined above)
         * W5b removes the io-threads-free-min-size gate: per the never-free
         * directive even tiny strings must leave the main thread. */
        break;
    case OBJ_LIST:
    case OBJ_SET:
    case OBJ_ZSET:
    case OBJ_HASH:
        /* Audited off-main-safe aggregates (see W4b). W5b removes the
         * io-threads-free-min-effort gate: every aggregate, including small
         * listpack/intset encodings, is offloaded rather than freed inline. */
        break;
    default:
        /* OBJ_MODULE (VM_Free callback) and OBJ_STREAM (unaudited rax/cgroup
         * teardown) cannot run on an IO thread. The caller routes these to bio
         * lazyfree, which is also off-main (see freeValueNeverOnMain). */
        return C_ERR;
    }

    /* Count the bytes now committed to an off-main free (decremented by the
     * IO thread that drains the slab). */
    offloadFreeAccountAdd(offloadObjFreeBytes(obj));
    slabAppendFree(obj, 1);
    server.stat_io_freed_objects++;
    return C_OK;
}

/* Offload the free of a raw heap buffer (reply block, argv array, decoded
 * scratch buffer) to an IO thread. zfree is thread-safe. On a full queue the
 * pointer is parked on the pending list (never freed inline). Returns C_OK when
 * handled off-main, C_ERR only when IO threads are disabled. */
int tryOffloadFreePtrToIOThreads(void *ptr) {
    if (ptr == NULL) return C_OK;
    if (inline_reclaim_depth) return C_ERR; /* emergency reclaim frees inline */
    if (server.active_io_threads_num <= 1) return C_ERR;

    offloadFreeAccountAdd(zmalloc_size(ptr));
    slabAppendFree(ptr, 0);
    return C_OK;
}

/* Route a terminal value free off the main thread, honoring the never-free
 * directive for every shape:
 *   - strings + audited aggregates -> IO thread (or pending list if full);
 *   - module / stream / (IO threads disabled) -> bio lazyfree, unconditionally
 *     (bio is not the main thread, so the directive is satisfied).
 * refcount > 1 objects are non-terminal and are decremented inline (no free).
 * key/dbid are forwarded to bio for the lazyfree effort estimate. */
void freeValueNeverOnMain(robj *key, robj *val, int dbid) {
    if (inline_reclaim_depth) { /* emergency reclaim frees inline */
        if (val) decrRefCount(val);
        return;
    }
    if (val->refcount > 1) {
        /* Non-terminal: just drops a reference, does not free. */
        decrRefCount(val);
        return;
    }

    if (server.active_io_threads_num > 1) {
        /* Offload substrate is live: enforce never-free-on-main. */
        armNoMainThreadFree();
        if (tryOffloadFreeObjToIOThreads(val) == C_OK) {
            disarmNoMainThreadFree();
            return;
        }
        /* Declined by the IO path (module / stream): route to bio,
         * unconditionally, so the free never lands on the main thread. */
        freeObjAsyncForce(val);
        disarmNoMainThreadFree();
        return;
    }

    /* DOCUMENTED EXCEPTION: IO threads are disabled (io-threads <= 1). There is
     * no cheap off-main worker for the common flat-object free, so forcing
     * every free onto bio would be a severe regression on the default single-
     * threaded configuration. We fall back to the pre-W5b behavior (bio for
     * high-effort objects via freeObjAsync, inline for the rest). The
     * never-free-on-main guarantee is only asserted with io-threads >= 2. */
    freeObjAsync(key, val, dbid);
}

/* Flush the accumulating slab and re-attempt parked slabs, from beforeSleep.
 * With IO threads live nothing is freed synchronously: slabs the ring still
 * rejects stay parked for the next cycle. With IO threads disabled, entries
 * settle to bio (objects) or the one documented inline zfree (raw buffers), so
 * the parked set cannot grow unbounded after io-threads is turned off. */
void drainPendingMainFrees(void) {
    /* A partial slab goes out when it is reasonably full or a millisecond
     * old: one job per event-loop iteration would be thousands of tiny slabs
     * per second for the IO threads to pick up one by one. */
    if (cur_free_slab && (cur_free_slab->count >= 256 || cur_free_slab_ms != server.mstime)) {
        freeSlab *s = cur_free_slab;
        cur_free_slab = NULL;
        submitFreeSlab(s);
    }
    if (pending_free_slabs_len == 0) return;

    if (server.active_io_threads_num > 1) {
        size_t kept = 0;
        for (size_t i = 0; i < pending_free_slabs_len; i++) {
            freeSlab *s = pending_free_slabs[i];
            if (!submitSlabJob(s, JOB_SPSC_FREE_SLAB)) pending_free_slabs[kept++] = s;
        }
        pending_free_slabs_len = kept;
        return;
    }

    /* No IO threads: settle every parked entry off-main where possible. These
     * bytes were counted at append; the IO-thread drain will not run, so the
     * accounting is settled here. */
    for (size_t i = 0; i < pending_free_slabs_len; i++) {
        freeSlab *s = pending_free_slabs[i];
        for (size_t j = 0; j < s->count; j++) {
            uintptr_t e = (uintptr_t)s->entries[j];
            void *ptr = (void *)(e & ~FREE_ENTRY_OBJ_BIT);
            if (e & FREE_ENTRY_OBJ_BIT) {
                robj *o = ptr;
                offloadFreeAccountSub(offloadObjFreeBytes(o));
                if (o->refcount == 1) {
                    freeObjAsyncForce(o);
                } else {
                    decrRefCount(o);
                }
            } else {
                /* Raw buffer with no off-main path available: bio has no
                 * generic zfree job, so this is the one documented residual
                 * inline free, reached only when IO threads are disabled. */
                offloadFreeAccountSub(zmalloc_size(ptr));
                zfree(ptr);
            }
        }
        zfree(s);
    }
    pending_free_slabs_len = 0;
}

/* This function retrieves the results of the IO Thread poll.
 * returns the number of fired events if the IO thread has finished processing poll events, 0 otherwise. */
static int getIOThreadPollResults(aeEventLoop *eventLoop) {
    int io_state;
    io_state = atomic_load_explicit(&server.io_poll_state, memory_order_acquire);
    if (io_state == AE_IO_STATE_POLL) {
        /* IO thread is still processing poll events. */
        return 0;
    }

    /* IO thread is done processing poll events. */
    serverAssert(io_state == AE_IO_STATE_DONE);
    server.stat_poll_processed_by_io_threads++;
    server.io_poll_state = AE_IO_STATE_NONE;

    /* Remove the custom poll proc. */
    aeSetCustomPollProc(eventLoop, NULL);
    aeSetPollProtect(eventLoop, 0);
    return server.io_ae_fired_events;
}

void trySendPollJobToIOThreads(void) {
    if (server.active_io_threads_num <= 1) {
        return;
    }

    /* If there are no pending jobs, let the main thread do the poll-wait by itself. */
    if (getPendingIOResponsesCount() == 0) {
        return;
    }

    /* If the IO thread is already processing poll events, don't send another job. */
    if (server.io_poll_state != AE_IO_STATE_NONE) {
        return;
    }

    server.io_poll_state = AE_IO_STATE_POLL;
    aeSetPollProtect(server.el, 1);

    /* Use SPMC to minimize polling overhead. At high thread counts, use private SPSC queues for lower latency. */
    if (server.active_io_threads_num <= 9) {
        if (unlikely(spmcEnqueue(&io_shared_inbox[JOB_PRIORITY_NORMAL], tagJob(server.el, JOB_REQ_POLL)) == false)) {
            server.io_poll_state = AE_IO_STATE_NONE;
            aeSetPollProtect(server.el, 0);
            return;
        }
    } else {
        cur_epoll_thread = ((cur_epoll_thread) % (server.active_io_threads_num - 1)) + 1;
        if (unlikely(spscIsFull(&io_private_inbox[cur_epoll_thread]))) {
            server.io_poll_state = AE_IO_STATE_NONE;
            aeSetPollProtect(server.el, 0);
            return;
        }
        spscEnqueue(&io_private_inbox[cur_epoll_thread], tagJob(server.el, JOB_SPSC_POLL), true);
    }

    aeSetCustomPollProc(server.el, getIOThreadPollResults);
    io_jobs_submitted++;
}

void sendToMainThread(void *data, int type) {
    jobPriority qidx = JOB_PRIORITY_NORMAL;
    if (type == JOB_RES_READ_CLIENT || type == JOB_RES_WRITE_CLIENT) {
        client *c = (client *)data;
        qidx = getJobPriority(c);
    } else if (type == JOB_RES_CLUSTER_READ || type == JOB_RES_CLUSTER_WRITE || type == JOB_RES_CLUSTER_ACCEPT) {
        qidx = JOB_PRIORITY_HIGH;
    }
    if (unlikely(pending_io_responses[qidx])) {
        flushPendingIOResponsesList(&pending_io_responses[qidx], &io_shared_outbox[qidx], &io_thread_ticket[qidx], 0);
    }
    void *job = tagJob(data, type);
    if (unlikely(pending_io_responses[qidx] || !mpscEnqueue(&io_shared_outbox[qidx], job, &io_thread_ticket[qidx]))) {
        if (pending_io_responses[qidx] == NULL) {
            pending_io_responses[qidx] = listCreate();
        }
        listAddNodeTail(pending_io_responses[qidx], job);
    }
}

static void ioThreadAccept(client *c) {
    connAccept(c->conn, NULL);
    atomic_thread_fence(memory_order_release);
    c->io_read_state = CLIENT_COMPLETED_IO;
    sendToMainThread(c, JOB_RES_READ_CLIENT);
}

/*
 * Attempts to offload an Accept operation (currently used for TLS accept) for a client
 * connection to I/O threads.
 *
 * Returns:
 *   C_OK  - If the accept operation was successfully queued for processing
 *   C_ERR - If the connection is not eligible for offloading
 *
 * Parameters:
 *   conn - The connection object to perform the accept operation on
 */
int trySendAcceptToIOThreads(connection *conn) {
    if (server.io_threads_num <= 1) {
        return C_ERR;
    }

    if (!(conn->flags & CONN_FLAG_ALLOW_ACCEPT_OFFLOAD)) {
        return C_ERR;
    }

    /* Cluster TLS accepts have no client private-data yet. Route them to the
     * dedicated cluster accept offload path. */
    if (connGetOwnerKind(conn) == CONN_OWNER_CLUSTER_LINK) {
        return trySendClusterAcceptToIOThreads(conn);
    }

    client *c = connGetPrivateData(conn);
    serverAssert(c != NULL);
    if (c->io_read_state != CLIENT_IDLE) {
        return C_OK;
    }

    if (server.active_io_threads_num <= 1) {
        return C_ERR;
    }

    c->io_read_state = CLIENT_PENDING_IO;
    c->flag.pending_read = 1;
    connSetPostponeUpdateState(c->conn, clientConnPostponeMaskFromIOState(c));

    void *job = tagJob(c, JOB_REQ_ACCEPT);
    if (unlikely(spmcEnqueue(&io_shared_inbox[JOB_PRIORITY_NORMAL], job) == false)) {
        c->io_read_state = CLIENT_IDLE;
        c->flag.pending_read = 0;
        connSetPostponeUpdateState(c->conn, 0);
        return C_ERR;
    }

    server.stat_io_reads_pending++;
    server.stat_io_accept_offloaded++;
    io_jobs_submitted++;
    return C_OK;
}

/* Function to handle read jobs */
static void handleReadJobs(client **read_jobs, int read_count) {
    server.stat_io_reads_pending -= read_count;
    serverAssert(server.stat_io_reads_pending >= 0);
    uint64_t read_client_ids[JOB_BATCH_SIZE];
    int id_count = 0;
    /* process each client */
    for (int i = 0; i < read_count; i++) {
        client *c = read_jobs[i];
        uint64_t id = c->id;
        if (processClientIOReadsDone(c)) read_client_ids[id_count++] = id;
    }

    /* Process commands in batch if we processed any reads */
    if (read_count) {
        server.stat_io_reads_processed += read_count;
        processClientsCommandsBatch();

        /* Resume transport after draining pending input and command queue. */
        for (int i = 0; i < id_count; i++) {
            client *c = lookupClientByID(read_client_ids[i]);
            if (!c || !c->conn) continue;

            if (processPendingCommandAndInputBuffer(c) == C_ERR) continue;
            beforeNextClient(c);

            c = lookupClientByID(read_client_ids[i]);
            if (!c || !c->conn) continue;
            connSetPostponeUpdateState(c->conn, clientConnPostponeMask(c));
            connUpdateState(c->conn);
        }
    }
}

/* Function to handle write jobs */
static void handleWriteJobs(client **write_jobs, int write_count) {
    server.stat_io_writes_pending -= write_count;
    serverAssert(server.stat_io_writes_pending >= 0);

    for (int i = 0; i < write_count; i++) {
        client *c = write_jobs[i];
        server.stat_io_writes_processed++;
        processClientIOWriteDone(c);
    }
}

static int processOutboxBatch(mpscQueue *outbox) {
    void *jobs[JOB_BATCH_SIZE];
    client *read_jobs[JOB_BATCH_SIZE];
    client *write_jobs[JOB_BATCH_SIZE];
    int received_responses = 0;
    int read_count = 0;
    int write_count = 0;

    /* Try to dequeue JOB_BATCH_SIZE */
    while (received_responses < JOB_BATCH_SIZE) {
        int dequeued_count = mpscDequeueBatch(outbox, jobs, JOB_BATCH_SIZE - received_responses);

        /* Stop if we can't get more jobs from the queue. */
        if (dequeued_count == 0) break;

        received_responses += dequeued_count;

        for (int i = 0; i < dequeued_count; i++) {
            void *data;
            int job_type;
            untagJob(jobs[i], &data, &job_type);
            if (job_type == JOB_RES_READ_CLIENT) {
                client *c = (client *)data;
                serverAssert(c->io_read_state == CLIENT_COMPLETED_IO);
                read_jobs[read_count++] = c;
            } else if (job_type == JOB_RES_WRITE_CLIENT) {
                client *c = (client *)data;
                serverAssert(c->io_write_state == CLIENT_COMPLETED_IO);
                write_jobs[write_count++] = c;
            } else if (job_type == JOB_RES_FP_CLOSE || job_type == JOB_RES_FP_HANDOFF) {
                fastpathHandoffDone((client *)data, job_type == JOB_RES_FP_CLOSE);
            } else if (job_type == JOB_RES_WRITE_SLAB) {
                /* A whole slab came back as one response. Flush any per-client
                 * writes collected so far first to keep completion order, then
                 * reconcile the written clients in place; re-arm entries need
                 * nothing from the main thread. */
                if (write_count) {
                    handleWriteJobs(write_jobs, write_count);
                    write_count = 0;
                }
                writeSlab *slab = (writeSlab *)data;
                size_t written = 0;
                for (size_t j = 0; j < slab->count; j++) {
                    uintptr_t e = slab->entries[j];
                    if (!(e & SLAB_WRITE)) continue;
                    client *wc = (client *)(e & ~SLAB_TAGS);
                    if (e & SLAB_LAZY) {
                        /* Only the entries the IO thread flagged need us; the
                         * others reconcile lazily. A flagged one had its lazy
                         * flag dropped before its state was published, so it is
                         * still COMPLETED here and was never counted as pending. */
                        if (e & SLAB_NOTIFY) {
                            serverAssert(wc->io_write_state == CLIENT_COMPLETED_IO);
                            server.stat_io_writes_processed++;
                            processClientIOWriteDone(wc);
                        }
                        continue;
                    }
                    serverAssert(wc->io_write_state == CLIENT_COMPLETED_IO);
                    slab->entries[written++] = (uintptr_t)wc;
                }
                if (written) handleWriteJobs((client **)slab->entries, (int)written);
                recycleWriteSlab(slab);
            } else if (job_type == JOB_RES_CLUSTER_READ) {
                serverAssert(cluster_io_pending_responses > 0);
                cluster_io_pending_responses--;
                server.stat_cluster_threaded_reads_processed++;
                clusterHandleReadCompletion((struct clusterLink *)data);
            } else if (job_type == JOB_RES_CLUSTER_WRITE) {
                serverAssert(cluster_io_pending_responses > 0);
                cluster_io_pending_responses--;
                server.stat_cluster_threaded_writes_processed++;
                clusterHandleWriteCompletion((struct clusterLink *)data);
            } else if (job_type == JOB_RES_CLUSTER_ACCEPT) {
                serverAssert(cluster_io_pending_responses > 0);
                cluster_io_pending_responses--;
                server.stat_cluster_threaded_accepts_processed++;
                clusterHandleAcceptCompletion((connection *)data);
            } else {
                serverPanic("Unknown job type %d", job_type);
            }
        }
    }

    if (read_count) handleReadJobs(read_jobs, read_count);
    if (write_count) handleWriteJobs(write_jobs, write_count);
    return received_responses;
}

/* Process completed IO jobs from worker threads back onto the main thread.
 * Drains the high-priority outbox first to guarantee control-plane responsiveness,
 * and performs periodic preemptive polling of QoS events while consuming normal jobs.
 *
 * Fast-path batches come first: they are not counted as pending responses.
 * Partitioned clients re-arm themselves on IO threads, so under load the
 * normal outbox refills as fast as it drains; one call handles at most
 * IO_RESPONSE_BATCHES_PER_CALL batches and the rest waits for the next
 * iteration, which follows immediately since responses are still pending. */
int processIOThreadsResponses(void) {
    /* We don't check for threads number since some threads may return jobs then deactivate/shut-down */

    int fp_processed = fastpathDrain();

    /* Quick check if any pending operations exist across any priority level.
     * Fast-path clients report closes and hand-offs through the outbox without
     * being counted, so the outbox is drained whenever any exist. */
    if (getPendingIOResponsesCount() == 0 && fastpathClientCount() == 0) return fp_processed;

    int total_processed = fp_processed + processCommandRing();
    int batches = 0;
    while (batches++ < IO_RESPONSE_BATCHES_PER_CALL) {
        /* 1. Strict Priority: First, drain high-priority events (cluster bus, replication, and slot migration jobs) */
        int processed = processOutboxBatch(&io_shared_outbox[JOB_PRIORITY_HIGH]);
        if (processed == 0) {
            /* 2. Preemptive Poll: When high-priority outbox is empty, check if any new
             * high-priority events arrived on QoS channels before processing normal traffic. */
            aeProcessQoSEventsPreemptively(server.el);
        }
        /* 3. Drain normal client events */
        processed += processOutboxBatch(&io_shared_outbox[JOB_PRIORITY_NORMAL]);
        total_processed += processed;
        if (processed == 0) break;
    }
    /* Write-done handling may have re-staged clients with more output. */
    flushWriteSlab();
    return total_processed;
}
