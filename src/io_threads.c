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
/* Bound response draining so main returns to the event loop under load. */
#define IO_RESPONSE_BATCHES_PER_CALL 256
/* Cap on partitioned read completions handled per call, for the same reason. */
#define IO_PARTITION_COMPLETIONS_PER_CALL 4096

/* io_epoll_seq lets main wait out stale references from the last epoll pass. */
static int io_epfd[IO_THREADS_MAX_NUM];
int ioThreadEpollFd(int tid) {
    return io_epfd[tid];
}
static _Atomic uint64_t io_epoll_seq[IO_THREADS_MAX_NUM];
static unsigned partition_rr = 0;      /* main-thread only */
static size_t partitioned_clients = 0; /* main-thread only */

/* Worker lifecycle: main stores RUNNING, QUIESCING and STOPPING; the thread stores STOPPED as its last act. */
typedef enum {
    IO_WORKER_ABSENT = 0,
    IO_WORKER_RUNNING,
    IO_WORKER_QUIESCING, /* takes no new client or job; hands back what it owns */
    IO_WORKER_STOPPING,  /* main saw it drained and owns nothing of it; the thread exits at its next loop boundary */
    IO_WORKER_STOPPED,
} ioWorkerLifecycle;
static _Atomic int io_worker_state[IO_THREADS_MAX_NUM];
static int io_worker_parked[IO_THREADS_MAX_NUM];       /* main only: main holds the thread's mutex */
static int io_worker_hwm = 1;                          /* main only: 1 + highest slot holding state */
static int io_ready_num = 1;                           /* main only: 1 + contiguous RUNNING slots; bounds activation, routing and admission */
static int io_converging = 0;                          /* main only: live workers differ from server.io_threads_num */
static int io_scale_base = 1;                          /* main only: worker count a failed deferred scale-up falls back to */
static list *io_partition_clients[IO_THREADS_MAX_NUM]; /* main only: partitioned clients watched by each thread */
static _Atomic int io_exit_abort = 0;                  /* process exit: threads stop where they are, nothing is handed back */
static int io_debug_fail_create = 0;                   /* main only: DEBUG injects one thread creation failure for this slot */

int ioThreadsReadyNum(void) {
    return io_ready_num;
}

static inline int ioWorkerState(int tid) {
    return atomic_load_explicit(&io_worker_state[tid], memory_order_acquire);
}

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

/* The parked flag is the only record of who holds a thread's mutex. */
static void ioWorkerPark(int tid) {
    if (io_worker_parked[tid]) return;
    pthread_mutex_lock(&io_threads_mutex[tid]);
    io_worker_parked[tid] = 1;
}

static void ioWorkerUnpark(int tid) {
    if (!io_worker_parked[tid]) return;
    pthread_mutex_unlock(&io_threads_mutex[tid]);
    io_worker_parked[tid] = 0;
}
static int cur_epoll_thread = 0;
// Main -> IO: Shared Queue (Single Producer Multi Consumer) where all IO threads pull jobs from
static spmcQueue io_shared_inbox[JOB_PRIORITY_COUNT] = {0};
// IO -> Main: Response Channel (Multi Producer Single Consumer) used by IO threads to send results back to main-thread
static mpscQueue io_shared_outbox[JOB_PRIORITY_COUNT] = {0};
// Main -> IO (Thread-Specific) for tasks that must run on specific IO thread where IO threads check their private inbox before the shared queue
static spscQueue io_private_inbox[IO_THREADS_MAX_NUM] = {0};
/* Each IO thread publishes parsed commands and one read epilogue to its SPSC ring. */
static spscQueue io_cmd_ring[IO_THREADS_MAX_NUM] = {0};
/* A full command ring falls back to the per-client completion path. */
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

/* Full IO queues park terminal frees rather than falling back to main. */

/* Main batches terminal frees into tagged slabs drained by IO threads. */
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

/* Pending-free bytes leave maxmemory pressure until physical reclamation completes. */
static _Atomic size_t offload_pending_free_bytes = 0;

/* Estimates never exceed physical frees, so maxmemory cannot under-evict. */
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

/* Client eviction reclaims inline so its next pressure check sees the freed bytes. */
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
/* A fired debug guard means a routed terminal free fell back to main. */
static _Thread_local int never_free_armed = 0;
void armNoMainThreadFree(void) { never_free_armed = 1; }
void disarmNoMainThreadFree(void) { never_free_armed = 0; }
int noMainThreadFreeArmed(void) { return never_free_armed; }
#else
void armNoMainThreadFree(void) {}
void disarmNoMainThreadFree(void) {}
int noMainThreadFreeArmed(void) { return 0; }
#endif

/* Full private inboxes park whole free slabs for the next beforeSleep pass. */
/* Free slabs use private SPSC inboxes because the shared job tag space is full. */
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

/* Slab publication releases client fences and IO state before tagged write/rearm work becomes visible. */
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

/* Failed slab submission restores writes and rearms to their retry paths. */
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
    /* One entry preserves write-before-rearm ordering for the same client. */
    if (cur_write_slab->count > 0 &&
        (cur_write_slab->entries[cur_write_slab->count - 1] & ~SLAB_TAGS) == (uintptr_t)c) {
        cur_write_slab->entries[cur_write_slab->count - 1] |= tag;
        return;
    }
    cur_write_slab->entries[cur_write_slab->count++] = (uintptr_t)c | tag;
    /* Early flush keeps IO completions concurrent with main-thread work. */
    if (cur_write_slab->count >= SLAB_FLUSH_THRESHOLD) flushWriteSlab();
}

static void stageWriteClient(client *c) {
    stageSlabEntry(c, SLAB_WRITE);
}

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

/* One-shot readiness serializes IO-thread reads against main-thread command drains. */
int tryPartitionClient(client *c) {
    if (!clientIsPartitionable(c)) return C_ERR;
    if (io_ready_num < 2) return C_ERR;
    int tid = 1 + (int)(partition_rr++ % (unsigned)(io_ready_num - 1));
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
    listInitNode(&c->io_owner_node, c);
    listLinkNodeTail(io_partition_clients[tid], &c->io_owner_node);
    return C_OK;
}

/* Main is the only party linking partitioned clients, so it also unlinks them. */
static void partitionRegistryRemove(client *c) {
    listUnlinkNode(io_partition_clients[c->io_tid], &c->io_owner_node);
    c->flag.partitioned = 0;
    c->io_tid = 0;
    partitioned_clients--;
}

void armPartitionedClientRead(client *c) {
    if (!c->flag.partitioned) return;
    if (c->flag.pending_read) return; /* armed, or a read is in flight */
    if (c->io_read_state != CLIENT_IDLE) return;
    if (c->flag.close_asap || c->flag.protected || c->flag.close_after_reply) return;
    if (c->flag.unblocked) return; /* main is about to resume it and re-arms then */
    if (!c->flag.blocked) {
        /* Blocked clients keep reading without parsing so partial commands can complete. */
        if (c->cmd_queue.off < c->cmd_queue.len) return; /* commands still to execute */
        if (c->flag.pending_command) return;            /* a complete command still in argv */
    }
    if (server.active_io_threads_num <= 1) return;   /* threads mid-scale; clientsCron retries */
    setClientReadFlagsForOffload(c);
    c->flag.pending_read = 1;
    server.stat_io_reads_pending++;
    /* CLIENT_ARMING_IO prevents teardown before the IO thread touches the socket. */
    c->io_read_state = CLIENT_ARMING_IO;
    stageSlabEntry(c, SLAB_REARM);
}

/* Wait out any epoll event array that may still reference the client. */
static void waitPartitionPass(int tid) {
    if (tid <= 0 || io_threads[tid] == 0) return;
    if (io_worker_parked[tid]) return; /* a parked thread is between passes */
    int st = ioWorkerState(tid);
    if (st != IO_WORKER_RUNNING && st != IO_WORKER_QUIESCING) return;
    uint64_t seq = atomic_load_explicit(&io_epoll_seq[tid], memory_order_acquire);
    while (atomic_load_explicit(&io_epoll_seq[tid], memory_order_acquire) == seq) {
        if (io_threads[tid] == 0) break;
    }
}

/* A read in flight keeps teardown asynchronous until its completion lands. */
void partitionedClientDetach(client *c) {
    if (!c->flag.partitioned) return;
    int tid = c->io_tid;
    if (io_epfd[tid] > 0 && c->conn) epoll_ctl(io_epfd[tid], EPOLL_CTL_DEL, c->conn->fd, NULL);
    /* Publish a staged rearm before claiming the client. */
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
    partitionRegistryRemove(c);
}

void unpartitionClient(client *c) {
    if (!c->flag.partitioned) return;
    int tid = c->io_tid;
    if (io_epfd[tid] > 0 && c->conn) epoll_ctl(io_epfd[tid], EPOLL_CTL_DEL, c->conn->fd, NULL);
    if (c->io_read_state == CLIENT_ARMING_IO) flushWriteSlab();
    waitPartitionPass(tid);
    while (c->io_read_state == CLIENT_ARMING_IO || c->io_read_state == CLIENT_PENDING_IO)
        atomic_thread_fence(memory_order_acquire);
    if (c->flag.pending_read && c->io_read_state == CLIENT_IDLE) {
        c->flag.pending_read = 0;
        server.stat_io_reads_pending--;
    }
    partitionRegistryRemove(c);
    if (c->conn && !c->flag.close_asap) connSetReadHandler(c->conn, readQueryFromClient);
}

/* Moves the sockets one thread watches to a remaining thread, or to the main event loop. */
static void unpartitionWorkerClients(int tid) {
    list *l = io_partition_clients[tid];
    if (!l) return;
    while (listLength(l) > 0) {
        client *c = listNodeValue(listFirst(l));
        unpartitionClient(c);
        if (c->io_read_state != CLIENT_IDLE || c->flag.pending_read || c->flag.close_asap) continue;
        if (c->cmd_queue.off < c->cmd_queue.len || c->flag.pending_command) continue;
        if (tryPartitionClient(c) == C_OK) connSetReadHandler(c->conn, NULL);
    }
}

void unpartitionAllClients(void) {
    if (partitioned_clients == 0) return;
    for (int tid = 1; tid < io_worker_hwm; tid++) unpartitionWorkerClients(tid);
}

/* Holding the socket prevents clientsCron from racing reads; release restores consumed readiness. */
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

/* One-shot readiness keeps a partitioned client idle until main rearms it. */
static int ioThreadPollPartition(int id) {
    /* Empty polls back off briefly while inbox work continues. */
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

/* Clean reads publish commands then RING_END; ring pressure falls back to per-client completion. */
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

static void accountRingRead(client *c) {
    server.stat_total_reads_processed++;
    c->last_interaction = server.unixtime;
    c->net_input_bytes += c->nread;
    server.stat_net_input_bytes += c->nread;
}

/* RING_END releases read ownership after every ringed command has executed. */
static void ringReadEnd(client *c) {
    serverAssert(c->io_read_state == CLIENT_COMPLETED_IO);
    server.stat_io_reads_pending--;
    server.stat_io_reads_processed++;
    if (c->flag.close_after_reply) {

        c->flag.pending_read = 0;
        c->io_read_state = CLIENT_IDLE;
        if (c->flag.protected) return;
        accountRingRead(c);
        beforeNextClient(c); /* frees a client marked close_asap, as the per-client path does */
        return;
    }

    if (c->argc > 0 || c->nread <= 0 || !(c->read_flags & READ_FLAGS_PARSING_COMPLETED) ||
        (c->read_flags & (READ_FLAGS_DONT_PARSE | READ_FLAGS_QB_LIMIT_REACHED | READ_FLAGS_ERROR_MASK))) {
        processClientIOReadsDone(c);
        return;
    }
    c->flag.pending_read = 0;
    c->io_read_state = CLIENT_IDLE;
    if (c->flag.protected) return;
    accountRingRead(c);

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

/* Command-ring drains stop at the prefetch budget or RING_BATCH. */
#define RING_CHUNK 8
static size_t ringAddChunkToPrefetch(uintptr_t *ents, size_t from, size_t to, getKeysResult *result, int *room) {
    /* prefetch-ring-stride trades key extraction cost for cache-miss coverage. */
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

static void ringPrefetchClients(uintptr_t *ents, size_t from, size_t to) {
    for (size_t i = from; i < to; i++) {
        client *c = (client *)(ents[i] & ~RING_TAGS);
        valkey_prefetch(c);
        valkey_prefetch(&c->cmd_queue);
        valkey_prefetch((const void *)&c->io_read_state);
    }
}

/* Thin ring backlogs wait briefly to amortize main-loop fixed costs. */
static void ringCoalesce(void) {
    int budget_us = server.io_ring_coalesce_us;
    if (budget_us <= 0 || partitioned_clients == 0) return;
    size_t backlog = 0;
    for (int t = 1; t < io_worker_hwm; t++)
        if (io_cmd_ring[t].buffer) backlog += spscBacklog(&io_cmd_ring[t]);
    if (backlog == 0 || backlog >= RING_BATCH / 2) return;
    monotime deadline = getMonotonicUs() + budget_us;
    do {
        for (int i = 0; i < 64; i++) cpuRelax();
        backlog = 0;
        for (int t = 1; t < io_worker_hwm; t++)
            if (io_cmd_ring[t].buffer) backlog += spscBacklog(&io_cmd_ring[t]);
        if (backlog >= RING_BATCH / 2) return;
    } while (getMonotonicUs() < deadline);
}

/* Drains one thread's ring up to RING_BATCH entries; returns how many it executed. */
static int processCommandRingOne(int t, int use_prefetch) {
    uintptr_t ents[RING_BATCH];
    spscQueue *q = &io_cmd_ring[t];
    if (q->buffer == NULL) return 0;
    size_t n = 0;
    if (use_prefetch) {
        /* Prefetch client state before extracting keys from each chunk. */
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
        prefetchBatchReset();
    } else {
        n = spscDequeueBatch(q, (void **)ents, RING_BATCH);
    }
    if (n == 0) return 0;
    for (size_t i = 0; i < n; i++) {
        client *c = (client *)(ents[i] & ~RING_TAGS);
        c->ring_seen = 0;
        if (ents[i] & RING_CMD) {
            ringExecuteOne(c);
        } else {
            ringReadEnd(c);
        }
    }
    processClientsCommandsBatch();
    return (int)n;
}

static int processCommandRing(void) {
    int total = 0;
    int use_prefetch = prefetchBatchEnabled() && !ProcessingEventsWhileBlocked;
    ringCoalesce();
    while (total < IO_PARTITION_COMPLETIONS_PER_CALL) {
        int got_any = 0;
        for (int t = 1; t < io_worker_hwm; t++) {
            int n = processCommandRingOne(t, use_prefetch);
            if (n == 0) continue;
            got_any = 1;
            total += n;
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

/* Lazy writes reconcile only when main next depends on their state. */
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
            for (int i = 1; i < server.active_io_threads_num; i++) ioWorkerPark(i);
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

        if ((numevents > 0 || strictOffloadActive()) && server.active_io_threads_num < io_ready_num) {
            for (int i = server.active_io_threads_num; i < io_ready_num; i++) ioWorkerUnpark(i);
            server.active_io_threads_num = io_ready_num;
        }
        return;
    }

    mstime_t now = server.mstime;
    static long long last_scale_time = 0;

    /* Parked threads would stall the partitioned sockets they own. */
    if (strictOffloadActive()) {
        if (server.active_io_threads_num < io_ready_num) {
            for (int i = server.active_io_threads_num; i < io_ready_num; i++) ioWorkerUnpark(i);
            server.active_io_threads_num = io_ready_num;
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

        if (strictOffloadActive()) should_ignite = 1;
        if (should_ignite && io_ready_num > 1) {
            ioWorkerUnpark(1);
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
    if (avg_q_size > 1 && active < (size_t)io_ready_num) {
        target++;
    } else if (avg_q_size == 0 && (now - last_scale_time > IO_COOLDOWN_MS)) {

        size_t min_active = strictOffloadActive() ? 2 : 1;
        if (target > min_active) target--;
    }

    /* Scale Up */
    if (target > active) {
        for (size_t i = active; i < target; i++) ioWorkerUnpark((int)i);
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

        ioWorkerPark(tid);
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
            if (pushed || !blocking || server.crashed) break; /* On server crash we kill the IO threads, no point in sending back jobs to the main-thread. */
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

    /* Blocking flush: ensure all pending jobs are sent before thread dies. A process
     * exit needs none of them and main is not draining, so it must not wait. */
    if (!atomic_load_explicit(&io_exit_abort, memory_order_acquire)) flushPendingIOResponses(1);

    /* Free the shared query buffer */
    freeSharedQueryBuf();
}

static void ioThreadWriteSlab(writeSlab *slab) {
    int notify = 0;
    for (size_t j = 0; j < slab->count; j++) {
        uintptr_t e = slab->entries[j];
        client *c = (client *)(e & ~SLAB_TAGS);
        /* Publish write completion only after write and rearm state. */
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
        /* A stop lands between iterations, after every dequeued job has run. */
        if (ioWorkerState((int)id) == IO_WORKER_STOPPING) break;
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

        if (io_epfd[id] > 0) processed += ioThreadPollPartition(id);

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
    pthread_cleanup_pop(1);
    atomic_store_explicit(&io_worker_state[id], IO_WORKER_STOPPED, memory_order_release);
    return NULL;
}

long long getIOThreadActiveTimeMicroseconds(int id) {
    return atomic_load_explicit(&used_active_time_io_thread[id], memory_order_relaxed);
}

static void freeIOThreadSlot(int id) {
    spscFree(&io_private_inbox[id]);
    spscFree(&io_cmd_ring[id]);
    fastpathFreeThread(id);
    if (io_epfd[id] >= 0) {
        close(io_epfd[id]);
        io_epfd[id] = -1;
    }
    if (io_partition_clients[id]) {
        listRelease(io_partition_clients[id]);
        io_partition_clients[id] = NULL;
    }
    io_threads[id] = 0;
    atomic_store_explicit(&io_worker_state[id], IO_WORKER_ABSENT, memory_order_release);
    while (io_worker_hwm > 1 && ioWorkerState(io_worker_hwm - 1) == IO_WORKER_ABSENT) io_worker_hwm--;
}

/* A new thread starts parked and RUNNING; the activation policy unparks it. */
static int createIOThread(int id) {
    serverAssert(id > 0 && id < IO_THREADS_MAX_NUM);
    serverAssert(ioWorkerState(id) == IO_WORKER_ABSENT);

    if (io_debug_fail_create == id) {
        io_debug_fail_create = 0;
        serverLog(LL_WARNING, "IO thread %d: creation failure injected by DEBUG", id);
        return C_ERR;
    }

    /* Initialize the private SPSC queue for this thread */
    spscInit(&io_private_inbox[id], IO_SPSC_QUEUE_SIZE);
    spscInit(&io_cmd_ring[id], IO_CMD_RING_SIZE);
    fastpathInitThread(id);
    io_partition_clients[id] = listCreate();

    io_epfd[id] = epoll_create1(EPOLL_CLOEXEC);
    if (io_epfd[id] < 0) serverLog(LL_WARNING, "IO thread %d: epoll_create1 failed (%s); no clients will be partitioned to it", id, strerror(errno));

    pthread_t tid;
    pthread_mutex_init(&io_threads_mutex[id], NULL);
    pthread_mutex_lock(&io_threads_mutex[id]); /* Thread will be stopped. */
    io_worker_parked[id] = 1;
    atomic_store_explicit(&io_worker_state[id], IO_WORKER_RUNNING, memory_order_release);
    if (id + 1 > io_worker_hwm) io_worker_hwm = id + 1;

    pthread_attr_t attr;
    serverInitThreadAttribute(&attr);

    int err = pthread_create(&tid, &attr, IOThreadMain, (void *)(long)id);
    pthread_attr_destroy(&attr);
    if (err) {
        serverLog(LL_WARNING, "Can't initialize IO thread %d, pthread_create failed with: %s", id, strerror(err));
        pthread_mutex_unlock(&io_threads_mutex[id]);
        io_worker_parked[id] = 0;
        pthread_mutex_destroy(&io_threads_mutex[id]);
        freeIOThreadSlot(id);
        return C_ERR;
    }
    io_threads[id] = tid;
    if (id == io_ready_num) {
        while (io_ready_num < io_worker_hwm && ioWorkerState(io_ready_num) == IO_WORKER_RUNNING) io_ready_num++;
    }
    return C_OK;
}

/* Terminates the IO thread specified by id. Crash path only: nothing is drained or handed back. */
static void shutdownIOThread(int id) {
    int err;
    pthread_t tid = io_threads[id];
    if (tid == pthread_self()) return;
    if (tid == 0) return;

    ioWorkerUnpark(id);
    pthread_cancel(tid);

    if ((err = pthread_join(tid, NULL)) != 0) {
        serverLog(LL_WARNING, "IO thread(tid:%lu) can not be joined: %s", (unsigned long)tid, strerror(err));
    } else {
        serverLog(LL_NOTICE, "IO thread(tid:%lu) terminated", (unsigned long)tid);
    }
    pthread_mutex_destroy(&io_threads_mutex[id]);
    io_threads[id] = 0;
    if (io_epfd[id] >= 0) {
        close(io_epfd[id]);
        io_epfd[id] = -1;
    }
}

void killIOThreads(void) {
    atomic_store_explicit(&io_exit_abort, 1, memory_order_release);
    for (int j = 1; j < io_worker_hwm; j++) { /* We don't kill thread 0, which is the main thread. */
        shutdownIOThread(j);
    }
}

/* Process exit: every thread stops at its next loop boundary and is joined, so no thread
 * touches server memory while exit handlers run. Clients are not handed back. */
void ioThreadsStopForExit(void) {
    serverAssert(inMainThread());
    atomic_store_explicit(&io_exit_abort, 1, memory_order_release);
    int any = 0;
    for (int id = 1; id < io_worker_hwm; id++) {
        if (io_threads[id] == 0) continue;
        int st = ioWorkerState(id);
        if (st == IO_WORKER_RUNNING || st == IO_WORKER_QUIESCING) {
            atomic_store_explicit(&io_worker_state[id], IO_WORKER_STOPPING, memory_order_release);
        }
        ioWorkerUnpark(id);
        any = 1;
    }
    if (!any) return;
    monotime deadline = getMonotonicUs() + 2000000;
    for (int id = 1; id < io_worker_hwm; id++) {
        if (io_threads[id] == 0) continue;
        while (ioWorkerState(id) != IO_WORKER_STOPPED && getMonotonicUs() < deadline) cpuRelax();
        if (ioWorkerState(id) != IO_WORKER_STOPPED) {
            serverLog(LL_WARNING, "IO thread %d did not stop before exit", id);
            continue;
        }
        pthread_join(io_threads[id], NULL);
        io_threads[id] = 0;
    }
}

/* Retirement begins: admission and routing exclude the slot from here on; the thread keeps
 * running so it can hand back what it owns. Idempotent. */
static void ioWorkerRetire(int tid) {
    if (ioWorkerState(tid) != IO_WORKER_RUNNING) return;
    flushWriteSlab(); /* staged work goes to a thread that is still routable */
    if (io_ready_num > tid) io_ready_num = tid;
    if (io_ready_num > server.io_threads_num) io_ready_num = server.io_threads_num; /* every slot above retires */
    if (server.active_io_threads_num > io_ready_num) server.active_io_threads_num = io_ready_num;
    spscCommit(&io_private_inbox[tid]); /* the last jobs main batched for it */
    ioWorkerUnpark(tid);
    atomic_store_explicit(&io_worker_state[tid], IO_WORKER_QUIESCING, memory_order_release);
    fastpathWorkerQuiesce(tid);
    unpartitionWorkerClients(tid);
}

/* Preconditions for STOPPING: nothing main owns still names the thread and nothing can reach it. */
static int ioWorkerDrained(int tid) {
    if (!fastpathWorkerDrained(tid)) return 0;
    if (listLength(io_partition_clients[tid]) > 0) return 0;
    while (processCommandRingOne(tid, 0) > 0) { /* completions a partitioned client left behind */
    }
    if (spscBacklog(&io_cmd_ring[tid]) > 0) return 0;
    if (!spscIsEmpty(&io_private_inbox[tid])) return 0;
    /* The shared inbox is drained by whichever threads remain; the last one finishes it. */
    if (io_ready_num <= 1 && getPendingIOThreadsJobs() > 0) return 0;
    return 1;
}

/* Fast-path role of a running thread follows the configuration; a quiesce completes before a reopen. */
static int ioWorkerReconcileFastpath(int tid) {
    int want = server.io_threads_fast_path && server.io_threads_strict_offload && server.io_threads_num >= 2;
    int role = fastpathWorkerRole(tid);
    if (!want) {
        fastpathWorkerQuiesce(tid);
        return role == FP_ROLE_QUIESCING || fastpathWorkerOwnedClients(tid) > 0;
    }
    if (role == FP_ROLE_OPEN) return 0;
    return !fastpathWorkerReopen(tid);
}

static void ioThreadsInitShared(void) {
    if (io_threads_initialized) return;
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

/* One reconciliation pass of the live worker set towards server.io_threads_num: slots above the
 * target retire top down and are destroyed once drained; slots below it are created in order,
 * a retiring slot being recreated only after its thread is gone. Returns C_ERR when creating a
 * thread failed; the target is then lowered to the last contiguous running slot. Teardown steps
 * run commands left in a thread's ring, so they are for beforeSleep, not for a CONFIG SET. */
static int ioThreadsConvergeOnce(int teardown) {
    int target = server.io_threads_num;
    int pending = 0;
    int rc = C_OK;

    for (int tid = io_worker_hwm - 1; tid >= 1 && tid >= target; tid--) {
        int st = ioWorkerState(tid);
        if (st == IO_WORKER_ABSENT) continue;
        if (st == IO_WORKER_RUNNING) {
            ioWorkerRetire(tid);
            st = IO_WORKER_QUIESCING;
        }
        if (!teardown) {
            pending = 1;
            continue;
        }
        if (st == IO_WORKER_QUIESCING) {
            if (ioWorkerDrained(tid)) {
                atomic_store_explicit(&io_worker_state[tid], IO_WORKER_STOPPING, memory_order_release);
            }
            pending = 1;
            continue;
        }
        if (st == IO_WORKER_STOPPING) {
            pending = 1;
            continue;
        }
        serverAssert(st == IO_WORKER_STOPPED);
        pthread_join(io_threads[tid], NULL);
        pthread_mutex_destroy(&io_threads_mutex[tid]);
        serverLog(LL_NOTICE, "IO thread %d retired", tid);
        freeIOThreadSlot(tid);
    }

    for (int tid = 1; tid < target; tid++) {
        int st = ioWorkerState(tid);
        if (st == IO_WORKER_RUNNING) continue;
        if (st != IO_WORKER_ABSENT) {
            pending = 1; /* a slot still retiring below the target is recreated once free */
            break;
        }
        if (createIOThread(tid) != C_OK) {
            /* Everything this scale-up started retires again with the lowered target. */
            serverLog(LL_WARNING, "IO thread %d could not be created; keeping %d IO threads", tid, io_scale_base);
            server.io_threads_num = io_scale_base;
            rc = C_ERR;
            pending = 1;
            break;
        }
    }

    for (int tid = 1; tid < io_worker_hwm; tid++) {
        if (ioWorkerState(tid) != IO_WORKER_RUNNING) continue;
        if (ioWorkerReconcileFastpath(tid)) pending = 1;
    }

    io_converging = pending;
    return rc;
}

void ioThreadsConverge(void) {
    if (!io_converging) return;
    ioThreadsConvergeOnce(1);
}

int ioThreadsRunningNum(void) {
    return io_ready_num - 1;
}

int ioThreadsRetiringNum(void) {
    int n = 0;
    for (int tid = 1; tid < io_worker_hwm; tid++) {
        int st = ioWorkerState(tid);
        if (st != IO_WORKER_ABSENT && st != IO_WORKER_RUNNING) n++;
    }
    return n;
}

void ioThreadsDebugFailCreate(int tid) {
    io_debug_fail_create = tid;
}

/* CONFIG SET io-threads: the first pass runs now, so free slots start immediately and a creation
 * failure is the command's error; retirement and deferred creation continue from beforeSleep. */
int updateIOThreads(const char **err) {
    serverAssert(inMainThread());
    ioThreadsInitShared();
    if (server.io_threads_num > io_ready_num && !io_converging) io_scale_base = io_ready_num;
    if (server.io_threads_num != io_ready_num || io_converging) {
        serverLog(LL_NOTICE, "Changing number of IO threads from %d to %d.", io_ready_num, server.io_threads_num);
    }
    io_converging = 1;
    if (ioThreadsConvergeOnce(0) != C_OK) {
        if (err) *err = "Can't create IO threads, check the server logs";
        return 0;
    }
    return 1;
}

/* Fast-path and strict-offload toggles: every running thread's role follows the new configuration. */
int applyIOThreadsFastpathConfig(const char **err) {
    UNUSED(err);
    serverAssert(inMainThread());
    if (!server.io_threads_strict_offload) unpartitionAllClients();
    if (!io_threads_initialized) return 1;
    io_converging = 1;
    ioThreadsConvergeOnce(0);
    return 1;
}

/* Initialize the data structures needed for I/O threads. */
void initIOThreads(int prev_threads_num) {
    /* Don't spawn any thread if the user selected a single thread:
     * we'll handle I/O directly from the main thread. */
    if (server.io_threads_num == 1) return;

    serverAssert(server.io_threads_num <= IO_THREADS_MAX_NUM);
    ioThreadsInitShared();

    /* Spawn and initialize the I/O threads. */
    for (int i = prev_threads_num; i < server.io_threads_num; i++) {
        if (createIOThread(i) != C_OK) {
            serverLog(LL_WARNING, "Fatal: Can't initialize IO thread %d", i);
            exit(1);
        }
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

/* Unit tests drive one fast-path thread inline; admission needs it ready and pollable. */
void testOnlySetIOThreadReady(int tid, int epfd) {
    io_epfd[tid] = epfd;
    atomic_store_explicit(&io_worker_state[tid], IO_WORKER_RUNNING, memory_order_release);
    if (io_worker_hwm <= tid) io_worker_hwm = tid + 1;
    if (io_ready_num <= tid) io_ready_num = tid + 1;
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
    /* Published writes cannot share a mutable payload header with main. */
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

    /* Successful plain partitioned writes reconcile lazily without an outbox response. */
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

/* Only sole-reference strings and object-local aggregates may free on IO threads. */
int tryOffloadFreeObjToIOThreads(robj *obj) {
    if (server.active_io_threads_num <= 1) {
        return C_ERR;
    }

    if (obj->refcount > 1) return C_ERR;

    switch (obj->type) {
    case OBJ_STRING:
        break;
    case OBJ_LIST:
    case OBJ_SET:
    case OBJ_ZSET:
    case OBJ_HASH:
        break;
    default:
        /* Module callbacks and stream teardown remain on bio. */
        return C_ERR;
    }

    offloadFreeAccountAdd(offloadObjFreeBytes(obj));
    slabAppendFree(obj, 1);
    server.stat_io_freed_objects++;
    return C_OK;
}

/* Full IO queues park raw buffers; disabled IO threads return them to the caller. */
int tryOffloadFreePtrToIOThreads(void *ptr) {
    if (ptr == NULL) return C_OK;
    if (inline_reclaim_depth) return C_ERR; /* emergency reclaim frees inline */
    if (server.active_io_threads_num <= 1) return C_ERR;

    offloadFreeAccountAdd(zmalloc_size(ptr));
    slabAppendFree(ptr, 0);
    return C_OK;
}

/* Terminal frees use IO threads when active; otherwise preserve freeObjAsync behavior. */
void freeValueNeverOnMain(robj *key, robj *val, int dbid) {
    if (inline_reclaim_depth) { /* emergency reclaim frees inline */
        if (val) decrRefCount(val);
        return;
    }
    if (val->refcount > 1) {
        decrRefCount(val);
        return;
    }

    if (server.active_io_threads_num > 1) {
        armNoMainThreadFree();
        if (tryOffloadFreeObjToIOThreads(val) == C_OK) {
            disarmNoMainThreadFree();
            return;
        }
        freeObjAsyncForce(val);
        disarmNoMainThreadFree();
        return;
    }

    /* Single-threaded mode preserves freeObjAsync behavior. */
    freeObjAsync(key, val, dbid);
}

void drainPendingMainFrees(void) {
    /* Partial slabs wait up to one millisecond to avoid tiny free jobs. */
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
                /* bio has no generic raw-buffer free. */
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

/* Response draining is bounded so main returns to the event loop under sustained load. */
int processIOThreadsResponses(void) {
    /* We don't check for threads number since some threads may return jobs then deactivate/shut-down */

    int fp_processed = fastpathDrain();

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
    flushWriteSlab();
    return total_processed;
}
