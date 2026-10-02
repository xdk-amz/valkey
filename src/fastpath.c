/* IO threads own fast-path clients; main only executes their published command batches. */

#include "server.h"
#include "fastpath.h"
#include "io_threads.h"
#include "memory_prefetch.h"
#include "throttle.h"
#include "module.h"
#include "dplus.h"
#include "bgiteration.h"
#ifdef HAVE_FASTPATH_EPOLL
#include <sys/epoll.h>
#else
#include "fastpath_no_epoll.h"
#endif
#include <sys/uio.h>

extern int ProcessingEventsWhileBlocked; /* networking.c */

/* Keep crossing-capable records compact: ClientHandle fits without growing the base command entry. */
static_assert(sizeof(ClientHandle) == 2 * sizeof(void *),
              "ClientHandle is a compact {control ref, generation, owner slot}");
static_assert(_Alignof(ClientHandle) == _Alignof(void *), "ClientHandle needs only pointer alignment");
static_assert(sizeof(cmdEntry) == 176, "cmdEntry layout changed; re-measure before/after for the report");
static_assert(sizeof(cmdBatch) == offsetof(cmdBatch, e) + IO_BATCH_MAX * sizeof(cmdEntry),
              "cmdBatch is its header plus IO_BATCH_MAX inline entries");
static_assert(offsetof(cmdBatch, pending_next) < offsetof(cmdBatch, e),
              "pending_next is a batch-header field, never part of the inline entry array");
/* The two reply counters each sit on their own cache line so the main producer and the IO consumer
 * never share a line, and neither shares the identity/control line at offset 0. */
static_assert(_Alignof(ClientControl) == CACHE_LINE_SIZE, "ClientControl aligns to a cache line for its counter lines");
static_assert(offsetof(ClientControl, reply_bytes_produced) == CACHE_LINE_SIZE,
              "reply_bytes_produced starts the producer line, past the identity/control line");
static_assert(offsetof(ClientControl, reply_bytes_released) == 2 * CACHE_LINE_SIZE,
              "reply_bytes_released starts its own consumer line, distinct from the producer line");
static_assert(offsetof(ClientControl, reply_bytes_released) - offsetof(ClientControl, reply_bytes_produced) >=
                  CACHE_LINE_SIZE,
              "the two reply counters never share a cache line");
/* The idle-timeout stamp rides the consumer line with reply_bytes_released and never crosses into a fourth line. */
static_assert(offsetof(ClientControl, last_interaction) >= 2 * CACHE_LINE_SIZE,
              "last_interaction sits on the consumer line, past the producer line");
static_assert(offsetof(ClientControl, last_interaction) + sizeof(time_t) <= 3 * CACHE_LINE_SIZE,
              "last_interaction stays within the consumer line, never a fourth line");
static_assert(sizeof(ClientControl) == 3 * CACHE_LINE_SIZE, "identity/control line plus the two counter lines");
/* The immutable limit pointer rides in the identity/control line's existing padding, so it adds no line
 * and never shares the producer or consumer counter line. */
static_assert(offsetof(ClientControl, limit) + sizeof(struct FastpathLimitEntry *) <= CACHE_LINE_SIZE,
              "the limit pointer fits line-0 padding without growing the control");

#define FP_RING_SIZE 1024        /* batches per ring; batches, not commands */
#define FP_ARENA_SIZE (16 * 1024) /* reply bytes per batch before a slot spills to the heap */
#define FP_FREELIST_MAX 64
#define FP_FREELIST_IDLE 4 /* batches a thread with nothing in flight keeps pooled */
#define FP_CLIENT_INFLIGHT_MAX 256 /* commands of one client on main at once */
#define FP_TAG_DETACH ((uintptr_t)1) /* return-ring entry is a client to close, not a batch */
#define FP_TAG_ATTACH ((uintptr_t)2) /* return-ring entry is a client the IO thread takes ownership of */
#define FP_TAGS (FP_TAG_DETACH | FP_TAG_ATTACH)
#define FP_RET_RESERVE 64 /* ring slots admission leaves free so returned batches and detaches never block */
#define FP_OWNER_SLOT_NONE UINT32_MAX

typedef struct fpOwnerSlot {
    ClientControl *control;
    union {
        client *connection;
        uint32_t next_free;
    };
} fpOwnerSlot;

typedef struct fpThread {
    spscQueue submit; /* IO thread -> main: cmdBatch * */
    spscQueue ret;    /* main -> IO thread: cmdBatch *, or client * | FP_TAG_ATTACH / FP_TAG_DETACH */
    cmdBatch *cur;    /* batch being assembled */
    cmdBatch *freelist[FP_FREELIST_MAX];
    int nfree;
    int inflight;     /* batches submitted, not yet returned */
    int cur_hold;     /* cur holds entries of a client that left behind held commands; cancelled once nothing is in flight */
    int quiescing;    /* IO thread only: quiesce observed, every owned client marked leaving */
    list owned;       /* IO thread only: clients this thread reads; registry with leaving */
    list leaving;     /* IO thread only: clients whose entries must return before hand-off or close */
    list deferred;    /* IO thread only: readable clients the in-flight cap turned away, oldest first */
    monotime cron_at; /* IO thread only: last pass of the owner's share of clientsCron */
    rax *registry;    /* IO thread only: lifecycle membership for owned + leaving clients */
    fpOwnerSlot *owner_slots; /* IO thread only: O(1) handle-to-connection resolution */
    uint32_t owner_slots_len;
    uint32_t owner_slots_cap;
    uint32_t owner_slots_free;
    uint32_t owner_slots_used;
    _Atomic int role; /* FP_ROLE_*: main stores OPEN and QUIESCING, the IO thread stores DRAINED */
    _Atomic int req_pending; /* set by any request publisher, cleared before the owner scans; a hint that some owned client has a pending CC_REQ_* */
    _Atomic int mem_hint;    /* set by the IO thread when an owned client alone exceeds maxmemory-clients; main clears it and re-accounts */
    size_t main_clients;   /* main only: clients routed here and not yet taken back */
    size_t detach_pending; /* main only: detach requests the IO thread has not consumed */
    list *ret_overflow;    /* main only: detach requests a full ret ring could not take */
    cmdBatch *pending_head; /* main only: FIFO head of batches held for appendfsync-always durability */
    cmdBatch *pending_tail; /* main only: FIFO tail of the held-batch list */
    int pending_count;      /* main only: held batches awaiting the post-fsync release */
    long long reads, net_input_bytes, net_output_bytes, writes, batches, deferrals, speculated;
    /* Observability: why a client/command left the fast path, and batch-queue pressure. IO-owner-only,
     * incremented on a decision the owner already makes, so no extra hot-path work. */
    long long fb_gate;      /* commands that stayed on / returned to main because a dynamic gate was closed at harvest */
    long long fb_ineligible; /* commands not admitted because their command flags are fast-path ineligible */
    long long fb_error;     /* commands left on main because the read carried a parse/protocol error */
    long long rq_gate;      /* entries main handed back unexecuted because a gate closed after admission */
    long long inflight_hwm; /* high-water mark of batches submitted-but-not-returned (queue depth pressure) */
    /* acl-offload: admission-read seqlock. The IO owner makes this odd around the region of a read
     * that dereferences a bound user's rule set (ACL tagging), and even otherwise. Main bumps the
     * ACL epoch, then waits (fastpathAdmissionQuiesce) until this is even or has advanced past the
     * odd value it saw, proving no reader still holds a pre-mutation selectors pointer. One relaxed
     * store on each side of the tag; no lock, no allocation. */
    _Atomic uint32_t admit_seq;
} fpThread;

static fpThread fp_threads[IO_THREADS_MAX_NUM];
static client *fp_exec_client[IO_THREADS_MAX_NUM]; /* main-thread executor per IO thread */
static size_t fastpath_clients = 0;                 /* main thread only */
static int fp_slots = 0;                            /* main thread only: 1 + highest initialized thread */
static unsigned fp_rr = 0;
static long long fp_retired[12]; /* main thread only: counters of threads since retired */

size_t fastpathClientCount(void) {
    return fastpath_clients;
}

size_t fastpathPendingBatches(int tid) {
    return (size_t)fp_threads[tid].pending_count;
}

int fastpathThreadIdle(int tid) {
    fpThread *t = &fp_threads[tid];
    if (t->submit.buffer == NULL) return 1;
    if (spscBacklog(&t->ret) > 0) return 0; /* returns or requests to take */
    if (atomic_load_explicit(&t->req_pending, memory_order_relaxed)) return 0;
    if (atomic_load_explicit(&t->role, memory_order_relaxed) == FP_ROLE_QUIESCING) return 0;
    /* A batch under assembly goes out on a later pass once the ring and the hold allow it; while main
     * still has batches, the returns wake the thread. */
    if (t->cur && t->cur->count > 0 && !t->cur_hold && spscBacklog(&t->submit) == 0) return 0;
    return 1;
}

int fastpathMainHasWork(void) {
    for (int tid = 1; tid < fp_slots; tid++) {
        fpThread *t = &fp_threads[tid];
        if (t->submit.buffer && spscBacklog(&t->submit) > 0) return 1;
    }
    return 0;
}

static cmdBatch *fpAllocBatch(fpThread *t, int tid) {
    cmdBatch *b;
    if (t->nfree > 0) {
        b = t->freelist[--t->nfree];
    } else {
        b = zmalloc(sizeof(cmdBatch));
        b->arena = zmalloc(FP_ARENA_SIZE);
        b->arena_cap = FP_ARENA_SIZE;
        b->refs = NULL;
        b->nrefs = b->refs_cap = 0;
        b->release = 0;
    }
    b->count = 0;
    b->io_tid = tid;
    b->arena_used = 0;
    b->opened_us = getMonotonicUs();
    return b;
}

static void fpFreeBatch(cmdBatch *b) {
    serverAssert(b->nrefs == 0); /* references are dropped on main before a batch is recycled */
    zfree(b->refs);
    zfree(b->arena);
    zfree(b);
}

static void fpRecycleBatch(fpThread *t, cmdBatch *b) {
    if (t->nfree < FP_FREELIST_MAX) {
        t->freelist[t->nfree++] = b;
    } else {
        fpFreeBatch(b);
    }
}

/* Batches pooled during a burst go back to the allocator once the thread has nothing in flight. */
static void fpTrimFreelist(fpThread *t) {
    while (t->nfree > FP_FREELIST_IDLE) fpFreeBatch(t->freelist[--t->nfree]);
}

void fastpathInitThread(int tid) {
    fpThread *t = &fp_threads[tid];
    memset(t, 0, sizeof(*t));
    spscInit(&t->submit, FP_RING_SIZE);
    spscInit(&t->ret, FP_RING_SIZE);
    t->registry = raxNew();
    t->owner_slots_free = FP_OWNER_SLOT_NONE;
    t->ret_overflow = listCreate();
    atomic_init(&t->role, FP_ROLE_OPEN);
    if (tid + 1 > fp_slots) fp_slots = tid + 1;
}

/* Destruction preconditions: no client, batch, ring entry or request may still name this thread. */
void fastpathFreeThread(int tid) {
    fpThread *t = &fp_threads[tid];
    if (t->submit.buffer == NULL) return;
    serverAssert(listLength(&t->owned) == 0 && listLength(&t->leaving) == 0 && raxSize(t->registry) == 0);
    serverAssert(listLength(&t->deferred) == 0);
    serverAssert(t->owner_slots_used == 0);
    serverAssert(t->inflight == 0 && t->cur == NULL);
    serverAssert(t->pending_head == NULL && t->pending_count == 0); /* no batch may be held for durability at destruction */
    serverAssert(spscBacklog(&t->submit) == 0 && spscIsEmpty(&t->ret));
    serverAssert(t->main_clients == 0 && t->detach_pending == 0 && listLength(t->ret_overflow) == 0);
    spscFree(&t->submit);
    spscFree(&t->ret);
    while (t->nfree > 0) fpFreeBatch(t->freelist[--t->nfree]);
    raxFree(t->registry);
    t->registry = NULL;
    zfree(t->owner_slots);
    t->owner_slots = NULL;
    t->owner_slots_len = t->owner_slots_cap = 0;
    t->owner_slots_free = FP_OWNER_SLOT_NONE;
    listRelease(t->ret_overflow);
    t->ret_overflow = NULL;
    fp_retired[0] += t->reads;
    fp_retired[1] += t->net_input_bytes;
    fp_retired[2] += t->net_output_bytes;
    fp_retired[3] += t->writes;
    fp_retired[4] += t->batches;
    fp_retired[5] += t->deferrals;
    fp_retired[6] += t->fb_gate;
    fp_retired[7] += t->fb_ineligible;
    fp_retired[8] += t->fb_error;
    fp_retired[9] += t->rq_gate;
    if (t->inflight_hwm > fp_retired[10]) fp_retired[10] = t->inflight_hwm; /* gauge: keep the peak across retired threads */
    fp_retired[11] += t->speculated;
    while (fp_slots > 0 && fp_threads[fp_slots - 1].submit.buffer == NULL) fp_slots--;
}

int fastpathWorkerRole(int tid) {
    return atomic_load_explicit(&fp_threads[tid].role, memory_order_acquire);
}

size_t fastpathWorkerOwnedClients(int tid) {
    return fp_threads[tid].main_clients;
}

void fastpathWorkerQuiesce(int tid) {
    fpThread *t = &fp_threads[tid];
    if (t->submit.buffer == NULL) return;
    int expected = FP_ROLE_OPEN;
    atomic_compare_exchange_strong_explicit(&t->role, &expected, FP_ROLE_QUIESCING, memory_order_release,
                                            memory_order_relaxed);
    ioThreadWake(tid);
}

/* Drained for main: the thread published DRAINED and main holds no reference or request for it. */
int fastpathWorkerDrained(int tid) {
    fpThread *t = &fp_threads[tid];
    if (t->submit.buffer == NULL) return 1;
    if (fastpathWorkerRole(tid) != FP_ROLE_DRAINED) return 0;
    if (t->main_clients || t->detach_pending || listLength(t->ret_overflow)) return 0;
    return spscBacklog(&t->submit) == 0 && spscIsEmpty(&t->ret);
}

int fastpathWorkerReopen(int tid) {
    fpThread *t = &fp_threads[tid];
    if (t->submit.buffer == NULL) return 0;
    if (fastpathWorkerRole(tid) == FP_ROLE_OPEN) return 1;
    if (!fastpathWorkerDrained(tid)) return 0;
    atomic_store_explicit(&t->role, FP_ROLE_OPEN, memory_order_release);
    return 1;
}

/* acl-offload admission seqlock. The IO owner brackets the rule-set-reading region of admission
 * (ACL tagging: the roles-list header AND each selectors list) with an odd/even publish. The Begin
 * store is seq_cst and pairs with the seq_cst load in fastpathAdmissionQuiesce: in the single total
 * order of seq_cst operations, either main's quiesce observes this worker odd (and waits the read
 * out) OR the worker's odd publish is ordered after main's load, in which case the worker's
 * subsequent rule-set loads are also ordered after main's epoch bump, so the worker re-snapshots
 * the new epoch and reads the new (swapped) lists. A plain release store would let those loads hoist
 * above the odd publish, so seq_cst is required here, not release. */
void fastpathAdmitReadBegin(int tid) {
    fpThread *t = &fp_threads[tid];
    atomic_store_explicit(&t->admit_seq, atomic_load_explicit(&t->admit_seq, memory_order_relaxed) + 1,
                          memory_order_seq_cst);
}

void fastpathAdmitReadEnd(int tid) {
    fpThread *t = &fp_threads[tid];
    atomic_store_explicit(&t->admit_seq, atomic_load_explicit(&t->admit_seq, memory_order_relaxed) + 1,
                          memory_order_release);
}

/* Main-thread side: return once no worker still holds a rule-set pointer from before the caller's
 * epoch bump. A worker is quiesced for our purpose when its seq is even (not in a read) or has moved
 * on from the odd value we first saw (that read finished; its pointer is dropped). The first load is
 * seq_cst to pair with fastpathAdmitReadBegin (see there); the spin load is acquire, enough to
 * observe the End store that flips it even. Caller must have bumped the ACL epoch first, so any read
 * that starts after this returns re-snapshots the epoch and will be punted by main, not trusted. */
void fastpathAdmissionQuiesce(void) {
    serverAssert(inMainThread());
    /* Order the caller's prior epoch bump (and selectors/roles swap) before the admit_seq
     * observations below, pairing with the seq_cst fence in aclOffloadTagCommand. In the single
     * total order of seq_cst ops, a worker we observe even has not yet snapshotted the old epoch
     * nor loaded the old rule-set list header, so it will read the new list (or be punted). */
    atomic_thread_fence(memory_order_seq_cst);
    for (int tid = 0; tid < fp_slots; tid++) {
        fpThread *t = &fp_threads[tid];
        if (t->submit.buffer == NULL) continue;
        uint32_t seen = atomic_load_explicit(&t->admit_seq, memory_order_seq_cst);
        if (!(seen & 1u)) continue; /* not inside an admission read */
        while (atomic_load_explicit(&t->admit_seq, memory_order_acquire) == seen) { /* spin: that read is still live */
        }
    }
}

/* Admitted clients carry only the session state a command entry can hold: user, db and RESP. */
static int fpDynamicGate(void); /* defined below; global-state capability gate consulted here too */
/* mstate outlives EXEC; only an open transaction or a live WATCH keeps the client on main. */
static int fpWatching(client *c) {
    return c->mstate && listLength(&c->mstate->watched_keys) > 0;
}

static int fpSessionEligible(client *c) {
#ifndef HAVE_FASTPATH_EPOLL
    (void)c;
    return 0; /* fast path requires epoll; the feature is compiled out on this platform */
#else
    if (!server.io_threads_fast_path) return 0;
    if (!strictOffloadActive() || server.io_threads_num < 2) return 0;
    if (!c->conn || c->flag.fake) return 0;
    if (c->conn->type != connectionByType(CONN_TYPE_SOCKET)) return 0;
    if (authRequired(c)) return 0; /* main enforces a later default-user password change per entry */
    if (server.cluster_enabled) return 0;
    if (!fpDynamicGate()) return 0; /* a global gate is closed: stay on main */
    if (c->flag.replica || c->flag.primary || c->flag.monitor || c->slot_migration_job) return 0;
    if (c->flag.blocked || c->flag.unblocked || c->flag.protected || c->flag.lua_debug) return 0;
    if (c->flag.close_asap || c->flag.close_after_reply || c->flag.close_after_command) return 0;
    if (c->flag.multi || fpWatching(c) || c->flag.pubsub || c->flag.tracking) return 0;
    if (c->flag.no_touch || c->flag.reply_off || c->flag.reply_skip || c->flag.reply_skip_next) return 0;
    if (c->flag.import_source) return 0;
    return 1;
#endif
}

int fastpathEligible(client *c) {
    return fpSessionEligible(c) && !c->flag.pending_read && !c->flag.partitioned;
}

/* Main publishes a return-ring request; a full ring parks detaches for the next drain pass. */
static void fpRetPublish(fpThread *t, client *c, uintptr_t tag) {
    if (listLength(t->ret_overflow) > 0 || spscFreeSlots(&t->ret) == 0) {
        serverAssert(tag == FP_TAG_DETACH);
        listAddNodeTail(t->ret_overflow, c);
        return;
    }
    spscEnqueue(&t->ret, (void *)((uintptr_t)c | tag), true);
    ioThreadWake((int)(t - fp_threads));
}

/* Both addresses are fixed for the life of the connection; a transport they cannot represent is not admitted. */
static int fpCaptureAddrs(client *c) {
    struct sockaddr_storage sa;
    socklen_t salen = sizeof(sa);
    if (getpeername(c->conn->fd, (struct sockaddr *)&sa, &salen) != 0) return C_ERR;
    if (peerIdentityFromSockaddr(&c->fp_peer, (struct sockaddr *)&sa, salen) != C_OK) return C_ERR;
    salen = sizeof(sa);
    if (getsockname(c->conn->fd, (struct sockaddr *)&sa, &salen) != 0) return C_ERR;
    c->fp_conn_type = (int8_t)connGetType(c->conn); /* fixed for the connection's life; the origin's transport for MONITOR/tracing */
    return peerIdentityFromSockaddr(&c->fp_local, (struct sockaddr *)&sa, salen);
}

/* Whether the entry is linked into the main-only live registry; an explicit state, not a node-null test. */
typedef enum fpLimitReg {
    FP_LIMIT_UNLINKED = 0, /* the entry exists but names no registry membership */
    FP_LIMIT_LINKED = 1,   /* reg_node is linked into fp_limit_registry */
} fpLimitReg;

/* Per-connection side object holding the main-owned registry/bucket/accounting bookkeeping the control's
 * three cache lines must not carry; allocated and freed with the control, never naming a connection.
 * Maxmemory-clients estimate for a fast-path client is base_usage + fastpathReplyOutstanding(control):
 * base_usage is the main-owned client allocation snapshotted at attach (struct, static buf, and querybuf
 * as they were before hand-off) and is not the live input/querybuf size while the IO thread owns the
 * socket; the reply term is the only part tracked live, through the control's counters. */
typedef struct FastpathLimitEntry {
    ClientControl *control;        /* Back-reference to the owning control; the only handle enforcement resolves. */
    listNode reg_node;             /* Membership in the main-only live registry while reg_state is LINKED. */
    listNode bucket_node;          /* Membership in a private fast-path size bucket while bucketed. */
    time_t soft_first_breach_time; /* Unixtime of the first soft breach; valid only while soft_breaching, same unit as obuf_soft_limit_reached_time. */
    size_t base_usage;             /* Main-owned client allocation captured at attach; valid only while base_captured. */
    size_t accounted_mem;          /* Maxmemory estimate last added to the NORMAL aggregate and used to bucket this entry; valid only while mem_accounted. */
    int bucket_index;              /* Private fast-path size bucket the entry sits in; valid only while bucketed. */
    fpLimitReg reg_state;          /* LINKED iff reg_node sits in the registry; keeps insert/remove idempotent. */
    bool soft_breaching;           /* True while a soft-limit breach is being timed; gates soft_first_breach_time. */
    bool base_captured;            /* True once base_usage was snapshotted at attach while main owned the client. */
    bool mem_accounted;            /* True while accounted_mem contributes to the NORMAL aggregate; keeps add/remove idempotent. */
    bool bucketed;                 /* True while bucket_node is linked in fp_mem_buckets; keeps link/unlink idempotent. */
    bool terminal_requested;       /* True once a terminal request was published for this entry, so it happens once. */
    bool terminal_pending_mem;     /* True while accounted_mem is pinned into fp_terminal_pending_mem; set once when terminal turns on while mem_accounted, cleared once at hand-off removal. */
    bool no_evict;                 /* CLIENT NO-EVICT at attach; it runs only on main, so it holds while the client is IO-owned. */
} FastpathLimitEntry;

/* Main-only registry of limit entries for currently IO-owned connections; a later COB/maxmemory pass
 * iterates it without dereferencing any client. Needs no lock: only main links, unlinks and walks it.
 * A file-scope list is zero-initialized, which is a valid empty list. */
static list fp_limit_registry;

/* Client eviction limit, 0 when maxmemory-clients is off; main publishes it so an IO thread can tell
 * when a client it owns exceeds the limit by itself. */
static _Atomic size_t fp_evict_limit;

static void fpPublishEvictLimit(void) {
    size_t limit = server.maxmemory_clients ? getClientEvictionLimit() : 0;
    atomic_store_explicit(&fp_evict_limit, limit, memory_order_relaxed);
}

/* Private fast-path size buckets, one list of FastpathLimitEntry per CLIENT_MEM_USAGE_BUCKETS class,
 * kept separate from server.client_mem_usage_buckets so fast-path entries never mix into normal client
 * bucket lists. Main-only; a file-scope list array is zero-initialized, which is a valid empty list. */
static list fp_mem_buckets[CLIENT_MEM_USAGE_BUCKETS];

/* Main-only running total of the reply-memory estimate fast-path entries contribute to
 * stat_clients_type_memory[CLIENT_TYPE_NORMAL]; kept so the contribution can be removed cleanly and
 * asserted. No atomic: only main updates it. */
static size_t fp_normal_mem_contribution = 0;

/* Main-only running total of accounted_mem for entries that are terminal (a maxmemory EVICT or COB CLOSE
 * was published) but whose stat contribution is still charged to the NORMAL aggregate because the IO
 * hand-off has not removed it yet. evictClients seeds pending_freed from this so the not-yet-freed memory
 * of an in-flight terminal client is not mistaken for live pressure across repeated calls. No atomic:
 * only main updates it. */
static size_t fp_terminal_pending_mem = 0;

/* Same log2 size-class index as getMemUsageBucket, so a fast-path client lands in the class a normal
 * client of the same estimate would; result is a valid fp_mem_buckets index. */
static int fpMemBucketIndex(size_t mem) {
    int size_in_bits = 8 * (int)sizeof(mem);
    int clz = mem > 0 ? __builtin_clzl(mem) : size_in_bits;
    int idx = size_in_bits - clz;
    if (idx > CLIENT_MEM_USAGE_BUCKET_MAX_LOG) idx = CLIENT_MEM_USAGE_BUCKET_MAX_LOG;
    else if (idx < CLIENT_MEM_USAGE_BUCKET_MIN_LOG) idx = CLIENT_MEM_USAGE_BUCKET_MIN_LOG;
    return idx - CLIENT_MEM_USAGE_BUCKET_MIN_LOG;
}

/* Remove an entry's maxmemory contribution from the NORMAL aggregate and its private bucket, each guarded
 * so a repeated call is a no-op. Releasing the stat contribution also releases any terminal-pending pin the
 * same entry holds, exactly once. This is the single site that drops a fast-path entry's stat contribution,
 * so hand-off removes it once. Main-only; never dereferences the connection. */
static void fpMemAccountRemove(FastpathLimitEntry *e) {
    if (e->mem_accounted) {
        server.stat_clients_type_memory[CLIENT_TYPE_NORMAL] -= e->accounted_mem;
        fp_normal_mem_contribution -= e->accounted_mem;
        if (e->terminal_pending_mem) { /* the same amount pinned when the entry turned terminal, released once */
            fp_terminal_pending_mem -= e->accounted_mem;
            e->terminal_pending_mem = false;
        }
        e->accounted_mem = 0;
        e->mem_accounted = false;
    }
    if (e->bucketed) {
        listUnlinkNode(&fp_mem_buckets[e->bucket_index], &e->bucket_node);
        e->bucketed = false;
    }
}

/* Recompute one entry's maxmemory estimate (attach-time base plus live outstanding reply bytes) and keep
 * its NORMAL aggregate contribution current. The stat contribution is maintained regardless of
 * server.maxmemory_clients so `info`/eviction see a fast-path client's memory even while enforcement is
 * off; only private bucket membership is gated on enforcement being enabled and the bucket array existing.
 * A no-op before base capture, or once terminal so a closing entry's estimate stays frozen (and stays out
 * of any selectable bucket) until hand-off removes it. Main-only; reads only the control's acquire-loaded
 * counters. */
/* in_floor: argument bytes main knows the client holds though its owner may not have published them yet. */
static void fpMemAccountUpdateWith(FastpathLimitEntry *e, size_t in_floor) {
    if (e->terminal_requested || !e->base_captured) return;
    size_t in = fastpathInputMem(e->control);
    if (in < in_floor) in = in_floor;
    size_t estimate = e->base_usage + fastpathReplyOutstanding(e->control) + in;
    size_t prev = e->mem_accounted ? e->accounted_mem : 0;
    server.stat_clients_type_memory[CLIENT_TYPE_NORMAL] -= prev;
    server.stat_clients_type_memory[CLIENT_TYPE_NORMAL] += estimate;
    fp_normal_mem_contribution -= prev;
    fp_normal_mem_contribution += estimate;
    e->accounted_mem = estimate;
    e->mem_accounted = true;
    if (server.maxmemory_clients && server.client_mem_usage_buckets && !e->no_evict) {
        int idx = fpMemBucketIndex(estimate);
        if (!e->bucketed) {
            listLinkNodeTail(&fp_mem_buckets[idx], &e->bucket_node);
            e->bucket_index = idx;
            e->bucketed = true;
        } else if (idx != e->bucket_index) {
            listUnlinkNode(&fp_mem_buckets[e->bucket_index], &e->bucket_node);
            listLinkNodeTail(&fp_mem_buckets[idx], &e->bucket_node);
            e->bucket_index = idx;
        }
    } else if (e->bucketed) {
        /* Enforcement was turned off (or the bucket array went away) since we last bucketed: drop the
         * bucket membership but keep the stat contribution above current. */
        listUnlinkNode(&fp_mem_buckets[e->bucket_index], &e->bucket_node);
        e->bucketed = false;
    }
}

static void fpMemAccountUpdate(FastpathLimitEntry *e) {
    fpMemAccountUpdateWith(e, 0);
}

/* Common terminal mark for both the maxmemory EVICT and the COB CLOSE paths: flip terminal_requested
 * exactly once, freeze the estimate by pinning any still-charged accounted_mem into the terminal-pending
 * total (once), and drop the entry from its selectable size bucket so it is never reselected. The stat
 * contribution itself stays until hand-off. Returns true only on the transition, so the caller publishes
 * its request and bumps its own stat once. Main-only; never dereferences the connection. */
static bool fpMarkTerminal(FastpathLimitEntry *e) {
    if (e->terminal_requested) return false;
    e->terminal_requested = true;
    if (e->mem_accounted && !e->terminal_pending_mem) { /* still charged to NORMAL: keep it counted as pending until hand-off */
        fp_terminal_pending_mem += e->accounted_mem;
        e->terminal_pending_mem = true;
    }
    if (e->bucketed) { /* a terminal entry must never sit in a selectable bucket */
        listUnlinkNode(&fp_mem_buckets[e->bucket_index], &e->bucket_node);
        e->bucketed = false;
    }
    return true;
}

/* Strip a client's normal-path memory accounting while main still owns it: drop its normal bucket
 * membership and its stat_clients_type_memory contribution, then zero last_memory_usage so neither
 * hand-off nor freeClient subtracts it again and the normal path re-adds it cleanly. Main-only. */
static void fpRemoveNormalAccounting(client *c) {
    removeClientFromMemUsageBucket(c, 0); /* subtracts last_memory_usage from the bucket sum, then unlinks */
    if (c->last_memory_usage) server.stat_clients_type_memory[c->last_memory_type] -= c->last_memory_usage;
    c->last_memory_usage = 0;
}

/* Link the connection's entry into the live registry; a no-op if already linked, so a repeated attach
 * path never double-inserts. Main-only. */
static void fpLimitRegistryInsert(client *c) {
    FastpathLimitEntry *e = c->control->limit;
    if (e->reg_state == FP_LIMIT_LINKED) return;
    /* Main still owns the client here: snapshot its base allocation and strip its normal accounting
     * before IO ownership is published, so the estimate has a base and the normal path double-counts
     * neither the base nor a stale bucket membership. The input side is left out of the base: the IO
     * owner publishes it live. */
    size_t base = getClientMemoryUsage(c, NULL);
    size_t input = (c->querybuf ? sdsAllocSize(c->querybuf) : 0) + c->argv_len_sum + sizeof(robj *) * c->argc;
    e->base_usage = base > input ? base - input : 0;
    e->no_evict = c->flag.no_evict;
    e->base_captured = true;
    fpRemoveNormalAccounting(c);
    listLinkNodeTail(&fp_limit_registry, &e->reg_node);
    e->reg_state = FP_LIMIT_LINKED;
    e->terminal_requested = false; /* fresh IO ownership epoch: no terminal published for this attach yet */
    e->soft_breaching = c->obuf_soft_limit_reached_time != 0; /* import the normal-path soft timer so a breach in progress is not evaded by crossing */
    e->soft_first_breach_time = c->obuf_soft_limit_reached_time;
}

/* Unlink the connection's entry from the live registry; a no-op if not linked, so close/quiesce/detach
 * paths that all funnel through hand-off remove it exactly once. Main-only. */
static void fpLimitRegistryRemove(client *c) {
    if (!c->control) return;
    FastpathLimitEntry *e = c->control->limit;
    if (!e || e->reg_state != FP_LIMIT_LINKED) return;
    fpMemAccountRemove(e); /* remove the fast-path aggregate/bucket contribution before normal maintenance resumes */
    e->base_captured = false; /* the base snapshot belongs to this ownership epoch only */
    c->obuf_soft_limit_reached_time = e->soft_breaching ? e->soft_first_breach_time : 0; /* hand the soft timer back so breach timing survives the handoff */
    listUnlinkNode(&fp_limit_registry, &e->reg_node);
    e->reg_state = FP_LIMIT_UNLINKED;
}

size_t fastpathLimitRegistryCount(void) {
    return listLength(&fp_limit_registry);
}

int fastpathLimitRegistryContains(const ClientControl *cc) {
    return cc && cc->limit && cc->limit->reg_state == FP_LIMIT_LINKED;
}

/* Fast-path clients are authenticated CLIENT_TYPE_NORMAL, so their reply memory is bounded by that class's limit; the unauthenticated cap never applies. */
#define FP_LIMIT_CLASS CLIENT_TYPE_NORMAL

/* Publish a terminal close for a limit breach exactly once and count one COB disconnection; the common
 * terminal mark makes a repeated inline or cron check idempotent and pins any still-accounted memory as
 * terminal-pending. Main-only; never dereferences the connection. */
static void fpLimitRequestTerminalClose(FastpathLimitEntry *e) {
    if (!fpMarkTerminal(e)) return;
    server.stat_client_outbuf_limit_disconnections++;
    fastpathControlRequest(e->control, CC_REQ_CLOSE);
}

/* Publish an idle-timeout CLOSE once and count one expired client; its own reason and metric though it shares the CLOSE request and terminal-pending pin. Main-only. */
static void fpLimitRequestTerminalIdleTimeout(FastpathLimitEntry *e) {
    if (!fpMarkTerminal(e)) return;
    server.stat_client_idle_timeout_disconnections++;
    fastpathControlRequest(e->control, CC_REQ_CLOSE);
}

/* Hard COB check for one control: an immediate terminal close once outstanding reply memory reaches the configured hard limit. Reads only the class limit and the control's acquire-loaded counters. Main-only. */
static void fpLimitHardCheck(ClientControl *cc) {
    size_t hard = server.client_obuf_limits[FP_LIMIT_CLASS].hard_limit_bytes;
    if (hard == 0) return;
    if (fastpathReplyOutstanding(cc) >= hard) fpLimitRequestTerminalClose(cc->limit);
}

/* Per-entry COB pass for the amortized cron: hard first (a fallback to the inline charge check for config reductions and already-buffered clients), then the soft limit in normal COB seconds semantics. Reads only the class limits, server.unixtime, and the control's acquire-loaded counters. Main-only. */
static void fpLimitCheckEntry(FastpathLimitEntry *e) {
    if (e->terminal_requested) return; /* already closing: leave its timer and the stat untouched */
    ClientControl *cc = e->control;
    size_t used = fastpathReplyOutstanding(cc);
    size_t hard = server.client_obuf_limits[FP_LIMIT_CLASS].hard_limit_bytes;
    if (hard != 0 && used >= hard) {
        fpLimitRequestTerminalClose(e);
        return;
    }
    size_t soft = server.client_obuf_limits[FP_LIMIT_CLASS].soft_limit_bytes;
    if (soft != 0 && used >= soft) {
        if (!e->soft_breaching) {
            e->soft_breaching = true;
            e->soft_first_breach_time = server.unixtime; /* first observation arms the timer and never fires immediately */
        } else if (server.unixtime - e->soft_first_breach_time > server.client_obuf_limits[FP_LIMIT_CLASS].soft_limit_seconds) {
            fpLimitRequestTerminalClose(e);
        }
    } else {
        e->soft_breaching = false; /* a below-threshold observation ends the breach so timing cannot accumulate across gaps */
    }
}

/* Close a fast-path client idle past server.maxidletime once with the normal path's strict-greater seconds semantics; zero maxidletime disables it. Main-only, reads only the acquire-loaded stamp. */
static void fpLimitCheckIdleTimeout(FastpathLimitEntry *e) {
    if (e->terminal_requested || e->control->lifecycle != FP_ACTIVE) return;
    if (server.maxidletime == 0) return; /* zero disables idle timeout, as on the normal path */
    time_t last = fastpathControlLastInteraction(e->control);
    if (server.unixtime - last > server.maxidletime) fpLimitRequestTerminalIdleTimeout(e);
}

/* Amortized main-only sweep of the registry: each call visits a rotating slice sized so every registered
 * control is covered about once per second at server.hz, never an O(N) scan per tick. Each visit runs the
 * COB check and re-accounts the entry's maxmemory-clients estimate in one pass. Rotating head to tail
 * advances the cursor without a saved node a concurrent unlink could dangle. */
void fastpathLimitsCron(void) {
    fpPublishEvictLimit(); /* follows maxmemory when the limit is a percentage */
    size_t n = listLength(&fp_limit_registry);
    if (n == 0) return;
    /* Input that reaches no batch, such as a partial command, is seen within a tick. */
    if (server.maxmemory_clients) {
        listIter li;
        listNode *ln;
        listRewind(&fp_limit_registry, &li);
        while ((ln = listNext(&li)) != NULL) fpMemAccountUpdate(listNodeValue(ln));
    }
    int hz = server.hz > 0 ? server.hz : 1;
    size_t budget = (n + (size_t)hz - 1) / (size_t)hz;
    if (budget > n) budget = n;
    for (size_t i = 0; i < budget; i++) {
        listNode *head = listFirst(&fp_limit_registry);
        FastpathLimitEntry *e = listNodeValue(head);
        listRotateHeadToTail(&fp_limit_registry);
        fpLimitCheckEntry(e);
        fpLimitCheckIdleTimeout(e); /* one idle-timeout check per entry visit, after the COB check so a COB close keeps its reason */
        fpMemAccountUpdate(e); /* a COB close set terminal_requested, which this skips, so the estimate freezes for hand-off */
    }
}

/* Highest non-empty private fast-path size bucket index, or -1 when no fast-path client is bucketed. */
int fastpathEvictionMaxBucket(void) {
    for (int i = CLIENT_MEM_USAGE_BUCKETS - 1; i >= 0; i--)
        if (listLength(&fp_mem_buckets[i]) > 0) return i;
    return -1;
}

size_t fastpathEvictTopFromBucket(int bucket_idx) {
    if (bucket_idx < 0 || bucket_idx >= CLIENT_MEM_USAGE_BUCKETS) return 0;
    listNode *head = listFirst(&fp_mem_buckets[bucket_idx]);
    if (!head) return 0;
    FastpathLimitEntry *e = listNodeValue(head);
    size_t freed = e->accounted_mem; /* the estimate in the NORMAL aggregate; the hand-off removes the same amount */
    if (fpMarkTerminal(e)) { /* unlinks it from the bucket and pins it pending; a COB-closing entry is not re-published */
        server.stat_evictedclients++;
        fastpathControlRequest(e->control, CC_REQ_EVICT);
    }
    return freed;
}

void fastpathApplyMaxmemoryClients(int enabled) {
    /* server.maxmemory_clients already reflects the new state and enabled mirrors it; fpMemAccountUpdate
     * reads the global to decide bucketing, and keeps each entry's stat contribution current either way,
     * so enabling rebuckets accounted entries and disabling only unbuckets them. */
    serverAssert((server.maxmemory_clients != 0) == (enabled != 0));
    fpPublishEvictLimit();
    listIter li;
    listNode *ln;
    listRewind(&fp_limit_registry, &li);
    while ((ln = listNext(&li)) != NULL) {
        FastpathLimitEntry *e = listNodeValue(ln);
        fpMemAccountUpdate(e); /* skips terminal entries internally, leaving their frozen contribution intact */
    }
}

/* Main-only: reply/base memory still charged to the NORMAL aggregate for terminal fast-path entries whose
 * IO hand-off has not yet removed the contribution. evictClients seeds pending_freed from this. */
size_t fastpathTerminalPendingMem(void) {
    return fp_terminal_pending_mem;
}

size_t fastpathMaxmemoryAggregate(void) {
    return fp_normal_mem_contribution;
}

size_t fastpathMaxmemoryAccounted(const ClientControl *cc) {
    return (cc && cc->limit && cc->limit->mem_accounted) ? cc->limit->accounted_mem : 0;
}

int fastpathMaxmemoryBucketOf(const ClientControl *cc) {
    return (cc && cc->limit && cc->limit->bucketed) ? cc->limit->bucket_index : -1;
}

size_t fastpathMaxmemoryBucketCount(int bucket_idx) {
    if (bucket_idx < 0 || bucket_idx >= CLIENT_MEM_USAGE_BUCKETS) return 0;
    return listLength(&fp_mem_buckets[bucket_idx]);
}

/* One connection, one ClientControl: allocated before its first fast-path admission and kept until the
 * connection is freed. The IO owner bumps generation whenever it assigns a private resolution slot. */
int fastpathControlEnsure(client *c) {
    if (c->control) return C_OK;
    ClientControl *cc = zmalloc_cache_aligned(sizeof(ClientControl));
    if (!cc) return C_ERR;
    FastpathLimitEntry *e = zcalloc(sizeof(*e)); /* one side object per connection, never per command or batch */
    if (!e) {
        zfree(cc); /* init failure leaves the client on main and leaks neither allocation */
        return C_ERR;
    }
    cc->client_id = c->id;
    cc->generation = 1;
    cc->owner_domain = CC_OWNER_MAIN; /* still main's until the attach hands it to the IO thread */
    cc->owner_tid = 0;
    cc->lifecycle = FP_DETACHED;
    cc->pin_refs = 0;
    cc->pin_bits = 0;
    cc->limit = e;
    atomic_init(&cc->requests, 0u);
    atomic_init(&cc->reply_bytes_produced, (size_t)0);
    atomic_init(&cc->reply_bytes_released, (size_t)0);
    atomic_init(&cc->last_interaction, (time_t)0);
    atomic_init(&cc->input_mem, (size_t)0);
    atomic_init(&cc->qbuf_len, (size_t)0);
    atomic_init(&cc->qbuf_free, (size_t)0);
    atomic_init(&cc->argv_mem, (size_t)0);
    atomic_init(&cc->rbuf_size, (size_t)0);
    atomic_init(&cc->rbuf_peak, (size_t)0);
    cc->name = NULL;
    e->control = cc;
    e->bucketed = false; /* zcalloc already cleared base/accounted/reg state; a fresh entry names no bucket */
    e->reg_state = FP_LIMIT_UNLINKED;
    listInitNode(&e->reg_node, e);
    listInitNode(&e->bucket_node, e);
    c->control = cc;
    return C_OK;
}

/* Lifetime gate: pin_refs counts the still-live cross-thread references to the control that must drain
 * before it can be reclaimed. Each is a coarse lifecycle-level pin, never a per-command refcount:
 *   PIN_OWNER  - an IO thread owns the connection (held attach..handoff), so its worker registry and
 *                any batch/return entry can still resolve the control.
 *   PIN_DETACH - a terminal detach record is still in the owner's return ring (held request..consumed),
 *                so the owner may still meet the pointer.
 * Every pin is taken and dropped on main (the sole caller of attach/detach/handoff/reclaim), so pin_refs
 * needs no atomic. Reply memory is a separate gate read from its own counters, not a pin. */
#define CC_PIN_OWNER (1u << 0)
#define CC_PIN_DETACH (1u << 1)

/* Take a lifecycle pin on the control if it does not already hold that pin; idempotent per bit so a
 * re-admission or a duplicate request never double-counts. Main-only. */
static void fastpathControlPin(client *c, uint32_t pin) {
    ClientControl *cc = c->control;
    if (!cc || (cc->pin_bits & pin)) return;
    cc->pin_bits |= pin;
    cc->pin_refs++;
}

/* Drop a lifecycle pin held on the control; idempotent per bit so a late or duplicate release is a
 * no-op. Main-only. */
static void fastpathControlUnpin(client *c, uint32_t pin) {
    ClientControl *cc = c->control;
    if (!cc || !(cc->pin_bits & pin)) return;
    cc->pin_bits &= ~pin;
    serverAssert(cc->pin_refs > 0);
    cc->pin_refs--;
}

/* True once every lifetime gate has cleared: no owner and no detach record pin the control, and every
 * external reply charged to it has been released. Only then may the control be freed; a client whose
 * control is still pinned or still owes released replies is not yet reclaimable. */
static int fastpathControlReclaimable(const ClientControl *cc) {
    return cc->pin_refs == 0 && fastpathReplyOutstanding(cc) == 0;
}

/* The one and only reclaimer for a ClientControl, called from freeClient once the connection is fully
 * torn down. By this point freeClient has already waited out the fast-path and unconsumed-detach gates,
 * so both pins are dropped; this asserts that lifetime invariant (no owner, no detach record, no
 * outstanding replies) before freeing and clearing the connection's link. */
void fastpathControlReclaim(client *c) {
    if (!c->control) return;
    serverAssert(fastpathControlReclaimable(c->control));
    FastpathLimitEntry *e = c->control->limit;
    serverAssert(e && e->reg_state == FP_LIMIT_UNLINKED); /* a still-registered entry means a hand-off did not remove it */
    zfree(e); /* freed exactly once, with its control */
    zfree(c->control);
    c->control = NULL;
}

/* CC_REQ_* precedence, high to low: CLOSE dominates every other request; EVICT (a memory/QoS free)
 * outranks a plain HANDOFF and a QUIESCE; HANDOFF (return to main) outranks QUIESCE (reassignable
 * hand-back). One winner decides the terminal disposition; the rest are subsumed. */
#define CC_REQ_TERMINAL (CC_REQ_CLOSE | CC_REQ_EVICT) /* the connection is freed, not reassigned */

/* Any authorized caller (main or the owner itself) publishes a lifecycle request against the control.
 * Nonblocking and idempotent: a bit already set is a no-op, and there is no synchronous or completion
 * handshake back to the caller, only the bit. Compatible requests coalesce into the same word.
 * Publishing CLOSE clears the requests it supersedes so the owner never has to re-resolve a dominated
 * bit. Release ordering pairs with the owner's acquire load in fpExecuteRequests so the request is
 * visible before the owner acts, and any state the caller wrote before requesting is visible with it. */
void fastpathControlRequest(ClientControl *cc, uint32_t req) {
    if (req == 0) return;
    if (req & CC_REQ_CLOSE) req = CC_REQ_CLOSE; /* terminal: it subsumes QUIESCE/HANDOFF/EVICT */
    uint32_t cur = atomic_load_explicit(&cc->requests, memory_order_relaxed);
    uint32_t next;
    do {
        if (cur & CC_REQ_CLOSE) return;         /* already closing; nothing outranks it */
        next = cur | req;
        if (req & CC_REQ_CLOSE) next = CC_REQ_CLOSE; /* drop the bits CLOSE supersedes */
        if (next == cur) return;                /* idempotent: bit already present */
    } while (!atomic_compare_exchange_weak_explicit(&cc->requests, &cur, next, memory_order_release,
                                                    memory_order_relaxed));
    /* Signal the owning IO thread so an idle owner (no traffic, nothing in flight) still observes the
     * request on its next pass without main walking into the owner's registry. owner_domain/owner_tid
     * are published by the owner with release at attach/handoff; a benign stale read only sets an extra
     * hint, which the scan clears harmlessly. */
    if (cc->owner_domain == CC_OWNER_IO) {
        atomic_store_explicit(&fp_threads[cc->owner_tid].req_pending, 1, memory_order_release);
        ioThreadWake(cc->owner_tid);
    }
}

/* Dominant pending request by precedence CLOSE > EVICT > HANDOFF > QUIESCE, EVICT immediate; 0 when no known bit is set, so an unknown future bit is not actioned. */
static uint32_t fpRequestWinner(uint32_t reqs) {
    if (reqs & CC_REQ_CLOSE) return CC_REQ_CLOSE;
    if (reqs & CC_REQ_EVICT) return CC_REQ_EVICT;
    if (reqs & CC_REQ_HANDOFF) return CC_REQ_HANDOFF;
    if (reqs & CC_REQ_QUIESCE) return CC_REQ_QUIESCE;
    return 0;
}

/* A handle captures the connection's control and the generation current at capture; a later mismatch
 * means the slot was reused. A control-bearing client always has a control by the time it can be
 * published into an entry (admission calls fastpathControlEnsure first). */
ClientHandle fastpathHandleFor(client *c) {
    serverAssert(c->control && c->fp_owner_slot != FP_OWNER_SLOT_NONE);
    return (ClientHandle){
        .control = c->control,
        .generation = c->control->generation,
        .owner_slot = c->fp_owner_slot,
    };
}

/* Stale iff the control's generation moved on from the snapshot; reads only the control, never a
 * connection, so a reused slot is caught without touching freed connection storage. */
int fastpathHandleStale(const ClientHandle *h) {
    return h->control == NULL || h->control->generation != h->generation;
}

/* Logical reply bytes an entry retains for its client: the arena run or the spilled heap block, never
 * both (main sets one or the other) and never allocator-rounded capacity, so produce and release count
 * the same unit. An encoded reply counts the bytes it sends, referenced strings included. Zero for a
 * requeued or reply-less entry, so charging it is a no-op. */
static inline size_t fpEntryReplyBytes(const cmdEntry *e) {
    if (e->reply_encoded) return e->reply_wire;
    return e->reply_big ? e->reply_big_len : e->reply_len;
}

/* Main publishes all reply charges before returning the batch, coalescing adjacent entries for one
 * control so pipelined commands share one producer-counter update. */
static void fpReplyChargeBatch(cmdBatch *b) {
    int i = 0;
    while (i < b->count) {
        ClientHandle *handle = &b->e[i].handle;
        ClientControl *cc = handle->control;
        uint32_t generation = handle->generation;
        size_t bytes = 0;
        int j = i;
        while (j < b->count && b->e[j].handle.control == cc && b->e[j].handle.generation == generation) {
            bytes += fpEntryReplyBytes(&b->e[j]);
            j++;
        }
        if (bytes > 0 && !fastpathHandleStale(handle)) {
            size_t produced = atomic_load_explicit(&cc->reply_bytes_produced, memory_order_relaxed);
            atomic_store_explicit(&cc->reply_bytes_produced, produced + bytes, memory_order_release);
            fpLimitHardCheck(cc); /* hard COB enforced once per charged group, before this batch is published back */
            if (server.maxmemory_clients) fpMemAccountUpdate(cc->limit);
        }
        i = j;
    }
}

/* Sole writer is the IO owner; release-ordered so the reclaim gate's acquire load sees it before reclaiming. */
static void fpControlReleaseBytes(ClientControl *cc, size_t bytes) {
    if (bytes == 0) return;
    size_t released = atomic_load_explicit(&cc->reply_bytes_released, memory_order_relaxed);
    atomic_store_explicit(&cc->reply_bytes_released, released + bytes, memory_order_release);
}

/* Charges reply bytes the IO owner produced itself (speculated reads) and left in fp_out, so their
 * later flush or hand-off release balances. Main owns reply_bytes_produced, so the charge is taken
 * back from released, which only the IO owner writes. */
static void fpControlChargeOwnBytes(ClientControl *cc, size_t bytes) {
    if (bytes == 0) return;
    size_t released = atomic_load_explicit(&cc->reply_bytes_released, memory_order_relaxed);
    atomic_store_explicit(&cc->reply_bytes_released, released - bytes, memory_order_release);
}

/* Reply bytes charged to a control but not yet released: produced minus released, read with acquire so a
 * charge and a release are both visible. The unsigned difference stays correct across benign wraparound
 * while outstanding < SIZE_MAX. Read-only: this does not enforce COB or maxmemory-clients. */
size_t fastpathReplyOutstanding(const ClientControl *cc) {
    size_t produced = atomic_load_explicit(&cc->reply_bytes_produced, memory_order_acquire);
    size_t released = atomic_load_explicit(&cc->reply_bytes_released, memory_order_acquire);
    return produced - released;
}

/* Argument bytes of a parsed command, counted the way getClientMemoryUsage counts them. */
static inline size_t fpArgvBytes(size_t argv_len_sum, int argc) {
    return argv_len_sum + sizeof(robj *) * (size_t)argc;
}

/* IO owner, outside a read: the query buffer is private or NULL here. */
static size_t fpInputMem(const client *c) {
    return (c->querybuf ? sdsAllocSize(c->querybuf) : 0) + fpArgvBytes(c->argv_len_sum, c->argc) +
           c->fp_inflight_argv;
}

/* Owner of the client (main before the transfer, then the IO thread): the buffer sizes CLIENT LIST shows. */
static void fpPublishBuffers(client *c) {
    ClientControl *cc = c->control;
    atomic_store_explicit(&cc->qbuf_len, c->querybuf ? sdslen(c->querybuf) : 0, memory_order_relaxed);
    atomic_store_explicit(&cc->qbuf_free, c->querybuf ? sdsavail(c->querybuf) : 0, memory_order_relaxed);
    atomic_store_explicit(&cc->argv_mem, c->argv_len_sum, memory_order_relaxed);
    atomic_store_explicit(&cc->rbuf_size, c->buf_usable_size, memory_order_relaxed);
    atomic_store_explicit(&cc->rbuf_peak, c->buf_peak, memory_order_relaxed);
}

void fastpathClientBuffers(const ClientControl *cc, fastpathBufferInfo *info) {
    info->qbuf = atomic_load_explicit(&cc->qbuf_len, memory_order_relaxed);
    info->qbuf_free = atomic_load_explicit(&cc->qbuf_free, memory_order_relaxed);
    info->argv_mem = atomic_load_explicit(&cc->argv_mem, memory_order_relaxed);
    info->rbs = atomic_load_explicit(&cc->rbuf_size, memory_order_relaxed);
    info->rbp = atomic_load_explicit(&cc->rbuf_peak, memory_order_relaxed);
}

static void fpPublishInput(fpThread *t, client *c) {
    fpPublishBuffers(c);
    size_t v = fpInputMem(c);
    if (v != c->fp_input_published) {
        c->fp_input_published = v;
        atomic_store_explicit(&c->control->input_mem, v, memory_order_relaxed);
    }
    size_t limit = atomic_load_explicit(&fp_evict_limit, memory_order_relaxed);
    if (!limit) return;
    /* A client over the whole limit is evicted whatever the others hold, so main is told now. */
    const FastpathLimitEntry *e = c->control->limit;
    int over = !e->no_evict && e->base_usage + fastpathReplyOutstanding(c->control) + v > limit;
    if (over && !c->fp_mem_hinted) atomic_store_explicit(&t->mem_hint, 1, memory_order_release);
    c->fp_mem_hinted = (uint8_t)over;
}

size_t fastpathInputMem(const ClientControl *cc) {
    return atomic_load_explicit(&cc->input_mem, memory_order_relaxed);
}

/* Main: memory of an IO-owned client from what main and its owner publish, never from the client itself. */
size_t fastpathClientMemory(const ClientControl *cc, size_t *output_mem) {
    size_t out = fastpathReplyOutstanding(cc);
    if (output_mem) *output_mem = out;
    const FastpathLimitEntry *e = cc->limit;
    return (e->base_captured ? e->base_usage : 0) + out + fastpathInputMem(cc);
}

/* IO owner (and main at attach-init) writes the idle stamp with release so main's acquire load sees the latest interaction. */
static void fpControlSetLastInteraction(ClientControl *cc, time_t t) {
    atomic_store_explicit(&cc->last_interaction, t, memory_order_release);
}

time_t fastpathControlLastInteraction(const ClientControl *cc) {
    return atomic_load_explicit(&cc->last_interaction, memory_order_acquire);
}

/* Ownership passes with the ring entry: after it main touches nothing of the client until it is handed back. */
int fastpathAttach(client *c) {
    int n = ioThreadsReadyNum() - 1;
    fpThread *t = NULL;
    int tid = 0;
    for (int i = 0; i < n; i++) {
        tid = 1 + (int)(fp_rr++ % (unsigned)n);
        t = &fp_threads[tid];
        if (t->submit.buffer && fastpathWorkerRole(tid) == FP_ROLE_OPEN && listLength(t->ret_overflow) == 0 &&
            spscFreeSlots(&t->ret) > FP_RET_RESERVE)
            break;
        t = NULL;
    }
    if (!t) return C_ERR;
    if (c->fp_peer.family == 0 && fpCaptureAddrs(c) != C_OK) return C_ERR;
    if (fastpathControlEnsure(c) != C_OK) return C_ERR;
    c->io_tid = tid;
    c->flag.fastpath = 1;
    c->fp_inflight = 0;
    c->fp_inflight_argv = 0;
    c->fp_input_published = 0;
    c->fp_mem_hinted = 0;
    atomic_store_explicit(&c->control->input_mem, (size_t)0, memory_order_relaxed);
    c->fp_held = 0;
    c->fp_owner_slot = FP_OWNER_SLOT_NONE;
    c->fp_out = NULL;
    c->fp_deferred = 0;
    listInitNode(&c->fp_defer_node, c);
    /* Main publishes IO ownership before the ring entry hands the connection over; the IO thread is the
     * next writer of these fields. control->lifecycle is the single source of truth for the state. */
    fpLimitRegistryInsert(c); /* register before ownership is published, so a pass never meets an unregistered owned client */
    fpControlSetLastInteraction(c->control, c->last_interaction); /* publish the idle stamp from main-owned state before the transfer */
    fpPublishBuffers(c);
    c->control->owner_domain = CC_OWNER_IO;
    c->control->owner_tid = (uint8_t)tid;
    c->control->lifecycle = FP_ACTIVE;
    c->control->name = c->name;
    c->control->capa = (uint8_t)c->capa;
    c->control->readonly = c->flag.readonly;
    fastpathControlPin(c, CC_PIN_OWNER); /* IO now owns the connection; hold until handoff returns it to main */
    listInitNode(&c->io_owner_node, c);
    fastpath_clients++;
    t->main_clients++;
    fpRetPublish(t, c, FP_TAG_ATTACH);
    return C_OK;
}

/* Centralized conservative capability decision for the fast path, split in two:
 *
 *   fpCommandAllowed() - STATIC, per command, from command flags alone. Stable for a given command,
 *       so the IO thread checks it once at admission.
 *   fpDynamicGate()    - DYNAMIC, global server state that a command's flags cannot express and that
 *       main can change at any time (module command filters, active throttlers, an in-progress
 *       failover, a client pause, a forkless iteration, a yielding module). Read lock-free from both
 *       the IO thread (admission) and main (execution). The execute-time check is the authoritative
 *       one: if a gate closes after a command was admitted, the executor requeues it to main rather
 *       than running it under stale eligibility, so ACL/MONITOR/module/throttle/failover invariants
 *       are always upheld by the main path. Reading a gate the moment it flips is a benign race: a
 *       missed close is caught at execution, and a missed open only defers a command to main
 *       (conservative, never wrong).
 * Both are pure reads of a few globals: no locks, allocations, lookups or handshakes on the hot path. */
static int fpDynamicGate(void) {
    if (isPausedActions(PAUSE_ACTION_CLIENT_ALL | PAUSE_ACTION_CLIENT_WRITE)) return 0; /* paused: main postpones */
    if (server.failover_state != NO_FAILOVER) return 0;  /* coordinated failover: writes belong on main */
    if (moduleHasCommandFilters()) return 0;             /* a filter may rewrite/redirect any command */
    if (throttle_active()) return 0;                     /* main runs the throttle check */
    if (bgIteration_iterationActive()) return 0;         /* a write may wait for the iterator, as its own client */
    if (server.busy_module_yield_flags != BUSY_MODULE_YIELD_NONE &&
        !(server.busy_module_yield_flags & BUSY_MODULE_YIELD_CLIENTS))
        return 0; /* a yielding module postpones commands until it returns */
    return 1;
}

static int fpCommandAllowed(struct serverCommand *cmd) {
    if (!cmd) return 0; /* unknown command: the main path replies (and runs the host:/post check) */
    if (cmd->proc == pingCommand) return 1; /* fast-path clients are never in pubsub mode */
    if (!(cmd->flags & (CMD_WRITE | CMD_READONLY))) return 0;
    if (cmd->flags & (CMD_BLOCKING | CMD_PUBSUB | CMD_ADMIN | CMD_NOSCRIPT | CMD_NO_MULTI | CMD_NO_ASYNC_LOADING |
                      CMD_ALLOW_BUSY | CMD_TOUCHES_ARBITRARY_KEYS | CMD_MODULE))
        return 0; /* CMD_MODULE runs module code with its own context: keep it on main */
    return 1;
}

static void fpSubmit(fpThread *t) {
    cmdBatch *b = t->cur;
    if (!b || b->count == 0) return;
    t->cur = NULL;
    spscEnqueue(&t->submit, b, true);
    ioThreadsWakeMain();
    t->inflight++;
    if (t->inflight > t->inflight_hwm) t->inflight_hwm = t->inflight; /* peak batch-queue depth */
    t->batches++;
}

void fastpathSubmitPending(int tid) {
    fpThread *t = &fp_threads[tid];
    if (!t->cur || t->cur->count == 0 || t->cur_hold || t->quiescing || spscBacklog(&t->submit) != 0) return;
    /* The hold amortizes per-batch work while bounding latency. */
    if (server.io_batch_hold_us > 0 && getMonotonicUs() - t->cur->opened_us < (monotime)server.io_batch_hold_us) return;
    fpSubmit(t);
}

/* The IO owner assigns a compact private slot before it can publish commands for the connection. */
static void fpOwnerSlotAssign(fpThread *t, client *c) {
    uint32_t index;
    if (t->owner_slots_free != FP_OWNER_SLOT_NONE) {
        index = t->owner_slots_free;
        t->owner_slots_free = t->owner_slots[index].next_free;
    } else {
        if (t->owner_slots_len == t->owner_slots_cap) {
            uint32_t cap = t->owner_slots_cap ? t->owner_slots_cap * 2 : 64;
            serverAssert(cap > t->owner_slots_cap);
            t->owner_slots = zrealloc(t->owner_slots, (size_t)cap * sizeof(*t->owner_slots));
            t->owner_slots_cap = cap;
        }
        index = t->owner_slots_len++;
    }
    ClientControl *cc = c->control;
    if (++cc->generation == 0) cc->generation++;
    t->owner_slots[index].control = cc;
    t->owner_slots[index].connection = c;
    t->owner_slots_used++;
    c->fp_owner_slot = index;
}

static void fpOwnerSlotRelease(fpThread *t, client *c) {
    uint32_t index = c->fp_owner_slot;
    serverAssert(index < t->owner_slots_len);
    serverAssert(t->owner_slots[index].control == c->control);
    serverAssert(t->owner_slots[index].connection == c);
    t->owner_slots[index].control = NULL;
    t->owner_slots[index].next_free = t->owner_slots_free;
    t->owner_slots_free = index;
    serverAssert(t->owner_slots_used > 0);
    t->owner_slots_used--;
    c->fp_owner_slot = FP_OWNER_SLOT_NONE;
}

/* Registry membership and the private resolution slot change only with ownership. */
static void fpRegister(fpThread *t, client *c) {
    fpOwnerSlotAssign(t, c);
    listLinkNodeTail(&t->owned, &c->io_owner_node);
    ClientControl *cc = c->control;
    raxInsert(t->registry, (unsigned char *)&cc, sizeof(cc), c, NULL);
}

static void fpUnregister(fpThread *t, client *c) {
    listUnlinkNode(&t->leaving, &c->io_owner_node);
    ClientControl *cc = c->control;
    raxRemove(t->registry, (unsigned char *)&cc, sizeof(cc), NULL);
    fpOwnerSlotRelease(t, c);
}

/* Resolve only through the owning thread's private table, validating both slot and generation. */
static client *fpResolve(fpThread *t, const ClientHandle *h) {
    if (fastpathHandleStale(h) || h->owner_slot >= t->owner_slots_len) return NULL;
    fpOwnerSlot *slot = &t->owner_slots[h->owner_slot];
    if (slot->control != h->control) return NULL;
    return slot->connection;
}

/* True while this thread still owns the connection. */
static int fpOwns(fpThread *t, client *c) {
    ClientControl *cc = c->control;
    return cc && raxFind(t->registry, (unsigned char *)&cc, sizeof(cc), NULL);
}

static int fpCurHasClient(fpThread *t, client *c) {
    if (!t->cur) return 0;
    for (int i = 0; i < t->cur->count; i++)
        if (fpResolve(t, &t->cur->e[i].handle) == c) return 1;
    return 0;
}

/* Handoff waits until every published command for the client returns. Entries of the client still
 * in the unpublished batch may only follow commands it holds, so hold_cur keeps them from publishing. */
static void fpBeginLeave(fpThread *t, client *c, int state, int hold_cur) {
    if (c->control->lifecycle != FP_ACTIVE) return;
    c->control->lifecycle = state;
    epoll_ctl(ioThreadEpollFd(c->io_tid), EPOLL_CTL_DEL, c->conn->fd, NULL);
    if (c->fp_deferred) {
        listUnlinkNode(&t->deferred, &c->fp_defer_node);
        c->fp_deferred = 0;
    }
    listUnlinkNode(&t->owned, &c->io_owner_node);
    listLinkNodeTail(&t->leaving, &c->io_owner_node);
    if (hold_cur && fpCurHasClient(t, c)) t->cur_hold = 1;
}

/* Owner-only lifecycle transition for one client; the requests bitmask is read with acquire to pair with the publisher's release. */
static void fpExecuteRequests(fpThread *t, client *c) {
    ClientControl *cc = c->control;
    uint32_t reqs = atomic_load_explicit(&cc->requests, memory_order_acquire);
    if (reqs == 0) return;
    uint32_t win = fpRequestWinner(reqs);
    if (win == 0) return; /* only unknown future bits set: neither leave the client nor clear them */
    if (cc->lifecycle == FP_ACTIVE) fpBeginLeave(t, c, (win & CC_REQ_TERMINAL) ? FP_CLOSING : FP_LEAVING, 1);
    else if ((win & CC_REQ_TERMINAL) && cc->lifecycle == FP_LEAVING)
        cc->lifecycle = FP_CLOSING;
    atomic_fetch_and_explicit(&cc->requests, ~win, memory_order_release);
}

static void fpAppendEntry(cmdBatch *b, client *c, robj **argv, int argc, int argv_len, size_t argv_len_sum,
                          unsigned long long input_bytes, struct serverCommand *cmd, int slot, int read_flags) {
    cmdEntry *e = &b->e[b->count++];
    e->handle = fastpathHandleFor(c);
    e->argv = argv;
    e->argc = argc;
    e->argv_len = argv_len;
    e->argv_len_sum = argv_len_sum;
    e->input_bytes = input_bytes;
    e->cmd = cmd;
    e->slot = slot;
    e->read_flags = read_flags;
    e->db = c->db;
    e->resp = (uint8_t)c->resp;
    e->origin.client_id = c->id;
    e->origin.principal = c->user;
    e->origin.authenticated = c->flag.authenticated;
    e->origin.peer = c->fp_peer;
    e->origin.local = c->fp_local;
    e->origin.conn_type = c->fp_conn_type;
    e->origin.acl_epoch_seen = c->acl_epoch_seen; /* acl-offload: epoch this command's verdict was tagged under */
    e->reply_off = e->reply_len = 0;
    e->reply_big = NULL;
    e->reply_big_len = 0;
    e->reply_wire = 0;
    e->reply_encoded = 0;
    e->requeued = 0;
    e->woff = c->woff; /* prior causal state in; main executes against it and returns the resulting offset */
    c->fp_inflight++;
    c->fp_inflight_argv += fpArgvBytes(argv_len_sum, argc);
}

/* The first unsupported command and all successors remain queued for main. */
static void fpHarvest(fpThread *t, int tid, client *c) {
    int leave = 0;
    int max = server.io_batch_commands;
    int acl_stop = 0; /* acl-offload: stop tagging for the rest of this read after an identity/db-changing command */
    cmdQueue *q = &c->cmd_queue;

    if (!fpDynamicGate()) {
        leave = 1; /* a global gate is closed: hand this client to main */
        t->fb_gate++;
    }

    if (!leave && c->argc > 0 && (c->read_flags & READ_FLAGS_PARSING_COMPLETED)) {
        if ((c->read_flags & READ_FLAGS_ERROR_MASK) || !fpCommandAllowed(c->parsed_cmd)) {
            leave = 1;
            if (c->read_flags & READ_FLAGS_ERROR_MASK) t->fb_error++; else t->fb_ineligible++;
        } else {
            if (!acl_stop) {
                aclOffloadTagCommand(c, c->parsed_cmd, c->argv, c->argc, c->db->id, &c->read_flags);
                if (aclOffloadShouldStopTagging(c->parsed_cmd)) acl_stop = 1;
            }
            if (!t->cur) t->cur = fpAllocBatch(t, tid);
            fpAppendEntry(t->cur, c, c->argv, c->argc, c->argv_len, c->argv_len_sum, c->net_input_bytes_curr_cmd,
                          c->parsed_cmd, c->slot, c->read_flags);
            c->argv = NULL;
            c->argc = 0;
            c->argv_len = 0;
            c->argv_len_sum = 0;
            c->parsed_cmd = NULL;
            c->read_flags = 0;
            c->net_input_bytes_curr_cmd = 0; /* the parser accumulates; resetClient never runs for this client */
            if (t->cur->count >= max) fpSubmit(t);
        }
    } else if (c->read_flags & READ_FLAGS_ERROR_MASK) {
        leave = 1;
        t->fb_error++;
    }

    while (!leave && q->off < q->len) {
        parsedCommand *p = &q->cmds[q->off];
        if (p->read_flags & READ_FLAGS_PARSING_NEGATIVE_MBULK_LEN) {
            /* An empty command ends the queued run: skip it, as main does, and parse on from a fresh request type. */
            q->off++;
            c->reqtype = 0;
            continue;
        }
        int complete = p->read_flags & READ_FLAGS_PARSING_COMPLETED;
        if (!complete && !(p->read_flags & READ_FLAGS_ERROR_MASK)) break; /* trailing partial */
        if ((p->read_flags & READ_FLAGS_ERROR_MASK) || !fpCommandAllowed(p->cmd)) {
            leave = 1;
            if (p->read_flags & READ_FLAGS_ERROR_MASK) t->fb_error++; else t->fb_ineligible++;
            break;
        }
        if (!acl_stop) {
            aclOffloadTagCommand(c, p->cmd, p->argv, p->argc, c->db->id, &p->read_flags);
            if (aclOffloadShouldStopTagging(p->cmd)) acl_stop = 1;
        }
        if (!t->cur) t->cur = fpAllocBatch(t, tid);
        fpAppendEntry(t->cur, c, p->argv, p->argc, p->argv_len, p->argv_len_sum, p->input_bytes, p->cmd, p->slot,
                      p->read_flags);
        q->off++;
        if (t->cur->count >= max) fpSubmit(t);
    }

    if (leave) {
        /* Promote the first queued command so the main path can resume parsing. */
        if (c->argc == 0 && q->off < q->len) {
            parsedCommand *p = &q->cmds[q->off++];
            c->argv = p->argv, c->argc = p->argc, c->argv_len = p->argv_len, c->argv_len_sum = p->argv_len_sum;
            c->net_input_bytes_curr_cmd = p->input_bytes, c->parsed_cmd = p->cmd, c->slot = p->slot;
            c->read_flags |= p->read_flags;
        }
        fpBeginLeave(t, c, FP_LEAVING, 0);
        return;
    }

    /* A trailing partial command continues in c->argv on the next read. */
    if (q->off < q->len) {
        parsedCommand *p = &q->cmds[q->off++];
        serverAssert(q->off == q->len);
        c->argv = p->argv, c->argc = p->argc, c->argv_len = p->argv_len, c->argv_len_sum = p->argv_len_sum;
        c->net_input_bytes_curr_cmd = p->input_bytes, c->parsed_cmd = NULL, c->slot = -1;
        c->read_flags = p->read_flags;
    }
    q->off = q->len = 0;
}

static size_t fpSend(fpThread *t, client *c, struct iovec *iov, int iovcnt);

/* Reads at the head of what a client just sent execute here, on its owning IO thread: a
 * contiguous prefix of GETs is answered from the keyspace under D+ version validation and the
 * replies go straight to the socket. The first command that cannot be executed here, and every
 * command after it, goes to main in the batch, so a read never overtakes an earlier write of the
 * same connection. For the same reason nothing is executed here while the client still has
 * commands pending on main. */
static void fpSpeculate(fpThread *t, int tid, client *c) {
    if (c->fp_inflight != 0 || c->argc == 0 || !(c->read_flags & READ_FLAGS_PARSING_COMPLETED)) return;
    int n = dplusSpeculateBatch(c, tid);
    if (n <= 0) return;
    dplusConsumeSpeculated(c, n, tid);
    t->speculated += n;
    struct iovec iov = {.iov_base = c->buf, .iov_len = c->bufpos};
    c->bufpos = 0;
    size_t released = fpSend(t, c, &iov, 1);
    fpControlChargeOwnBytes(c->control, iov.iov_len - released);
}

static void fpRead(fpThread *t, int tid, client *c) {
    c->read_flags = 0; /* authenticated at admission, not replicated; parse state lives in multibulklen/bulklen */
    readToQueryBuf(c);
    t->reads++;
    if (c->nread <= 0) {
        /* The client may still point at this thread's shared query buffer:
         * give it back before anyone else can free it with the client. */
        trimClientQueryBuffer(c);
        if (c->nread < 0 && connGetState(c->conn) == CONN_STATE_CONNECTED) return; /* EAGAIN */
        /* EOF or error: the client closes. Nothing more is read; entries in
         * flight return first, then the main thread frees the client. */
        fpBeginLeave(t, c, FP_CLOSING, 0);
        return;
    }
    t->net_input_bytes += c->nread;
    c->net_input_bytes += c->nread;
    fpControlSetLastInteraction(c->control, server.unixtime); /* IO owner publishes the idle stamp; main reads only the control, not c->last_interaction */
    if (c->read_flags & READ_FLAGS_QB_LIMIT_REACHED) {
        trimClientQueryBuffer(c);
        fpBeginLeave(t, c, FP_LEAVING, 0);
        return;
    }
    /* The multibulk parser queues the pipelined RESP commands behind the first one, but the
     * inline parser stops after one command and a queue run stops at the first non-RESP byte.
     * The main path drains the leftover bytes by looping in processInputBuffer; do the same
     * here, or the rest of the packet waits for the connection's next read event. */
    for (;;) {
        size_t pos = c->qb_pos;
        parseInputBuffer(c);
        int completed = c->read_flags & READ_FLAGS_PARSING_COMPLETED;
        if (c->read_flags & READ_FLAGS_PARSING_NEGATIVE_MBULK_LEN) {
            /* A multibulk count below one is an empty command: skip it and parse on, as main does. */
            c->reqtype = 0;
            completed = 1;
        }
        prepareCommandQueue(c);
        aclOffloadBeginRead(c); /* snapshot the ACL epoch these commands are tagged under */
        fpSpeculate(t, tid, c);
        if (c->control->lifecycle == FP_ACTIVE) fpHarvest(t, tid, c);
        /* Stop once the client has left the fast path, a partial command needs more
         * bytes, a parse error is pending for main, or the buffer is drained. */
        if (c->control->lifecycle != FP_ACTIVE) break;
        if (!completed || c->argc > 0 || (c->read_flags & READ_FLAGS_ERROR_MASK)) break;
        if (c->querybuf == NULL || c->qb_pos >= sdslen(c->querybuf) || c->qb_pos == pos) break;
        c->read_flags = 0;
    }
    trimClientQueryBuffer(c);
    fpPublishInput(t, c);
}

/* Clients turned away by the thread's in-flight cap wait in arrival order. The socket stays
 * level-triggered readable, but the poll only enqueues it; reads come from the FIFO head as
 * returned batches free capacity, so service order does not follow the kernel's ready list. */
static void fpDefer(fpThread *t, client *c) {
    if (c->fp_deferred) return;
    c->fp_deferred = 1;
    t->deferrals++;
    listLinkNodeTail(&t->deferred, &c->fp_defer_node);
}

static void fpServeDeferred(fpThread *t, int tid) {
    while (listLength(&t->deferred) > 0 && t->inflight < server.io_batch_inflight) {
        client *c = listNodeValue(listFirst(&t->deferred));
        listUnlinkNode(&t->deferred, &c->fp_defer_node);
        c->fp_deferred = 0;
        if (c->fp_inflight >= FP_CLIENT_INFLIGHT_MAX) continue; /* still readable; the poll brings it back */
        fpRead(t, tid, c);
    }
}

void fastpathClientReadable(int tid, client *c) {
    fpThread *t = &fp_threads[tid];
    if (c->control->lifecycle != FP_ACTIVE) return;
    if (c->fp_deferred) return; /* already waiting its turn */
    if (c->fp_inflight >= FP_CLIENT_INFLIGHT_MAX) return;
    if (t->inflight >= server.io_batch_inflight || listLength(&t->deferred) > 0) {
        fpDefer(t, c);
        fpServeDeferred(t, tid);
        return;
    }
    fpRead(t, tid, c);
}

static void fpEnableWriteInterest(client *c, int on) {
    struct epoll_event ev = {.events = on ? (EPOLLIN | EPOLLOUT) : EPOLLIN, .data.ptr = c};
    epoll_ctl(ioThreadEpollFd(c->io_tid), EPOLL_CTL_MOD, c->conn->fd, &ev);
}

static int fpFlushOut(fpThread *t, client *c) {
    while (c->fp_out && sdslen(c->fp_out) > 0) {
        ssize_t n = write(c->conn->fd, c->fp_out, sdslen(c->fp_out));
        if (n <= 0) {
            if (n < 0 && (errno == EAGAIN || errno == EINTR)) return 0;
            /* Fatal socket: force FP_CLOSING so fpFinishLeaving stops retrying; fpBeginLeave no-ops once past FP_ACTIVE. */
            if (c->control->lifecycle == FP_ACTIVE) fpBeginLeave(t, c, FP_CLOSING, 0);
            else c->control->lifecycle = FP_CLOSING;
            return 0;
        }
        t->net_output_bytes += n;
        c->net_output_bytes += n;
        t->writes++;
        fpControlReleaseBytes(c->control, (size_t)n); /* these bytes left the retained set */
        sdsrange(c->fp_out, n, -1);
    }
    return 1;
}

/* Total logical bytes across the reply iovecs; computed only on the rare discard paths, not per send. */
static size_t fpIovLen(const struct iovec *iov, int iovcnt) {
    size_t total = 0;
    for (int i = 0; i < iovcnt; i++) total += iov[i].iov_len;
    return total;
}

/* Returns the reply bytes that left the retained set: written to the socket or discarded whole on a fatal write; buffered bytes stay charged behind any existing residue. */
static size_t fpSend(fpThread *t, client *c, struct iovec *iov, int iovcnt) {
    if (c->control->lifecycle == FP_CLOSING) return fpIovLen(iov, iovcnt); /* the closing client drops these */
    if (c->fp_out && sdslen(c->fp_out) > 0) {
        for (int i = 0; i < iovcnt; i++) c->fp_out = sdscatlen(c->fp_out, iov[i].iov_base, iov[i].iov_len);
        return 0;
    }
    ssize_t written = writev(c->conn->fd, iov, iovcnt);
    if (written < 0) {
        if (errno != EAGAIN && errno != EINTR) {
            /* Fatal write must reach FP_CLOSING even past FP_ACTIVE, so a discarded reply cannot hand off a live connection; fpBeginLeave no-ops once leaving. */
            if (c->control->lifecycle == FP_ACTIVE) fpBeginLeave(t, c, FP_CLOSING, 0);
            else c->control->lifecycle = FP_CLOSING;
            return fpIovLen(iov, iovcnt); /* whole group discarded, released once by the caller */
        }
        written = 0;
    }
    t->net_output_bytes += written;
    c->net_output_bytes += written;
    t->writes++;
    size_t rem = (size_t)written;
    for (int i = 0; i < iovcnt; i++) {
        if (rem >= iov[i].iov_len) {
            rem -= iov[i].iov_len;
            continue;
        }
        if (!c->fp_out) c->fp_out = sdsempty();
        c->fp_out = sdscatlen(c->fp_out, (char *)iov[i].iov_base + rem, iov[i].iov_len - rem);
        rem = 0;
    }
    if (c->fp_out && sdslen(c->fp_out) > 0) fpEnableWriteInterest(c, 1);
    return (size_t)written;
}

void fastpathClientWritable(int tid, client *c) {
    fpThread *t = &fp_threads[tid];
    if (fpFlushOut(t, c)) fpEnableWriteInterest(c, 0);
}

static parsedCommand fpEntryToParsed(cmdEntry *e) {
    parsedCommand p = {.read_flags = e->read_flags,
                       .argc = e->argc,
                       .argv = e->argv,
                       .argv_len = e->argv_len,
                       .slot = e->slot,
                       .argv_len_sum = e->argv_len_sum,
                       .input_bytes = e->input_bytes,
                       .cmd = e->cmd};
    e->argv = NULL; /* the queue owns it now */
    return p;
}

/* Entries main returned unexecuted go back in front of anything parsed since, after any
 * already held; the client then leaves so the main path runs them with its own semantics. */
static void fpRequeue(fpThread *t, client *c, cmdEntry *e, int n) {
    cmdQueue *q = &c->cmd_queue;
    int qheld = c->fp_held ? c->fp_held - 1 : 0; /* held commands queued behind c->argv */
    int rest = q->len - q->off - qheld;
    int first = c->fp_held ? 0 : 1; /* with nothing held, e[0] becomes c->argv */
    parsedCommand cur;
    int has_cur = !c->fp_held && c->argc > 0; /* a promoted command or a trailing partial, parsed after the entries */
    if (has_cur) {
        cur = (parsedCommand){.read_flags = c->read_flags,
                              .argc = c->argc,
                              .argv = c->argv,
                              .argv_len = c->argv_len,
                              .slot = c->slot,
                              .argv_len_sum = c->argv_len_sum,
                              .input_bytes = c->net_input_bytes_curr_cmd,
                              .cmd = c->parsed_cmd};
    }
    parsedCommand *cmds = zmalloc(sizeof(parsedCommand) * (qheld + (n - first) + has_cur + rest));
    int k = 0;
    if (qheld) memcpy(cmds, q->cmds + q->off, sizeof(parsedCommand) * qheld);
    k += qheld;
    for (int i = first; i < n; i++) cmds[k++] = fpEntryToParsed(&e[i]);
    if (has_cur) cmds[k++] = cur;
    if (rest) memcpy(cmds + k, q->cmds + q->off + qheld, sizeof(parsedCommand) * rest);
    k += rest;
    zfree(q->cmds);
    q->cmds = cmds;
    q->off = 0;
    q->len = q->cap = k;
    if (first) {
        parsedCommand head = fpEntryToParsed(&e[0]);
        c->argv = head.argv, c->argc = head.argc, c->argv_len = head.argv_len, c->argv_len_sum = head.argv_len_sum;
        c->net_input_bytes_curr_cmd = head.input_bytes, c->parsed_cmd = head.cmd, c->slot = head.slot;
        c->read_flags = head.read_flags;
    }
    c->fp_held += n;
    fpBeginLeave(t, c, FP_LEAVING, 1);
}

#define FP_OUT_IOV 128
#define FP_OUT_PREFIX (LONG_STR_SIZE + 3)

/* A strand's reply segments, sent through fpSend each time the vector fills. Once a send leaves residue,
 * later segments queue behind it in fp_out, so splitting a strand keeps its byte order. */
typedef struct fpOut {
    fpThread *t;
    client *c;
    int n, npfx;
    size_t released;
    struct iovec iov[FP_OUT_IOV];
    char prefix[FP_OUT_IOV][FP_OUT_PREFIX];
} fpOut;

static void fpOutFlush(fpOut *o) {
    if (o->n) o->released += fpSend(o->t, o->c, o->iov, o->n);
    o->n = o->npfx = 0;
}

static void fpOutAdd(void *ctx, const char *p, size_t len, int transient) {
    fpOut *o = ctx;
    if (len == 0) return;
    if (o->n == FP_OUT_IOV) fpOutFlush(o);
    if (transient) {
        serverAssert(len <= FP_OUT_PREFIX);
        memcpy(o->prefix[o->npfx], p, len);
        p = o->prefix[o->npfx++];
    }
    o->iov[o->n].iov_base = (void *)p;
    o->iov[o->n].iov_len = len;
    o->n++;
}

/* Consecutive entries for one client share a writev. */
static void fpDeliverBatch(fpThread *t, cmdBatch *b) {
    fpOut out;
    int i = 0;
    while (i < b->count) {
        client *c = fpResolve(t, &b->e[i].handle);
        ClientControl *cc = b->e[i].handle.control;
        int j = i;
        int requeued = 0;
        long long strand_woff = 0;
        size_t strand_argv = 0;
        struct serverCommand *strand_last = NULL;
        while (j < b->count && b->e[j].handle.control == cc) {
            cmdEntry *e = &b->e[j];
            strand_argv += fpArgvBytes(e->argv_len_sum, e->argc);
            if (!e->requeued && e->cmd) strand_last = e->cmd;
            if (e->requeued) requeued++;
            if (!e->requeued && e->woff > strand_woff) strand_woff = e->woff; /* highest offset any executed entry reached */
            j++;
        }
        if (c) {
            /* Apply the write offset to the origin before delivery, while the IO owner still holds it
             * (ACTIVE, or LEAVING for the WAIT/WAITAOF requeue); not CLOSING/DETACHED. */
            uint8_t lc = c->control->lifecycle;
            if (strand_woff > c->woff && (lc == FP_ACTIVE || lc == FP_LEAVING)) c->woff = strand_woff;
            out.t = t, out.c = c, out.n = out.npfx = 0, out.released = 0;
            for (int k = i; k < j; k++) {
                cmdEntry *e = &b->e[k];
                if (e->requeued) continue;
                char *region = e->reply_big ? e->reply_big : b->arena + e->reply_off;
                size_t len = e->reply_big ? e->reply_big_len : e->reply_len;
                if (e->reply_encoded) replyRegionWalk(region, len, fpOutAdd, &out);
                else fpOutAdd(&out, region, len, 0);
            }
            fpOutFlush(&out);
            fpControlReleaseBytes(cc, out.released); /* resolved: the handle is current, so release directly */
            c->fp_inflight -= (j - i);
            c->fp_inflight_argv -= strand_argv;
            c->commands_processed += (j - i) - requeued;
            if (strand_last) c->lastcmd = strand_last; /* CLIENT LIST cmd= */
            if (requeued) {
                t->rq_gate += requeued; /* main handed these back unexecuted (a gate closed after admission) */
                fpRequeue(t, c, &b->e[j - requeued], requeued); /* main stops executing a client at its first held entry */
            }
            fpPublishInput(t, c);
        } else {
            /* A current handle always resolves in flight, so c==NULL implies stale: never write a reused control, but still balance a current-but-unresolved strand in release builds. */
            const ClientHandle *h = &b->e[i].handle;
            int stale = fastpathHandleStale(h);
            debugServerAssert(stale);
            if (!stale) {
                size_t bytes = 0;
                for (int k = i; k < j; k++) bytes += fpEntryReplyBytes(&b->e[k]);
                fpControlReleaseBytes(h->control, bytes);
            }
        }
        i = j;
    }
    for (int k = 0; k < b->count; k++) {
        cmdEntry *e = &b->e[k];
        if (e->reply_big) zfree(e->reply_big);
        /* Terminal frees of what main did not keep, on the thread that
         * allocated it; no free traffic through the shared inbox. */
        if (e->argv) {
            for (int j = 0; j < e->argc; j++)
                if (e->argv[j]) decrRefCount(e->argv[j]);
            zfree(e->argv);
            e->argv = NULL;
        }
    }
}

/* Clients that stopped reading and now have nothing in flight are handed to
 * the main thread: to be freed (closing) or taken over (leaving). What the
 * socket accepts is written here first; the residue goes to main with the
 * client, so a slow reader cannot hold back the hand-off. */
static void fpFinishLeaving(fpThread *t) {
    if (listLength(&t->leaving) == 0) return;
    int open = atomic_load_explicit(&t->role, memory_order_relaxed) == FP_ROLE_OPEN;
    listNode *ln = t->leaving.head;
    while (ln) {
        listNode *next = ln->next;
        client *c = listNodeValue(ln);
        ln = next;
        if (c->fp_inflight > 0) continue;
        if (c->control->lifecycle == FP_LEAVING && open) fpFlushOut(t, c); /* a fatal write turns it into a close */
        /* Residue leaves the retained set exactly once here, posted before the hand-off so fastpathHandoffDone never re-releases it. */
        if (c->fp_out && sdslen(c->fp_out) > 0) fpControlReleaseBytes(c->control, sdslen(c->fp_out));
        fpUnregister(t, c);
        sendToMainThread(c, c->control->lifecycle == FP_CLOSING ? JOB_RES_FP_CLOSE : JOB_RES_FP_HANDOFF);
    }
}

/* Unpublished entries of clients that left go back to them in order; a closing client's are dropped.
 * Only valid with nothing in flight, since anything published for those clients precedes them. */
static void fpCancelLeavingInCur(fpThread *t) {
    cmdBatch *b = t->cur;
    serverAssert(t->inflight == 0);
    t->cur_hold = 0;
    if (!b) return;
    cmdEntry grp[IO_BATCH_MAX];
    int kept = 0;
    for (int i = 0; i < b->count; i++) {
        ClientControl *cc = b->e[i].handle.control;
        if (cc == NULL) continue; /* already moved into its client's queue */
        client *c = fpResolve(t, &b->e[i].handle);
        if (c && c->control->lifecycle == FP_ACTIVE) {
            b->e[kept++] = b->e[i];
            continue;
        }
        int n = 0;
        for (int j = i; j < b->count; j++) {
            if (b->e[j].handle.control != cc) continue;
            grp[n++] = b->e[j];
            b->e[j].handle.control = NULL;
        }
        if (c) {
            c->fp_inflight -= n;
            for (int k = 0; k < n; k++) c->fp_inflight_argv -= fpArgvBytes(grp[k].argv_len_sum, grp[k].argc);
        }
        if (c && c->control->lifecycle == FP_CLOSING) {
            for (int k = 0; k < n; k++) {
                for (int a = 0; a < grp[k].argc; a++) decrRefCount(grp[k].argv[a]);
                zfree(grp[k].argv);
            }
        } else if (c) {
            fpRequeue(t, c, grp, n);
        }
    }
    b->count = kept;
    if (kept == 0) {
        fpRecycleBatch(t, b);
        t->cur = NULL;
    }
}

/* Quiesce, observed once: every reading client leaves; the batch under assembly is
 * cancelled once nothing is in flight; DRAINED follows the last hand-off. */
static void fpQuiesceStep(fpThread *t) {
    if (!t->quiescing) {
        t->quiescing = 1;
        listNode *ln = t->owned.head;
        while (ln) {
            listNode *next = ln->next;
            fpBeginLeave(t, listNodeValue(ln), FP_LEAVING, 1);
            ln = next;
        }
    }
    if (t->inflight == 0 && t->cur) fpCancelLeavingInCur(t);
    fpFinishLeaving(t);
    if (t->inflight == 0 && t->cur == NULL && listLength(&t->owned) == 0 && listLength(&t->leaving) == 0) {
        t->quiescing = 0;
        atomic_store_explicit(&t->role, FP_ROLE_DRAINED, memory_order_release);
    }
}

/* Owner-side execution across leaving then owned, so a request that moves an owned client into leaving is not revisited this pass. */
static void fpExecuteTrackedRequests(fpThread *t) {
    listNode *ln = t->leaving.head;
    while (ln) {
        listNode *next = ln->next;
        fpExecuteRequests(t, listNodeValue(ln));
        ln = next;
    }
    ln = t->owned.head;
    while (ln) {
        listNode *next = ln->next;
        fpExecuteRequests(t, listNodeValue(ln));
        ln = next;
    }
}

/* Gated by the publisher's signal, cleared with an exchange before the walk so a request published during it re-arms and is caught next pass. */
static void fpDrainRequests(fpThread *t) {
    if (!atomic_exchange_explicit(&t->req_pending, 0, memory_order_acquire)) return;
    fpExecuteTrackedRequests(t);
}

/* Pre-delivery: execute requests only for the current controls this batch names, resolving each through the owner table so a stale handle is skipped and a reused control is never written. */
static void fpExecuteBatchRequests(fpThread *t, cmdBatch *b) {
    int i = 0;
    while (i < b->count) {
        ClientControl *cc = b->e[i].handle.control;
        client *c = fpResolve(t, &b->e[i].handle);
        int j = i + 1;
        while (j < b->count && b->e[j].handle.control == cc) j++;
        if (c) fpExecuteRequests(t, c);
        i = j;
    }
}

/* Attach and detach requests name clients by pointer only until the registry confirms ownership. */
static void fpTakeClient(fpThread *t, client *c) {
    fpRegister(t, c);
    if (atomic_load_explicit(&t->role, memory_order_relaxed) != FP_ROLE_OPEN) {
        fpBeginLeave(t, c, FP_LEAVING, 0); /* admitted as the role closed: straight back to main */
        return;
    }
    struct epoll_event ev = {.events = EPOLLIN, .data.ptr = c};
    if (epoll_ctl(ioThreadEpollFd(c->io_tid), EPOLL_CTL_ADD, c->conn->fd, &ev) != 0) fpBeginLeave(t, c, FP_LEAVING, 0);
}

#define FP_CRON_PERIOD_US 100000

/* clientsCron skips IO-owned clients, so their owner applies its buffer policies: a share of the clients
 * every pass, each about once a second, paused like serverCron by DEBUG PAUSE-CRON. */
static void fpOwnerCron(fpThread *t) {
    monotime now = getMonotonicUs();
    if (now - t->cron_at < FP_CRON_PERIOD_US) return;
    t->cron_at = now;
    if (server.pause_cron) return;
    mstime_t now_ms = mstime();
    for (size_t n = listLength(&t->owned) / 10 + 1; n > 0 && listLength(&t->owned) > 0; n--) {
        client *c = listNodeValue(listFirst(&t->owned));
        listRotateHeadToTail(&t->owned);
        if (c->control->lifecycle != FP_ACTIVE) continue;
        time_t last = atomic_load_explicit(&c->control->last_interaction, memory_order_relaxed);
        clientResizeQueryBuffer(c, server.unixtime - last);
        clientsCronResizeOutputBuffer(c, now_ms);
        fpPublishInput(t, c);
    }
}

int fastpathProcessReturns(int tid) {
    fpThread *t = &fp_threads[tid];
    if (t->ret.buffer == NULL) return 0;
    void *items[16];
    int total = 0;
    size_t n;
    while ((n = spscDequeueBatch(&t->ret, items, 16)) > 0) {
        for (size_t i = 0; i < n; i++) {
            uintptr_t v = (uintptr_t)items[i];
            if (v & FP_TAGS) {
                client *c = (client *)(v & ~FP_TAGS);
                if (v & FP_TAG_ATTACH) {
                    fpTakeClient(t, c);
                } else if (fpOwns(t, c)) {
                    if (c->control->lifecycle == FP_LEAVING)
                        c->control->lifecycle = FP_CLOSING; /* no reason left to flush its output */
                    fpBeginLeave(t, c, FP_CLOSING, 0);
                } /* else already handed back: main frees it once this request is consumed */
                continue;
            }
            cmdBatch *b = (cmdBatch *)v;
            if (b->release) { /* main dropped its references: the batch is free */
                b->release = 0;
                fpRecycleBatch(t, b);
                t->inflight--;
                continue;
            }
            /* Peeked, not cleared: a request against a control still behind an unconsumed attach in this ring must survive for the post-loop drain. */
            if (atomic_load_explicit(&t->req_pending, memory_order_acquire)) fpExecuteBatchRequests(t, b);
            fpDeliverBatch(t, b);
            if (b->nrefs) {
                /* Every referenced byte was sent or copied: main drops the references, still in flight. */
                b->release = 1;
                spscEnqueue(&t->submit, b, true);
                ioThreadsWakeMain();
                continue;
            }
            fpRecycleBatch(t, b);
            t->inflight--;
        }
        total += (int)n;
    }
    if (atomic_load_explicit(&t->role, memory_order_acquire) == FP_ROLE_QUIESCING) {
        fpDrainRequests(t); /* a CLOSE arriving mid-quiesce still upgrades a leaving client to closing */
        fpQuiesceStep(t);
        return total;
    }
    fpDrainRequests(t);
    if (t->cur_hold && t->inflight == 0) fpCancelLeavingInCur(t);
    fpFinishLeaving(t);
    fpServeDeferred(t, tid);
    fpOwnerCron(t);
    if (t->inflight == 0 && !t->cur && t->nfree > FP_FREELIST_IDLE) fpTrimFreelist(t);
    return total;
}

static client *fpExecutor(int tid) {
    client *ec = fp_exec_client[tid];
    if (ec) return ec;
    ec = createClient(NULL);
    ec->flag.executor = 1;
    ec->cur_tid = tid; /* argv frees are offloaded to the owning thread */
    fp_exec_client[tid] = ec;
    return ec;
}

/* Controls of clients main asked to close; their queued commands must not run, as with close_asap.
 * Keyed by the stable control pointer so main tests an entry by its handle without touching the connection. */
static rax *fp_detaching = NULL;

static int fpControlDetaching(ClientControl *cc) {
    return cc && fp_detaching && raxSize(fp_detaching) > 0 &&
           raxFind(fp_detaching, (unsigned char *)&cc, sizeof(cc), NULL);
}

/* The executor borrows argv and writes replies into the batch arena. */
static void fpExecute(client *ec, cmdBatch *b, cmdEntry *e) {
    char *saved_buf = ec->buf;
    size_t saved_usable = ec->buf_usable_size;
    user *principal = e->origin.principal;

    if (fpControlDetaching(e->handle.control)) goto release_argv; /* no reply: the IO thread is closing it */
    if (!fpDynamicGate()) {
        e->requeued = 1; /* a global gate closed since admission: main runs it under current eligibility */
        return;
    }
    if (server.maxmemory_clients && e->handle.control) {
        /* As on the main path, a client over the limit is evicted before its command runs. */
        FastpathLimitEntry *limit = e->handle.control->limit;
        fpMemAccountUpdateWith(limit, fpArgvBytes(e->argv_len_sum, e->argc));
        evictClients();
        if (limit->terminal_requested) goto release_argv;
    }

    if (principal->flags & USER_FLAG_RETIRED) {
        /* The IO thread may have tagged this command under the retired rule set after the epoch
         * bump that retired it: its ALLOW does not describe the live successor. */
        e->read_flags &= ~READ_FLAGS_ACL_ALLOWED;
        principal = ACLResolveUser(principal);
    }
    serverAssert(principal); /* a deleted user's clients are detaching */
    ec->user = principal;
    ec->flag.authenticated = ec->flag.ever_authenticated = e->origin.authenticated;

    ec->argv = e->argv;
    ec->argc = e->argc;
    ec->argv_len = e->argv_len;
    ec->argv_len_sum = e->argv_len_sum;
    ec->net_input_bytes_curr_cmd = e->input_bytes;
    ec->parsed_cmd = e->cmd;
    ec->slot = e->slot;
    ec->read_flags = e->read_flags;
    ec->acl_epoch_seen = e->origin.acl_epoch_seen; /* acl-offload: verdict valid only if this still matches the epoch */
    ec->db = e->db;
    ec->resp = e->resp;
    ec->origin = &e->origin;
    ec->name = e->handle.control ? e->handle.control->name : NULL; /* SLOWLOG, COMMANDLOG and ACL LOG name the origin */
    /* A replica redirects or serves the origin's commands by its CLIENT CAPA and READONLY mode. */
    ec->capa = e->handle.control ? e->handle.control->capa : 0;
    ec->flag.readonly = e->handle.control ? e->handle.control->readonly : 0;
    ec->buf = b->arena + b->arena_used;
    ec->buf_usable_size = b->arena_cap - b->arena_used;
    ec->bufpos = 0;
    ec->flag.buf_encoded = 0;
    ec->last_header = NULL;
    ec->flag.pending_command = 1;

    ec->woff = e->woff; /* execute against the origin's prior causal state, not another client's */
    processCommandAndResetClient(ec);
    e->woff = ec->woff; /* call() advanced it iff this command propagated; otherwise it stays the prior value */

    e->reply_off = (uint32_t)b->arena_used;
    e->reply_len = (uint32_t)ec->bufpos;
    b->arena_used += ec->bufpos;
    int encoded = replyIsEncoded(ec);
    if (listLength(ec->reply) > 0) {
        size_t total = 0;
        char *big;
        if (encoded) {
            big = replyFlattenEncoded(ec, &total);
        } else {
            total = ec->bufpos;
            listIter li;
            listNode *ln;
            listRewind(ec->reply, &li);
            while ((ln = listNext(&li))) total += ((clientReplyBlock *)listNodeValue(ln))->used;
            big = zmalloc(total);
            memcpy(big, ec->buf, ec->bufpos);
            size_t off = ec->bufpos;
            listRewind(ec->reply, &li);
            while ((ln = listNext(&li))) {
                clientReplyBlock *o = listNodeValue(ln);
                memcpy(big + off, o->buf, o->used);
                off += o->used;
            }
            listEmpty(ec->reply);
            ec->reply_bytes = 0;
        }
        b->arena_used -= ec->bufpos;
        e->reply_off = 0;
        e->reply_len = 0;
        e->reply_big = big;
        e->reply_big_len = (uint32_t)total;
    }
    if (encoded) {
        /* Copy-avoided strings stay referenced until the IO owner has sent or copied them. */
        char *region = e->reply_big ? e->reply_big : b->arena + e->reply_off;
        size_t len = e->reply_big ? e->reply_big_len : e->reply_len;
        e->reply_wire = replyRegionTakeRefs(region, len, &b->refs, &b->nrefs, &b->refs_cap);
        e->reply_encoded = 1;
    }
    ec->bufpos = 0;
    ec->flag.buf_encoded = 0;
    ec->last_header = NULL;
    ec->buf = saved_buf;
    ec->buf_usable_size = saved_usable;
    ec->name = NULL;
release_argv:
    /* IO threads receive only sole-reference argv objects for terminal frees. */
    for (int j = 0; j < e->argc; j++) {
        robj *o = e->argv[j];
        if (!o) continue;
        unsigned rc = objectGetRefcount(o);
        if (rc == 1) continue; /* sole reference: freed by the IO thread */
        if (rc < OBJ_FIRST_SPECIAL_REFCOUNT) decrRefCount(o);
        e->argv[j] = NULL;
    }
}

/* Overflowed detach requests take ring slots as they free up; the request's position is recorded then. */
static void fpRetFlushOverflow(fpThread *t) {
    int published = 0;
    while (listLength(t->ret_overflow) > 0 && spscFreeSlots(&t->ret) > 0) {
        listNode *ln = listFirst(t->ret_overflow);
        client *c = listNodeValue(ln);
        listDelNode(t->ret_overflow, ln);
        spscEnqueue(&t->ret, (void *)((uintptr_t)c | FP_TAG_DETACH), true);
        ClientControl *cc = c->control;
        raxInsert(fp_detaching, (unsigned char *)&cc, sizeof(cc), (void *)t->ret.tail_local, NULL);
        published = 1;
    }
    if (published) ioThreadWake((int)(t - fp_threads));
}

/* Hold a fully executed and charged batch in the thread's main-only durability FIFO instead of returning it;
 * released to the owner ring only after the beforeSleep AOF fsync. Ordering holds because once a thread holds
 * one batch every later batch of the same drain is held behind it, so the owner ring stays per-thread FIFO. */
static void fpHoldBatch(fpThread *t, cmdBatch *b) {
    b->pending_next = NULL;
    if (t->pending_tail) t->pending_tail->pending_next = b;
    else t->pending_head = b;
    t->pending_tail = b;
    t->pending_count++;
}

/* A thread saw a client alone exceed maxmemory-clients: re-account now, not at the next tick, so the
 * next eviction pass sees it. Returns 1 when it re-accounted. */
int fastpathServeMemHints(void) {
    if (ProcessingEventsWhileBlocked) return 0;
    int hinted = 0;
    for (int tid = 1; tid < fp_slots; tid++) {
        _Atomic int *hint = &fp_threads[tid].mem_hint;
        if (atomic_load_explicit(hint, memory_order_relaxed) && atomic_exchange_explicit(hint, 0, memory_order_acquire))
            hinted = 1;
    }
    if (!hinted || !server.maxmemory_clients) return 0;
    listIter li;
    listNode *ln;
    listRewind(&fp_limit_registry, &li);
    while ((ln = listNext(&li)) != NULL) fpMemAccountUpdate(listNodeValue(ln));
    return 1;
}

int fastpathDrain(void) {
    int total = 0;
    if (fastpathServeMemHints()) evictClients();
    int use_prefetch = prefetchBatchEnabled() && !ProcessingEventsWhileBlocked;
    /* While a global gate is closed every entry is handed back for the main path to run under current
     * eligibility. */
    int gated = !fpDynamicGate();
    int aof_always = (server.aof_state == AOF_ON || server.aof_state == AOF_WAIT_REWRITE) &&
                     server.aof_fsync == AOF_FSYNC_ALWAYS;
    /* io-batch-drain-us bounds how long a thin batch waits for amortization. */
    monotime deadline = server.io_batch_drain_us > 0 ? getMonotonicUs() + server.io_batch_drain_us : 0;
    int enough = server.io_batch_commands * 4;
again:
    for (int tid = 1; tid < fp_slots; tid++) {
        fpThread *t = &fp_threads[tid];
        if (t->submit.buffer == NULL) continue;
        if (unlikely(listLength(t->ret_overflow) > 0)) fpRetFlushOverflow(t);
        void *items[8];
        size_t n = spscDequeueBatch(&t->submit, items, 8);
        if (n == 0) continue;
        client *ec = fpExecutor(tid);
        for (size_t i = 0; i < n; i++) {
            cmdBatch *b = items[i];
            if (b->release) {
                replyReleaseRefs(b->refs, b->nrefs);
                b->nrefs = 0;
                spscEnqueue(&t->ret, b, false); /* carries no replies, so it may pass a held batch */
                continue;
            }
            if (use_prefetch && !gated) {
                getKeysResult result;
                initGetKeysResult(&result);
                int room = 1;
                ClientControl *last_control = NULL;
                for (int k = 0; k < b->count && room; k++) {
                    cmdEntry *e = &b->e[k];
                    ClientControl *cc = e->handle.control;
                    if (cc != last_control) {
                        __builtin_prefetch(cc);
                        __builtin_prefetch(&cc->reply_bytes_produced, 1, 1);
                        last_control = cc;
                    }
                    if (!e->cmd || (e->read_flags & READ_FLAGS_BAD_ARITY)) continue;
                    room = prefetchBatchAddCommand(e->cmd, e->argv, e->argc, e->db, e->slot, &result);
                }
                getKeysFreeResult(&result);
                prefetchBatchRun();
                prefetchBatchReset();
            }
            int hold = t->pending_count > 0; /* FIFO: never overtake a batch this thread already holds */
            if (likely(!aof_always && !hold)) {
                for (int k = 0; k < b->count; k++) fpExecute(ec, b, &b->e[k]);
            } else {
                for (int k = 0; k < b->count; k++) {
                    fpExecute(ec, b, &b->e[k]);
                    /* A non-requeued write dirtied the AOF covered by this fsync. */
                    if (aof_always && !b->e[k].requeued && b->e[k].cmd && (b->e[k].cmd->flags & CMD_WRITE)) hold = 1;
                }
                /* A read may have observed a write not yet fsynced; its reply waits for the same fsync. */
                if (aof_always && sdslen(server.aof_buf) > 0) hold = 1;
            }
            fpReplyChargeBatch(b);
            ec->origin = NULL;
            clientSetUser(ec, DefaultUser, 0); /* never keep a principal that may retire */
            total += b->count;
            if (hold) fpHoldBatch(t, b);
            else spscEnqueue(&t->ret, b, false);
        }
        if (spscCommit(&t->ret)) ioThreadWake(tid);
    }
    if (deadline && total < enough && getMonotonicUs() < deadline) goto again;
    return total;
}

/* See header. Called on main right after the beforeSleep AOF flush/fsync, which already made every held write
 * durable (or, on an always-policy write error, flushAppendOnlyFile exited before returning here); this then
 * mirrors normal reply semantics without inventing stronger behavior. Capacity: a held batch was drained out
 * of the submit ring and still counts in t->inflight, which the design bounds below the ret ring size, so its
 * one deferred enqueue never overflows and main never waits on the IO thread. */
void fastpathReleaseDurableReplies(void) {
    for (int tid = 1; tid < fp_slots; tid++) {
        fpThread *t = &fp_threads[tid];
        if (t->submit.buffer == NULL || t->pending_head == NULL) continue;
        for (cmdBatch *b = t->pending_head; b;) {
            cmdBatch *next = b->pending_next;
            b->pending_next = NULL;
            spscEnqueue(&t->ret, b, false);
            b = next;
        }
        t->pending_head = t->pending_tail = NULL;
        t->pending_count = 0;
        spscCommit(&t->ret); /* one publish per thread, matching the drain's release/commit ordering */
        ioThreadWake(tid);
    }
}

/* Main requests a close through the IO-owned return ring; the client stays allocated until the
 * ring's consumer passed the request, so the IO thread never meets a recycled pointer. */
void fastpathRequestDetach(client *c) {
    fpThread *t = &fp_threads[c->io_tid];
    if (c->flag.fp_detach_sent) return;
    c->flag.fp_detach_sent = 1;
    fastpathControlRequest(c->control, CC_REQ_CLOSE); /* record the terminal request on the control; the ring carries the pointer safely */
    fastpathControlPin(c, CC_PIN_DETACH); /* a detach record now sits in the ring; hold until the owner consumes it */
    t->detach_pending++;
    if (!fp_detaching) fp_detaching = raxNew();
    ClientControl *cc = c->control;
    raxInsert(fp_detaching, (unsigned char *)&cc, sizeof(cc), NULL, NULL);
    fpRetPublish(t, c, FP_TAG_DETACH);
    if (listLength(t->ret_overflow) == 0)
        raxInsert(fp_detaching, (unsigned char *)&cc, sizeof(cc), (void *)t->ret.tail_local, NULL);
}

/* True once the IO thread consumed the detach request; only then may main free the client. */
int fastpathDetachConsumed(client *c) {
    void *pos = NULL;
    if (!c->flag.fp_detach_sent) return 1;
    ClientControl *cc = c->control;
    if (!raxFind(fp_detaching, (unsigned char *)&cc, sizeof(cc), &pos) || pos == NULL) return 0;
    fpThread *t = &fp_threads[c->io_tid];
    if (atomic_load_explicit(&t->ret.head, memory_order_acquire) < (size_t)pos) return 0;
    raxRemove(fp_detaching, (unsigned char *)&cc, sizeof(cc), NULL);
    t->detach_pending--;
    c->flag.fp_detach_sent = 0;
    fastpathControlUnpin(c, CC_PIN_DETACH); /* the owner passed the record; nothing in the ring names the control now */
    return 1;
}

/* A client that left to authenticate returns once main has nothing further to do for it;
 * any other unsupported command keeps it on the main path, as before. */
static int fpQuiescent(client *c) {
    if (c->argc > 0 || c->flag.pending_command || c->cmd_queue.off < c->cmd_queue.len) return 0;
    if (c->querybuf && sdslen(c->querybuf) > c->qb_pos) return 0;
    if (c->io_read_state != CLIENT_IDLE || c->io_write_state != CLIENT_IDLE) return 0;
    return 1;
}

static int fpReadmit(client *c) {
    if (!fpQuiescent(c)) return 0;
    if (clientHasPendingReplies(c) && (writeToClient(c) != C_OK || clientHasPendingReplies(c))) return 0;
    if (!fastpathEligible(c)) return 0;
    if (c->flag.pending_write) {
        c->flag.pending_write = 0;
        listUnlinkNode(server.clients_pending_write, &c->clients_pending_write_node);
    }
    trimClientQueryBuffer(c);
    connSetReadHandler(c->conn, NULL);
    if (fastpathAttach(c) == C_OK) return 1;
    connSetReadHandler(c->conn, readQueryFromClient);
    return 0;
}

/* Ineligibility the client or the server can undo later (EXEC, UNWATCH, UNSUBSCRIBE, TRACKING OFF, unblock, gate). */
static int fpIneligibleTransient(client *c) {
    return c->flag.multi || fpWatching(c) || c->flag.pubsub || c->flag.tracking || c->flag.blocked ||
           c->flag.unblocked || !fpDynamicGate();
}

/* Called by main for a client it owns (partitioned or event-loop) after a command ran on it. Returns 1 once the
 * client is on the fast path; a client that is not yet quiescent, or only transiently ineligible, keeps the flag
 * and is retried after its next command. */
int fastpathTryReadmit(client *c) {
    if (c->flag.fastpath || c->flag.executor) {
        c->flag.fp_readmit = 0;
        return 0;
    }
    if (!fpQuiescent(c) || (c->flag.partitioned && c->flag.pending_read)) return 0;
    if (!fpSessionEligible(c)) {
        if (!fpIneligibleTransient(c)) c->flag.fp_readmit = 0;
        return 0;
    }
    c->flag.fp_readmit = 0;
    int was_partitioned = c->flag.partitioned;
    if (was_partitioned) unpartitionClient(c);
    if (fpReadmit(c)) return 1;
    if (was_partitioned && tryPartitionClient(c) == C_OK) connSetReadHandler(c->conn, NULL);
    return 0;
}

void fastpathHandoffDone(client *c, int closing) {
    fpThread *t = &fp_threads[c->io_tid];
    serverAssert(t->main_clients > 0);
    t->main_clients--;
    c->flag.fastpath = 0;
    /* Main is the owner again; publish the terminal state on the single source of truth. control is
     * still live here (reclaimed only at freeClient), so the read/write is safe. */
    c->control->owner_domain = CC_OWNER_MAIN;
    c->control->owner_tid = 0;
    c->control->lifecycle = FP_DETACHED;
    c->control->name = NULL; /* main owns the name again; SETNAME may now replace it */
    c->last_interaction = fastpathControlLastInteraction(c->control); /* acquire the IO owner's stamp back before normal idle-timeout maintenance resumes */
    fastpathControlUnpin(c, CC_PIN_OWNER); /* IO no longer owns it; drop the ownership pin as main takes over */
    fpLimitRegistryRemove(c); /* main owns it again; unlink before normal-client maintenance resumes */
    fastpath_clients--;
    ACLFastpathClientReturned(c);
    /* The IO owner already released this residue's fast-path charge at hand-off; here it only moves bytes. */
    sds out = c->fp_out;
    c->fp_out = NULL;
    /* A detach still in the ring keeps the client allocated: freeClientsInAsyncFreeQueue frees it once consumed. */
    if (c->flag.fp_detach_sent) {
        sdsfree(out);
        return;
    }
    if (closing || c->flag.close_asap) {
        sdsfree(out);
        freeClient(c); /* removes it from clients_to_close itself when close_asap is set */
        return;
    }
    connSetReadHandler(c->conn, readQueryFromClient);
    if (out) {
        /* Replies the IO thread could not write yet precede anything main produces for this client. */
        if (sdslen(out) > 0) addReplyProto(c, out, sdslen(out));
        sdsfree(out);
    }
    if (c->argc > 0 && (c->read_flags & READ_FLAGS_PARSING_COMPLETED) && !(c->read_flags & READ_FLAGS_ERROR_MASK))
        c->flag.pending_command = 1;
    if (c->read_flags & READ_FLAGS_ERROR_MASK) {
        handleParseError(c); /* replies, sets close_after_reply; the write path closes it */
        return;
    }
    c->flag.fp_readmit = 1; /* the command that sent it to main does not keep it there */
    if (processPendingCommandAndInputBuffer(c) != C_OK) return;
    if (c->flag.fp_readmit && fastpathTryReadmit(c)) return;
    beforeNextClient(c);
}

static long long fp_net_in_base = 0, fp_net_out_base = 0;

/* Bytes fast-path clients moved since the last CONFIG RESETSTAT, for the server-wide net totals. */
void fastpathNetBytes(long long *in, long long *out) {
    long long i = fp_retired[1], o = fp_retired[2];
    for (int k = 1; k < fp_slots; k++) {
        if (fp_threads[k].submit.buffer == NULL) continue;
        i += fp_threads[k].net_input_bytes;
        o += fp_threads[k].net_output_bytes;
    }
    *in = i - fp_net_in_base;
    *out = o - fp_net_out_base;
}

/* The IO-owned counters are never written by main; a reset moves the baseline instead. */
void fastpathResetNetStats(void) {
    long long in, out;
    fp_net_in_base = fp_net_out_base = 0;
    fastpathNetBytes(&in, &out);
    fp_net_in_base = in;
    fp_net_out_base = out;
}

void fastpathInfo(sds *info) {
    long long reads = fp_retired[0], in = fp_retired[1], out = fp_retired[2], writes = fp_retired[3],
              batches = fp_retired[4], deferrals = fp_retired[5], speculated = fp_retired[11];
    long long fb_gate = fp_retired[6], fb_ineligible = fp_retired[7], fb_error = fp_retired[8], rq_gate = fp_retired[9],
              inflight_hwm = fp_retired[10];
    long long pending = 0; /* live only: held batches never retire, so they are not summed from fp_retired */
    int open = 0, quiescing = 0;
    for (int i = 1; i < fp_slots; i++) {
        if (fp_threads[i].submit.buffer == NULL) continue;
        int role = fastpathWorkerRole(i);
        open += role == FP_ROLE_OPEN;
        quiescing += role == FP_ROLE_QUIESCING;
        reads += fp_threads[i].reads;
        in += fp_threads[i].net_input_bytes;
        out += fp_threads[i].net_output_bytes;
        writes += fp_threads[i].writes;
        batches += fp_threads[i].batches;
        deferrals += fp_threads[i].deferrals;
        speculated += fp_threads[i].speculated;
        fb_gate += fp_threads[i].fb_gate;
        fb_ineligible += fp_threads[i].fb_ineligible;
        fb_error += fp_threads[i].fb_error;
        rq_gate += fp_threads[i].rq_gate;
        if (fp_threads[i].inflight_hwm > inflight_hwm) inflight_hwm = fp_threads[i].inflight_hwm;
        pending += fp_threads[i].pending_count;
    }
    *info = sdscatprintf(*info,
                         "fastpath_clients:%zu\r\n"
                         "fastpath_workers_open:%d\r\n"
                         "fastpath_workers_quiescing:%d\r\n"
                         "fastpath_reads:%lld\r\n"
                         "fastpath_writes:%lld\r\n"
                         "fastpath_batches:%lld\r\n"
                         "fastpath_deferrals:%lld\r\n"
                         "fastpath_speculated:%lld\r\n"
                         "fastpath_pending_durable_batches:%lld\r\n"
                         "fastpath_net_input_bytes:%lld\r\n"
                         "fastpath_net_output_bytes:%lld\r\n"
                         "fastpath_fallback_gate:%lld\r\n"
                         "fastpath_fallback_ineligible:%lld\r\n"
                         "fastpath_fallback_error:%lld\r\n"
                         "fastpath_requeue_gate:%lld\r\n"
                         "fastpath_inflight_batches_peak:%lld\r\n",
                         fastpath_clients, open, quiescing, reads, writes, batches, deferrals, speculated, pending, in, out,
                         fb_gate, fb_ineligible, fb_error, rq_gate, inflight_hwm);
}
