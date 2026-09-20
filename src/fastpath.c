/* IO threads own fast-path clients; main only executes their published command batches. */

#include "server.h"
#include "fastpath.h"
#include "io_threads.h"
#include "memory_prefetch.h"
#include <sys/epoll.h>
#include <sys/uio.h>

extern int ProcessingEventsWhileBlocked; /* networking.c */

/* Layout invariants for the crossing-capable connection types. Base (worker-lifecycle @ 2b5d42831,
 * CACHE_LINE_SIZE 64): cmdEntry 160, cmdBatch 10280, client 744, CommandOrigin 64. This deliverable
 * replaces the entry's 8-byte io_client cookie with a 16-byte ClientHandle, so cmdEntry is now 168 and
 * cmdBatch its header plus IO_BATCH_MAX * 168. */
static_assert(sizeof(ClientHandle) == 2 * sizeof(void *),
              "ClientHandle is a compact {control ref, generation}, not a fatter record");
static_assert(_Alignof(ClientHandle) == _Alignof(void *), "ClientHandle needs only pointer alignment");
static_assert(sizeof(cmdEntry) == 168, "cmdEntry layout changed; re-measure before/after for the report");
static_assert(sizeof(cmdBatch) == offsetof(cmdBatch, e) + IO_BATCH_MAX * sizeof(cmdEntry),
              "cmdBatch is its header plus IO_BATCH_MAX inline entries");
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
static_assert(sizeof(ClientControl) == 3 * CACHE_LINE_SIZE, "identity/control line plus the two counter lines");

#define FP_RING_SIZE 1024        /* batches per ring; batches, not commands */
#define FP_ARENA_SIZE (16 * 1024) /* reply bytes per batch before a slot spills to the heap */
#define FP_FREELIST_MAX 64
#define FP_CLIENT_INFLIGHT_MAX 256 /* commands of one client on main at once */
#define FP_TAG_DETACH ((uintptr_t)1) /* return-ring entry is a client to close, not a batch */
#define FP_TAG_ATTACH ((uintptr_t)2) /* return-ring entry is a client the IO thread takes ownership of */
#define FP_TAGS (FP_TAG_DETACH | FP_TAG_ATTACH)
#define FP_RET_RESERVE 64 /* ring slots admission leaves free so returned batches and detaches never block */

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
    rax *registry;    /* IO thread only: owned + leaving, keyed by the control pointer with the owned client* as value, so an entry's ClientHandle resolves back to its connection without a pointer in the control */
    _Atomic int role; /* FP_ROLE_*: main stores OPEN and QUIESCING, the IO thread stores DRAINED */
    _Atomic int req_pending; /* set by any request publisher, cleared before the owner scans; a hint that some owned client has a pending CC_REQ_* */
    size_t main_clients;   /* main only: clients routed here and not yet taken back */
    size_t detach_pending; /* main only: detach requests the IO thread has not consumed */
    list *ret_overflow;    /* main only: detach requests a full ret ring could not take */
    long long reads, net_input_bytes, net_output_bytes, writes, batches;
} fpThread;

static fpThread fp_threads[IO_THREADS_MAX_NUM];
static client *fp_exec_client[IO_THREADS_MAX_NUM]; /* main-thread executor per IO thread */
static size_t fastpath_clients = 0;                 /* main thread only */
static int fp_slots = 0;                            /* main thread only: 1 + highest initialized thread */
static unsigned fp_rr = 0;
static long long fp_retired[5]; /* main thread only: counters of threads since retired */

size_t fastpathClientCount(void) {
    return fastpath_clients;
}

static cmdBatch *fpAllocBatch(fpThread *t, int tid) {
    cmdBatch *b;
    if (t->nfree > 0) {
        b = t->freelist[--t->nfree];
    } else {
        b = zmalloc(sizeof(cmdBatch));
        b->arena = zmalloc(FP_ARENA_SIZE);
        b->arena_cap = FP_ARENA_SIZE;
    }
    b->count = 0;
    b->io_tid = tid;
    b->arena_used = 0;
    b->opened_us = getMonotonicUs();
    return b;
}

static void fpRecycleBatch(fpThread *t, cmdBatch *b) {
    if (t->nfree < FP_FREELIST_MAX) {
        t->freelist[t->nfree++] = b;
    } else {
        zfree(b->arena);
        zfree(b);
    }
}

void fastpathInitThread(int tid) {
    fpThread *t = &fp_threads[tid];
    memset(t, 0, sizeof(*t));
    spscInit(&t->submit, FP_RING_SIZE);
    spscInit(&t->ret, FP_RING_SIZE);
    t->registry = raxNew();
    t->ret_overflow = listCreate();
    atomic_init(&t->role, FP_ROLE_OPEN);
    if (tid + 1 > fp_slots) fp_slots = tid + 1;
}

/* Destruction preconditions: no client, batch, ring entry or request may still name this thread. */
void fastpathFreeThread(int tid) {
    fpThread *t = &fp_threads[tid];
    if (t->submit.buffer == NULL) return;
    serverAssert(listLength(&t->owned) == 0 && listLength(&t->leaving) == 0 && raxSize(t->registry) == 0);
    serverAssert(t->inflight == 0 && t->cur == NULL);
    serverAssert(spscBacklog(&t->submit) == 0 && spscIsEmpty(&t->ret));
    serverAssert(t->main_clients == 0 && t->detach_pending == 0 && listLength(t->ret_overflow) == 0);
    spscFree(&t->submit);
    spscFree(&t->ret);
    while (t->nfree > 0) {
        cmdBatch *b = t->freelist[--t->nfree];
        zfree(b->arena);
        zfree(b);
    }
    raxFree(t->registry);
    t->registry = NULL;
    listRelease(t->ret_overflow);
    t->ret_overflow = NULL;
    fp_retired[0] += t->reads;
    fp_retired[1] += t->net_input_bytes;
    fp_retired[2] += t->net_output_bytes;
    fp_retired[3] += t->writes;
    fp_retired[4] += t->batches;
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

/* Admitted clients carry only the session state a command entry can hold: user, db and RESP. */
static int fpSessionEligible(client *c) {
    if (!server.io_threads_fast_path) return 0;
    if (!strictOffloadActive() || server.io_threads_num < 2) return 0;
    if (!c->conn || c->flag.fake) return 0;
    if (c->conn->type != connectionByType(CONN_TYPE_SOCKET)) return 0;
    if (authRequired(c)) return 0; /* main enforces a later default-user password change per entry */
    if (server.cluster_enabled) return 0;
    if (isPausedActions(PAUSE_ACTION_CLIENT_ALL | PAUSE_ACTION_CLIENT_WRITE)) return 0; /* paused clients are postponed on main */
    if (c->flag.replica || c->flag.primary || c->flag.monitor || c->slot_migration_job) return 0;
    if (c->flag.blocked || c->flag.unblocked || c->flag.protected || c->flag.lua_debug) return 0;
    if (c->flag.close_asap || c->flag.close_after_reply || c->flag.close_after_command) return 0;
    if (c->mstate || c->flag.pubsub || c->flag.tracking || c->name) return 0;
    if (c->flag.no_touch || c->flag.reply_off || c->flag.reply_skip || c->flag.reply_skip_next) return 0;
    if (c->flag.import_source) return 0;
    return 1;
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
}

/* Both addresses are fixed for the life of the connection; a transport they cannot represent is not admitted. */
static int fpCaptureAddrs(client *c) {
    struct sockaddr_storage sa;
    socklen_t salen = sizeof(sa);
    if (getpeername(c->conn->fd, (struct sockaddr *)&sa, &salen) != 0) return C_ERR;
    if (peerIdentityFromSockaddr(&c->fp_peer, (struct sockaddr *)&sa, salen) != C_OK) return C_ERR;
    salen = sizeof(sa);
    if (getsockname(c->conn->fd, (struct sockaddr *)&sa, &salen) != 0) return C_ERR;
    return peerIdentityFromSockaddr(&c->fp_local, (struct sockaddr *)&sa, salen);
}

/* One connection, one ClientControl: allocated the first time the connection is admitted to the fast
 * path, then kept for its life so its stable id and generation, and any pin the IO owner holds, survive
 * a return to main and a later readmission. Idempotent, so a re-admission after a handoff reuses the
 * existing control. Cache-line aligned (zmalloc_cache_aligned) so its two reply counters keep the
 * padding the layout assertions require; freed only by fastpathControlReclaim. Main allocates it at the
 * admission point before ownership passes to the IO thread, so no other domain observes a half-built
 * control. */
int fastpathControlEnsure(client *c) {
    if (c->control) return C_OK;
    ClientControl *cc = zmalloc_cache_aligned(sizeof(ClientControl));
    if (!cc) return C_ERR;
    cc->client_id = c->id;
    cc->generation = 1;
    cc->owner_domain = CC_OWNER_MAIN; /* still main's until the attach hands it to the IO thread */
    cc->owner_tid = 0;
    cc->lifecycle = FP_DETACHED;
    cc->pin_refs = 0;
    cc->pin_bits = 0;
    atomic_init(&cc->requests, 0u);
    atomic_init(&cc->reply_bytes_produced, (size_t)0);
    atomic_init(&cc->reply_bytes_released, (size_t)0);
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
    if (cc->owner_domain == CC_OWNER_IO)
        atomic_store_explicit(&fp_threads[cc->owner_tid].req_pending, 1, memory_order_release);
}

/* The single dominant request the owner should act on, given the pending bitmask and whether the
 * client's external replies are all released (EVICT waits for that; the others do not). Returns 0 when
 * nothing is actionable yet. Pure precedence, no side effects, so the owner's execution stays deterministic. */
static uint32_t fpRequestWinner(uint32_t reqs, int replies_released) {
    if (reqs & CC_REQ_CLOSE) return CC_REQ_CLOSE;
    if (reqs & CC_REQ_EVICT) return replies_released ? CC_REQ_EVICT : 0;
    if (reqs & CC_REQ_HANDOFF) return CC_REQ_HANDOFF;
    if (reqs & CC_REQ_QUIESCE) return CC_REQ_QUIESCE;
    return 0;
}

/* A handle captures the connection's control and the generation current at capture; a later mismatch
 * means the slot was reused. A control-bearing client always has a control by the time it can be
 * published into an entry (admission calls fastpathControlEnsure first). */
ClientHandle fastpathHandleFor(client *c) {
    serverAssert(c->control);
    return (ClientHandle){.control = c->control, .generation = c->control->generation};
}

/* Stale iff the control's generation moved on from the snapshot; reads only the control, never a
 * connection, so a reused slot is caught without touching freed connection storage. */
int fastpathHandleStale(const ClientHandle *h) {
    return h->control == NULL || h->control->generation != h->generation;
}

/* Logical reply bytes an entry retains for its client: the arena run or the spilled heap block, never
 * both (main sets one or the other) and never allocator-rounded capacity, so produce and release count
 * the same unit. Zero for a requeued or reply-less entry, so charging it is a no-op. */
static inline size_t fpEntryReplyBytes(const cmdEntry *e) {
    return e->reply_big ? e->reply_big_len : e->reply_len;
}

/* Main charges an entry's reply bytes the moment the storage becomes retained for the client, once per
 * entry. Sole writer main, so the add is a plain load/store published with release; the IO owner's
 * acquire load pairs with it. Charging against the entry's own control keeps producer and consumer on
 * the same counter, so outstanding stays meaningful; a stale handle (reused slot) is never charged. */
static void fpReplyCharge(cmdEntry *e) {
    size_t bytes = fpEntryReplyBytes(e);
    if (bytes == 0 || fastpathHandleStale(&e->handle)) return;
    ClientControl *cc = e->handle.control;
    size_t produced = atomic_load_explicit(&cc->reply_bytes_produced, memory_order_relaxed);
    atomic_store_explicit(&cc->reply_bytes_produced, produced + bytes, memory_order_release);
}

/* The IO owner releases an entry's reply bytes once, when it reclaims/discards the charged storage
 * (sent then freed, or dropped for a closing client). Sole writer the owner, so a plain load/store with
 * release; released never exceeds produced because every released entry was charged with the same byte
 * count against the same control. */
static void fpReplyRelease(cmdEntry *e) {
    size_t bytes = fpEntryReplyBytes(e);
    if (bytes == 0 || fastpathHandleStale(&e->handle)) return;
    ClientControl *cc = e->handle.control;
    size_t released = atomic_load_explicit(&cc->reply_bytes_released, memory_order_relaxed);
    atomic_store_explicit(&cc->reply_bytes_released, released + bytes, memory_order_release);
}

/* Reply bytes charged to a control but not yet released: produced minus released, read with acquire so a
 * charge and a release are both visible. The unsigned difference stays correct across benign wraparound
 * while outstanding < SIZE_MAX. Read-only: this does not enforce COB or maxmemory-clients. */
size_t fastpathReplyOutstanding(const ClientControl *cc) {
    size_t produced = atomic_load_explicit(&cc->reply_bytes_produced, memory_order_acquire);
    size_t released = atomic_load_explicit(&cc->reply_bytes_released, memory_order_acquire);
    return produced - released;
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
    c->fp_held = 0;
    c->fp_out = NULL;
    /* Main publishes IO ownership before the ring entry hands the connection over; the IO thread is the
     * next writer of these fields. control->lifecycle is the single source of truth for the state. */
    c->control->owner_domain = CC_OWNER_IO;
    c->control->owner_tid = (uint8_t)tid;
    c->control->lifecycle = FP_ACTIVE;
    fastpathControlPin(c, CC_PIN_OWNER); /* IO now owns the connection; hold until handoff returns it to main */
    listInitNode(&c->io_owner_node, c);
    fastpath_clients++;
    t->main_clients++;
    fpRetPublish(t, c, FP_TAG_ATTACH);
    return C_OK;
}

static int fpCommandAllowed(struct serverCommand *cmd) {
    if (!cmd) return 0; /* unknown command: the main path replies (and runs the host:/post check) */
    if (cmd->proc == pingCommand) return 1; /* fast-path clients are never in pubsub mode */
    if (!(cmd->flags & (CMD_WRITE | CMD_READONLY))) return 0;
    if (cmd->flags & (CMD_BLOCKING | CMD_PUBSUB | CMD_ADMIN | CMD_NOSCRIPT | CMD_NO_MULTI | CMD_NO_ASYNC_LOADING |
                      CMD_ALLOW_BUSY | CMD_TOUCHES_ARBITRARY_KEYS))
        return 0;
    return 1;
}

static void fpSubmit(fpThread *t) {
    cmdBatch *b = t->cur;
    if (!b || b->count == 0) return;
    t->cur = NULL;
    spscEnqueue(&t->submit, b, true);
    t->inflight++;
    t->batches++;
}

void fastpathSubmitPending(int tid) {
    fpThread *t = &fp_threads[tid];
    if (!t->cur || t->cur->count == 0 || t->cur_hold || t->quiescing || spscBacklog(&t->submit) != 0) return;
    /* The hold amortizes per-batch work while bounding latency. */
    if (server.io_batch_hold_us > 0 && getMonotonicUs() - t->cur->opened_us < (monotime)server.io_batch_hold_us) return;
    fpSubmit(t);
}

/* Registry membership changes only with ownership: taken here, dropped when the client is handed back.
 * Keyed by the connection's control pointer so an entry's ClientHandle resolves back to the owned
 * client, with the owned client* as the value. */
static void fpRegister(fpThread *t, client *c) {
    listLinkNodeTail(&t->owned, &c->io_owner_node);
    ClientControl *cc = c->control;
    raxInsert(t->registry, (unsigned char *)&cc, sizeof(cc), c, NULL);
}

static void fpUnregister(fpThread *t, client *c) {
    listUnlinkNode(&t->leaving, &c->io_owner_node);
    ClientControl *cc = c->control;
    raxRemove(t->registry, (unsigned char *)&cc, sizeof(cc), NULL);
}

/* Resolve an entry's handle back to the connection this thread owns, or NULL if the handle is stale
 * (its control's slot was reused) or the client is no longer owned here. Owner-private: it consults
 * this thread's registry and never dereferences a connection through the control. */
static client *fpResolve(fpThread *t, const ClientHandle *h) {
    void *found = NULL;
    if (fastpathHandleStale(h)) return NULL;
    ClientControl *cc = h->control;
    if (!raxFind(t->registry, (unsigned char *)&cc, sizeof(cc), &found)) return NULL;
    return (client *)found;
}

/* True while this thread still owns the connection (registry keyed by its control pointer). */
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
    listUnlinkNode(&t->owned, &c->io_owner_node);
    listLinkNodeTail(&t->leaving, &c->io_owner_node);
    if (hold_cur && fpCurHasClient(t, c)) t->cur_hold = 1;
}

/* Owner-side execution of pending lifecycle requests for one client this thread owns. Only the owner
 * runs this; a request is a published intent, the transition is the owner's alone, so no other domain
 * mutates the client's lifecycle. Reads the bitmask with acquire to pair with the publisher's release.
 * Maps the dominant request onto the existing leave transitions rather than a second lifecycle model:
 * CLOSE/EVICT free the connection (FP_CLOSING), HANDOFF/QUIESCE return it to main (FP_LEAVING). EVICT
 * defers until the client's external replies are all released so no charged reply memory is dropped
 * mid-flight. Executed bits are cleared with release; the transition is idempotent (fpBeginLeave is a
 * no-op once the client is no longer FP_ACTIVE), so a duplicate or late request does nothing. */
static void fpExecuteRequests(fpThread *t, client *c) {
    ClientControl *cc = c->control;
    uint32_t reqs = atomic_load_explicit(&cc->requests, memory_order_acquire);
    if (reqs == 0) return;
    uint32_t win = fpRequestWinner(reqs, fastpathReplyOutstanding(cc) == 0);
    if (win == 0) return; /* an EVICT still waiting on outstanding replies: revisit on a later pass */
    if (cc->lifecycle == FP_ACTIVE) fpBeginLeave(t, c, (win & CC_REQ_TERMINAL) ? FP_CLOSING : FP_LEAVING, 1);
    else if ((win & CC_REQ_TERMINAL) && cc->lifecycle == FP_LEAVING)
        cc->lifecycle = FP_CLOSING; /* a close arriving after a handoff started still frees it */
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
    e->reply_off = e->reply_len = 0;
    e->reply_big = NULL;
    e->reply_big_len = 0;
    e->requeued = 0;
    c->fp_inflight++;
}

/* The first unsupported command and all successors remain queued for main. */
static void fpHarvest(fpThread *t, int tid, client *c) {
    int leave = 0;
    int max = server.io_batch_commands;
    cmdQueue *q = &c->cmd_queue;

    if (c->argc > 0 && (c->read_flags & READ_FLAGS_PARSING_COMPLETED)) {
        if ((c->read_flags & READ_FLAGS_ERROR_MASK) || !fpCommandAllowed(c->parsed_cmd)) {
            leave = 1;
        } else {
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
    }

    while (!leave && q->off < q->len) {
        parsedCommand *p = &q->cmds[q->off];
        int complete = p->read_flags & READ_FLAGS_PARSING_COMPLETED;
        if (!complete && !(p->read_flags & READ_FLAGS_ERROR_MASK)) break; /* trailing partial */
        if ((p->read_flags & READ_FLAGS_ERROR_MASK) || !fpCommandAllowed(p->cmd)) {
            leave = 1;
            break;
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

void fastpathClientReadable(int tid, client *c) {
    fpThread *t = &fp_threads[tid];
    if (c->control->lifecycle != FP_ACTIVE) return;
    /* Backpressure: the batch pipeline is deep enough; the socket stays
     * readable (level triggered) and is served on a later pass. */
    if (t->inflight >= server.io_batch_inflight || c->fp_inflight >= FP_CLIENT_INFLIGHT_MAX) return;

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
    c->last_interaction = server.unixtime;
    if (c->read_flags & READ_FLAGS_QB_LIMIT_REACHED) {
        trimClientQueryBuffer(c);
        fpBeginLeave(t, c, FP_LEAVING, 0);
        return;
    }
    parseInputBuffer(c);
    prepareCommandQueue(c);
    fpHarvest(t, tid, c);
    trimClientQueryBuffer(c);
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
            fpBeginLeave(t, c, FP_CLOSING, 0);
            return 0;
        }
        t->net_output_bytes += n;
        c->net_output_bytes += n;
        t->writes++;
        sdsrange(c->fp_out, n, -1);
    }
    return 1;
}

/* Buffered bytes always precede newly returned replies. */
static void fpSend(fpThread *t, client *c, struct iovec *iov, int iovcnt) {
    if (c->control->lifecycle == FP_CLOSING) return;
    if (c->fp_out && sdslen(c->fp_out) > 0) {
        for (int i = 0; i < iovcnt; i++) c->fp_out = sdscatlen(c->fp_out, iov[i].iov_base, iov[i].iov_len);
        return;
    }
    ssize_t n = writev(c->conn->fd, iov, iovcnt);
    if (n < 0) {
        if (errno != EAGAIN && errno != EINTR) {
            fpBeginLeave(t, c, FP_CLOSING, 0);
            return;
        }
        n = 0;
    }
    t->net_output_bytes += n;
    c->net_output_bytes += n;
    t->writes++;
    for (int i = 0; i < iovcnt; i++) {
        if ((size_t)n >= iov[i].iov_len) {
            n -= iov[i].iov_len;
            continue;
        }
        if (!c->fp_out) c->fp_out = sdsempty();
        c->fp_out = sdscatlen(c->fp_out, (char *)iov[i].iov_base + n, iov[i].iov_len - n);
        n = 0;
    }
    if (c->fp_out && sdslen(c->fp_out) > 0) fpEnableWriteInterest(c, 1);
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

/* Consecutive entries for one client share a writev. */
static void fpDeliverBatch(fpThread *t, cmdBatch *b) {
    struct iovec iov[IO_BATCH_MAX];
    int i = 0;
    while (i < b->count) {
        client *c = fpResolve(t, &b->e[i].handle);
        ClientControl *cc = b->e[i].handle.control;
        int n = 0;
        int j = i;
        int requeued = 0;
        while (j < b->count && b->e[j].handle.control == cc) {
            cmdEntry *e = &b->e[j];
            if (e->requeued) {
                requeued++;
            } else if (e->reply_big) {
                iov[n].iov_base = e->reply_big, iov[n].iov_len = e->reply_big_len, n++;
            } else if (e->reply_len) {
                iov[n].iov_base = b->arena + e->reply_off, iov[n].iov_len = e->reply_len, n++;
            }
            j++;
        }
        if (c) {
            if (n) fpSend(t, c, iov, n);
            c->fp_inflight -= (j - i);
            c->commands_processed += (j - i) - requeued;
            if (requeued) fpRequeue(t, c, &b->e[j - requeued], requeued); /* main stops executing a client at its first held entry */
        }
        i = j;
    }
    for (int k = 0; k < b->count; k++) {
        cmdEntry *e = &b->e[k];
        fpReplyRelease(e); /* charged storage reclaimed on this pass: sent-then-freed or copied into fp_out then freed */
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
 * the main thread: to be freed (closing) or taken over (leaving). Buffered
 * output goes out first while the role is open; a quiescing thread hands the
 * residue over with the client so a slow reader cannot hold it. */
static void fpFinishLeaving(fpThread *t) {
    if (listLength(&t->leaving) == 0) return;
    int open = atomic_load_explicit(&t->role, memory_order_relaxed) == FP_ROLE_OPEN;
    listNode *ln = t->leaving.head;
    while (ln) {
        listNode *next = ln->next;
        client *c = listNodeValue(ln);
        ln = next;
        if (c->fp_inflight > 0) continue;
        if (c->control->lifecycle == FP_LEAVING && open && !fpFlushOut(t, c)) continue; /* still draining output */
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
        if (c) c->fp_inflight -= n;
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

/* Owner-side sweep of pending lifecycle requests. Runs only when a publisher signalled this thread, so
 * an idle owner costs nothing until a request arrives. The signal is cleared before the walk so a
 * request published during it re-arms the flag and is caught next pass rather than lost. Only owned
 * (still FP_ACTIVE) clients carry actionable requests; leaving/closing clients are already past the
 * decision fpExecuteRequests would make. */
static void fpDrainRequests(fpThread *t) {
    if (!atomic_exchange_explicit(&t->req_pending, 0, memory_order_acquire)) return;
    listNode *ln = t->owned.head;
    while (ln) {
        listNode *next = ln->next;
        fpExecuteRequests(t, listNodeValue(ln));
        ln = next;
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
            fpDeliverBatch(t, b);
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
    if (isPausedActions(PAUSE_ACTION_CLIENT_ALL | PAUSE_ACTION_CLIENT_WRITE)) {
        e->requeued = 1; /* the main path postpones it like any other client's command */
        return;
    }

    if (principal->flags & USER_FLAG_RETIRED) principal = ACLResolveUser(principal);
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
    ec->db = e->db;
    ec->resp = e->resp;
    ec->origin = &e->origin;
    ec->buf = b->arena + b->arena_used;
    ec->buf_usable_size = b->arena_cap - b->arena_used;
    ec->bufpos = 0;
    ec->flag.buf_encoded = 0;
    ec->flag.pending_command = 1;

    processCommandAndResetClient(ec);

    e->reply_off = (uint32_t)b->arena_used;
    e->reply_len = (uint32_t)ec->bufpos;
    b->arena_used += ec->bufpos;
    if (listLength(ec->reply) > 0) {
        size_t total = ec->bufpos;
        listIter li;
        listNode *ln;
        listRewind(ec->reply, &li);
        while ((ln = listNext(&li))) total += ((clientReplyBlock *)listNodeValue(ln))->used;
        char *big = zmalloc(total);
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
        b->arena_used -= ec->bufpos;
        e->reply_off = 0;
        e->reply_len = 0;
        e->reply_big = big;
        e->reply_big_len = (uint32_t)total;
    }
    ec->bufpos = 0;
    ec->buf = saved_buf;
    ec->buf_usable_size = saved_usable;
    fpReplyCharge(e); /* the entry's reply storage is now retained for the client; charge it once */
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
    while (listLength(t->ret_overflow) > 0 && spscFreeSlots(&t->ret) > 0) {
        listNode *ln = listFirst(t->ret_overflow);
        client *c = listNodeValue(ln);
        listDelNode(t->ret_overflow, ln);
        spscEnqueue(&t->ret, (void *)((uintptr_t)c | FP_TAG_DETACH), true);
        ClientControl *cc = c->control;
        raxInsert(fp_detaching, (unsigned char *)&cc, sizeof(cc), (void *)t->ret.tail_local, NULL);
    }
}

int fastpathDrain(void) {
    int total = 0;
    int use_prefetch = prefetchBatchEnabled() && !ProcessingEventsWhileBlocked;
    /* While clients are paused every entry is handed back for the main path to postpone. */
    int paused = isPausedActions(PAUSE_ACTION_CLIENT_ALL | PAUSE_ACTION_CLIENT_WRITE);
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
            if (use_prefetch && !paused) {
                getKeysResult result;
                initGetKeysResult(&result);
                int room = 1;
                for (int k = 0; k < b->count && room; k++) {
                    cmdEntry *e = &b->e[k];
                    if (!e->cmd || (e->read_flags & READ_FLAGS_BAD_ARITY)) continue;
                    room = prefetchBatchAddCommand(e->cmd, e->argv, e->argc, e->db, e->slot, &result);
                }
                getKeysFreeResult(&result);
                prefetchBatchRun();
                prefetchBatchReset();
            }
            for (int k = 0; k < b->count; k++) fpExecute(ec, b, &b->e[k]);
            ec->origin = NULL;
            clientSetUser(ec, DefaultUser, 0); /* never keep a principal that may retire */
            total += b->count;
            spscEnqueue(&t->ret, b, false);
        }
        spscCommit(&t->ret);
    }
    if (deadline && total < enough && getMonotonicUs() < deadline) goto again;
    return total;
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

/* Called by main for a client it owns (partitioned or event-loop) after AUTH ran on it. Returns 1 once the
 * client is on the fast path; a client that is not yet quiescent keeps the flag and is retried next time. */
int fastpathReadmitAuthenticated(client *c) {
    if (c->flag.fastpath || c->flag.executor) {
        c->flag.fp_readmit = 0;
        return 0;
    }
    if (!fpQuiescent(c) || (c->flag.partitioned && c->flag.pending_read)) return 0;
    c->flag.fp_readmit = 0;
    if (!fpSessionEligible(c)) return 0;
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
    fastpathControlUnpin(c, CC_PIN_OWNER); /* IO no longer owns it; drop the ownership pin as main takes over */
    fastpath_clients--;
    ACLFastpathClientReturned(c);
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
    int authenticating = c->parsed_cmd && (c->parsed_cmd->proc == authCommand || c->parsed_cmd->proc == helloCommand);
    if (c->argc > 0 && (c->read_flags & READ_FLAGS_PARSING_COMPLETED) && !(c->read_flags & READ_FLAGS_ERROR_MASK))
        c->flag.pending_command = 1;
    if (c->read_flags & READ_FLAGS_ERROR_MASK) {
        handleParseError(c); /* replies, sets close_after_reply; the write path closes it */
        return;
    }
    if (processPendingCommandAndInputBuffer(c) != C_OK) return;
    if (authenticating || c->flag.fp_readmit) {
        c->flag.fp_readmit = 0;
        if (fpReadmit(c)) return;
    }
    beforeNextClient(c);
}

void fastpathInfo(sds *info) {
    long long reads = fp_retired[0], in = fp_retired[1], out = fp_retired[2], writes = fp_retired[3],
              batches = fp_retired[4];
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
    }
    *info = sdscatprintf(*info,
                         "fastpath_clients:%zu\r\n"
                         "fastpath_workers_open:%d\r\n"
                         "fastpath_workers_quiescing:%d\r\n"
                         "fastpath_reads:%lld\r\n"
                         "fastpath_writes:%lld\r\n"
                         "fastpath_batches:%lld\r\n"
                         "fastpath_net_input_bytes:%lld\r\n"
                         "fastpath_net_output_bytes:%lld\r\n",
                         fastpath_clients, open, quiescing, reads, writes, batches, in, out);
}
