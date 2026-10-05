#ifndef FASTPATH_H
#define FASTPATH_H

#include "server.h"
#include "queues.h"

/* The fast path partitions clients onto per-IO-thread epoll instances, so it is compiled only where
 * epoll exists; elsewhere the feature is disabled and fpSessionEligible() returns 0. Gate on the
 * existing HAVE_EPOLL contract. FASTPATH_FORCE_NO_EPOLL forces the disabled path for a build test
 * on an epoll host. */
#if defined(HAVE_EPOLL) && !defined(FASTPATH_FORCE_NO_EPOLL)
#define HAVE_FASTPATH_EPOLL 1
#endif

#define IO_BATCH_MAX 64

#define FP_ACTIVE 0   /* read by its IO thread, commands flow through batches */
#define FP_LEAVING 1  /* stopped reading; hands over to main once nothing is in flight */
#define FP_CLOSING 2  /* stopped reading; main frees it once nothing is in flight */
#define FP_DETACHED 3 /* main owns it again */

/* Fast-path role of one IO thread: main opens and quiesces it, the thread publishes drained. */
#define FP_ROLE_OPEN 0      /* admits clients, publishes batches */
#define FP_ROLE_QUIESCING 1 /* admits nothing, publishes nothing; hands off or closes every owned client */
#define FP_ROLE_DRAINED 2   /* owns no client, batch or ring entry; main may reopen or destroy it */

/* Which domain currently owns the connection; the owner is the only party that executes requests. */
#define CC_OWNER_MAIN 0 /* main event loop owns the ClientConnection (partitioned / normal path) */
#define CC_OWNER_IO 1   /* an IO thread owns it end to end (fast path) */

/* Pending lifecycle requests published against a control; the owner clears each once executed.
 * Bits so compatible requests coalesce; CLOSE is terminal and outranks the rest. */
#define CC_REQ_QUIESCE (1u << 0) /* stop admitting; hand back in flight so the owner can be reassigned */
#define CC_REQ_HANDOFF (1u << 1) /* return the connection to main once nothing is in flight */
#define CC_REQ_EVICT (1u << 2)   /* terminal memory/QoS free; never waits for buffered replies */
#define CC_REQ_CLOSE (1u << 3)   /* terminal: free once drained; supersedes every other request */

/* Stable, separately allocated shared control for one crossing-capable connection.
 * Holds only intentionally cross-thread state: identity, current owner, lifecycle, pending
 * requests, and reply-memory accounting. The concrete client remains the owner-only
 * ClientConnection; never place connection/socket/TLS/parser/querybuf/output/poller/general
 * client flags here, and take no lock around it. Referenced only through a generation-checked
 * ClientHandle so a stale reference is detected without dereferencing a freed connection.
 * Reply-memory accounting is two unsigned monotonic counters, each with a single writer, so
 * outstanding = produced - released needs no contended read-modify-write and no lock. Both count
 * the SAME unit: logical reply bytes retained for the client (never allocator-rounded capacity),
 * so the difference is meaningful; producer and consumer must never mix units. Wraparound is
 * benign because the unsigned difference stays correct while outstanding < SIZE_MAX. The two
 * counters live on separate cache lines (below) so the main producer and the IO consumer never
 * contend on the same line. Memory ordering: the writer publishes with release, the reader (and
 * the reclamation gate) loads with acquire so a produced charge is visible before its bytes are
 * observed, and a release is visible before the control is reclaimed. */
typedef struct ClientControl {
    /* Identity + control cache line: mostly-immutable identity and owner-published fields. */
    uint64_t client_id;               /* Immutable: the connection's stable client id, set once at init. */
    uint32_t generation;              /* IO owner bumps on each private-slot assignment to invalidate an earlier ownership epoch. */
    uint8_t owner_domain;             /* CC_OWNER_MAIN/CC_OWNER_IO: current owning domain (published by the owner). */
    uint8_t owner_tid;                /* Owning IO thread id when owner_domain == CC_OWNER_IO; meaningless for main. */
    uint8_t lifecycle;                /* Single source of truth: FP_ACTIVE/LEAVING/CLOSING/DETACHED. */
    uint8_t capa;                     /* Main-only, set at attach: the client's CLIENT CAPA bits; CAPA runs only while detached. */
    _Atomic(uint32_t) requests;       /* CC_REQ_* bitmask; any authorized caller sets, only the owner clears on execution. */
    uint32_t pin_refs;                /* Minimal reclamation gate: control is freed only when this reaches zero. */
    uint32_t pin_bits;                /* Main-only: which CC_PIN_* lifecycle pins are currently held, so each stays idempotent. */
    uint8_t readonly;                 /* Main-only, set at attach: READONLY mode; READONLY/READWRITE run only while detached. */
    struct FastpathLimitEntry *limit; /* Immutable after fastpathControlEnsure: the connection's one main-owned limit side object. */
    robj *name;                       /* Main-only, set at attach: the client's CLIENT SETNAME, borrowed; SETNAME runs only while detached. */

    /* Producer cache line: written only by main. */
    _Alignas(CACHE_LINE_SIZE) _Atomic(size_t) reply_bytes_produced; /* Sole writer main: logical reply bytes retained for this client so far; monotonic. */

    /* Consumer cache line: written only by the IO owner. */
    _Alignas(CACHE_LINE_SIZE) _Atomic(size_t) reply_bytes_released; /* Sole writer the IO owner: logical reply bytes reclaimed/discarded; monotonic. outstanding = produced - released. */
    _Atomic(time_t) last_interaction;                               /* Sole writer the IO owner (main only at attach-init before transfer); unixtime of the last successful fast-path read, acquire-read by main for idle timeout. */
    _Atomic(size_t) input_mem;                                      /* Sole writer the IO owner (main only at attach-init before transfer): bytes of its private query buffer and of the arguments it holds, parsed or on main; read by main for maxmemory-clients. */
    /* Sole writer the IO owner (main only at attach-init): what CLIENT LIST reports as qbuf, qbuf-free,
     * argv-mem, rbs and rbp, so main never reads the buffers themselves. */
    _Atomic(size_t) qbuf_len;
    _Atomic(size_t) qbuf_free;
    _Atomic(size_t) argv_mem;
    _Atomic(size_t) rbuf_size;
    _Atomic(size_t) rbuf_peak;
} ClientControl;

/* Compact reference to shared control plus an owner-private connection-table slot. Main uses only
 * control and generation; the IO owner validates all three fields before resolving its connection.
 * Deliberately no connection pointer, refcount, per-command allocation, or owner-side tree lookup. */
typedef struct ClientHandle {
    ClientControl *control; /* The referenced control; valid only while generation matches. */
    uint32_t generation;    /* Snapshot of control->generation at capture; a mismatch means the slot was reused. */
    uint32_t owner_slot;    /* Index in the owning IO thread's private connection table. */
} ClientHandle;

/* Ring publication transfers each entry from its IO thread to main and back. */
typedef struct cmdEntry {
    ClientHandle handle; /* Owning connection's control by generation-checked reference; main never reaches the connection through it, and the IO owner resolves it back to its own client. */
    robj **argv;
    size_t argv_len_sum;
    unsigned long long input_bytes;
    struct serverCommand *cmd;
    serverDb *db;
    char *reply_big;
    long long woff;    /* Origin client's write offset: prior value in, resulting offset out; the IO owner applies it back before handoff. */
    size_t reply_wire; /* Bytes an encoded reply sends; its region holds string references owned by the batch. */
    int argc;
    int argv_len;
    int slot;
    int read_flags;
    uint32_t reply_off;
    uint32_t reply_len;
    uint32_t reply_big_len;
    uint8_t resp;
    uint8_t requeued; /* Main did not execute it; the IO thread hands it back for the main path. */
    uint8_t reply_encoded;
    CommandOrigin origin; /* Written by the IO thread with the entry; main reads it only to attribute events. */
} cmdEntry;

typedef struct cmdBatch {
    int count;
    int io_tid;
    char *arena;
    size_t arena_cap;
    size_t arena_used;
    monotime opened_us;
    struct cmdBatch *pending_next; /* Main-only intrusive link while held for the appendfsync-always barrier; NULL otherwise. */
    robj **refs;                   /* Strings encoded replies point at; main takes and drops these references. */
    uint32_t nrefs;
    uint32_t refs_cap;
    uint8_t release; /* Delivered and sent back so main can drop refs; it carries no commands. */
    cmdEntry e[IO_BATCH_MAX];
} cmdBatch;

int fastpathEligible(client *c);
int fastpathAttach(client *c);
int fastpathTryReadmit(client *c);
int fastpathDrain(void);
void fastpathRequestDetach(client *c);
int fastpathDetachConsumed(client *c);
void fastpathHandoffDone(client *c, int closing);
/* Runs the pending input of clients main took back, in hand-off order; returns how many it resumed.
 * Called after each drain of IO responses, including from a busy script's event processing. */
int fastpathResumeHandedOff(void);
size_t fastpathClientCount(void);
void fastpathInfo(sds *info);
void fastpathNetBytes(long long *in, long long *out);
void fastpathResetNetStats(void);

/* acl-offload: main spins here after bumping the ACL epoch, until every fast-path worker
 * has left any admission-time ACL rule-set read it may have been inside, so main may then
 * mutate or free that rule-set memory. Cheap when idle; on the rare ACL-mutation path only. */
void fastpathAdmissionQuiesce(void);
/* IO-thread side: mark entry/exit of the admission region that dereferences a user's rule set,
 * on the calling thread's own admission slot. */
void fastpathAdmitReadBegin(void);
void fastpathAdmitReadEnd(void);
uint32_t testOnlyFastpathAdmitSeq(int tid);
void testOnlyFastpathAfterDequeue(void (*cb)(int tid));
/* Main-only introspection for tests: batches this thread holds for the durability barrier, not yet delivered. */
size_t fastpathPendingBatches(int tid);
/* IO owner: nothing fast-path work could do now without a socket event or a wake from main. */
int fastpathThreadIdle(int tid);
/* Main: whether an IO thread submitted batches main has not drained. */
int fastpathMainHasWork(void);

/* Build a generation-checked handle for a control-bearing client. */
ClientHandle fastpathHandleFor(client *c);
/* True when a handle no longer matches its control's generation: the slot was reused. Reads only the
 * control, never a connection, so a stale handle is caught without touching freed connection storage. */
int fastpathHandleStale(const ClientHandle *h);

/* Allocate and initialize the connection's ClientControl once, on first crossing-capable admission; a
 * no-op if it already has one. Reclaimed only by fastpathControlReclaim at free. */
int fastpathControlEnsure(client *c);
/* Publish a lifecycle request (CC_REQ_*) against a control. Any authorized caller may publish;
 * nonblocking and idempotent, with no completion handshake. Only the owning domain executes it.
 * Compatible requests coalesce; CLOSE supersedes the rest. */
void fastpathControlRequest(ClientControl *cc, uint32_t req);
/* The single final reclaimer: free the connection's ClientControl at client teardown once nothing pins it. */
void fastpathControlReclaim(client *c);

/* Reply bytes charged to a control but not yet released (produced - released). Read-only: reports the
 * outstanding external reply memory; it does not enforce COB or maxmemory-clients. */
size_t fastpathReplyOutstanding(const ClientControl *cc);
size_t fastpathInputMem(const ClientControl *cc);
size_t fastpathClientMemory(const ClientControl *cc, size_t *output_mem);
/* Main: CLIENT LIST buffer fields of an IO-owned client, as its owner last published them. */
typedef struct fastpathBufferInfo {
    size_t qbuf, qbuf_free, argv_mem, rbs, rbp;
} fastpathBufferInfo;
void fastpathClientBuffers(const ClientControl *cc, fastpathBufferInfo *info);

/* Acquire-loaded last-interaction unixtime for a control; read-only, main-only, never a connection deref. */
time_t fastpathControlLastInteraction(const ClientControl *cc);

/* Count of controls currently in the main-only limit registry. Reads only main-owned bookkeeping. */
size_t fastpathLimitRegistryCount(void);
/* Whether this control's limit entry is currently linked into the registry. Reads only main-owned entry state. */
int fastpathLimitRegistryContains(const ClientControl *cc);

/* Main-only amortized output-buffer-limit and maxmemory-clients enforcement over the registry; call once
 * per server cron cycle. Checks a rotating subset so every registered control is enforced and re-accounted
 * about once per second at server.hz. */
void fastpathLimitsCron(void);

/* Main-only: re-apply maxmemory-clients enable/disable to every registered fast-path entry synchronously.
 * Each entry's estimated contribution to stat_clients_type_memory[CLIENT_TYPE_NORMAL] and the fast-path
 * aggregate is kept current either way; enabling rebuckets accounted entries into the private size buckets
 * and disabling only unbuckets them. Called from the CONFIG SET maxmemory-clients apply, never from the
 * command path. */
void fastpathApplyMaxmemoryClients(int enabled);

/* Main-only eviction selectors for the private fast-path size buckets, used only by evictClients.
 * fastpathEvictionMaxBucket returns the highest non-empty fast-path bucket index, or -1 when none exist,
 * so the caller can compare it against the largest normal bucket for a size-fair choice. */
int fastpathEvictionMaxBucket(void);
/* Terminally evict the head entry of one fast-path bucket: publish an immediate CC_REQ_EVICT, count one
 * eviction, mark it terminal, and unlink it from the bucket so it is never reselected. Returns the reply
 * memory estimate that was accounted for it, which the caller adds to pending_freed; the aggregate/stat
 * removal itself happens once at hand-off. Never dereferences the connection or calls freeClient. */
size_t fastpathEvictTopFromBucket(int bucket_idx);

/* Main-only: total reply/base memory still charged to stat_clients_type_memory[CLIENT_TYPE_NORMAL] for
 * terminal fast-path entries awaiting IO hand-off. evictClients seeds its local pending_freed from this on
 * every call so an in-flight terminal client's not-yet-removed memory is not counted as live pressure and
 * does not trigger collateral eviction of a healthy client. Reads only main-owned bookkeeping. */
size_t fastpathTerminalPendingMem(void);
int fastpathServeMemHints(void);

/* Main-only introspection for tests: the total reply-memory estimate currently contributed by fast-path
 * entries to stat_clients_type_memory[CLIENT_TYPE_NORMAL]. Reads only main-owned bookkeeping. */
size_t fastpathMaxmemoryAggregate(void);
/* Main-only introspection for tests: the maxmemory estimate currently accounted for one control, and the
 * private fast-path bucket index it sits in (-1 when not bucketed). Read only main-owned entry state. */
size_t fastpathMaxmemoryAccounted(const ClientControl *cc);
int fastpathMaxmemoryBucketOf(const ClientControl *cc);
/* Main-only introspection for tests: number of entries linked in one private fast-path size bucket. */
size_t fastpathMaxmemoryBucketCount(int bucket_idx);

void fastpathInitThread(int tid);
void fastpathFreeThread(int tid);
void fastpathClientReadable(int tid, client *c);
void fastpathClientWritable(int tid, client *c);
void fastpathSubmitPending(int tid);
int fastpathProcessReturns(int tid);

/* Main-only: publish every batch held for appendfsync-always durability to its owner return ring, in the
 * order held, immediately after the beforeSleep AOF flush/fsync so no write reply is observable before its
 * durability point. Nonblocking and a no-op when nothing is held. */
void fastpathReleaseDurableReplies(void);

/* Role transitions driven by main; a quiesce is idempotent and never restarts publication. */
void fastpathWorkerQuiesce(int tid);
int fastpathWorkerReopen(int tid);
int fastpathWorkerDrained(int tid);
int fastpathWorkerRole(int tid);
size_t fastpathWorkerOwnedClients(int tid);

#endif
