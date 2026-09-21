#ifndef FASTPATH_H
#define FASTPATH_H

#include "server.h"
#include "queues.h"

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
#define CC_REQ_EVICT (1u << 2)   /* free under a memory/QoS limit once replies are released */
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
    uint64_t client_id;         /* Immutable: the connection's stable client id, set once at init. */
    uint32_t generation;        /* IO owner bumps on each private-slot assignment to invalidate an earlier ownership epoch. */
    uint8_t owner_domain;       /* CC_OWNER_MAIN/CC_OWNER_IO: current owning domain (published by the owner). */
    uint8_t owner_tid;          /* Owning IO thread id when owner_domain == CC_OWNER_IO; meaningless for main. */
    uint8_t lifecycle;          /* Single source of truth: FP_ACTIVE/LEAVING/CLOSING/DETACHED. */
    _Atomic(uint32_t) requests; /* CC_REQ_* bitmask; any authorized caller sets, only the owner clears on execution. */
    uint32_t pin_refs;          /* Minimal reclamation gate: control is freed only when this reaches zero. */
    uint32_t pin_bits;          /* Main-only: which CC_PIN_* lifecycle pins are currently held, so each stays idempotent. */

    /* Producer cache line: written only by main. */
    _Alignas(CACHE_LINE_SIZE) _Atomic(size_t) reply_bytes_produced; /* Sole writer main: logical reply bytes retained for this client so far; monotonic. */

    /* Consumer cache line: written only by the IO owner. */
    _Alignas(CACHE_LINE_SIZE) _Atomic(size_t) reply_bytes_released; /* Sole writer the IO owner: logical reply bytes reclaimed/discarded; monotonic. outstanding = produced - released. */
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
    int argc;
    int argv_len;
    int slot;
    int read_flags;
    uint32_t reply_off;
    uint32_t reply_len;
    uint32_t reply_big_len;
    uint8_t resp;
    uint8_t requeued;     /* Main did not execute it; the IO thread hands it back for the main path. */
    CommandOrigin origin; /* Written by the IO thread with the entry; main reads it only to attribute events. */
} cmdEntry;

typedef struct cmdBatch {
    int count;
    int io_tid;
    char *arena;
    size_t arena_cap;
    size_t arena_used;
    monotime opened_us;
    cmdEntry e[IO_BATCH_MAX];
} cmdBatch;

int fastpathEligible(client *c);
int fastpathAttach(client *c);
int fastpathReadmitAuthenticated(client *c);
int fastpathDrain(void);
void fastpathRequestDetach(client *c);
int fastpathDetachConsumed(client *c);
void fastpathHandoffDone(client *c, int closing);
size_t fastpathClientCount(void);
void fastpathInfo(sds *info);

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

void fastpathInitThread(int tid);
void fastpathFreeThread(int tid);
void fastpathClientReadable(int tid, client *c);
void fastpathClientWritable(int tid, client *c);
void fastpathSubmitPending(int tid);
int fastpathProcessReturns(int tid);

/* Role transitions driven by main; a quiesce is idempotent and never restarts publication. */
void fastpathWorkerQuiesce(int tid);
int fastpathWorkerReopen(int tid);
int fastpathWorkerDrained(int tid);
int fastpathWorkerRole(int tid);
size_t fastpathWorkerOwnedClients(int tid);

#endif
