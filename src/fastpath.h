#ifndef FASTPATH_H
#define FASTPATH_H

#include "server.h"
#include "queues.h"

#define IO_BATCH_MAX 64

#define FP_ACTIVE 0   /* read by IO threads, commands flow through batches */
#define FP_LEAVING 1  /* stopped reading; main takes it back after the reply phase */
#define FP_CLOSING 2  /* stopped reading; main frees it after the reply phase */
#define FP_DETACHED 3 /* main owns it again */

/* Stable, separately allocated shared control for one crossing-capable connection.
 * Holds only intentionally cross-thread state: identity and lifecycle. The concrete client remains the owner-only ClientConnection; never place
 * connection/socket/TLS/parser/querybuf/output/poller/general client flags here, and take no lock
 * around it. Referenced only through a generation-checked ClientHandle so a stale reference is
 * detected without dereferencing a freed connection. */
typedef struct ClientControl {
    uint64_t client_id;         /* Immutable: the connection's stable client id, set once at init. */
    uint32_t generation;        /* IO owner bumps on each private-slot assignment to invalidate an earlier ownership epoch. */
    uint8_t lifecycle;          /* Single source of truth: FP_ACTIVE/LEAVING/CLOSING/DETACHED. */
    uint8_t closing;            /* Main only: freeClient asked to close it; its entries are skipped. */
} ClientControl;

/* Compact reference to shared control plus an owner-private connection-table slot. Main uses only
 * control and generation; the IO owner validates all three fields before resolving its connection.
 * Deliberately no connection pointer, refcount, per-command allocation, or owner-side tree lookup. */
typedef struct ClientHandle {
    ClientControl *control; /* The referenced control; valid only while generation matches. */
    uint32_t generation;    /* Snapshot of control->generation at capture; a mismatch means the slot was reused. */
    uint32_t owner_slot;    /* Index in the owning IO thread's private connection table. */
} ClientHandle;

/* Produced by an IO thread in the read phase, executed by main in the write phase, replied by the same IO thread. */
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
/* The single final reclaimer: free the connection's ClientControl at client teardown. */
void fastpathControlReclaim(client *c);

void fastpathInitThread(int tid);
void fastpathFreeThread(int tid);

/* Staged loop (driven by staged_loop.c; see docs/staged-event-loop.md). */
int fastpathStagedOn(void);
void fastpathStagedSet(int on);
void fastpathStagedSetDepth(int depth);
void fastpathStagedReadPhase(int tid);
int fastpathStagedExecute(void);
int fastpathStagedRepliesPending(void);
void fastpathStagedReplyPhase(int tid);
void fastpathStagedFinish(void);
void fastpathStagedReleaseAll(void);
void fastpathStagedAdoptClients(void);

#endif
