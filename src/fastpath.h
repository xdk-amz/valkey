#ifndef FASTPATH_H
#define FASTPATH_H

#include "server.h"
#include "queues.h"

#define IO_BATCH_MAX 64

/* Ring publication transfers each entry from its IO thread to main and back. */
typedef struct cmdEntry {
    client *io_client; /* IO-thread-only return cookie; main never dereferences it. */
    robj **argv;
    int argc;
    int argv_len;
    size_t argv_len_sum;
    unsigned long long input_bytes;
    struct serverCommand *cmd;
    int slot;
    int read_flags;
    serverDb *db;
    uint8_t resp;
    uint8_t requeued; /* Main did not execute it; the IO thread hands it back for the main path. */
    uint32_t reply_off;
    uint32_t reply_len;
    char *reply_big;
    uint32_t reply_big_len;
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

#define FP_ACTIVE 0   /* read by its IO thread, commands flow through batches */
#define FP_LEAVING 1  /* stopped reading; hands over to main once nothing is in flight */
#define FP_CLOSING 2  /* stopped reading; main frees it once nothing is in flight */
#define FP_DETACHED 3 /* main owns it again */

/* Fast-path role of one IO thread: main opens and quiesces it, the thread publishes drained. */
#define FP_ROLE_OPEN 0      /* admits clients, publishes batches */
#define FP_ROLE_QUIESCING 1 /* admits nothing, publishes nothing; hands off or closes every owned client */
#define FP_ROLE_DRAINED 2   /* owns no client, batch or ring entry; main may reopen or destroy it */

int fastpathEligible(client *c);
int fastpathAttach(client *c);
int fastpathReadmitAuthenticated(client *c);
int fastpathDrain(void);
void fastpathRequestDetach(client *c);
int fastpathDetachConsumed(client *c);
void fastpathHandoffDone(client *c, int closing);
size_t fastpathClientCount(void);
void fastpathInfo(sds *info);

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
