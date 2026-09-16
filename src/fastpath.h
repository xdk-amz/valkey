#ifndef FASTPATH_H
#define FASTPATH_H

#include "server.h"
#include "queues.h"

#define IO_BATCH_MAX 64

/* One parsed command, produced by an IO thread, executed by main, delivered by
 * the IO thread. The IO-thread fields are written before the batch is
 * published; main writes only the reply fields. */
typedef struct cmdEntry {
    client *c;                 /* IO-thread use only: main never dereferences it */
    robj **argv;               /* allocated by the IO thread's parser; consumed by the executor's reset */
    int argc;
    int argv_len;
    size_t argv_len_sum;
    unsigned long long input_bytes;
    struct serverCommand *cmd; /* looked up by the IO thread */
    int slot;
    int read_flags;
    serverDb *db;              /* connection state snapshot: selected db and RESP version */
    uint8_t resp;
    /* written by main */
    uint32_t reply_off;        /* into the batch arena */
    uint32_t reply_len;
    char *reply_big;           /* heap reply when the arena had no room; freed by the IO thread */
    uint32_t reply_big_len;
} cmdEntry;

typedef struct cmdBatch {
    int count;
    int io_tid;
    char *arena;
    size_t arena_cap;
    size_t arena_used;
    monotime opened_us; /* when the first entry went in (IO thread) */
    cmdEntry e[IO_BATCH_MAX];
} cmdBatch;

/* client->fp_state */
#define FP_ACTIVE 0   /* read by its IO thread, commands flow through batches */
#define FP_LEAVING 1  /* stopped reading; hands over to main once nothing is in flight */
#define FP_CLOSING 2  /* stopped reading; main frees it once nothing is in flight */
#define FP_DETACHED 3 /* main owns it again */

/* main thread */
int fastpathEligible(client *c);
int fastpathAttach(client *c);
int fastpathDrain(void);
void fastpathRequestDetach(client *c);
void fastpathHandoffDone(client *c, int closing);
size_t fastpathClientCount(void);
void fastpathInfo(sds *info);

/* IO threads */
void fastpathInitThread(int tid);
void fastpathFreeThread(int tid);
void fastpathClientReadable(int tid, client *c);
void fastpathClientWritable(int tid, client *c);
void fastpathSubmitPending(int tid);
int fastpathProcessReturns(int tid);

#endif
