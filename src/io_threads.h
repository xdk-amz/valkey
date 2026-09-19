#ifndef IO_THREADS_H
#define IO_THREADS_H

#include "server.h"

/* Tag values for tagged pointers on work/response queues.
 * Tags must fit in 3 bits (0-7) due to 8-byte alignment from jemalloc
 * with --with-lg-quantum=3. SPSC and SPMC tags are separate enums
 * because they occupy independent queues and may reuse values. */

/* Tags for the SPSC private inbox (main thread → specific I/O thread). */
typedef enum {
    JOB_SPSC_FREE_ARGV = 0,
    JOB_SPSC_POLL = 1,
    JOB_SPSC_WRITE_SLAB = 2, /* batch of clients to write, one job per event loop drain */
    JOB_SPSC_FREE_SLAB = 3,  /* batch of terminal frees from main */
} JobRequestSPSC;

/* Tags for the SPMC shared inbox (main thread → any I/O thread). */
typedef enum {
    JOB_REQ_READ_CLIENT = 0,
    JOB_REQ_WRITE_CLIENT,
    JOB_REQ_FREE_OBJ,
    JOB_REQ_POLL,
    JOB_REQ_ACCEPT,
    JOB_REQ_CLUSTER_READ,
    JOB_REQ_CLUSTER_WRITE,
    JOB_REQ_CLUSTER_ACCEPT,
    JOB_REQ_COUNT
} JobRequestSPMC;
static_assert(JOB_REQ_COUNT <= 8, "JOB_REQ_COUNT must not exceed 8 for pointer arithmetic");

/* Tags for the MPSC response queue (I/O threads → main thread). */
typedef enum {
    JOB_RES_READ_CLIENT = 0,
    JOB_RES_WRITE_CLIENT,
    JOB_RES_CLUSTER_READ,
    JOB_RES_CLUSTER_WRITE,
    JOB_RES_CLUSTER_ACCEPT,
    JOB_RES_WRITE_SLAB, /* the write slab coming back with every client written */
    JOB_RES_FP_CLOSE,   /* fast path: the IO thread is done with a closing client, main frees it */
    JOB_RES_FP_HANDOFF, /* fast path: the IO thread hands a client to the main path */
    JOB_RES_COUNT
} JobResult;
static_assert(JOB_RES_COUNT <= 8, "JOB_RES_COUNT must not exceed 8 for pointer arithmetic");

typedef void (*job_handler)(void *);

void initIOThreads(int prev_threads_num);
void killIOThreads(void);
int inMainThread(void);
int trySendReadToIOThreads(client *c);
int trySendWriteToIOThreads(client *c);
void flushWriteSlab(void);
/* Partitioned client sockets remain owned by one IO thread. */
int tryPartitionClient(client *c);
void unpartitionClient(client *c);
void unpartitionAllClients(void);
void partitionedClientDetach(client *c);
void armPartitionedClientRead(client *c);
int partitionedClientHold(client *c);
void ioThreadQueueReadCompletion(client *c);
void reconcileLazyWrite(client *c);
int ioThreadEpollFd(int tid);
void partitionedClientRelease(client *c);
int tryOffloadFreeObjToIOThreads(robj *o);
int tryOffloadFreeArgvToIOThreads(client *c, int argc, robj **argv);
int tryOffloadFreePtrToIOThreads(void *ptr);
void beginInlineReclaim(void);
void endInlineReclaim(void);
void freeValueNeverOnMain(robj *key, robj *val, int dbid);
void drainPendingMainFrees(void);
size_t pendingMainFreesLen(void);
/* Bytes committed to off-main frees but not yet physically reclaimed. */
size_t offloadPendingFreeBytes(void);
void IOThreadsAfterSleep(int numevents);
void IOThreadsBeforeSleep(long long current_time);
void drainIOThreadsQueue(void);
void testOnlyInitIOThreadQueues(void);
void testOnlyFreeIOThreadQueues(void);
void testOnlyFillIOThreadInbox(void);
size_t testOnlyGetClusterIOPendingResponses(void);
void testOnlySetIOThreadReady(int tid, int epfd);
void trySendPollJobToIOThreads(void);
int trySendAcceptToIOThreads(connection *conn);
struct clusterLink;
int trySendClusterReadToIOThreads(struct clusterLink *link);
int trySendClusterWriteToIOThreads(struct clusterLink *link);
int trySendClusterAcceptToIOThreads(connection *conn);
int updateIOThreads(const char **err);
int applyIOThreadsFastpathConfig(const char **err);
/* Worker lifecycle: main reconciles live threads with the configuration from beforeSleep. */
void ioThreadsConverge(void);
void ioThreadsStopForExit(void);
int ioThreadsReadyNum(void);
int ioThreadsRunningNum(void);
int ioThreadsRetiringNum(void);
void ioThreadsDebugFailCreate(int tid);
long long getIOThreadActiveTimeMicroseconds(int id);
int clientHasPendingIO(struct client *c);
int processIOThreadsResponses(void);
int getCurTid(void);
void sendToMainThread(void *data, int type);

#endif /* IO_THREADS_H */
