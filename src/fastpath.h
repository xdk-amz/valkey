/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef FASTPATH_H
#define FASTPATH_H

#include "server.h"
#include "queues.h"

#define IO_BATCH_MAX 64

#define FP_ACTIVE 0   /* read by IO threads, commands flow through batches */
#define FP_LEAVING 1  /* stopped reading; main takes it back after the reply phase */
#define FP_CLOSING 2  /* stopped reading; main frees it after the reply phase */
#define FP_DETACHED 3 /* main owns it again */

/* Separately allocated fast-path state of one connection: identity and lifecycle. Main reads it while
 * executing an entry so it never touches the client itself; never place connection, socket, parser,
 * query buffer or output state here. */
typedef struct ClientControl {
    uint64_t client_id; /* Immutable: the connection's stable client id, set once at init. */
    uint8_t lifecycle;  /* Single source of truth: FP_ACTIVE/LEAVING/CLOSING/DETACHED. */
    uint8_t closing;    /* Main only: freeClient asked to close it; its entries are skipped. */
} ClientControl;

/* Produced by an IO thread in the read phase, executed by main in the write phase, replied by the same IO thread. */
typedef struct cmdEntry {
    client *c;              /* Owning client: valid until the reply phase of the iteration that produced the entry. Main never dereferences it. */
    ClientControl *control; /* c->control, which main reads instead of the client. */
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
    uint8_t readonly;     /* The client sent READONLY: a cluster replica serves its reads. */
    uint8_t obuf_close;   /* A write lane found its reply past the output buffer limit; main closes the client. */
    CommandOrigin origin; /* Written by the IO thread with the entry; main reads it only to attribute events. */
    char *reply_base;     /* Reply bytes are at reply_base + reply_off; NULL means the batch arena. */
    struct laneProp *prop; /* Propagation a write lane captured as ops; main replays it in T order. */
    uint32_t frames_off;   /* Replication frames a write lane formatted for it, in that lane's buffer. */
    uint32_t frames_len;
    int32_t lane;          /* Write lane, or -1: executed by main alone (a barrier, or lanes off). */
    int32_t shard;         /* Keyspace hashtable index of every key of a lane entry. */
    uint32_t seg;          /* Barriers this IO thread harvested before this entry, in this iteration. */
    uint8_t frames_wrap;   /* The frames go out inside MULTI/EXEC. */
} cmdEntry;

typedef struct cmdBatch {
    int count;
    int io_tid;
    char *arena;
    size_t arena_cap;
    size_t arena_used;     /* main writes this per executed command while a STREAM worker reads the arena. */
    /* executed and delivered are the streaming hand-off and sit on their own cache line, away from
     * arena_used: a worker spins on executed (and reads delivered) while main advances arena_used per
     * command, so sharing a line would bounce it to main on every reply byte. */
    _Alignas(CACHE_LINE_SIZE) _Atomic(int) executed; /* entries main has finished executing; published release at client-run boundaries. */
    int delivered;        /* worker-owned: entries this thread has already written in STREAM/REPLY. */
    _Alignas(CACHE_LINE_SIZE) cmdEntry e[IO_BATCH_MAX]; /* own line so the first entries never share the polled line. */
} cmdBatch;

int fastpathEligible(client *c);
int fastpathAttach(client *c);
int fastpathReadmitAuthenticated(client *c);
int fastpathExecutorOutputLimitReached(client *ec);
/* acl-offload: a reading thread brackets its rule-set reads; main waits them out after an ACL change. */
void fastpathAdmitReadBegin(void);
void fastpathAdmitReadEnd(void);
void fastpathAdmissionQuiesce(void);
uint32_t testOnlyFastpathAdmitSeq(int tid);
void fastpathRequestDetach(client *c);
int fastpathDetachConsumed(client *c);
void fastpathHandoffDone(client *c, int closing);
size_t fastpathClientCount(void);
void fastpathInfo(sds *info);

/* Allocate and initialize the connection's ClientControl once, on first crossing-capable admission; a
 * no-op if it already has one. Reclaimed only by fastpathControlReclaim at free. */
int fastpathControlEnsure(client *c);
/* The single final reclaimer: free the connection's ClientControl at client teardown. */
void fastpathControlReclaim(client *c);

void fastpathInitThread(int tid);
void fastpathFreeThread(int tid);

/* Staged loop (driven by staged_loop.c). */
int fastpathStagedOn(void);
void fastpathStagedSet(int on);
void fastpathStagedSetDepth(int depth);
void fastpathStagedReadPhase(int tid);
int fastpathStagedExecute(void);
int fastpathStagedExecuteStreaming(void);
int fastpathStagedRepliesPending(void);
void fastpathStagedReplyPhase(int tid);
void fastpathStagedStreamPhase(int tid);
void fastpathStagedStreamBegin(void);
void fastpathStagedStreamAbort(void);
int fastpathStagedStreamBatchCount(void);
void fastpathStagedFinish(void);
void fastpathStagedReleaseAll(void);
void fastpathStagedAdoptClients(void);

/* Parallel write phase (docs/parallel-write-stage.md). */
#define FP_MAX_LANES 64 /* write lanes; more IO threads than this leave the rest idle in the write phase */
/* Main, before the read phase: decide whether this iteration's entries are sorted into write lanes. */
void fastpathStagedPrepareLanes(void);
/* Main, after the read phase: the write phase will run lane phases on the IO threads. */
int fastpathStagedLanesWillRun(void);
/* Thread tid (0 = main): run its lane of the current write segment. */
void fastpathStagedWriteLane(int tid);

/* Replica: the primary's stream on the write lanes (repl_apply.c). */
/* Main: nothing outside a lane's own hashtables would observe or order the stream's writes. */
int fastpathStreamLanesAllowed(void);
/* IO thread, read phase: the keyspace hashtable of every key the command declares, or -1 when it has
 * none, they are in several, or a client is blocked on or watches one. */
int fastpathKeysShard(serverDb *db, struct serverCommand *cmd, robj **argv, int argc, int slot);
/* Main, before an APPLY phase of the stream on lanes [0, nlanes). */
void fastpathStreamLanesPrepare(int nlanes, int main_lane);
/* Lane thread, APPLY phase: enter the lane, apply commands of the stream on it, leave. */
void fastpathStreamLaneEnter(int lane);
void fastpathStreamLaneCall(int lane, serverDb *db, struct serverCommand *cmd, robj **argv, int argc, int argv_len,
                            size_t argv_len_sum, unsigned long long input_bytes, int shard);
void fastpathStreamLaneLeave(void);
/* Main, after an APPLY phase of the stream: fold what lanes [0, nlanes) accumulated. */
void fastpathStreamLanesMerge(int nlanes);

#endif
