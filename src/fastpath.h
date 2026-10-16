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
    CommandOrigin origin; /* Written by the IO thread with the entry; main reads it only to attribute events. */
} cmdEntry;

typedef struct cmdBatch {
    int count;
    int io_tid;
    char *arena;
    size_t arena_cap;
    size_t arena_used;
    cmdEntry e[IO_BATCH_MAX];
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
int fastpathStagedRepliesPending(void);
void fastpathStagedReplyPhase(int tid);
void fastpathStagedFinish(void);
void fastpathStagedReleaseAll(void);
void fastpathStagedAdoptClients(void);

#endif
