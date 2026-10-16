/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef REPL_APPLY_H
#define REPL_APPLY_H

/* Parallel apply of the replication stream on a replica.
 *
 * With keyspace-shard-bits > 0 each db is split into shards by key slot. While main parses the
 * primary's stream, eligible writes are held back and bucketed by shard into one lane per staged
 * thread (main plus the enrolled IO threads). A lane holds whole shards, so every key's writes stay
 * in stream order and no two threads touch the same hashtable. Before any other command runs, and
 * before the parse loop returns, main opens an APPLY phase and every thread applies its lane.
 * Commands that are not eligible, and every command while a feature that observes individual
 * writes is active, run serially on main in stream order. */

#include <stdint.h>

struct client;

/* Main, parse loop of the primary client: take the parsed command into a lane. Returns 1 when
 * taken; the caller then finishes the command without executing it. */
int replApplyDefer(struct client *c);
/* Main: apply every held command before anything else observes the keyspace. */
void replApplyFlush(struct client *c);
/* Main: the primary's writes may be held for parallel apply, so main need not prefetch them. */
int replApplyMayDefer(void);
/* Main: commands held and not yet applied. */
int replApplyPending(void);
/* Staged thread tid (0 = main): apply its lane during an APPLY phase. */
void replApplyRunLane(int tid);

/* Set on a thread while it applies a lane. Paths gated off by replApplyDefer must not run. */
extern _Thread_local int repl_apply_in_lane;
/* The key a lane is applying and its shard, so the db paths do not rehash it. */
extern _Thread_local const char *repl_apply_key;
extern _Thread_local int repl_apply_shard;

#endif /* REPL_APPLY_H */
