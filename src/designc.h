/*
 * Copyright (c) Valkey Contributors
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * Design C worker-ownership spike (Stage 1: GET fast path). LOCAL SPIKE ONLY.
 * See TASK-designc-valkey.md. Architecture is LOCKED by that spec.
 *
 * Summary of the split this file implements:
 *   - main keeps accept(); on accept, when designc-enabled, the raw fd is
 *     handed off round-robin to an io worker PERMANENTLY and never appears in
 *     server.el again.
 *   - each worker owns its own raw epoll set, does all read()/parse/classify/
 *     write() for its clients. FAST path (exactly `GET <key>`) becomes a bare
 *     op record pushed to that worker's per-worker SPSC queue to main. Every
 *     other command is answered worker-side with a framed stage-1 error.
 *   - main's beforeSleep drains all per-worker SPSC op queues into batched
 *     hashtableIncrementalFind (cap 31) against the REAL keyspace, applies real
 *     expiry semantics, memcpys value bytes (cap 512B) into a reply record, and
 *     pushes it back to the origin worker's reply SPSC queue.
 */
#ifndef DESIGNC_H
#define DESIGNC_H

/* Validate config preconditions and, if designc-enabled, spawn workers.
 * Called from initServer() after listening sockets are created. On an invalid
 * combination (auth/ACL/replica/AOF/cluster set) this logs and exits. */
void designcInit(void);

/* Accept-time handoff: give a freshly accepted raw fd to a worker. Returns 1 if
 * the fd was taken by designc (caller must NOT create a client), 0 otherwise. */
int designcHandoffFd(int fd);

/* Drain all per-worker op queues and run the incremental-find applier batch.
 * Called from beforeSleep on the main thread. No-op if designc disabled. */
void designcApplierTick(void);

/* Clean worker shutdown (join threads, close fds). Called from
 * prepareForShutdown(). No-op if designc disabled. */
void designcShutdown(void);

/* Append the INFO `# designc` section (ops, ns/op, busy%, depth histogram). */
struct _rio;
void designcInfoAppend(sds *info);

#endif /* DESIGNC_H */
