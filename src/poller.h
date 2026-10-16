/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef POLLER_H
#define POLLER_H

/* First-class fd poller for the staged fast-path loop, kept separate from ae.
 *
 * A dedicated epoll thread (io_epoll) waits on the epoll set for as long as the poller is started
 * and appends every fired fd to the "next" ready buffer. Consumers read only the "active" buffer.
 * Main calls pollerGetNextBatch() once per iteration: it makes what accumulated in next the new
 * active buffer and hands the epoll thread an empty next, blocking only while next is empty.
 * Each buffer holds three classes by registered affinity: PRIORITY and IO fds, which the read-phase
 * IO threads claim through an atomic cursor (PRIORITY first), and MAIN fds, which main walks in
 * the write phase. Without a started thread, pollerGetNextBatch waits on epoll itself.
 *
 * IO and PRIORITY fds are edge-triggered: an fd is reported once per readiness edge, so a
 * consumer that stops before EAGAIN must hand the fd back with pollerRequeue(). MAIN fds are
 * one-shot level-triggered and are re-armed when the batch that reported them is replaced. */

#include "fastpath_no_epoll.h"

/* Declared on every platform so fast-path code compiles unchanged; without epoll the calls are
 * stubs that register nothing and report no events, and staged mode never turns on. */

#include <stddef.h>

/* Interest and fired masks. Independent of ae's AE_* so the poller does not depend on ae.h. */
#define POLLER_NONE 0
#define POLLER_READABLE 1
#define POLLER_WRITABLE 2
#define POLLER_ERROR 4 /* only ever reported to a handler, never requested */

/* Which stage consumes a ready fd and on which thread its handler runs. */
typedef enum {
    POLLER_AFF_IO = 0,       /* client sockets: IO threads, read phase */
    POLLER_AFF_MAIN = 1,     /* ae's fd (listeners, cluster bus, replication, module pipes): main, write phase */
    POLLER_AFF_PRIORITY = 2, /* IO threads, read phase, claimed before every IO fd */
} pollerAffinity;

typedef void (*pollerHandler)(int fd, int mask, void *privdata, pollerAffinity affinity);

/* One claimed ready fd, resolved against its current registration. */
typedef struct pollerEvent {
    void *privdata;
    pollerHandler handler;
    int fd;
    int mask; /* fired mask */
    pollerAffinity affinity;
} pollerEvent;

typedef struct poller poller;

poller *pollerCreate(int setsize);
/* Stops the epoll thread first if it runs. */
void pollerFree(poller *p);

/* Register, change, or drop an fd; 0 on success, -1 on failure. Each fd's slot has one writer at a
 * time: main between phases, or the IO thread that claimed the fd during a phase, so IO threads may
 * modify and unregister different fds concurrently; the registered count is atomic for that reason.
 * pollerRegister is main only, outside the read and reply phases: it may grow the fd table. Register fails on a duplicate
 * fd, an fd outside setsize, or an epoll_ctl error. Modify fails on an fd that is not
 * registered. Unregister of an fd that is not registered is a no-op returning 0.
 * POLLER_NONE keeps the fd registered; errors and hangups are still reported.
 * Every register and unregister starts a new generation of the fd's slot: a ready entry queued
 * under an older generation is dropped when claimed. */
int pollerRegister(poller *p, int fd, int mask, pollerHandler handler, void *privdata, pollerAffinity affinity);
int pollerModify(poller *p, int fd, int mask);
int pollerUnregister(poller *p, int fd);

/* Main only, while no read phase claims from the active buffer. Retires the active buffer and
 * makes the accumulated next buffer active. Blocks while next is empty, up to timeout_us
 * (negative = forever, 0 = never). Returns the number of fds in the new active buffer, 0 on
 * timeout, -1 on an epoll error of the inline wait. With work pending it makes no syscall
 * except re-arming the MAIN fds the retired buffer reported. */
int pollerGetNextBatch(poller *p, long long timeout_us);

/* Owner of a registered PRIORITY or IO fd (the IO thread that claimed it, or main between phases):
 * queue it in the next buffer with the given fired mask, so the next batch serves it without a new
 * readiness edge. At most one entry per fd per buffer; masks of repeats are merged. */
void pollerRequeue(poller *p, int fd, int mask);

/* Active PRIORITY and IO entries: consumed by IO threads. Claims PRIORITY entries first, then IO,
 * through an atomic cursor; returns 1 and fills out on success, 0 when both are exhausted.
 * Entries whose fd changed generation since they were queued are skipped. Safe from many threads. */
int pollerClaimIO(poller *p, pollerEvent *out);
int pollerIOCount(const poller *p); /* PRIORITY plus IO entries in the active buffer */

/* Active MAIN entries: walked by main. pollerMainAt returns 1 and fills out, 0 for a stale entry
 * or an index out of range. */
int pollerMainCount(const poller *p);
int pollerMainAt(const poller *p, int i, pollerEvent *out);

/* Main: start the epoll thread, named io_epoll, on the CPU set last given to
 * pollerSetThreadAffinity (the creator's otherwise). 0 on success or when already started. */
int pollerStart(poller *p);
/* Main: join the epoll thread and empty both buffers. No-op when not started. */
void pollerStop(poller *p);
int pollerThreadRunning(const poller *p);
/* Main: CPU set (a cpu_set_t of cpusetsize bytes) for the epoll thread, applied now when it runs
 * and at every later start. 0 on success. */
int pollerSetThreadAffinity(poller *p, const void *cpuset, size_t cpusetsize);

int pollerFd(const poller *p);    /* underlying epoll fd, for debug/nesting */
int pollerCount(const poller *p); /* number of registered fds */
int pollerSetSize(const poller *p);
/* Main, between phases: change setsize like aeResizeSetSize. Fails if a registered fd would fall
 * outside the new size. Pauses a running epoll thread; queued entries are kept. */
int pollerResize(poller *p, int setsize);

#endif /* POLLER_H */
