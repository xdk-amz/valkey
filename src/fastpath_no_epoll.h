/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* No-op epoll shims for builds without epoll (HAVE_FASTPATH_EPOLL undefined). The fast path
 * partitions clients onto per-IO-thread epoll instances, so it is compiled out where epoll does
 * not exist; fpSessionEligible() then returns 0 and none of these are reached at runtime. They
 * exist only so fastpath.c and io_threads.c compile without <sys/epoll.h>. */

#ifndef FASTPATH_NO_EPOLL_H
#define FASTPATH_NO_EPOLL_H

#include <stdint.h>

#define EPOLLIN 0x001u
#define EPOLLOUT 0x004u
#define EPOLLERR 0x008u
#define EPOLLHUP 0x010u
#define EPOLLONESHOT 0u
#define EPOLL_CTL_ADD 1
#define EPOLL_CTL_DEL 2
#define EPOLL_CTL_MOD 3
#define EPOLL_CLOEXEC 0

struct epoll_event {
    uint32_t events;
    union {
        void *ptr;
        int fd;
    } data;
};

static inline int epoll_create1(int flags) {
    (void)flags;
    return -1;
}
static inline int epoll_ctl(int epfd, int op, int fd, struct epoll_event *ev) {
    (void)epfd;
    (void)op;
    (void)fd;
    (void)ev;
    return 0;
}
static inline int epoll_wait(int epfd, struct epoll_event *evs, int maxevents, int timeout) {
    (void)epfd;
    (void)evs;
    (void)maxevents;
    (void)timeout;
    return 0;
}

#endif /* FASTPATH_NO_EPOLL_H */
