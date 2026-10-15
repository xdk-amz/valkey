/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef FASTPATH_NO_EPOLL_H
#define FASTPATH_NO_EPOLL_H

/* The staged fast-path loop and its poller are Linux epoll only. This header is the single
 * switch every fast-path translation unit shares: it defines HAVE_FASTPATH_EPOLL when the
 * platform provides epoll, and leaves it undefined otherwise. A build without it compiles
 * with the fast path and its poller compiled out and keeps stock ae. */

#if defined(__linux__)
#include <sys/epoll.h>
#define HAVE_FASTPATH_EPOLL 1
#endif

#endif /* FASTPATH_NO_EPOLL_H */
