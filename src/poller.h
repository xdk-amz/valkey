#ifndef POLLER_H
#define POLLER_H

/* First-class fd poller for the staged fast-path loop, kept separate from ae.
 *
 * Main calls pollerGetFDs() once at the top of an iteration. It demultiplexes the fired fds
 * by each fd's registered affinity into two ready sets: an IO set the read-phase IO threads
 * drain through an atomic cursor, and a main set main walks in the write phase. A registered
 * fd carries its handler, private data, interest mask and affinity; the ready entry carries a
 * snapshot of those so a consumer needs no second lookup. */

#include "fastpath_no_epoll.h"

#ifdef HAVE_FASTPATH_EPOLL

#include <stddef.h>

/* Interest and fired masks. Independent of ae's AE_* so the poller does not depend on ae.h. */
#define POLLER_NONE 0
#define POLLER_READABLE 1
#define POLLER_WRITABLE 2
#define POLLER_ERROR 4 /* only ever reported to a handler, never requested */

/* Which stage consumes a ready fd and on which thread its handler runs. */
typedef enum {
    POLLER_AFF_IO = 0,   /* client sockets: IO threads, read phase */
    POLLER_AFF_MAIN = 1, /* listeners, cluster bus, replication links, module/pipe fds: main, write phase */
} pollerAffinity;

typedef void (*pollerHandler)(int fd, int mask, void *privdata, pollerAffinity affinity);

/* One ready fd published by pollerGetFDs, self-describing so a consumer needs no registry lookup. */
typedef struct pollerEvent {
    void *privdata;
    pollerHandler handler;
    int fd;
    int mask; /* fired mask */
    pollerAffinity affinity;
} pollerEvent;

typedef struct poller poller;

poller *pollerCreate(int setsize);
void pollerFree(poller *p);

/* Register, change, or drop an fd; 0 on success, -1 on failure. Register fails on a duplicate
 * fd, an fd outside setsize, or an epoll_ctl error. Modify fails on an fd that is not
 * registered. Unregister of an fd that is not registered is a no-op returning 0.
 * POLLER_NONE keeps the fd registered; errors and hangups are still reported. */
int pollerRegister(poller *p, int fd, int mask, pollerHandler handler, void *privdata, pollerAffinity affinity);
int pollerModify(poller *p, int fd, int mask);
int pollerUnregister(poller *p, int fd);

/* Block up to timeout_us (negative = forever, 0 = return immediately, positive rounds up to
 * whole ms), fill both ready sets, and return the total number of fired fds, 0 on timeout or
 * EINTR, -1 on an epoll error. Resets both ready sets first. Main only; must not overlap a
 * read phase that is still claiming from the IO set. */
int pollerGetFDs(poller *p, long long timeout_us);

/* IO ready set: consumed by IO threads. Claim the next entry with an atomic cursor; returns 1
 * and fills out on success, 0 when the set is exhausted. Safe to call from many threads. */
int pollerClaimIO(poller *p, pollerEvent *out);
int pollerIOCount(const poller *p);

/* Main ready set: walked by main in the write phase. */
int pollerMainCount(const poller *p);
const pollerEvent *pollerMainAt(const poller *p, int i);

int pollerFd(const poller *p);    /* underlying epoll fd, for debug/nesting */
int pollerCount(const poller *p); /* number of registered fds */

#endif /* HAVE_FASTPATH_EPOLL */
#endif /* POLLER_H */
