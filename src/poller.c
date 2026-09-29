/* First-class epoll poller for the staged fast-path loop. See poller.h. */

#include "fmacros.h"
#include "poller.h"

#ifdef HAVE_FASTPATH_EPOLL

#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <stdint.h>
#include <limits.h>
#include <stdatomic.h>
#include <sys/epoll.h>
#include "zmalloc.h"

/* One registered fd. Indexed by fd, like ae's file-event table. */
typedef struct pollerReg {
    pollerHandler handler;
    void *privdata;
    int mask; /* requested interest */
    uint8_t affinity;
    uint8_t registered;
} pollerReg;

struct poller {
    int epfd;
    int setsize;
    int maxevents; /* fds one pollerGetFDs reports at most; level-triggered, so the rest fire next time */
    _Atomic(int) count;        /* registered fds; IO threads register/unregister concurrently in a phase */
    pollerReg *regs;           /* regs_cap entries, indexed by fd; grows with the highest fd registered */
    int regs_cap;
    struct epoll_event *fired; /* maxevents entries, epoll_wait scratch */
    pollerEvent *io_ready;     /* maxevents entries; drained by IO threads via io_cursor */
    int io_count;              /* filled by pollerGetFDs (single writer: main) */
    _Atomic(int) io_cursor;    /* next unclaimed IO entry */
    pollerEvent *main_ready;   /* maxevents entries; walked by main */
    int main_count;
};

#define POLLER_MAX_EVENTS 1024

static int pollerMaxEvents(int setsize) {
    return setsize < POLLER_MAX_EVENTS ? setsize : POLLER_MAX_EVENTS;
}

static int maskToEpoll(int mask) {
    int ev = 0;
    if (mask & POLLER_READABLE) ev |= EPOLLIN;
    if (mask & POLLER_WRITABLE) ev |= EPOLLOUT;
    return ev;
}

poller *pollerCreate(int setsize) {
    if (setsize <= 0) return NULL;
    poller *p = zcalloc(sizeof(*p));
    if (!p) return NULL;
    p->setsize = setsize;
    p->maxevents = pollerMaxEvents(setsize);
    p->epfd = epoll_create1(EPOLL_CLOEXEC);
    if (p->epfd < 0) {
        zfree(p);
        return NULL;
    }
    p->fired = zmalloc(sizeof(struct epoll_event) * p->maxevents);
    p->io_ready = zmalloc(sizeof(pollerEvent) * p->maxevents);
    p->main_ready = zmalloc(sizeof(pollerEvent) * p->maxevents);
    if (!p->fired || !p->io_ready || !p->main_ready) {
        pollerFree(p);
        return NULL;
    }
    atomic_init(&p->io_cursor, 0);
    atomic_init(&p->count, 0);
    return p;
}

void pollerFree(poller *p) {
    if (!p) return;
    if (p->epfd >= 0) close(p->epfd);
    zfree(p->regs);
    zfree(p->fired);
    zfree(p->io_ready);
    zfree(p->main_ready);
    zfree(p);
}

/* Main only: may move the table, so IO threads must not be using it. */
static int pollerRegsCover(poller *p, int fd) {
    if (fd < p->regs_cap) return 0;
    int cap = p->regs_cap ? p->regs_cap : 64;
    while (cap <= fd) cap *= 2;
    if (cap > p->setsize) cap = p->setsize;
    pollerReg *regs = zrealloc(p->regs, sizeof(pollerReg) * cap);
    memset(regs + p->regs_cap, 0, sizeof(pollerReg) * (cap - p->regs_cap));
    p->regs = regs;
    p->regs_cap = cap;
    return 0;
}

int pollerRegister(poller *p, int fd, int mask, pollerHandler handler, void *privdata, pollerAffinity affinity) {
    if (fd < 0 || fd >= p->setsize) return -1;
    pollerRegsCover(p, fd);
    pollerReg *r = &p->regs[fd];
    if (r->registered) return -1;
    if (affinity != POLLER_AFF_IO && affinity != POLLER_AFF_MAIN) return -1;
    struct epoll_event ee = {0};
    ee.events = maskToEpoll(mask);
    ee.data.fd = fd;
    if (epoll_ctl(p->epfd, EPOLL_CTL_ADD, fd, &ee) != 0) return -1;
    r->handler = handler;
    r->privdata = privdata;
    r->mask = mask;
    r->affinity = affinity;
    r->registered = 1;
    atomic_fetch_add_explicit(&p->count, 1, memory_order_relaxed);
    return 0;
}

int pollerModify(poller *p, int fd, int mask) {
    if (fd < 0 || fd >= p->regs_cap) return -1;
    pollerReg *r = &p->regs[fd];
    if (!r->registered) return -1;
    if (r->mask == mask) return 0;
    struct epoll_event ee = {0};
    ee.events = maskToEpoll(mask);
    ee.data.fd = fd;
    if (epoll_ctl(p->epfd, EPOLL_CTL_MOD, fd, &ee) != 0) return -1;
    r->mask = mask;
    return 0;
}

int pollerUnregister(poller *p, int fd) {
    if (fd < 0 || fd >= p->setsize) return -1;
    if (fd >= p->regs_cap) return 0;
    pollerReg *r = &p->regs[fd];
    if (!r->registered) return 0;
    /* Kernels before 2.6.9 need a non-null event even for DEL; a closed fd is already gone, so errors are ignored. */
    struct epoll_event ee = {0};
    epoll_ctl(p->epfd, EPOLL_CTL_DEL, fd, &ee);
    memset(r, 0, sizeof(*r));
    atomic_fetch_sub_explicit(&p->count, 1, memory_order_relaxed);
    return 0;
}

/* Map an epoll fired mask to the poller mask, reporting an error/hup as both directions plus
 * POLLER_ERROR so a handler that only requested one direction still runs and sees the error. */
static int epollToMask(uint32_t events) {
    int mask = 0;
    if (events & EPOLLIN) mask |= POLLER_READABLE;
    if (events & EPOLLOUT) mask |= POLLER_WRITABLE;
    if (events & (EPOLLERR | EPOLLHUP)) mask |= POLLER_READABLE | POLLER_WRITABLE | POLLER_ERROR;
    return mask;
}

int pollerGetFDs(poller *p, long long timeout_us) {
    int timeout_ms;
    if (timeout_us < 0)
        timeout_ms = -1;
    else if (timeout_us > (long long)INT_MAX * 1000 - 999)
        timeout_ms = INT_MAX;
    else
        timeout_ms = (int)((timeout_us + 999) / 1000);

    p->io_count = 0;
    p->main_count = 0;
    atomic_store_explicit(&p->io_cursor, 0, memory_order_relaxed);

    /* EINTR is reported as zero fired fds rather than retried, so a signal cannot extend the wait. */
    int n = epoll_wait(p->epfd, p->fired, p->maxevents, timeout_ms);
    if (n < 0) return errno == EINTR ? 0 : -1;

    for (int i = 0; i < n; i++) {
        int fd = p->fired[i].data.fd;
        if (fd < 0 || fd >= p->regs_cap) continue;
        pollerReg *r = &p->regs[fd];
        if (!r->registered) continue;
        pollerEvent e = {
            .privdata = r->privdata,
            .handler = r->handler,
            .fd = fd,
            .mask = epollToMask(p->fired[i].events),
            .affinity = r->affinity,
        };
        if (r->affinity == POLLER_AFF_IO)
            p->io_ready[p->io_count++] = e;
        else
            p->main_ready[p->main_count++] = e;
    }
    return p->io_count + p->main_count;
}

/* Relaxed is enough: the ready set and io_count are published to IO threads by the phase
 * barrier that opens the read phase, and fetch_add alone guarantees each index is claimed once. */
int pollerClaimIO(poller *p, pollerEvent *out) {
    if (atomic_load_explicit(&p->io_cursor, memory_order_relaxed) >= p->io_count) return 0;
    int idx = atomic_fetch_add_explicit(&p->io_cursor, 1, memory_order_relaxed);
    if (idx >= p->io_count) return 0;
    *out = p->io_ready[idx];
    return 1;
}

int pollerIOCount(const poller *p) {
    return p->io_count;
}

int pollerMainCount(const poller *p) {
    return p->main_count;
}

const pollerEvent *pollerMainAt(const poller *p, int i) {
    if (i < 0 || i >= p->main_count) return NULL;
    return &p->main_ready[i];
}

int pollerFd(const poller *p) {
    return p->epfd;
}

int pollerCount(const poller *p) {
    return atomic_load_explicit(&((poller *)p)->count, memory_order_relaxed);
}

int pollerSetSize(const poller *p) {
    return p->setsize;
}

int pollerResize(poller *p, int setsize) {
    if (setsize <= 0) return -1;
    if (setsize == p->setsize) return 0;
    for (int fd = setsize; fd < p->regs_cap; fd++)
        if (p->regs[fd].registered) return -1;
    if (p->regs_cap > setsize) {
        p->regs = zrealloc(p->regs, sizeof(pollerReg) * setsize);
        p->regs_cap = setsize;
    }
    p->maxevents = pollerMaxEvents(setsize);
    p->fired = zrealloc(p->fired, sizeof(struct epoll_event) * p->maxevents);
    p->io_ready = zrealloc(p->io_ready, sizeof(pollerEvent) * p->maxevents);
    p->main_ready = zrealloc(p->main_ready, sizeof(pollerEvent) * p->maxevents);
    p->io_count = p->main_count = 0;
    atomic_store_explicit(&p->io_cursor, 0, memory_order_relaxed);
    p->setsize = setsize;
    return 0;
}

#endif /* HAVE_FASTPATH_EPOLL */
