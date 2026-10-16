/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* First-class epoll poller for the staged fast-path loop. See poller.h. */

#include "fmacros.h"
#include "poller.h"

#ifdef HAVE_FASTPATH_EPOLL

#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <stdint.h>
#include <limits.h>
#include <time.h>
#include <sched.h>
#include <signal.h>
#include <pthread.h>
#include <stdatomic.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include "zmalloc.h"

/* One registered fd. Indexed by fd, like ae's file-event table. gen survives unregister so a
 * reused fd never matches an entry queued for its previous registration. */
typedef struct pollerReg {
    pollerHandler handler;
    void *privdata;
    int mask; /* requested interest */
    uint32_t gen;
    uint8_t affinity;
    uint8_t registered;
} pollerReg;

/* One queued ready fd. fd is -1 once a later entry for the same fd replaced it. */
typedef struct pollerSlot {
    int fd;
    uint32_t gen;
    int mask;
} pollerSlot;

#define POLLER_NCLASS 3 /* indexed by pollerAffinity */
#define POLLER_POS_CLASS_SHIFT 30
#define POLLER_POS_IDX_MASK ((1u << POLLER_POS_CLASS_SHIFT) - 1)
#define POLLER_FD_LIMIT (1 << 30) /* fds and affinity share the low word of the epoll tag */
#define POLLER_CTL_TAG UINT64_MAX /* the control eventfd's epoll tag */

/* One ready buffer. pos[fd] is 0 when fd is not queued here, else its class and 1 + its index. */
typedef struct pollerBuf {
    pollerSlot *e[POLLER_NCLASS];
    int n[POLLER_NCLASS];
    int cap[POLLER_NCLASS];
    uint32_t *pos; /* setsize entries */
} pollerBuf;

enum {
    POLLER_CTL_RUN = 0,
    POLLER_CTL_PAUSE,
    POLLER_CTL_STOP
};
enum {
    POLLER_T_RUNNING = 0,
    POLLER_T_PAUSED,
    POLLER_T_EXITED
};

/* Locking: lock guards the next buffer (buf[active ^ 1]) and its pos array, active, and the thread
 * control fields. The active buffer is written only by main, in pollerGetNextBatch, and read by
 * consumers after main published it (IO threads through the phase barrier). */
struct poller {
    int epfd;
    int evfd; /* wakes the epoll thread for pause and stop */
    int setsize;
    int maxevents; /* inline wait: fds one epoll_wait reports at most */
    _Atomic(int) count;
    pollerReg *regs; /* regs_cap entries, indexed by fd; grows with the highest fd registered */
    int regs_cap;
    struct epoll_event *fired; /* maxevents entries, inline epoll_wait scratch */
    pollerBuf buf[2];
    int active;
    _Atomic(int) cursor; /* next unclaimed PRIORITY/IO entry of the active buffer */
    pthread_mutex_t lock;
    pthread_cond_t ready_cv; /* main waits for a non-empty next buffer */
    pthread_cond_t ctl_cv;   /* pause/stop handshake */
    int main_waiting;
    int ctl, tstate;
    int started; /* main only */
    pthread_t thread;
    cpu_set_t cpuset;
    int has_cpuset;
};

#define POLLER_MAX_EVENTS 1024

static int pollerMaxEvents(int setsize) {
    return setsize < POLLER_MAX_EVENTS ? setsize : POLLER_MAX_EVENTS;
}

static uint32_t maskToEpoll(int mask, int affinity) {
    uint32_t ev = 0;
    if (mask & POLLER_READABLE) ev |= EPOLLIN;
    if (mask & POLLER_WRITABLE) ev |= EPOLLOUT;
    /* IO fds: one report per edge, never re-reported while unread. MAIN fds: level-triggered but
     * disarmed after each report, re-armed once main consumed the batch that carried it. */
    ev |= affinity == POLLER_AFF_MAIN ? EPOLLONESHOT : EPOLLET;
    return ev;
}

static uint64_t epollTag(int fd, uint32_t gen, int affinity) {
    return (uint64_t)gen << 32 | (uint64_t)affinity << POLLER_POS_CLASS_SHIFT | (uint32_t)fd;
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

static int bufTotal(const pollerBuf *b) {
    return b->n[0] + b->n[1] + b->n[2];
}

static void bufFree(pollerBuf *b) {
    for (int c = 0; c < POLLER_NCLASS; c++) zfree(b->e[c]);
    zfree(b->pos);
    memset(b, 0, sizeof(*b));
}

/* Drop every entry. The caller owns the buffer (main on active, or the lock on next). */
static void bufClear(pollerBuf *b) {
    for (int c = 0; c < POLLER_NCLASS; c++) {
        for (int i = 0; i < b->n[c]; i++)
            if (b->e[c][i].fd >= 0) b->pos[b->e[c][i].fd] = 0;
        b->n[c] = 0;
    }
}

/* Lock held. Queue fd in the next buffer, merging with an entry it already has there. */
static void postLocked(poller *p, int fd, uint32_t gen, int cls, int mask) {
    if (fd < 0 || fd >= p->setsize || cls < 0 || cls >= POLLER_NCLASS) return;
    pollerBuf *b = &p->buf[p->active ^ 1];
    uint32_t pv = b->pos[fd];
    if (pv) {
        int oc = (int)(pv >> POLLER_POS_CLASS_SHIFT);
        pollerSlot *s = &b->e[oc][(pv & POLLER_POS_IDX_MASK) - 1];
        if (oc == cls) {
            if (s->gen == gen) {
                s->mask |= mask;
            } else {
                s->gen = gen;
                s->mask = mask;
            }
            return;
        }
        s->fd = -1;
    }
    if (b->n[cls] == b->cap[cls]) {
        int cap = b->cap[cls] * 2;
        b->e[cls] = zrealloc(b->e[cls], sizeof(pollerSlot) * cap);
        b->cap[cls] = cap;
    }
    int idx = b->n[cls]++;
    b->e[cls][idx] = (pollerSlot){.fd = fd, .gen = gen, .mask = mask};
    b->pos[fd] = (uint32_t)cls << POLLER_POS_CLASS_SHIFT | (uint32_t)(idx + 1);
}

/* Lock held. */
static void postFiredLocked(poller *p, const struct epoll_event *ev) {
    uint64_t tag = ev->data.u64;
    int fd = (int)(tag & POLLER_POS_IDX_MASK);
    int cls = (int)((tag >> POLLER_POS_CLASS_SHIFT) & 3);
    postLocked(p, fd, (uint32_t)(tag >> 32), cls, epollToMask(ev->events));
}

static void drainEventfd(poller *p) {
    uint64_t v;
    while (read(p->evfd, &v, sizeof(v)) == (ssize_t)sizeof(v));
}

static void wakeThread(poller *p) {
    uint64_t one = 1;
    ssize_t n;
    do {
        n = write(p->evfd, &one, sizeof(one));
    } while (n < 0 && errno == EINTR);
}

poller *pollerCreate(int setsize) {
    if (setsize <= 0 || setsize > POLLER_FD_LIMIT) return NULL;
    poller *p = zcalloc(sizeof(*p));
    p->setsize = setsize;
    p->maxevents = pollerMaxEvents(setsize);
    p->epfd = epoll_create1(EPOLL_CLOEXEC);
    p->evfd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    pthread_condattr_t ca;
    pthread_condattr_init(&ca);
    pthread_condattr_setclock(&ca, CLOCK_MONOTONIC);
    pthread_mutex_init(&p->lock, NULL);
    pthread_cond_init(&p->ready_cv, &ca);
    pthread_cond_init(&p->ctl_cv, &ca);
    pthread_condattr_destroy(&ca);
    struct epoll_event ee = {.events = EPOLLIN, .data.u64 = POLLER_CTL_TAG};
    if (p->epfd < 0 || p->evfd < 0 || epoll_ctl(p->epfd, EPOLL_CTL_ADD, p->evfd, &ee) != 0) {
        pollerFree(p);
        return NULL;
    }
    p->fired = zmalloc(sizeof(struct epoll_event) * p->maxevents);
    for (int i = 0; i < 2; i++) {
        pollerBuf *b = &p->buf[i];
        b->pos = zcalloc(sizeof(uint32_t) * setsize);
        /* Sized up front so the epoll thread does not allocate: a buffer holds each fd at most once. */
        b->cap[POLLER_AFF_IO] = setsize;
        b->cap[POLLER_AFF_MAIN] = b->cap[POLLER_AFF_PRIORITY] = 16;
        for (int c = 0; c < POLLER_NCLASS; c++) b->e[c] = zmalloc(sizeof(pollerSlot) * b->cap[c]);
    }
    atomic_init(&p->cursor, 0);
    atomic_init(&p->count, 0);
    return p;
}

void pollerFree(poller *p) {
    if (!p) return;
    pollerStop(p);
    if (p->epfd >= 0) close(p->epfd);
    if (p->evfd >= 0) close(p->evfd);
    pthread_cond_destroy(&p->ctl_cv);
    pthread_cond_destroy(&p->ready_cv);
    pthread_mutex_destroy(&p->lock);
    zfree(p->regs);
    zfree(p->fired);
    bufFree(&p->buf[0]);
    bufFree(&p->buf[1]);
    zfree(p);
}

/* Main only, between phases: may move the table. The epoll thread never reads it. */
static void pollerRegsCover(poller *p, int fd) {
    if (fd < p->regs_cap) return;
    int cap = p->regs_cap ? p->regs_cap : 64;
    while (cap <= fd) cap *= 2;
    if (cap > p->setsize) cap = p->setsize;
    pollerReg *regs = zrealloc(p->regs, sizeof(pollerReg) * cap);
    memset(regs + p->regs_cap, 0, sizeof(pollerReg) * (cap - p->regs_cap));
    p->regs = regs;
    p->regs_cap = cap;
}

int pollerRegister(poller *p, int fd, int mask, pollerHandler handler, void *privdata, pollerAffinity affinity) {
    if (fd < 0 || fd >= p->setsize) return -1;
    if (affinity != POLLER_AFF_IO && affinity != POLLER_AFF_MAIN && affinity != POLLER_AFF_PRIORITY) return -1;
    pollerRegsCover(p, fd);
    pollerReg *r = &p->regs[fd];
    if (r->registered) return -1;
    uint32_t gen = r->gen + 1;
    struct epoll_event ee = {0};
    ee.events = maskToEpoll(mask, affinity);
    ee.data.u64 = epollTag(fd, gen, affinity);
    if (epoll_ctl(p->epfd, EPOLL_CTL_ADD, fd, &ee) != 0) return -1;
    r->gen = gen;
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
    ee.events = maskToEpoll(mask, r->affinity);
    ee.data.u64 = epollTag(fd, r->gen, r->affinity);
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
    uint32_t gen = r->gen + 1;
    memset(r, 0, sizeof(*r));
    r->gen = gen;
    atomic_fetch_sub_explicit(&p->count, 1, memory_order_relaxed);
    return 0;
}

/* The slot's registration is still the one it was queued under. */
static int resolveSlot(const poller *p, const pollerSlot *s, pollerEvent *out) {
    int fd = s->fd;
    if (fd < 0 || fd >= p->regs_cap) return 0;
    const pollerReg *r = &p->regs[fd];
    if (!r->registered || r->gen != s->gen) return 0;
    out->privdata = r->privdata;
    out->handler = r->handler;
    out->fd = fd;
    out->mask = s->mask;
    out->affinity = (pollerAffinity)r->affinity;
    return 1;
}

/* Main: the retired active buffer. Re-arm the one-shot MAIN fds it reported, then empty it. */
static void retireActive(poller *p) {
    pollerBuf *b = &p->buf[p->active];
    for (int i = 0; i < b->n[POLLER_AFF_MAIN]; i++) {
        const pollerSlot *s = &b->e[POLLER_AFF_MAIN][i];
        if (s->fd < 0 || s->fd >= p->regs_cap) continue;
        const pollerReg *r = &p->regs[s->fd];
        if (!r->registered || r->gen != s->gen) continue;
        struct epoll_event ee = {0};
        ee.events = maskToEpoll(r->mask, r->affinity);
        ee.data.u64 = epollTag(s->fd, r->gen, r->affinity);
        epoll_ctl(p->epfd, EPOLL_CTL_MOD, s->fd, &ee);
    }
    bufClear(b);
    atomic_store_explicit(&p->cursor, 0, memory_order_relaxed);
}

static int timeoutMs(long long timeout_us) {
    if (timeout_us < 0) return -1;
    if (timeout_us > (long long)INT_MAX * 1000 - 999) return INT_MAX;
    return (int)((timeout_us + 999) / 1000);
}

/* Lock held, thread running: wait for a non-empty next buffer until timeout_us passes. */
static void waitForNextLocked(poller *p, long long timeout_us) {
    struct timespec dl;
    if (timeout_us > 0) {
        clock_gettime(CLOCK_MONOTONIC, &dl);
        long long ns = dl.tv_nsec + (timeout_us % 1000000) * 1000;
        dl.tv_sec += timeout_us / 1000000 + ns / 1000000000;
        dl.tv_nsec = ns % 1000000000;
    }
    p->main_waiting = 1;
    while (bufTotal(&p->buf[p->active ^ 1]) == 0 && p->tstate != POLLER_T_EXITED) {
        if (timeout_us < 0) {
            pthread_cond_wait(&p->ready_cv, &p->lock);
        } else if (pthread_cond_timedwait(&p->ready_cv, &p->lock, &dl) == ETIMEDOUT) {
            break;
        }
    }
    p->main_waiting = 0;
}

int pollerGetNextBatch(poller *p, long long timeout_us) {
    retireActive(p);
    int rc = 0;
    if (p->started) {
        pthread_mutex_lock(&p->lock);
        if (p->tstate == POLLER_T_EXITED) {
            pthread_mutex_unlock(&p->lock);
            goto inline_wait;
        }
        if (timeout_us != 0 && bufTotal(&p->buf[p->active ^ 1]) == 0) waitForNextLocked(p, timeout_us);
    } else {
    inline_wait:
        pthread_mutex_lock(&p->lock);
        int pending = bufTotal(&p->buf[p->active ^ 1]);
        pthread_mutex_unlock(&p->lock);
        /* EINTR is reported as nothing fired rather than retried, so a signal cannot extend the wait. */
        int n = epoll_wait(p->epfd, p->fired, p->maxevents, pending ? 0 : timeoutMs(timeout_us));
        if (n < 0 && errno != EINTR) rc = -1;
        int ctl = 0;
        pthread_mutex_lock(&p->lock);
        for (int i = 0; i < n; i++) {
            if (p->fired[i].data.u64 == POLLER_CTL_TAG)
                ctl = 1;
            else
                postFiredLocked(p, &p->fired[i]);
        }
        if (ctl) drainEventfd(p);
    }
    p->active ^= 1;
    int total = bufTotal(&p->buf[p->active]);
    pthread_mutex_unlock(&p->lock);
    return rc < 0 ? -1 : total;
}

void pollerRequeue(poller *p, int fd, int mask) {
    if (fd < 0 || fd >= p->regs_cap) return;
    const pollerReg *r = &p->regs[fd];
    if (!r->registered || r->affinity == POLLER_AFF_MAIN) return;
    pthread_mutex_lock(&p->lock);
    postLocked(p, fd, r->gen, r->affinity, mask);
    if (p->main_waiting) pthread_cond_signal(&p->ready_cv);
    pthread_mutex_unlock(&p->lock);
}

/* Relaxed is enough: the active buffer is published to IO threads by the phase barrier that opens
 * the read phase, and fetch_add alone guarantees each index is claimed once. */
int pollerClaimIO(poller *p, pollerEvent *out) {
    const pollerBuf *b = &p->buf[p->active];
    int np = b->n[POLLER_AFF_PRIORITY];
    int total = np + b->n[POLLER_AFF_IO];
    for (;;) {
        if (atomic_load_explicit(&p->cursor, memory_order_relaxed) >= total) return 0;
        int idx = atomic_fetch_add_explicit(&p->cursor, 1, memory_order_relaxed);
        if (idx >= total) return 0;
        const pollerSlot *s = idx < np ? &b->e[POLLER_AFF_PRIORITY][idx] : &b->e[POLLER_AFF_IO][idx - np];
        if (resolveSlot(p, s, out)) return 1;
    }
}

int pollerIOCount(const poller *p) {
    const pollerBuf *b = &p->buf[p->active];
    return b->n[POLLER_AFF_PRIORITY] + b->n[POLLER_AFF_IO];
}

int pollerMainCount(const poller *p) {
    return p->buf[p->active].n[POLLER_AFF_MAIN];
}

int pollerMainAt(const poller *p, int i, pollerEvent *out) {
    const pollerBuf *b = &p->buf[p->active];
    if (i < 0 || i >= b->n[POLLER_AFF_MAIN]) return 0;
    return resolveSlot(p, &b->e[POLLER_AFF_MAIN][i], out);
}

static void *pollerThreadMain(void *arg) {
    poller *p = arg;
    /* Asynchronous signals go to the other threads; faults stay deliverable here. */
    sigset_t set;
    sigfillset(&set);
    sigdelset(&set, SIGSEGV);
    sigdelset(&set, SIGBUS);
    sigdelset(&set, SIGFPE);
    sigdelset(&set, SIGILL);
    sigdelset(&set, SIGABRT);
    sigdelset(&set, SIGTRAP);
    pthread_sigmask(SIG_BLOCK, &set, NULL);
    pthread_setname_np(pthread_self(), "io_epoll");

    struct epoll_event ev[POLLER_MAX_EVENTS];
    for (;;) {
        int n = epoll_wait(p->epfd, ev, POLLER_MAX_EVENTS, -1);
        if (n < 0 && errno != EINTR) {
            pthread_mutex_lock(&p->lock);
            break;
        }
        int ctl = 0;
        for (int i = 0; i < n && !ctl; i++) ctl = ev[i].data.u64 == POLLER_CTL_TAG;
        if (ctl) drainEventfd(p);
        pthread_mutex_lock(&p->lock);
        for (int i = 0; i < n; i++)
            if (ev[i].data.u64 != POLLER_CTL_TAG) postFiredLocked(p, &ev[i]);
        if (p->main_waiting && bufTotal(&p->buf[p->active ^ 1]) > 0) pthread_cond_signal(&p->ready_cv);
        while (p->ctl == POLLER_CTL_PAUSE) {
            p->tstate = POLLER_T_PAUSED;
            pthread_cond_broadcast(&p->ctl_cv);
            pthread_cond_wait(&p->ctl_cv, &p->lock);
        }
        p->tstate = POLLER_T_RUNNING;
        if (p->ctl == POLLER_CTL_STOP) break;
        pthread_mutex_unlock(&p->lock);
    }
    /* Lock held. */
    p->tstate = POLLER_T_EXITED;
    pthread_cond_broadcast(&p->ctl_cv);
    pthread_cond_broadcast(&p->ready_cv);
    pthread_mutex_unlock(&p->lock);
    return NULL;
}

int pollerStart(poller *p) {
    if (p->started) return 0;
    pthread_mutex_lock(&p->lock);
    p->ctl = POLLER_CTL_RUN;
    p->tstate = POLLER_T_RUNNING;
    pthread_mutex_unlock(&p->lock);
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    /* Placed before it first runs, so it never starts on the creator's (main's) CPU. */
    if (p->has_cpuset) pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &p->cpuset);
    int err = pthread_create(&p->thread, &attr, pollerThreadMain, p);
    if (err == EINVAL && p->has_cpuset) {
        pthread_attr_destroy(&attr);
        pthread_attr_init(&attr);
        err = pthread_create(&p->thread, &attr, pollerThreadMain, p);
    }
    pthread_attr_destroy(&attr);
    if (err != 0) return -1;
    p->started = 1;
    return 0;
}

void pollerStop(poller *p) {
    if (!p->started) return;
    pthread_mutex_lock(&p->lock);
    p->ctl = POLLER_CTL_STOP;
    pthread_cond_broadcast(&p->ctl_cv);
    pthread_mutex_unlock(&p->lock);
    wakeThread(p);
    pthread_join(p->thread, NULL);
    p->started = 0;
    pthread_mutex_lock(&p->lock);
    p->ctl = POLLER_CTL_RUN;
    bufClear(&p->buf[0]);
    bufClear(&p->buf[1]);
    atomic_store_explicit(&p->cursor, 0, memory_order_relaxed);
    pthread_mutex_unlock(&p->lock);
    drainEventfd(p);
}

int pollerThreadRunning(const poller *p) {
    return p->started;
}

int pollerSetThreadAffinity(poller *p, const void *cpuset, size_t cpusetsize) {
    CPU_ZERO(&p->cpuset);
    memcpy(&p->cpuset, cpuset, cpusetsize < sizeof(cpu_set_t) ? cpusetsize : sizeof(cpu_set_t));
    if (CPU_COUNT(&p->cpuset) == 0) return -1;
    p->has_cpuset = 1;
    if (p->started && pthread_setaffinity_np(p->thread, sizeof(cpu_set_t), &p->cpuset) != 0) return -1;
    return 0;
}

/* Main: hold a running epoll thread outside the buffers until pollerResume. */
static int pollerPause(poller *p) {
    if (!p->started) return 0;
    pthread_mutex_lock(&p->lock);
    p->ctl = POLLER_CTL_PAUSE;
    pthread_mutex_unlock(&p->lock);
    wakeThread(p);
    pthread_mutex_lock(&p->lock);
    while (p->tstate != POLLER_T_PAUSED && p->tstate != POLLER_T_EXITED) pthread_cond_wait(&p->ctl_cv, &p->lock);
    pthread_mutex_unlock(&p->lock);
    return 1;
}

static void pollerResume(poller *p) {
    pthread_mutex_lock(&p->lock);
    p->ctl = POLLER_CTL_RUN;
    pthread_cond_broadcast(&p->ctl_cv);
    pthread_mutex_unlock(&p->lock);
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
    if (setsize <= 0 || setsize > POLLER_FD_LIMIT) return -1;
    if (setsize == p->setsize) return 0;
    for (int fd = setsize; fd < p->regs_cap; fd++)
        if (p->regs[fd].registered) return -1;
    int paused = pollerPause(p);
    if (p->regs_cap > setsize) {
        p->regs = zrealloc(p->regs, sizeof(pollerReg) * setsize);
        p->regs_cap = setsize;
    }
    for (int i = 0; i < 2; i++) {
        pollerBuf *b = &p->buf[i];
        /* Entries past the new size name fds that are not registered any more. */
        for (int c = 0; c < POLLER_NCLASS; c++)
            for (int k = 0; k < b->n[c]; k++)
                if (b->e[c][k].fd >= setsize) b->e[c][k].fd = -1;
        b->pos = zrealloc(b->pos, sizeof(uint32_t) * setsize);
        if (setsize > p->setsize) memset(b->pos + p->setsize, 0, sizeof(uint32_t) * (setsize - p->setsize));
        if (b->cap[POLLER_AFF_IO] < setsize) {
            b->e[POLLER_AFF_IO] = zrealloc(b->e[POLLER_AFF_IO], sizeof(pollerSlot) * setsize);
            b->cap[POLLER_AFF_IO] = setsize;
        }
    }
    p->maxevents = pollerMaxEvents(setsize);
    p->fired = zrealloc(p->fired, sizeof(struct epoll_event) * p->maxevents);
    p->setsize = setsize;
    if (paused) pollerResume(p);
    return 0;
}

#else /* !HAVE_FASTPATH_EPOLL */

poller *pollerCreate(int setsize) {
    (void)setsize;
    return NULL;
}
void pollerFree(poller *p) {
    (void)p;
}
int pollerRegister(poller *p, int fd, int mask, pollerHandler handler, void *privdata, pollerAffinity affinity) {
    (void)p, (void)fd, (void)mask, (void)handler, (void)privdata, (void)affinity;
    return -1;
}
int pollerModify(poller *p, int fd, int mask) {
    (void)p, (void)fd, (void)mask;
    return -1;
}
int pollerUnregister(poller *p, int fd) {
    (void)p, (void)fd;
    return 0;
}
int pollerGetNextBatch(poller *p, long long timeout_us) {
    (void)p, (void)timeout_us;
    return -1;
}
void pollerRequeue(poller *p, int fd, int mask) {
    (void)p, (void)fd, (void)mask;
}
int pollerClaimIO(poller *p, pollerEvent *out) {
    (void)p, (void)out;
    return 0;
}
int pollerIOCount(const poller *p) {
    (void)p;
    return 0;
}
int pollerMainCount(const poller *p) {
    (void)p;
    return 0;
}
int pollerMainAt(const poller *p, int i, pollerEvent *out) {
    (void)p, (void)i, (void)out;
    return 0;
}
int pollerStart(poller *p) {
    (void)p;
    return -1;
}
void pollerStop(poller *p) {
    (void)p;
}
int pollerThreadRunning(const poller *p) {
    (void)p;
    return 0;
}
int pollerSetThreadAffinity(poller *p, const void *cpuset, size_t cpusetsize) {
    (void)p, (void)cpuset, (void)cpusetsize;
    return -1;
}
int pollerFd(const poller *p) {
    (void)p;
    return -1;
}
int pollerCount(const poller *p) {
    (void)p;
    return 0;
}
int pollerSetSize(const poller *p) {
    (void)p;
    return 0;
}
int pollerResize(poller *p, int setsize) {
    (void)p, (void)setsize;
    return -1;
}

#endif /* HAVE_FASTPATH_EPOLL */
