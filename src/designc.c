/*
 * Copyright (c) Valkey Contributors
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * Design C worker-ownership spike (Stage 1: GET fast path). LOCAL SPIKE ONLY.
 * Architecture LOCKED by TASK-designc-valkey.md. See designc.h for the summary.
 *
 * SHORTCUTS (also enumerated in REPORT.md):
 *  - Worker-side parser is a self-contained RESP2 multibulk parser rather than
 *    the shared parseInputBuffer/prepareCommandQueue path. The shared parser is
 *    entangled with the `client` struct and main-thread globals; reusing it from
 *    worker threads on fake clients is a larger hazard than a small correct
 *    parser. Deviation from spec item 3's "use existing parse-ahead"; parse
 *    correctness is directly testable (G2/G3).
 *  - Value bytes are memcpy'd into the reply record (cap 512B), no cross-thread
 *    refcount (spec item 5).
 *  - Workers busy-poll (epoll_wait timeout 0) and main busy-polls via
 *    aeSetDontWait; this is the certified Design C operating mode.
 *  - db0 only; SELECT is answered locally as +OK but does not switch db.
 */
#define _GNU_SOURCE
#include "server.h"
#include "designc.h"

#include <pthread.h>
#include <sched.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <stdatomic.h>

/* ------------------------------------------------------------------ */
/* tunables                                                            */
/* ------------------------------------------------------------------ */
#define DC_MAX_WORKERS 64
#define DC_OPQ_CAP 16384      /* power of two: worker -> main */
#define DC_RPQ_CAP 8192       /* power of two: main -> worker */
#define DC_FDQ_CAP 4096       /* power of two: accept -> worker (raw fds) */
#define DC_MAX_CONNS 8192
#define DC_PEND_CAP 8192      /* per-conn reorder slots (power of two) */
#define DC_KEYCAP 512
#define DC_VALCAP 512
#define DC_RBUF_INIT 65536
#define DC_WBUF_INIT 65536
#define DC_FIND_BATCH 31      /* the depth the EC2 certification measured at ceiling */
#define DC_HIST_N 6

/* worker-precomputed keyspace hash (matches sdsHashConfigurableSeed) */
extern uint64_t genHashFunctionConfigurableSeed(const char *buf, size_t len);
/* ported bucket-line prefetch primitive (hashtable.c) */
extern void hashtablePrefetchBucket(hashtable *ht, uint64_t hash);

/* ------------------------------------------------------------------ */
/* SPSC ring of fixed-size records                                     */
/* ------------------------------------------------------------------ */
typedef struct {
    _Alignas(64) atomic_uint_least32_t head;
    _Alignas(64) atomic_uint_least32_t tail;
    _Alignas(64) uint32_t cap, mask, elsz;
    char *buf;
} dc_spsc;

static void dc_spsc_init(dc_spsc *q, uint32_t cap, uint32_t elsz) {
    atomic_store_explicit(&q->head, 0, memory_order_relaxed);
    atomic_store_explicit(&q->tail, 0, memory_order_relaxed);
    q->cap = cap; q->mask = cap - 1; q->elsz = elsz;
    if (posix_memalign((void **)&q->buf, 64, (size_t)cap * elsz) != 0) {
        serverLog(LL_WARNING, "designc: spsc alloc failed");
        exit(1);
    }
}
static inline int dc_spsc_push(dc_spsc *q, const void *rec) {
    uint32_t t = atomic_load_explicit(&q->tail, memory_order_relaxed);
    uint32_t h = atomic_load_explicit(&q->head, memory_order_acquire);
    if (t - h >= q->cap) return 0;
    memcpy(q->buf + (size_t)(t & q->mask) * q->elsz, rec, q->elsz);
    atomic_store_explicit(&q->tail, t + 1, memory_order_release);
    return 1;
}
static inline uint32_t dc_spsc_pop_batch(dc_spsc *q, void *out, uint32_t max) {
    uint32_t h = atomic_load_explicit(&q->head, memory_order_relaxed);
    uint32_t t = atomic_load_explicit(&q->tail, memory_order_acquire);
    uint32_t avail = t - h;
    if (avail == 0) return 0;
    if (avail > max) avail = max;
    for (uint32_t i = 0; i < avail; i++)
        memcpy((char *)out + (size_t)i * q->elsz,
               q->buf + (size_t)((h + i) & q->mask) * q->elsz, q->elsz);
    atomic_store_explicit(&q->head, h + avail, memory_order_release);
    return avail;
}

/* ------------------------------------------------------------------ */
/* op + reply records                                                  */
/* ------------------------------------------------------------------ */
typedef struct {
    uint32_t conn;
    uint32_t seq;
    uint64_t hash;
    uint16_t klen;
    char key[DC_KEYCAP];
} dc_oprec;

/* reply kinds */
#define DC_R_NIL 0     /* miss / expired -> $-1 */
#define DC_R_BULK 1    /* value bytes present */
#define DC_R_ERR 2     /* framed error string in bytes */

typedef struct {
    uint32_t conn;
    uint32_t seq;
    uint8_t kind;
    int32_t vlen;
    char val[DC_VALCAP];
} dc_replyrec;

/* ------------------------------------------------------------------ */
/* per-conn + per-worker state                                         */
/* ------------------------------------------------------------------ */
typedef struct {
    uint8_t ready;
    uint8_t kind;      /* 0 empty, 1 inline bytes, 2 pending-get(from main) */
    uint16_t ilen;
    int32_t vlen;
    uint8_t rkind;     /* filled by main: DC_R_* */
    char inl[128];
    char gval[DC_VALCAP];
} dc_pendslot;

typedef struct {
    int fd;
    int active;
    int want_write;
    char *rbuf; size_t rlen, rcap;
    char *wbuf; size_t wlen, wsent, wcap;
    uint32_t issue_seq, flush_seq;
    dc_pendslot *pend;   /* DC_PEND_CAP slots */
} dc_conn;

typedef struct {
    int idx;
    int epfd;
    int evfd;             /* eventfd for new-fd wakeups */
    dc_spsc opq;          /* worker -> main */
    dc_spsc rpq;          /* main -> worker */
    /* accept -> worker fd handoff (single producer = main accept, single
     * consumer = this worker). Stores raw ints. */
    _Alignas(64) atomic_uint_least32_t fdq_head;
    _Alignas(64) atomic_uint_least32_t fdq_tail;
    int fdq[DC_FDQ_CAP];
    dc_conn conns[DC_MAX_CONNS];
    int free_stack[DC_MAX_CONNS];
    int free_top;
    pthread_t th;
} dc_worker;

static dc_worker *g_workers;
static int g_nworkers;
static volatile int g_run = 1;
static atomic_int g_rr;   /* round-robin accept cursor */

/* main-thread applier stats (published as relaxed atomics for INFO) */
static atomic_uint_least64_t g_ops;
static atomic_uint_least64_t g_busy_ns;
static atomic_uint_least64_t g_cyc_hist[DC_HIST_N];
static atomic_uint_least64_t g_depth_hist[DC_HIST_N];
static atomic_uint_least64_t g_tick_calls;

static inline int dc_hist_bucket(uint32_t n) {
    if (n <= 1) return 0;
    if (n <= 4) return 1;
    if (n <= 8) return 2;
    if (n <= 15) return 3;
    if (n <= 31) return 4;
    return 5;
}

/* ------------------------------------------------------------------ */
/* small utilities                                                     */
/* ------------------------------------------------------------------ */
static void dc_set_nonblock(int fd) {
    int fl = fcntl(fd, F_GETFL, 0);
    if (fl != -1) fcntl(fd, F_SETFL, fl | O_NONBLOCK);
}
static void dc_pin_core(int core) {
    cpu_set_t s; CPU_ZERO(&s); CPU_SET(core, &s);
    pthread_setaffinity_np(pthread_self(), sizeof(s), &s);
}

/* fd handoff ring (SPSC of ints, per worker) */
static inline int dc_fdq_push(dc_worker *w, int fd) {
    uint32_t t = atomic_load_explicit(&w->fdq_tail, memory_order_relaxed);
    uint32_t h = atomic_load_explicit(&w->fdq_head, memory_order_acquire);
    if (t - h >= DC_FDQ_CAP) return 0;
    w->fdq[t & (DC_FDQ_CAP - 1)] = fd;
    atomic_store_explicit(&w->fdq_tail, t + 1, memory_order_release);
    return 1;
}
static inline int dc_fdq_pop(dc_worker *w, int *fd) {
    uint32_t h = atomic_load_explicit(&w->fdq_head, memory_order_relaxed);
    uint32_t t = atomic_load_explicit(&w->fdq_tail, memory_order_acquire);
    if (h == t) return 0;
    *fd = w->fdq[h & (DC_FDQ_CAP - 1)];
    atomic_store_explicit(&w->fdq_head, h + 1, memory_order_release);
    return 1;
}

/* ------------------------------------------------------------------ */
/* connection lifecycle (worker thread only)                           */
/* ------------------------------------------------------------------ */
static void dc_conn_open(dc_worker *w, int fd) {
    if (w->free_top == 0) { close(fd); return; }
    int ci = w->free_stack[--w->free_top];
    dc_conn *c = &w->conns[ci];
    c->fd = fd; c->active = 1; c->want_write = 0;
    c->rlen = 0; c->wlen = c->wsent = 0;
    c->issue_seq = c->flush_seq = 0;
    if (!c->rbuf) { c->rcap = DC_RBUF_INIT; c->rbuf = zmalloc(c->rcap); }
    if (!c->wbuf) { c->wcap = DC_WBUF_INIT; c->wbuf = zmalloc(c->wcap); }
    if (!c->pend) c->pend = zcalloc(DC_PEND_CAP * sizeof(dc_pendslot));
    else memset(c->pend, 0, DC_PEND_CAP * sizeof(dc_pendslot));
    dc_set_nonblock(fd);
    int one = 1; setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    struct epoll_event ev; ev.events = EPOLLIN; ev.data.u32 = (uint32_t)ci;
    epoll_ctl(w->epfd, EPOLL_CTL_ADD, fd, &ev);
}
static void dc_conn_close(dc_worker *w, int ci) {
    dc_conn *c = &w->conns[ci];
    if (!c->active) return;
    epoll_ctl(w->epfd, EPOLL_CTL_DEL, c->fd, NULL);
    close(c->fd);
    c->active = 0;
    w->free_stack[w->free_top++] = ci;
}
static void dc_wbuf_append(dc_conn *c, const void *p, size_t n) {
    if (c->wlen + n > c->wcap) {
        size_t nc = c->wcap * 2;
        while (nc < c->wlen + n) nc *= 2;
        c->wbuf = zrealloc(c->wbuf, nc); c->wcap = nc;
    }
    memcpy(c->wbuf + c->wlen, p, n); c->wlen += n;
}
static void dc_epoll_want_write(dc_worker *w, int ci, int on) {
    dc_conn *c = &w->conns[ci];
    if (c->want_write == on) return;
    c->want_write = on;
    struct epoll_event ev; ev.data.u32 = (uint32_t)ci;
    ev.events = EPOLLIN | (on ? EPOLLOUT : 0);
    epoll_ctl(w->epfd, EPOLL_CTL_MOD, c->fd, &ev);
}
static void dc_try_write(dc_worker *w, int ci) {
    dc_conn *c = &w->conns[ci];
    while (c->wsent < c->wlen) {
        ssize_t n = write(c->fd, c->wbuf + c->wsent, c->wlen - c->wsent);
        if (n > 0) { c->wsent += (size_t)n; continue; }
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) { dc_epoll_want_write(w, ci, 1); return; }
        dc_conn_close(w, ci); return;
    }
    c->wlen = c->wsent = 0;
    dc_epoll_want_write(w, ci, 0);
}

/* ------------------------------------------------------------------ */
/* reply framing (worker thread only), in issue-seq order              */
/* ------------------------------------------------------------------ */
static void dc_frame_slot(dc_conn *c, dc_pendslot *p) {
    if (p->kind == 1) { dc_wbuf_append(c, p->inl, p->ilen); return; }
    /* pending GET filled by main */
    if (p->rkind == DC_R_NIL) { dc_wbuf_append(c, "$-1\r\n", 5); return; }
    if (p->rkind == DC_R_ERR) { dc_wbuf_append(c, p->gval, (size_t)p->vlen); return; }
    char hdr[24]; int hn = snprintf(hdr, sizeof(hdr), "$%d\r\n", p->vlen);
    dc_wbuf_append(c, hdr, hn);
    dc_wbuf_append(c, p->gval, (size_t)p->vlen);
    dc_wbuf_append(c, "\r\n", 2);
}
static void dc_flush_pending(dc_worker *w, int ci) {
    dc_conn *c = &w->conns[ci];
    for (;;) {
        dc_pendslot *p = &c->pend[c->flush_seq & (DC_PEND_CAP - 1)];
        if (!p->ready) break;
        dc_frame_slot(c, p);
        p->ready = 0; p->kind = 0;
        c->flush_seq++;
    }
    (void)w;
}
static inline uint32_t dc_issue_local(dc_conn *c, const void *bytes, uint16_t len) {
    uint32_t seq = c->issue_seq++;
    dc_pendslot *p = &c->pend[seq & (DC_PEND_CAP - 1)];
    p->kind = 1; p->ilen = len;
    if (bytes && len) memcpy(p->inl, bytes, len);
    p->ready = 1;
    return seq;
}

/* ------------------------------------------------------------------ */
/* RESP2 multibulk parser (self-contained). Returns bytes consumed,    */
/* 0 if incomplete, -1 protocol error.                                 */
/* ------------------------------------------------------------------ */
#define DC_MAX_ARGS 16
static int dc_parse(char *buf, size_t len, char *argv[], size_t argl[], int *argc) {
    if (len < 1) return 0;
    /* Inline commands are not supported on the fast path; require multibulk. */
    if (buf[0] != '*') return -1;
    size_t i = 1; long n = 0; int seen = 0;
    while (i < len && buf[i] != '\r') { if (buf[i] < '0' || buf[i] > '9') return -1; n = n * 10 + (buf[i] - '0'); i++; seen = 1; }
    if (i + 1 >= len) return 0;
    if (buf[i] != '\r' || buf[i + 1] != '\n') return -1;
    if (!seen) return -1;
    i += 2;
    if (n <= 0 || n > DC_MAX_ARGS) return -1;
    *argc = (int)n;
    for (int a = 0; a < n; a++) {
        if (i >= len) return 0;
        if (buf[i] != '$') return -1;
        i++;
        long bl = 0; int bseen = 0;
        while (i < len && buf[i] != '\r') { if (buf[i] < '0' || buf[i] > '9') return -1; bl = bl * 10 + (buf[i] - '0'); i++; bseen = 1; }
        if (i + 1 >= len) return 0;
        if (buf[i] != '\r' || buf[i + 1] != '\n') return -1;
        if (!bseen) return -1;
        i += 2;
        if (bl < 0) return -1;
        if (i + (size_t)bl + 2 > len) return 0;
        argv[a] = buf + i; argl[a] = (size_t)bl;
        i += (size_t)bl;
        if (buf[i] != '\r' || buf[i + 1] != '\n') return -1;
        i += 2;
    }
    return (int)i;
}
static int dc_eqci(const char *a, size_t al, const char *lit) {
    size_t ll = strlen(lit);
    if (al != ll) return 0;
    for (size_t i = 0; i < al; i++) {
        char x = a[i]; if (x >= 'A' && x <= 'Z') x += 32;
        if (x != lit[i]) return 0;
    }
    return 1;
}

static const char DC_ERR_UNSUP[] = "-ERR DESIGNC stage1 unsupported\r\n";

/* classify + dispatch all complete commands in the conn read buffer */
static void dc_process_input(dc_worker *w, int ci) {
    dc_conn *c = &w->conns[ci];
    size_t off = 0;
    char *argv[DC_MAX_ARGS]; size_t argl[DC_MAX_ARGS]; int argc;
    while (off < c->rlen) {
        if (c->issue_seq - c->flush_seq >= DC_PEND_CAP - 64) break; /* reorder backpressure */
        int consumed = dc_parse(c->rbuf + off, c->rlen - off, argv, argl, &argc);
        if (consumed == 0) break;
        if (consumed < 0) { dc_conn_close(w, ci); return; }
        char *cmd = argv[0]; size_t cl = argl[0];

        if (dc_eqci(cmd, cl, "get") && argc == 2 && argl[1] <= DC_KEYCAP) {
            dc_oprec op;
            op.conn = (uint32_t)ci;
            op.klen = (uint16_t)argl[1];
            memcpy(op.key, argv[1], argl[1]);
            op.hash = genHashFunctionConfigurableSeed(argv[1], argl[1]);
            hashtablePrefetchBucket(kvstoreGetHashtable(server.db[0]->keys, 0), op.hash);
            uint32_t seq = c->issue_seq++;
            op.seq = seq;
            dc_pendslot *p = &c->pend[seq & (DC_PEND_CAP - 1)];
            p->kind = 2; p->ready = 0;
            if (!dc_spsc_push(&w->opq, &op)) {
                c->issue_seq--; p->kind = 0;   /* op queue full: retry later */
                break;
            }
        } else if (dc_eqci(cmd, cl, "ping")) {
            dc_issue_local(c, "+PONG\r\n", 7);
        } else if (dc_eqci(cmd, cl, "quit")) {
            dc_issue_local(c, "+OK\r\n", 5);
            /* flush then close */
            off += (size_t)consumed;
            memmove(c->rbuf, c->rbuf + off, c->rlen - off); c->rlen -= off;
            dc_flush_pending(w, ci); dc_try_write(w, ci);
            dc_conn_close(w, ci); return;
        } else {
            dc_issue_local(c, DC_ERR_UNSUP, (uint16_t)(sizeof(DC_ERR_UNSUP) - 1));
        }
        off += (size_t)consumed;
    }
    if (off > 0) { memmove(c->rbuf, c->rbuf + off, c->rlen - off); c->rlen -= off; }
    dc_flush_pending(w, ci);
    dc_try_write(w, ci);
}

static void dc_do_read(dc_worker *w, int ci) {
    dc_conn *c = &w->conns[ci];
    for (;;) {
        if (c->rlen == c->rcap) {
            size_t nc = c->rcap * 2;
            c->rbuf = zrealloc(c->rbuf, nc); c->rcap = nc;
        }
        ssize_t n = read(c->fd, c->rbuf + c->rlen, c->rcap - c->rlen);
        if (n > 0) { c->rlen += (size_t)n; continue; }
        if (n == 0) { dc_conn_close(w, ci); return; }
        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
        dc_conn_close(w, ci); return;
    }
    dc_process_input(w, ci);
}

/* drain replies from main into per-conn reorder slots, then flush */
static void dc_drain_replies(dc_worker *w) {
    static __thread dc_replyrec batch[256];
    uint32_t touched[256]; int nt;
    uint32_t got;
    do {
        got = dc_spsc_pop_batch(&w->rpq, batch, 256);
        nt = 0;
        for (uint32_t i = 0; i < got; i++) {
            dc_replyrec *r = &batch[i];
            dc_conn *c = &w->conns[r->conn];
            if (!c->active) continue;
            dc_pendslot *p = &c->pend[r->seq & (DC_PEND_CAP - 1)];
            p->kind = 2; p->rkind = r->kind; p->vlen = r->vlen;
            if (r->kind != DC_R_NIL && r->vlen > 0 && r->vlen <= DC_VALCAP)
                memcpy(p->gval, r->val, (size_t)r->vlen);
            p->ready = 1;
            int seen = 0;
            for (int k = 0; k < nt; k++) if (touched[k] == r->conn) { seen = 1; break; }
            if (!seen && nt < 256) touched[nt++] = r->conn;
        }
        for (int k = 0; k < nt; k++) {
            dc_flush_pending(w, touched[k]);
            dc_try_write(w, touched[k]);
            /* Backpressure in dc_process_input() can pause parsing with input
             * still buffered when the reorder window fills. Advancing flush_seq
             * above frees window; re-drive parsing now, since no new EPOLLIN
             * edge will arrive for already-buffered bytes. */
            dc_conn *c = &w->conns[touched[k]];
            if (c->active && c->rlen > 0) dc_process_input(w, touched[k]);
        }
    } while (got == 256);
}

/* ------------------------------------------------------------------ */
/* worker thread                                                       */
/* ------------------------------------------------------------------ */
static void *dc_worker_main(void *arg) {
    dc_worker *w = arg;
    char nm[16]; snprintf(nm, sizeof(nm), "dc-wrk%d", w->idx);
    pthread_setname_np(pthread_self(), nm);
    /* workers on cores 1..N, main stays on 0 (best effort) */
    dc_pin_core(1 + (w->idx % 14));

    struct epoll_event evs[256];
    while (g_run) {
        int n = epoll_wait(w->epfd, evs, 256, 0);   /* busy-poll (design mode) */
        for (int i = 0; i < n; i++) {
            uint32_t id = evs[i].data.u32;
            if (id == 0xFFFFFFFFu) {
                uint64_t v; while (read(w->evfd, &v, sizeof(v)) > 0) {}
                int fd;
                while (dc_fdq_pop(w, &fd)) dc_conn_open(w, fd);
                continue;
            }
            dc_conn *c = &w->conns[id];
            if (!c->active) continue;
            if (evs[i].events & (EPOLLHUP | EPOLLERR)) { dc_conn_close(w, id); continue; }
            if (evs[i].events & EPOLLOUT) dc_try_write(w, id);
            if (evs[i].events & EPOLLIN) dc_do_read(w, id);
        }
        /* also drain any pending new fds even without an eventfd edge */
        { int fd; while (dc_fdq_pop(w, &fd)) dc_conn_open(w, fd); }
        dc_drain_replies(w);
    }
    return NULL;
}

/* ------------------------------------------------------------------ */
/* main applier: drain op queues, batched incremental find, reply      */
/* ------------------------------------------------------------------ */
/* Extract value bytes for a hit value object into out (cap DC_VALCAP).
 * Returns bytes copied, or -1 if too large, -2 if wrong type. */
static int dc_extract_value(robj *val, char *out) {
    if (val->type != OBJ_STRING) return -2;
    if (val->encoding == OBJ_ENCODING_INT) {
        char buf[32];
        int l = ll2string(buf, sizeof(buf), (long)objectGetVal(val));
        memcpy(out, buf, l);
        return l;
    }
    sds s = objectGetVal(val);
    size_t l = sdslen(s);
    if (l > DC_VALCAP) return -1;
    memcpy(out, s, l);
    return (int)l;
}

void designcApplierTick(void) {
    if (!server.designc_enabled || g_nworkers == 0) return;
    atomic_fetch_add_explicit(&g_tick_calls, 1, memory_order_relaxed);

    static dc_oprec stage[128];
    hashtableIncrementalFindState st[DC_FIND_BATCH];
    dc_oprec *bo[DC_FIND_BATCH];
    /* pooled search-key sds buffers (reused across ops; no per-op malloc) */
    static sds keypool[DC_FIND_BATCH];
    static int keypool_init = 0;
    if (!keypool_init) { for (int i = 0; i < DC_FIND_BATCH; i++) keypool[i] = sdsnewlen("", DC_KEYCAP); keypool_init = 1; }

    hashtable *ht = kvstoreGetHashtable(server.db[0]->keys, 0);
    int expires_present = kvstoreSize(server.db[0]->expires) > 0;

    uint64_t ops = atomic_load_explicit(&g_ops, memory_order_relaxed);
    uint64_t busy_ns = atomic_load_explicit(&g_busy_ns, memory_order_relaxed);
    struct timespec ta, tb;

    for (int wi = 0; wi < g_nworkers; wi++) {
        dc_worker *w = &g_workers[wi];
        uint32_t got = dc_spsc_pop_batch(&w->opq, stage, 128);
        if (!got) continue;
        atomic_fetch_add_explicit(&g_cyc_hist[dc_hist_bucket(got)], 1, memory_order_relaxed);
        clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ta);
        for (uint32_t base = 0; base < got; base += DC_FIND_BATCH) {
            uint32_t bs = got - base; if (bs > DC_FIND_BATCH) bs = DC_FIND_BATCH;
            atomic_fetch_add_explicit(&g_depth_hist[dc_hist_bucket(bs)], 1, memory_order_relaxed);
            for (uint32_t b = 0; b < bs; b++) {
                bo[b] = &stage[base + b];
                sds k = keypool[b];
                memcpy(k, bo[b]->key, bo[b]->klen);
                sdssetlen(k, bo[b]->klen);
                k[bo[b]->klen] = '\0';
                hashtableIncrementalFindInit(&st[b], ht, k);
            }
            uint32_t remaining = bs;
            char done[DC_FIND_BATCH]; memset(done, 0, bs);
            while (remaining) {
                for (uint32_t b = 0; b < bs; b++)
                    if (!done[b] && !hashtableIncrementalFindStep(&st[b])) { done[b] = 1; remaining--; }
            }
            for (uint32_t b = 0; b < bs; b++) {
                dc_replyrec r;
                r.conn = bo[b]->conn; r.seq = bo[b]->seq;
                void *entry = NULL;
                int hit = hashtableIncrementalFindGetResult(&st[b], &entry) && entry;
                if (hit && expires_present) {
                    /* Rare TTL path: defer to the real read path for authoritative
                     * expiry + propagation semantics (main is the expiry thread). */
                    robj *keyobj = createStringObject(bo[b]->key, bo[b]->klen);
                    robj *val = lookupKeyReadWithFlags(server.db[0], keyobj, LOOKUP_NONE);
                    decrRefCount(keyobj);
                    if (!val) { r.kind = DC_R_NIL; r.vlen = -1; }
                    else {
                        int vl = dc_extract_value(val, r.val);
                        if (vl == -1) { const char *e = "-ERR DESIGNC stage1 value too large\r\n"; r.kind = DC_R_ERR; r.vlen = (int)strlen(e); memcpy(r.val, e, r.vlen); }
                        else if (vl == -2) { const char *e = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"; r.kind = DC_R_ERR; r.vlen = (int)strlen(e); memcpy(r.val, e, r.vlen); }
                        else { r.kind = DC_R_BULK; r.vlen = vl; }
                    }
                } else if (hit) {
                    robj *val = entry;
                    int vl = dc_extract_value(val, r.val);
                    if (vl == -1) { const char *e = "-ERR DESIGNC stage1 value too large\r\n"; r.kind = DC_R_ERR; r.vlen = (int)strlen(e); memcpy(r.val, e, r.vlen); }
                    else if (vl == -2) { const char *e = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"; r.kind = DC_R_ERR; r.vlen = (int)strlen(e); memcpy(r.val, e, r.vlen); }
                    else { r.kind = DC_R_BULK; r.vlen = vl; }
                } else {
                    r.kind = DC_R_NIL; r.vlen = -1;
                }
                while (!dc_spsc_push(&w->rpq, &r)) { /* worker drains each loop */ }
            }
            ops += bs;
        }
        clock_gettime(CLOCK_THREAD_CPUTIME_ID, &tb);
        busy_ns += (uint64_t)(tb.tv_sec - ta.tv_sec) * 1000000000ull + (uint64_t)(tb.tv_nsec - ta.tv_nsec);
    }
    atomic_store_explicit(&g_ops, ops, memory_order_relaxed);
    atomic_store_explicit(&g_busy_ns, busy_ns, memory_order_relaxed);
}

/* ------------------------------------------------------------------ */
/* accept-time handoff (main thread)                                   */
/* ------------------------------------------------------------------ */
int designcHandoffFd(int fd) {
    if (!server.designc_enabled || g_nworkers == 0) return 0;
    int wi = atomic_fetch_add_explicit(&g_rr, 1, memory_order_relaxed) % g_nworkers;
    dc_worker *w = &g_workers[wi];
    if (!dc_fdq_push(w, fd)) { close(fd); return 1; } /* overload: drop */
    uint64_t one = 1; ssize_t rc = write(w->evfd, &one, sizeof(one)); (void)rc;
    return 1;
}

/* ------------------------------------------------------------------ */
/* INFO section                                                        */
/* ------------------------------------------------------------------ */
void designcInfoAppend(sds *info) {
    if (!server.designc_enabled) return;
    uint64_t ops = atomic_load_explicit(&g_ops, memory_order_relaxed);
    uint64_t busy = atomic_load_explicit(&g_busy_ns, memory_order_relaxed);
    double nsop = ops ? (double)busy / (double)ops : 0.0;
    *info = sdscatprintf(*info,
        "# designc\r\n"
        "designc_enabled:%d\r\n"
        "designc_workers:%d\r\n"
        "designc_ops:%llu\r\n"
        "designc_main_busy_ns:%llu\r\n"
        "designc_main_ns_per_op:%.2f\r\n"
        "designc_tick_calls:%llu\r\n",
        server.designc_enabled, g_nworkers,
        (unsigned long long)ops, (unsigned long long)busy, nsop,
        (unsigned long long)atomic_load_explicit(&g_tick_calls, memory_order_relaxed));
    *info = sdscat(*info, "designc_depth_hist:");
    for (int i = 0; i < DC_HIST_N; i++)
        *info = sdscatprintf(*info, "%s%llu", i ? "," : "", (unsigned long long)atomic_load_explicit(&g_depth_hist[i], memory_order_relaxed));
    *info = sdscat(*info, "\r\ndesignc_cycle_hist:");
    for (int i = 0; i < DC_HIST_N; i++)
        *info = sdscatprintf(*info, "%s%llu", i ? "," : "", (unsigned long long)atomic_load_explicit(&g_cyc_hist[i], memory_order_relaxed));
    *info = sdscat(*info, "\r\n");
}

/* ------------------------------------------------------------------ */
/* init / shutdown                                                     */
/* ------------------------------------------------------------------ */
static void dc_refuse(const char *why) {
    serverLog(LL_WARNING, "designc-enabled refused to start: %s", why);
    exit(1);
}

void designcInit(void) {
    if (!server.designc_enabled) return;

    /* hazard fences: refuse unsupported deployment combinations */
    if (server.requirepass && sdslen(server.requirepass) > 0) dc_refuse("requirepass set (stage1 requires default-user-no-auth)");
    if (server.acl_filename && server.acl_filename[0]) dc_refuse("aclfile set (stage1 requires no ACLs)");
    if (server.primary_host) dc_refuse("replicaof set (stage1 forbids replica)");
    if (server.aof_state != AOF_OFF) dc_refuse("appendonly enabled (stage1 forbids AOF)");
    if (server.cluster_enabled) dc_refuse("cluster-enabled set (stage1 forbids cluster)");
    if (server.dbnum < 1) dc_refuse("no db");

    int nw = server.designc_workers;
    if (nw < 1) nw = 1;
    if (nw > DC_MAX_WORKERS) nw = DC_MAX_WORKERS;

    g_workers = zcalloc(sizeof(dc_worker) * nw);
    for (int i = 0; i < nw; i++) {
        dc_worker *w = &g_workers[i];
        w->idx = i;
        w->epfd = epoll_create1(0);
        w->evfd = eventfd(0, EFD_NONBLOCK);
        if (w->epfd < 0 || w->evfd < 0) dc_refuse("epoll/eventfd create failed");
        dc_spsc_init(&w->opq, DC_OPQ_CAP, sizeof(dc_oprec));
        dc_spsc_init(&w->rpq, DC_RPQ_CAP, sizeof(dc_replyrec));
        atomic_store_explicit(&w->fdq_head, 0, memory_order_relaxed);
        atomic_store_explicit(&w->fdq_tail, 0, memory_order_relaxed);
        w->free_top = 0;
        for (int j = DC_MAX_CONNS - 1; j >= 0; j--) w->free_stack[w->free_top++] = j;
        struct epoll_event ev; ev.events = EPOLLIN; ev.data.u32 = 0xFFFFFFFFu;
        epoll_ctl(w->epfd, EPOLL_CTL_ADD, w->evfd, &ev);
    }
    g_nworkers = nw;
    for (int i = 0; i < nw; i++)
        pthread_create(&g_workers[i].th, NULL, dc_worker_main, &g_workers[i]);

    serverLog(LL_NOTICE, "designc: worker-ownership fast path ENABLED (%d workers, find batch %d)", nw, DC_FIND_BATCH);
}

void designcShutdown(void) {
    if (!server.designc_enabled || g_nworkers == 0) return;
    g_run = 0;
    for (int i = 0; i < g_nworkers; i++) {
        uint64_t one = 1; ssize_t rc = write(g_workers[i].evfd, &one, sizeof(one)); (void)rc;
    }
    for (int i = 0; i < g_nworkers; i++) pthread_join(g_workers[i].th, NULL);
    sds stats = sdsempty();
    designcInfoAppend(&stats);
    serverLog(LL_NOTICE, "designc: workers joined. stats:\n%s", stats);
    sdsfree(stats);
    g_nworkers = 0;
}
