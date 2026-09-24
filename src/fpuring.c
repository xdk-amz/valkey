/* Fast-path io_uring send path (prototype): per-IO-thread ring, send-side only.
 * Reads stay on epoll; fpSend queues IORING_OP_SEND SQEs, submitted once per loop.
 * A no-op stub when built without liburing. */
#include "fpuring.h"

#ifndef HAVE_LIBURING

int fpUringWanted(void) { return 0; }
struct fpUringRing *fpUringInit(int tid) { UNUSED(tid); return NULL; }
int fpUringFd(struct fpUringRing *r) { UNUSED(r); return -1; }
void fpUringFree(struct fpUringRing *r) { UNUSED(r); }
int fpUringQueueSend(struct fpUringRing *r, client *c, int fd, const void *buf, size_t len) {
    UNUSED(r); UNUSED(c); UNUSED(fd); UNUSED(buf); UNUSED(len); return C_ERR;
}
void fpUringSubmit(struct fpUringRing *r) { UNUSED(r); }
int fpUringReap(struct fpUringRing *r) { UNUSED(r); return 0; }
int fpUringClientInflight(struct fpUringRing *r, client *c) { UNUSED(r); UNUSED(c); return 0; }
sds fpUringInfo(sds info) { return info; }

#else

#include <liburing.h>
#include <string.h>

#define FPU_RING_ENTRIES 4096   /* SQ/CQ depth per IO thread */
#define FPU_SEND_MAX 65536      /* one queued send copies at most this many bytes */

/* One in-flight send. The bytes are copied here so the client's buffer can be reused
 * immediately; owner is the control pointer + generation the send was queued for, so a
 * completion can be matched to a still-live client without touching freed connection storage. */
typedef struct fpuSend {
    struct fpuSend *free_next; /* freelist link when idle */
    void *control;             /* ClientControl * of the owning client (match key) */
    uint32_t generation;       /* control generation at queue time */
    int fd;                    /* socket fd (kept so a short send can resubmit) */
    uint32_t len;              /* bytes still to send from off */
    uint32_t off;              /* bytes already sent (short-send resume point) */
    uint32_t cap;              /* allocated capacity of buf */
    char *buf;                 /* copied reply bytes */
} fpuSend;

struct fpUringRing {
    struct io_uring ring;
    int ring_fd;
    int inited;
    fpuSend *freelist;
    int inflight;              /* SQEs submitted, not yet completed */
    int pending;              /* SQEs prepared, not yet submitted */
    long long n_queued, n_sqe, n_bytes, n_reaped, n_submit_calls; /* diagnostics, INFO fastpath_uring_*
      n_queued = reply buffers accepted (one per reply); n_sqe = SEND SQEs prepared (queued + resubmits);
      n_bytes = reply payload bytes accepted; n_reaped = CQEs; n_submit_calls = io_uring_submit calls. */
};

/* Per-thread ring diagnostics summed for INFO; only the owning thread writes its slot. */
static struct fpUringRing *fpu_rings[64];

/* liburing user_data carries the fpuSend pointer directly. */

int fpUringWanted(void) {
    return server.io_threads_uring ? 1 : 0;
}

struct fpUringRing *fpUringInit(int tid) {
    struct fpUringRing *r = zcalloc(sizeof(*r));
    struct io_uring_params p;
    memset(&p, 0, sizeof(p));
    /* Plain setup for the send-side step; SINGLE_ISSUER/DEFER_TASKRUN are a later data point. */
    if (io_uring_queue_init_params(FPU_RING_ENTRIES, &r->ring, &p) < 0) {
        zfree(r);
        return NULL;
    }
    r->ring_fd = r->ring.ring_fd;
    r->inited = 1;
    if (tid >= 0 && tid < 64) fpu_rings[tid] = r;
    return r;
}

int fpUringFd(struct fpUringRing *r) { return r ? r->ring_fd : -1; }

void fpUringFree(struct fpUringRing *r) {
    if (!r) return;
    if (r->inited) io_uring_queue_exit(&r->ring);
    fpuSend *s = r->freelist;
    while (s) { fpuSend *n = s->free_next; zfree(s->buf); zfree(s); s = n; }
    zfree(r);
}

static fpuSend *fpuAlloc(struct fpUringRing *r, size_t len) {
    fpuSend *s = r->freelist;
    if (s) {
        r->freelist = s->free_next;
    } else {
        s = zcalloc(sizeof(*s));
    }
    if (s->cap < len) {
        zfree(s->buf);
        s->buf = zmalloc(len);
        s->cap = len;
    }
    return s;
}

static void fpuRecycle(struct fpUringRing *r, fpuSend *s) {
    s->control = NULL;
    s->free_next = r->freelist;
    r->freelist = s;
}

/* Prepare one IORING_OP_SEND for s at its current off/len. */
static int fpuPrepSend(struct fpUringRing *r, fpuSend *s) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(&r->ring);
    if (!sqe) return C_ERR; /* SQ full; caller falls back */
    io_uring_prep_send(sqe, s->fd, s->buf + s->off, s->len, MSG_NOSIGNAL);
    io_uring_sqe_set_data(sqe, s);
    r->pending++;
    r->n_sqe++;
    return C_OK;
}

int fpUringQueueSend(struct fpUringRing *r, client *c, int fd, const void *buf, size_t len) {
    if (!r || !r->inited || len == 0 || len > FPU_SEND_MAX) return C_ERR;
    fpuSend *s = fpuAlloc(r, len);
    memcpy(s->buf, buf, len);
    s->control = c->control; /* opaque match key only; never dereferenced here */
    s->generation = 0;
    s->fd = fd;
    s->len = (uint32_t)len;
    s->off = 0;
    if (fpuPrepSend(r, s) != C_OK) {
        fpuRecycle(r, s);
        return C_ERR;
    }
    r->inflight++;
    r->n_queued++;
    r->n_bytes += (long long)len;
    return C_OK;
}

void fpUringSubmit(struct fpUringRing *r) {
    if (!r || !r->inited || r->pending == 0) return;
    io_uring_submit(&r->ring);
    r->n_submit_calls++;
    r->pending = 0;
}

int fpUringReap(struct fpUringRing *r) {
    if (!r || !r->inited) return 0;
    struct io_uring_cqe *cqe;
    unsigned head;
    int n = 0;
    io_uring_for_each_cqe(&r->ring, head, cqe) {
        fpuSend *s = io_uring_cqe_get_data(cqe);
        int res = cqe->res;
        n++;
        if (!s) continue;
        r->inflight--;
        if (res > 0 && (uint32_t)res < s->len) {
            /* Short send: resubmit the remainder, preserving per-client order. */
            s->off += (uint32_t)res;
            s->len -= (uint32_t)res;
            if (fpuPrepSend(r, s) == C_OK) {
                r->inflight++;
                continue; /* still owns s */
            }
        }
        /* res <= 0 is a dead peer; the epoll recv path observes EOF and leaves the client. */
        fpuRecycle(r, s);
    }
    io_uring_cq_advance(&r->ring, n);
    r->n_reaped += n;
    if (r->pending) fpUringSubmit(r); /* flush any resubmitted remainders */
    return n;
}

int fpUringClientInflight(struct fpUringRing *r, client *c) {
    if (!r || !r->inited) return 0;
    UNUSED(c);
    /* Ring-wide inflight is the hand-back gate: the caller reaps to 0 before handing any client
     * back, so no completion can still reference a client's copied send buffer. */
    return r->inflight;
}

/* Sum the per-thread ring diagnostics into an INFO fastpath_uring_* block. */
sds fpUringInfo(sds info) {
    long long q = 0, sqe = 0, bytes = 0, reap = 0, calls = 0;
    for (int i = 0; i < 64; i++) {
        struct fpUringRing *r = fpu_rings[i];
        if (!r) continue;
        q += r->n_queued; sqe += r->n_sqe; bytes += r->n_bytes; reap += r->n_reaped; calls += r->n_submit_calls;
    }
    return sdscatprintf(info,
        "fastpath_uring_replies:%lld\r\n"
        "fastpath_uring_sqe:%lld\r\n"
        "fastpath_uring_bytes:%lld\r\n"
        "fastpath_uring_reaped:%lld\r\n"
        "fastpath_uring_submit_calls:%lld\r\n",
        q, sqe, bytes, reap, calls);
}

#endif /* HAVE_LIBURING */
