/* Fast-path io_uring path: recv-driven, per-IO-thread ring.
 *
 * When io-threads-uring is on the ring is the IO thread loop's blocking point: one
 * io_uring_submit_and_wait_timeout per pass submits queued SQEs and waits (bounded by the tick)
 * for at least one completion, then every ready CQE is reaped in that pass. There is no per-pass
 * poll/reap spin. Fast-path client sockets are driven entirely by the ring: a multishot
 * IORING_OP_RECV per client draws bytes into a per-thread provided buffer ring, and each recv
 * CQE feeds the existing parse path; replies leave via queued IORING_OP_SEND. The IO thread's
 * epoll fd is folded in with a multishot IORING_OP_POLL_ADD, so non-fastpath clients and control
 * fds still wake the loop and are serviced by the existing zero-timeout epoll drain.
 *
 * A no-op stub compiles without liburing, and when the ring is off the epoll path is unchanged. */
#include "fpuring.h"

#ifndef HAVE_LIBURING

int fpUringWanted(void) { return 0; }
struct fpUringRing *fpUringInit(int tid) { UNUSED(tid); return NULL; }
int fpUringFd(struct fpUringRing *r) { UNUSED(r); return -1; }
void fpUringFree(struct fpUringRing *r) { UNUSED(r); }
void fpUringFoldEpoll(struct fpUringRing *r, int epfd) { UNUSED(r); UNUSED(epfd); }
int fpUringClientAdd(struct fpUringRing *r, client *c, int fd) { UNUSED(r); UNUSED(c); UNUSED(fd); return C_ERR; }
void fpUringClientCancel(struct fpUringRing *r, client *c, int fd) { UNUSED(r); UNUSED(c); UNUSED(fd); }
int fpUringQueueSend(struct fpUringRing *r, client *c, int fd, const void *buf, size_t len) {
    UNUSED(r); UNUSED(c); UNUSED(fd); UNUSED(buf); UNUSED(len); return C_ERR;
}
int fpUringWaitAndReap(struct fpUringRing *r, int timeout_us, fpUringEvents *ev) {
    UNUSED(r); UNUSED(timeout_us); if (ev) memset(ev, 0, sizeof(*ev)); return 0;
}
int fpUringReap(struct fpUringRing *r) { UNUSED(r); return 0; }
void fpUringSubmit(struct fpUringRing *r) { UNUSED(r); }
void fpUringSetRecvSink(struct fpUringRing *r, int tid, fpUringRecvFn fn) { UNUSED(r); UNUSED(tid); UNUSED(fn); }
int fpUringClientInflight(struct fpUringRing *r, client *c) { UNUSED(r); UNUSED(c); return 0; }
int fpUringInflight(struct fpUringRing *r) { UNUSED(r); return 0; }
sds fpUringInfo(sds info) { return info; }

#else

#include <liburing.h>
#include <string.h>
#include <poll.h>

#define FPU_RING_ENTRIES 4096    /* SQ/CQ depth per IO thread */
#define FPU_SEND_MAX 65536       /* one queued send copies at most this many bytes */
#define FPU_BUF_COUNT 2048       /* provided buffers in the recv ring (power of two) */
#define FPU_BUF_SIZE 16384       /* bytes per provided buffer = PROTO_IOBUF read size */
#define FPU_BGID 0               /* buffer-group id for the recv ring */

/* user_data tag lives in the low 2 bits; the high bits carry a pointer or fd. */
#define FPU_UD_SEND 0ULL   /* data = fpuSend * (aligned, low bits clear) */
#define FPU_UD_RECV 1ULL   /* data = (fd << 2) | tag; the owner resolves fd -> client */
#define FPU_UD_POLL 2ULL   /* data = folded epoll POLL_ADD */
#define FPU_UD_CANCEL 3ULL /* data = async cancel completion (ignored) */
#define FPU_UD_TAG(ud) ((ud) & 3ULL)

/* One in-flight send. Bytes are copied here so the client buffer can be reused at once; owner is
 * the control pointer the send was queued for, matched without touching freed connection storage. */
typedef struct fpuSend {
    struct fpuSend *free_next;
    void *control;
    int fd;
    uint32_t len;
    uint32_t off;
    uint32_t cap;
    char *buf;
} fpuSend;

/* Per-client recv registration: how many recv/send SQEs of this client are still in flight, so a
 * leaving client is handed back only when both reach zero. Keyed by fd in a small open table. */
typedef struct fpuClient {
    client *c;
    int fd;
    int recv_inflight; /* multishot recv armed (0 or 1) */
    int send_inflight; /* queued sends not yet completed */
    int cancelling;    /* cancel issued; do not re-arm recv */
} fpuClient;

struct fpUringRing {
    struct io_uring ring;
    int ring_fd;
    int inited;
    int epfd_folded;
    struct io_uring_buf_ring *buf_ring;
    void *buf_base;            /* FPU_BUF_COUNT * FPU_BUF_SIZE */
    fpuSend *freelist;
    int inflight;              /* send SQEs submitted, not yet completed */
    int pending;               /* SQEs prepared, not yet submitted */
    int tid;
    fpUringRecvFn recv_fn;
    /* fd -> client table; small linear map keyed by fd, sized to maxclients is overkill so grow. */
    fpuClient *clients;
    int clients_cap;
    long long n_recv, n_recv_bytes, n_send, n_send_bytes, n_reaped, n_enter, n_epoll_wakes, n_enobufs, n_short;
};

static struct fpUringRing *fpu_rings[64];

int fpUringWanted(void) { return server.io_threads_uring ? 1 : 0; }

static void fpuBufRingRecycle(struct fpUringRing *r, int bid);

/* Map the SETUP_* variant from server.io_threads_uring_flags:
 * 0 baseline, 1 SINGLE_ISSUER|DEFER_TASKRUN, 2 COOP_TASKRUN, 3 SQPOLL (idle 1000ms). */
static unsigned fpuSetupFlags(void) {
    switch (server.io_threads_uring_flags) {
    case 1: return IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN;
    case 2: return IORING_SETUP_COOP_TASKRUN;
    case 3: return IORING_SETUP_SQPOLL;
    default: return 0;
    }
}

struct fpUringRing *fpUringInit(int tid) {
    struct fpUringRing *r = zcalloc(sizeof(*r));
    struct io_uring_params p;
    memset(&p, 0, sizeof(p));
    p.flags = fpuSetupFlags();
    if (p.flags & IORING_SETUP_SQPOLL) {
        p.sq_thread_idle = 1000; /* ms; SQPOLL steals a core, noted in the report */
    }
    if (io_uring_queue_init_params(FPU_RING_ENTRIES, &r->ring, &p) < 0) {
        /* Retry once with baseline flags so an unsupported variant still yields a ring. */
        memset(&p, 0, sizeof(p));
        if (io_uring_queue_init_params(FPU_RING_ENTRIES, &r->ring, &p) < 0) { zfree(r); return NULL; }
    }
    r->ring_fd = r->ring.ring_fd;
    r->tid = tid;

    /* Provided buffer ring for multishot recv. */
    int ret = 0;
    r->buf_ring = io_uring_setup_buf_ring(&r->ring, FPU_BUF_COUNT, FPU_BGID, 0, &ret);
    if (!r->buf_ring) { io_uring_queue_exit(&r->ring); zfree(r); return NULL; }
    r->buf_base = zmalloc((size_t)FPU_BUF_COUNT * FPU_BUF_SIZE);
    unsigned mask = io_uring_buf_ring_mask(FPU_BUF_COUNT);
    for (int i = 0; i < FPU_BUF_COUNT; i++) {
        io_uring_buf_ring_add(r->buf_ring, (char *)r->buf_base + (size_t)i * FPU_BUF_SIZE, FPU_BUF_SIZE,
                              i, mask, i);
    }
    io_uring_buf_ring_advance(r->buf_ring, FPU_BUF_COUNT);

    r->clients_cap = 1024;
    r->clients = zcalloc((size_t)r->clients_cap * sizeof(fpuClient));
    r->inited = 1;
    if (tid >= 0 && tid < 64) fpu_rings[tid] = r;
    return r;
}

int fpUringFd(struct fpUringRing *r) { return r ? r->ring_fd : -1; }

void fpUringSetRecvSink(struct fpUringRing *r, int tid, fpUringRecvFn fn) {
    if (!r) return;
    r->recv_fn = fn;
    r->tid = tid;
}

void fpUringFree(struct fpUringRing *r) {
    if (!r) return;
    if (r->inited) io_uring_queue_exit(&r->ring);
    fpuSend *s = r->freelist;
    while (s) { fpuSend *n = s->free_next; zfree(s->buf); zfree(s); s = n; }
    zfree(r->buf_base);
    zfree(r->clients);
    zfree(r);
}

static fpuClient *fpuClientSlot(struct fpUringRing *r, int fd) {
    if (fd < 0) return NULL;
    if (fd >= r->clients_cap) {
        int cap = r->clients_cap;
        while (cap <= fd) cap *= 2;
        r->clients = zrealloc(r->clients, (size_t)cap * sizeof(fpuClient));
        memset(r->clients + r->clients_cap, 0, (size_t)(cap - r->clients_cap) * sizeof(fpuClient));
        r->clients_cap = cap;
    }
    return &r->clients[fd];
}

/* Arm (or re-arm) a multishot recv for fd, selecting a buffer from the provided ring. */
static int fpuArmRecv(struct fpUringRing *r, int fd) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(&r->ring);
    if (!sqe) return C_ERR;
    io_uring_prep_recv_multishot(sqe, fd, NULL, 0, 0);
    sqe->flags |= IOSQE_BUFFER_SELECT;
    sqe->buf_group = FPU_BGID;
    io_uring_sqe_set_data64(sqe, ((uint64_t)fd << 2) | FPU_UD_RECV);
    r->pending++;
    return C_OK;
}

int fpUringClientAdd(struct fpUringRing *r, client *c, int fd) {
    if (!r || !r->inited) return C_ERR;
    fpuClient *fc = fpuClientSlot(r, fd);
    if (!fc) return C_ERR;
    fc->c = c;
    fc->fd = fd;
    fc->cancelling = 0;
    fc->send_inflight = 0;
    if (fpuArmRecv(r, fd) != C_OK) { fc->c = NULL; return C_ERR; }
    fc->recv_inflight = 1;
    return C_OK;
}

void fpUringClientCancel(struct fpUringRing *r, client *c, int fd) {
    if (!r || !r->inited) return;
    fpuClient *fc = fpuClientSlot(r, fd);
    if (!fc || fc->c != c) return;
    fc->cancelling = 1;
    struct io_uring_sqe *sqe = io_uring_get_sqe(&r->ring);
    if (sqe) {
        io_uring_prep_cancel_fd(sqe, fd, IORING_ASYNC_CANCEL_ALL);
        io_uring_sqe_set_data64(sqe, FPU_UD_CANCEL);
        r->pending++;
    }
}

static fpuSend *fpuAlloc(struct fpUringRing *r, size_t len) {
    fpuSend *s = r->freelist;
    if (s) r->freelist = s->free_next;
    else s = zcalloc(sizeof(*s));
    if (s->cap < len) { zfree(s->buf); s->buf = zmalloc(len); s->cap = len; }
    return s;
}

static void fpuRecycle(struct fpUringRing *r, fpuSend *s) {
    s->control = NULL;
    s->free_next = r->freelist;
    r->freelist = s;
}

static int fpuPrepSend(struct fpUringRing *r, fpuSend *s) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(&r->ring);
    if (!sqe) return C_ERR;
    io_uring_prep_send(sqe, s->fd, s->buf + s->off, s->len, MSG_NOSIGNAL);
    io_uring_sqe_set_data(sqe, s); /* aligned pointer: low bits are FPU_UD_SEND (0) */
    r->pending++;
    r->n_send++;
    return C_OK;
}

int fpUringQueueSend(struct fpUringRing *r, client *c, int fd, const void *buf, size_t len) {
    if (!r || !r->inited || len == 0 || len > FPU_SEND_MAX) return C_ERR;
    fpuSend *s = fpuAlloc(r, len);
    memcpy(s->buf, buf, len);
    s->control = c->control;
    s->fd = fd;
    s->len = (uint32_t)len;
    s->off = 0;
    if (fpuPrepSend(r, s) != C_OK) { fpuRecycle(r, s); return C_ERR; }
    r->inflight++;
    r->n_send_bytes += (long long)len;
    fpuClient *fc = fpuClientSlot(r, fd);
    if (fc && fc->fd == fd) fc->send_inflight++;
    return C_OK;
}

void fpUringSubmit(struct fpUringRing *r) {
    if (!r || !r->inited || r->pending == 0) return;
    io_uring_submit(&r->ring);
    r->n_enter++;
    r->pending = 0;
}

static void fpuBufRingRecycle(struct fpUringRing *r, int bid) {
    unsigned mask = io_uring_buf_ring_mask(FPU_BUF_COUNT);
    io_uring_buf_ring_add(r->buf_ring, (char *)r->buf_base + (size_t)bid * FPU_BUF_SIZE, FPU_BUF_SIZE,
                          bid, mask, 0);
    io_uring_buf_ring_advance(r->buf_ring, 1);
}

/* Reap every ready CQE. Recv completions feed the recv sink; sends recycle or resubmit short
 * remainders; the folded epoll POLL_ADD sets ev->epoll_ready. Returns CQEs processed. */
static int fpuReapAll(struct fpUringRing *r, fpUringEvents *ev) {
    struct io_uring_cqe *cqe;
    unsigned head;
    int n = 0;
    io_uring_for_each_cqe(&r->ring, head, cqe) {
        n++;
        uint64_t ud = cqe->user_data;
        int tag = FPU_UD_TAG(ud);
        int res = cqe->res;
        if (tag == FPU_UD_SEND) {
            fpuSend *s = (fpuSend *)(uintptr_t)ud;
            if (!s) continue;
            r->inflight--;
            fpuClient *fc = fpuClientSlot(r, s->fd);
            if (fc && fc->fd == s->fd && fc->send_inflight > 0) fc->send_inflight--;
            if (res > 0 && (uint32_t)res < s->len) {
                s->off += (uint32_t)res;
                s->len -= (uint32_t)res;
                r->n_short++;
                if (fpuPrepSend(r, s) == C_OK) {
                    r->inflight++;
                    if (fc && fc->fd == s->fd) fc->send_inflight++;
                    continue;
                }
            }
            fpuRecycle(r, s);
            continue;
        }
        if (tag == FPU_UD_POLL) {
            if (ev) ev->epoll_ready = 1;
            r->n_epoll_wakes++;
            /* Multishot POLL_ADD stays armed unless F_MORE cleared; re-arm if it did. */
            if (!(cqe->flags & IORING_CQE_F_MORE) && r->epfd_folded >= 0) {
                struct io_uring_sqe *sqe = io_uring_get_sqe(&r->ring);
                if (sqe) {
                    io_uring_prep_poll_multishot(sqe, r->epfd_folded, POLLIN);
                    io_uring_sqe_set_data64(sqe, FPU_UD_POLL);
                    r->pending++;
                }
            }
            continue;
        }
        if (tag == FPU_UD_CANCEL) continue;
        if (tag == FPU_UD_RECV) {
            int fd = (int)(ud >> 2);
            fpuClient *fc = fpuClientSlot(r, fd);
            int has_buf = cqe->flags & IORING_CQE_F_BUFFER;
            int bid = has_buf ? (int)(cqe->flags >> IORING_CQE_BUFFER_SHIFT) : -1;
            if (res == -ENOBUFS) {
                r->n_enobufs++;
                /* No buffer was available; re-arm recv (buffers recycle as parses complete). */
                if (fc && fc->c && !fc->cancelling) fpuArmRecv(r, fd);
                else if (fc) fc->recv_inflight = 0;
                continue;
            }
            int eof = (res <= 0);
            if (res > 0 && bid >= 0 && fc && fc->c && r->recv_fn) {
                const char *b = (const char *)r->buf_base + (size_t)bid * FPU_BUF_SIZE;
                r->recv_fn(r->tid, fc->c, b, res, 0);
                r->n_recv++;
                r->n_recv_bytes += res;
                if (ev) ev->recvs++;
                if (ev) ev->reads_ready++;
            } else if (eof && fc && fc->c && r->recv_fn) {
                r->recv_fn(r->tid, fc->c, NULL, 0, 1); /* EOF/error: client leaves */
            }
            if (bid >= 0) fpuBufRingRecycle(r, bid);
            /* Re-arm on multishot termination (F_MORE cleared) unless leaving. */
            if (!(cqe->flags & IORING_CQE_F_MORE)) {
                if (fc && fc->c && !fc->cancelling && !eof) fpuArmRecv(r, fd);
                else if (fc) fc->recv_inflight = 0;
            }
            continue;
        }
    }
    io_uring_cq_advance(&r->ring, n);
    r->n_reaped += n;
    return n;
}

int fpUringWaitAndReap(struct fpUringRing *r, int timeout_us, fpUringEvents *ev) {
    if (ev) memset(ev, 0, sizeof(*ev));
    if (!r || !r->inited) return 0;
    struct __kernel_timespec ts = {.tv_sec = timeout_us / 1000000, .tv_nsec = (long)(timeout_us % 1000000) * 1000};
    /* Single enter: submit queued SQEs and wait up to the tick for one completion. cqe_ptr must be
     * a real pointer; liburing writes the first CQE through it, so passing NULL segfaults. */
    struct io_uring_cqe *first = NULL;
    io_uring_submit_and_wait_timeout(&r->ring, &first, 1, &ts, NULL);
    r->n_enter++;
    r->pending = 0;
    int n = fpuReapAll(r, ev);
    /* Flush any SQEs the reap prepared (recv re-arms, short-send remainders, poll re-arm). */
    if (r->pending) { io_uring_submit(&r->ring); r->pending = 0; }
    return n;
}

int fpUringReap(struct fpUringRing *r) {
    if (!r || !r->inited) return 0;
    fpUringEvents ev;
    int n = fpuReapAll(r, &ev);
    if (r->pending) { io_uring_submit(&r->ring); r->pending = 0; }
    return n;
}

int fpUringClientInflight(struct fpUringRing *r, client *c) {
    if (!r || !r->inited) return 0;
    if (c == NULL) return r->inflight; /* ring-wide send inflight (freelist gate) */
    /* Per-client: recv armed + this client's queued sends still outstanding. A leaving client
     * hands back once its own count reaches 0, independent of other clients' traffic. */
    int fd = c->conn ? c->conn->fd : -1;
    fpuClient *fc = fpuClientSlot(r, fd);
    if (!fc || fc->fd != fd) return 0;
    return fc->recv_inflight + fc->send_inflight;
}

int fpUringInflight(struct fpUringRing *r) { return (r && r->inited) ? r->inflight : 0; }

void fpUringFoldEpoll(struct fpUringRing *r, int epfd) {
    if (!r || !r->inited || epfd < 0) return;
    r->epfd_folded = epfd;
    struct io_uring_sqe *sqe = io_uring_get_sqe(&r->ring);
    if (!sqe) return;
    io_uring_prep_poll_multishot(sqe, epfd, POLLIN);
    io_uring_sqe_set_data64(sqe, FPU_UD_POLL);
    r->pending++;
    io_uring_submit(&r->ring);
    r->pending = 0;
}

sds fpUringInfo(sds info) {
    long long recv = 0, recvb = 0, send = 0, sendb = 0, reap = 0, enter = 0, ep = 0, nob = 0, sh = 0;
    for (int i = 0; i < 64; i++) {
        struct fpUringRing *r = fpu_rings[i];
        if (!r) continue;
        recv += r->n_recv; recvb += r->n_recv_bytes; send += r->n_send; sendb += r->n_send_bytes;
        reap += r->n_reaped; enter += r->n_enter; ep += r->n_epoll_wakes; nob += r->n_enobufs; sh += r->n_short;
    }
    return sdscatprintf(info,
        "fastpath_uring_recv:%lld\r\n"
        "fastpath_uring_recv_bytes:%lld\r\n"
        "fastpath_uring_send:%lld\r\n"
        "fastpath_uring_send_bytes:%lld\r\n"
        "fastpath_uring_reaped:%lld\r\n"
        "fastpath_uring_enter:%lld\r\n"
        "fastpath_uring_epoll_wakes:%lld\r\n"
        "fastpath_uring_enobufs:%lld\r\n"
        "fastpath_uring_short_sends:%lld\r\n",
        recv, recvb, send, sendb, reap, enter, ep, nob, sh);
}

#endif /* HAVE_LIBURING */
