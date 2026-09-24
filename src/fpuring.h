/* Fast-path io_uring path: per-IO-thread ring is the loop's blocking point when
 * io-threads-uring is on. Fast-path client sockets receive via multishot RECV with a
 * provided buffer ring and send via queued SEND; the epoll fd is folded in through a
 * multishot POLL_ADD so non-fastpath clients and control fds still wake the loop.
 * Build-gated on HAVE_LIBURING; a no-op stub otherwise, and the epoll path is unchanged
 * when the ring is off. */
#ifndef FPURING_H
#define FPURING_H

#include "server.h"

struct fpUringRing; /* opaque per-IO-thread ring state */

/* Outcome of one ring wait: what the caller must service after the single enter. */
typedef struct fpUringEvents {
    int epoll_ready; /* the folded epoll POLL_ADD completed: drain epoll (zero timeout) */
    int recvs;       /* recv CQEs fed to the parse path this pass */
    int sends;       /* send CQEs reaped this pass */
    int reads_ready; /* fast-path clients with new bytes in querybuf to parse this pass */
} fpUringEvents;

/* True when the binary was built with liburing AND server.io_threads_uring is set. */
int fpUringWanted(void);

/* Per-IO-thread ring lifecycle. init returns a ring handle or NULL (falls back to epoll,
 * logged). flags selects the SETUP_* variant per server.io_threads_uring_flags. */
struct fpUringRing *fpUringInit(int tid);
int fpUringFd(struct fpUringRing *r);
void fpUringFree(struct fpUringRing *r);

/* Fold the IO thread's epoll fd into the ring as a multishot POLL_ADD so epoll readiness
 * arrives as a CQE and the ring can be the sole blocking point. Idempotent. */
void fpUringFoldEpoll(struct fpUringRing *r, int epfd);

/* Register a fast-path client's socket for multishot RECV on this ring. The socket leaves
 * epoll and is driven entirely by the ring until fpUringClientCancel. */
int fpUringClientAdd(struct fpUringRing *r, client *c, int fd);

/* Cancel a client's outstanding recv/send SQEs (client leaving or closing). Bytes already
 * parsed are unaffected; the caller hands the client back only once inflight reaches 0. */
void fpUringClientCancel(struct fpUringRing *r, client *c, int fd);

/* Queue a send of buf[0..len) to fd for client c. Copies the bytes; C_OK if queued, C_ERR to
 * fall back to a direct write. Per-client order preserved by submission order. */
int fpUringQueueSend(struct fpUringRing *r, client *c, int fd, const void *buf, size_t len);

/* The loop's blocking point when uring is on: submit all queued SQEs and wait up to
 * timeout_us for at least one completion, then reap every ready CQE. Recv completions are fed
 * to the client parse callback (below) via fpUringSetRecvSink. Fills ev with what to service.
 * Returns total CQEs reaped. */
int fpUringWaitAndReap(struct fpUringRing *r, int timeout_us, fpUringEvents *ev);

/* Reap without waiting (used on the epoll-driven fallback pass and on hand-back drain). */
int fpUringReap(struct fpUringRing *r);
void fpUringSubmit(struct fpUringRing *r);

/* The owning IO thread registers its recv sink once: called per recv CQE with the client and
 * the received bytes to append to the client's querybuf and parse. */
typedef void (*fpUringRecvFn)(int tid, client *c, const char *buf, int len, int eof);
void fpUringSetRecvSink(struct fpUringRing *r, int tid, fpUringRecvFn fn);

/* Outstanding recv+send SQEs still referencing this client (must be 0 before hand-back). */
int fpUringClientInflight(struct fpUringRing *r, client *c);
/* Ring-wide outstanding sends (hand-back gate for the send-buffer freelist). */
int fpUringInflight(struct fpUringRing *r);

/* Append fastpath_uring_* diagnostic counters to an INFO buffer. */
sds fpUringInfo(sds info);

#endif
