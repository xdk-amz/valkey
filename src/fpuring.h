/* Fast-path io_uring send path (prototype): per-IO-thread ring, send-side only.
 * Reads stay on epoll; only fpSend is diverted through IORING_OP_SEND when the
 * ring is active. Build-gated on HAVE_LIBURING; a no-op stub otherwise. */
#ifndef FPURING_H
#define FPURING_H

#include "server.h"

struct fpUringRing; /* opaque per-IO-thread ring state */

/* True when the binary was built with liburing AND server.io_threads_uring is set. */
int fpUringWanted(void);

/* Per-IO-thread ring lifecycle. init returns a ring handle or NULL (falls back to epoll,
 * logged). ring_fd is registered so the caller can add it to its epoll for CQE readiness. */
struct fpUringRing *fpUringInit(int tid);
int fpUringFd(struct fpUringRing *r);
void fpUringFree(struct fpUringRing *r);

/* Queue a send of buf[0..len) to fd for client c (identified by control pointer + generation
 * for completion matching). Copies the bytes; returns C_OK if queued, C_ERR if it must fall
 * back to a direct write. Ordering per client is preserved by submission order. */
int fpUringQueueSend(struct fpUringRing *r, client *c, int fd, const void *buf, size_t len);

/* Submit all queued SQEs once per loop iteration. */
void fpUringSubmit(struct fpUringRing *r);

/* Reap completions. Calls back for send errors (peer gone). Returns count reaped. */
int fpUringReap(struct fpUringRing *r);

/* Outstanding sends still referencing this client (must be 0 before hand-back). */
int fpUringClientInflight(struct fpUringRing *r, client *c);

/* Append fastpath_uring_* diagnostic counters to an INFO buffer. */
sds fpUringInfo(sds info);

#endif
