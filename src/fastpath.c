/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* IO threads own fast-path clients; main only executes their published command batches. */

#include "server.h"
#include "fastpath.h"
#include "io_threads.h"
#include "memory_prefetch.h"
#include "fastget.h"
#include "fastpath_no_epoll.h"
#include "poller.h"
#include "staged_loop.h"
#include "module.h"
#include "bgiteration.h"
#include "throttle.h"
#include <sys/uio.h>

extern int ProcessingEventsWhileBlocked; /* networking.c */

static_assert(sizeof(cmdBatch) == offsetof(cmdBatch, e) + IO_BATCH_MAX * sizeof(cmdEntry),
              "cmdBatch is its header plus IO_BATCH_MAX inline entries");

#define FP_ARENA_SIZE (16 * 1024) /* reply bytes per batch before a slot spills to the heap */
#define FP_FREELIST_MAX 64

typedef struct fpThread {
    int ready;     /* initialized by fastpathInitThread */
    cmdBatch *cur; /* batch being assembled */
    cmdBatch *freelist[FP_FREELIST_MAX];
    int nfree;
    long long reads, net_input_bytes, net_output_bytes, writes, batches, speculated;
    long long reads_synced, writes_synced; /* main only: reads/writes already folded into server stats */
    /* Staged loop. Batches this thread published in the read phase, per nesting depth: written by the
     * thread in the read phase, executed by main in the write phase, delivered and recycled by the same
     * thread in the reply phase. staged_leaving holds the clients this thread saw leave; main hands
     * them back after the reply phase. */
    cmdBatch **sb[STAGED_MAX_DEPTH];
    int sbn[STAGED_MAX_DEPTH];
    int sbcap[STAGED_MAX_DEPTH];
    list staged_leaving;
} fpThread;

static fpThread fp_threads[IO_THREADS_MAX_NUM];

/* acl-offload admission seqlock, one per thread, written only by that thread: odd while the thread
 * reads a bound user's rule set (ACL tagging), even otherwise. Main bumps the ACL epoch, then waits
 * in fastpathAdmissionQuiesce until each seq is even or has moved past the odd value it saw, so no
 * reader still holds a rule-set pointer from before the mutation. */
static _Atomic uint32_t fp_admit_seq[IO_THREADS_MAX_NUM];

/* The Begin store is seq_cst and pairs with the seq_cst load in fastpathAdmissionQuiesce: either
 * main observes this thread odd and waits the read out, or the thread's rule-set loads are ordered
 * after main's epoch bump and read the new lists. A release store would let those loads hoist above
 * the odd publish. */
void fastpathAdmitReadBegin(void) {
    _Atomic uint32_t *s = &fp_admit_seq[getCurTid()];
    atomic_store_explicit(s, atomic_load_explicit(s, memory_order_relaxed) + 1, memory_order_seq_cst);
}

void fastpathAdmitReadEnd(void) {
    _Atomic uint32_t *s = &fp_admit_seq[getCurTid()];
    atomic_store_explicit(s, atomic_load_explicit(s, memory_order_relaxed) + 1, memory_order_release);
}

uint32_t testOnlyFastpathAdmitSeq(int tid) {
    return atomic_load_explicit(&fp_admit_seq[tid], memory_order_acquire);
}

/* Main: return once no thread still holds a rule-set pointer from before the caller's epoch bump.
 * The caller bumps the epoch first, so a read that starts after this re-snapshots the epoch and its
 * verdict is not trusted. */
void fastpathAdmissionQuiesce(void) {
    serverAssert(inMainThread());
    atomic_thread_fence(memory_order_seq_cst); /* pairs with the seq_cst fence in aclOffloadTagCommand */
    for (int tid = 0; tid < IO_THREADS_MAX_NUM; tid++) {
        uint32_t seen = atomic_load_explicit(&fp_admit_seq[tid], memory_order_seq_cst);
        if (!(seen & 1u)) continue;
        while (atomic_load_explicit(&fp_admit_seq[tid], memory_order_acquire) == seen) {
        }
    }
}
/* Main-thread executor per IO thread and staged nesting depth: a nested iteration executes while the
 * outer one's executor is still inside processCommand. */
static client *fp_exec_client[STAGED_MAX_DEPTH][IO_THREADS_MAX_NUM];
/* Staged loop state. fp_staged and fp_depth change only between phases (main), so IO threads read them
 * inside a phase after the barrier published them. Staged clients are owned by the IO side as a whole,
 * not by one thread: fp_staged_clients lists them and changes only on main, between phases or in the
 * write phase. fp_staged_closing lists clients main asked to close. A staged client is freed only
 * after main took it back following the reply barrier of every iteration that produced entries for
 * it, so an entry names its client by plain pointer. */
static _Atomic int fp_staged = 0;
static _Atomic int fp_depth = 0;
static list *fp_staged_closing = NULL;
static list fp_staged_clients;
static _Thread_local int fp_cur_tid = 0; /* IO thread running the current staged phase */
static size_t fastpath_clients = 0;      /* main thread only */
static int fp_slots = 0;                 /* main thread only: 1 + highest initialized thread */
static long long fp_retired[6];          /* main thread only: counters of threads since retired */

size_t fastpathClientCount(void) {
    return fastpath_clients;
}

static inline int fpStaged(void) {
    return atomic_load_explicit(&fp_staged, memory_order_relaxed);
}

static int fp_staged_admit = 0; /* main only: new staged clients are admitted */

static inline int fpDepth(void) {
    return atomic_load_explicit(&fp_depth, memory_order_relaxed);
}

static cmdBatch *fpAllocBatch(fpThread *t, int tid) {
    cmdBatch *b;
    if (t->nfree > 0) {
        b = t->freelist[--t->nfree];
    } else {
        b = zmalloc(sizeof(cmdBatch));
        b->arena = zmalloc(FP_ARENA_SIZE);
        b->arena_cap = FP_ARENA_SIZE;
    }
    b->count = 0;
    b->io_tid = tid;
    b->arena_used = 0;
    return b;
}

static void fpRecycleBatch(fpThread *t, cmdBatch *b) {
    if (t->nfree < FP_FREELIST_MAX) {
        t->freelist[t->nfree++] = b;
    } else {
        zfree(b->arena);
        zfree(b);
    }
}

void fastpathInitThread(int tid) {
    fpThread *t = &fp_threads[tid];
    memset(t, 0, sizeof(*t));
    t->ready = 1;
    if (tid + 1 > fp_slots) fp_slots = tid + 1;
}

/* Destruction preconditions: no batch or leaving client may still name this thread. */
void fastpathFreeThread(int tid) {
    fpThread *t = &fp_threads[tid];
    if (!t->ready) return;
    serverAssert(t->cur == NULL);
    for (int d = 0; d < STAGED_MAX_DEPTH; d++) {
        serverAssert(t->sbn[d] == 0);
        zfree(t->sb[d]);
        t->sb[d] = NULL;
        t->sbcap[d] = 0;
    }
    serverAssert(listLength(&t->staged_leaving) == 0);
    while (t->nfree > 0) {
        cmdBatch *b = t->freelist[--t->nfree];
        zfree(b->arena);
        zfree(b);
    }
    t->ready = 0;
    server.stat_io_reads_processed += t->reads - t->reads_synced;
    server.stat_io_writes_processed += t->writes - t->writes_synced;
    fp_retired[0] += t->reads;
    fp_retired[1] += t->net_input_bytes;
    fp_retired[2] += t->net_output_bytes;
    fp_retired[3] += t->writes;
    fp_retired[4] += t->batches;
    fp_retired[5] += t->speculated;
    while (fp_slots > 0 && !fp_threads[fp_slots - 1].ready) fp_slots--;
}

/* Admitted clients carry only the session state a command entry can hold: user, db and RESP. */
static int fpSessionEligible(client *c) {
    if (!server.io_threads_fast_path || !fpStaged()) return 0; /* the staged loop is the only fast-path mode */
    if (!server.io_threads_strict_offload || ProcessingEventsWhileBlocked || server.io_threads_num < 2) return 0;
    if (!c->conn || c->flag.fake) return 0;
    if (c->conn->type != connectionByType(CONN_TYPE_SOCKET)) return 0;
    if (authRequired(c)) return 0; /* main enforces a later default-user password change per entry */
    if (server.cluster_enabled) return 0;
    if (isPausedActions(PAUSE_ACTION_CLIENT_ALL | PAUSE_ACTION_CLIENT_WRITE)) return 0; /* paused clients are postponed on main */
    if (bgIteration_iterationActive()) return 0;                                        /* a forkless save may block commands on keys in use */
    if (throttle_anyThrottlers()) return 0;                                             /* a throttler may hold a command; main runs the check */
    if (c->flag.replica || c->flag.primary || c->flag.monitor || c->slot_migration_job) return 0;
    if (c->flag.repl_rdb_channel || c->repl_data) return 0; /* a connection on its way to becoming a replica */
    if (c->flag.blocked || c->flag.unblocked || c->flag.protected || c->flag.lua_debug) return 0;
    if (c->flag.close_asap || c->flag.close_after_reply || c->flag.close_after_command) return 0;
    if (c->mstate || c->flag.pubsub || c->flag.tracking || c->name) return 0;
    if (c->capa) return 0; /* CLIENT CAPA changes replies main builds for this client, e.g. REDIRECT */
    if (c->flag.no_touch || c->flag.reply_off || c->flag.reply_skip || c->flag.reply_skip_next) return 0;
    if (c->flag.import_source) return 0;
    return 1;
}

int fastpathEligible(client *c) {
    return fpSessionEligible(c) && !c->flag.pending_read && !c->flag.partitioned;
}

/* Both addresses are fixed for the life of the connection; a transport they cannot represent is not admitted. */
static int fpCaptureAddrs(client *c) {
    struct sockaddr_storage sa;
    socklen_t salen = sizeof(sa);
    if (getpeername(c->conn->fd, (struct sockaddr *)&sa, &salen) != 0) return C_ERR;
    if (peerIdentityFromSockaddr(&c->fp_peer, (struct sockaddr *)&sa, salen) != C_OK) return C_ERR;
    salen = sizeof(sa);
    if (getsockname(c->conn->fd, (struct sockaddr *)&sa, &salen) != 0) return C_ERR;
    return peerIdentityFromSockaddr(&c->fp_local, (struct sockaddr *)&sa, salen);
}

/* One connection, one ClientControl: allocated before its first fast-path admission and kept until the
 * connection is freed. */
int fastpathControlEnsure(client *c) {
    if (c->control) return C_OK;
    ClientControl *cc = zmalloc(sizeof(ClientControl));
    if (!cc) return C_ERR;
    cc->client_id = c->id;
    cc->lifecycle = FP_DETACHED;
    cc->closing = 0;
    c->control = cc;
    return C_OK;
}

/* The one and only reclaimer for a ClientControl, called from freeClient once the connection is fully
 * torn down. freeClient waits until main took the client back after a reply barrier, so no batch entry
 * can name the control any more. */
void fastpathControlReclaim(client *c) {
    if (!c->control) return;
    serverAssert(c->control->lifecycle == FP_DETACHED);
    zfree(c->control);
    c->control = NULL;
}

static int fpCommandAllowed(struct serverCommand *cmd) {
    if (!cmd) return 0;                     /* unknown command: the main path replies (and runs the host:/post check) */
    if (cmd->proc == pingCommand) return 1; /* fast-path clients are never in pubsub mode */
    if (!(cmd->flags & (CMD_WRITE | CMD_READONLY))) return 0;
    if (cmd->flags & (CMD_BLOCKING | CMD_PUBSUB | CMD_ADMIN | CMD_NOSCRIPT | CMD_NO_MULTI | CMD_NO_ASYNC_LOADING |
                      CMD_ALLOW_BUSY | CMD_TOUCHES_ARBITRARY_KEYS))
        return 0;
    /* A module command or a module keyspace subscriber may block the client running the command, which
     * an executor cannot be. Main changes both only between phases. */
    if (cmd->module_cmd || moduleHasKeyspaceSubscribers()) return 0;
    return 1;
}

static void fpSubmit(fpThread *t) {
    cmdBatch *b = t->cur;
    if (!b || b->count == 0) return;
    t->cur = NULL;
    int d = fpDepth();
    if (t->sbn[d] == t->sbcap[d]) {
        t->sbcap[d] = t->sbcap[d] ? t->sbcap[d] * 2 : 16;
        t->sb[d] = zrealloc(t->sb[d], sizeof(cmdBatch *) * t->sbcap[d]);
    }
    t->sb[d][t->sbn[d]++] = b;
    t->batches++;
}

/* The client stops reading; nothing of it is on main past this iteration, so main hands it back after
 * the reply phase. */
static void fpBeginLeave(fpThread *t, client *c, int state) {
    if (c->control->lifecycle != FP_ACTIVE) return;
    c->control->lifecycle = state;
    pollerUnregister(stagedPoller(), c->conn->fd);
    listLinkNodeTail(&t->staged_leaving, &c->fp_stage_node);
}

static void fpAppendEntry(cmdBatch *b, client *c, robj **argv, int argc, int argv_len, size_t argv_len_sum, unsigned long long input_bytes, struct serverCommand *cmd, int slot, int read_flags) {
    cmdEntry *e = &b->e[b->count++];
    e->c = c;
    e->control = c->control;
    e->argv = argv;
    e->argc = argc;
    e->argv_len = argv_len;
    e->argv_len_sum = argv_len_sum;
    e->input_bytes = input_bytes;
    e->cmd = cmd;
    e->slot = slot;
    /* acl-offload: the IO thread checks the command's ACL here; main trusts an ALLOW only while the
     * ACL epoch it was checked under is current. */
    aclOffloadTagCommand(c, cmd, argv, argc, c->db->id, &read_flags);
    e->origin.acl_epoch_seen = c->acl_epoch_seen;
    e->read_flags = read_flags;
    e->db = c->db;
    e->resp = (uint8_t)c->resp;
    e->origin.client_id = c->id;
    e->origin.principal = c->user;
    e->origin.authenticated = c->flag.authenticated;
    e->origin.peer = c->fp_peer;
    e->origin.local = c->fp_local;
    e->reply_off = e->reply_len = 0;
    e->reply_big = NULL;
    e->reply_big_len = 0;
    e->requeued = 0;
    c->fp_inflight++;
}

/* The first unsupported command and all successors remain queued for main. */
static void fpHarvest(fpThread *t, int tid, client *c) {
    int leave = 0;
    int max = server.io_batch_commands;
    cmdQueue *q = &c->cmd_queue;

    if (c->argc > 0 && (c->read_flags & READ_FLAGS_PARSING_COMPLETED)) {
        if ((c->read_flags & READ_FLAGS_ERROR_MASK) || !fpCommandAllowed(c->parsed_cmd)) {
            leave = 1;
        } else {
            if (!t->cur) t->cur = fpAllocBatch(t, tid);
            fpAppendEntry(t->cur, c, c->argv, c->argc, c->argv_len, c->argv_len_sum, c->net_input_bytes_curr_cmd,
                          c->parsed_cmd, c->slot, c->read_flags);
            c->argv = NULL;
            c->argc = 0;
            c->argv_len = 0;
            c->argv_len_sum = 0;
            c->parsed_cmd = NULL;
            c->read_flags = 0;
            c->net_input_bytes_curr_cmd = 0; /* the parser accumulates; resetClient never runs for this client */
            if (t->cur->count >= max) fpSubmit(t);
        }
    } else if (c->read_flags & READ_FLAGS_ERROR_MASK) {
        leave = 1;
    } else if (c->read_flags & READ_FLAGS_PARSING_NEGATIVE_MBULK_LEN) {
        c->read_flags = 0; /* an empty multibulk is skipped, as on main */
        c->reqtype = 0;
    }

    while (!leave && q->off < q->len) {
        parsedCommand *p = &q->cmds[q->off];
        if (p->read_flags & READ_FLAGS_PARSING_NEGATIVE_MBULK_LEN) {
            /* An empty multibulk: nothing to run. It ends the queue, so the next command starts fresh. */
            q->off++;
            c->reqtype = 0;
            continue;
        }
        int complete = p->read_flags & READ_FLAGS_PARSING_COMPLETED;
        if (!complete && !(p->read_flags & READ_FLAGS_ERROR_MASK)) break; /* trailing partial */
        if ((p->read_flags & READ_FLAGS_ERROR_MASK) || !fpCommandAllowed(p->cmd)) {
            leave = 1;
            break;
        }
        if (!t->cur) t->cur = fpAllocBatch(t, tid);
        fpAppendEntry(t->cur, c, p->argv, p->argc, p->argv_len, p->argv_len_sum, p->input_bytes, p->cmd, p->slot,
                      p->read_flags);
        q->off++;
        if (t->cur->count >= max) fpSubmit(t);
    }

    if (leave) {
        /* Promote the first queued command so the main path can resume parsing. */
        if (c->argc == 0 && q->off < q->len) {
            parsedCommand *p = &q->cmds[q->off++];
            c->argv = p->argv, c->argc = p->argc, c->argv_len = p->argv_len, c->argv_len_sum = p->argv_len_sum;
            c->net_input_bytes_curr_cmd = p->input_bytes, c->parsed_cmd = p->cmd, c->slot = p->slot;
            c->read_flags |= p->read_flags;
            if (q->off == q->len) q->off = q->len = 0; /* an emptied queue reads as empty to the parser */
        }
        fpBeginLeave(t, c, FP_LEAVING);
        return;
    }

    /* A trailing partial command continues in c->argv on the next read. */
    if (q->off < q->len) {
        parsedCommand *p = &q->cmds[q->off++];
        serverAssert(q->off == q->len);
        c->argv = p->argv, c->argc = p->argc, c->argv_len = p->argv_len, c->argv_len_sum = p->argv_len_sum;
        c->net_input_bytes_curr_cmd = p->input_bytes, c->parsed_cmd = NULL, c->slot = -1;
        c->read_flags = p->read_flags;
    }
    q->off = q->len = 0;
}

static void fpSend(fpThread *t, client *c, struct iovec *iov, int iovcnt);

/* Reads at the head of what a client just sent execute here, in the read phase, on the IO thread
 * reading the client: a contiguous prefix of GETs is answered from the keyspace and the replies go
 * straight to the socket. The first command that cannot be executed here, and every
 * command after it, goes to main in the batch, so a read never overtakes an earlier write of the
 * same connection. For the same reason nothing is executed here while the client still has
 * commands pending on main. */
static void fpSpeculate(fpThread *t, int tid, client *c) {
    if (c->fp_inflight != 0 || c->argc == 0 || !(c->read_flags & READ_FLAGS_PARSING_COMPLETED)) return;
    if (fpDepth() > 0) return; /* main is blocked: every command goes to main for its BUSY/LOADING reply */
    int n = fastGetSpeculateBatch(c, tid);
    if (n <= 0) return;
    fastGetConsumeSpeculated(c, n, tid);
    if (c->argc == 0) c->net_input_bytes_curr_cmd = 0; /* resetClient never runs for this client */
    t->speculated += n;
    struct iovec iov = {.iov_base = c->buf, .iov_len = c->bufpos};
    c->bufpos = 0;
    fpSend(t, c, &iov, 1);
}

static void fpRead(fpThread *t, int tid, client *c) {
    c->read_flags = 0; /* authenticated at admission, not replicated; parse state lives in multibulklen/bulklen */
    /* The fd is edge-triggered: a read that filled its buffer may have left bytes in the socket that
     * no new edge will report, so the next batch reads the client again. */
    int more = readToQueryBuf(c);
    t->reads++;
    if (c->nread <= 0) {
        /* The client may still point at this thread's shared query buffer:
         * give it back before anyone else can free it with the client. */
        trimClientQueryBuffer(c);
        if (c->nread < 0 && connGetState(c->conn) == CONN_STATE_CONNECTED) return; /* EAGAIN */
        /* EOF or error: the client closes. Nothing more is read; entries in
         * flight return first, then the main thread frees the client. */
        fpBeginLeave(t, c, FP_CLOSING);
        return;
    }
    t->net_input_bytes += c->nread;
    c->net_input_bytes += c->nread;
    c->last_interaction = server.unixtime;
    if (c->read_flags & READ_FLAGS_QB_LIMIT_REACHED) {
        trimClientQueryBuffer(c);
        fpBeginLeave(t, c, FP_LEAVING);
        return;
    }
    /* The multibulk parser queues the pipelined RESP commands behind the first one, but the
     * inline parser stops after one command and a queue run stops at the first non-RESP byte.
     * The main path drains the leftover bytes by looping in processInputBuffer; do the same
     * here, or the rest of the packet waits for the connection's next read event. */
    for (;;) {
        size_t pos = c->qb_pos;
        parseInputBuffer(c);
        int completed = c->read_flags & (READ_FLAGS_PARSING_COMPLETED | READ_FLAGS_PARSING_NEGATIVE_MBULK_LEN);
        prepareCommandQueue(c);
        fpSpeculate(t, tid, c);
        if (c->control->lifecycle == FP_ACTIVE) fpHarvest(t, tid, c);
        /* Stop once the client has left the fast path, a partial command needs more
         * bytes, a parse error is pending for main, or the buffer is drained. */
        if (c->control->lifecycle != FP_ACTIVE) break;
        if (!completed || c->argc > 0 || (c->read_flags & READ_FLAGS_ERROR_MASK)) break;
        if (c->querybuf == NULL || c->qb_pos >= sdslen(c->querybuf) || c->qb_pos == pos) break;
        c->read_flags = 0;
    }
    trimClientQueryBuffer(c);
    if (more && c->control->lifecycle == FP_ACTIVE) pollerRequeue(stagedPoller(), c->conn->fd, POLLER_READABLE);
}

static void fpEnableWriteInterest(client *c, int on) {
    pollerModify(stagedPoller(), c->conn->fd, on ? (POLLER_READABLE | POLLER_WRITABLE) : POLLER_READABLE);
}

static int fpFlushOut(fpThread *t, client *c) {
    while (c->fp_out && sdslen(c->fp_out) > 0) {
        ssize_t n = write(c->conn->fd, c->fp_out, sdslen(c->fp_out));
        if (n <= 0) {
            if (n < 0 && (errno == EAGAIN || errno == EINTR)) return 0;
            fpBeginLeave(t, c, FP_CLOSING);
            return 0;
        }
        t->net_output_bytes += n;
        c->net_output_bytes += n;
        t->writes++;
        sdsrange(c->fp_out, n, -1);
    }
    /* A drained buffer is given back, so a burst of replies does not stay charged to the client. */
    if (c->fp_out && sdsalloc(c->fp_out) > PROTO_REPLY_CHUNK_BYTES) {
        sdsfree(c->fp_out);
        c->fp_out = NULL;
    }
    return 1;
}

/* Unwritten replies past the normal class limit: the client leaves and main applies the limit when
 * it takes the bytes back. The limit counts what does not fit the static buffer, as on main. */
static void fpCheckOutLimit(fpThread *t, client *c) {
    clientBufferLimitsConfig *l = &server.client_obuf_limits[CLIENT_TYPE_NORMAL];
    size_t len = sdslen(c->fp_out);
    if (len <= c->buf_usable_size) return;
    size_t used = len - c->buf_usable_size;
    if ((l->hard_limit_bytes && used >= l->hard_limit_bytes) || (l->soft_limit_bytes && used >= l->soft_limit_bytes))
        fpBeginLeave(t, c, FP_LEAVING);
}

/* Buffered bytes always precede newly returned replies. */
static void fpSend(fpThread *t, client *c, struct iovec *iov, int iovcnt) {
    if (c->control->lifecycle == FP_CLOSING) return;
    if (c->fp_out && sdslen(c->fp_out) > 0) {
        for (int i = 0; i < iovcnt; i++) c->fp_out = sdscatlen(c->fp_out, iov[i].iov_base, iov[i].iov_len);
        fpCheckOutLimit(t, c);
        return;
    }
    ssize_t n = writev(c->conn->fd, iov, iovcnt);
    if (n < 0) {
        if (errno != EAGAIN && errno != EINTR) {
            fpBeginLeave(t, c, FP_CLOSING);
            return;
        }
        n = 0;
    }
    t->net_output_bytes += n;
    c->net_output_bytes += n;
    t->writes++;
    for (int i = 0; i < iovcnt; i++) {
        if ((size_t)n >= iov[i].iov_len) {
            n -= iov[i].iov_len;
            continue;
        }
        if (!c->fp_out) c->fp_out = sdsempty();
        c->fp_out = sdscatlen(c->fp_out, (char *)iov[i].iov_base + n, iov[i].iov_len - n);
        n = 0;
    }
    if (c->fp_out && sdslen(c->fp_out) > 0) {
        fpEnableWriteInterest(c, 1);
        fpCheckOutLimit(t, c);
    }
}

static parsedCommand fpEntryToParsed(cmdEntry *e) {
    parsedCommand p = {.read_flags = e->read_flags,
                       .argc = e->argc,
                       .argv = e->argv,
                       .argv_len = e->argv_len,
                       .slot = e->slot,
                       .argv_len_sum = e->argv_len_sum,
                       .input_bytes = e->input_bytes,
                       .cmd = e->cmd};
    e->argv = NULL; /* the queue owns it now */
    return p;
}

/* Entries main returned unexecuted go back in front of anything parsed since, after any
 * already held; the client then leaves so the main path runs them with its own semantics. */
static void fpRequeue(fpThread *t, client *c, cmdEntry *e, int n) {
    cmdQueue *q = &c->cmd_queue;
    int qheld = c->fp_held ? c->fp_held - 1 : 0; /* held commands queued behind c->argv */
    int rest = q->len - q->off - qheld;
    int first = c->fp_held ? 0 : 1; /* with nothing held, e[0] becomes c->argv */
    parsedCommand cur;
    int has_cur = !c->fp_held && c->argc > 0; /* a promoted command or a trailing partial, parsed after the entries */
    if (has_cur) {
        cur = (parsedCommand){.read_flags = c->read_flags,
                              .argc = c->argc,
                              .argv = c->argv,
                              .argv_len = c->argv_len,
                              .slot = c->slot,
                              .argv_len_sum = c->argv_len_sum,
                              .input_bytes = c->net_input_bytes_curr_cmd,
                              .cmd = c->parsed_cmd};
    }
    parsedCommand *cmds = zmalloc(sizeof(parsedCommand) * (qheld + (n - first) + has_cur + rest));
    int k = 0;
    if (qheld) memcpy(cmds, q->cmds + q->off, sizeof(parsedCommand) * qheld);
    k += qheld;
    for (int i = first; i < n; i++) cmds[k++] = fpEntryToParsed(&e[i]);
    if (has_cur) cmds[k++] = cur;
    if (rest) memcpy(cmds + k, q->cmds + q->off + qheld, sizeof(parsedCommand) * rest);
    k += rest;
    zfree(q->cmds);
    q->cmds = cmds;
    q->off = 0;
    q->len = q->cap = k;
    if (first) {
        parsedCommand head = fpEntryToParsed(&e[0]);
        c->argv = head.argv, c->argc = head.argc, c->argv_len = head.argv_len, c->argv_len_sum = head.argv_len_sum;
        c->net_input_bytes_curr_cmd = head.input_bytes, c->parsed_cmd = head.cmd, c->slot = head.slot;
        c->read_flags = head.read_flags;
    }
    c->fp_held += n;
    fpBeginLeave(t, c, FP_LEAVING);
}

/* Consecutive entries for one client share a writev. */
static void fpDeliverBatch(fpThread *t, cmdBatch *b) {
    struct iovec iov[IO_BATCH_MAX];
    int i = 0;
    while (i < b->count) {
        client *c = b->e[i].c;
        int n = 0;
        int j = i;
        int requeued = 0;
        while (j < b->count && b->e[j].c == c) {
            cmdEntry *e = &b->e[j];
            if (e->requeued) {
                requeued++;
            } else if (e->reply_big) {
                iov[n].iov_base = e->reply_big, iov[n].iov_len = e->reply_big_len, n++;
            } else if (e->reply_len) {
                iov[n].iov_base = b->arena + e->reply_off, iov[n].iov_len = e->reply_len, n++;
            }
            j++;
        }
        if (n) fpSend(t, c, iov, n);
        c->fp_inflight -= (j - i);
        c->commands_processed += (j - i) - requeued;
        if (requeued) fpRequeue(t, c, &b->e[j - requeued], requeued); /* main stops executing a client at its first held entry */
        i = j;
    }
    for (int k = 0; k < b->count; k++) {
        cmdEntry *e = &b->e[k];
        if (e->reply_big) zfree(e->reply_big);
        /* Terminal frees of what main did not keep, on the thread that
         * allocated it; no free traffic through the shared inbox. */
        if (e->argv) {
            for (int j = 0; j < e->argc; j++)
                if (e->argv[j]) decrRefCount(e->argv[j]);
            zfree(e->argv);
            e->argv = NULL;
        }
    }
}

static client *fpExecutor(int tid) {
    int d = fpDepth();
    client *ec = fp_exec_client[d][tid];
    if (ec) return ec;
    ec = createClient(NULL);
    ec->flag.executor = 1;
    ec->cur_tid = tid; /* argv frees are offloaded to the owning thread */
    fp_exec_client[d][tid] = ec;
    return ec;
}

/* A client main asked to close runs no more queued commands, as with close_asap. Read from the control
 * so main tests an entry without touching the connection. */
static int fpControlDetaching(ClientControl *cc) {
    return cc && cc->closing;
}

static cmdEntry *fp_exec_entry = NULL; /* main only: the entry an executor is running */

/* Main, from closeClientOnOutputBufferLimitReached while an executor builds a reply: a reply past the
 * normal class hard limit closes its client, as on the main path. The reply is dropped and commands
 * that build long replies stop at close_asap. */
int fastpathExecutorOutputLimitReached(client *ec) {
    cmdEntry *e = fp_exec_entry;
    if (!e || !e->control) return 0;
    if (ec->flag.close_asap) return 1;
    clientBufferLimitsConfig *l = &server.client_obuf_limits[CLIENT_TYPE_NORMAL];
    if (!l->hard_limit_bytes || getClientOutputBufferMemoryUsage(ec) < l->hard_limit_bytes) return 0;
    ec->flag.close_asap = 1;
    freeClientAsync(e->c); /* records the close; main frees it once it took the client back */
    serverLog(LL_WARNING, "Client id=%llu closed for overcoming of output buffer limits.",
              (unsigned long long)e->control->client_id);
    server.stat_client_outbuf_limit_disconnections++;
    return 1;
}

/* The executor borrows argv and writes replies into the batch arena. */
static void fpExecute(client *ec, cmdBatch *b, cmdEntry *e) {
    char *saved_buf = ec->buf;
    size_t saved_usable = ec->buf_usable_size;
    user *principal = e->origin.principal;

    if (fpControlDetaching(e->control)) goto release_argv; /* no reply: the IO thread is closing it */
    if (isPausedActions(PAUSE_ACTION_CLIENT_ALL | PAUSE_ACTION_CLIENT_WRITE)) {
        e->requeued = 1; /* the main path postpones it like any other client's command */
        return;
    }
    if (bgIteration_iterationActive()) {
        e->requeued = 1; /* a forkless save may block it on a key in use, which only a real client can wait for */
        return;
    }
    if (throttle_anyThrottlers()) {
        e->requeued = 1; /* a throttler may hold the command, which only a real client can wait for */
        return;
    }

    if (principal->flags & USER_FLAG_RETIRED) {
        /* An ALLOW checked under the retired rule set does not describe its live successor. */
        e->read_flags &= ~READ_FLAGS_ACL_ALLOWED;
        principal = ACLResolveUser(principal);
    }
    serverAssert(principal); /* a deleted user's clients are detaching */
    ec->user = principal;
    ec->flag.authenticated = ec->flag.ever_authenticated = e->origin.authenticated;

    ec->argv = e->argv;
    ec->argc = e->argc;
    ec->argv_len = e->argv_len;
    ec->argv_len_sum = e->argv_len_sum;
    ec->net_input_bytes_curr_cmd = e->input_bytes;
    ec->parsed_cmd = e->cmd;
    ec->slot = e->slot;
    ec->read_flags = e->read_flags;
    ec->acl_epoch_seen = e->origin.acl_epoch_seen;
    ec->db = e->db;
    ec->resp = e->resp;
    ec->origin = &e->origin;
    ec->buf = b->arena + b->arena_used;
    ec->buf_usable_size = b->arena_cap - b->arena_used;
    ec->bufpos = 0;
    ec->flag.buf_encoded = 0;
    ec->flag.pending_command = 1;

    fp_exec_entry = e;
    ec->woff = 0;
    processCommandAndResetClient(ec);
    fp_exec_entry = NULL;
    /* WAIT and WAITAOF on the client wait for its last write's replication offset. IO threads wait at
     * the barrier while main executes, so main may set it on the client. */
    if (ec->woff && e->c) e->c->woff = ec->woff;
    if (ec->flag.close_asap) {
        /* Closed for the output buffer limit: nothing of this reply is sent. */
        ec->flag.close_asap = 0;
        listEmpty(ec->reply);
        ec->reply_bytes = 0;
        ec->bufpos = 0;
    }
    e->reply_off = (uint32_t)b->arena_used;
    e->reply_len = (uint32_t)ec->bufpos;
    b->arena_used += ec->bufpos;
    if (listLength(ec->reply) > 0) {
        size_t total = ec->bufpos;
        listIter li;
        listNode *ln;
        listRewind(ec->reply, &li);
        while ((ln = listNext(&li))) total += ((clientReplyBlock *)listNodeValue(ln))->used;
        char *big = zmalloc(total);
        memcpy(big, ec->buf, ec->bufpos);
        size_t off = ec->bufpos;
        listRewind(ec->reply, &li);
        while ((ln = listNext(&li))) {
            clientReplyBlock *o = listNodeValue(ln);
            memcpy(big + off, o->buf, o->used);
            off += o->used;
        }
        listEmpty(ec->reply);
        ec->reply_bytes = 0;
        b->arena_used -= ec->bufpos;
        e->reply_off = 0;
        e->reply_len = 0;
        e->reply_big = big;
        e->reply_big_len = (uint32_t)total;
    }
    ec->bufpos = 0;
    ec->buf = saved_buf;
    ec->buf_usable_size = saved_usable;
release_argv:
    /* IO threads receive only sole-reference argv objects for terminal frees. */
    for (int j = 0; j < e->argc; j++) {
        robj *o = e->argv[j];
        if (!o) continue;
        unsigned rc = objectGetRefcount(o);
        if (rc == 1) continue; /* sole reference: freed by the IO thread */
        if (rc < OBJ_FIRST_SPECIAL_REFCOUNT) decrRefCount(o);
        e->argv[j] = NULL;
    }
}

/* Prefetch the keys a batch touches before main executes it. */
static void fpPrefetchBatch(cmdBatch *b) {
    getKeysResult result;
    initGetKeysResult(&result);
    int room = 1;
    ClientControl *last_control = NULL;
    for (int k = 0; k < b->count && room; k++) {
        cmdEntry *e = &b->e[k];
        ClientControl *cc = e->control;
        if (cc != last_control) {
            __builtin_prefetch(cc);
            last_control = cc;
        }
        if (!e->cmd || (e->read_flags & READ_FLAGS_BAD_ARITY)) continue;
        room = prefetchBatchAddCommand(e->cmd, e->argv, e->argc, e->db, e->slot, &result);
    }
    getKeysFreeResult(&result);
    prefetchBatchRun();
    prefetchBatchReset();
}

/* Main records a close; the write phase skips the client's entries and main unmaps it after the reply
 * phase, so the client stays allocated while any batch entry names it. */
void fastpathRequestDetach(client *c) {
    if (c->flag.fp_detach_sent) return;
    c->flag.fp_detach_sent = 1;
    c->control->closing = 1;
    if (!fp_staged_closing) fp_staged_closing = listCreate();
    listAddNodeTail(fp_staged_closing, c);
}

/* True once main unmapped the closing client after a reply phase; only then may main free it. */
int fastpathDetachConsumed(client *c) {
    if (!c->flag.fp_detach_sent) return 1;
    if (c->flag.fastpath) return 0;
    c->flag.fp_detach_sent = 0;
    if (fp_staged_closing) {
        listNode *ln = listSearchKey(fp_staged_closing, c);
        if (ln) listDelNode(fp_staged_closing, ln);
    }
    return 1;
}

/* A client that left to authenticate returns once main has nothing further to do for it;
 * any other unsupported command keeps it on the main path, as before. */
static int fpQuiescent(client *c) {
    if (c->argc > 0 || c->flag.pending_command || c->cmd_queue.off < c->cmd_queue.len) return 0;
    if (c->querybuf && sdslen(c->querybuf) > c->qb_pos) return 0;
    if (c->io_read_state != CLIENT_IDLE || c->io_write_state != CLIENT_IDLE) return 0;
    return 1;
}

static int fpReadmit(client *c) {
    if (!fpQuiescent(c)) return 0;
    if (clientHasPendingReplies(c) && (writeToClient(c) != C_OK || clientHasPendingReplies(c))) return 0;
    if (!fastpathEligible(c)) return 0;
    if (c->flag.pending_write) {
        c->flag.pending_write = 0;
        listUnlinkNode(server.clients_pending_write, &c->clients_pending_write_node);
    }
    trimClientQueryBuffer(c);
    connSetReadHandler(c->conn, NULL);
    if (fastpathAttach(c) == C_OK) return 1;
    connSetReadHandler(c->conn, readQueryFromClient);
    return 0;
}

/* Called by main for a client it owns (partitioned or event-loop) after AUTH ran on it. Returns 1 once the
 * client is on the fast path; a client that is not yet quiescent keeps the flag and is retried next time. */
int fastpathReadmitAuthenticated(client *c) {
    if (c->flag.fastpath || c->flag.executor) {
        c->flag.fp_readmit = 0;
        return 0;
    }
    if (!fp_staged_admit) return 0; /* admission needs staged mode; the flag is kept until it starts */
    if (!fpQuiescent(c) || (c->flag.partitioned && c->flag.pending_read)) return 0;
    c->flag.fp_readmit = 0;
    if (!fpSessionEligible(c)) return 0;
    int was_partitioned = c->flag.partitioned;
    if (was_partitioned) unpartitionClient(c);
    if (fpReadmit(c)) return 1;
    if (was_partitioned && tryPartitionClient(c) == C_OK) connSetReadHandler(c->conn, NULL);
    return 0;
}

void fastpathHandoffDone(client *c, int closing) {
    serverAssert(fastpath_clients > 0);
    c->flag.fastpath = 0;
    /* Main is the owner again; publish the terminal state on the single source of truth. control is
     * still live here (reclaimed only at freeClient), so the read/write is safe. */
    c->control->lifecycle = FP_DETACHED;
    fastpath_clients--;
    ACLFastpathClientReturned(c);
    sds out = c->fp_out;
    c->fp_out = NULL;
    /* A recorded close: freeClientsInAsyncFreeQueue frees it now that main owns it. */
    if (c->flag.fp_detach_sent) {
        sdsfree(out);
        return;
    }
    if (closing || c->flag.close_asap) {
        sdsfree(out);
        freeClient(c); /* removes it from clients_to_close itself when close_asap is set */
        return;
    }
    connSetReadHandler(c->conn, readQueryFromClient);
    if (out) {
        /* Replies the IO thread could not write yet precede anything main produces for this client. */
        if (sdslen(out) > 0) addReplyProto(c, out, sdslen(out));
        sdsfree(out);
    }
    if (c->read_flags & READ_FLAGS_QB_LIMIT_REACHED) {
        handleQbLimitReached(c);
        return;
    }
    if (c->flag.close_asap) return; /* the returned replies passed the output buffer limit */
    if (c->argc > 0 && (c->read_flags & READ_FLAGS_PARSING_COMPLETED) && !(c->read_flags & READ_FLAGS_ERROR_MASK))
        c->flag.pending_command = 1;
    if (c->read_flags & READ_FLAGS_ERROR_MASK) {
        handleParseError(c); /* replies, sets close_after_reply; the write path closes it */
        return;
    }
    if (processPendingCommandAndInputBuffer(c) != C_OK) return;
    /* The command that sent it to main does not keep it there: it rejoins once main has nothing further
     * to do for it and it is still eligible. */
    if (!fp_staged_admit) {
        c->flag.fp_readmit = 1; /* no admission while staged mode is off: rejoins when it starts */
    } else {
        c->flag.fp_readmit = 0;
        if (fpReadmit(c)) return;
        c->flag.fp_readmit = 1; /* not quiescent yet: beforeNextClient retries */
    }
    beforeNextClient(c);
}

void fastpathInfo(sds *info) {
    long long reads = fp_retired[0], in = fp_retired[1], out = fp_retired[2], writes = fp_retired[3],
              batches = fp_retired[4], speculated = fp_retired[5];
    for (int i = 1; i < fp_slots; i++) {
        if (!fp_threads[i].ready) continue;
        reads += fp_threads[i].reads;
        in += fp_threads[i].net_input_bytes;
        out += fp_threads[i].net_output_bytes;
        writes += fp_threads[i].writes;
        batches += fp_threads[i].batches;
        speculated += fp_threads[i].speculated;
    }
    *info = sdscatprintf(*info,
                         "fastpath_clients:%zu\r\n"
                         "fastpath_workers_open:%d\r\n"
                         "fastpath_reads:%lld\r\n"
                         "fastpath_writes:%lld\r\n"
                         "fastpath_batches:%lld\r\n"
                         "fastpath_speculated:%lld\r\n"
                         "fastpath_net_input_bytes:%lld\r\n"
                         "fastpath_net_output_bytes:%lld\r\n",
                         fastpath_clients, stagedWorkerCount(), reads, writes, batches, speculated, in, out);
}

/* ---------------------------------------------------------------------------------------------
 * Staged loop. Main runs collect -> read -> write -> reply -> crons (staged_loop.c); these are the
 * fast-path halves of the read, write and reply phases. A staged client belongs to no single IO
 * thread: whichever thread claims its ready fd in a read phase reads it, publishes its entries on
 * its own batch list, and delivers those batches in the following reply phase, so one client's
 * replies are always written by the thread that produced its entries, in order.
 * --------------------------------------------------------------------------------------------- */

int fastpathStagedOn(void) {
    return fpStaged();
}

void fastpathStagedSetDepth(int depth) {
    serverAssert(depth >= 0 && depth < STAGED_MAX_DEPTH);
    atomic_store_explicit(&fp_depth, depth, memory_order_relaxed);
}

static void fastpathStagedEvent(int fd, int mask, void *privdata, pollerAffinity affinity);

/* Main, between phases or in the write phase. */
int fastpathAttach(client *c) {
    if (!fp_staged_admit) return C_ERR;
    if (c->fp_peer.family == 0 && fpCaptureAddrs(c) != C_OK) return C_ERR;
    if (fastpathControlEnsure(c) != C_OK) return C_ERR;
    if (pollerRegister(stagedPoller(), c->conn->fd, POLLER_READABLE, fastpathStagedEvent, c, POLLER_AFF_IO) != 0)
        return C_ERR;
    c->io_tid = 0;
    c->flag.fastpath = 1;
    c->fp_inflight = 0;
    c->fp_held = 0;
    c->fp_out = NULL;
    listInitNode(&c->fp_stage_node, c);
    listInitNode(&c->io_owner_node, c);
    c->control->lifecycle = FP_ACTIVE;
    c->control->closing = 0;
    listLinkNodeTail(&fp_staged_clients, &c->io_owner_node);
    fastpath_clients++;
    return C_OK;
}

/* Main, between phases. Turning off first hands every staged client back (fastpathStagedReleaseAll). */
void fastpathStagedSet(int on) {
    if (on) {
        fp_staged_admit = 1;
        atomic_store_explicit(&fp_staged, 1, memory_order_relaxed);
    } else {
        serverAssert(listLength(&fp_staged_clients) == 0 && fastpath_clients == 0);
        fp_staged_admit = 0;
        atomic_store_explicit(&fp_staged, 0, memory_order_relaxed);
    }
}

/* IO thread, read phase: the handler of a staged client's ready fd. */
static void fastpathStagedEvent(int fd, int mask, void *privdata, pollerAffinity affinity) {
    UNUSED(affinity);
    client *c = privdata;
    int tid = fp_cur_tid;
    fpThread *t = &fp_threads[tid];
    if (c->control->lifecycle != FP_ACTIVE) return;
    /* A nested iteration skips clients whose entries of the outer iteration are still on main; the
     * edge it consumed is handed to the next batch. */
    if (c->fp_inflight > 0) {
        pollerRequeue(stagedPoller(), fd, mask);
        return;
    }
    if (mask & POLLER_WRITABLE) {
        if (fpFlushOut(t, c)) fpEnableWriteInterest(c, 0);
        if (c->control->lifecycle != FP_ACTIVE) return;
    }
    if (mask & POLLER_READABLE) fpRead(t, tid, c);
}

/* IO thread, read phase: claim ready client fds until none are left, then publish the open batch. */
void fastpathStagedReadPhase(int tid) {
    fpThread *t = &fp_threads[tid];
    poller *p = stagedPoller();
    pollerEvent ev;
    fp_cur_tid = tid;
    while (pollerClaimIO(p, &ev)) ev.handler(ev.fd, ev.mask, ev.privdata, ev.affinity);
    fpSubmit(t);
    if (t->cur && t->cur->count == 0) {
        fpRecycleBatch(t, t->cur);
        t->cur = NULL;
    }
}

/* Main, write phase: execute every batch the read phase published, filling each entry's reply. */
int fastpathStagedExecute(void) {
    int d = fpDepth();
    int total = 0;
    int use_prefetch = prefetchBatchEnabled() && !ProcessingEventsWhileBlocked;
    int paused = isPausedActions(PAUSE_ACTION_CLIENT_ALL | PAUSE_ACTION_CLIENT_WRITE);
    for (int tid = 1; tid < fp_slots; tid++) {
        fpThread *t = &fp_threads[tid];
        if (t->sbn[d] == 0) continue;
        client *ec = fpExecutor(tid);
        for (int i = 0; i < t->sbn[d]; i++) {
            cmdBatch *b = t->sb[d][i];
            if (use_prefetch && !paused) fpPrefetchBatch(b);
            for (int k = 0; k < b->count; k++) fpExecute(ec, b, &b->e[k]);
            ec->origin = NULL;
            clientSetUser(ec, DefaultUser, 0); /* never keep a principal that may retire */
            total += b->count;
        }
    }
    return total;
}

int fastpathStagedRepliesPending(void) {
    int d = fpDepth();
    for (int tid = 1; tid < fp_slots; tid++)
        if (fp_threads[tid].sbn[d]) return 1;
    return 0;
}

/* IO thread, reply phase: write the replies of the batches this thread produced, in order. */
void fastpathStagedReplyPhase(int tid) {
    fpThread *t = &fp_threads[tid];
    int d = fpDepth();
    fp_cur_tid = tid;
    for (int i = 0; i < t->sbn[d]; i++) {
        cmdBatch *b = t->sb[d][i];
        fpDeliverBatch(t, b);
        fpRecycleBatch(t, b);
    }
    t->sbn[d] = 0;
}

static void fpStagedHandoff(client *c) {
    int closing = c->control->lifecycle == FP_CLOSING;
    listUnlinkNode(&fp_staged_clients, &c->io_owner_node);
    fastpathHandoffDone(c, closing);
}

/* Main, after the reply phase: hand back the clients IO threads saw leave, then unmap the clients main
 * asked to close. A handoff can run the client's queued commands, and one that blocks (a busy script)
 * runs nested iterations that call this again and change the leaving list, so that walk restarts from
 * the head after every handoff instead of keeping a node across it. */
void fastpathStagedFinish(void) {
    for (int tid = 1; tid < fp_slots; tid++) {
        fpThread *t = &fp_threads[tid];
        if (!t->ready) continue;
        server.stat_io_reads_processed += t->reads - t->reads_synced;
        server.stat_io_writes_processed += t->writes - t->writes_synced;
        t->reads_synced = t->reads;
        t->writes_synced = t->writes;
    }
    if (!fpStaged()) return;
    for (int tid = 1; tid < fp_slots; tid++) {
        fpThread *t = &fp_threads[tid];
        listNode *ln = listFirst(&t->staged_leaving);
        while (ln) {
            client *c = listNodeValue(ln);
            if (c->fp_inflight > 0) { /* entries of an outer iteration still on main */
                ln = ln->next;
                continue;
            }
            listUnlinkNode(&t->staged_leaving, &c->fp_stage_node);
            fpStagedHandoff(c);
            ln = listFirst(&t->staged_leaving);
        }
    }
    if (!fp_staged_closing) return;
    listNode *ln = listFirst(fp_staged_closing);
    while (ln) {
        listNode *next = ln->next;
        client *c = listNodeValue(ln);
        ln = next;
        if (!c->flag.fastpath) continue; /* already handed back; fastpathDetachConsumed drops it */
        if (c->control->lifecycle != FP_ACTIVE || c->fp_inflight > 0) continue;
        pollerUnregister(stagedPoller(), c->conn->fd);
        c->control->lifecycle = FP_CLOSING;
        fpStagedHandoff(c);
    }
}

/* Main, between phases, before staged mode turns off: every staged client returns to the main path. */
void fastpathStagedReleaseAll(void) {
    if (!fpStaged()) return;
    fp_staged_admit = 0;
    fastpathStagedFinish();
    while (listLength(&fp_staged_clients) > 0) {
        client *c = listNodeValue(listFirst(&fp_staged_clients));
        serverAssert(c->fp_inflight == 0);
        pollerUnregister(stagedPoller(), c->conn->fd);
        if (c->control->lifecycle == FP_ACTIVE) {
            c->control->lifecycle = FP_LEAVING;
            c->flag.fp_readmit = 1; /* left only because staged mode stopped: rejoins when it starts */
        }
        fpStagedHandoff(c);
    }
}

/* Main, right after staged mode turns on: clients accepted by the stock loop while it was turning on
 * join the fast path now instead of staying on main for good. */
void fastpathStagedAdoptClients(void) {
    listIter li;
    listNode *ln;
    listRewind(server.clients, &li);
    while ((ln = listNext(&li))) {
        client *c = listNodeValue(ln);
        if (c->flag.fastpath || c->flag.executor || !c->conn) continue;
        /* Clients that have run nothing yet, or that left only because staged mode stopped; one that
         * ran an unsupported command on main stays there. */
        if (!c->flag.fp_readmit && (c->control || c->commands_processed || c->argc || c->flag.pending_command))
            continue;
        if (!fpSessionEligible(c)) continue;
        if (fpReadmit(c)) c->flag.fp_readmit = 0;
    }
}
