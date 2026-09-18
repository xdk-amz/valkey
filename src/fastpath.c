/* IO threads own fast-path clients; main only executes their published command batches. */

#include "server.h"
#include "fastpath.h"
#include "io_threads.h"
#include "memory_prefetch.h"
#include <sys/epoll.h>
#include <sys/uio.h>

extern int ProcessingEventsWhileBlocked; /* networking.c */

#define FP_RING_SIZE 1024        /* batches per ring; batches, not commands */
#define FP_ARENA_SIZE (16 * 1024) /* reply bytes per batch before a slot spills to the heap */
#define FP_FREELIST_MAX 64
#define FP_CLIENT_INFLIGHT_MAX 256 /* commands of one client on main at once */
#define FP_TAG_DETACH ((uintptr_t)1) /* return-ring entry is a client to detach, not a batch */

typedef struct fpThread {
    spscQueue submit; /* IO thread -> main: cmdBatch * */
    spscQueue ret;    /* main -> IO thread: cmdBatch *, or client * | FP_TAG_DETACH */
    cmdBatch *cur;    /* batch being assembled */
    cmdBatch *freelist[FP_FREELIST_MAX];
    int nfree;
    int inflight;     /* batches submitted, not yet returned */
    list *leaving;    /* clients waiting for their entries to return before hand-off or close */
    long long reads, net_input_bytes, net_output_bytes, writes, batches;
} fpThread;

static fpThread fp_threads[IO_THREADS_MAX_NUM];
static client *fp_exec_client[IO_THREADS_MAX_NUM]; /* main-thread executor per IO thread */
static size_t fastpath_clients = 0;                 /* main thread only */
static unsigned fp_rr = 0;

size_t fastpathClientCount(void) {
    return fastpath_clients;
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
    b->opened_us = getMonotonicUs();
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
    spscInit(&t->submit, FP_RING_SIZE);
    spscInit(&t->ret, FP_RING_SIZE);
    t->leaving = listCreate();
}

void fastpathFreeThread(int tid) {
    fpThread *t = &fp_threads[tid];
    if (t->submit.buffer == NULL) return;
    spscFree(&t->submit);
    spscFree(&t->ret);
    while (t->nfree > 0) {
        cmdBatch *b = t->freelist[--t->nfree];
        zfree(b->arena);
        zfree(b);
    }
    if (t->cur) {
        zfree(t->cur->arena);
        zfree(t->cur);
        t->cur = NULL;
    }
    listRelease(t->leaving);
    t->leaving = NULL;
}

/* Admitted clients carry only the session state a command entry can hold: user, db and RESP. */
static int fpSessionEligible(client *c) {
    if (!server.io_threads_fast_path) return 0;
    if (!strictOffloadActive() || server.io_threads_num < 2) return 0;
    if (!c->conn || c->flag.fake) return 0;
    if (c->conn->type != connectionByType(CONN_TYPE_SOCKET)) return 0;
    if (authRequired(c)) return 0; /* main enforces a later default-user password change per entry */
    if (server.cluster_enabled) return 0;
    if (isPausedActions(PAUSE_ACTION_CLIENT_ALL | PAUSE_ACTION_CLIENT_WRITE)) return 0; /* paused clients are postponed on main */
    if (c->flag.replica || c->flag.primary || c->flag.monitor || c->slot_migration_job) return 0;
    if (c->flag.blocked || c->flag.unblocked || c->flag.protected || c->flag.lua_debug) return 0;
    if (c->flag.close_asap || c->flag.close_after_reply || c->flag.close_after_command) return 0;
    if (c->mstate || c->flag.pubsub || c->flag.tracking || c->name) return 0;
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

/* Publish all client state before level-triggered epoll can expose the socket. */
int fastpathAttach(client *c) {
    int tid = 1 + (int)(fp_rr++ % (unsigned)(server.io_threads_num - 1));
    int epfd = ioThreadEpollFd(tid);
    if (epfd <= 0) return C_ERR;
    if (c->fp_peer.family == 0 && fpCaptureAddrs(c) != C_OK) return C_ERR;
    c->io_tid = tid;
    c->flag.fastpath = 1;
    c->fp_state = FP_ACTIVE;
    c->fp_inflight = 0;
    c->fp_held = 0;
    c->fp_out = NULL;
    struct epoll_event ev = {.events = EPOLLIN, .data.ptr = c};
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, c->conn->fd, &ev) != 0) {
        c->flag.fastpath = 0;
        c->io_tid = 0;
        return C_ERR;
    }
    fastpath_clients++;
    return C_OK;
}

static int fpCommandAllowed(struct serverCommand *cmd) {
    if (!cmd) return 0; /* unknown command: the main path replies (and runs the host:/post check) */
    if (cmd->proc == pingCommand) return 1; /* fast-path clients are never in pubsub mode */
    if (!(cmd->flags & (CMD_WRITE | CMD_READONLY))) return 0;
    if (cmd->flags & (CMD_BLOCKING | CMD_PUBSUB | CMD_ADMIN | CMD_NOSCRIPT | CMD_NO_MULTI | CMD_NO_ASYNC_LOADING |
                      CMD_ALLOW_BUSY | CMD_TOUCHES_ARBITRARY_KEYS))
        return 0;
    return 1;
}

static void fpSubmit(fpThread *t) {
    cmdBatch *b = t->cur;
    if (!b || b->count == 0) return;
    t->cur = NULL;
    spscEnqueue(&t->submit, b, true);
    t->inflight++;
    t->batches++;
}

void fastpathSubmitPending(int tid) {
    fpThread *t = &fp_threads[tid];
    if (!t->cur || t->cur->count == 0 || spscBacklog(&t->submit) != 0) return;
    /* The hold amortizes per-batch work while bounding latency. */
    if (server.io_batch_hold_us > 0 && getMonotonicUs() - t->cur->opened_us < (monotime)server.io_batch_hold_us) return;
    fpSubmit(t);
}

/* Handoff waits until every published command for the client returns. */
static void fpBeginLeave(fpThread *t, client *c, int state) {
    if (c->fp_state != FP_ACTIVE) return;
    c->fp_state = state;
    epoll_ctl(ioThreadEpollFd(c->io_tid), EPOLL_CTL_DEL, c->conn->fd, NULL);
    listAddNodeTail(t->leaving, c);
}

static void fpAppendEntry(cmdBatch *b, client *c, robj **argv, int argc, int argv_len, size_t argv_len_sum,
                          unsigned long long input_bytes, struct serverCommand *cmd, int slot, int read_flags) {
    cmdEntry *e = &b->e[b->count++];
    e->io_client = c;
    e->argv = argv;
    e->argc = argc;
    e->argv_len = argv_len;
    e->argv_len_sum = argv_len_sum;
    e->input_bytes = input_bytes;
    e->cmd = cmd;
    e->slot = slot;
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
    }

    while (!leave && q->off < q->len) {
        parsedCommand *p = &q->cmds[q->off];
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

void fastpathClientReadable(int tid, client *c) {
    fpThread *t = &fp_threads[tid];
    if (c->fp_state != FP_ACTIVE) return;
    /* Backpressure: the batch pipeline is deep enough; the socket stays
     * readable (level triggered) and is served on a later pass. */
    if (t->inflight >= server.io_batch_inflight || c->fp_inflight >= FP_CLIENT_INFLIGHT_MAX) return;

    c->read_flags = 0; /* authenticated at admission, not replicated; parse state lives in multibulklen/bulklen */
    readToQueryBuf(c);
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
    parseInputBuffer(c);
    prepareCommandQueue(c);
    fpHarvest(t, tid, c);
    trimClientQueryBuffer(c);
}

static void fpEnableWriteInterest(client *c, int on) {
    struct epoll_event ev = {.events = on ? (EPOLLIN | EPOLLOUT) : EPOLLIN, .data.ptr = c};
    epoll_ctl(ioThreadEpollFd(c->io_tid), EPOLL_CTL_MOD, c->conn->fd, &ev);
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
    return 1;
}

/* Buffered bytes always precede newly returned replies. */
static void fpSend(fpThread *t, client *c, struct iovec *iov, int iovcnt) {
    if (c->fp_state == FP_CLOSING) return;
    if (c->fp_out && sdslen(c->fp_out) > 0) {
        for (int i = 0; i < iovcnt; i++) c->fp_out = sdscatlen(c->fp_out, iov[i].iov_base, iov[i].iov_len);
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
    if (c->fp_out && sdslen(c->fp_out) > 0) fpEnableWriteInterest(c, 1);
}

void fastpathClientWritable(int tid, client *c) {
    fpThread *t = &fp_threads[tid];
    if (fpFlushOut(t, c)) fpEnableWriteInterest(c, 0);
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
        client *c = b->e[i].io_client;
        int n = 0;
        int j = i;
        int requeued = 0;
        while (j < b->count && b->e[j].io_client == c) {
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

/* Clients that stopped reading and now have nothing in flight are handed to
 * the main thread: to be freed (closing) or taken over (leaving). Buffered
 * output goes out first. */
static void fpFinishLeaving(fpThread *t) {
    if (listLength(t->leaving) == 0) return;
    listIter li;
    listNode *ln;
    listRewind(t->leaving, &li);
    while ((ln = listNext(&li))) {
        client *c = listNodeValue(ln);
        if (c->fp_inflight > 0) continue;
        if (c->fp_state == FP_LEAVING && !fpFlushOut(t, c)) continue; /* still draining output */
        listDelNode(t->leaving, ln);
        sendToMainThread(c, c->fp_state == FP_CLOSING ? JOB_RES_FP_CLOSE : JOB_RES_FP_HANDOFF);
    }
}

int fastpathProcessReturns(int tid) {
    fpThread *t = &fp_threads[tid];
    if (t->ret.buffer == NULL) return 0;
    void *items[16];
    int total = 0;
    size_t n;
    while ((n = spscDequeueBatch(&t->ret, items, 16)) > 0) {
        for (size_t i = 0; i < n; i++) {
            uintptr_t v = (uintptr_t)items[i];
            if (v & FP_TAG_DETACH) {
                fpBeginLeave(t, (client *)(v & ~FP_TAG_DETACH), FP_CLOSING);
                continue;
            }
            cmdBatch *b = (cmdBatch *)v;
            fpDeliverBatch(t, b);
            fpRecycleBatch(t, b);
            t->inflight--;
        }
        total += (int)n;
    }
    fpFinishLeaving(t);
    return total;
}

static client *fpExecutor(int tid) {
    client *ec = fp_exec_client[tid];
    if (ec) return ec;
    ec = createClient(NULL);
    ec->flag.executor = 1;
    ec->cur_tid = tid; /* argv frees are offloaded to the owning thread */
    fp_exec_client[tid] = ec;
    return ec;
}

/* Cookies of clients main asked to close; their queued commands must not run, as with close_asap. */
static rax *fp_detaching = NULL;

static int fpClientDetaching(client *io_client) {
    return fp_detaching && raxSize(fp_detaching) > 0 &&
           raxFind(fp_detaching, (unsigned char *)&io_client, sizeof(io_client), NULL);
}

/* The executor borrows argv and writes replies into the batch arena. */
static void fpExecute(client *ec, cmdBatch *b, cmdEntry *e) {
    char *saved_buf = ec->buf;
    size_t saved_usable = ec->buf_usable_size;
    user *principal = e->origin.principal;

    if (fpClientDetaching(e->io_client)) goto release_argv; /* no reply: the IO thread is closing it */
    if (isPausedActions(PAUSE_ACTION_CLIENT_ALL | PAUSE_ACTION_CLIENT_WRITE)) {
        e->requeued = 1; /* the main path postpones it like any other client's command */
        return;
    }

    if (principal->flags & USER_FLAG_RETIRED) principal = ACLResolveUser(principal);
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
    ec->db = e->db;
    ec->resp = e->resp;
    ec->origin = &e->origin;
    ec->buf = b->arena + b->arena_used;
    ec->buf_usable_size = b->arena_cap - b->arena_used;
    ec->bufpos = 0;
    ec->flag.buf_encoded = 0;
    ec->flag.pending_command = 1;

    processCommandAndResetClient(ec);

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

int fastpathDrain(void) {
    int total = 0;
    int use_prefetch = prefetchBatchEnabled() && !ProcessingEventsWhileBlocked;
    /* While clients are paused every entry is handed back for the main path to postpone. */
    int paused = isPausedActions(PAUSE_ACTION_CLIENT_ALL | PAUSE_ACTION_CLIENT_WRITE);
    /* io-batch-drain-us bounds how long a thin batch waits for amortization. */
    monotime deadline = server.io_batch_drain_us > 0 ? getMonotonicUs() + server.io_batch_drain_us : 0;
    int enough = server.io_batch_commands * 4;
again:
    for (int tid = 1; tid < server.io_threads_num; tid++) {
        fpThread *t = &fp_threads[tid];
        if (t->submit.buffer == NULL) continue;
        void *items[8];
        size_t n = spscDequeueBatch(&t->submit, items, 8);
        if (n == 0) continue;
        client *ec = fpExecutor(tid);
        for (size_t i = 0; i < n; i++) {
            cmdBatch *b = items[i];
            if (use_prefetch && !paused) {
                getKeysResult result;
                initGetKeysResult(&result);
                int room = 1;
                for (int k = 0; k < b->count && room; k++) {
                    cmdEntry *e = &b->e[k];
                    if (!e->cmd || (e->read_flags & READ_FLAGS_BAD_ARITY)) continue;
                    room = prefetchBatchAddCommand(e->cmd, e->argv, e->argc, e->db, e->slot, &result);
                }
                getKeysFreeResult(&result);
                prefetchBatchRun();
                prefetchBatchReset();
            }
            for (int k = 0; k < b->count; k++) fpExecute(ec, b, &b->e[k]);
            ec->origin = NULL;
            clientSetUser(ec, DefaultUser, 0); /* never keep a principal that may retire */
            total += b->count;
            spscEnqueue(&t->ret, b, false);
        }
        spscCommit(&t->ret);
    }
    if (deadline && total < enough && getMonotonicUs() < deadline) goto again;
    return total;
}

/* Main requests detach through the IO-owned return ring. */
void fastpathRequestDetach(client *c) {
    fpThread *t = &fp_threads[c->io_tid];
    if (c->flag.fp_detach_sent) return;
    c->flag.fp_detach_sent = 1;
    if (!fp_detaching) fp_detaching = raxNew();
    raxInsert(fp_detaching, (unsigned char *)&c, sizeof(c), NULL, NULL);
    spscEnqueue(&t->ret, (void *)((uintptr_t)c | FP_TAG_DETACH), true);
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
    c->flag.fastpath = 0;
    c->fp_state = FP_DETACHED;
    fastpath_clients--;
    if (c->flag.fp_detach_sent) {
        c->flag.fp_detach_sent = 0;
        raxRemove(fp_detaching, (unsigned char *)&c, sizeof(c), NULL);
    }
    ACLFastpathClientReturned(c);
    if (c->fp_out) {
        sdsfree(c->fp_out);
        c->fp_out = NULL;
    }
    if (closing || c->flag.close_asap) {
        freeClient(c); /* removes it from clients_to_close itself when close_asap is set */
        return;
    }
    connSetReadHandler(c->conn, readQueryFromClient);
    int authenticating = c->parsed_cmd && (c->parsed_cmd->proc == authCommand || c->parsed_cmd->proc == helloCommand);
    if (c->argc > 0 && (c->read_flags & READ_FLAGS_PARSING_COMPLETED) && !(c->read_flags & READ_FLAGS_ERROR_MASK))
        c->flag.pending_command = 1;
    if (c->read_flags & READ_FLAGS_ERROR_MASK) {
        handleParseError(c); /* replies, sets close_after_reply; the write path closes it */
        return;
    }
    if (processPendingCommandAndInputBuffer(c) != C_OK) return;
    if (authenticating || c->flag.fp_readmit) {
        c->flag.fp_readmit = 0;
        if (fpReadmit(c)) return;
    }
    beforeNextClient(c);
}

void fastpathInfo(sds *info) {
    long long reads = 0, in = 0, out = 0, writes = 0, batches = 0;
    for (int i = 1; i < server.io_threads_num; i++) {
        reads += fp_threads[i].reads;
        in += fp_threads[i].net_input_bytes;
        out += fp_threads[i].net_output_bytes;
        writes += fp_threads[i].writes;
        batches += fp_threads[i].batches;
    }
    *info = sdscatprintf(*info,
                         "fastpath_clients:%zu\r\n"
                         "fastpath_reads:%lld\r\n"
                         "fastpath_writes:%lld\r\n"
                         "fastpath_batches:%lld\r\n"
                         "fastpath_net_input_bytes:%lld\r\n"
                         "fastpath_net_output_bytes:%lld\r\n",
                         fastpath_clients, reads, writes, batches, in, out);
}
