/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Parallel apply of the replication stream on a replica. See repl_apply.h. */

#include "server.h"
#include "repl_apply.h"
#include "staged_loop.h"
#include "fastpath.h"
#include "bgiteration.h"
#include "hotkeys.h"
#include "throttle.h"
#include "module.h"


_Thread_local int repl_apply_in_lane = 0;
_Thread_local const char *repl_apply_key = NULL;
_Thread_local int repl_apply_shard = 0;

typedef struct {
    serverDb *db;
    robj **argv;
    int argc;
    int shard;
    long long pxat; /* -1: no expire */
} replApplyOp;

typedef struct {
    replApplyOp *ops;
    int len;
    int cap;
    monotime usec;
} replApplyLane;

static replApplyLane ra_lanes[IO_THREADS_MAX_NUM];
static int ra_nlanes = 0; /* lanes of the held batch; fixed until it is applied */
static int ra_pending = 0;
static int ra_main_lane = 1; /* lane 0 runs on main; otherwise lane i runs on IO thread i + 1 */
static struct serverCommand *ra_set_cmd = NULL;

/* The keyspace is split into tables: by slot in cluster mode, by keyspace-shard-bits otherwise. Lanes
 * apply writes to disjoint tables. */
static int replApplySharded(void) {
    return server.cluster_enabled || server.keyspace_shard_bits;
}

/* ---------------------------------------------------------------------------------------------
 * The primary's stream on the write lanes.
 *
 * A planned read of the primary (READ_FLAGS_STREAM_LANES) is parsed in two steps. In the read phase the IO
 * thread that reads it parses the first command as usual and scans the rest for command boundaries, cutting
 * the complete commands into up to one chunk per staged thread and noting, at each cut, the database and
 * transaction state the commands before it leave. In the write phase main opens a PARSE phase in which
 * every staged thread parses its chunk into its slice of the client's command queue and classifies each
 * command, in stream order (T): a write lane by the shard of its keys, with the lane commands and key checks
 * of a primary's write lanes, or a barrier. Main then runs T segment by segment: a segment's lane commands
 * in an APPLY phase, one lane per staged thread, and the barrier that ends it on main, as the primary client
 * runs it. After a segment main moves the replication offset past it and proxies its bytes to the backlog
 * and the sub-replicas, so nothing is acknowledged or forwarded before it is applied. Whatever main does not
 * run this way is parsed and run the usual way.
 * --------------------------------------------------------------------------------------------- */

#define RP_STOP 2                /* rpRunOnMain: the client cannot take its next command now */
#define RP_MAX_CMDS 65000        /* commands of one planned read: the command queue holds 65535 */
#define RP_MIN_CHUNK (16 * 1024) /* bytes; a read with fewer is one chunk, parsed by its IO thread */
#define RP_IDLE 0
#define RP_PARSE 1 /* the phase running is the plan's PARSE phase */
#define RP_APPLY 2 /* ... or one of its APPLY phases */

#define RP_GROW(arr, n, cap, init)                                   \
    do {                                                             \
        if ((n) == (cap)) {                                          \
            (cap) = (cap) ? (cap) * 2 : (init);                      \
            (arr) = zrealloc((arr), sizeof(*(arr)) * (size_t)(cap)); \
        }                                                            \
    } while (0)

#define RP_NOOP (-2) /* rpEntry.lane: consumed with its segment, run by nobody (a transparent MULTI or EXEC) */

/* One lane's share of a multi-key write whose keys belong to several lanes: the command name and the
 * key (and value) arguments of that lane's keys, run by the lane as the primary ran its own part. */
typedef struct rpPiece {
    robj **argv; /* NULL once a lane ran it (commandProcessed freed it) */
    int argc;
    size_t argv_len_sum;
    int lane;
} rpPiece;

typedef struct rpEntry {
    size_t qb_end; /* querybuf offset just past the command */
    serverDb *db;
    int shard;    /* lane entries: the keyspace hashtable of all its keys; -1: each key finds its own */
    int lane;     /* -1: a barrier; RP_NOOP: nothing to run */
    uint32_t seg; /* barriers before it in its chunk */
    rpPiece *pieces; /* a split entry: its pieces, each on its lane's vector in this entry's place */
    int npieces;
} rpEntry;

/* A lane vector element: the plan position, and for a split entry which piece of it. */
#define RP_LV(idx, piece) ((int)((idx) | ((piece) << 16)))
#define RP_LV_IDX(v) ((v) & 0xFFFF)
#define RP_LV_PIECE(v) ((v) >> 16)

/* What a command leaves for the next ones' classification. */
typedef struct rpState {
    serverDb *db;
    int in_multi;
    int stop; /* the rest of the read are barriers */
} rpState;

/* Plan positions [first, first + n): written by the one thread that parses the chunk. */
typedef struct __attribute__((aligned(64))) rpChunk {
    size_t start; /* querybuf offset of its first command */
    int first, n;
    rpState st;                  /* before its first command; while parsing, the current state */
    int *lane_idx[FP_MAX_LANES]; /* per lane: plan positions of its lane commands, in T order */
    int lane_n[FP_MAX_LANES], lane_cap[FP_MAX_LANES];
    int *bpos; /* plan positions of its barriers */
    int nb, bcap;
    int splits, txs; /* split entries and transparent transactions it classified; folded into the stats by main */
    uint32_t base; /* barriers of the chunks before it */
} rpChunk;

/* Lane l's place in the plan: a chunk and a position in that chunk's vector for l. */
typedef struct __attribute__((aligned(64))) rpCursor {
    int chunk, pos;
} rpCursor;

static struct {
    client *c;             /* the primary client of the plan */
    int ready;             /* built by the IO thread, not yet run by main */
    int parsed;            /* the chunks are parsed and classified */
    int mode;              /* RP_PARSE or RP_APPLY while a phase runs the plan */
    int nlanes, main_lane; /* main sets them before the read */
    rpChunk ch[FP_MAX_LANES];
    int nchunks;
    rpEntry *e; /* plan positions: 0 is c->argv, k >= 1 the command queue's slot k - 1 */
    int n, cap;
    int *bpos; /* plan positions of all barriers, in T order */
    int nb, bcap;
    size_t scan_end;                /* querybuf offset past the last command of the plan */
    uint32_t seg;                   /* the segment an APPLY phase runs */
    long long read_us, classify_us; /* IO thread: the read job, and the scan and planning in it */
} rp;

static rpCursor rp_cur[FP_MAX_LANES];
static robj *rp_mset_name = NULL; /* shared: pieces of one MSET run on several lanes at once */

int replApplyPlanBegin(void) {
    if (!server.replica_parallel_apply || !stagedApplyReady() || !fastpathStreamLanesAllowed()) return 0;
    int main_lane = stagedMainTakesLane();
    int n = stagedWorkerCount() + main_lane;
    if (n > FP_MAX_LANES) n = FP_MAX_LANES;
    if (n < 2) return 0;
    rp.nlanes = n;
    rp.main_lane = main_lane;
    if (!rp_mset_name) rp_mset_name = fastpathPartNameMset();
    return 1;
}

void replApplyPlanReset(void) {
    rp.ready = 0;
    rp.read_us = rp.classify_us = 0;
}

void replApplyPlanReadTime(long long us) {
    rp.read_us = us;
}

static int rpNameIs(const char *s, size_t len, const char *name) {
    return len == strlen(name) && !strncasecmp(s, name, len);
}

/* After a barrier named name (first argument arg): MULTI, EXEC and DISCARD open and close a transaction, and
 * SELECT moves to a database. A database not created yet is created when main runs the SELECT, so the rest of
 * the read waits for it. */
static void rpTrack(rpState *st, const char *name, size_t nlen, const char *arg, size_t alen, int argc) {
    if (nlen < 4 || nlen > 7) return;
    if (rpNameIs(name, nlen, "multi")) {
        st->in_multi = 1;
    } else if (rpNameIs(name, nlen, "exec") || rpNameIs(name, nlen, "discard")) {
        st->in_multi = 0;
    } else if (rpNameIs(name, nlen, "select")) {
        long long id;
        serverDb *db = NULL;
        if (!st->in_multi && argc == 2 && string2ll(arg, alen, &id) && id >= 0 && id < server.dbnum)
            db = server.db[id];
        if (db)
            st->db = db;
        else
            st->stop = 1;
    }
}

#define RP_SPLIT (-3) /* rpLaneOf: the entry's keys belong to several lanes; its pieces are built */

static void rpFreePieces(rpEntry *e) {
    for (int p = 0; p < e->npieces; p++) {
        rpPiece *pc = &e->pieces[p];
        if (!pc->argv) continue; /* a lane ran it */
        for (int j = 0; j < pc->argc; j++) decrRefCount(pc->argv[j]);
        zfree(pc->argv);
        pc->argv = NULL;
    }
    zfree(e->pieces);
    e->pieces = NULL;
    e->npieces = 0;
}

/* An MSET (standalone, keyspace shards on) whose keys are in several hashtables: one lane when every key
 * is that lane's, as the primary's part of the same command; otherwise one piece per lane with those
 * keys and values, stored in e. Pieces hold their own references to the arguments. */
static int rpSplit(rpChunk *k, rpEntry *e, struct serverCommand *cmd, robj **argv, int argc) {
    if (server.cluster_enabled || !server.keyspace_shard_bits || cmd->proc != msetCommand) return -1;
    if (argc < 5 || (argc - 1) % 2) return -1; /* one key, or the arity error, is main's */
    int nkeys = (argc - 1) / 2;
    int count[FP_MAX_LANES];
    int8_t piece_of[FP_MAX_LANES];
    memset(count, 0, sizeof(int) * (size_t)rp.nlanes);
    memset(piece_of, -1, sizeof(piece_of));
    int lanes[FP_MAX_LANES];
    int npieces = 0;
    int8_t *keypiece = zmalloc((size_t)nkeys);
    for (int i = 0; i < nkeys; i++) {
        robj *key = argv[1 + i * 2];
        if (fastpathKeyObserved(k->st.db, key)) {
            zfree(keypiece);
            return -1;
        }
        int lane = getKVStoreIndexForKey(objectGetVal(key)) % rp.nlanes;
        if (piece_of[lane] < 0) {
            piece_of[lane] = (int8_t)npieces;
            lanes[npieces++] = lane;
        }
        count[lane]++;
        keypiece[i] = piece_of[lane];
    }
    e->shard = -1; /* each key finds its own hashtable */
    if (npieces == 1) {
        zfree(keypiece);
        return lanes[0];
    }
    e->pieces = zmalloc(sizeof(rpPiece) * (size_t)npieces);
    e->npieces = npieces;
    for (int p = 0; p < npieces; p++) {
        rpPiece *pc = &e->pieces[p];
        pc->argv = zmalloc(sizeof(robj *) * (size_t)(1 + count[lanes[p]] * 2));
        pc->argv[0] = rp_mset_name; /* shared: decrRefCount leaves it alone */
        pc->argc = 1;
        pc->argv_len_sum = 4;
        pc->lane = lanes[p];
    }
    for (int i = 0; i < nkeys; i++) {
        rpPiece *pc = &e->pieces[keypiece[i]];
        for (int j = 0; j < 2; j++) {
            robj *o = argv[1 + i * 2 + j];
            pc->argv[pc->argc++] = o;
            incrRefCount(o);
            pc->argv_len_sum += sdsEncodedObject(o) ? sdslen(objectGetVal(o)) : 0;
        }
    }
    zfree(keypiece);
    return RP_SPLIT;
}

/* The lane of a lane-eligible command, RP_SPLIT with its pieces built in e, or -1 for a barrier. Leaves the
 * chunk's state alone. */
static int rpLaneOf(rpChunk *k, rpEntry *e, struct serverCommand *cmd, robj **argv, int argc, int read_flags, int slot) {
    if (k->st.stop || !cmd || !cmd->write_lane || (read_flags & (READ_FLAGS_COMMAND_NOT_FOUND | READ_FLAGS_BAD_ARITY)))
        return -1;
    int shard = fastpathKeysShard(k->st.db, cmd, argv, argc, slot);
    if (shard >= 0) {
        e->shard = shard;
        return shard % rp.nlanes;
    }
    return rpSplit(k, e, cmd, argv, argc);
}

static void rpInitEntry(rpChunk *k, int idx, size_t qb_end) {
    rpEntry *e = &rp.e[idx];
    e->qb_end = qb_end;
    e->db = k->st.db;
    e->shard = -1;
    e->lane = -1;
    e->seg = (uint32_t)k->nb;
    e->pieces = NULL;
    e->npieces = 0;
}

static void rpPushLane(rpChunk *k, int lane, int v) {
    RP_GROW(k->lane_idx[lane], k->lane_n[lane], k->lane_cap[lane], 256);
    k->lane_idx[lane][k->lane_n[lane]++] = v;
}

/* Put the entry at idx on its lane, or each of its pieces on theirs. */
static void rpCommitLane(rpChunk *k, int idx, int lane) {
    rpEntry *e = &rp.e[idx];
    if (lane == RP_SPLIT) {
        e->lane = e->pieces[0].lane;
        for (int p = 0; p < e->npieces; p++) rpPushLane(k, e->pieces[p].lane, RP_LV(idx, p));
        k->splits++;
    } else {
        e->lane = lane;
        rpPushLane(k, lane, RP_LV(idx, 0));
    }
}

static void rpCommitBarrier(rpChunk *k, int idx, robj **argv, int argc) {
    RP_GROW(k->bpos, k->nb, k->bcap, 16);
    k->bpos[k->nb++] = idx;
    if (argc > 0 && sdsEncodedObject(argv[0])) {
        int a = argc > 1 && sdsEncodedObject(argv[1]);
        rpTrack(&k->st, objectGetVal(argv[0]), sdslen(objectGetVal(argv[0])), a ? objectGetVal(argv[1]) : NULL,
                a ? sdslen(objectGetVal(argv[1])) : 0, argc);
    }
}

/* The lane of the command at plan position idx, or a barrier. */
static void rpClassify(rpChunk *k, int idx, struct serverCommand *cmd, robj **argv, int argc, int read_flags, int slot, size_t qb_end) {
    rpInitEntry(k, idx, qb_end);
    int lane = k->st.in_multi ? -1 : rpLaneOf(k, &rp.e[idx], cmd, argv, argc, read_flags, slot);
    if (lane != -1)
        rpCommitLane(k, idx, lane);
    else
        rpCommitBarrier(k, idx, argv, argc);
}

/* A RESP integer ending at the next \r\n of [p, end). Returns the position after the \n, or 0 when the
 * line is not complete or not a number. */
static size_t rpScanInt(const char *qb, size_t p, size_t end, long long *v) {
    size_t q = p;
    while (q < end && qb[q] != '\r') q++;
    if (q + 1 >= end || qb[q + 1] != '\n') return 0;
    if (!string2ll(qb + p, q - p, v)) return 0;
    return q + 2;
}

/* IO thread: cut the complete multibulk commands from qb_pos on into chunks of about equal bytes, carrying
 * the classification state across each cut. Stops at a partial command, or at anything the parser should
 * judge. Returns the number of commands. */
static int rpScan(client *c, rpState st) {
    const char *qb = c->querybuf;
    size_t end = sdslen(c->querybuf), p = c->qb_pos;
    size_t target = (end - p) / (size_t)rp.nlanes + 1;
    if (target < RP_MIN_CHUNK) target = RP_MIN_CHUNK;
    int total = 0, t = 0;
    rpChunk *k = &rp.ch[0];
    k->start = p;
    k->n = 0;
    while (p < end && total < RP_MAX_CMDS) {
        long long n, len;
        if (qb[p] != '*') break;
        size_t q = rpScanInt(qb, p + 1, end, &n);
        if (!q || n <= 0 || n > 1024 * 1024) break;
        const char *name = NULL, *arg = NULL;
        size_t nlen = 0, alen = 0;
        long long j;
        for (j = 0; j < n; j++) {
            if (q >= end || qb[q] != '$') break;
            size_t d = rpScanInt(qb, q + 1, end, &len);
            if (!d || len < 0 || d + (size_t)len + 2 > end || qb[d + len] != '\r' || qb[d + len + 1] != '\n') break;
            if (j == 0) name = qb + d, nlen = (size_t)len;
            if (j == 1) arg = qb + d, alen = (size_t)len;
            q = d + (size_t)len + 2;
        }
        if (j < n) break;
        /* A cut inside a transaction would hide its end from the chunk that classifies it. */
        if (k->n > 0 && p - k->start >= target && t + 1 < rp.nlanes && !st.in_multi) {
            t++;
            k = &rp.ch[t];
            k->start = p;
            k->first = 1 + total;
            k->n = 0;
            k->st = st;
        }
        k->n++;
        total++;
        rpTrack(&st, name, nlen, arg, alen, (int)n);
        p = q;
    }
    rp.nchunks = t + 1;
    rp.scan_end = p;
    return total;
}

/* Parse the command the scan found at offset p into pc. Returns its length. */
static size_t rpParseOne(const char *qb, size_t p, parsedCommand *pc) {
    long long n, len;
    size_t q = rpScanInt(qb, p + 1, SIZE_MAX, &n);
    pc->argc = (int)n;
    pc->argv_len = (int)n;
    pc->argv = zmalloc(sizeof(robj *) * (size_t)n);
    pc->argv_len_sum = 0;
    for (int j = 0; j < n; j++) {
        size_t d = rpScanInt(qb, q + 1, SIZE_MAX, &len);
        pc->argv[j] = createStringObject(qb + d, (size_t)len);
        pc->argv_len_sum += (size_t)len;
        q = d + (size_t)len + 2;
    }
    pc->input_bytes = q - p;
    pc->read_flags = READ_FLAGS_PARSING_COMPLETED;
    pc->cmd = NULL;
    pc->slot = -1;
    prepareParsedCommand(pc);
    return q - p;
}

static int rpCmdIs(parsedCommand *pc, void (*proc)(client *)) {
    return pc->cmd && pc->cmd->proc == proc && !(pc->read_flags & (READ_FLAGS_COMMAND_NOT_FOUND | READ_FLAGS_BAD_ARITY));
}

/* Chunk position i holds a MULTI outside a transaction. When the EXEC that closes it is in this chunk and
 * every command between them has a lane, the transaction is transparent: MULTI and EXEC are consumed with
 * the body's segment and nothing runs them, and the body runs on the lanes. The body is one segment, so
 * it lands in one APPLY phase, which no reader observes part-way: the primary's atomicity is kept without
 * main running the pair. Returns the chunk position of the EXEC, or i when the transaction stays as it is
 * (a barrier MULTI, whose body the usual classification makes barriers). */
static int rpTryTransparentTx(rpChunk *k, int i) {
    client *c = rp.c;
    int j;
    for (j = i + 1; j < k->n; j++) {
        int idx = k->first + j;
        parsedCommand *pc = &c->cmd_queue.cmds[idx - 1];
        if (rpCmdIs(pc, execCommand)) break;
        if (rpCmdIs(pc, multiCommand) || rpCmdIs(pc, discardCommand)) goto fail;
        rpEntry *e = &rp.e[idx];
        rpInitEntry(k, idx, e->qb_end);
        int lane = rpLaneOf(k, e, pc->cmd, pc->argv, pc->argc, pc->read_flags, pc->slot);
        if (lane == -1) goto fail;
        e->lane = lane; /* RP_SPLIT until committed */
    }
    if (j == k->n) goto fail; /* the EXEC is not in this chunk */
    for (int m = i; m <= j; m++) {
        int idx = k->first + m;
        if (m == i || m == j) {
            rpInitEntry(k, idx, rp.e[idx].qb_end);
            rp.e[idx].lane = RP_NOOP;
        } else {
            rpCommitLane(k, idx, rp.e[idx].lane);
        }
    }
    k->txs++;
    return j;
fail:
    for (int m = i + 1; m < j; m++) rpFreePieces(&rp.e[k->first + m]);
    return i;
}

/* Thread of lane t: parse and classify chunk t into its slice of the command queue. */
static void rpParseChunk(int t) {
    rpChunk *k = &rp.ch[t];
    client *c = rp.c;
    size_t p = k->start;
    for (int i = 0; i < k->n; i++) {
        int idx = k->first + i;
        p += rpParseOne(c->querybuf, p, &c->cmd_queue.cmds[idx - 1]);
        rp.e[idx].qb_end = p;
    }
    for (int i = 0; i < k->n; i++) {
        int idx = k->first + i;
        parsedCommand *pc = &c->cmd_queue.cmds[idx - 1];
        if (!k->st.in_multi && !k->st.stop && rpCmdIs(pc, multiCommand)) {
            int j = rpTryTransparentTx(k, i);
            if (j > i) {
                i = j;
                continue;
            }
        }
        rpClassify(k, idx, pc->cmd, pc->argv, pc->argc, pc->read_flags, pc->slot, rp.e[idx].qb_end);
    }
}

static void rpResetChunk(rpChunk *k) {
    for (int l = 0; l < rp.nlanes; l++) k->lane_n[l] = 0;
    k->nb = 0;
    k->splits = k->txs = 0;
}

void replApplyPlanRead(client *c) {
    rp.ready = 0;
    if (!(c->read_flags & READ_FLAGS_PARSING_COMPLETED) || c->argc == 0 || (c->read_flags & READ_FLAGS_ERROR_MASK))
        return;
    monotime start = getMonotonicUs();
    rp.c = c;
    rp.parsed = 0;
    for (int t = 0; t < rp.nlanes; t++) rpResetChunk(&rp.ch[t]);
    rpChunk *k0 = &rp.ch[0];
    k0->first = 1;
    k0->st = (rpState){.db = c->db, .in_multi = c->flag.multi, .stop = 0};
    if (!rp.cap) {
        rp.cap = 1024;
        rp.e = zmalloc(sizeof(rpEntry) * (size_t)rp.cap);
    }
    rpClassify(k0, 0, c->parsed_cmd, c->argv, c->argc, c->read_flags, c->slot, c->qb_applied);
    int total = rpScan(c, k0->st);
    rp.n = 1 + total;
    if (rp.cap < rp.n) {
        rp.cap = rp.n;
        rp.e = zrealloc(rp.e, sizeof(rpEntry) * (size_t)rp.cap);
    }
    cmdQueue *q = &c->cmd_queue;
    serverAssert(q->len == 0);
    if (q->cap < total) {
        q->cmds = zrealloc(q->cmds, sizeof(parsedCommand) * (size_t)total);
        q->cap = (uint16_t)total;
    }
    /* A small read is parsed here; chunks wait for the PARSE phase. */
    if (rp.nchunks == 1) {
        rpParseChunk(0);
        q->len = (uint16_t)total;
        q->off = 0;
        c->qb_pos = rp.scan_end;
        rp.parsed = 1;
    }
    rp.classify_us = getMonotonicUs() - start;
    rp.ready = 1;
}

/* Main, after the chunks are parsed: the barriers of all chunks in T order. */
static void rpMerge(void) {
    rp.nb = 0;
    uint32_t base = 0;
    for (int t = 0; t < rp.nchunks; t++) {
        rpChunk *k = &rp.ch[t];
        k->base = base;
        server.stat_repl_lane_splits += k->splits;
        server.stat_repl_lane_tx_transparent += k->txs;
        for (int i = 0; i < k->nb; i++) {
            RP_GROW(rp.bpos, rp.nb, rp.bcap, 64);
            rp.bpos[rp.nb++] = k->bpos[i];
        }
        base += (uint32_t)k->nb;
    }
    for (int l = 0; l < rp.nlanes; l++) rp_cur[l] = (rpCursor){0, 0};
}

/* Lane thread, APPLY phase: apply this lane's commands of the current segment, chunk after chunk. */
static void rpRunLane(int lane) {
    client *c = rp.c;
    rpCursor *cur = &rp_cur[lane];
    int entered = 0;
    while (cur->chunk < rp.nchunks) {
        rpChunk *k = &rp.ch[cur->chunk];
        int n = k->lane_n[lane];
        while (cur->pos < n) {
            int v = k->lane_idx[lane][cur->pos];
            int idx = RP_LV_IDX(v);
            rpEntry *e = &rp.e[idx];
            uint32_t seg = k->base + e->seg;
            if (seg > rp.seg) goto done;
            cur->pos++;
            if (seg < rp.seg) continue; /* its segment ran on main */
            if (!entered) {
                fastpathStreamLaneEnter(lane);
                entered = 1;
            }
            if (e->npieces) {
                int pi = RP_LV_PIECE(v);
                rpPiece *pc = &e->pieces[pi];
                struct serverCommand *cmd = idx == 0 ? c->parsed_cmd : c->cmd_queue.cmds[idx - 1].cmd;
                unsigned long long bytes = idx == 0 ? c->net_input_bytes_curr_cmd : c->cmd_queue.cmds[idx - 1].input_bytes;
                fastpathStreamLaneCall(lane, e->db, cmd, pc->argv, pc->argc, pc->argc, pc->argv_len_sum,
                                       pi == 0 ? bytes : 0, -1);
                pc->argv = NULL; /* commandProcessed freed it */
            } else if (idx == 0) {
                fastpathStreamLaneCall(lane, e->db, c->parsed_cmd, c->argv, c->argc, c->argv_len, c->argv_len_sum,
                                       c->net_input_bytes_curr_cmd, e->shard);
            } else {
                parsedCommand *p = &c->cmd_queue.cmds[idx - 1];
                fastpathStreamLaneCall(lane, e->db, p->cmd, p->argv, p->argc, p->argv_len, p->argv_len_sum,
                                       p->input_bytes, e->shard);
            }
        }
        cur->chunk++;
        cur->pos = 0;
    }
done:
    if (entered) fastpathStreamLaneLeave();
}

/* Staged thread tid (0 = main) in a phase of the plan. */
static void rpPhase(int tid) {
    int lane = rp.main_lane ? tid : tid - 1;
    if (lane < 0 || lane >= rp.nlanes) return;
    if (rp.mode == RP_PARSE) {
        if (lane < rp.nchunks) rpParseChunk(lane);
    } else {
        rpRunLane(lane);
    }
}

/* Main: the stream is applied up to qb_applied. Advance the replication offset and proxy the bytes to the
 * backlog and the sub-replicas, as commandProcessed does after a command. */
static void rpAdvance(client *c) {
    long long prev = c->repl_data->reploff;
    c->repl_data->reploff = c->repl_data->read_reploff - sdslen(c->querybuf) + c->qb_applied;
    serverAssert(c->repl_data->reploff > prev);
    long long applied = c->repl_data->reploff - prev;
    replicationFeedStreamFromPrimaryStream(c->querybuf + c->repl_data->repl_applied, applied);
    c->repl_data->repl_applied += applied;
}

/* Main: the lanes applied the commands at positions [from, to); take them off the client. */
static void rpConsume(client *c, int from, int to) {
    if (from == 0) {
        rpEntry *e0 = &rp.e[0];
        if (e0->npieces) {
            /* The lanes ran its pieces; the command itself nobody freed. */
            for (int j = 0; j < c->argc; j++) decrRefCount(c->argv[j]);
            zfree(c->argv);
            rpFreePieces(e0);
        }
        /* The lane freed the arguments of the client's current command. */
        c->argv = NULL;
        c->argc = 0;
        c->argv_len = 0;
        c->argv_len_sum = 0;
        c->parsed_cmd = NULL;
        c->slot = -1;
        c->net_input_bytes_curr_cmd = 0;
    }
    if (to > 1) {
        cmdQueue *q = &c->cmd_queue;
        /* A split entry's lanes ran its pieces, and nothing runs a transparent MULTI or EXEC: free the
         * command as the lane would have. */
        for (int i = from > 0 ? from : 1; i < to; i++) {
            rpEntry *e = &rp.e[i];
            if (!e->npieces && e->lane != RP_NOOP) continue;
            parsedCommand *p = &q->cmds[i - 1];
            for (int j = 0; j < p->argc; j++) decrRefCount(p->argv[j]);
            zfree(p->argv);
            p->argv = NULL;
            p->argc = 0;
            if (e->npieces) rpFreePieces(e);
        }
        q->off = (uint16_t)(to - 1);
        if (q->off == q->len) q->off = q->len = 0;
    }
    c->qb_applied = rp.e[to - 1].qb_end;
    c->commands_processed += to - from;
    rpAdvance(c);
}

/* Main: the client is where the plan expects it before position idx. A barrier that discarded the queue or
 * replaced the link leaves nothing of the plan to run. */
static int rpQueueAt(client *c, int idx, int qlen) {
    if (server.primary != c || c->cmd_queue.len != qlen) return 0;
    if (idx == 0) return c->argc > 0 && c->cmd_queue.off == 0;
    return c->argc == 0 && c->cmd_queue.off == idx - 1;
}

/* Main: run the command at position idx as the primary client, the usual way. C_ERR: the client is gone. */
static int rpRunOnMain(client *c, int idx, int qlen) {
    if (!rpQueueAt(c, idx, qlen)) return RP_STOP;
    if (rp.e[idx].npieces) rpFreePieces(&rp.e[idx]); /* main runs it whole */
    int r = processPlannedCommand(c, idx > 0);
    return r == C_OK || r == C_ERR ? r : RP_STOP;
}

/* Main: a phase of the plan on all staged threads. */
static void rpRunPhase(int mode) {
    rp.mode = mode;
    if (mode == RP_APPLY) dbLanePhaseBegin();
    stagedRunApplyPhase(1);
    if (mode == RP_APPLY) dbLanePhaseEnd();
    rp.mode = RP_IDLE;
}

static void rpApplySegment(uint32_t s, int count) {
    monotime start = getMonotonicUs();
    fastpathStreamLanesPrepare(rp.nlanes, rp.main_lane);
    rp.seg = s;
    rpRunPhase(RP_APPLY);
    fastpathStreamLanesMerge(rp.nlanes);
    server.stat_repl_parallel_applied += count;
    server.stat_repl_apply_phases++;
    server.stat_repl_lane_apply_usec += getMonotonicUs() - start;
}

/* Main: the plan, segment by segment. Commands of a segment too small for a lane phase run the usual way,
 * where plain SETs are held for one parallel apply phase. C_ERR: the client is gone. */
static int rpRunSegments(client *c) {
    int qlen = c->cmd_queue.len;
    int pos = 0;
    for (int s = 0;; s++) {
        int end = s < rp.nb ? rp.bpos[s] : rp.n;
        if (end - pos >= server.io_threads_parallel_writes_min) {
            if (!rpQueueAt(c, pos, qlen)) return C_OK;
            replApplyFlush(c); /* writes held from earlier segments land first */
            rpApplySegment((uint32_t)s, end - pos);
            rpConsume(c, pos, end);
        } else {
            /* Too few to pay for a lane phase: on main, in order; the lanes skip them. */
            for (int i = pos; i < end; i++) {
                int r = rpRunOnMain(c, i, qlen);
                if (r != C_OK) return r == C_ERR ? C_ERR : C_OK;
            }
        }
        if (s == rp.nb) break;
        server.stat_repl_lane_barriers++;
        int r = rpRunOnMain(c, end, qlen);
        if (r != C_OK) return r == C_ERR ? C_ERR : C_OK;
        pos = end + 1;
        /* A barrier may turn on something that observes writes: the rest goes the usual way. */
        if (!fastpathStreamLanesAllowed()) return C_OK;
    }
    return C_OK;
}

static int rpRun(client *c) {
    int rc = rpRunSegments(c);
    if (rc == C_OK)
        replApplyFlush(c);
    else
        serverAssert(!replApplyPending());
    /* A plan left part-way (rpQueueAt): whatever the lanes did not run goes the usual way, whole. */
    for (int i = 0; i < rp.n; i++)
        if (rp.e[i].npieces) rpFreePieces(&rp.e[i]);
    return rc;
}

int replApplyRunPlan(client *c) {
    if (!rp.ready || rp.c != c) return C_OK;
    rp.ready = 0;
    /* Lanes or threads changed since the read, or the client cannot run commands now: the usual way. */
    if (ProcessingEventsWhileBlocked || !stagedApplyReady() || !fastpathStreamLanesAllowed()) return C_OK;
    int main_lane = stagedMainTakesLane();
    int nlanes = stagedWorkerCount() + main_lane;
    if (nlanes > FP_MAX_LANES) nlanes = FP_MAX_LANES;
    if (nlanes != rp.nlanes || main_lane != rp.main_lane) return C_OK;
    if (c->argc == 0 || c->cmd_queue.off != 0 || c->flag.pending_command || !canParseCommand(c)) return C_OK;
    monotime start = getMonotonicUs();
    if (!rp.parsed) {
        monotime parse_start = getMonotonicUs();
        rpRunPhase(RP_PARSE);
        server.stat_repl_lane_parse_usec += getMonotonicUs() - parse_start;
        c->cmd_queue.len = (uint16_t)(rp.n - 1);
        c->cmd_queue.off = 0;
        c->qb_pos = rp.scan_end;
    }
    rpMerge();
    server.stat_repl_lane_reads++;
    server.stat_repl_lane_read_usec += rp.read_us;
    server.stat_repl_lane_classify_usec += rp.classify_us;
    int rc = rpRun(c);
    server.stat_repl_lane_plan_usec += getMonotonicUs() - start;
    return rc;
}

int replApplyMayDefer(void) {
    return server.replica_parallel_apply && replApplySharded() && stagedApplyReady();
}

int replApplyPending(void) {
    return ra_pending;
}

/* Nothing that observes an individual write may be active: those writes must go through call(). */
static int replApplyAllowed(client *c) {
    if (!server.replica_parallel_apply || !replApplySharded()) return 0;
    if (!c->flag.primary || c->flag.multi || !inMainThread()) return 0;
    if (!stagedApplyReady()) return 0;
    if (server.loading || server.aof_state != AOF_OFF || server.maxmemory) return 0;
    if (server.paused_actions || listLength(server.monitors) || server.tracking_clients) return 0;
    if (server.notify_keyspace_events || moduleObservesWrites()) return 0;
    if (bgIteration_iterationActive() || throttle_anyThrottlers()) return 0;
    if (dictSize(c->db->watched_keys) || dictSize(c->db->blocking_keys)) return 0;
    return 1;
}

/* SET key value, or SET key value PXAT ms: the forms a primary propagates for a plain SET. */
static int replApplyEligible(client *c, long long *pxat) {
    if (!ra_set_cmd) ra_set_cmd = lookupCommandByCString("set");
    struct serverCommand *cmd = c->parsed_cmd ? c->parsed_cmd : lookupCommand(c->argv, c->argc);
    if (cmd != ra_set_cmd) return 0;
    *pxat = -1;
    if (c->argc == 3) return 1;
    if (c->argc != 5 || strcasecmp(objectGetVal(c->argv[3]), "pxat")) return 0;
    long long ms;
    if (getLongLongFromObject(c->argv[4], &ms) != C_OK || ms <= 0) return 0;
    *pxat = ms;
    return 1;
}

int replApplyDefer(client *c) {
    long long pxat;
    if (!replApplyAllowed(c) || !replApplyEligible(c, &pxat)) return 0;
    if (ra_pending == 0) {
        ra_main_lane = stagedMainTakesLane();
        ra_nlanes = stagedWorkerCount() + ra_main_lane;
    }

    int shard = getKVStoreIndexForKey(objectGetVal(c->argv[1]));
    int lane = shard % ra_nlanes;
    replApplyLane *l = &ra_lanes[lane];
    if (l->len == l->cap) {
        l->cap = l->cap ? l->cap * 2 : 256;
        l->ops = zrealloc(l->ops, sizeof(replApplyOp) * l->cap);
    }
    l->ops[l->len++] = (replApplyOp){.db = c->db, .argv = c->argv, .argc = c->argc, .shard = shard, .pxat = pxat};
    ra_pending++;
    hotkeysRecordLookup(c->argv[1], c->db->id, LOOKUP_WRITE);

    /* The lane owns the arguments now. */
    c->argv = NULL;
    c->argc = 0;
    c->argv_len = 0;
    c->argv_len_sum = 0;
    c->commands_processed++;

    if (ra_pending >= 16384) replApplyFlush(c);
    return 1;
}

/* setGenericCommand for the eligible forms, without the reply, the dirty count and the events. */
static void replApplyOne(replApplyOp *op) {
    robj **argv = op->argv;
    robj *key = argv[1];
    robj *val = argv[2];
    repl_apply_key = objectGetVal(key);
    repl_apply_shard = op->shard;
    if (!val->hasembkey) val = tryObjectEncoding(val);
    int flags = lookupKeyWriteWithFlags(op->db, key, LOOKUP_NOHOTKEYS) ? SETKEY_ALREADY_EXIST : SETKEY_DOESNT_EXIST;
    flags |= SETKEY_NO_SIGNAL;
    if (op->pxat >= 0) flags |= SETKEY_KEEPTTL;
    setKey(NULL, op->db, key, &val, flags);
    if (op->pxat >= 0) setExpire(NULL, op->db, key, op->pxat);
    repl_apply_key = NULL;
    for (int j = 0; j < op->argc; j++)
        if (j != 2) decrRefCount(argv[j]);
    zfree(argv);
}

void replApplyRunLane(int tid) {
    if (rp.mode != RP_IDLE) {
        rpPhase(tid);
        return;
    }
    int lane = ra_main_lane ? tid : tid - 1;
    if (lane < 0 || lane >= ra_nlanes) return;
    replApplyLane *l = &ra_lanes[lane];
    if (!l->len) return;
    repl_apply_in_lane = 1;
    monotime start = getMonotonicUs();
    for (int i = 0; i < l->len; i++) replApplyOne(&l->ops[i]);
    l->usec = getMonotonicUs() - start;
    repl_apply_in_lane = 0;
}

void replApplyFlush(client *c) {
    if (!ra_pending) return;
    client *old = ectx->current_client;
    ectx->current_client = c; /* replicated writes never see a key as expired */
    dbLanePhaseBegin();
    stagedRunApplyPhase(ra_nlanes > 1 || !ra_main_lane);
    /* Fold each thread's per-table changes into the kvstore-wide bookkeeping. */
    dbLanePhaseEnd();
    ectx->current_client = old;

    monotime usec = 0;
    for (int i = 0; i < ra_nlanes; i++) {
        usec += ra_lanes[i].usec;
        ra_lanes[i].usec = 0;
        ra_lanes[i].len = 0;
    }
    ectx->dirty += ra_pending;
    ectx->stat_numcommands += ra_pending;
    server.stat_repl_parallel_applied += ra_pending;
    server.stat_repl_apply_phases++;
    ra_set_cmd->calls += ra_pending;
    ra_set_cmd->microseconds += usec;
    ra_pending = 0;
}
