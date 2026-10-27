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
#include "hotkeys.h"
#include "throttle.h"
#include "kvstore.h"
#include "cluster.h"
#include "cluster_migrateslots.h"
#include "cluster_slot_stats.h"
#include "hdr_histogram.h"
#include <sys/uio.h>
#include <sched.h>


/* arena_used (main writes it per command) and executed (a STREAM worker spins on it) must sit on
 * different cache lines, and the inline entries must start on their own line clear of executed. */
static_assert(offsetof(cmdBatch, arena_used) / CACHE_LINE_SIZE != offsetof(cmdBatch, executed) / CACHE_LINE_SIZE,
              "the polled executed must not share arena_used's cache line");
static_assert(offsetof(cmdBatch, executed) / CACHE_LINE_SIZE != offsetof(cmdBatch, e) / CACHE_LINE_SIZE,
              "the inline entries must not share the polled executed/delivered cache line");

#define FP_ARENA_SIZE (16 * 1024) /* reply bytes per batch before a slot spills to the heap */
#define FP_ARENA_MAX (256 * 1024) /* an arena grows up to this when its batch's replies spill */
#define FP_FREELIST_MAX 64

struct fpBarrier {
    cmdEntry *e;
    cmdBatch *b;
};

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
    int sb_streamed[STAGED_MAX_DEPTH]; /* STREAM/REPLY: index of the first batch not fully delivered. */
    list staged_leaving;
    /* Write lanes (depth 0). Written by this thread in the read phase: the entries it harvested per
     * lane in harvest order, its barrier entries in order, and the lane entries of each of its
     * segments. Read by the lanes and main in the write phase; reset by the next read phase. */
    cmdEntry **lane_ents[FP_MAX_LANES];
    int lane_n[FP_MAX_LANES];
    int lane_cap[FP_MAX_LANES];
    struct fpBarrier *barriers;
    int nbarriers, barriers_cap;
    int *seg_count;
    int seg_cap;
    /* Main, write phase: where serial execution of this thread's batches resumes. */
    int scur_b, scur_e;
    /* Main, write phase: the first barrier whose keys are not prefetched yet. */
    int pf_barrier;
    /* Main, merge after a lane phase: where the walk over this thread's entries in T order resumes. */
    int mcur_b, mcur_e;
    /* Split entries of this iteration and the arena their parts live in, both reset by the next read
     * phase: written by this thread in the read phase, read by the lanes and main in the write phase,
     * and by this thread again when it delivers. */
    struct fpSplitChunk *sa;
    int sa_n, sa_cap, sa_cur;
    size_t sa_used;
    struct fpSplit **splits;
    int nsplits, splits_cap;
} fpThread;

/* A multi-key command whose keys belong to several lanes runs as one part per lane, each with the keys
 * that lane owns. Every lane runs its entries in T order, so any two commands meet in the same order on
 * every lane they share, and the result is the same as running them one after the other. */
typedef struct fpPart {
    cmdEntry e;     /* first member: the lane runs the part as an entry */
    cmdEntry *orig; /* the batch entry the part belongs to */
    int *orig_idx;  /* orig_idx[j]: the index in orig->argv of the part's argv[j], j >= 1 */
} fpPart;

typedef struct fpSplit {
    int nparts, nkeys, mset;
    fpPart **parts;   /* in the order of the lanes of the command's first keys */
    uint8_t *keypart; /* keypart[i]: the part that holds the command's i-th key */
} fpSplit;

typedef struct fpSplitChunk {
    char *p;
    size_t size;
} fpSplitChunk;

static fpThread fp_threads[IO_THREADS_MAX_NUM];

/* Write lanes: the write phase runs fast-path commands on IO threads, each lane owning a set of
 * keyspace shards, and main merges each lane's replies, stats and propagation in command order. */

#define FP_LANE_CHUNK (64 * 1024)   /* reply arena chunk of a lane */
#define FP_LANE_MIN_ROOM (4 * 1024) /* below this, a reply starts a new chunk */

/* Propagation one entry's execution unit produced on a lane; main replays it in T order. */
typedef struct laneProp {
    serverOp *ops;
    int numops;
    int wrap; /* MULTI/EXEC around the ops, as propagatePendingCommands would have decided */
} laneProp;

typedef struct laneCmdStat {
    struct serverCommand *cmd;
    long long calls, usec, failed;
} laneCmdStat;

typedef struct laneError {
    sds msg; /* the reply's first bytes: enough to find the error prefix */
    int flags;
    struct serverCommand *cmd;
} laneError;

typedef struct laneLatency {
    const char *event;
    long long us;
} laneLatency;

typedef struct laneLog {
    void *entry;
    int type;
} laneLog;

typedef struct __attribute__((aligned(64))) fpLane {
    execContext ctx; /* the lane thread's ectx inside a write phase */
    client *ec;      /* executor, created by main */
    client *sec;     /* replica: executor of the primary's stream, flagged as the primary, created by main */
    /* Reply arena: chunks kept across iterations, refilled from the start every iteration. */
    char **chunks;
    int nchunks, chunk_cap, chunk;
    size_t used;
    int cursor[IO_THREADS_MAX_NUM]; /* per IO thread: next entry of its vector for this lane */
    /* Replication frames of this phase's entries (FP_PROP_FRAMES); main feeds them in T order. */
    sds frames;
    /* Handed to main after each phase. */
    laneError *errors;
    int nerrors, errors_cap;
    laneLatency *lat;
    int nlat, lat_cap;
    cmdEntry **closes;
    int ncloses, closes_cap;
    laneLog *logs;
    int nlogs, logs_cap;
    /* Commandstats by command id; cs_touched lists the ids this phase used, for the merge. */
    laneCmdStat *cs;
    int cs_cap;
    int *cs_touched;
    int ncs, cs_touched_cap;
    long long dur_cnt, dur_sum, dur_max, executed;
    /* Write phase: the lane's entries in T order, and its key prefetch batch (its thread's own). */
    cmdEntry **run;
    int nrun, run_cap;
    void *pf;
    long long prefetches; /* this phase: prefetch batches run */
    /* Latency histograms by command id, kept across phases; main folds them before a reader. */
    struct hdr_histogram **hist;
    struct serverCommand **hist_cmd;
    int hist_cap;
} fpLane;

static fpLane fp_lanes[FP_MAX_LANES];
static _Thread_local fpLane *fp_cur_lane = NULL;
/* Main writes these before the read phase; IO threads read them in it. Like every plain variable
 * shared across a phase, they are ordered by the phase barrier (release on begin, acquire on wait). */
static int fp_lanes_iter = 0; /* this iteration's entries are sorted into lanes */
static int fp_nlanes = 0;     /* lanes of this iteration */
static int fp_lane_main = 0;  /* main runs lane 0, IO thread i lane i; otherwise IO thread i lane i - 1 */
/* Main writes these before each write phase; the lanes read them in it. */
typedef struct fpSegPart {
    int tid;
    uint32_t seg;
} fpSegPart;
static fpSegPart *fp_parts = NULL;
static int fp_nparts = 0, fp_parts_cap = 0;
static mstime_t fp_phase_time = 0;
/* How a lane hands an entry's propagation to main. Main sets it before each lane phase; it stays fixed
 * through the phase and its merge, since only main-path commands and crons change the sinks. */
#define FP_PROP_NONE 0   /* no sink is fed: the ops are dropped */
#define FP_PROP_FRAMES 1 /* the replication backlog is the only sink: the lane formats the frames */
#define FP_PROP_OPS 2    /* AOF or slot export too: main replays the ops through propagateNow */
static int fp_prop_mode = FP_PROP_OPS;
/* INFO, main only. */
static long long fp_stat_lane_phases = 0, fp_stat_lane_entries = 0, fp_stat_barriers = 0, fp_stat_serial_segments = 0;
static long long fp_stat_lane_prefetches = 0;

#define FP_GROW(arr, n, cap, init)                                   \
    do {                                                             \
        if ((n) == (cap)) {                                          \
            (cap) = (cap) ? (cap) * 2 : (init);                      \
            (arr) = zrealloc((arr), sizeof(*(arr)) * (size_t)(cap)); \
        }                                                            \
    } while (0)

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
static _Thread_local int fp_cur_tid = 0; /* thread running the current staged phase; 0 is main */
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
        b = zmalloc_cache_aligned(sizeof(cmdBatch)); /* cmdBatch has cache-line-aligned members */
        b->arena = zmalloc(FP_ARENA_SIZE);
        b->arena_cap = FP_ARENA_SIZE;
    }
    b->count = 0;
    b->io_tid = tid;
    b->arena_used = 0;
    b->arena_spilled = 0;
    atomic_store_explicit(&b->executed, 0, memory_order_relaxed);
    b->delivered = 0;
    return b;
}

static void fpRecycleBatch(fpThread *t, cmdBatch *b) {
    if (t->nfree < FP_FREELIST_MAX) {
        /* Size the arena to what this batch's replies needed: replies that spill are each copied
         * into their own allocation on main. Shrink back once a batch uses a small part of it. */
        size_t need = b->arena_used + b->arena_spilled;
        size_t cap = b->arena_cap;
        if (b->arena_spilled && cap < FP_ARENA_MAX) {
            while (cap < need && cap < FP_ARENA_MAX) cap *= 2;
        } else if (!b->arena_spilled && cap > FP_ARENA_SIZE && need < cap / 8) {
            cap = FP_ARENA_SIZE;
        }
        if (cap != b->arena_cap) {
            zfree(b->arena);
            b->arena = zmalloc(cap);
            b->arena_cap = cap;
        }
        t->freelist[t->nfree++] = b;
    } else {
        zfree(b->arena);
        zfree(b);
    }
}

static client *fpExecutor(int tid);

/* Main. A thread's first batch and its executor are allocated here rather than by the first command
 * the thread reads, so memory a thread needs to serve at all is in use before any eviction limit is
 * measured against it. */
void fastpathInitThread(int tid) {
    fpThread *t = &fp_threads[tid];
    memset(t, 0, sizeof(*t));
    t->ready = 1;
    if (tid + 1 > fp_slots) fp_slots = tid + 1;
    fpRecycleBatch(t, fpAllocBatch(t, tid));
    fpExecutor(tid);
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
    for (int l = 0; l < FP_MAX_LANES; l++) zfree(t->lane_ents[l]);
    zfree(t->barriers);
    zfree(t->seg_count);
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
    if (authRequired(c)) return 0;                                                      /* main enforces a later default-user password change per entry */
    if (c->flag.asking) return 0;                                                       /* ASKING applies to the next command only, which main runs */
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
    c->fp_conn_type = (int8_t)connGetType(c->conn); /* MONITOR renders unix-socket origins from it */
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

/* IO thread, read phase: the write lane that may execute this command, or -1 when only main may. A
 * lane runs a command only if every key it declares is in one keyspace hashtable, which the lane owns
 * for the phase, and nothing about the command needs main: an ACL or auth failure, a retired user,
 * an arity error. */
/* Commands a lane may run: everything they touch outside their keys' hashtables is in the
 * execution context, frozen for the phase, or a reason fpLanesAllowed turns the lanes off. Commands
 * that draw random numbers (SPOP, SRANDMEMBER, HRANDFIELD, ZRANDMEMBER) stay off: the generators are
 * process-wide. */
static const char *fp_lane_commands[] = {
    /* strings and keys */
    "set", "setnx", "setex", "psetex", "getset", "getdel", "getex", "get", "mget", "mset",
    "strlen", "append", "setrange", "getrange", "substr", "incr", "decr", "incrby", "decrby",
    "incrbyfloat", "del", "unlink", "exists", "touch", "type", "ttl", "pttl", "expiretime",
    "pexpiretime", "expire", "pexpire", "expireat", "pexpireat", "persist",
    /* hashes, including field expiration */
    "hset", "hsetnx", "hget", "hmset", "hmget", "hdel", "hlen", "hstrlen", "hexists", "hincrby",
    "hincrbyfloat", "hkeys", "hvals", "hgetall", "hscan", "hexpire", "hpexpire", "hexpireat",
    "hpexpireat", "httl", "hpttl", "hexpiretime", "hpexpiretime", "hpersist", "hsetex", "hgetex",
    "hgetdel",
    /* sets */
    "sadd", "srem", "sismember", "smismember", "smembers", "scard", "smove", "sinter", "sintercard",
    "sinterstore", "sunion", "sunionstore", "sdiff", "sdiffstore", "sscan",
    /* sorted sets */
    "zadd", "zincrby", "zrem", "zscore", "zmscore", "zcard", "zcount", "zlexcount", "zrange",
    "zrangebyscore", "zrangebylex", "zrevrange", "zrevrangebyscore", "zrevrangebylex", "zrangestore",
    "zrank", "zrevrank", "zremrangebyrank", "zremrangebyscore", "zremrangebylex", "zpopmin", "zpopmax",
    "zmpop", "zunion", "zinter", "zdiff", "zunionstore", "zinterstore", "zdiffstore", "zintercard",
    "zscan",
    /* lists */
    "lpush", "rpush", "lpushx", "rpushx", "linsert", "lset", "lrem", "ltrim", "lpop", "rpop", "lmpop",
    "lindex", "llen", "lrange", "lpos", "lmove", "rpoplpush", NULL};

/* Command names of the parts: shared, so lanes running parts of one command at once never touch a
 * common refcount. Created by main with the lane commands. */
static robj *fp_part_name_mget = NULL, *fp_part_name_mset = NULL;

/* Main: mark the lane commands, once (command renames apply first, so a renamed one stays off). */
static void fpMarkLaneCommands(void) {
    static int done = 0;
    if (done) return;
    for (int i = 0; fp_lane_commands[i]; i++) {
        struct serverCommand *cmd = lookupCommandByCString(fp_lane_commands[i]);
        if (cmd) cmd->write_lane = 1;
    }
    fp_part_name_mget = makeObjectShared(createStringObject("MGET", 4));
    fp_part_name_mset = makeObjectShared(createStringObject("MSET", 4));
    done = 1;
}

/* Main writes these before the read phase; IO threads read them in it. */
static int fp_check_blocked = 0; /* clients are blocked on list, sorted set or stream keys */
static int fp_check_watched = 0; /* clients watch keys */

/* IO thread, read phase: a client is blocked on the key or watches it, so the command must run at its
 * place on main, which serves or dirties that client. While lanes are on, none of these dicts is
 * rehashing (fpKeyDictsSettled), so the lookups only read. The sets only shrink until the fast path's
 * part of the write phase ends: no fast-path command blocks or watches. */
static int fpKeyObserved(serverDb *db, robj *key) {
    if (fp_check_blocked && dictFind(db->blocking_keys, key)) return 1;
    if (fp_check_watched && dictFind(db->watched_keys, key)) return 1;
    return 0;
}

/* IO thread: the keyspace hashtable of every key the command declares (the parser's slot in cluster
 * mode), or -1 when it has none, they are in several, or one is observed (fpKeyObserved). */
static int fpKeysShard(serverDb *db, struct serverCommand *cmd, robj **argv, int argc, int slot) {
    int checks = fp_check_blocked || fp_check_watched;
    if (cmd->proc == setCommand || cmd->proc == getCommand) {
        if (argc < 2 || (checks && fpKeyObserved(db, argv[1]))) return -1;
        return server.cluster_enabled ? slot : getKVStoreIndexForKey(objectGetVal(argv[1]));
    }
    if (server.cluster_enabled && !checks) return slot;
    getKeysResult result;
    initGetKeysResult(&result);
    int n = getKeysFromCommand(cmd, argv, argc, &result);
    int shard = -1;
    for (int i = 0; i < n; i++) {
        robj *k = argv[result.keys[i].pos];
        if (checks && fpKeyObserved(db, k)) {
            shard = -1;
            break;
        }
        int s = server.cluster_enabled ? slot : getKVStoreIndexForKey(objectGetVal(k));
        if (i == 0) {
            shard = s;
        } else if (s != shard) {
            shard = -1;
            break;
        }
    }
    getKeysFreeResult(&result);
    return shard;
}

/* IO thread: the client may run this command on a lane at all. */
static int fpLaneAuthOk(client *c, int read_flags) {
    if (read_flags & (READ_FLAGS_ERROR_MASK | READ_FLAGS_BAD_ARITY)) return 0;
    if ((c->user->flags & USER_FLAG_RETIRED) || authRequired(c)) return 0;
    if (!(read_flags & READ_FLAGS_ACL_ALLOWED) && !ACLUserUnrestricted(c->user)) return 0;
    return 1;
}

static int fpLaneOf(client *c, struct serverCommand *cmd, robj **argv, int argc, int slot, int read_flags, int *shard) {
    if (!fpLaneAuthOk(c, read_flags)) return -1;
    if (cmd->proc == pingCommand) {
        *shard = -1;
        return (int)(c->id % (uint64_t)fp_nlanes);
    }
    if (!cmd->write_lane) return -1;
    *shard = fpKeysShard(c->db, cmd, argv, argc, slot);
    if (*shard < 0) return -1;
    if (server.cluster_enabled && !clusterServesSlotStably(*shard)) return -1; /* main produces the redirect */
    return *shard % fp_nlanes;
}

#define FP_SPLIT_CHUNK (64 * 1024)

/* IO thread, read phase: memory for this iteration's split entries, 16-byte aligned. */
static void *fpSplitAlloc(fpThread *t, size_t size) {
    size = (size + 15) & ~(size_t)15;
    while (t->sa_cur < t->sa_n) {
        fpSplitChunk *ch = &t->sa[t->sa_cur];
        if (ch->size - t->sa_used >= size) {
            void *p = ch->p + t->sa_used;
            t->sa_used += size;
            return p;
        }
        t->sa_cur++;
        t->sa_used = 0;
    }
    FP_GROW(t->sa, t->sa_n, t->sa_cap, 4);
    size_t cs = size > FP_SPLIT_CHUNK ? size : FP_SPLIT_CHUNK;
    t->sa[t->sa_n++] = (fpSplitChunk){.p = zmalloc(cs), .size = cs};
    t->sa_cur = t->sa_n - 1;
    t->sa_used = size;
    return t->sa[t->sa_cur].p;
}

/* IO thread, read phase: put an MGET or MSET (standalone, keyspace shards on) whose keys belong to
 * several lanes on each of those lanes as one part, or on one lane when they all belong to it. Returns
 * 0 when the command stays a barrier. */
static int fpTrySplit(fpThread *t, cmdEntry *e, client *c) {
    if (server.cluster_enabled || !server.keyspace_shard_bits || !e->cmd->write_lane) return 0;
    int mset = e->cmd->proc == msetCommand;
    if (!mset && e->cmd->proc != mgetCommand) return 0;
    int step = mset ? 2 : 1;
    if (mset && (e->argc - 1) % 2) return 0; /* the arity error comes from main */
    int nkeys = (e->argc - 1) / step;
    if (nkeys < 2 || !fpLaneAuthOk(c, e->read_flags)) return 0;
    int checks = fp_check_blocked || fp_check_watched;
    uint8_t *keypart = fpSplitAlloc(t, (size_t)nkeys);
    int count[FP_MAX_LANES];
    int8_t part_of[FP_MAX_LANES];
    memset(count, 0, sizeof(int) * fp_nlanes);
    memset(part_of, -1, sizeof(part_of));
    int lanes[FP_MAX_LANES];
    int nparts = 0;
    for (int i = 0; i < nkeys; i++) {
        robj *k = e->argv[1 + i * step];
        if (checks && fpKeyObserved(e->db, k)) return 0;
        int lane = getKVStoreIndexForKey(objectGetVal(k)) % fp_nlanes;
        if (part_of[lane] < 0) {
            part_of[lane] = (int8_t)nparts;
            lanes[nparts++] = lane;
        }
        count[lane]++;
        keypart[i] = (uint8_t)part_of[lane];
    }
    if (nparts == 1) {
        /* One lane owns every key; the keys are in several of its hashtables, so the executor carries no
         * shard and each key finds its own. */
        e->lane = lanes[0];
        e->shard = -1;
        FP_GROW(t->lane_ents[e->lane], t->lane_n[e->lane], t->lane_cap[e->lane], 64);
        t->lane_ents[e->lane][t->lane_n[e->lane]++] = e;
        t->seg_count[t->nbarriers]++;
        return 1;
    }
    fpSplit *s = fpSplitAlloc(t, sizeof(*s));
    s->nparts = nparts;
    s->nkeys = nkeys;
    s->mset = mset;
    s->keypart = keypart;
    s->parts = fpSplitAlloc(t, sizeof(fpPart *) * (size_t)nparts);
    for (int p = 0; p < nparts; p++) {
        int n = count[lanes[p]];
        fpPart *pt = fpSplitAlloc(t, sizeof(*pt));
        pt->e = *e;
        pt->orig = e;
        pt->e.argv = fpSplitAlloc(t, sizeof(robj *) * (size_t)(1 + n * step));
        pt->orig_idx = fpSplitAlloc(t, sizeof(int) * (size_t)(1 + n * step));
        pt->e.argv[0] = mset ? fp_part_name_mset : fp_part_name_mget;
        pt->orig_idx[0] = -1;
        pt->e.argc = 1; /* filled below */
        pt->e.argv_len = 1 + n * step;
        pt->e.argv_len_sum = 0;
        pt->e.input_bytes = p == 0 ? e->input_bytes : 0;
        pt->e.lane = lanes[p];
        pt->e.shard = -1;
        pt->e.part = 1;
        pt->e.part_extra = p > 0;
        pt->e.split = NULL;
        pt->e.reply_big = NULL;
        pt->e.reply_base = NULL;
        pt->e.reply_off = pt->e.reply_len = pt->e.reply_big_len = 0;
        pt->e.prop = NULL;
        pt->e.frames_off = pt->e.frames_len = 0;
        pt->e.frames_wrap = 0;
        s->parts[p] = pt;
    }
    for (int i = 0; i < nkeys; i++) {
        fpPart *pt = s->parts[keypart[i]];
        for (int j = 0; j < step; j++) {
            int src = 1 + i * step + j;
            pt->orig_idx[pt->e.argc] = src;
            pt->e.argv[pt->e.argc++] = e->argv[src];
        }
    }
    for (int p = 0; p < nparts; p++) {
        int lane = lanes[p];
        FP_GROW(t->lane_ents[lane], t->lane_n[lane], t->lane_cap[lane], 64);
        t->lane_ents[lane][t->lane_n[lane]++] = &s->parts[p]->e;
        t->seg_count[t->nbarriers]++;
    }
    /* Run serially, main executes the entry whole: its lane is only what places it in a segment. */
    e->lane = lanes[0];
    e->shard = -1;
    e->split = s;
    FP_GROW(t->splits, t->nsplits, t->splits_cap, 16);
    t->splits[t->nsplits++] = s;
    return 1;
}

/* IO thread, read phase: put the entry on its lane's vector, or record it as a barrier. A barrier
 * closes this thread's current segment. */
static void fpClassify(fpThread *t, int tid, cmdBatch *b, cmdEntry *e, client *c) {
    UNUSED(tid);
    e->seg = (uint32_t)t->nbarriers;
    int shard = -1;
    int lane = fpLaneOf(c, e->cmd, e->argv, e->argc, e->slot, e->read_flags, &shard);
    if (lane >= 0) {
        e->lane = lane;
        e->shard = shard;
        FP_GROW(t->lane_ents[lane], t->lane_n[lane], t->lane_cap[lane], 64);
        t->lane_ents[lane][t->lane_n[lane]++] = e;
        t->seg_count[t->nbarriers]++;
    } else if (!fpTrySplit(t, e, c)) {
        FP_GROW(t->barriers, t->nbarriers, t->barriers_cap, 16);
        t->barriers[t->nbarriers++] = (struct fpBarrier){.e = e, .b = b};
        FP_GROW(t->seg_count, t->nbarriers, t->seg_cap, 16);
        t->seg_count[t->nbarriers] = 0;
    }
}

/* IO thread: free what this iteration's split parts still hold. Their replies were assembled at
 * delivery; this catches parts of entries that were not delivered. */
static void fpReleaseSplits(fpThread *t) {
    for (int i = 0; i < t->nsplits; i++) {
        fpSplit *s = t->splits[i];
        for (int p = 0; p < s->nparts; p++) {
            cmdEntry *pe = &s->parts[p]->e;
            if (pe->reply_big) zfree(pe->reply_big);
            pe->reply_big = NULL;
        }
    }
    t->nsplits = 0;
    t->sa_cur = 0;
    t->sa_used = 0;
}

/* IO thread, start of a depth-0 read phase: forget the previous iteration's lane vectors. */
static void fpResetLaneVectors(fpThread *t) {
    for (int l = 0; l < FP_MAX_LANES; l++) t->lane_n[l] = 0;
    t->nbarriers = 0;
    if (!t->seg_cap) {
        t->seg_cap = 16;
        t->seg_count = zmalloc(sizeof(int) * t->seg_cap);
    }
    t->seg_count[0] = 0;
    t->scur_b = t->scur_e = 0;
    t->mcur_b = t->mcur_e = 0;
    t->pf_barrier = 0;
    fpReleaseSplits(t);
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
    e->readonly = c->flag.readonly;
    e->origin.client_id = c->id;
    e->origin.principal = c->user;
    e->origin.authenticated = c->flag.authenticated;
    e->origin.peer = c->fp_peer;
    e->origin.local = c->fp_local;
    e->origin.conn_type = c->fp_conn_type;
    e->reply_off = e->reply_len = 0;
    e->reply_big = NULL;
    e->reply_big_len = 0;
    e->requeued = 0;
    e->obuf_close = 0;
    e->reply_base = NULL;
    e->prop = NULL;
    e->lane = -1;
    e->shard = -1;
    e->seg = 0;
    e->frames_off = e->frames_len = 0;
    e->frames_wrap = 0;
    e->part = e->part_extra = 0;
    e->split = NULL;
    if (fp_lanes_iter && fpDepth() == 0) fpClassify(&fp_threads[fp_cur_tid], fp_cur_tid, b, e, c);
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
            c->slot = -1;                    /* the next command's slot is computed afresh */
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
/* Free what main did not keep in a run of entries, on the thread that produced them; no free
 * traffic through the shared inbox. Requeued entries' argv was moved to the client's queue by
 * fpRequeue (set to NULL there), so this skips them. */
static void fpFreeEntries(cmdEntry *e, int n) {
    for (int k = 0; k < n; k++) {
        if (e[k].reply_big) {
            zfree(e[k].reply_big);
            e[k].reply_big = NULL;
        }
        if (e[k].argv) {
            for (int j = 0; j < e[k].argc; j++)
                if (e[k].argv[j]) decrRefCount(e[k].argv[j]);
            zfree(e[k].argv);
            e[k].argv = NULL;
        }
    }
}

/* The reply bytes an entry holds, or NULL when it has none. */
static const char *fpEntryReply(cmdBatch *b, cmdEntry *e, size_t *len) {
    if (e->reply_big) {
        *len = e->reply_big_len;
        return e->reply_big;
    }
    *len = e->reply_len;
    if (!e->reply_len) return NULL;
    return (e->reply_base ? e->reply_base : b->arena) + e->reply_off;
}

/* Length of the RESP line at p (through its CRLF). */
static size_t fpRespLineLen(const char *p, const char *end) {
    const char *nl = memchr(p, '\n', (size_t)(end - p));
    serverAssert(nl);
    return (size_t)(nl - p) + 1;
}

/* IO thread, delivery: build a split entry's reply from its parts' replies, as the command run whole
 * would have replied. Leaves the entry without a reply when a part has none (its client is closing). */
static void fpSplitAssemble(cmdBatch *b, cmdEntry *e) {
    fpSplit *s = e->split;
    e->split = NULL;
    const char *cur[FP_MAX_LANES], *end[FP_MAX_LANES];
    size_t total = 0;
    int complete = 1;
    for (int p = 0; p < s->nparts; p++) {
        size_t len;
        const char *r = fpEntryReply(b, &s->parts[p]->e, &len);
        if (!r) complete = 0;
        cur[p] = r;
        end[p] = r ? r + len : NULL;
        total += len;
    }
    if (complete && s->mset) {
        /* Every part replied +OK. */
        static const char ok[] = "+OK\r\n";
        e->reply_base = (char *)ok;
        e->reply_off = 0;
        e->reply_len = sizeof(ok) - 1;
    } else if (complete) {
        /* MGET: one array of the parts' elements, in the command's key order. Each part's reply is an
         * array header followed by its keys' elements in order: a bulk string or a null. */
        for (int p = 0; p < s->nparts; p++) cur[p] += fpRespLineLen(cur[p], end[p]);
        char *out = zmalloc(total + 32);
        size_t o = (size_t)snprintf(out, 32, "*%d\r\n", s->nkeys);
        for (int i = 0; i < s->nkeys; i++) {
            int p = s->keypart[i];
            const char *q = cur[p];
            size_t n = fpRespLineLen(q, end[p]);
            if (q[0] == '$' && q[1] != '-') n += (size_t)strtoll(q + 1, NULL, 10) + 2;
            serverAssert(q[0] == '$' || q[0] == '_');
            serverAssert(q + n <= end[p]);
            memcpy(out + o, q, n);
            o += n;
            cur[p] = q + n;
        }
        e->reply_big = out;
        e->reply_big_len = (uint32_t)o;
    }
    for (int p = 0; p < s->nparts; p++) {
        cmdEntry *pe = &s->parts[p]->e;
        if (pe->reply_big) zfree(pe->reply_big);
        pe->reply_big = NULL;
    }
}

/* Deliver the entries [from, to) of a batch. from and to must each fall on a client-run boundary,
 * so a run is never split across two calls. Consecutive entries for one client share a writev.
 * Shared by the STREAM phase (runs as main finishes them, to = b->executed) and the REPLY phase
 * (the remaining runs at once, from = b->delivered, to = b->count). */
static void fpDeliverRange(fpThread *t, cmdBatch *b, int from, int to) {
    struct iovec iov[IO_BATCH_MAX];
    int i = from;
    while (i < to) {
        client *c = b->e[i].c;
        int n = 0;
        int j = i;
        int requeued = 0;
        while (j < to && b->e[j].c == c) {
            cmdEntry *e = &b->e[j];
            if (e->split) fpSplitAssemble(b, e);
            if (e->requeued) {
                requeued++;
            } else if (e->reply_big) {
                iov[n].iov_base = e->reply_big, iov[n].iov_len = e->reply_big_len, n++;
            } else if (e->reply_len) {
                iov[n].iov_base = (e->reply_base ? e->reply_base : b->arena) + e->reply_off, iov[n].iov_len = e->reply_len, n++;
            }
            j++;
        }
        if (n) fpSend(t, c, iov, n);
        c->fp_inflight -= (j - i);
        c->commands_processed += (j - i) - requeued;
        if (requeued) fpRequeue(t, c, &b->e[j - requeued], requeued); /* main stops executing a client at its first held entry */
        fpFreeEntries(&b->e[i], j - i);
        i = j;
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

static _Thread_local cmdEntry *fp_exec_entry = NULL; /* the entry this thread's executor is running */

static void fpLanePushClose(fpLane *L, cmdEntry *e);

/* Main: close a client whose executor reply passed the output buffer hard limit. */
static void fpCloseOverLimit(cmdEntry *e) {
    freeClientAsync(e->c); /* records the close; main frees it once it took the client back */
    serverLog(LL_WARNING, "Client id=%llu closed for overcoming of output buffer limits.",
              (unsigned long long)e->control->client_id);
    server.stat_client_outbuf_limit_disconnections++;
}

/* Main, from closeClientOnOutputBufferLimitReached while an executor builds a reply: a reply past the
 * normal class hard limit closes its client, as on the main path. The reply is dropped and commands
 * that build long replies stop at close_asap. A write lane leaves the close to main. */
int fastpathExecutorOutputLimitReached(client *ec) {
    cmdEntry *e = fp_exec_entry;
    if (!e || !e->control) return 0;
    if (ec->flag.close_asap) return 1;
    clientBufferLimitsConfig *l = &server.client_obuf_limits[CLIENT_TYPE_NORMAL];
    if (!l->hard_limit_bytes || getClientOutputBufferMemoryUsage(ec) < l->hard_limit_bytes) return 0;
    ec->flag.close_asap = 1;
    if (exec_lane) {
        fpLanePushClose(fp_cur_lane, e);
        return 1;
    }
    fpCloseOverLimit(e);
    return 1;
}

/* Conditions under which a fast-path command must run as a real client on main. Each can change only
 * on main, so checking them as each entry executes is exact. */
static int fpMustRunOnMain(void) {
    if (isPausedActions(PAUSE_ACTION_CLIENT_ALL | PAUSE_ACTION_CLIENT_WRITE)) return 1; /* main postpones it */
    if (server.failover_state != NO_FAILOVER) return 1;                                 /* coordinated failover owns writes */
    if (moduleHasCommandFilters()) return 1;                                            /* a filter may rewrite argv, which the executor borrows */
    if (bgIteration_iterationActive()) return 1;                                        /* a forkless save may block it on a key in use */
    if (throttle_anyThrottlers()) return 1;                                             /* a throttler may hold the command */
    if (server.busy_module_yield_flags != BUSY_MODULE_YIELD_NONE &&
        !(server.busy_module_yield_flags & BUSY_MODULE_YIELD_CLIENTS))
        return 1; /* a yielding module postpones commands until it returns */
    return 0;
}

/* The executor borrows argv and writes replies into the batch arena. */
static void fpExecute(client *ec, cmdBatch *b, cmdEntry *e) {
    char *saved_buf = ec->buf;
    size_t saved_usable = ec->buf_usable_size;
    user *principal = e->origin.principal;

    /* Main runs the command whole; its parts do not run. */
    e->split = NULL;
    if (fpControlDetaching(e->control)) goto release_argv; /* no reply: the IO thread is closing it */
    if (fpMustRunOnMain()) {
        e->requeued = 1; /* handed back for the main path */
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
    ec->flag.readonly = e->readonly;
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
        b->arena_spilled += total;
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

/* Prefetch the keys of b's entries from `from` on, until `to` or the prefetch batch is full.
 * Returns the index past the last entry added, at least from + 1, so a caller executing up to it
 * always makes progress. */
static int fpPrefetchRange(cmdBatch *b, int from, int to) {
    getKeysResult result;
    initGetKeysResult(&result);
    int room = 1;
    int k = from;
    ClientControl *last_control = NULL;
    for (; k < to && room; k++) {
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
    return k > from ? k : from + 1;
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
    for (int i = 0; i < fp_slots; i++) {
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
                         "keyspace_shard_bits:%d\r\n"
                         "fastpath_reads:%lld\r\n"
                         "fastpath_writes:%lld\r\n"
                         "fastpath_batches:%lld\r\n"
                         "fastpath_speculated:%lld\r\n"
                         "fastpath_net_input_bytes:%lld\r\n"
                         "fastpath_net_output_bytes:%lld\r\n"
                         "fastpath_write_lane_phases:%lld\r\n"
                         "fastpath_write_lane_entries:%lld\r\n"
                         "fastpath_write_barriers:%lld\r\n"
                         "fastpath_write_serial_segments:%lld\r\n"
                         "fastpath_write_lane_prefetches:%lld\r\n",
                         fastpath_clients, stagedWorkerCount(), server.keyspace_shard_bits, reads, writes, batches,
                         speculated, in, out, fp_stat_lane_phases, fp_stat_lane_entries, fp_stat_barriers,
                         fp_stat_serial_segments, fp_stat_lane_prefetches);
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
        if (!fp_threads[0].ready) fastpathInitThread(0); /* main takes a share of every read and reply phase */
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

/* Read phase, on each IO thread and on main (tid 0): claim ready client fds until none are left, then
 * publish the open batch. */
void fastpathStagedReadPhase(int tid) {
    fpThread *t = &fp_threads[tid];
    poller *p = stagedPoller();
    pollerEvent ev;
    fp_cur_tid = tid;
    if (fpDepth() == 0) fpResetLaneVectors(t);
    while (pollerClaimIO(p, &ev)) ev.handler(ev.fd, ev.mask, ev.privdata, ev.affinity);
    fpSubmit(t);
    if (t->cur && t->cur->count == 0) {
        fpRecycleBatch(t, t->cur);
        t->cur = NULL;
    }
}

/* Main: no blocked-key or watched-key dict is rehashing, so lookups in them (fpKeyObserved on the IO
 * threads, the lanes' own wake-up and WATCH checks) only read. A barrier entry can start a rehash
 * (serving a blocked client deletes from them); main asks again after each one. */
static int fpKeyDictsSettled(void) {
    for (int j = 0; j < server.dbnum; j++) {
        serverDb *db = server.db[j];
        if (!db) continue;
        if (dictIsRehashing(db->blocking_keys) || dictIsRehashing(db->blocking_keys_unblock_on_nokey) ||
            dictIsRehashing(db->watched_keys))
            return 0;
    }
    return 1;
}

/* Main: nothing outside a lane's own hashtables would observe or order the iteration's writes. Every
 * condition is changed only by main between phases or by main-path commands, so it holds from the
 * read phase until the fast path's part of the write phase is done, unless a barrier entry changes
 * it; main re-checks after each one. */
static int fpLanesAllowed(void) {
    /* Cluster mode: one hashtable per slot already. An exporting slot's writes reach its export job
     * through the replay, but keep the cutover simple: no lanes while one is exported. */
    if (server.cluster_enabled ? clusterIsAnySlotExporting() : !server.keyspace_shard_bits) return 0;
    if (server.primary_host || server.loading || server.async_loading) return 0;
    if (server.paused_actions || listLength(server.monitors)) return 0;
    if (!trackingIsIdle()) return 0;
    if (server.notify_keyspace_events || moduleObservesWrites()) return 0;
    if (bgIteration_iterationActive() || throttle_anyThrottlers()) return 0;
    if (server.maxmemory || server.maxmemory_clients) return 0;
    /* A module can block a client on any key and wake it by its own rules. */
    if (server.blocked_clients_by_type[BLOCKED_MODULE]) return 0;
    int blocked = server.blocked_clients_by_type[BLOCKED_LIST] || server.blocked_clients_by_type[BLOCKED_ZSET] ||
                  server.blocked_clients_by_type[BLOCKED_STREAM];
    if ((blocked || server.watching_clients) && !fpKeyDictsSettled()) return 0;
    if (hotkeysEnabled() || server.busy_module_yield_flags) return 0;
    if (writeCommandsDeniedByDiskError() != DISK_ERROR_TYPE_NONE || !checkGoodReplicasStatus()) return 0;
    return 1;
}

void fastpathStagedPrepareLanes(void) {
    int n = 0;
    if (fpDepth() == 0 && server.io_threads_parallel_writes) {
        fpMarkLaneCommands();
        n = stagedWorkerCount() + stagedMainTakesLane();
        if (n > FP_MAX_LANES) n = FP_MAX_LANES;
    }
    fp_lane_main = stagedMainTakesLane();
    fp_nlanes = n;
    fp_lanes_iter = n >= 2 && fpLanesAllowed();
    fp_check_blocked = server.blocked_clients_by_type[BLOCKED_LIST] || server.blocked_clients_by_type[BLOCKED_ZSET] ||
                       server.blocked_clients_by_type[BLOCKED_STREAM];
    fp_check_watched = server.watching_clients > 0;
    /* The previous iteration's replies were delivered in its reply phase. */
    for (int l = 0; l < FP_MAX_LANES; l++) {
        fp_lanes[l].chunk = 0;
        fp_lanes[l].used = 0;
    }
}

/* Main, after the read phase: the write phase has enough lane commands to run a lane phase, so the IO
 * threads will be executing in it. A thread without batches did not read in this iteration. */
int fastpathStagedLanesWillRun(void) {
    if (fpDepth() != 0 || !fp_lanes_iter) return 0;
    long long n = 0;
    for (int tid = 0; tid < fp_slots; tid++) {
        fpThread *t = &fp_threads[tid];
        if (t->sbn[0] == 0) continue;
        for (int l = 0; l < fp_nlanes; l++) n += t->lane_n[l];
    }
    return n >= server.io_threads_parallel_writes_min;
}

static void fpLanePushClose(fpLane *L, cmdEntry *e) {
    e->obuf_close = 1;
    FP_GROW(L->closes, L->ncloses, L->closes_cap, 8);
    L->closes[L->ncloses++] = e;
}

static laneCmdStat *fpLaneCmdStat(fpLane *L, struct serverCommand *cmd);

void fastpathLaneErrorReply(client *c, const char *s, size_t len, int flags) {
    fpLane *L = fp_cur_lane;
    if (flags & ERR_REPLY_FLAG_NO_STATS_UPDATE) {
        /* No error stats; the command still counts as failed, as afterErrorReply counts it. */
        fpLaneCmdStat(L, c->realcmd)->failed++;
        return;
    }
    ectx->stat_total_error_replies++;
    FP_GROW(L->errors, L->nerrors, L->errors_cap, 8);
    L->errors[L->nerrors++] =
        (laneError){.msg = sdsnewlen(s, len < 64 ? len : 64), .flags = flags, .cmd = c->realcmd};
}

void fastpathLaneLatencySample(const char *event, long long latency_us) {
    fpLane *L = fp_cur_lane;
    FP_GROW(L->lat, L->nlat, L->lat_cap, 8);
    L->lat[L->nlat++] = (laneLatency){.event = event, .us = latency_us};
}

static laneCmdStat *fpLaneCmdStat(fpLane *L, struct serverCommand *cmd) {
    if (cmd->id >= L->cs_cap) {
        int cap = L->cs_cap ? L->cs_cap : 256;
        while (cap <= cmd->id) cap *= 2;
        L->cs = zrealloc(L->cs, sizeof(*L->cs) * cap);
        memset(L->cs + L->cs_cap, 0, sizeof(*L->cs) * (cap - L->cs_cap));
        L->cs_cap = cap;
    }
    laneCmdStat *cs = &L->cs[cmd->id];
    if (!cs->cmd) {
        cs->cmd = cmd;
        FP_GROW(L->cs_touched, L->ncs, L->cs_touched_cap, 16);
        L->cs_touched[L->ncs++] = cmd->id;
    }
    return cs;
}

static void fpLaneHistogram(fpLane *L, struct serverCommand *cmd, long long ns) {
    if (cmd->id >= L->hist_cap) {
        int cap = L->hist_cap ? L->hist_cap : 256;
        while (cap <= cmd->id) cap *= 2;
        L->hist = zrealloc(L->hist, sizeof(*L->hist) * cap);
        L->hist_cmd = zrealloc(L->hist_cmd, sizeof(*L->hist_cmd) * cap);
        memset(L->hist + L->hist_cap, 0, sizeof(*L->hist) * (cap - L->hist_cap));
        memset(L->hist_cmd + L->hist_cap, 0, sizeof(*L->hist_cmd) * (cap - L->hist_cap));
        L->hist_cap = cap;
    }
    L->hist_cmd[cmd->id] = cmd;
    updateCommandLatencyHistogram(&L->hist[cmd->id], ns);
}

void fastpathFoldLaneHistograms(void) {
    for (int l = 0; l < FP_MAX_LANES; l++) {
        fpLane *L = &fp_lanes[l];
        for (int id = 0; id < L->hist_cap; id++) {
            struct hdr_histogram *h = L->hist[id];
            if (!h || h->total_count == 0) continue;
            struct serverCommand *cmd = L->hist_cmd[id];
            if (cmd->latency_histogram == NULL)
                hdr_init(LATENCY_HISTOGRAM_MIN_VALUE, LATENCY_HISTOGRAM_MAX_VALUE, LATENCY_HISTOGRAM_PRECISION,
                         &cmd->latency_histogram);
            hdr_add(cmd->latency_histogram, h);
            hdr_reset(h);
        }
    }
}

void fastpathResetLaneStats(void) {
    for (int l = 0; l < FP_MAX_LANES; l++) {
        fpLane *L = &fp_lanes[l];
        for (int id = 0; id < L->hist_cap; id++)
            if (L->hist[id]) hdr_reset(L->hist[id]);
    }
}

/* Lane thread: the arena slot the next reply goes to. */
static char *fpLaneArena(fpLane *L, size_t *room) {
    if (L->nchunks && FP_LANE_CHUNK - L->used < FP_LANE_MIN_ROOM) {
        L->chunk++;
        L->used = 0;
    }
    if (L->chunk >= L->nchunks) {
        FP_GROW(L->chunks, L->nchunks, L->chunk_cap, 4);
        L->chunks[L->nchunks++] = zmalloc(FP_LANE_CHUNK);
    }
    *room = FP_LANE_CHUNK - L->used;
    return L->chunks[L->chunk] + L->used;
}

/* Lane thread: the part of processCommand and call() that applies to an entry the read phase found
 * lane-eligible. Statistics go to the lane, propagation is captured for main, nothing is emitted. */
static void fpLaneCall(fpLane *L, client *c) {
    struct serverCommand *real_cmd = c->realcmd;
    ectx->in_call++;
    ectx->executing_client = c;
    c->flag.force_aof = 0;
    c->flag.force_repl = 0;
    c->flag.prevent_prop = 0;
    c->redact_arg_bitmap = 0;
    long long dirty = ectx->dirty;
    long long errors = ectx->stat_total_error_replies;
    enterExecutionUnit(0, 0);
    c->flag.executing_command = 1;
    c->flag.buffered_reply = 0;
    c->flag.keyspace_notified = 0;
    monotime start = getMonotonicUs();

    c->cmd->proc(c);

    exitExecutionUnit();
    c->flag.executing_command = 0;
    ustime_t duration = getMonotonicUs() - start;
    dirty = ectx->dirty - dirty;
    if (dirty < 0) dirty = 0;

    laneCmdStat *cs = fpLaneCmdStat(L, real_cmd);
    /* A split command is one call: its first part counts it, every part adds its time. */
    int counted = !(fp_exec_entry && fp_exec_entry->part_extra);
    if (ectx->stat_total_error_replies > errors || c->deferred_reply_errors) cs->failed++;
    latencyAddSampleIfNeeded((real_cmd->flags & CMD_FAST) ? "fast-command" : "command", duration);
    if (ectx->execution_nesting == 0 && counted) {
        L->dur_cnt++;
        L->dur_sum += duration;
        if (duration > L->dur_max) L->dur_max = duration;
    }
    c->duration = duration;
    void *entries[3];
    int types[3];
    int n = counted ? commandlogPrepareCurrentCommand(c, real_cmd, entries, types) : 0;
    for (int i = 0; i < n; i++) {
        FP_GROW(L->logs, L->nlogs, L->logs_cap, 4);
        L->logs[L->nlogs++] = (laneLog){.entry = entries[i], .type = types[i]};
    }
    c->duration = 0;
    if (counted) cs->calls++;
    cs->usec += duration;
    if (server.latency_tracking_enabled) fpLaneHistogram(L, real_cmd, duration * 1000);
    /* A slot maps to exactly one lane (shard % fp_nlanes), and main runs only its own lane in the
     * phase, so no other thread writes this slot's CLUSTER SLOT-STATS counters. */
    clusterSlotStatsAddCpuDuration(c, duration);
    clusterSlotStatsAddNetworkBytesOutForUserClient(c);

    if (fp_prop_mode != FP_PROP_NONE && !c->flag.prevent_prop && !(c->cmd->flags & CMD_MODULE)) {
        int flags = PROPAGATE_NONE;
        if (dirty) flags |= (PROPAGATE_AOF | PROPAGATE_REPL);
        if (c->flag.force_repl) flags |= PROPAGATE_REPL;
        if (c->flag.force_aof) flags |= PROPAGATE_AOF;
        if (c->flag.prevent_repl_prop || c->flag.module_prevent_repl_prop) flags &= ~PROPAGATE_REPL;
        if (c->flag.prevent_aof_prop || c->flag.module_prevent_aof_prop) flags &= ~PROPAGATE_AOF;
        /* In standalone the executor's slot is the lane shard, which no sink uses. */
        if (flags != PROPAGATE_NONE)
            alsoPropagate(c->db->id, c->argv, c->argc, flags, server.cluster_enabled ? c->slot : -1);
    }
    c->flag.force_aof = 0;
    c->flag.force_repl = 0;
    c->flag.prevent_prop = 0;
    if (counted) ectx->stat_numcommands++;
    ectx->executing_client = NULL;
    ectx->in_call--;
}


static int fpPropMode(void) {
    if (server.aof_state != AOF_OFF || clusterIsAnySlotExporting()) return FP_PROP_OPS;
    if (server.primary_host || (!server.repl_backlog && !listLength(server.replicas))) return FP_PROP_NONE;
    return server.repl_backlog ? FP_PROP_FRAMES : FP_PROP_OPS;
}

/* Lane thread: take what the entry's execution unit would have propagated. */
static void fpLaneTakeProp(fpLane *L, cmdEntry *e, client *ec) {
    serverOpArray *oa = &ectx->also_propagate;
    int n = oa->numops;
    int wrap = n > 1 && !(ec->cmd->flags & CMD_TOUCHES_ARBITRARY_KEYS);
    if (fp_prop_mode == FP_PROP_NONE) {
        serverOpArrayFree(oa);
        return;
    }
    if (fp_prop_mode == FP_PROP_FRAMES) {
        int same_db = 1;
        for (int j = 0; j < n; j++) same_db &= oa->ops[j].dbid == e->db->id;
        if (same_db) {
            if (!L->frames) L->frames = sdsempty();
            size_t start = sdslen(L->frames);
            for (int j = 0; j < n; j++) L->frames = replicationCatCommand(L->frames, oa->ops[j].argv, oa->ops[j].argc);
            e->frames_off = (uint32_t)start;
            e->frames_len = (uint32_t)(sdslen(L->frames) - start);
            e->frames_wrap = (uint8_t)wrap;
            serverOpArrayFree(oa);
            return;
        }
    }
    laneProp *p = zmalloc(sizeof(*p));
    p->ops = zmalloc(sizeof(serverOp) * n);
    memcpy(p->ops, oa->ops, sizeof(serverOp) * n);
    p->numops = n;
    p->wrap = wrap;
    oa->numops = 0; /* the ops' argv now belong to p */
    e->prop = p;
}

/* Lane thread: execute one entry on the lane's executor, its reply into the lane arena. */
static void fpLaneExecute(fpLane *L, cmdEntry *e) {
    client *ec = L->ec;
    char *saved_buf = ec->buf;
    size_t saved_usable = ec->buf_usable_size;

    if (fpControlDetaching(e->control)) goto release_argv; /* no reply: main is closing it */

    ec->user = e->origin.principal;
    ec->flag.authenticated = ec->flag.ever_authenticated = e->origin.authenticated;
    ec->argv = e->argv;
    ec->argc = e->argc;
    ec->argv_len = e->argv_len;
    ec->argv_len_sum = e->argv_len_sum;
    ec->net_input_bytes_curr_cmd = e->input_bytes;
    ec->parsed_cmd = e->cmd;
    ec->cmd = ec->lastcmd = ec->realcmd = e->cmd;
    ec->slot = e->shard;
    ec->read_flags = e->read_flags;
    ec->acl_epoch_seen = e->origin.acl_epoch_seen;
    ec->db = e->db;
    ec->resp = e->resp;
    ec->origin = &e->origin;
    size_t room;
    char *slot = fpLaneArena(L, &room);
    ec->buf = slot;
    ec->buf_usable_size = room;
    ec->bufpos = 0;
    ec->flag.buf_encoded = 0;
    ec->flag.pending_command = 1;
    ec->woff = 0;

    fp_exec_entry = e;
    ectx->current_client = ec;
    fpLaneCall(L, ec);
    if (ectx->also_propagate.numops) fpLaneTakeProp(L, e, ec);
    commandProcessed(ec);
    ectx->current_client = NULL;
    fp_exec_entry = NULL;
    L->executed++;

    if (ec->flag.close_asap) {
        /* Closed for the output buffer limit: nothing of this reply is sent. */
        ec->flag.close_asap = 0;
        listEmpty(ec->reply);
        ec->reply_bytes = 0;
        ec->bufpos = 0;
    }
    e->reply_base = L->chunks[L->chunk];
    e->reply_off = (uint32_t)L->used;
    e->reply_len = (uint32_t)ec->bufpos;
    L->used += ec->bufpos;
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
        L->used -= ec->bufpos;
        e->reply_base = NULL;
        e->reply_off = 0;
        e->reply_len = 0;
        e->reply_big = big;
        e->reply_big_len = (uint32_t)total;
    }
    ec->bufpos = 0;
    ec->buf = saved_buf;
    ec->buf_usable_size = saved_usable;
release_argv:
    /* IO threads receive only sole-reference argv objects for terminal frees. A part's objects are
     * its batch entry's, which the IO thread frees: hand the entry what the part's argv now holds, as
     * a command may have replaced an argument object (an encoded or key-embedded value). */
    for (int j = 0; j < e->argc; j++) {
        robj *o = e->argv[j];
        if (o) {
            unsigned rc = objectGetRefcount(o);
            if (rc > 1) { /* rc == 1: sole reference, freed by the IO thread */
                if (rc < OBJ_FIRST_SPECIAL_REFCOUNT) decrRefCount(o);
                e->argv[j] = NULL;
            }
        }
        if (e->part && ((fpPart *)e)->orig_idx[j] >= 0) ((fpPart *)e)->orig->argv[((fpPart *)e)->orig_idx[j]] = e->argv[j];
    }
}

void fastpathStagedWriteLane(int tid) {
    int lane = fp_lane_main ? tid : tid - 1;
    if (lane < 0 || lane >= fp_nlanes) return;
    fpLane *L = &fp_lanes[lane];
    execContext *saved = ectx;
    ectx = &L->ctx;
    ectx->cmd_time_snapshot = fp_phase_time;
    fp_cur_lane = L;
    exec_lane = 1;
    if (L->frames) sdsclear(L->frames); /* main fed the previous phase's frames in its merge */
    /* The lane's entries of this phase, in T order, so their keys can be prefetched ahead of them. */
    L->nrun = 0;
    for (int p = 0; p < fp_nparts; p++) {
        int t = fp_parts[p].tid;
        uint32_t s = fp_parts[p].seg;
        fpThread *th = &fp_threads[t];
        int *cur = &L->cursor[t];
        while (*cur < th->lane_n[lane]) {
            cmdEntry *e = th->lane_ents[lane][*cur];
            if (e->seg > s) break;
            (*cur)++;
            if (e->seg != s) continue; /* an earlier segment's entry ran serially on main */
            FP_GROW(L->run, L->nrun, L->run_cap, 256);
            L->run[L->nrun++] = e;
        }
    }
    int use_prefetch = server.io_threads_lane_prefetch && L->nrun > 1;
    if (use_prefetch && prefetchLaneBatchSize(L->pf) != server.prefetch_batch_max_size) {
        prefetchLaneBatchFree(L->pf);
        L->pf = prefetchLaneBatchCreate();
    }
    if (!L->pf) use_prefetch = 0;
    if (use_prefetch) prefetchLaneBatchUse(L->pf);
    for (int i = 0; i < L->nrun;) {
        int end = L->nrun;
        if (use_prefetch) {
            /* Prefetch the keys of the next entries until the batch is full, then run exactly those. */
            getKeysResult result;
            initGetKeysResult(&result);
            int room = 1;
            end = i;
            while (end < L->nrun && room) {
                cmdEntry *e = L->run[end++];
                if (e->cmd && !(e->read_flags & READ_FLAGS_BAD_ARITY))
                    room = prefetchBatchAddCommand(e->cmd, e->argv, e->argc, e->db, e->slot, &result);
            }
            getKeysFreeResult(&result);
            prefetchBatchRun();
            prefetchBatchReset();
            L->prefetches++;
        }
        for (; i < end; i++) fpLaneExecute(L, L->run[i]);
    }
    if (use_prefetch) prefetchLaneBatchUse(NULL);
    exec_lane = 0;
    fp_cur_lane = NULL;
    ectx = saved;
}

/* Main, after a write or apply phase: fold what lanes [0, nlanes) accumulated. A lane error of the primary's
 * stream is logged as main logs one for the primary client. */
static void fpFoldLaneStats(int nlanes, int from_primary) {
    execContext *m = &server.exec_ctx;
    for (int l = 0; l < nlanes; l++) {
        fpLane *L = &fp_lanes[l];
        execContext *x = &L->ctx;
        m->dirty += x->dirty;
        m->stat_numcommands += x->stat_numcommands;
        m->stat_keyspace_hits += x->stat_keyspace_hits;
        m->stat_keyspace_misses += x->stat_keyspace_misses;
        m->stat_expiredkeys += x->stat_expiredkeys;
        m->stat_expiredfields += x->stat_expiredfields;
        m->stat_total_error_replies += x->stat_total_error_replies;
        x->dirty = x->stat_numcommands = x->stat_keyspace_hits = x->stat_keyspace_misses = 0;
        x->stat_expiredkeys = x->stat_expiredfields = x->stat_total_error_replies = x->prev_err_count = 0;
        for (int i = 0; i < L->ncs; i++) {
            laneCmdStat *cs = &L->cs[L->cs_touched[i]];
            cs->cmd->calls += cs->calls;
            cs->cmd->microseconds += cs->usec;
            cs->cmd->failed_calls += cs->failed;
            *cs = (laneCmdStat){0};
        }
        L->ncs = 0;
        if (L->dur_cnt) {
            durationStats *ds = &server.duration_stats[EL_DURATION_TYPE_CMD];
            ds->cnt += L->dur_cnt;
            ds->sum += L->dur_sum;
            if ((long long)ds->max < L->dur_max) ds->max = L->dur_max;
            L->dur_cnt = L->dur_sum = L->dur_max = 0;
        }
        for (int i = 0; i < L->nerrors; i++) {
            incrementErrorStatsForReply(L->errors[i].msg, sdslen(L->errors[i].msg), L->errors[i].flags);
            if (from_primary) {
                const char *cmdname = L->errors[i].cmd ? L->errors[i].cmd->fullname : "<unknown>";
                serverLog(LL_WARNING,
                          "== CRITICAL == This replica is sending an error to its primary: '%.*s' after processing "
                          "the command '%s'",
                          (int)sdslen(L->errors[i].msg), L->errors[i].msg, cmdname);
                if (server.repl_backlog && server.repl_backlog->histlen > 0) showLatestBacklog();
                server.stat_unexpected_error_replies++;
                if (server.repl_replica_ro &&
                    (server.propagation_error_behavior == PROPAGATION_ERR_BEHAVIOR_PANIC ||
                     server.propagation_error_behavior == PROPAGATION_ERR_BEHAVIOR_PANIC_ON_REPLICAS))
                    serverPanic("This replica panicked sending an error to its primary after processing the "
                                "command '%s'",
                                cmdname);
            }
            sdsfree(L->errors[i].msg);
        }
        L->nerrors = 0;
        for (int i = 0; i < L->nlat; i++) latencyAddSample(L->lat[i].event, L->lat[i].us);
        L->nlat = 0;
        for (int i = 0; i < L->nlogs; i++) commandlogPushPrepared(L->logs[i].entry, L->logs[i].type);
        L->nlogs = 0;
        for (int i = 0; i < L->ncloses; i++) fpCloseOverLimit(L->closes[i]);
        L->ncloses = 0;
        fp_stat_lane_entries += L->executed;
        L->executed = 0;
        fp_stat_lane_prefetches += L->prefetches;
        L->prefetches = 0;
        if (L->ec) {
            L->ec->origin = NULL;
            clientSetUser(L->ec, DefaultUser, 0); /* never keep a principal that may retire */
        }
    }
}

/* Main, merge after a write phase: propagate a split entry's parts as one transaction, in part order.
 * Returns 0 when no part propagated anything. The propagation mode is the phase's, so either every
 * part formatted frames or every part captured ops. */
static int fpFeedSplitPropagation(cmdEntry *e) {
    static sds frames = NULL;
    fpSplit *s = e->split;
    int nframes = 0, nprop = 0, nops = 0;
    for (int p = 0; p < s->nparts; p++) {
        cmdEntry *pe = &s->parts[p]->e;
        if (pe->frames_len) {
            nframes++;
        } else if (pe->prop) {
            nprop++;
            nops += pe->prop->numops;
        }
    }
    if (!nframes && !nprop) return 0;
    serverAssert(!nframes || !nprop);
    ectx->current_client = fp_lanes[e->lane].ec;
    if (nframes) {
        if (!frames) frames = sdsempty();
        sdsclear(frames);
        int wrap = nframes > 1;
        for (int p = 0; p < s->nparts; p++) {
            cmdEntry *pe = &s->parts[p]->e;
            if (!pe->frames_len) continue;
            frames = sdscatlen(frames, fp_lanes[pe->lane].frames + pe->frames_off, pe->frames_len);
            wrap |= pe->frames_wrap;
        }
        replicationFeedFrames(e->db->id, frames, sdslen(frames), wrap);
    } else {
        serverOp *ops = zmalloc(sizeof(serverOp) * (size_t)nops);
        int n = 0, wrap = nprop > 1;
        for (int p = 0; p < s->nparts; p++) {
            cmdEntry *pe = &s->parts[p]->e;
            if (!pe->prop) continue;
            memcpy(ops + n, pe->prop->ops, sizeof(serverOp) * (size_t)pe->prop->numops);
            n += pe->prop->numops;
            wrap |= pe->prop->wrap;
            zfree(pe->prop->ops);
            zfree(pe->prop);
            pe->prop = NULL;
        }
        replayLanePropagation(ops, n, wrap);
        zfree(ops);
    }
    ectx->current_client = NULL;
    return 1;
}

/* Main, after a write phase: fold what the lanes accumulated and replay their propagation in T order. */
static void fpMergeLanes(void) {
    fpFoldLaneStats(fp_nlanes, 0);
    /* Propagation in T order: each part's entries in its IO thread's harvest order. A client's writes
     * of one iteration are contiguous there, so its replication offset is stored once per run. With no
     * sink fed the lanes kept nothing to propagate. */
    if (fp_prop_mode == FP_PROP_NONE) return;
    client *last = NULL;
    long long last_woff = 0;
    for (int p = 0; p < fp_nparts; p++) {
        fpThread *t = &fp_threads[fp_parts[p].tid];
        uint32_t s = fp_parts[p].seg;
        while (t->mcur_b < t->sbn[0]) {
            cmdBatch *b = t->sb[0][t->mcur_b];
            while (t->mcur_e < b->count) {
                cmdEntry *e = &b->e[t->mcur_e];
                if (e->seg > s) goto next_part;
                t->mcur_e++;
                if (e->seg < s || e->lane < 0) continue;
                long long offset = server.primary_repl_offset;
                if (e->split) {
                    if (!fpFeedSplitPropagation(e)) continue;
                } else {
                    if (!e->frames_len && !e->prop) continue;
                    /* CLUSTER SLOT-STATS attributes replication bytes to the current client's slot. */
                    client *attr = fp_lanes[e->lane].ec;
                    attr->slot = e->shard;
                    ectx->current_client = attr;
                    if (e->frames_len) {
                        replicationFeedFrames(e->db->id, fp_lanes[e->lane].frames + e->frames_off, e->frames_len,
                                              e->frames_wrap);
                    } else {
                        replayLanePropagation(e->prop->ops, e->prop->numops, e->prop->wrap);
                        zfree(e->prop->ops);
                        zfree(e->prop);
                        e->prop = NULL;
                    }
                    ectx->current_client = NULL;
                    attr->slot = -1;
                }
                /* WAIT and WAITAOF on the client wait for its last write's replication offset. */
                if (server.primary_repl_offset != offset && e->c) {
                    if (last && last != e->c) last->woff = last_woff;
                    last = e->c;
                    last_woff = server.primary_repl_offset;
                }
            }
            t->mcur_b++;
            t->mcur_e = 0;
        }
    next_part:;
    }
    if (last) last->woff = last_woff;
}

/* ---------------------------------------------------------------------------------------------
 * Replica: the primary's stream on the write lanes. repl_apply.c plans it and runs the segments;
 * these run its commands on the lanes.
 * --------------------------------------------------------------------------------------------- */

static _Thread_local execContext *fp_stream_saved_ectx = NULL;

/* As fpLanesAllowed, for the stream a replica applies. Main proxies the stream to the backlog and the
 * sub-replicas itself, so of the sinks only the AOF, which execution feeds, needs stream order. */
int fastpathStreamLanesAllowed(void) {
    if (!server.io_threads_parallel_writes || fpDepth() != 0) return 0;
    if (server.cluster_enabled ? (clusterIsAnySlotExporting() || clusterIsAnySlotImporting())
                               : !server.keyspace_shard_bits)
        return 0;
    if (!server.primary_host || server.loading || server.async_loading) return 0;
    if (server.aof_state != AOF_OFF) return 0;
    if (server.paused_actions || listLength(server.monitors)) return 0;
    if (!trackingIsIdle()) return 0;
    if (server.notify_keyspace_events || moduleObservesWrites()) return 0;
    if (bgIteration_iterationActive() || throttle_anyThrottlers()) return 0;
    if (server.maxmemory || server.maxmemory_clients) return 0;
    if (server.blocked_clients_by_type[BLOCKED_MODULE]) return 0;
    int blocked = server.blocked_clients_by_type[BLOCKED_LIST] || server.blocked_clients_by_type[BLOCKED_ZSET] ||
                  server.blocked_clients_by_type[BLOCKED_STREAM];
    if ((blocked || server.watching_clients) && !fpKeyDictsSettled()) return 0;
    if (hotkeysEnabled() || server.busy_module_yield_flags) return 0;
    return 1;
}

int fastpathKeysShard(serverDb *db, struct serverCommand *cmd, robj **argv, int argc, int slot) {
    return fpKeysShard(db, cmd, argv, argc, slot);
}

void fastpathStreamLanesPrepare(int nlanes, int main_lane) {
    fpMarkLaneCommands();
    for (int l = 0; l < nlanes; l++) {
        fpLane *L = &fp_lanes[l];
        if (!L->sec) {
            client *ec = createClient(NULL);
            ec->flag.primary = 1; /* what a command sees of the primary client: no reply, no expiry */
            ec->user = NULL;      /* as the primary client: no ACL */
            L->sec = ec;
        }
        L->sec->cur_tid = main_lane ? l : l + 1;
    }
    updateCachedTime(0);
    fp_phase_time = server.mstime;
    fp_prop_mode = FP_PROP_NONE;
}

void fastpathStreamLaneEnter(int lane) {
    fpLane *L = &fp_lanes[lane];
    fp_stream_saved_ectx = ectx;
    ectx = &L->ctx;
    ectx->cmd_time_snapshot = fp_phase_time;
    fp_cur_lane = L;
    exec_lane = 1;
}

void fastpathStreamLaneCall(int lane, serverDb *db, struct serverCommand *cmd, robj **argv, int argc, int argv_len, size_t argv_len_sum, unsigned long long input_bytes, int shard) {
    fpLane *L = &fp_lanes[lane];
    client *ec = L->sec;
    ec->argv = argv;
    ec->argc = argc;
    ec->argv_len = argv_len;
    ec->argv_len_sum = argv_len_sum;
    ec->net_input_bytes_curr_cmd = input_bytes;
    ec->parsed_cmd = cmd;
    ec->cmd = ec->lastcmd = ec->realcmd = cmd;
    ec->slot = shard;
    ec->db = db;
    ec->flag.pending_command = 1;
    ectx->current_client = ec;
    fpLaneCall(L, ec);
    if (ectx->also_propagate.numops) serverOpArrayFree(&ectx->also_propagate);
    commandProcessed(ec); /* frees the arguments */
    ectx->current_client = NULL;
}

void fastpathStreamLaneLeave(void) {
    exec_lane = 0;
    fp_cur_lane = NULL;
    ectx = fp_stream_saved_ectx;
}

void fastpathStreamLanesMerge(int nlanes) {
    fpFoldLaneStats(nlanes, 1);
}

/* Main: execute thread tid's segment s on main, in T order: its lane entries between the barriers
 * that bound it. A thread's batches are walked once per write phase across all its segments. */
static int fpSerialSegment(int tid, uint32_t s, int use_prefetch) {
    fpThread *t = &fp_threads[tid];
    client *ec = fpExecutor(tid);
    int n = 0;
    int pf_end = 0;
    while (t->scur_b < t->sbn[0]) {
        cmdBatch *b = t->sb[0][t->scur_b];
        while (t->scur_e < b->count) {
            cmdEntry *e = &b->e[t->scur_e];
            if (e->seg > s || (e->lane < 0 && e->seg == s)) goto done; /* the next segment, or this one's barrier */
            t->scur_e++;
            if (e->seg < s) continue; /* ran earlier: on a lane, or as a barrier */
            if (use_prefetch && t->scur_e > pf_end) pf_end = fpPrefetchRange(b, t->scur_e - 1, b->count);
            fpExecute(ec, b, e);
            n++;
        }
        t->scur_b++;
        t->scur_e = 0;
        pf_end = 0;
    }
done:
    ec->origin = NULL;
    clientSetUser(ec, DefaultUser, 0); /* never keep a principal that may retire */
    return n;
}

/* Main: run the pending segment parts on the lanes, or on main when too small to pay for a phase. */
static int fpRunSegment(int pending, int serial, int use_prefetch) {
    if (!fp_nparts) return 0;
    int n = 0;
    if (pending == 0) {
        /* nothing to run */
    } else if (serial || pending < server.io_threads_parallel_writes_min) {
        for (int p = 0; p < fp_nparts; p++) n += fpSerialSegment(fp_parts[p].tid, fp_parts[p].seg, use_prefetch);
        fp_stat_serial_segments++;
    } else {
        for (int l = 0; l < fp_nlanes; l++) {
            fpLane *L = &fp_lanes[l];
            if (!L->ec) {
                L->ec = createClient(NULL);
                L->ec->flag.executor = 1;
                L->ec->cur_tid = fp_lane_main ? l : l + 1;
            }
        }
        updateCachedTime(0);
        fp_phase_time = server.mstime;
        fp_prop_mode = fpPropMode();
        dbLanePhaseBegin();
        stagedRunWritePhase();
        dbLanePhaseEnd();
        fpMergeLanes();
        fp_stat_lane_phases++;
        n = pending;
    }
    fp_nparts = 0;
    return n;
}

static void fpAddPart(int tid, uint32_t seg) {
    FP_GROW(fp_parts, fp_nparts, fp_parts_cap, 16);
    fp_parts[fp_nparts++] = (fpSegPart){.tid = tid, .seg = seg};
}

/* Main: prefetch the keys of thread t's barriers from barrier s on, up to the prefetch batch size. */
static void fpPrefetchBarriers(fpThread *t, int s) {
    getKeysResult result;
    initGetKeysResult(&result);
    int room = 1;
    int k = s;
    for (; k < t->nbarriers && room; k++) {
        cmdEntry *e = t->barriers[k].e;
        if (!e->cmd || (e->read_flags & READ_FLAGS_BAD_ARITY)) continue;
        room = prefetchBatchAddCommand(e->cmd, e->argv, e->argc, e->db, e->slot, &result);
    }
    t->pf_barrier = k;
    getKeysFreeResult(&result);
    prefetchBatchRun();
    prefetchBatchReset();
}

/* Main, write phase with lanes: T split by the barrier entries into segments; each segment runs on the
 * lanes in parallel, each barrier alone on main between them. */
static int fpExecuteWithLanes(void) {
    int total = 0;
    int use_prefetch = prefetchBatchEnabled() && !ProcessingEventsWhileBlocked;
    int serial = !fpLanesAllowed();
    for (int l = 0; l < fp_nlanes; l++) memset(fp_lanes[l].cursor, 0, sizeof(int) * fp_slots);
    fp_nparts = 0;
    int pending = 0;
    for (int tid = 0; tid < fp_slots; tid++) { /* 0: the commands main read in its share of the read phase */
        fpThread *t = &fp_threads[tid];
        if (t->sbn[0] == 0) continue;
        for (int s = 0; s <= t->nbarriers; s++) {
            fpAddPart(tid, (uint32_t)s);
            pending += t->seg_count[s];
            if (s == t->nbarriers) break;
            total += fpRunSegment(pending, serial, use_prefetch);
            pending = 0;
            if (use_prefetch && s >= t->pf_barrier) fpPrefetchBarriers(t, s);
            struct fpBarrier *br = &t->barriers[s];
            client *ec = fpExecutor(tid);
            fpExecute(ec, br->b, br->e);
            ec->origin = NULL;
            clientSetUser(ec, DefaultUser, 0);
            total++;
            fp_stat_barriers++;
            if (!serial && !fpLanesAllowed()) serial = 1;
        }
    }
    total += fpRunSegment(pending, serial, use_prefetch);
    return total;
}

/* Main, write phase: execute every batch the read phase published, filling each entry's reply. */
int fastpathStagedExecute(void) {
    int d = fpDepth();
    if (d == 0 && fp_lanes_iter) return fpExecuteWithLanes();
    int total = 0;
    int use_prefetch = prefetchBatchEnabled() && !ProcessingEventsWhileBlocked;
    int paused = isPausedActions(PAUSE_ACTION_CLIENT_ALL | PAUSE_ACTION_CLIENT_WRITE);
    for (int tid = 0; tid < fp_slots; tid++) {
        fpThread *t = &fp_threads[tid];
        if (t->sbn[d] == 0) continue;
        client *ec = fpExecutor(tid);
        for (int i = 0; i < t->sbn[d]; i++) {
            cmdBatch *b = t->sb[d][i];
            if (use_prefetch && !paused) {
                for (int k = 0; k < b->count;) {
                    int end = fpPrefetchRange(b, k, b->count);
                    for (; k < end; k++) fpExecute(ec, b, &b->e[k]);
                }
            } else {
                for (int k = 0; k < b->count; k++) fpExecute(ec, b, &b->e[k]);
            }
            ec->origin = NULL;
            clientSetUser(ec, DefaultUser, 0); /* never keep a principal that may retire */
            total += b->count;
        }
    }
    return total;
}

int fastpathStagedRepliesPending(void) {
    int d = fpDepth();
    for (int tid = 0; tid < fp_slots; tid++)
        if (fp_threads[tid].sbn[d]) return 1;
    return 0;
}

/* Streaming executor: main executes runs round-robin across threads so every worker gets replies
 * early, while the STREAM-phase workers deliver completed runs concurrently. */

/* Per-thread streaming cursor: the next run to execute, as a batch index and an entry index. */
typedef struct fpStreamCursor {
    int bi; /* batch index into t->sb[d] */
    int ei; /* entry index into that batch */
} fpStreamCursor;

static _Atomic int fp_stream_abort = 0; /* main asks STREAM workers to stop before the barrier close */

/* True once no thread has a run left to execute. */
static int fpStreamDone(fpThread **ts, fpStreamCursor *cur, int n, int d) {
    for (int i = 0; i < n; i++) {
        fpThread *t = ts[i];
        if (cur[i].bi < t->sbn[d]) return 0;
    }
    return 1;
}

/* Advance one thread's cursor to the start of its next run, skipping empty batches. Returns the
 * batch of the run at the cursor, or NULL when the thread is exhausted. */
static cmdBatch *fpStreamRunAt(fpThread *t, fpStreamCursor *c, int d) {
    while (c->bi < t->sbn[d]) {
        cmdBatch *b = t->sb[d][c->bi];
        if (c->ei < b->count) return b;
        c->bi++;
        c->ei = 0;
    }
    return NULL;
}

/* Execute the one run of client b->e[c->ei] starting at the cursor and move the cursor past it.
 * The caller publishes b->executed at chunk granularity (always on a run boundary, since a run runs
 * whole), so a STREAM worker that reads executed with acquire sees every reply byte of the runs in
 * [prev published, executed). Prefetch is done by the caller window. */
static void fpStreamExecRun(client *ec, cmdBatch *b, fpStreamCursor *c, int max) {
    int from = c->ei;
    client *cl = b->e[from].c;
    int j = from;
    while (j < b->count && b->e[j].c == cl && j - from < max) {
        fpExecute(ec, b, &b->e[j]);
        j++;
    }
    c->ei = j;
}

/* True when the cursor sits at the start of a run (or the end of its batch). */
static int fpStreamAtRunStart(cmdBatch *b, int ei) {
    return ei == 0 || ei >= b->count || b->e[ei].c != b->e[ei - 1].c;
}

/* Publish b->executed up to the executor cursor with a release store, so the STREAM worker can
 * deliver the newly finished runs. Skipped when nothing new finished since the last publish. */
static void fpStreamPublish(cmdBatch *b, int upto) {
    if (upto > atomic_load_explicit(&b->executed, memory_order_relaxed))
        atomic_store_explicit(&b->executed, upto, memory_order_release);
}

/* Prefetch the keys of up to `window` entries, in the order the executor runs them (one run per
 * thread per round), without disturbing the executor cursors. Stops when the prefetch batch is
 * full; returns how many entries it covered, at least 1, which the executor then runs. */
static int fpStreamPrefetchWindow(fpThread **ts, fpStreamCursor *cur, int n, int d, int window) {
    getKeysResult result;
    initGetKeysResult(&result);
    fpStreamCursor look[IO_THREADS_MAX_NUM];
    for (int i = 0; i < n; i++) look[i] = cur[i];
    int added = 0, active = 1, room = 1;
    ClientControl *last_control = NULL;
    while (active && room && added < window) {
        active = 0;
        for (int i = 0; i < n && room && added < window; i++) {
            cmdBatch *b = fpStreamRunAt(ts[i], &look[i], d);
            if (!b) continue;
            active = 1;
            client *cl = b->e[look[i].ei].c;
            int j = look[i].ei;
            while (j < b->count && b->e[j].c == cl && room && added < window) {
                cmdEntry *e = &b->e[j];
                if (e->control != last_control) {
                    __builtin_prefetch(e->control);
                    last_control = e->control;
                }
                if (e->cmd && !(e->read_flags & READ_FLAGS_BAD_ARITY))
                    room = prefetchBatchAddCommand(e->cmd, e->argv, e->argc, e->db, e->slot, &result);
                j++;
                added++;
            }
            look[i].ei = j;
        }
    }
    getKeysFreeResult(&result);
    prefetchBatchRun();
    prefetchBatchReset();
    return added ? added : 1;
}

/* After a chunk is published, open a fresh-line gap in the batch arena so the next reply main
 * writes starts at least STREAM_ARENA_GUARD bytes past the published end, on a 64-byte boundary. A
 * STREAM worker reading the published chunk and its prefetcher then never touch the line main writes
 * next. The skipped bytes are wasted arena; account them. A reply that no longer fits spills to the heap
 * exactly as without the guard. */
#define STREAM_ARENA_GUARD 256
static void fpStreamArenaGuard(cmdBatch *b) {
    size_t want = b->arena_used + STREAM_ARENA_GUARD;
    want = (want + 63) & ~(size_t)63; /* round up to a 64-byte line */
    if (want >= b->arena_cap) return; /* no room: the next reply spills as usual */
    server.stat_staged_stream_arena_guard_bytes += (long long)(want - b->arena_used);
    b->arena_used = want;
}

/* Main, write phase (exec/loop overlap): execute runs round-robin across threads, publishing each
 * batch's executed count as runs finish, so STREAM workers can deliver them while main runs on. */
int fastpathStagedExecuteStreaming(void) {
    int d = fpDepth();
    int use_prefetch = prefetchBatchEnabled() && !ProcessingEventsWhileBlocked;
    int paused = isPausedActions(PAUSE_ACTION_CLIENT_ALL | PAUSE_ACTION_CLIENT_WRITE);
    int window = server.prefetch_batch_max_size;
    if (window < 1) window = 1;

    fpThread *ts[IO_THREADS_MAX_NUM];
    fpStreamCursor cur[IO_THREADS_MAX_NUM];
    client *ecs[IO_THREADS_MAX_NUM];
    int n = 0, total = 0;
    /* tid 0 holds main's own read share; main delivers it in the reply phase, not in STREAM. */
    for (int tid = 0; tid < fp_slots; tid++) {
        fpThread *t = &fp_threads[tid];
        if (t->sbn[d] == 0) continue;
        ts[n] = t;
        cur[n].bi = 0;
        cur[n].ei = 0;
        ecs[n] = fpExecutor(tid);
        n++;
    }
    if (n == 0) return 0;

    /* Entries main executes for one thread between publishes. */
    const int chunk = 16;
    cmdBatch *pubb[IO_THREADS_MAX_NUM]; /* batch each thread is currently publishing into */
    int acc[IO_THREADS_MAX_NUM];        /* entries executed in that batch since its last publish */
    for (int i = 0; i < n; i++) {
        pubb[i] = NULL;
        acc[i] = 0;
    }

    while (!fpStreamDone(ts, cur, n, d)) {
        /* Run exactly the entries whose keys were just prefetched: with multi-key commands the
         * prefetch batch fills before `window` entries. */
        int run_window = (use_prefetch && !paused) ? fpStreamPrefetchWindow(ts, cur, n, d, window) : window;
        for (int w = 0; w < run_window;) {
            int progressed = 0;
            for (int i = 0; i < n && w < run_window; i++) {
                cmdBatch *b = fpStreamRunAt(ts[i], &cur[i], d);
                if (!b) continue;
                /* A new batch for this thread: finish publishing the previous one so no run is left
                 * unpublished behind the cursor, then start accounting the new batch. */
                if (b != pubb[i]) {
                    if (pubb[i]) fpStreamPublish(pubb[i], pubb[i]->count);
                    pubb[i] = b;
                    acc[i] = 0;
                }
                int before = cur[i].ei;
                fpStreamExecRun(ecs[i], b, &cur[i], run_window - w);
                int ran = cur[i].ei - before;
                acc[i] += ran;
                w += ran;
                total += ran;
                /* Publish a chunk's worth, only at a run boundary: a run is delivered whole. */
                if (acc[i] >= chunk && fpStreamAtRunStart(b, cur[i].ei)) {
                    fpStreamPublish(b, cur[i].ei);
                    fpStreamArenaGuard(b);
                    acc[i] = 0;
                }
                progressed = 1;
            }
            if (!progressed) break;
        }
    }
    /* Publish whatever each thread's last batch has left; stagedStreamClose then drains the workers. */
    for (int i = 0; i < n; i++)
        if (pubb[i]) fpStreamPublish(pubb[i], pubb[i]->count);
    for (int i = 0; i < n; i++) {
        ecs[i]->origin = NULL;
        clientSetUser(ecs[i], DefaultUser, 0); /* never keep a principal that may retire */
    }
    return total;
}

/* IO thread, STREAM phase: deliver this thread's completed runs as main publishes them, advancing
 * delivered within each batch and the thread's batch cursor, until every batch is delivered or main
 * set the abort flag. Undelivered runs are left for the following REPLY phase, which resumes at the
 * same cursor. */
void fastpathStagedStreamPhase(int tid) {
    fpThread *t = &fp_threads[tid];
    int d = fpDepth();
    fp_cur_tid = tid;
    int bi = t->sb_streamed[d];
    while (bi < t->sbn[d]) {
        cmdBatch *b = t->sb[d][bi];
        for (;;) {
            int exec = atomic_load_explicit(&b->executed, memory_order_acquire);
            if (b->delivered < exec) {
                fpDeliverRange(t, b, b->delivered, exec);
                b->delivered = exec;
            }
            if (b->delivered >= b->count) break; /* this batch is fully delivered */
            if (atomic_load_explicit(&fp_stream_abort, memory_order_acquire)) {
                t->sb_streamed[d] = bi; /* REPLY resumes here */
                return;
            }
            /* Spin for the next chunk. Streaming is eligible only when no worker shares main's CPU
             * (stagedStreamEligible), so this worker never steals main's core; a sched_yield would
             * only hand this core to an unrelated thread and delay the next poll. */
            for (int s = 0; s < 1024; s++) {
                if (atomic_load_explicit(&b->executed, memory_order_acquire) > b->delivered) break;
#if defined(__x86_64__) || defined(__i386__)
                __builtin_ia32_pause();
#elif defined(__aarch64__)
                __asm__ __volatile__("isb" ::: "memory");
#endif
            }
        }
        bi++;
    }
    t->sb_streamed[d] = bi;
}

/* Main: open streaming for this iteration (clear the abort flag before the STREAM phase opens). */
void fastpathStagedStreamBegin(void) {
    atomic_store_explicit(&fp_stream_abort, 0, memory_order_relaxed);
}

/* Main: tell STREAM workers to stop at a batch boundary; the barrier close then waits for them. */
void fastpathStagedStreamAbort(void) {
    atomic_store_explicit(&fp_stream_abort, 1, memory_order_release);
}

/* Main: total batches queued for the current depth, to decide whether STREAM and REPLY run. */
int fastpathStagedStreamBatchCount(void) {
    int d = fpDepth();
    int n = 0;
    for (int tid = 1; tid < fp_slots; tid++) n += fp_threads[tid].sbn[d];
    return n;
}

/* Reply phase, on each IO thread and on main (tid 0): write the replies not yet delivered by a
 * preceding STREAM phase, in order, then recycle every batch. Without streaming, sb_streamed and
 * delivered are 0, so it writes all. */
void fastpathStagedReplyPhase(int tid) {
    fpThread *t = &fp_threads[tid];
    int d = fpDepth();
    fp_cur_tid = tid;
    for (int i = t->sb_streamed[d]; i < t->sbn[d]; i++) {
        cmdBatch *b = t->sb[d][i];
        if (b->delivered < b->count) fpDeliverRange(t, b, b->delivered, b->count);
        fpRecycleBatch(t, b);
    }
    for (int i = 0; i < t->sb_streamed[d]; i++) fpRecycleBatch(t, t->sb[d][i]); /* fully delivered in STREAM */
    t->sbn[d] = 0;
    t->sb_streamed[d] = 0;
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
    for (int tid = 1; tid < fp_slots; tid++) { /* main's own share is not IO-threaded */
        fpThread *t = &fp_threads[tid];
        if (!t->ready) continue;
        server.stat_io_reads_processed += t->reads - t->reads_synced;
        server.stat_io_writes_processed += t->writes - t->writes_synced;
        t->reads_synced = t->reads;
        t->writes_synced = t->writes;
    }
    if (!fpStaged()) return;
    for (int tid = 0; tid < fp_slots; tid++) {
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

/* Main, right after staged mode turns on: every eligible client served by the stock loop, including
 * ones connected before the IO threads were added, joins the fast path. One with a command in progress
 * joins once main has nothing further to do for it. */
void fastpathStagedAdoptClients(void) {
    listIter li;
    listNode *ln;
    listRewind(server.clients, &li);
    while ((ln = listNext(&li))) {
        client *c = listNodeValue(ln);
        if (c->flag.fastpath || c->flag.executor || !c->conn) continue;
        if (!fpSessionEligible(c)) continue;
        if (fpReadmit(c))
            c->flag.fp_readmit = 0;
        else
            c->flag.fp_readmit = 1; /* beforeNextClient retries */
    }
}
