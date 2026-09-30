/* Parallel apply of the replication stream on a replica. See repl_apply.h. */

#include "server.h"
#include "repl_apply.h"
#include "staged_loop.h"
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

int replApplyMayDefer(void) {
    return server.replica_parallel_apply && server.keyspace_shard_bits && stagedApplyReady();
}

int replApplyPending(void) {
    return ra_pending;
}

/* Nothing that observes an individual write may be active: those writes must go through call(). */
static int replApplyAllowed(client *c) {
    if (!server.replica_parallel_apply || !server.keyspace_shard_bits) return 0;
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
    client *old = server.current_client;
    server.current_client = c; /* replicated writes never see a key as expired */
    kvstoreSetConcurrent(1);
    stagedRunApplyPhase(ra_nlanes > 1 || !ra_main_lane);
    kvstoreSetConcurrent(0);
    server.current_client = old;

    monotime usec = 0;
    for (int i = 0; i < ra_nlanes; i++) {
        usec += ra_lanes[i].usec;
        ra_lanes[i].usec = 0;
        ra_lanes[i].len = 0;
    }
    server.dirty += ra_pending;
    server.stat_numcommands += ra_pending;
    server.stat_repl_parallel_applied += ra_pending;
    server.stat_repl_apply_phases++;
    ra_set_cmd->calls += ra_pending;
    ra_set_cmd->microseconds += usec;
    ra_pending = 0;
}
