/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Fast GET: GET execution on IO threads. See fastget.h. */

#include "server.h"
#include "fastget.h"
#include "io_threads.h"
#include "module.h"
#include "cluster.h"

#include <string.h>
#include <stdatomic.h>

fastGetThreadStats fastget_thread_stats[FASTGET_MAX_IO_THREADS] = {{0}};

/* Stats (with -DIO_LOOKUP_OFFLOAD_STATS) */
#ifdef IO_LOOKUP_OFFLOAD_STATS
fastGetStats fastget_stats[FASTGET_MAX_IO_THREADS] = {{0}};

uint64_t fastGetStatsSum(size_t field_offset) {
    uint64_t sum = 0;
    for (int i = 0; i < FASTGET_MAX_IO_THREADS; i++) {
        _Atomic(uint64_t) *p = (_Atomic(uint64_t) *)((char *)&fastget_stats[i] + field_offset);
        sum += atomic_load_explicit(p, memory_order_relaxed);
    }
    return sum;
}
#endif

/* The command of each kind, resolved on first use. */
static struct serverCommand *fastGetKindCmd(int kind) {
    static struct serverCommand *cmds[FASTGET_KINDS];
    static const char *names[FASTGET_KINDS] = {"get", "sismember", "hget", "mget"};
    if (!cmds[kind]) cmds[kind] = lookupCommandByCString(names[kind]);
    return cmds[kind];
}

static const int fastget_kind_argc[FASTGET_KINDS] = {2, 3, 3, 2}; /* MGET: the fewest */

/* A command of a kind has an argument count the reading thread answers. */
static inline int fastGetKindArgcOk(int kind, int argc) {
    if (kind == FASTGET_KIND_MGET) return argc >= 2 && argc - 1 <= FASTGET_MGET_MAX_KEYS;
    return argc == fastget_kind_argc[kind];
}

/* Main only. Commands answered on IO threads skip processCommand, so they skip the NOAUTH and ACL checks;
 * this byte stands in for both, one bit per kind. It is recomputed at every auth change (clientSetUser),
 * on SELECT (per-db ACLs) and when ACL rules change (fastGetOnAclRulesChanged).
 *
 * A kind's bit is set when the client is authenticated and some selector allows its command on every key
 * (~*, %R~* or allkeys). Users with key patterns go to main, since matching patterns on IO threads would
 * race main changing the user. flag.authenticated is used rather than authRequired(), which reads
 * DefaultUser's flags; those can change with no per-client event, and the flag can only make a client go
 * to main, never let it skip a check. */
void fastGetRecomputeSpecAclOk(client *c) {
    uint8_t ok = 0;
    /* ACL LOAD retires the user a fast-path client still points at; its successor holds the rules. */
    user *u = c->user && (c->user->flags & USER_FLAG_RETIRED) ? ACLResolveUser(c->user) : c->user;
    if (c->flag.authenticated && (u || !c->user)) {
        for (int k = 0; k < FASTGET_KINDS; k++) {
            struct serverCommand *cmd = fastGetKindCmd(k);
            if (!cmd) continue; /* sentinel has none of them */
            if (u == NULL) {
                /* No associated user = unrestricted context (stock semantics). */
                ok |= 1 << k;
                continue;
            }
            /* The check needs only the command and db; key access is decided by the unrestricted-key
             * requirement, so the placeholder arguments can only make the result stricter. */
            robj *argv[3];
            argv[0] = createStringObject(cmd->fullname, strlen(cmd->fullname));
            argv[1] = createStringObject("k", 1);
            argv[2] = createStringObject("f", 1);
            int dbid = c->db ? c->db->id : 0;
            if (ACLUserCheckCmdWithUnrestrictedKeyAccess(u, cmd, argv, fastget_kind_argc[k], dbid, CMD_KEY_ACCESS))
                ok |= 1 << k;
            for (int j = 0; j < 3; j++) decrRefCount(argv[j]);
        }
    }
    atomic_store_explicit(&c->spec_acl_ok, ok, memory_order_release);
}

/* Main only, after ACL SETUSER, DELUSER, LOAD or a module ACL change: recompute every client's gate. */
void fastGetOnAclRulesChanged(void) {
    listIter li;
    listNode *ln;
    listRewind(server.clients, &li);
    while ((ln = listNext(&li))) fastGetRecomputeSpecAclOk((client *)listNodeValue(ln));
}

/* A module subscribed to command results gets one event per command from call(), which GETs answered on
 * IO threads skip, so those stop while any success subscriber exists. They can only succeed (a failure
 * goes to main), so only success subscribers count. */
_Atomic int fastget_module_cmdresult_gate = 0;

void fastGetOnCommandResultListenersChanged(int success_listeners) {
    atomic_store_explicit(&fastget_module_cmdresult_gate, success_listeners > 0, memory_order_release);
}

/* MONITOR is fed from call(), which GETs answered on IO threads skip, so those stop while any monitor is
 * attached. */
_Atomic int fastget_monitor_gate = 0;

void fastGetOnMonitorsChanged(void) {
    atomic_store_explicit(&fastget_monitor_gate, listLength(server.monitors) > 0, memory_order_release);
}

/* Format a RESP2/3 bulk string reply directly into the client's output buffer.
 * Returns bytes written, or 0 if insufficient space (punt to main). */
static int fastGetWriteBulkReply(client *c, const char *val, size_t vallen) {
    /* "$<len>\r\n" with ll2string: snprintf is measurably slower here. */
    char hdr[32];
    hdr[0] = '$';
    int numlen = ll2string(hdr + 1, sizeof(hdr) - 3, (long long)vallen);
    hdr[numlen + 1] = '\r';
    hdr[numlen + 2] = '\n';
    int hdrlen = numlen + 3;
    size_t total = hdrlen + vallen + 2; /* trailing \r\n */
    size_t available = c->buf_usable_size - c->bufpos;
    if (total > available) return 0; /* does not fit: main replies */

    memcpy(c->buf + c->bufpos, hdr, hdrlen);
    c->bufpos += hdrlen;
    memcpy(c->buf + c->bufpos, val, vallen);
    c->bufpos += vallen;
    memcpy(c->buf + c->bufpos, "\r\n", 2);
    c->bufpos += 2;
    if (c->buf_peak < c->bufpos) c->buf_peak = c->bufpos;
    return (int)total;
}

/* Maximum batch depth for prefetch overlap. Matches server.prefetch_batch_max_size default. */
#define FASTGET_BATCH_DEPTH 16

/* Per-key state for the batched prefetch + reply pipeline. */
typedef struct fastGetBatchEntry {
    hashtableIncrementalFindState find_state; /* Incremental find state */
    int is_first_cmd;                         /* 1 if this is c->argv, 0 if queue */
    int queue_idx;                            /* Index in cmd_queue (if !is_first_cmd) */
    robj *arg;                                /* SISMEMBER's member or HGET's field */
} fastGetBatchEntry;

/* IO thread, read phase: this node serves a GET on the slot for client c with no redirect or cluster
 * error. The common states are decided from the slot owner directly: this node owns the slot, or it is a
 * replica of the owner and the client sent READONLY, with the cluster healthy and no slot migrating or
 * importing anywhere. Any other state goes through getNodeByQuery, which reads the command and slot from
 * the client, so they are set for the call. */
static int fastGetClusterServes(client *c, struct serverCommand *cmd, int slot, int read_flags) {
    if (slot < 0 || (read_flags & (READ_FLAGS_CROSSSLOT | READ_FLAGS_NO_KEYS))) return 0;
    if (!isAnySlotInManualMigratingState() && !isAnySlotInManualImportingState() && isClusterHealthy() &&
        !(server.cluster_module_flags & CLUSTER_MODULE_FLAG_NO_REDIRECTION)) {
        clusterNode *owner = getNodeBySlot(slot);
        if (owner == NULL) return 0;
        clusterNode *myself = getMyClusterNode();
        if (owner == myself) return 1;
        return c->flag.readonly && clusterNodeIsReplica(myself) && clusterNodeGetPrimary(myself) == owner;
    }
    if (getMigratingSlotDest(slot) != NULL || getImportingSlotSource(slot) != NULL) return 0;
    struct serverCommand *saved_cmd = c->cmd;
    int saved_slot = c->slot, saved_flags = c->read_flags;
    c->cmd = cmd;
    c->slot = slot;
    c->read_flags = read_flags;
    int err = CLUSTER_REDIR_NONE;
    clusterNode *n = getNodeByQuery(c, &err);
    c->cmd = saved_cmd;
    c->slot = saved_slot;
    c->read_flags = saved_flags;
    return n != NULL && clusterNodeIsMyself(n) && err == CLUSTER_REDIR_NONE;
}

/* The keyspace table of a GET's key. In cluster mode that is the slot the parser already computed. */
static inline int fastGetTableIndex(sds key, int slot) {
    if (server.cluster_enabled && slot >= 0) return slot;
    return getKVStoreIndexForKey(key);
}

/* The commandlog threshold of a type, or -1 while that log records nothing. */
static long long fastGetCommandlogThreshold(int type) {
    if (server.commandlog[type].threshold < 0 || server.commandlog[type].max_len == 0) return -1;
    return server.commandlog[type].threshold;
}

/* Write a short fixed reply into the client's buffer. Returns 0 if it does not fit. */
static int fastGetWriteRaw(client *c, const char *s, size_t len) {
    if (len > c->buf_usable_size - c->bufpos) return 0;
    memcpy(c->buf + c->bufpos, s, len);
    c->bufpos += len;
    if (c->buf_peak < c->bufpos) c->buf_peak = c->bufpos;
    return 1;
}

/* A string argument as sds, or NULL when it is not one. */
static inline sds fastGetArgSds(robj *o) {
    return o && sdsEncodedObject(o) ? (sds)objectGetVal(o) : NULL;
}

/* SISMEMBER key member on a set nobody writes in the read phase. A set table mid-rehash goes to main,
 * since a lookup there takes a rehash step. */
static int fastGetReplySismember(client *c, robj *o, robj *arg) {
    if (objectGetType(o) != OBJ_SET) return 0;
    sds member = fastGetArgSds(arg);
    if (!member) return 0;
    if (objectGetEncoding(o) == OBJ_ENCODING_HASHTABLE && hashtableIsRehashing(objectGetVal(o))) return 0;
    int is = setTypeIsMemberAux(o, member, sdslen(member), 0, 1);
    return fastGetWriteRaw(c, is ? ":1\r\n" : ":0\r\n", 4);
}

/* HGET key field. A hash with fields that expire goes to main, which may reap them; so does a hash
 * table mid-rehash. */
static int fastGetReplyHget(client *c, robj *o, robj *arg) {
    if (objectGetType(o) != OBJ_HASH) return 0;
    sds field = fastGetArgSds(arg);
    if (!field) return 0;
    if (hashTypeHasVolatileFields(o)) return 0;
    if (objectGetEncoding(o) == OBJ_ENCODING_HASHTABLE && hashtableIsRehashing(objectGetVal(o))) return 0;
    unsigned char *vstr = NULL;
    unsigned int vlen = 0;
    long long vll = 0;
    if (hashTypeGetValue(o, field, &vstr, &vlen, &vll, NULL) != C_OK)
        return c->resp == 2 ? fastGetWriteRaw(c, "$-1\r\n", 5) : fastGetWriteRaw(c, "_\r\n", 3);
    char intbuf[LONG_STR_SIZE];
    if (!vstr) {
        vlen = ll2string(intbuf, sizeof(intbuf), vll);
        vstr = (unsigned char *)intbuf;
    }
    if (vlen > FASTGET_MAX_SPECULATIVE_VALUE_LEN) return 0;
    long long reply_threshold = fastGetCommandlogThreshold(COMMANDLOG_TYPE_LARGE_REPLY);
    if (reply_threshold >= 0 && (long long)(1 + digits10(vlen) + 2 + vlen + 2) >= reply_threshold) return 0;
    return fastGetWriteBulkReply(c, (const char *)vstr, vlen) > 0;
}

/* Reply to one prefetched command from the keyspace. Returns 1 on success (reply
 * written), 0 on punt. The keyspace is quiescent while IO threads read: main
 * mutates it only outside the read phase. */
static int fastGetReplyFromEntry(client *c, fastGetBatchEntry *e, int kind) {
    /* A command the large-request commandlog would record goes to main, which records it. */
    long long request_threshold = fastGetCommandlogThreshold(COMMANDLOG_TYPE_LARGE_REQUEST);
    if (request_threshold >= 0) {
        size_t in = e->is_first_cmd ? c->net_input_bytes_curr_cmd : c->cmd_queue.cmds[e->queue_idx].input_bytes;
        if ((long long)in >= request_threshold) return 0;
    }
    void *entry = NULL;
    if (!hashtableIncrementalFindGetResult(&e->find_state, &entry)) {
        /* A miss goes to main, which fires the keymiss notification and counts the miss. */
#ifdef IO_LOOKUP_OFFLOAD_STATS
        FASTGET_STAT_ADD(miss_punts, 1);
#endif
        return 0;
    }

    robj *o = (robj *)entry;

    /* Expired keys punt: only main may lazily delete them. */
    if (o->hasexpire) {
        mstime_t when = objectGetExpire(o);
        if (when >= 0 && mstime() >= when) {
#ifdef IO_LOOKUP_OFFLOAD_STATS
            FASTGET_STAT_ADD(expired_replies, 1);
#endif
            return 0;
        }
    }

    if (kind == FASTGET_KIND_SISMEMBER) return fastGetReplySismember(c, o, e->arg);
    if (kind == FASTGET_KIND_HGET) return fastGetReplyHget(c, o, e->arg);
    if (objectGetType(o) != OBJ_STRING) return 0;

    char intbuf[LONG_STR_SIZE];
    const char *val;
    size_t vallen;
    int encoding = objectGetEncoding(o);
    if (encoding == OBJ_ENCODING_INT) {
        vallen = ll2string(intbuf, sizeof(intbuf), (long long)(long)objectGetVal(o));
        val = intbuf;
    } else if (encoding == OBJ_ENCODING_EMBSTR || encoding == OBJ_ENCODING_RAW) {
        val = objectGetVal(o);
        vallen = sdslen((sds)val);
        if (vallen > FASTGET_MAX_SPECULATIVE_VALUE_LEN) {
#ifdef IO_LOOKUP_OFFLOAD_STATS
            FASTGET_STAT_ADD(large_value_punts, 1);
#endif
            return 0;
        }
    } else {
        return 0;
    }

    /* A reply the large-reply commandlog would record goes to main, which records it. */
    long long reply_threshold = fastGetCommandlogThreshold(COMMANDLOG_TYPE_LARGE_REPLY);
    if (reply_threshold >= 0 && (long long)(1 + digits10(vallen) + 2 + vallen + 2) >= reply_threshold) return 0;

    if (fastGetWriteBulkReply(c, val, vallen) <= 0) return 0;
#ifdef IO_LOOKUP_OFFLOAD_STATS
    FASTGET_STAT_ADD(speculative_hits, 1);
#endif
    return 1;
}

/* The value of a found string key, or 0 when the reading thread does not answer it. */
static int fastGetStringValue(robj *o, char *intbuf, const char **val, size_t *vallen) {
    if (objectGetType(o) != OBJ_STRING) return 0;
    if (o->hasexpire) {
        mstime_t when = objectGetExpire(o);
        if (when >= 0 && mstime() >= when) return 0;
    }
    int encoding = objectGetEncoding(o);
    if (encoding == OBJ_ENCODING_INT) {
        *vallen = ll2string(intbuf, LONG_STR_SIZE, (long long)(long)objectGetVal(o));
        *val = intbuf;
    } else if (encoding == OBJ_ENCODING_EMBSTR || encoding == OBJ_ENCODING_RAW) {
        *val = objectGetVal(o);
        *vallen = sdslen((sds)*val);
        if (*vallen > FASTGET_MAX_SPECULATIVE_VALUE_LEN) return 0;
    } else {
        return 0;
    }
    return 1;
}

/* Answer one MGET whose keys all exist as strings: their finds run interleaved, and the reply is written
 * only once every key is known, so a punt leaves nothing behind. Returns 1 when answered. */
static int fastGetAnswerMget(client *c, robj **argv, int argc, size_t input_bytes) {
    long long request_threshold = fastGetCommandlogThreshold(COMMANDLOG_TYPE_LARGE_REQUEST);
    if (request_threshold >= 0 && (long long)input_bytes >= request_threshold) return 0;
    int n = argc - 1;
    hashtableIncrementalFindState fs[FASTGET_MGET_MAX_KEYS];
    for (int i = 0; i < n; i++) {
        hashtable *ht = kvstoreGetHashtable(c->db->keys, fastGetTableIndex(objectGetVal(argv[i + 1]), -1));
        if (!ht) return 0;
        hashtableIncrementalFindInit(&fs[i], ht, objectGetVal(argv[i + 1]));
    }
    for (int pending = n; pending;) {
        pending = 0;
        for (int i = 0; i < n; i++) pending += hashtableIncrementalFindStep(&fs[i]);
    }
    char intbuf[FASTGET_MGET_MAX_KEYS][LONG_STR_SIZE];
    const char *val[FASTGET_MGET_MAX_KEYS];
    size_t vallen[FASTGET_MGET_MAX_KEYS];
    size_t total = 1 + 20 + 2;
    for (int i = 0; i < n; i++) {
        void *entry = NULL;
        /* A miss goes to main, which fires the keymiss notification and counts the miss. */
        if (!hashtableIncrementalFindGetResult(&fs[i], &entry)) return 0;
        if (!fastGetStringValue(entry, intbuf[i], &val[i], &vallen[i])) return 0;
        total += 1 + 20 + 2 + vallen[i] + 2;
    }
    long long reply_threshold = fastGetCommandlogThreshold(COMMANDLOG_TYPE_LARGE_REPLY);
    if (reply_threshold >= 0 && (long long)total >= reply_threshold) return 0;
    if (total > c->buf_usable_size - c->bufpos) return 0; /* the bound overestimates: main replies */
    char hdr[32];
    hdr[0] = '*';
    int len = ll2string(hdr + 1, sizeof(hdr) - 3, n);
    hdr[len + 1] = '\r';
    hdr[len + 2] = '\n';
    fastGetWriteRaw(c, hdr, len + 3);
    for (int i = 0; i < n; i++) fastGetWriteBulkReply(c, val[i], vallen[i]);
    return 1;
}

/* fastGetSpeculateBatch for a read of MGETs: answer them in order until the first that goes to main. */
static int fastGetSpeculateMget(client *c, int tid, monotime spec_start) {
    if (server.cluster_enabled) return 0;
    int speculated = 0, keys = 0;
    cmdQueue *queue = &c->cmd_queue;
    if (!fastGetAnswerMget(c, c->argv, c->argc, c->net_input_bytes_curr_cmd)) return 0;
    c->read_flags |= READ_FLAGS_FASTGET_SPECULATED;
    speculated++;
    keys += c->argc - 1;
    for (int i = queue->off; i < queue->len; i++) {
        parsedCommand *p = &queue->cmds[i];
        if (!(p->read_flags & READ_FLAGS_PARSING_COMPLETED)) break;
        if (!fastGetAnswerMget(c, p->argv, p->argc, p->input_bytes)) break;
        p->read_flags |= READ_FLAGS_FASTGET_SPECULATED;
        speculated++;
        keys += p->argc - 1;
    }
    /* fastGetConsumeSpeculated counts one keyspace hit per command; an MGET hit every key. */
    fastget_thread_stats[tid].keyspace_hits += keys - speculated;
    fastget_thread_stats[tid].calls[FASTGET_KIND_MGET] += speculated;
    fastget_thread_stats[tid].usec[FASTGET_KIND_MGET] += (long long)(getMonotonicUs() - spec_start);
    return speculated;
}

/* Called from the IO thread after parsing is complete. Executes a contiguous
 * prefix of GET commands, using batched incremental finds to overlap memory
 * prefetches across keys before replying in command order.
 *
 * REPLY ORDERING: all speculative replies are written into c->buf BEFORE the
 * main thread processes any punted command, whose replies append after.
 *
 * Returns the number of commands speculatively completed. */
int fastGetSpeculateBatch(client *c, int tid) {
    int speculated = 0;

    if (server.io_threads_num <= 1) return 0;

    /* io-threads-speculation-replica-only: only a replica answers GETs on IO threads. */
    if (server.io_threads_speculation_replica_only && server.primary_host == NULL) return 0;

    /* Inside MULTI a GET must be queued, not run. A blocked or just-unblocked client's replies must
     * follow its unblock reply. The reply is written as plain RESP into c->buf, which would corrupt a
     * buffer in copy-avoidance encoded mode, and would reorder replies if the reply list is not
     * empty. */
    if (c->flag.multi || c->flag.blocked || c->flag.unblocked ||
        c->flag.buf_encoded || listLength(c->reply) != 0)
        return 0;

    /* A tracking client must have its keys remembered, which this path skips, or a later write would
     * not invalidate its cache. BCAST tracking invalidates by prefix and is unaffected. Main does not
     * run this client's commands while its read is parsed, so the plain flags are stable here. */
    if (c->flag.tracking && !c->flag.tracking_bcast) return 0;

    /* A module subscribed to command results (see fastGetOnCommandResultListenersChanged). */
    if (atomic_load_explicit(&fastget_module_cmdresult_gate, memory_order_acquire)) return 0;

    /* A module command filter may rewrite or redirect the GET. Filters change only on main, which
     * waits at the barrier while IO threads run this. */
    if (moduleHasCommandFilters()) return 0;

    /* An attached monitor (see fastGetOnMonitorsChanged). */
    if (atomic_load_explicit(&fastget_monitor_gate, memory_order_acquire)) return 0;

    /* Only a read that starts with a GET, or a SISMEMBER or HGET with io-threads-speculate-reads, is
     * answered here. */
    int kind = -1;
    for (int k = 0; k < FASTGET_KINDS; k++) {
        if (c->parsed_cmd == fastGetKindCmd(k)) {
            kind = k;
            break;
        }
    }
    if (kind < 0 || !fastGetKindArgcOk(kind, c->argc)) return 0;
    if (kind != FASTGET_KIND_GET && !server.io_threads_speculate_reads) return 0;
    struct serverCommand *kind_cmd = c->parsed_cmd;
    int kind_argc = fastget_kind_argc[kind];

    /* Answer the read here only when every complete command in it is of the same kind. A prefix
     * answered here while the rest waits for main splits one pipeline's replies into two writes; the
     * client then refills its pipeline in pieces, the next reads are smaller, and under load the server
     * settles into serving fragments of a couple of commands per read. */
    for (int i = c->cmd_queue.off; i < c->cmd_queue.len; i++) {
        parsedCommand *p = &c->cmd_queue.cmds[i];
        if (!(p->read_flags & READ_FLAGS_PARSING_COMPLETED)) break; /* the trailing partial */
        if (p->cmd != kind_cmd || !fastGetKindArgcOk(kind, p->argc)) return 0;
    }

    /* In cluster mode only a command this node answers without a redirect: the same check main runs, on
     * cluster state main changes only outside the read phase. A slot being migrated or imported needs
     * key-by-key checks and a possible ASK or TRYAGAIN, so its commands go to main. With per-slot stats
     * on, main runs every command so the stats count it. */
    if (server.cluster_enabled && server.cluster_slot_stats_enabled) return 0;
    if (server.cluster_enabled && !fastGetClusterServes(c, c->parsed_cmd, c->slot, c->read_flags)) return 0;

    /* The client's ACL and auth gate (see fastGetRecomputeSpecAclOk). */
    if (!(atomic_load_explicit(&c->spec_acl_ok, memory_order_acquire) & (1 << kind))) return 0;

    /* With a slow threshold of 0 the commandlog records every command, so every command goes to main,
     * which records it. Large requests and replies are checked per command. */
    if (fastGetCommandlogThreshold(COMMANDLOG_TYPE_SLOW) == 0) return 0;

    /* Time the batch for commandstats parity: speculated commands bypass call(),
     * so their duration must be accumulated per-thread and folded into the
     * command's microseconds by fastGetAggregateStats (as with calls). */
    monotime spec_start = getMonotonicUs();
    if (kind == FASTGET_KIND_MGET) return fastGetSpeculateMget(c, tid, spec_start);

    hashtable *ht = kvstoreGetHashtable(c->db->keys, fastGetTableIndex(objectGetVal(c->argv[1]), c->slot));
    if (!ht) return 0;

    /* STREAMING SLOT-REFILL: a ring of FASTGET_BATCH_DEPTH slots holds in-flight
     * finds. Each sweep steps every pending find once (one prefetch per slot);
     * the head slot replies as soon as its find completes, and the fill loop
     * tops the window back up from the queue, so prefetch depth stays constant
     * across the eligible prefix. The first ineligible command or punt ends
     * speculation for the whole read. head..tail are monotonic; slot index =
     * seq % FASTGET_BATCH_DEPTH; done[] marks completed finds. */
    fastGetBatchEntry batch[FASTGET_BATCH_DEPTH];
    uint8_t done[FASTGET_BATCH_DEPTH];
    cmdQueue *queue = &c->cmd_queue;
    int queue_pos = queue->off;
    int prefix_open = 1;
    unsigned head = 0, tail = 1;

    batch[0].is_first_cmd = 1;
    batch[0].queue_idx = -1;
    batch[0].arg = kind_argc > 2 ? c->argv[2] : NULL;
    hashtableIncrementalFindInit(&batch[0].find_state, ht, objectGetVal(c->argv[1]));
    done[0] = 0;

    while (head < tail || prefix_open) {
        /* Fill: top the window up from the queue. */
        while (prefix_open && tail - head < FASTGET_BATCH_DEPTH && queue_pos < queue->len) {
            parsedCommand *p = &queue->cmds[queue_pos];
            if (p->cmd != kind_cmd || p->argc != kind_argc ||
                (server.cluster_enabled && !fastGetClusterServes(c, p->cmd, p->slot, p->read_flags))) {
                prefix_open = 0; /* ineligible command ends the global prefix */
                break;
            }
            hashtable *pht = kvstoreGetHashtable(c->db->keys, fastGetTableIndex(objectGetVal(p->argv[1]), p->slot));
            if (!pht) {
                prefix_open = 0; /* an unallocated shard ends the prefix; main answers it */
                break;
            }
            fastGetBatchEntry *e = &batch[tail % FASTGET_BATCH_DEPTH];
            e->is_first_cmd = 0;
            e->queue_idx = queue_pos;
            e->arg = kind_argc > 2 ? p->argv[2] : NULL;
            hashtableIncrementalFindInit(&e->find_state, pht, objectGetVal(p->argv[1]));
            done[tail % FASTGET_BATCH_DEPTH] = 0;
            tail++;
            queue_pos++;
        }
        if (queue_pos >= queue->len) prefix_open = 0; /* queue exhausted -- drain */
        if (head == tail) break;                      /* nothing in flight and nothing to fill */

        /* Sweep: one find step per pending slot (one prefetch each). */
        for (unsigned s = head; s < tail; s++) {
            unsigned i = s % FASTGET_BATCH_DEPTH;
            if (!done[i]) done[i] = !hashtableIncrementalFindStep(&batch[i].find_state);
        }

        /* Drain: reply completed HEADS in command order. */
        while (head < tail && done[head % FASTGET_BATCH_DEPTH]) {
            fastGetBatchEntry *e = &batch[head % FASTGET_BATCH_DEPTH];
#ifdef IO_LOOKUP_OFFLOAD_STATS
            FASTGET_STAT_ADD(speculative_attempts, 1);
#endif
            if (!fastGetReplyFromEntry(c, e, kind)) {
                /* Contiguous-prefix rule: in-flight slots behind a punt are
                 * abandoned and their commands fall through to main. */
                prefix_open = 0;
                head = tail;
                break;
            }
            if (e->is_first_cmd)
                c->read_flags |= READ_FLAGS_FASTGET_SPECULATED;
            else
                queue->cmds[e->queue_idx].read_flags |= READ_FLAGS_FASTGET_SPECULATED;
            speculated++;
            head++;
        }
    }

    /* Commandstats parity: fold calls and execution time into the per-thread counters. */
    if (speculated > 0) {
        fastget_thread_stats[tid].calls[kind] += speculated;
        fastget_thread_stats[tid].usec[kind] += (long long)(getMonotonicUs() - spec_start);
    }
    return speculated;
}

/* --- Phase-1 no-enqueue: IO-thread command consumption --- */

/* IO thread: drop the commands fastGetSpeculateBatch answered, so main never runs them. Frees their argv,
 * clears pending_command, advances the command queue past them and counts them per thread. The IO thread
 * owns the client for the whole read phase, so it updates commands_processed itself. Afterwards
 * c->argc is 0 and the queue holds only commands main still has to run. */
void fastGetConsumeSpeculated(client *c, int count, int tid) {
    int consumed = 0;

    /* --- First command (c->argv / c->parsed_cmd case) --- */
    if (count > 0 && (c->read_flags & READ_FLAGS_FASTGET_SPECULATED)) {
        c->read_flags &= ~READ_FLAGS_FASTGET_SPECULATED;
        /* The parser set pending_command; left set, main would run a stale c->cmd with argc 0. */
        c->flag.pending_command = 0;
        /* Freed here, not with freeClientArgv(): that may offload the free to an IO thread, which only
         * main may do (it updates main's job counters and a main-produced queue). */
        for (int j = 0; j < c->argc; j++) decrRefCount(c->argv[j]);
        zfree(c->argv);
        c->argv = NULL;
        c->argc = 0;
        c->cmd = NULL;
        c->parsed_cmd = NULL;
        c->argv_len_sum = 0;
        c->argv_len = 0;
        c->slot = -1;
        c->commands_processed++;
        consumed++;
    }

    /* --- Command queue entries --- */
    cmdQueue *queue = &c->cmd_queue;
    while (consumed < count && queue->off < queue->len) {
        parsedCommand *p = &queue->cmds[queue->off];
        if (!(p->read_flags & READ_FLAGS_FASTGET_SPECULATED)) break;
        p->read_flags &= ~READ_FLAGS_FASTGET_SPECULATED;
        queue->off++;
        /* Free argv for the consumed command. */
        for (int j = 0; j < p->argc; j++) {
            decrRefCount(p->argv[j]);
        }
        zfree(p->argv);
        p->argv = NULL;
        p->argc = 0;
        c->commands_processed++;
        consumed++;
    }

    /* Compact queue if fully consumed. */
    if (queue->off == queue->len) {
        queue->off = queue->len = 0;
    }

    /* Accumulate into per-IO-thread counter (no shared-line write). */
    fastget_thread_stats[tid].commands_processed += consumed;
    /* Every answered GET is a keyspace hit: misses, expired keys and large values go to main. Main folds
     * this into stat_keyspace_hits, which lookupKey would have counted. */
    fastget_thread_stats[tid].keyspace_hits += consumed;
}

/* Main: fold the per-thread counts of GETs answered on IO threads into stat_numcommands,
 * stat_keyspace_hits and GET's commandstats. Runs in beforeSleep, and also before CONFIG RESETSTAT
 * zeroes the stats and before INFO renders them, so a reset clears pending counts and a read includes
 * them. */
void fastGetAggregateStats(void) {
    /* IO threads only ever increase their counters; main folds the growth since its last snapshot.
     * Zeroing a counter from main would race an IO thread's increment and lose counts, while a stale
     * read here only defers counts to the next fold. */
    static long long seen_n[FASTGET_MAX_IO_THREADS] = {0};
    static long long seen_calls[FASTGET_MAX_IO_THREADS][FASTGET_KINDS] = {{0}};
    static long long seen_usec[FASTGET_MAX_IO_THREADS][FASTGET_KINDS] = {{0}};
    static long long seen_hits[FASTGET_MAX_IO_THREADS] = {0};
    for (int i = 0; i < server.io_threads_num; i++) {
        long long n = fastget_thread_stats[i].commands_processed;
        if (n > seen_n[i]) {
            ectx->stat_numcommands += n - seen_n[i];
            seen_n[i] = n;
        }
        /* Keyspace hits, folded the same way. */
        long long h = fastget_thread_stats[i].keyspace_hits;
        if (h > seen_hits[i]) {
            ectx->stat_keyspace_hits += h - seen_hits[i];
            seen_hits[i] = h;
        }
        for (int k = 0; k < FASTGET_KINDS; k++) {
            struct serverCommand *cmd = fastGetKindCmd(k);
            long long calls = fastget_thread_stats[i].calls[k];
            long long calls_delta = 0;
            if (calls > seen_calls[i][k]) {
                calls_delta = calls - seen_calls[i][k];
                seen_calls[i][k] = calls;
                if (cmd) cmd->calls += calls_delta;
            }
            long long us = fastget_thread_stats[i].usec[k];
            long long us_delta = 0;
            if (us > seen_usec[i][k]) {
                us_delta = us - seen_usec[i][k];
                seen_usec[i][k] = us;
                if (cmd) cmd->microseconds += us_delta;
            }
            /* Speculated commands count in LATENCY HISTOGRAM and latencystats at their average duration. */
            if (cmd && calls_delta > 0 && server.latency_tracking_enabled) {
                int64_t avg_ns = (int64_t)(us_delta * 1000 / calls_delta);
                if (avg_ns < 1) avg_ns = 1;
                updateCommandLatencyHistogramCount(&cmd->latency_histogram, avg_ns, calls_delta);
            }
        }
    }
}

sds fastGetInfoString(sds info) {
    long long speculated = 0;
    for (int i = 0; i < FASTGET_MAX_IO_THREADS; i++) speculated += fastget_thread_stats[i].commands_processed;
    info = sdscatprintf(info, "# Fastget\r\nfastget_speculated_commands:%lld\r\n", speculated);
#ifdef IO_LOOKUP_OFFLOAD_STATS
    info = sdscatprintf(info,
                        "fastget_speculative_attempts:%llu\r\n"
                        "fastget_speculative_hits:%llu\r\n"
                        "fastget_large_value_punts:%llu\r\n"
                        "fastget_expired_replies:%llu\r\n"
                        "fastget_miss_punts:%llu\r\n",
                        (unsigned long long)FASTGET_STAT_SUM(speculative_attempts),
                        (unsigned long long)FASTGET_STAT_SUM(speculative_hits),
                        (unsigned long long)FASTGET_STAT_SUM(large_value_punts),
                        (unsigned long long)FASTGET_STAT_SUM(expired_replies),
                        (unsigned long long)FASTGET_STAT_SUM(miss_punts));
#endif
    return info;
}
