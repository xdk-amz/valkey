/* D+ -- GET execution on IO threads. See dplus.h. */

#include "server.h"
#include "dplus.h"
#include "io_threads.h"

#include <string.h>
#include <stdatomic.h>

dplusThreadStats dplus_thread_stats[DPLUS_MAX_IO_THREADS] = {{0}};

/* --- Component 6: Stats --- */
#ifdef IO_LOOKUP_OFFLOAD_STATS
dplusStats dplus_stats[DPLUS_MAX_IO_THREADS] = {{0}};

uint64_t dplusStatsSum(size_t field_offset) {
    uint64_t sum = 0;
    for (int i = 0; i < DPLUS_MAX_IO_THREADS; i++) {
        _Atomic(uint64_t) *p = (_Atomic(uint64_t) *)((char *)&dplus_stats[i] + field_offset);
        sum += atomic_load_explicit(p, memory_order_relaxed);
    }
    return sum;
}
#endif

/* --- ACL/AUTH gate for speculation --- */

/* Recompute the per-client speculation ACL gate. MAIN THREAD ONLY.
 * Speculation bypasses processCommand(), so the NOAUTH check and
 * ACLCheckAllPerm never run on the speculative path — this byte is the
 * substitute, computed at every auth-state change (clientSetUser covers
 * AUTH/HELLO/RESET/module-auth/deluser-kick), on SELECT (per-db ACLs), and
 * on ACL admin ops (dplusOnAclRulesChanged).
 *
 * Condition (graceful give-up, per design): authenticated AND some selector
 * permits GET with UNRESTRICTED key read access (~* / %R~* / allkeys).
 * Key-pattern-restricted users PUNT to the stock main-thread path — per-key
 * pattern matching on workers would race main-mutated user structs.
 * flag.authenticated (sticky, only changed via clientSetUser) is used
 * instead of authRequired(): the latter reads DefaultUser->flags which can
 * change with no per-client event (CONFIG SET requirepass) — conservative
 * punts only, never a bypass. */
void dplusRecomputeSpecAclOk(client *c) {
    uint8_t ok = 0;
    /* ACL LOAD retires the user a fast-path client still points at; its successor holds the rules. */
    user *u = c->user && (c->user->flags & USER_FLAG_RETIRED) ? ACLResolveUser(c->user) : c->user;
    if (c->flag.authenticated && (u || !c->user)) {
        if (u == NULL) {
            /* No associated user = unrestricted context (stock semantics). */
            ok = 1;
        } else {
            static struct serverCommand *get_cmd = NULL;
            if (!get_cmd) get_cmd = lookupCommandByCString("get");
            /* Synthetic argv: the helper only needs cmd identity + dbid for
             * the command/db checks; the ALLKEYS-or-%R~* requirement is what
             * actually gates key access, so a placeholder key can only cause
             * a (safe) conservative punt, never a false allow. */
            robj *argv[2];
            argv[0] = createStringObject("get", 3);
            argv[1] = createStringObject("k", 1);
            int dbid = c->db ? c->db->id : 0;
            if (ACLUserCheckCmdWithUnrestrictedKeyAccess(u, get_cmd, argv, 2, dbid, CMD_KEY_ACCESS)) ok = 1;
            decrRefCount(argv[0]);
            decrRefCount(argv[1]);
        }
    }
    atomic_store_explicit(&c->spec_acl_ok, ok, memory_order_release);
}

/* ACL rules changed (ACL SETUSER/DELUSER/LOAD, module SetUserACL).
 * MAIN THREAD ONLY. Recompute every client's gate under the new rules. Rare
 * admin op -- the blanket client walk is deliberate simplicity. */
void dplusOnAclRulesChanged(void) {
    listIter li;
    listNode *ln;
    listRewind(server.clients, &li);
    while ((ln = listNext(&li))) dplusRecomputeSpecAclOk((client *)listNodeValue(ln));
}

/* F6: module command-result SUCCESS subscribers require an exact per-command
 * event from call(); speculated GETs bypass call() entirely, so speculation
 * must be off while any such subscriber exists. Speculated GETs can only be
 * SUCCESS events (failures always punt), so only the success listener count
 * matters. Published gate read by workers each batch; transitions are rare
 * module admin ops (subscribe / unload). */
_Atomic int dplus_module_cmdresult_gate = 0;

void dplusOnCommandResultListenersChanged(int success_listeners) {
    atomic_store_explicit(&dplus_module_cmdresult_gate, success_listeners > 0, memory_order_release);
}

/* F1: MONITOR feeds every executed command to attached monitor clients from
 * main (replicationFeedMonitors in call()); speculated GETs bypass call(), so
 * ~all speculative reads are invisible to MONITOR. Monitors are a rare
 * diagnostic feature, so gate speculation OFF while any monitor is attached --
 * MONITOR then observes the exact stock command stream. Zero cost when no
 * monitor (one acquire load of a main-written int per batch). */
_Atomic int dplus_monitor_gate = 0;

void dplusOnMonitorsChanged(void) {
    atomic_store_explicit(&dplus_monitor_gate, listLength(server.monitors) > 0, memory_order_release);
}

/* --- Component 2: Speculative GET — reply helpers --- */

/* Format a RESP2/3 bulk string reply directly into the client's output buffer.
 * Returns bytes written, or 0 if insufficient space (punt to main). */
static int dplusWriteBulkReply(client *c, const char *val, size_t vallen) {
    /* Fast bulk-header formatting: "$<len>\r\n" using ll2string instead of
     * snprintf (eliminates 2.6% __vfprintf_internal from worker profile). */
    char hdr[32];
    hdr[0] = '$';
    int numlen = ll2string(hdr + 1, sizeof(hdr) - 3, (long long)vallen);
    hdr[numlen + 1] = '\r';
    hdr[numlen + 2] = '\n';
    int hdrlen = numlen + 3;
    size_t total = hdrlen + vallen + 2; /* trailing \r\n */
    size_t available = c->buf_usable_size - c->bufpos;
    if (total > available) return 0; /* Won't fit — punt */

    memcpy(c->buf + c->bufpos, hdr, hdrlen);
    c->bufpos += hdrlen;
    memcpy(c->buf + c->bufpos, val, vallen);
    c->bufpos += vallen;
    memcpy(c->buf + c->bufpos, "\r\n", 2);
    c->bufpos += 2;
    if (c->buf_peak < c->bufpos) c->buf_peak = c->bufpos;
    return (int)total;
}

/* --- Component 2/5: IO-thread batch speculation with batched prefetch --- */

/* Maximum batch depth for prefetch overlap. Matches server.prefetch_batch_max_size default. */
#define DPLUS_BATCH_DEPTH 16

/* Per-key state for the batched prefetch + reply pipeline. */
typedef struct dplusBatchEntry {
    hashtableIncrementalFindState find_state; /* Incremental find state */
    int is_first_cmd;                         /* 1 if this is c->argv, 0 if queue */
    int queue_idx;                            /* Index in cmd_queue (if !is_first_cmd) */
} dplusBatchEntry;

/* The commandlog threshold of a type, or -1 while that log records nothing. */
static long long dplusCommandlogThreshold(int type) {
    if (server.commandlog[type].threshold < 0 || server.commandlog[type].max_len == 0) return -1;
    return server.commandlog[type].threshold;
}

/* Reply to one prefetched GET from the keyspace. Returns 1 on success (reply
 * written), 0 on punt. The keyspace is quiescent while IO threads read: main
 * mutates it only outside the read phase. */
static int dplusReplyFromEntry(client *c, dplusBatchEntry *e) {
    /* A GET the large-request commandlog would record goes to main, which records it. */
    long long request_threshold = dplusCommandlogThreshold(COMMANDLOG_TYPE_LARGE_REQUEST);
    if (request_threshold >= 0) {
        size_t in = e->is_first_cmd ? c->net_input_bytes_curr_cmd : c->cmd_queue.cmds[e->queue_idx].input_bytes;
        if ((long long)in >= request_threshold) return 0;
    }
    void *entry = NULL;
    if (!hashtableIncrementalFindGetResult(&e->find_state, &entry)) {
        /* Key not found -- punt on miss (E4): main fires the keymiss notification
         * and increments stat_keyspace_misses exactly. */
#ifdef IO_LOOKUP_OFFLOAD_STATS
        DPLUS_STAT_ADD(miss_punts, 1);
#endif
        return 0;
    }

    robj *o = (robj *)entry;
    if (objectGetType(o) != OBJ_STRING) return 0;

    /* Expired keys punt: only main may lazily delete them. */
    if (o->hasexpire) {
        mstime_t when = objectGetExpire(o);
        if (when >= 0 && mstime() >= when) {
#ifdef IO_LOOKUP_OFFLOAD_STATS
            DPLUS_STAT_ADD(expired_replies, 1);
#endif
            return 0;
        }
    }

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
        if (vallen > DPLUS_MAX_SPECULATIVE_VALUE_LEN) {
#ifdef IO_LOOKUP_OFFLOAD_STATS
            DPLUS_STAT_ADD(large_value_punts, 1);
#endif
            return 0;
        }
    } else {
        return 0;
    }

    /* A reply the large-reply commandlog would record goes to main, which records it. */
    long long reply_threshold = dplusCommandlogThreshold(COMMANDLOG_TYPE_LARGE_REPLY);
    if (reply_threshold >= 0 && (long long)(1 + digits10(vallen) + 2 + vallen + 2) >= reply_threshold) return 0;

    if (dplusWriteBulkReply(c, val, vallen) <= 0) return 0;
#ifdef IO_LOOKUP_OFFLOAD_STATS
    DPLUS_STAT_ADD(speculative_hits, 1);
#endif
    return 1;
}

/* Called from the IO thread after parsing is complete. Executes a contiguous
 * prefix of GET commands, using batched incremental finds to overlap memory
 * prefetches across keys before replying in command order.
 *
 * REPLY ORDERING: all speculative replies are written into c->buf BEFORE the
 * main thread processes any punted command, whose replies append after.
 *
 * Returns the number of commands speculatively completed. */
int dplusSpeculateBatch(client *c, int tid) {
    int speculated = 0;

    /* Early exit: speculation disabled (single-threaded or cluster mode).
     * In this tree speculative readers exist only under door-2 ownership: with
     * ownership off, IO-thread reads belong to the main-thread-offload fast path
     * and its own free routing, so nothing here may run. */
    if (server.io_threads_num <= 1 || server.cluster_enabled) return 0;

    /* ROLE GATE (replica-only deployment): with io-threads-speculation-replica-only
     * set, a primary or standalone server never speculates and runs the stock
     * read path. */
    if (server.io_threads_speculation_replica_only && server.primary_host == NULL) return 0;

    /* CLIENT-STATE GUARD: inside MULTI every command must reply +QUEUED and
     * execute only at EXEC — speculating a GET here would execute it early
     * and strand it from the transaction (real bug: pipelined MULTI batches
     * were safe only because MULTI heads the batch; a GET arriving in a
     * LATER read while flag.multi is set would be speculated). Blocked /
     * just-unblocked clients must have replies ordered after the unblock
     * reply. Cheap plain-byte tests, ordered before any atomics.
     *
     * BUFFER-STATE GUARD: dplusWriteBulkReply appends RAW RESP to c->buf.
     * If the buffer is in copy-avoidance ENCODED mode (header-framed
     * payloads), raw bytes corrupt the framing — trackBufReferences walks
     * off the payload chain ('ptr == buf + bufpos' assert; found by the
     * fork's dataset tests mixing KEYS/HSET-encoded replies with speculated
     * GETs). A non-empty reply list is equally disqualifying: new reply
     * data belongs at the list tail, not in buf, or replies reorder. */
    if (c->flag.multi || c->flag.blocked || c->flag.unblocked ||
        c->flag.buf_encoded || listLength(c->reply) != 0)
        return 0;

    /* CLIENT TRACKING GUARD (E7): speculation bypasses trackingRememberKeys,
     * so a non-BCAST tracking client could cache a speculatively-read value
     * WITHOUT being registered in the TrackingTable — a later write would
     * never invalidate it (stale client cache = correctness violation).
     * Punt such clients to the stock path. BCAST tracking is unaffected
     * (invalidation is prefix-based, not registration-based) and keeps
     * speculating. Safe to test plain flags here: per-client io_read_state
     * serialization means main cannot be executing this client's CLIENT
     * TRACKING command while its read is parsed, and a tracking command
     * inside this batch ends the speculative prefix before any later GET. */
    if (c->flag.tracking && !c->flag.tracking_bcast) return 0;

    /* MODULE COMMAND-RESULT GUARD (F6): a subscribed module expects an exact
     * success event per executed command; speculation bypasses call() and
     * would silently drop those events. Global, module-rare gate -- zero cost
     * when no module subscribes (single acquire load of a main-written int
     * that changes only on module admin ops). */
    if (atomic_load_explicit(&dplus_module_cmdresult_gate, memory_order_acquire)) return 0;

    /* MONITOR GUARD (F1): a monitor feed is produced per executed command in
     * call() on main; speculation bypasses call() and would hide the read from
     * every attached monitor. Global, diagnostic-rare gate -- zero cost when no
     * monitor is attached. */
    if (atomic_load_explicit(&dplus_monitor_gate, memory_order_acquire)) return 0;

    /* Lazily cache the GET command pointer -- eliminates repeated case-insensitive
     * siphash lookups. Only a batch that starts with GET can speculate. */
    static struct serverCommand *dplus_get_cmd = NULL;
    if (!dplus_get_cmd) dplus_get_cmd = lookupCommandByCString("get");
    if (c->argc != 2 || c->parsed_cmd != dplus_get_cmd) return 0;

    /* ACL/AUTH GUARD: speculation bypasses processCommand, so neither the
     * NOAUTH check nor ACLCheckAllPerm ever runs on this path. Only clients
     * pre-cleared on the main thread may speculate; everyone else punts to
     * the stock path which enforces auth/ACL as usual. The byte is written
     * only at rare auth-state changes, so this read does not ping-pong the
     * line. Placed after the GET fast-exit to keep pure-write batches at
     * zero added cost. */
    if (!atomic_load_explicit(&c->spec_acl_ok, memory_order_acquire)) return 0;

    /* COMMANDLOG GUARD: with a slow threshold of 0 the commandlog records every command, so every
     * GET goes to main, which records it. Large requests and replies are checked per GET. */
    if (dplusCommandlogThreshold(COMMANDLOG_TYPE_SLOW) == 0) return 0;

    /* Time the batch for commandstats parity: speculated GETs bypass call(),
     * so their duration must be accumulated per-thread and folded into the
     * GET command's microseconds by dplusAggregateStats (as with calls). */
    monotime spec_start = getMonotonicUs();

    hashtable *ht = kvstoreGetHashtable(c->db->keys, getKVStoreIndexForKey(objectGetVal(c->argv[1])));
    if (!ht) return 0;

    /* STREAMING SLOT-REFILL: a ring of DPLUS_BATCH_DEPTH slots holds in-flight
     * finds. Each sweep steps every pending find once (one prefetch per slot);
     * the head slot replies as soon as its find completes, and the fill loop
     * tops the window back up from the queue, so prefetch depth stays constant
     * across the eligible prefix. The first ineligible command or punt ends
     * speculation for the whole read. head..tail are monotonic; slot index =
     * seq % DPLUS_BATCH_DEPTH; done[] marks completed finds. */
    dplusBatchEntry batch[DPLUS_BATCH_DEPTH];
    uint8_t done[DPLUS_BATCH_DEPTH];
    cmdQueue *queue = &c->cmd_queue;
    int queue_pos = queue->off;
    int prefix_open = 1;
    unsigned head = 0, tail = 1;

    batch[0].is_first_cmd = 1;
    batch[0].queue_idx = -1;
    hashtableIncrementalFindInit(&batch[0].find_state, ht, objectGetVal(c->argv[1]));
    done[0] = 0;

    while (head < tail || prefix_open) {
        /* Fill: top the window up from the queue. */
        while (prefix_open && tail - head < DPLUS_BATCH_DEPTH && queue_pos < queue->len) {
            parsedCommand *p = &queue->cmds[queue_pos];
            if (p->cmd != dplus_get_cmd || p->argc != 2) {
                prefix_open = 0; /* ineligible command ends the global prefix */
                break;
            }
            hashtable *pht = kvstoreGetHashtable(c->db->keys, getKVStoreIndexForKey(objectGetVal(p->argv[1])));
            if (!pht) {
                prefix_open = 0; /* an unallocated shard ends the prefix; main answers it */
                break;
            }
            dplusBatchEntry *e = &batch[tail % DPLUS_BATCH_DEPTH];
            e->is_first_cmd = 0;
            e->queue_idx = queue_pos;
            hashtableIncrementalFindInit(&e->find_state, pht, objectGetVal(p->argv[1]));
            done[tail % DPLUS_BATCH_DEPTH] = 0;
            tail++;
            queue_pos++;
        }
        if (queue_pos >= queue->len) prefix_open = 0; /* queue exhausted -- drain */
        if (head == tail) break;                      /* nothing in flight and nothing to fill */

        /* Sweep: one find step per pending slot (one prefetch each). */
        for (unsigned s = head; s < tail; s++) {
            unsigned i = s % DPLUS_BATCH_DEPTH;
            if (!done[i]) done[i] = !hashtableIncrementalFindStep(&batch[i].find_state);
        }

        /* Drain: reply completed HEADS in command order. */
        while (head < tail && done[head % DPLUS_BATCH_DEPTH]) {
            dplusBatchEntry *e = &batch[head % DPLUS_BATCH_DEPTH];
#ifdef IO_LOOKUP_OFFLOAD_STATS
            DPLUS_STAT_ADD(speculative_attempts, 1);
#endif
            if (!dplusReplyFromEntry(c, e)) {
                /* Contiguous-prefix rule: in-flight slots behind a punt are
                 * abandoned and their commands fall through to main. */
                prefix_open = 0;
                head = tail;
                break;
            }
            if (e->is_first_cmd)
                c->read_flags |= READ_FLAGS_DPLUS_SPECULATED;
            else
                queue->cmds[e->queue_idx].read_flags |= READ_FLAGS_DPLUS_SPECULATED;
            speculated++;
            head++;
        }
    }

    /* Commandstats parity: fold execution time into the per-thread counter
     * (calls are accumulated in dplusConsumeSpeculated). */
    if (speculated > 0) dplus_thread_stats[tid].usec += (long long)(getMonotonicUs() - spec_start);
    return speculated;
}

/* --- Phase-1 no-enqueue: IO-thread command consumption --- */

/* Consume speculated commands at the IO thread so they never reach main.
 *
 * After dplusSpeculateBatch writes replies into c->buf and marks commands
 * with READ_FLAGS_DPLUS_SPECULATED, this function:
 * 1. Frees the first command's argv (c->argv) and clears pending_command state.
 * 2. Advances cmd_queue past all DPLUS_SPECULATED entries, freeing their argv.
 * 3. Increments per-IO-thread command counter (NOT shared stat_numcommands).
 * 4. Increments c->commands_processed directly — safe because the IO thread
 *    owns the client exclusively during the read phase (io_read_state ==
 *    CLIENT_PENDING_IO; main thread will not touch the client until we
 *    set CLIENT_COMPLETED_IO and send to main).
 *
 * After this, c->argc == 0, pending_command is clear, and cmd_queue has no
 * speculated entries. Main-thread processClientIOReadsDone sees nothing to
 * execute. */
void dplusConsumeSpeculated(client *c, int count, int tid) {
    int consumed = 0;

    /* --- First command (c->argv / c->parsed_cmd case) --- */
    if (count > 0 && (c->read_flags & READ_FLAGS_DPLUS_SPECULATED)) {
        c->read_flags &= ~READ_FLAGS_DPLUS_SPECULATED;
        /* CRITICAL: clear pending_command — the parse path set it, and without
         * clearing it here the main thread's processPendingCommandAndInputBuffer
         * would call processCommandAndResetClient() on argc==0 + a STALE c->cmd
         * (the removed main-side skip path used to clear this flag). */
        c->flag.pending_command = 0;
        /* Free argv INLINE — do NOT call freeClientArgv() which routes through
         * tryOffloadFreeArgvToIOThreads(). That function increments the non-atomic
         * io_jobs_submitted from the IO thread (data race with main) and enqueues
         * to a SPSC queue potentially owned by this same thread (invariant
         * violation: SPSC is single-producer = main thread only). The race causes
         * lost increments -> submitted < finished -> infinite spin in
         * drainIOThreadsQueue on shutdown. */
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
        if (!(p->read_flags & READ_FLAGS_DPLUS_SPECULATED)) break;
        p->read_flags &= ~READ_FLAGS_DPLUS_SPECULATED;
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
    dplus_thread_stats[tid].commands_processed += consumed;
    /* E3: every consumed speculation is a GET keyspace HIT (valid-nil misses now
     * punt to main via E4; large/expired/torn also punt). Count them per-thread
     * so main can fold into stat_keyspace_hits, which the worker path bypasses. */
    dplus_thread_stats[tid].keyspace_hits += consumed;
}

/* Aggregate per-IO-thread command counters into server.stat_numcommands.
 * Called from beforeSleep once per event-loop iteration. The main thread
 * is the sole writer of stat_numcommands, so this is a plain add-and-zero
 * with no atomics needed on the server side. The IO threads only increment
 * their own counter (single-writer per slot), so a plain read + zero is
 * safe: worst case we miss a few commands this iteration and catch them
 * next time (bounded lag of one event-loop cycle, ~1ms).
 *
 * BOUNDARY CALLERS (beyond beforeSleep): CONFIG RESETSTAT and INFO
 * commandstats must call this too — otherwise pending per-thread counts
 * survive a reset (stale +N folds in later) or are missing from a read
 * (found by valkey-benchmark.tcl: calls=101 vs 100, calls=10000 vs 10010).
 * For RESETSTAT the settle happens BEFORE the zeroing, absorbing pending
 * counts into the stats about to be cleared. */
void dplusAggregateStats(void) {
    /* Commandstats parity: speculated GETs bypass call(), which is where
     * cmd->calls / cmd->microseconds are normally incremented. Fold the
     * per-thread counters into the GET command here so INFO COMMANDSTATS
     * (and everything downstream) sees them. Only GET is ever speculated,
     * so a single target command suffices.
     *
     * MONOTONIC-DELTA scheme (NOT add-and-zero): workers increment their
     * slots concurrently with this fold; zeroing a slot from main races the
     * worker's read-modify-write and PERMANENTLY loses counts (observed as
     * calls=99-vs-100 once INFO/RESETSTAT-time folds made folding frequent).
     * Workers' counters only ever grow; main folds the delta since its own
     * last_seen snapshot. A racy read can only UNDER-read (missing the very
     * latest increment), which the next fold picks up — never loses. */
    static long long seen_calls[DPLUS_MAX_IO_THREADS] = {0};
    static long long seen_usec[DPLUS_MAX_IO_THREADS] = {0};
    static long long seen_hits[DPLUS_MAX_IO_THREADS] = {0};
    static struct serverCommand *get_cmd = NULL;
    if (!get_cmd) get_cmd = lookupCommandByCString("get");
    for (int i = 0; i < server.io_threads_num; i++) {
        long long n = dplus_thread_stats[i].commands_processed;
        long long calls_delta = 0;
        if (n > seen_calls[i]) {
            calls_delta = n - seen_calls[i];
            seen_calls[i] = n;
            server.stat_numcommands += calls_delta;
            if (get_cmd) get_cmd->calls += calls_delta;
        }
        /* E3: fold speculative GET hits into stat_keyspace_hits (the worker path
         * bypasses main's lookupKey where this is normally incremented). Same
         * monotonic-delta scheme: workers only grow their slot, main folds the
         * delta since its last snapshot; a racy read only under-reads, never loses. */
        long long h = dplus_thread_stats[i].keyspace_hits;
        if (h > seen_hits[i]) {
            server.stat_keyspace_hits += h - seen_hits[i];
            seen_hits[i] = h;
        }
        long long us = dplus_thread_stats[i].usec;
        long long us_delta = 0;
        if (us > seen_usec[i]) {
            us_delta = us - seen_usec[i];
            seen_usec[i] = us;
            if (get_cmd) get_cmd->microseconds += us_delta;
        }
        /* Speculated GETs count in LATENCY HISTOGRAM and latencystats at their average duration. */
        if (get_cmd && calls_delta > 0 && server.latency_tracking_enabled) {
            int64_t avg_ns = (int64_t)(us_delta * 1000 / calls_delta);
            if (avg_ns < 1) avg_ns = 1;
            updateCommandLatencyHistogramCount(&get_cmd->latency_histogram, avg_ns, calls_delta);
        }
    }
}

sds dplusInfoString(sds info) {
    long long speculated = 0;
    for (int i = 0; i < DPLUS_MAX_IO_THREADS; i++) speculated += dplus_thread_stats[i].commands_processed;
    info = sdscatprintf(info, "# Dplus\r\ndplus_speculated_commands:%lld\r\n", speculated);
#ifdef IO_LOOKUP_OFFLOAD_STATS
    info = sdscatprintf(info,
        "dplus_speculative_attempts:%llu\r\n"
        "dplus_speculative_hits:%llu\r\n"
        "dplus_large_value_punts:%llu\r\n"
        "dplus_expired_replies:%llu\r\n"
        "dplus_miss_punts:%llu\r\n",
        (unsigned long long)DPLUS_STAT_SUM(speculative_attempts),
        (unsigned long long)DPLUS_STAT_SUM(speculative_hits),
        (unsigned long long)DPLUS_STAT_SUM(large_value_punts),
        (unsigned long long)DPLUS_STAT_SUM(expired_replies),
        (unsigned long long)DPLUS_STAT_SUM(miss_punts));
#endif
    return info;
}
