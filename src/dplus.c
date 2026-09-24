/* D+ Read-Side Prototype — Speculative GET execution on IO threads.
 * See dplus.h for architecture overview and reply-ordering guarantee. */

#include "server.h"
#include "dplus.h"
#include "io_threads.h"

#include <string.h>
#include <stdatomic.h>
#include <unistd.h>

/* --- Component 4: Exclusive mode and epoch reader state --- */
_Atomic(int) dplus_exclusive_mode = 0;
static _Atomic(uint64_t) dplus_reclaim_epoch = 1;
static _Atomic(int) dplus_reclaim_pressure_gate = 0;
static int dplus_reclaim_gate_drained = 0; /* main-thread only */
#ifdef IO_LOOKUP_OFFLOAD_STATS
static _Atomic(long long) dplus_debug_reader_hold_us = 0;
/* S5: second preemption point -- holds the reader AFTER the value copy and
 * BEFORE validation, the window every torn-state test needs. */
static _Atomic(long long) dplus_debug_prevalidate_hold_us = 0;
static _Atomic(int) dplus_debug_prevalidate_bump = 0;
static _Atomic(uint64_t) dplus_debug_prevalidate_consumed = 0;
static char dplus_debug_pv_last_key[32] = "";      /* diagnostic only */
static _Atomic(uint64_t) dplus_debug_pv_last_client = 0;
static _Atomic(int) dplus_debug_reader_holding = 0;
static _Atomic(int) dplus_debug_reader_release = 0;
static _Atomic(int) dplus_debug_pressure_sync = 0;
#endif

typedef struct dplusReaderEpochSlot {
    _Atomic(uint64_t) state;
    char pad[DPLUS_CACHELINE - sizeof(_Atomic(uint64_t))];
} __attribute__((aligned(DPLUS_CACHELINE))) dplusReaderEpochSlot;

static dplusReaderEpochSlot dplus_reader_slots[DPLUS_MAX_IO_THREADS] = {0};
#define DPLUS_READER_ONLINE_WORDS (DPLUS_MAX_IO_THREADS / 64)
static _Atomic(uint64_t) dplus_reader_online[DPLUS_READER_ONLINE_WORDS] = {0};

typedef struct dplusEpochThreadStats {
    uint64_t entries;
    uint64_t retries;
    uint64_t exclusive_punts;
    uint64_t pressure_punts;
    char pad[DPLUS_CACHELINE - 4 * sizeof(uint64_t)];
} __attribute__((aligned(DPLUS_CACHELINE))) dplusEpochThreadStats;

static dplusEpochThreadStats dplus_epoch_thread_stats[DPLUS_MAX_IO_THREADS] = {0};

static_assert(sizeof(dplusReaderEpochSlot) == DPLUS_CACHELINE, "epoch reader slot must occupy one cache line");
static_assert(sizeof(dplusEpochThreadStats) == DPLUS_CACHELINE, "epoch reader stats must occupy one cache line");

/* --- Per-IO-thread command counters (no-enqueue Phase-1) --- */
dplusThreadStats dplus_thread_stats[DPLUS_MAX_IO_THREADS] = {{0}};

/* --- Write-tax gate (per-IO-thread) --- */
dplusWriteTaxGate dplus_write_tax[DPLUS_MAX_IO_THREADS] = {{.history = ~(uint64_t)0}}; /* Start optimistic (all-ones = all speculated) */

/* D5: worker -> main signal that some owned client's output has crossed
 * maxmemory-clients while no completion is flowing (WAITING_WRITABLE hog).
 * Consumed (exchange 0) by evictClients on main. NOT stats -- must exist
 * in flagless builds (correctness machinery, not diagnostics). */
_Atomic int dplus_client_mem_pressure = 0;

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

/* --- Component 4: Exclusive mode implementation --- */

/* A slot is published before its thread can read speculative pointers and is
 * excluded only after join, preventing slot-reuse ABA during runtime resize. */
void dplusReaderWorkerOnline(int tid) {
    serverAssert(tid > 0 && tid < DPLUS_MAX_IO_THREADS);
    atomic_store_explicit(&dplus_reader_slots[tid].state, DPLUS_READER_QUIESCENT, memory_order_seq_cst);
    atomic_fetch_or_explicit(&dplus_reader_online[tid / 64], UINT64_C(1) << (tid % 64), memory_order_seq_cst);
}

void dplusReaderWorkerQuiescent(int tid) {
    if (tid <= 0 || tid >= DPLUS_MAX_IO_THREADS) return;
    atomic_store_explicit(&dplus_reader_slots[tid].state, DPLUS_READER_QUIESCENT, memory_order_seq_cst);
}

void dplusReaderWorkerOffline(int tid) {
    serverAssert(tid > 0 && tid < DPLUS_MAX_IO_THREADS);
    atomic_store_explicit(&dplus_reader_slots[tid].state, DPLUS_READER_OFFLINE, memory_order_seq_cst);
    atomic_fetch_and_explicit(&dplus_reader_online[tid / 64], ~(UINT64_C(1) << (tid % 64)), memory_order_seq_cst);
}

/* D+ (S1.5): see dplus.h. Called at worker park points. */
void dplusReaderAssertParkSafe(int tid) {
    if (tid <= 0 || tid >= DPLUS_MAX_IO_THREADS) return;
    debugServerAssert(atomic_load_explicit(&dplus_reader_slots[tid].state, memory_order_seq_cst) == DPLUS_READER_QUIESCENT ||
                      atomic_load_explicit(&dplus_reader_slots[tid].state, memory_order_seq_cst) == DPLUS_READER_OFFLINE);
}

/* Publish ACTIVE(epoch), then check exclusion and re-read the epoch before the
 * first pointer load. The second epoch read closes the late-announcement race:
 * a reader that publishes an old epoch after a collector scan retries before
 * touching the hashtable. Collector advancement may force a retry here. */
static int dplusReaderEnter(int tid) {
    dplusEpochThreadStats *stats = &dplus_epoch_thread_stats[tid];
    stats->entries++;
    while (1) {
        uint64_t epoch = atomic_load_explicit(&dplus_reclaim_epoch, memory_order_seq_cst);
        atomic_store_explicit(&dplus_reader_slots[tid].state, epoch, memory_order_seq_cst);
        if (atomic_load_explicit(&dplus_exclusive_mode, memory_order_seq_cst)) {
            stats->exclusive_punts++;
#ifdef IO_LOOKUP_OFFLOAD_STATS
            DPLUS_STAT_ADD(exclusive_punts, 1);
#endif
            dplusReaderWorkerQuiescent(tid);
            return 0;
        }
        if (atomic_load_explicit(&dplus_reclaim_pressure_gate, memory_order_seq_cst)) {
            stats->pressure_punts++;
            dplusReaderWorkerQuiescent(tid);
            return 0;
        }
        if (atomic_load_explicit(&dplus_reclaim_epoch, memory_order_seq_cst) != epoch) {
            stats->retries++;
            dplusReaderWorkerQuiescent(tid);
            continue;
        }
        return 1;
    }
}

/* Exclusive mode remains a counter because nested main/BIO windows are valid.
 * Every ACTIVE(epoch) slot is a reader; OFFLINE and QUIESCENT are safe. Scan
 * all slots until runtime-resize ordering is proven under the new lifecycle. */
void dplusExclusiveEnter(void) {
    atomic_fetch_add_explicit(&dplus_exclusive_mode, 1, memory_order_seq_cst);
#ifdef IO_LOOKUP_OFFLOAD_STATS
    /* Test-only: a debug-held reader models a descheduled thread. ANY
     * exclusive entrant (hard pressure, eviction, INFO temp-table release,
     * resize) must be able to preempt it, so the release lives here -- the
     * held reader then sleeps its configured lag and quiesces, exactly the
     * scenario the hold exists to exercise. Without this, any non-pressure
     * exclusive caller would spin against the handshake to the panic bound. */
    atomic_store_explicit(&dplus_debug_reader_release, 1, memory_order_seq_cst);
#endif
    for (int word = 0; word < DPLUS_READER_ONLINE_WORDS; word++) {
        uint64_t online = atomic_load_explicit(&dplus_reader_online[word], memory_order_seq_cst);
        while (online) {
            int i = word * 64 + __builtin_ctzll(online);
            online &= online - 1;
            uint64_t state = atomic_load_explicit(&dplus_reader_slots[i].state, memory_order_seq_cst);
            if (state == DPLUS_READER_OFFLINE || state == DPLUS_READER_QUIESCENT) continue;
            monotime _w = getMonotonicUs();
            while ((state = atomic_load_explicit(&dplus_reader_slots[i].state, memory_order_seq_cst)) != DPLUS_READER_OFFLINE &&
                   state != DPLUS_READER_QUIESCENT) {
                if (getMonotonicUs() - _w > 5000000) serverPanic("dplusExclusiveEnter spin timeout on slot %d epoch %llu", i, (unsigned long long)state);
            }
        }
    }
}

void dplusExclusiveLeave(void) {
    atomic_fetch_sub_explicit(&dplus_exclusive_mode, 1, memory_order_seq_cst);
}

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
    if (c->flag.authenticated) {
        if (c->user == NULL) {
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
            if (ACLUserCheckCmdWithUnrestrictedKeyAccess(c->user, get_cmd, argv, 2, dbid, CMD_KEY_ACCESS)) ok = 1;
            decrRefCount(argv[0]);
            decrRefCount(argv[1]);
        }
    }
    atomic_store_explicit(&c->spec_acl_ok, ok, memory_order_release);
}

/* ACL rules changed (ACL SETUSER/DELUSER/LOAD, module SetUserACL).
 * MAIN THREAD ONLY. Enter exclusive FIRST: blocks new speculation and drains
 * in-flight batches that passed the OLD gate (they serialize before the
 * change — stock-equivalent semantics, F12-C temporal-window class), then
 * recompute every client's gate under the new rules. Rare admin op — the
 * blanket client walk is deliberate simplicity. */
void dplusOnAclRulesChanged(void) {
    dplusExclusiveEnter();
    listIter li;
    listNode *ln;
    listRewind(server.clients, &li);
    while ((ln = listNext(&li))) dplusRecomputeSpecAclOk((client *)listNodeValue(ln));
    dplusExclusiveLeave();
}

/* F6: module command-result SUCCESS subscribers require an exact per-command
 * event from call(); speculated GETs bypass call() entirely, so speculation
 * must be off while any such subscriber exists. Speculated GETs can only be
 * SUCCESS events (failures always punt), so only the success listener count
 * matters. Published gate read by workers each batch; transitions are rare
 * module admin ops (subscribe / unload) and drain in-flight speculation via
 * exclusive mode, same temporal-window treatment as ACL changes. */
_Atomic int dplus_module_cmdresult_gate = 0;

void dplusOnCommandResultListenersChanged(int success_listeners) {
    dplusExclusiveEnter();
    atomic_store_explicit(&dplus_module_cmdresult_gate, success_listeners > 0, memory_order_release);
    dplusExclusiveLeave();
}

/* F1: MONITOR feeds every executed command to attached monitor clients from
 * main (replicationFeedMonitors in call()); speculated GETs bypass call(), so
 * ~all speculative reads are invisible to MONITOR. Monitors are a rare
 * diagnostic feature, so gate speculation OFF while any monitor is attached --
 * MONITOR then observes the exact stock command stream. Published by main under
 * an exclusive drain (in-flight speculation that passed the old gate settles
 * first), same temporal treatment as the module/ACL gates; zero cost when no
 * monitor (one acquire load of a main-written int per batch). */
_Atomic int dplus_monitor_gate = 0;

void dplusOnMonitorsChanged(void) {
    dplusExclusiveEnter();
    atomic_store_explicit(&dplus_monitor_gate, listLength(server.monitors) > 0, memory_order_release);
    dplusExclusiveLeave();
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

/* D+ (S1.5): the single-GET speculative path (dplusSpeculativeGet) was
 * removed as dead code -- it had NO epoch reader-slot bracketing
 * (dplusReaderEnter/Quiescent), so any future caller would dereference
 * published objects with no lifetime protection. The batch path
 * (dplusSpeculateBatch) is the only speculation entry point.  */

/* --- Component 2/5: IO-thread batch speculation with batched prefetch --- */

/* Maximum batch depth for prefetch overlap. Matches server.prefetch_batch_max_size default. */
#define DPLUS_BATCH_DEPTH 16

/* Per-key state for the batched prefetch + validate pipeline. */
typedef struct dplusBatchEntry {
    void *key_sds;                           /* SDS key string */
    uint64_t hash;                           /* Pre-computed hash */
    unsigned shard;                          /* Version shard index */
    uint64_t v_before;                       /* Version read before find */
    hashtableIncrementalFindState find_state; /* Incremental find state */
    int is_first_cmd;                        /* 1 if this is c->argv, 0 if queue */
    int queue_idx;                           /* Index in cmd_queue (if !is_first_cmd) */
} dplusBatchEntry;

/* Execute the validate+copy+reply phase for a single prefetched entry.
 * Returns 1 on success (reply written), 0 on punt. */
static int dplusValidateAndReply(client *c, dplusBatchEntry *e, hashtable *ht, int resp) {
    dplusVersionArray *va = hashtableGetVersionArray(ht);
    (void)resp; /* GET bulk reply is RESP-version-agnostic; misses now punt (E4). */

    /* Get the find result — entry is now in cache from prefetch. */
    void *entry = NULL;
    bool found = hashtableIncrementalFindGetResult(&e->find_state, &entry);

    if (!found) {
        /* Key not found — punt on miss (E4): main fires the keymiss notification
         * and increments stat_keyspace_misses exactly. */
#ifdef IO_LOOKUP_OFFLOAD_STATS
        DPLUS_STAT_ADD(miss_punts, 1);
#endif
        return 0;
    }

    /* Found an entry. It's an robj*. */
    robj *o = (robj *)entry;

    /* Type check: must be OBJ_STRING. */
    if (objectGetType(o) != OBJ_STRING) return 0;

    /* Expiry check. */
    if (o->hasexpire) {
        mstime_t when = objectGetExpire(o);
        if (when >= 0 && mstime() >= when) {
#ifdef IO_LOOKUP_OFFLOAD_STATS
            DPLUS_STAT_ADD(expired_replies, 1);
#endif
            return 0;
        }
    }

    /* Value extraction: copy BEFORE validation re-read. */
    char valbuf[DPLUS_MAX_SPECULATIVE_VALUE_LEN + 21];
    size_t vallen;
    int encoding = objectGetEncoding(o);

    if (encoding == OBJ_ENCODING_INT) {
        long long intval = (long long)(long)objectGetVal(o);
        vallen = ll2string(valbuf, sizeof(valbuf), intval);
    } else if (encoding == OBJ_ENCODING_EMBSTR || encoding == OBJ_ENCODING_RAW) {
        sds s = objectGetVal(o);
        vallen = sdslen(s);
        if (vallen > (size_t)server.io_threads_speculation_max_value) {
#ifdef IO_LOOKUP_OFFLOAD_STATS
            DPLUS_STAT_ADD(large_value_punts, 1);
#endif
            return 0;
        }
        memcpy(valbuf, s, vallen);
    } else {
        return 0;
    }

#ifdef IO_LOOKUP_OFFLOAD_STATS
    /* S5 test-only preemption point: pause between the value copy and the
     * validation re-read. A mutation running on main during this pause MUST
     * cause the validation below to fail (T-* torn-window battery). Reuses
     * the holding/release handshake of the pre-lookup hold. */
    {
        long long pv_hold_us = atomic_exchange_explicit(&dplus_debug_prevalidate_hold_us, 0, memory_order_seq_cst);
        if (unlikely(pv_hold_us > 0)) {
            atomic_fetch_add_explicit(&dplus_debug_prevalidate_consumed, 1, memory_order_seq_cst);
            {
                size_t klen = sdslen((sds)e->key_sds);
                if (klen > sizeof(dplus_debug_pv_last_key) - 1) klen = sizeof(dplus_debug_pv_last_key) - 1;
                memcpy(dplus_debug_pv_last_key, e->key_sds, klen);
                dplus_debug_pv_last_key[klen] = '\0';
                atomic_store_explicit(&dplus_debug_pv_last_client, c->id, memory_order_seq_cst);
            }
            /* Plain bounded sleep -- UNLIKE the pre-lookup hold there is no
             * release handshake: that loop is released by exclusive
             * entrants (pressure tests), but torn-window tests mutate on
             * main WITHOUT entering exclusive, so a wait here would spin to
             * its panic bound. The test observes 'holding', runs its
             * mutation (micro to milliseconds), and the reader wakes into
             * validation on its own. */
            atomic_store_explicit(&dplus_debug_reader_holding, 1, memory_order_seq_cst);
            usleep((useconds_t)pv_hold_us);
            /* Injected writer bracket (T-* torn-window battery): a LIVE
             * mutation can never land inside this window -- the parked
             * worker freezes main at the waitForClientIO rendezvous
             * (verified: zero serverCron heartbeats for the full hold), so
             * every command-driven mutation serializes after the wake.
             * Instead, model the concurrent writer minimally: one full
             * odd/even bracket on this entry's shard. Validation below MUST
             * fail. Writer-side bracket coverage is proven separately by
             * the dplus-shard-version probe tests (no timing needed). */
            if (atomic_exchange_explicit(&dplus_debug_prevalidate_bump, 0, memory_order_seq_cst)) {
                dplusVersionBracketBegin(va, e->shard);
                dplusVersionBracketEnd(va, e->shard);
            }
            atomic_store_explicit(&dplus_debug_reader_holding, 0, memory_order_seq_cst);
        }
    }
#endif

    /* VALIDATION RE-READ: fenced check that shard version is unchanged.
     * dplusVersionValidate issues the acquire fence that orders the value
     * copy above before the version re-read. */
    if (!dplusVersionValidate(va, e->shard, e->v_before)) {
#ifdef IO_LOOKUP_OFFLOAD_STATS
        DPLUS_STAT_ADD(validation_misses, 1);
#endif
        return 0;
    }

    /* Version valid — write reply. */
    int written = dplusWriteBulkReply(c, valbuf, vallen);
    if (written <= 0) return 0;

#ifdef IO_LOOKUP_OFFLOAD_STATS
    DPLUS_STAT_ADD(speculative_hits, 1);
#endif
    return 1;
}

/* Called from ioThreadReadQueryFromClient after parsing is complete.
 * Iterates the parsed command queue attempting speculative GETs on a
 * contiguous prefix. Uses batched incremental-find to overlap memory
 * prefetches across multiple keys before executing the serial
 * validate+copy+reply phase with warm caches.
 *
 * REPLY ORDERING: The contiguous-prefix invariant guarantees that all
 * speculative replies are written into c->buf BEFORE the main thread
 * processes any punted commands (which append their replies after).
 * This preserves pipeline reply ordering without any reordering logic.
 *
 * Returns the number of commands speculatively completed.
 */
int dplusSpeculateBatch(client *c, int tid) {
    int speculated = 0;

    /* Early exit: speculation disabled (single-threaded or cluster mode).
     * In this tree speculative readers exist only under door-2 ownership: with
     * ownership off, IO-thread reads belong to the main-thread-offload fast path
     * and its own free routing, so nothing here may run. */
    if (server.io_threads_num <= 1 || server.cluster_enabled) return 0;

    /* ROLE GATE (replica-only deployment): with io-threads-speculation-replica-only
     * set, a primary or standalone server never speculates and runs the stock
     * read path. primary_host is written by main on REPLICAOF and read here as a
     * plain word: a stale read only decides whether THIS batch is speculated,
     * and a speculation racing a role change is no different from one racing
     * any main-thread write, which the validate step already covers. */
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

    /* DEFRAG GUARD: active defrag MOVES allocations (entries/sds) and frees
     * the originals outside the limbo hooks — reads racing a move are the
     * same UAF class. Punt speculation while defrag is running (rare,
     * default-off feature; costs nothing otherwise). */
    if (server.active_defrag_cpu_percent > 0) return 0;

    /* ZERO-COST WRITE-BATCH EXIT: if the first command isn't even a GET,
     * return before touching any atomics or gate state. Eliminates the r100
     * write-tax (2 seq_cst ops + gate history shift cost on pure-write batches).
     * Uses a lazily-cached command pointer (static, idempotent init). */
    {
        static struct serverCommand *dplus_get_cmd_fast = NULL;
        if (!dplus_get_cmd_fast) dplus_get_cmd_fast = lookupCommandByCString("get");
        if (c->argc != 2 || c->parsed_cmd != dplus_get_cmd_fast) return 0;
    }

    /* ACL/AUTH GUARD: speculation bypasses processCommand, so neither the
     * NOAUTH check nor ACLCheckAllPerm ever runs on this path. Only clients
     * pre-cleared on the main thread may speculate; everyone else punts to
     * the stock path which enforces auth/ACL as usual. The byte is written
     * only at rare auth-state changes, so this read does not ping-pong the
     * line. Placed after the GET fast-exit to keep pure-write batches at
     * zero added cost. ACL admin ops drain in-flight speculation before
     * gates update (dplusOnAclRulesChanged), closing the temporal window. */
    if (!atomic_load_explicit(&c->spec_acl_ok, memory_order_acquire)) return 0;

    /* Write-tax gate pointer — declared early so the out: label can update it.
     * The actual gate CHECK happens below, after first-command eligibility is
     * determined (to avoid a dead-state where the gate blocks all speculation
     * including the GET traffic that would recover it). */
    dplusWriteTaxGate *gate = &dplus_write_tax[tid];

    /* Lazily cache the GET command pointer — eliminates repeated case-insensitive
     * siphash lookups (7% of worker profile per the per-TID decomposition). */
    static struct serverCommand *dplus_get_cmd = NULL;
    if (!dplus_get_cmd) dplus_get_cmd = lookupCommandByCString("get");

    /* Epoch reader entry publishes ACTIVE(epoch), then checks exclusive mode
     * and rechecks the epoch before the first speculative pointer load. */
    if (!dplusReaderEnter(tid)) goto out;

    /* Time the batch for commandstats parity: speculated GETs bypass call(),
     * so their duration must be accumulated per-thread and folded into the
     * GET command's microseconds by dplusAggregateStats (as with calls). */
    monotime spec_start = getMonotonicUs();

    /* --- Phase 1: Collect eligible prefix keys into batch --- */
    serverDb *db = c->db;
    int dict_index = 0;
    hashtable *ht = kvstoreGetHashtable(db->keys, dict_index);
    if (!ht) goto out;

    dplusVersionArray *va = hashtableGetVersionArray(ht);
    if (!va) goto out;

    dplusBatchEntry batch[DPLUS_BATCH_DEPTH];
    int batch_count = 0;

    /* First command (c->argv / c->parsed_cmd). */
    if (c->argc == 2 && c->parsed_cmd != NULL &&
        c->parsed_cmd == dplus_get_cmd &&
        (c->parsed_cmd->flags & (CMD_READONLY | CMD_FAST)) == (CMD_READONLY | CMD_FAST)) {

        /* Write-tax gate: first command IS eligible (GET), so we know the
         * workload can speculate. Check the gate to decide whether recent
         * traffic justifies the prefetch investment. If traffic has been
         * write-heavy, speculation attempts mostly hit validation misses,
         * wasting prefetch cycles. Skip the batch but shift in a 1 so the
         * gate recovers once GETs resume. */
        if (__builtin_popcountll(gate->history) < DPLUS_WRITE_TAX_THRESHOLD) {
#ifdef IO_LOOKUP_OFFLOAD_STATS
            DPLUS_STAT_ADD(intra_batch_write_punts, 1);
#endif
            /* Shift in 1 (eligible batch seen) to allow recovery. */
            gate->history = (gate->history << 1) | 1;
            goto out;
        }

#ifdef IO_LOOKUP_OFFLOAD_STATS
        /* Test-only preemption point: hold one admitted real GET reader after
         * the final entry recheck and before its first speculative lookup. */
        long long hold_us = atomic_exchange_explicit(&dplus_debug_reader_hold_us, 0, memory_order_seq_cst);
        if (unlikely(hold_us > 0)) {
            atomic_store_explicit(&dplus_debug_reader_holding, 1, memory_order_seq_cst);
            monotime wait_start = getMonotonicUs();
            while (!atomic_load_explicit(&dplus_debug_reader_release, memory_order_seq_cst)) {
                if (getMonotonicUs() - wait_start > 10000000)
                    serverPanic("timed out waiting to release D+ debug reader");
                usleep(100);
            }
            /* Model a descheduled reader that does not run immediately when
             * hard pressure asks it to quiesce. */
            usleep((useconds_t)hold_us);
            atomic_store_explicit(&dplus_debug_reader_holding, 0, memory_order_seq_cst);
            atomic_store_explicit(&dplus_debug_reader_release, 0, memory_order_seq_cst);
        }
#endif

        dplusBatchEntry *e = &batch[batch_count];
        e->key_sds = objectGetVal(c->argv[1]);
        e->hash = hashtableHashKey(ht, e->key_sds);
        e->shard = DPLUS_SHARD_INDEX(e->hash);
        e->v_before = dplusVersionRead(va, e->shard);
        if (e->v_before & 1) {
            /* S2.2a: writer bracket open -- punt to main */
#ifdef IO_LOOKUP_OFFLOAD_STATS
            DPLUS_STAT_ADD(bracket_entry_punts, 1);
#endif
            goto out;
        }
        e->is_first_cmd = 1;
        e->queue_idx = -1;
        batch_count++;
    } else {
        /* First command not eligible — nothing to speculate. */
        goto out;
    }

    /* Command queue (pipelined commands): STREAMING SLOT-REFILL (fix #1b).
     * The chunked design processed the prefix in phase-bounded windows of
     * DPLUS_BATCH_DEPTH: fill 16, step all finds to completion, validate all
     * 16, repeat. Every chunk boundary was a pipeline bubble — the next
     * chunk's finds started cold with zero overlap.
     *
     * Streaming replaces the phases with a ring of DPLUS_BATCH_DEPTH slots
     * holding IN-FLIGHT finds. Each sweep steps every pending find once
     * (one prefetch issued per slot per sweep — the same interleave cadence
     * that made the chunked design cache-friendly). Replies must be emitted
     * in command order, so validation+reply happens only at the HEAD slot:
     * the moment the head's find completes it is validated and replied
     * (cache-warm), the slot is freed, and the fill loop tops the window
     * back up from the queue. Prefetch depth stays constant across the
     * entire eligible prefix — no bubbles. Out-of-order completions simply
     * wait as completed find states until they become the head; the seqlock
     * validation at reply time covers the (unchanged) racy window.
     * The contiguous-prefix rule holds globally: a validation failure or
     * ineligible command ends speculation for the whole read. */
    cmdQueue *queue = &c->cmd_queue;
    int queue_pos = queue->off;
    int prefix_open = 1;

    /* Ring state: batch[] reused as the slot array. head..tail are monotonic;
     * slot index = seq % DPLUS_BATCH_DEPTH. done[] marks completed finds so
     * they are not re-stepped. batch_count==1 from the first-command block
     * above: seed the ring with it. */
    unsigned head = 0, tail = 0;
    uint8_t done[DPLUS_BATCH_DEPTH];
    hashtableIncrementalFindInit(&batch[0].find_state, ht, batch[0].key_sds);
    done[0] = 0;
    tail = 1;
    (void)batch_count;

    while (head < tail || prefix_open) {
        /* Fill: top the window up from the queue. */
        while (prefix_open && tail - head < DPLUS_BATCH_DEPTH && queue_pos < queue->len) {
            parsedCommand *p = &queue->cmds[queue_pos];
            if (p->cmd == NULL || !(p->cmd->flags & CMD_READONLY) || p->argc != 2 ||
                !(p->cmd->flags & CMD_FAST) || p->cmd != dplus_get_cmd) {
                prefix_open = 0; /* ineligible command ends the global prefix */
                break;
            }
            dplusBatchEntry *e = &batch[tail % DPLUS_BATCH_DEPTH];
            e->key_sds = objectGetVal(p->argv[1]);
            e->hash = hashtableHashKey(ht, e->key_sds);
            e->shard = DPLUS_SHARD_INDEX(e->hash);
            e->v_before = dplusVersionRead(va, e->shard);
            if (e->v_before & 1) {
                /* S2.2a: writer bracket open -- refuse at entry */
#ifdef IO_LOOKUP_OFFLOAD_STATS
                DPLUS_STAT_ADD(bracket_entry_punts, 1);
#endif
                continue;
            }
            e->is_first_cmd = 0;
            e->queue_idx = queue_pos;
            hashtableIncrementalFindInit(&e->find_state, ht, e->key_sds);
            done[tail % DPLUS_BATCH_DEPTH] = 0;
            tail++;
            queue_pos++;
        }
        if (queue_pos >= queue->len) prefix_open = 0; /* queue exhausted — drain */
        if (head == tail) break; /* nothing in flight and nothing to fill */

        /* Sweep: one find step per pending slot (one prefetch each). */
        for (unsigned s = head; s < tail; s++) {
            unsigned i = s % DPLUS_BATCH_DEPTH;
            if (!done[i]) done[i] = !hashtableIncrementalFindStep(&batch[i].find_state);
        }

        /* Drain: validate + reply completed HEADS in command order. */
        while (head < tail && done[head % DPLUS_BATCH_DEPTH]) {
            dplusBatchEntry *e = &batch[head % DPLUS_BATCH_DEPTH];
#ifdef IO_LOOKUP_OFFLOAD_STATS
            DPLUS_STAT_ADD(speculative_attempts, 1);
#endif
            if (dplusValidateAndReply(c, e, ht, c->resp)) {
                if (e->is_first_cmd) {
                    c->read_flags |= READ_FLAGS_DPLUS_SPECULATED;
                } else {
                    queue->cmds[e->queue_idx].read_flags |= READ_FLAGS_DPLUS_SPECULATED;
                }
                speculated++;
                head++;
            } else {
                /* Contiguous-prefix rule: first failure ends speculation
                 * globally. In-flight slots behind it are abandoned (their
                 * commands fall through to main execution). */
                prefix_open = 0;
                head = tail; /* discard remaining in-flight slots */
                break;
            }
        }
    }

out:
    /* Update write-tax gate: shift in 1 if any speculation succeeded, 0 if punted. */
    gate->history = (gate->history << 1) | (speculated > 0 ? 1 : 0);

    /* Commandstats parity: fold execution time into the per-thread counter
     * (calls are accumulated in dplusConsumeSpeculated). */
    if (speculated > 0) dplus_thread_stats[tid].usec += (long long)(getMonotonicUs() - spec_start);

    /* Publish quiescence on every post-entry exit path. */
    dplusReaderWorkerQuiescent(tid);
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

    /* D5: a WAITING_WRITABLE client keeps speculating reads (B13) while its
     * peer refuses to drain the socket, so its output grows with NO completion
     * ever reaching main — main's eviction sums stay stale for a full cron
     * period while CLIENT LIST reads fresh memory, and a hog can be observed
     * over maxmemory-clients without being evicted. If this client's output
     * alone exceeds the whole budget, raise the pressure flag; evictClients
     * (per-command + per-drain on main) refreshes owned accounting when set.
     * Racy config read + relaxed flag: established pattern, main re-verifies. */
    if (consumed > 0 && server.maxmemory_clients != 0) {
        size_t out = atomic_load_explicit(&c->io_tracked_reply_len, memory_order_relaxed) +
                     c->reply_bytes + (size_t)c->bufpos;
        if (out > (size_t)server.maxmemory_clients)
            atomic_store_explicit(&dplus_client_mem_pressure, 1, memory_order_release);
    }
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
    static long long seen_writes[DPLUS_MAX_IO_THREADS] = {0};
    static long long seen_net_bytes[DPLUS_MAX_IO_THREADS] = {0};
    static long long seen_hits[DPLUS_MAX_IO_THREADS] = {0};
    static struct serverCommand *get_cmd = NULL;
    if (!get_cmd) get_cmd = lookupCommandByCString("get");
    for (int i = 0; i < server.io_threads_num; i++) {
        long long n = dplus_thread_stats[i].commands_processed;
        if (n > seen_calls[i]) {
            long long delta = n - seen_calls[i];
            seen_calls[i] = n;
            server.stat_numcommands += delta;
            if (get_cmd) get_cmd->calls += delta;
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
        if (us > seen_usec[i]) {
            long long delta = us - seen_usec[i];
            seen_usec[i] = us;
            if (get_cmd) get_cmd->microseconds += delta;
        }
        /* Fix #2 (main-free completion): fold worker-side write completions
         * into the global write stats. Same monotonic-delta scheme. */
        long long w = dplus_thread_stats[i].owned_writes;
        if (w > seen_writes[i]) {
            server.stat_total_writes_processed += w - seen_writes[i];
            seen_writes[i] = w;
        }
        long long b = dplus_thread_stats[i].owned_net_bytes;
        if (b > seen_net_bytes[i]) {
            server.stat_net_output_bytes += b - seen_net_bytes[i];
            seen_net_bytes[i] = b;
        }
    }
}

/* --- Component 7: Epoch/QSBR-deferred reclamation (entry lifetime) ---
 *
 * Objects are unlinked before retirement, so only readers already active in
 * the retirement epoch can hold their pointers. beforeSleep seals the current
 * segment, advances the global epoch, and reclaims only segments older than
 * every ACTIVE reader. Normal collection never enters exclusive mode.
 *
 * Fixed-size chunks replace the legacy growable limbo vector. This makes
 * append O(1), avoids geometric copy/realloc costs, and permits bounded
 * reclamation without moving the remaining entries. */

#define DPLUS_RETIRE_CHUNK_ENTRIES 128
#define DPLUS_RECLAIM_BUDGET_ENTRIES 1024
#define DPLUS_RECLAIM_BUDGET_US 50
#define DPLUS_RECLAIM_SOFT_ENTRIES 16384
#define DPLUS_RECLAIM_HARD_ENTRIES 32768
#define DPLUS_TEST_READER_SLOT (DPLUS_MAX_IO_THREADS - 1)

typedef struct dplusRetireEntry {
    void *ptr;
    uint32_t bytes_lower_bound;
    uint8_t route;
} dplusRetireEntry;

typedef struct dplusRetireChunk {
    struct dplusRetireChunk *next;
    uint16_t first;
    uint16_t count;
    dplusRetireEntry entries[DPLUS_RETIRE_CHUNK_ENTRIES];
} dplusRetireChunk;

typedef struct dplusRetireSegment {
    struct dplusRetireSegment *next;
    uint64_t epoch;
    size_t entries;
    size_t bytes_lower_bound;
    dplusRetireChunk *head;
    dplusRetireChunk *tail;
    int safe;
} dplusRetireSegment;

static dplusRetireSegment *dplus_retire_open = NULL;
static dplusRetireSegment *dplus_retire_head = NULL;
static dplusRetireSegment *dplus_retire_tail = NULL;
static dplusRetireSegment *dplus_retire_segment_freelist = NULL;
static dplusRetireChunk *dplus_retire_chunk_freelist = NULL;

static size_t dplus_retired_entries = 0;
static size_t dplus_retired_bytes_lower_bound = 0;
static size_t dplus_retired_segments = 0;
static size_t dplus_retired_peak = 0;
static uint64_t dplus_reclaimed_entries = 0;
static uint64_t dplus_epoch_advances = 0;
static uint64_t dplus_epoch_scans = 0;
static uint64_t dplus_reclaim_budget_exhaustions = 0;
static uint64_t dplus_forced_reclaims = 0;
static uint64_t dplus_pressure_activations = 0;
static uint64_t dplus_pressure_forced_drains = 0;
static uint64_t dplus_pressure_forced_wait_us = 0;

static dplusRetireChunk *dplusAllocRetireChunk(void) {
    dplusRetireChunk *chunk = dplus_retire_chunk_freelist;
    if (chunk) {
        dplus_retire_chunk_freelist = chunk->next;
    } else {
        chunk = zmalloc(sizeof(*chunk));
    }
    chunk->next = NULL;
    chunk->first = 0;
    chunk->count = 0;
    return chunk;
}

static void dplusRecycleRetireChunk(dplusRetireChunk *chunk) {
    chunk->next = dplus_retire_chunk_freelist;
    dplus_retire_chunk_freelist = chunk;
}

static dplusRetireSegment *dplusAllocRetireSegment(void) {
    dplusRetireSegment *segment = dplus_retire_segment_freelist;
    if (segment) {
        dplus_retire_segment_freelist = segment->next;
    } else {
        segment = zmalloc(sizeof(*segment));
    }
    memset(segment, 0, sizeof(*segment));
    segment->epoch = atomic_load_explicit(&dplus_reclaim_epoch, memory_order_seq_cst);
    dplus_retired_segments++;
    return segment;
}

static void dplusRecycleRetireSegment(dplusRetireSegment *segment) {
    serverAssert(segment->head == NULL && segment->tail == NULL && segment->entries == 0);
    dplus_retired_segments--;
    segment->next = dplus_retire_segment_freelist;
    dplus_retire_segment_freelist = segment;
}

static uint32_t dplusAllocationLowerBound(void *ptr) {
    size_t bytes = zmalloc_size(ptr);
    return bytes > UINT32_MAX ? UINT32_MAX : (uint32_t)bytes;
}

static int dplusAllReadersQuiescent(void) {
    for (int word = 0; word < DPLUS_READER_ONLINE_WORDS; word++) {
        uint64_t online = atomic_load_explicit(&dplus_reader_online[word], memory_order_seq_cst);
        while (online) {
            int i = word * 64 + __builtin_ctzll(online);
            online &= online - 1;
            uint64_t state = atomic_load_explicit(&dplus_reader_slots[i].state, memory_order_seq_cst);
            if (state != DPLUS_READER_OFFLINE && state != DPLUS_READER_QUIESCENT) return 0;
        }
    }
    return 1;
}

static void dplusActivatePressureGate(void) {
    if (!atomic_load_explicit(&dplus_reclaim_pressure_gate, memory_order_relaxed)) {
        atomic_store_explicit(&dplus_reclaim_pressure_gate, 1, memory_order_seq_cst);
        dplus_pressure_activations++;
    }
    if (!dplus_reclaim_gate_drained && dplusAllReadersQuiescent()) dplus_reclaim_gate_drained = 1;
}

static void dplusClearPressureGate(void) {
    dplus_reclaim_gate_drained = 0;
    atomic_store_explicit(&dplus_reclaim_pressure_gate, 0, memory_order_seq_cst);
}

static void dplusForceRetirePressure(void) {
    monotime start = getMonotonicUs();
    dplusExclusiveEnter(); /* releases any debug-held reader (see enter) */
    dplus_pressure_forced_wait_us += getMonotonicUs() - start;
    dplus_pressure_forced_drains++;
    /* The pressure gate excluded new readers and exclusive entry drained every
     * admitted reader. Keep the gate closed and reclaim subsequent writes
     * immediately until beforeSleep observes an empty backlog. */
    dplus_reclaim_gate_drained = 1;
    dplusForceReclaimAll();
    dplusExclusiveLeave();
}

static int dplusAppendRetired(void *ptr, int route) {
    /* No configured workers, or no ownership (speculation is gated on it), means
     * no speculative pointer can exist: the caller frees on its own route. */
    if (server.io_threads_num <= 1) return 0;
    /* Exclusive mode has already drained readers. Immediate free is required
     * by RENAME/MOVE refcount re-embedding and maxmemory delta accounting. */
    if (atomic_load_explicit(&dplus_exclusive_mode, memory_order_relaxed) > 0) return 0;
    serverAssert(inMainThread());
#ifdef IO_LOOKUP_OFFLOAD_STATS
    /* Deterministic test handshake: do not begin the armed pressure command
     * until the target real reader reaches the worst preemption point. */
    if (atomic_exchange_explicit(&dplus_debug_pressure_sync, 0, memory_order_seq_cst)) {
        monotime start = getMonotonicUs();
        while (!atomic_load_explicit(&dplus_debug_reader_holding, memory_order_seq_cst)) {
            if (getMonotonicUs() - start > 5000000)
                serverPanic("timed out waiting for D+ debug reader hold");
        }
    }
#endif
    /* Reclaim only entries from completed mutation frames. The current pointer
     * has not been appended yet, so its caller may still finish safely after
     * this function returns. */
    if (dplus_retired_entries >= DPLUS_RECLAIM_HARD_ENTRIES && !dplus_reclaim_gate_drained)
        dplusForceRetirePressure();
    /* Once the pressure gate has drained old readers, no new reader can acquire
     * a pointer. Preserve stock immediate reclamation until backlog reaches 0. */
    if (dplus_reclaim_gate_drained) return 0;

    if (dplus_retire_open == NULL) dplus_retire_open = dplusAllocRetireSegment();
    dplusRetireSegment *segment = dplus_retire_open;
    if (segment->tail == NULL || segment->tail->count == DPLUS_RETIRE_CHUNK_ENTRIES) {
        dplusRetireChunk *chunk = dplusAllocRetireChunk();
        if (segment->tail)
            segment->tail->next = chunk;
        else
            segment->head = chunk;
        segment->tail = chunk;
    }

    dplusRetireChunk *chunk = segment->tail;
    dplusRetireEntry *entry = &chunk->entries[chunk->count++];
    entry->ptr = ptr;
    entry->route = (uint8_t)route;
    entry->bytes_lower_bound = dplusAllocationLowerBound(ptr);
    segment->entries++;
    segment->bytes_lower_bound += entry->bytes_lower_bound;
    dplus_retired_entries++;
    dplus_retired_bytes_lower_bound += entry->bytes_lower_bound;
    if (dplus_retired_entries > dplus_retired_peak) dplus_retired_peak = dplus_retired_entries;
    if (dplus_retired_entries >= DPLUS_RECLAIM_SOFT_ENTRIES &&
        !atomic_load_explicit(&dplus_reclaim_pressure_gate, memory_order_relaxed)) {
        dplusActivatePressureGate();
    } else if (atomic_load_explicit(&dplus_reclaim_pressure_gate, memory_order_relaxed) &&
               !dplus_reclaim_gate_drained && (dplus_retired_entries & 1023) == 0) {
        /* Recheck at bounded intervals so an active reader that exits during a
         * large main-thread command batch stops further retirement growth. */
        dplusActivatePressureGate();
    }
    return 1;
}

int dplusDeferFree(robj *o, int route) {
    return dplusAppendRetired(o, route);
}

int dplusDeferFreeRaw(void *ptr) {
    return dplusAppendRetired(ptr, DPLUS_LIMBO_RAW);
}

/* Seal retirements from the current event-loop epoch before publishing the
 * next epoch. A worker entering the new epoch observes all preceding unlinks. */
static void dplusSealRetireEpoch(void) {
    if (dplus_retire_open == NULL) return;
    dplusRetireSegment *segment = dplus_retire_open;
    dplus_retire_open = NULL;
    serverAssert(segment->entries > 0);
    if (dplus_retire_tail)
        dplus_retire_tail->next = segment;
    else
        dplus_retire_head = segment;
    dplus_retire_tail = segment;

    uint64_t old_epoch = atomic_fetch_add_explicit(&dplus_reclaim_epoch, 1, memory_order_seq_cst);
    serverAssert(old_epoch < DPLUS_READER_QUIESCENT - 1);
    dplus_epoch_advances++;
}

static int dplusReaderStateBlocksEpoch(uint64_t state, uint64_t epoch) {
    return state != DPLUS_READER_OFFLINE && state != DPLUS_READER_QUIESCENT && state <= epoch;
}

static int dplusRetireSegmentIsSafe(const dplusRetireSegment *segment) {
    dplus_epoch_scans++;
    for (int word = 0; word < DPLUS_READER_ONLINE_WORDS; word++) {
        uint64_t online = atomic_load_explicit(&dplus_reader_online[word], memory_order_seq_cst);
        while (online) {
            int i = word * 64 + __builtin_ctzll(online);
            online &= online - 1;
            uint64_t state = atomic_load_explicit(&dplus_reader_slots[i].state, memory_order_seq_cst);
            if (dplusReaderStateBlocksEpoch(state, segment->epoch)) return 0;
        }
    }
    return 1;
}

static void dplusReclaimEntry(const dplusRetireEntry *entry) {
    robj *o = entry->ptr;
    switch (entry->route) {
    /* A retired value is a terminal free: it leaves main the same way an
     * undeferred one does (IO-thread slab, else bio), never inline here. */
    case DPLUS_LIMBO_OFFLOAD_PREF:
    case DPLUS_LIMBO_SYNC:
    case DPLUS_LIMBO_ASYNC: freeValueNeverOnMain(NULL, o, -1); break;
    case DPLUS_LIMBO_RAW: zfree(o); break;
    default: serverPanic("invalid D+ retirement route %d", entry->route);
    }
}

static int dplusReclaimSealed(int force) {
    if (dplus_retire_head == NULL) return 1;
    size_t processed = 0;
    monotime start = getMonotonicUs();
    while (dplus_retire_head) {
        dplusRetireSegment *segment = dplus_retire_head;
        if (!segment->safe) {
            if (!force && !dplusRetireSegmentIsSafe(segment)) break;
            segment->safe = 1;
        }

        while (segment->head) {
            if (!force && processed > 0 &&
                (processed >= DPLUS_RECLAIM_BUDGET_ENTRIES ||
                 ((processed & 31) == 0 && getMonotonicUs() - start >= DPLUS_RECLAIM_BUDGET_US))) {
                dplus_reclaim_budget_exhaustions++;
                return 0;
            }
            dplusRetireChunk *chunk = segment->head;
            dplusRetireEntry *entry = &chunk->entries[chunk->first++];
            chunk->count--;
            segment->entries--;
            segment->bytes_lower_bound -= entry->bytes_lower_bound;
            dplus_retired_entries--;
            dplus_retired_bytes_lower_bound -= entry->bytes_lower_bound;
            dplusReclaimEntry(entry);
            dplus_reclaimed_entries++;
            processed++;

            if (chunk->count == 0) {
                segment->head = chunk->next;
                if (segment->head == NULL) segment->tail = NULL;
                dplusRecycleRetireChunk(chunk);
            }
        }

        dplus_retire_head = segment->next;
        if (dplus_retire_head == NULL) dplus_retire_tail = NULL;
        dplusRecycleRetireSegment(segment);
    }
    return dplus_retire_head == NULL;
}

static void dplusManageRetirePressure(void) {
    if (!atomic_load_explicit(&dplus_reclaim_pressure_gate, memory_order_relaxed)) return;
    if (!dplus_reclaim_gate_drained) dplusActivatePressureGate();

    if (dplus_retired_entries >= DPLUS_RECLAIM_HARD_ENTRIES) {
        /* Hard pressure retains the proven fallback. The pressure gate is
         * already closed, so this wait covers only readers admitted earlier. */
        dplusForceRetirePressure();
    }
    if (dplus_retired_entries == 0) dplusClearPressureGate();
}

/* Normal beforeSleep path: publish an epoch boundary and perform bounded work.
 * It never waits unless hard retirement pressure invokes the explicit fallback. */
void dplusReclaimRetired(void) {
    dplusSealRetireEpoch();
    dplusReclaimSealed(0);
    dplusManageRetirePressure();
}

/* Hard-pressure path. The caller must already hold exclusive mode, which is
 * the proof that every reader has quiesced. Preserve every recorded free route
 * but remove the normal event-loop work budget. */
static void dplusTrimRetireFreelists(void) {
    while (dplus_retire_chunk_freelist) {
        dplusRetireChunk *next = dplus_retire_chunk_freelist->next;
        zfree(dplus_retire_chunk_freelist);
        dplus_retire_chunk_freelist = next;
    }
    while (dplus_retire_segment_freelist) {
        dplusRetireSegment *next = dplus_retire_segment_freelist->next;
        zfree(dplus_retire_segment_freelist);
        dplus_retire_segment_freelist = next;
    }
}

void dplusForceReclaimAll(void) {
    serverAssert(atomic_load_explicit(&dplus_exclusive_mode, memory_order_relaxed) > 0);
    dplus_forced_reclaims++;
    dplusSealRetireEpoch();
    dplusReclaimSealed(1);
    serverAssert(dplus_retire_open == NULL && dplus_retire_head == NULL && dplus_retired_entries == 0);
    /* Hard memory pressure must not retain metadata sized for a historical
     * retirement peak. Normal operation still recycles these allocations. */
    dplusTrimRetireFreelists();
}

size_t dplusLimboPeak(void) {
    return dplus_retired_peak;
}

/* Instrumented-build test hook: delay the next admitted real reader at the
 * worst preemption point, after its final recheck and before pointer access. */
int dplusDebugHoldNextReader(long long usec) {
#ifdef IO_LOOKUP_OFFLOAD_STATS
    if (usec <= 0 || usec > 5000000) return C_ERR;
    long long expected = 0;
    if (!atomic_compare_exchange_strong_explicit(&dplus_debug_reader_hold_us, &expected, usec,
                                                 memory_order_seq_cst, memory_order_seq_cst))
        return C_ERR;
    atomic_store_explicit(&dplus_debug_reader_release, 0, memory_order_seq_cst);
    atomic_store_explicit(&dplus_debug_pressure_sync, 1, memory_order_seq_cst);
    return C_OK;
#else
    UNUSED(usec);
    return C_ERR;
#endif
}

/* S5: lock-free observation of the prevalidate hold -- INFO must NOT be
 * used to poll during a hold: its clients section acquires every worker
 * event-loop mutex (b13 info lock), including the parked worker's, so the
 * INFO reply is assembled mid-hold but delivered only after the hold
 * expires -- the poller acts on stale state. This reads two atomics. */
void dplusDebugPrevalidateState(uint64_t *holding, uint64_t *consumed) {
#ifdef IO_LOOKUP_OFFLOAD_STATS
    *holding = (uint64_t)atomic_load_explicit(&dplus_debug_reader_holding, memory_order_seq_cst);
    *consumed = atomic_load_explicit(&dplus_debug_prevalidate_consumed, memory_order_seq_cst);
#else
    *holding = 0;
    *consumed = 0;
#endif
}

/* S5: arm the pre-validation hold (see dplusSpeculateBatch). Overwrite
 * semantics: a previously armed-but-unconsumed hold (the reader punted
 * before validation) is replaced, not an error -- tests retry arming and a
 * stale arm must never poison the next attempt. Also reset the release
 * flag: an exclusive entrant (eviction, defrag duty cycle, FLUSH) may have
 * left it set, which would make the hold a no-op. */
void dplusDebugPrevalidateBump(void) {
#ifdef IO_LOOKUP_OFFLOAD_STATS
    atomic_store_explicit(&dplus_debug_prevalidate_bump, 1, memory_order_seq_cst);
#endif
}

int dplusDebugHoldPrevalidate(long long usec) {
#ifndef IO_LOOKUP_OFFLOAD_STATS
    (void)usec;
    return C_ERR;
#else
    if (usec <= 0 || usec > 5000000) return C_ERR;
    atomic_store_explicit(&dplus_debug_prevalidate_hold_us, usec, memory_order_seq_cst);
    return C_OK;
#endif
}

/* Deterministic test hook: pin an otherwise unused slot in the current epoch.
 * This is reachable only through DEBUG and refuses to overlap a real worker. */
int dplusDebugPinReader(uint64_t *epoch) {
    if (server.io_threads_num > DPLUS_TEST_READER_SLOT) return C_ERR;
    uint64_t expected = DPLUS_READER_OFFLINE;
    uint64_t current = atomic_load_explicit(&dplus_reclaim_epoch, memory_order_seq_cst);
    if (!atomic_compare_exchange_strong_explicit(&dplus_reader_slots[DPLUS_TEST_READER_SLOT].state,
                                                 &expected, current,
                                                 memory_order_seq_cst, memory_order_seq_cst))
        return C_ERR;
    atomic_fetch_or_explicit(&dplus_reader_online[DPLUS_TEST_READER_SLOT / 64],
                             UINT64_C(1) << (DPLUS_TEST_READER_SLOT % 64), memory_order_seq_cst);
    *epoch = current;
    return C_OK;
}

int dplusDebugUnpinReader(void) {
    if (server.io_threads_num > DPLUS_TEST_READER_SLOT) return C_ERR;
    uint64_t state = atomic_load_explicit(&dplus_reader_slots[DPLUS_TEST_READER_SLOT].state, memory_order_seq_cst);
    if (state == DPLUS_READER_OFFLINE || state == DPLUS_READER_QUIESCENT) return C_ERR;
    atomic_store_explicit(&dplus_reader_slots[DPLUS_TEST_READER_SLOT].state, DPLUS_READER_OFFLINE, memory_order_seq_cst);
    atomic_fetch_and_explicit(&dplus_reader_online[DPLUS_TEST_READER_SLOT / 64],
                              ~(UINT64_C(1) << (DPLUS_TEST_READER_SLOT % 64)), memory_order_seq_cst);
    return C_OK;
}

void dplusDebugEpochStats(uint64_t stats[8]) {
    stats[0] = atomic_load_explicit(&dplus_reclaim_epoch, memory_order_seq_cst);
    stats[1] = dplus_retired_entries;
    stats[2] = dplus_reclaimed_entries;
    stats[3] = dplus_forced_reclaims;
    stats[4] = dplus_epoch_advances;
    stats[5] = dplus_epoch_scans;
    stats[6] = atomic_load_explicit(&dplus_reclaim_pressure_gate, memory_order_seq_cst);
    stats[7] = dplus_pressure_activations;
}

/* --- Component 6: INFO section --- */
/* Door-2 wakeup-coalescing counters (prototype): sums of per-worker plain
 * fields written only by their owning worker — racy-by-design stats reads.
 * Always compiled (not gated on IO_LOOKUP_OFFLOAD_STATS) so the coalescing
 * A/B can read them from a default build's INFO stats. */
long long dplusDoorbellRings(void) {
    long long sum = 0;
    for (int i = 0; i < DPLUS_MAX_IO_THREADS; i++) sum += dplus_thread_stats[i].doorbell_rings;
    return sum;
}

long long dplusDoorbellCoalesced(void) {
    long long sum = 0;
    for (int i = 0; i < DPLUS_MAX_IO_THREADS; i++) sum += dplus_thread_stats[i].doorbell_coalesced;
    return sum;
}

sds dplusInfoString(sds info) {
    /* Worker-owned counters are intentionally sampled without shared-line
     * writes. Lifecycle states are atomic because main/BIO exclusive scans
     * consume the same values for correctness. */
    uint64_t entries = 0, retries = 0, epoch_exclusive_punts = 0, pressure_punts = 0;
    unsigned online = 0, active = 0, quiescent = 0;
    long long doorbell_rings = 0, doorbell_coalesced = 0, punted_replies = 0;
    int debug_reader_holding = 0;
    long long debug_reader_hold_us = 0;
#ifdef IO_LOOKUP_OFFLOAD_STATS
    debug_reader_holding = atomic_load_explicit(&dplus_debug_reader_holding, memory_order_relaxed);
    debug_reader_hold_us = atomic_load_explicit(&dplus_debug_reader_hold_us, memory_order_relaxed);
#endif
    for (int i = 0; i < DPLUS_MAX_IO_THREADS; i++) {
        uint64_t state = atomic_load_explicit(&dplus_reader_slots[i].state, memory_order_acquire);
        if (state != DPLUS_READER_OFFLINE) {
            online++;
            if (state == DPLUS_READER_QUIESCENT)
                quiescent++;
            else
                active++;
        }
        entries += dplus_epoch_thread_stats[i].entries;
        retries += dplus_epoch_thread_stats[i].retries;
        epoch_exclusive_punts += dplus_epoch_thread_stats[i].exclusive_punts;
        pressure_punts += dplus_epoch_thread_stats[i].pressure_punts;
        doorbell_rings += dplus_thread_stats[i].doorbell_rings;
        doorbell_coalesced += dplus_thread_stats[i].doorbell_coalesced;
        punted_replies += dplus_thread_stats[i].punted_replies_written;
    }
    info = sdscatprintf(info,
        "# Dplus\r\n"
        "dplus_reclaim_epoch:%llu\r\n"
        "dplus_epoch_reader_entries:%llu\r\n"
        "dplus_epoch_reader_retries:%llu\r\n"
        "dplus_epoch_exclusive_punts:%llu\r\n"
        "dplus_epoch_pressure_punts:%llu\r\n"
        "dplus_reclaim_pressure_gate:%d\r\n"
        "dplus_pressure_activations:%llu\r\n"
        "dplus_epoch_workers_online:%u\r\n"
        "dplus_epoch_workers_active:%u\r\n"
        "dplus_epoch_workers_quiescent:%u\r\n"
        "dplus_retired_entries:%zu\r\n"
        "dplus_retired_bytes_lower_bound:%zu\r\n"
        "dplus_retired_segments:%zu\r\n"
        "dplus_retired_peak:%zu\r\n"
        "dplus_reclaimed_entries:%llu\r\n"
        "dplus_epoch_advances:%llu\r\n"
        "dplus_epoch_scans:%llu\r\n"
        "dplus_reclaim_budget_exhaustions:%llu\r\n"
        "dplus_forced_reclaims:%llu\r\n"
        "dplus_pressure_forced_drains:%llu\r\n"
        "dplus_pressure_forced_wait_us:%llu\r\n"
        "dplus_epoch_debug_reader_holding:%d\r\n"
        "dplus_epoch_debug_reader_hold_us:%lld\r\n"
        "dplus_doorbell_rings:%llu\r\n"
        "dplus_doorbell_coalesced:%llu\r\n"
        "dplus_punted_replies_written:%llu\r\n",
        (unsigned long long)atomic_load_explicit(&dplus_reclaim_epoch, memory_order_relaxed),
        (unsigned long long)entries,
        (unsigned long long)retries,
        (unsigned long long)epoch_exclusive_punts,
        (unsigned long long)pressure_punts,
        atomic_load_explicit(&dplus_reclaim_pressure_gate, memory_order_relaxed),
        (unsigned long long)dplus_pressure_activations,
        online,
        active,
        quiescent,
        dplus_retired_entries,
        dplus_retired_bytes_lower_bound,
        dplus_retired_segments,
        dplus_retired_peak,
        (unsigned long long)dplus_reclaimed_entries,
        (unsigned long long)dplus_epoch_advances,
        (unsigned long long)dplus_epoch_scans,
        (unsigned long long)dplus_reclaim_budget_exhaustions,
        (unsigned long long)dplus_forced_reclaims,
        (unsigned long long)dplus_pressure_forced_drains,
        (unsigned long long)dplus_pressure_forced_wait_us,
        debug_reader_holding,
        debug_reader_hold_us,
        (unsigned long long)doorbell_rings,
        (unsigned long long)doorbell_coalesced,
        (unsigned long long)punted_replies);
#ifdef IO_LOOKUP_OFFLOAD_STATS
    info = sdscatprintf(info,
        "dplus_speculative_attempts:%llu\r\n"
        "dplus_speculative_hits:%llu\r\n"
        "dplus_validation_misses:%llu\r\n"
        "dplus_exclusive_punts:%llu\r\n"
        "dplus_large_value_punts:%llu\r\n"
        "dplus_prevalidate_consumed:%llu\r\n"
        "dplus_pv_last_key:%s\r\n"
        "dplus_pv_last_client:%llu\r\n"
        "dplus_bracket_entry_punts:%llu\r\n"
        "dplus_expired_replies:%llu\r\n"
        "dplus_intra_batch_write_punts:%llu\r\n"
        "dplus_miss_punts:%llu\r\n"
        "dplus_b13_waiting_transitions:%llu\r\n"
        "dplus_b13_handler_fires:%llu\r\n"
        "dplus_b13_read_suspends:%llu\r\n"
        "dplus_b13_rearms:%llu\r\n"
        "dplus_b13_info_lock_calls:%llu\r\n"
        "dplus_b13_info_lock_wait_us:%llu\r\n"
        "dplus_b13_info_lock_hold_us:%llu\r\n",
        (unsigned long long)DPLUS_STAT_SUM(speculative_attempts),
        (unsigned long long)DPLUS_STAT_SUM(speculative_hits),
        (unsigned long long)DPLUS_STAT_SUM(validation_misses),
        (unsigned long long)DPLUS_STAT_SUM(exclusive_punts),
        (unsigned long long)DPLUS_STAT_SUM(large_value_punts),
        (unsigned long long)atomic_load_explicit(&dplus_debug_prevalidate_consumed, memory_order_seq_cst),
        dplus_debug_pv_last_key,
        (unsigned long long)atomic_load_explicit(&dplus_debug_pv_last_client, memory_order_seq_cst),
        (unsigned long long)DPLUS_STAT_SUM(bracket_entry_punts),
        (unsigned long long)DPLUS_STAT_SUM(expired_replies),
        (unsigned long long)DPLUS_STAT_SUM(intra_batch_write_punts),
        (unsigned long long)DPLUS_STAT_SUM(miss_punts),
        (unsigned long long)DPLUS_STAT_SUM(b13_waiting_transitions),
        (unsigned long long)DPLUS_STAT_SUM(b13_handler_fires),
        (unsigned long long)DPLUS_STAT_SUM(b13_read_suspends),
        (unsigned long long)DPLUS_STAT_SUM(b13_rearms),
        (unsigned long long)DPLUS_STAT_SUM(b13_info_lock_calls),
        (unsigned long long)DPLUS_STAT_SUM(b13_info_lock_wait_us),
        (unsigned long long)DPLUS_STAT_SUM(b13_info_lock_hold_us));
#endif
    return info;
}
