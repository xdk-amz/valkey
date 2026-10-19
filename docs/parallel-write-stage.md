# Parallel write stage

Design for running the staged loop's write phase on main and the IO threads at once. Base:
staged-pr `e43737058` (`docs/staged-event-loop.md`). Code audits behind it, with file:line
references at that commit: `valkey-main-thread-offload/parallel-writes/audit/{A,B,C}*.md`.

## Why

On SET-only load the staged loop is main-bound in the write phase while the IO threads wait at
the barrier. Two-host sweep, c8g.metal-24xl, P16, 100k keys, 64-byte values, 3 reps
(`parallel-writes/data/base1-set-p16.jsonl`):

| cores | stock SET/s | staged SET/s | staged main CPU | staged IO-thread cores |
|---:|---:|---:|---:|---:|
| 2 | 2.25M | 1.45M | 52% | 0.88 |
| 4 | 2.41M | 1.82M | 64% | 1.19 |
| 8 | 2.35M | 1.79M | 67% | 1.87 |

Staged loses 25-35% to stock and does not scale past 4 cores: the write phase serializes every
command on main, and the read and reply phases cannot overlap it. Splitting the write phase over
N threads shortens the iteration by up to (N-1)/N of its write share.

## Prototype status (2026-10-16)

Implemented in `~/repos/parallel-writes` (uncommitted; build snapshots `8a21e82e2` standalone
SET/GET, `b71c40209` adds cluster mode and the string family, `80edcafda` the allocator counter
fix below, `298197a0d` hashes, sets, sorted sets and lists with per-key blocked and watched checks,
`9ea2a705e` lane-formatted replication frames, `51d2ad4bb` commandstats by command id, `4e4d24177`
no propagation work without a sink):

- the execution-context refactor (`execContext`, `ectx`), mechanical;
- IO-thread classification at harvest, per-thread lane vectors, segments split by barrier
  entries, `STAGED_PHASE_WRITE`, the lane executor, per-lane reply arenas, T-order propagation
  with `woff`, per-lane stats folded after each phase (latency histograms folded on read);
- lane commands: SET (all forms), GET, the string family and key commands (DEL, UNLINK, EXISTS,
  TOUCH, TYPE, TTL/PTTL, EXPIRE family, PERSIST), MGET/MSET; hashes including field expiration
  (HSET to HGETDEL, HSCAN), sets (SADD to SDIFFSTORE, SSCAN), sorted sets (ZADD to ZINTERCARD,
  ZSCAN), lists (LPUSH to RPOPLPUSH, LMPOP); any of them only when every key is in one shard.
  SPOP, SRANDMEMBER, HRANDFIELD and ZRANDMEMBER stay on main: the random generators are
  process-wide. PING runs on any lane;
- clients blocked on list, sorted set or stream keys, and clients watching keys, no longer turn
  the lanes off: the IO thread checks each key at harvest, and a command on a key someone is
  blocked on or watches is a barrier, so main serves or dirties that client at its place. The
  blocked and watched key sets only shrink until the fast path's part of the write phase ends, and
  main keeps those dicts from rehashing while lanes run, so every lookup in them only reads.
  Module-blocked clients still turn the lanes off;
- per-thread state the type code assumed single-threaded: the hash listpack "ignore TTL" flag is
  thread-local, and the expired-fields counter is in the execution context;
- propagation: when the replication backlog is the only sink (AOF off, no slot export), each lane
  formats its entries' replication frames itself and main feeds them in T order, adding SELECT,
  MULTI and EXEC as replicationFeedReplicas would; with no sink at all the lanes drop the ops;
  otherwise main replays the ops through propagateNow. The merge walks each phase's entries in
  their IO threads' harvest order, which is T order, and stores each client's `woff` once per run;
- cluster mode: the staged loop and the fast path now run with `cluster-enabled yes`; commands
  for slots this node does not serve stably go to main for their redirect; lanes are by slot;
- config `io-threads-parallel-writes` (default yes), hidden `io-threads-parallel-writes-min`
  (default 32); INFO `fastpath_write_lane_phases|lane_entries|barriers|serial_segments`;
- `tests/unit/staged-loop-lanes.tcl` (24 tests: order, replies, stats, errors, expiry, barriers,
  the four types, emptied keys, field expiry, a blocked client served in order with lanes on, a
  WATCH aborted with lanes on, MONITOR fallback, replica digest over all types, stream length equal
  to the serial path's, WAIT, cluster slot lanes, slot stats, one lane running 67 distinct commands
  in a phase, CROSSSLOT).
- Suites with the lanes forced on (`--config notify-keyspace-events '""'`, every segment to the
  lanes): all type suites, hashexpire, expire, keyspace, multi, scan, sort, geo, dump, lazyfree, ACL,
  staged and fast-path suites, cluster slot-stats/multi-slot/replica-redirect, replication, AOF pass,
  except tests that fail on the base too or with 1024 shards (unit/info rehashing count; one
  replication rdb-pipe flake, 3/3 on rerun) and one hashexpire test whose third server inherits the
  notifications-off config (passes with it on). A TSan build (gcc 10) of the lane suite and of a
  mixed-type load with a replica shows only the bio `makeThreadKillable` report that the same load
  shows with lanes off.

SET-only, P16, 100k keys, 64-byte values, metal-10 server and metal-13 generator (0.12 ms), 3
reps per cell, builds rotated, every rep valid (`parallel-writes/data/ab1-set-p16.jsonl`). Peak
over 16/32/64 connections per core; stock at io-threads = cores, staged and lanes with main and
io_thd_1 on one core (io-threads = cores + 1):

| cores | stock | staged | staged, 1024 shards | lanes | lanes vs stock | lanes vs staged |
|---:|---:|---:|---:|---:|---:|---:|
| 2 | 2.27M | 1.52M | 1.47M | 2.12M | 0.93x | 1.39x |
| 4 | 2.40M | 1.88M | 1.80M | 3.82M | 1.59x | 2.03x |
| 8 | 2.40M | 2.17M | 2.08M | 6.10M | 2.54x | 2.81x |
| 16 | 2.44M | 2.35M | 2.23M | 9.79M | 4.01x | 4.17x |

At 16 cores and 1024 connections, p50 is 1.66 ms with lanes against 6.78 ms stock and 7.93 ms
staged. Main's CPU drops to 2-14%: it only orchestrates. The 1024-shard keyspace costs staged
3-5%.

Cluster mode, one node serving every slot (no redirects), same rig and cell shape, 3 reps, all
valid (`parallel-writes/data/ab2-cluster-set-p16.jsonl`); snapshot `b71c40209`:

| cores | stock, cluster | lanes, cluster | lanes vs stock | lanes, standalone (same build) |
|---:|---:|---:|---:|---:|
| 4 | 2.21M | 3.58M | 1.62x | 3.76M |
| 8 | 2.20M | 5.72M | 2.60x | 6.22M |
| 16 | 2.18M | 9.22M | 4.22x | 9.58M |

Cluster lanes run 4-8% below standalone ones (a CRC16 slot per command on the IO thread and 16384
hashtables against 1024).

80/20 GET/SET (cachecannon `mixed`), same rig, P16, 32 and 64 connections per core, 3 reps per
cell, builds rotated, all valid (`parallel-writes/data/mix1-8020-p16.jsonl`); rr is the
pr-readiness branch (`59674df38`: IO-thread GETs with D+ speculation, ACL offload), lanes is
`80edcafda`. Peak over the two connection counts:

| cores | stock | staged | rr | lanes | lanes vs stock | lanes vs rr |
|---:|---:|---:|---:|---:|---:|---:|
| 2 | 2.92M | 1.78M | 1.81M | 2.25M | 0.77x | 1.24x |
| 4 | 3.15M | 2.52M | 3.85M | 4.43M | 1.41x | 1.15x |
| 8 | 3.12M | 3.12M | 3.58M | 8.46M | 2.71x | 2.36x |
| 16 | 3.17M | 3.58M | 3.52M | 14.31M | 4.51x | 4.07x |

With 20% writes in every pipeline, most GETs sit behind a SET of their own client, so they are
not answered in the read phase: stock, staged and rr all run them on main and stop at 3.1-3.9M
whatever the core count. The lanes run them in parallel with the SETs. At 16 cores and 1024
connections p50 is 1.06 ms with lanes against 5.31 ms stock and 4.42 ms rr. At 2 cores (one
core shared by main and an IO thread, one more for the other IO thread) the staged phases cost
more than two lanes save. (One lanes rep at 4 cores, 256 connections ran while a build was
compiling on the server host by mistake; it was 3% below the cell's other two reps, and the cell
median is unchanged.)

Hash, set, sorted set and list writes: `valkey-benchmark` (64 threads) on the generator host, one
command each, P16, 64 connections per core, keys drawn from 100k and fields/members from 100k, no
replica, 3 reps per cell, all valid (`parallel-writes/data/r3-types-*.jsonl`); lanes is
`9ea2a705e`:

| command | cores | stock | staged | lanes | lanes vs stock |
|---|---:|---:|---:|---:|---:|
| `HSET h f v` | 8 | 0.70M | 0.67M | 1.92M | 2.7x |
| | 16 | 0.69M | 0.67M | 3.82M | 5.6x |
| `SADD s m` | 8 | 1.22M | 1.07M | 5.71M | 4.7x |
| | 16 | 1.23M | 1.14M | 9.50M | 7.7x |
| `ZADD z score m` | 8 | 0.61M | 0.60M | 2.68M | 4.4x |
| | 16 | 0.60M | 0.59M | 4.74M | 7.9x |
| `LPUSH l v` | 8 | 1.98M | 1.82M | 6.09M | 3.1x |
| | 16 | 1.94M | 1.87M | 9.14M | 4.7x |

Stock and staged are main-bound (main 87-100%) on every one; the lanes keep main under 10%.

A profile at 16 cores (`parallel-writes/data/profile-16c-lanes2-flat.txt`) put 15% of all CPU in
`zfree`: zmalloc's per-thread used-memory counters were 8-byte neighbours, eight to a cache line,
and on Linux aarch64 (not in the architecture list) every update was an atomic read-modify-write,
so 16 lanes allocating at once fought over the lines. Snapshot `80edcafda` gives each counter its
own cache line and updates it with a relaxed load and store (one writer per counter):

| cores | lanes `b71c40209` | lanes `80edcafda` | change | stock |
|---:|---:|---:|---:|---:|
| 4 | 3.74M | 3.96M | +6% | 2.40M |
| 8 | 6.12M | 6.78M | +11% | 2.36M |
| 16 | 9.57M | 10.06M | +5% | 2.41M |

(64 connections per core, 3 reps, `parallel-writes/data/ab4-zmalloc.jsonl`.) The fix is in upstream
code and helps any path where several threads allocate at once.

The refactor costs nothing measurable: with io-threads 1 (plain main loop) the build runs at
1.417-1.419M against the base's 1.421-1.425M, and on the staged path with lanes off at 4 cores at
1.804M against 1.801M (`ab3-refactor-*.jsonl`).

What the 16-core profile shows next (`profile-16c-lanes3-flat.txt`): the lane loop and executor
setup 12%, waiting at the barrier 9%, jemalloc size lookups and frees about 14% (SET allocates a
new embedded object per write), hashtable lookups 12% (lanes do not prefetch yet), parsing 4%.
Load is even across the 16 IO threads; main and the epoll thread are under 0.3%.

## Model

### Reference order T

Main executes a write phase's entries today in one order: IO thread id, then that thread's
batches in order, then entries in a batch. Call it T. T respects every client's order, because a
client's entries in one iteration are produced by one IO thread, contiguously and in the order
the client sent them. Any order with that property is a valid linearization of the iteration:
commands read in the same read phase are concurrent, none has observed another's reply.

### Lanes

Each entry that is lane-eligible (below) maps to one shard: a keyspace hashtable index, which is
the cluster slot in cluster mode and the `keyspace-shard-bits` shard in standalone mode. Lane =
shard mod nlanes, where nlanes is the number of write-phase participants (the enrolled IO
threads, plus main when it is not pinned onto a worker's CPU). A lane executes its entries in T
order. A lane exclusively owns its shards' `keys`, `expires` and `keys_with_volatile_items`
hashtables for the phase.

Why the result equals executing T serially: two entries conflict only through state both touch.
Lane-eligible entries touch only their shard's hashtables plus state that is frozen or
lane-private during the phase (see Shared state). Entries on one shard run on one lane, in T
order. So every conflicting pair runs in T order, and the outcome (keyspace, every reply, every
propagated effect once merged in T order) is the outcome of serial T. Physical interleaving
across lanes is not observable: nothing reads two shards inside a phase except a barrier entry,
which runs alone.

Per-client sequence numbers are not enough on their own: they reorder one client's replies but
do not make two clients' interleaving serializable. With `SET a 1; GET b` from X and `SET b 1;
GET a` from Y on four lanes, both GETs can read nil, which no serial order allows. Following T on
every lane prevents that. T is a sequence number shared by all clients.

Replies stay ordered without reordering: an entry's reply is stored in the entry, and the reply
phase delivers each batch's entries in order, as today.

### Classification, on the IO thread at harvest

The IO thread that parses a command already knows its command, keys, db, ACL verdict and (in
cluster mode) slot. At `fpAppendEntry` it also records a lane id, or marks the entry a barrier.
An entry is lane-eligible when all of these hold:

- lanes are on for this iteration (main decides once before the read phase; see Fallbacks);
- the command is on the lane allowlist (a per-command flag, rolled out by command family);
- all its keys are in one shard (cluster mode: no CROSSSLOT, slot >= 0);
- the ACL tag is ALLOW under the current ACL epoch and the principal is not retired, and auth
  is not required (otherwise the main path must produce NOPERM/NOAUTH and the ACL log entry);
- cluster mode: the slot is served by this node, is neither migrating nor importing, and the
  cluster state is ok (otherwise the main path produces MOVED/ASK/TRYAGAIN/CLUSTERDOWN).

Keyless commands with no keyspace effect (PING) are lane-eligible on any lane. Everything else
that is on the fast path is a barrier entry. The IO thread computes the shard once; the lane's
executor carries it in `c->slot`, so lookups do not hash the key again (standalone included).

The state these checks read is frozen from the start of the read phase to the end of the fast
path's part of the write phase: cluster topology, ACL rules and users, blocked and watched keys,
config, are changed only by main-path commands, cluster bus handlers and crons, which run after
`fastpathStagedExecute` or between iterations. A barrier entry can change some of it (a served
blocked client that blocks again, a value turned into a module type by RESTORE); main
re-evaluates the lane conditions after every barrier entry and runs the rest of the iteration
serially if they no longer hold.

### The write phase

Barriers split T into segments. Main runs, in T order:

1. For each segment: one `STAGED_PHASE_WRITE` phase on the staged barrier, where each
   participant runs its lane's entries of that segment, then the merge (below) on main. A
   segment too small to pay for a phase (hidden knob, default 32 entries) runs serially on main
   with `fpExecute` instead.
2. For each barrier entry: `fpExecute` on main, the full `processCommand` path, then
   `handleClientsBlockedOnKeys` as today.

Then `aeProcessEvents` for the main-path clients and main fds, as today, then the reply phase.

Each IO thread keeps, per depth, one entry vector per lane and its barrier positions, filled at
harvest. Lane L in segment g walks, for each thread in id order, that thread's lane-L entries
whose segment number is g. Main does no per-entry work to build lanes; its per-segment work is
proportional to threads times lanes.

The read phase that precedes and the reply phase that follows are unchanged. Workers come out of
the read phase spinning, so the write phase usually starts without a sleep/wake round trip.

### What a lane does per entry

`laneCall` is the part of `processCommand` plus `call()` that applies to a pre-validated entry,
run on the lane's own executor client with the lane's execution context:

- skip the entry if main asked to close its client (`control->closing`), as `fpExecute` does;
- point the executor at the entry (argv, command, db, RESP, user, slot) and its reply buffer at
  the lane's reply arena;
- `cmd->proc`, bracketed by the execution-unit counters, with the phase's command time;
- the dirty delta decides propagation exactly as in `call()`; the unit's ops are captured into
  the entry with the MULTI/EXEC wrap decision instead of being propagated;
- commandstats, error stats, duration stats, keyspace hit/miss and expired counts go to the
  lane's accumulators; a commandlog or latency-monitor sample over threshold is prepared on the
  lane and handed to main;
- the reply is left in the lane arena (spilling to a heap block like `reply_big`), and argv is
  released as `fpExecute` releases it.

### Merge, on main after each lane phase

- Propagation, in T order: main walks the phase's entries in their IO threads' harvest order
  (which is T) and, for each lane entry that propagated, either feeds the frames the lane already
  formatted (the replication backlog is the only sink: the lane wrote the RESP of each op with
  `replicationCatCommand`, main adds SELECT by the backlog's cursor and MULTI/EXEC as
  `propagatePendingCommands` would) or replays its ops through `propagateNow` (AOF on or a slot
  exporting). With no sink at all the lanes drop the ops. A client's `woff` is set once per run of
  its entries. The stream is the one serial T produces; a test compares its length with the
  serial path's for the same pipeline. The `appendfsync always` flush sits between the write and
  reply phases.
- Counters: lane accumulators added into the server's and zeroed. Order does not matter.
- Ordered side effects (commandlog inserts, error-count increments, deferred client closes):
  merged after the phase.
- Main's cost per propagated write: one frame copy into the backlog, against formatting and
  copying every argument when it replayed the ops (`80edcafda`), which capped a lanes primary
  with a replica at 1.8M SET/s on 8 cores.
- With no sink the lanes skip the capture and main skips the walk (`4e4d24177`): walking every entry
  for nothing had cost 2-5% without a replica (16 cores: SET 9.77M against 10.13M with the old op
  replay, 10.30M with the skip; 80/20 13.55M, 14.27M, 14.27M; `r7-*.jsonl`). With a replica the
  two are equal (6.60M and 6.57M at 16 cores).

## Execution context

The per-execution-unit globals become one `execContext`, reached through a thread-local pointer
that every thread starts with pointing at main's context. Lane threads switch to their lane's
context for the length of a write phase and back after it. Module threads holding the GIL, bio
threads and IO threads outside a lane keep main's context, so their behavior is unchanged.

| field | today | sites (`src/*.c`) |
|---|---|---|
| `dirty` | `server.dirty` | 118, about 84 inside command implementations |
| `current_client`, `executing_client` | `server.*` | 73, 17 |
| `execution_nesting`, `in_call` | `server.*` | 14, ~5 |
| `cmd_time_snapshot` | `server.*`, `commandTimeSnapshot()` | 50 |
| `also_propagate` | `server.*` | 15 |
| `lazy_expire_disabled` | `server.*` | 6 |
| lane accumulators | `stat_numcommands`, `stat_keyspace_hits/misses`, `stat_expiredkeys`, `stat_total_error_replies`, `incrCommandStatsOnError`'s static `prev_err_count`, `duration_stats` | a few each |

The rename is mechanical (`server.dirty` to `ectx->dirty` and so on). On main, `ectx->dirty` is
the server's dirty count; a lane's starts at 0 every phase and is added at the merge. A lane does
not update the cached clock: main updates it before the phase and every lane uses that phase's
`cmd_time_snapshot`, so all of a phase's commands execute at one instant. This is a valid
serial execution of T, and the instant is within the iteration that read the commands.

Commandstats (`calls`, `microseconds`, `failed_calls`, `rejected_calls`) and the per-command
latency histograms live per lane and are folded where they are read (INFO, LATENCY HISTOGRAM,
CONFIG RESETSTAT, beforeSleep), the way `dplus.c` folds IO-thread GETs today.

## Shared state, by class

Everything a lane-eligible command touches is in one of four classes. The audits list every
item with file:line; the summary:

| class | members |
|---|---|
| lane-owned | the shard's `keys`, `expires`, `keys_with_volatile_items` hashtables, including their incremental rehash steps and resizes; the value objects in them (LRU/LFU bits, encodings, field expiry); per-slot `CLUSTER SLOT-STATS` cpu and network-in counters (a lane owns its slots) |
| frozen (read-only in the phase) | config, ACL rules and users, cluster topology, `blocking_keys`, `watched_keys`, monitors list, module subscriber lists, cached clock, replication role and state, `paused_actions`, child state, `shared.*` objects, kvstore importing flags |
| execution context / lane accumulators | the table above; per-lane reply arena and executor client; per-lane commandstats and histograms |
| merged by main after the phase | propagation and `woff` (T order); commandlog inserts and latency-monitor samples (T order); error-count increments; counters (any order) |

Shared kvstore bookkeeping (`key_count`, the slot-size Fenwick tree, the rehashing list, bucket
and overhead counters, `allocated_hashtables`) is updated under the kvstore's concurrent-mode
mutex that replica parallel apply already uses. Two changes: `freeHashtableIfNeeded` decrements
`allocated_hashtables` outside the lock today and must take it; and the mutex is taken on every
key add and delete, so the key count and Fenwick deltas should become per-lane and be applied at
the merge (no reader runs inside the phase).

Memory accounting is already per-thread (`used_memory_thread[]`).

## Frees

`freeValueNeverOnMain` funnels into a single main-only slab, so a lane cannot use it. In a lane,
an overwritten or deleted value is freed on the lane thread, which is already off main, except
that streams and module values go to bio as they do from main, and so does any collection whose
lazyfree effort is above 64 (so one big free cannot hold the phase; main would have sent it to an
IO thread, so `lazyfreed_objects` counts it where the base would not). Replica parallel apply
already takes the inline branch. With lane-formatted frames the lanes also drop the ops' argv
references themselves.

## Expiry

A lane may delete an expired key it finds in its own shard: the delete, the captured DEL (wrapped
with the command, as today), the expired count and the free are all lane-local or merged. The
`inMainThread()` debug asserts on keyspace deletes and lazy expiry become "main, or a lane thread
inside a write phase". Active expire, `kvstoreIncrementallyRehash`, resize and defrag stay main-
only and never overlap a phase (crons and beforeSleep run between phases). `moduleNotifyKeyUnlink`
moves `lazy_expire_disabled` into the execution context; its module callbacks only matter when
modules observe writes, which is a fallback condition.

## Eviction and maxmemory

`performEvictions` deletes keys in any shard, so it cannot run in a lane. With
`maxmemory-eviction-batch` (this branch) eviction already runs once per iteration in beforeSleep,
and the OOM verdict (`pre_command_oom_state`) is frozen for the iteration. With that config lanes
can run under maxmemory: DENYOOM entries get the frozen verdict. Without it, maxmemory set is a
fallback. Client eviction (`maxmemory-clients`) follows the same rule.

## Blocked clients, WATCH, tracking, notifications, MONITOR

- Blocked clients: after a push to a key with blocked clients, `processCommand` serves them
  (`handleClientsBlockedOnKeys`) before the next command on that key, so such an entry must run
  at its T position on main. Implemented per key: while clients are blocked on list, sorted set or
  stream keys, the IO thread looks each key of a lane command up in `db->blocking_keys` at
  harvest, and a hit makes the entry a barrier. Sound because the set only shrinks between the
  read phase and the end of the fast path's execution (no fast-path command blocks; a barrier that
  serves a blocked client re-blocks it, if at all, on the same keys; main-path commands run after),
  and because main runs lanes only while none of these dicts is rehashing, so the IO threads' and
  the lanes' lookups (`signalKeyAsReady`'s) only read. Module-blocked clients still turn lanes off.
- WATCH: same scheme with `db->watched_keys`: a write to a watched key is a barrier, so main
  marks the watcher dirty at its place; `touchWatchedKey` in a lane finds nothing and returns.
- Tracking: lanes off while any client tracks or the tracking tables hold entries
  (`TrackingTable` is never freed once created, so a NULL check is not enough). Later: the lane
  looks the key up read-only and defers the invalidation to the merge, in T order, ahead of the
  reply phase, which keeps the BCAST ordering fix intact.
- Keyspace notifications: lanes off while `notify-keyspace-events` is set or modules observe
  writes (`moduleObservesWrites`: filters, event listeners, keyspace subscribers, result
  listeners, module data types). Later for pubsub-only notifications: per-lane (event, key, db)
  records replayed in T order. Module subscribers stay a fallback: their callbacks are arbitrary.
- MONITOR: lanes off while a monitor is attached (the feed would need T-ordered replay; not worth
  it).

## Standalone sharding

Lanes need more than one hashtable per db. Standalone uses `keyspace-shard-bits` (immutable;
replica parallel apply introduced it). Choices:

- Shard function. Today a word-at-a-time hash that ignores hash tags. Using the cluster slot
  (`keyHashSlot`, CRC16 with hash tags) instead gives one lane and kvstore code path for both
  modes and lets hash-tagged multi-key commands stay single-shard. It costs a slower hash, paid
  once per command on the IO thread when the shard is cached in the entry. To measure.
- Shard count. Lanes balance when shards outnumber lanes by a wide margin (1024 shards over 12
  lanes: max lane 1% above mean; 64 shards: 12%). Replica apply measured a 17-18% read cost for
  1024 shards, mostly from hashing every lookup again; the cached shard removes that. To measure.
- Default. Lanes do nothing at 0 bits. A default (for example 10 bits when io-threads > 1) is a
  decision once the cost is measured.

## Cluster mode

Three gates keep the staged loop, fast-path admission and READ-phase GETs off in cluster mode
(`staged_loop.c` `stagedWanted`, `fastpath.c` `fpSessionEligible`, `dplus.c` speculation). For
writes:

- Lift the first two. Slot lanes need nothing new in the keyspace: the kvstore is already one
  hashtable per slot and every keyed command is single-slot.
- The IO thread already computes `c->slot` at parse (`clusterSlotByCommand`). It adds the
  stable-slot redirect check (served by this node, not migrating or importing, cluster ok, not
  ASKING); an entry that fails it is a barrier, and `processCommand` produces the redirect.
  Topology changes only in main-path handlers, cluster bus handlers and crons, all outside the
  window.
- Atomic slot migration: writes to an exporting slot reach the export job through the T-order
  replay (`clusterFeedSlotExportJobs` is one of `propagateNow`'s sinks). The cutover pause is a
  client pause, already a fallback (entries requeue).
- `CLUSTER SLOT-STATS`: cpu and network-in are written by the lane that owns the slot; repl
  network-out is written by main at replay, which must set the op's slot as the attribution.
- `getCachedKeySlot` reads `current_client`, which the execution context makes per lane.
- READ-phase GET speculation stays off in cluster mode until it has the same redirect check;
  GETs then execute in write-phase lanes.

## Complex types

The execution context and the classes above are what any single-key command needs; what differs
per command is its allowlist entry and its audit (audit B's per-command table). Rollout:

1. SET (all forms), GET. Done.
2. The string family and single-key key commands. Done.
3. Hashes (including field expiry: its HDEL/DEL propagation order is per-entry and captured in
   order), sets, sorted sets, lists, with per-key blocked-client and WATCH checks. Done (`298197a0d`).
   What the audit found in the type code: the hash listpack "ignore TTL" bracket kept its state in
   a file-scope flag ("safe because command execution is single threaded"), now thread-local; the
   expired-fields counter, now in the execution context; and SPOP/SRANDMEMBER/HRANDFIELD/
   ZRANDMEMBER draw from process-wide generators (`rand()`, mt19937 in `hashtableFairRandomEntry`),
   so they stay on main. Testing found one lane bug: per-lane commandstats were a fixed 64-entry
   table and asserted when one lane ran more than 64 distinct commands in a phase; they are now
   indexed by command id (`51d2ad4bb`, with a test that crashes the old build).
4. Streams after an audit of consumer-group state.

Never lane-eligible: commands that touch keys in more than one shard (in standalone), create or
copy module values (RESTORE, COPY), touch arbitrary keys (SCAN, RANDOMKEY, KEYS, DBSIZE, FLUSH*,
SWAPDB, MOVE), or run scripts. Later, independent multi-key commands (MGET, MSET, DEL, EXISTS,
UNLINK, TOUCH) can be split into per-shard pieces executed at the same T position on each lane;
the result is still serial T. Dependent ones (MSETNX, RENAME, SMOVE, LMOVE, *STORE) stay barriers.

## Replicas

### Writes with a replica attached, before the replica lanes (measured)

Primary metal-10, replica metal-16, generator metal-13 (all within 0.12 ms, us-east-1c placement
group); the replica runs the same build and config (so staged and lanes replicas use replica
parallel apply over 1024 shards); P16, 100k keys, 64-byte values, 3 reps per cell. A rep counts
the replica in sync when it applied at least 99% of the primary's writes in the window with no
growing lag. After every rep the writers stop, the replica catches up, and its `DEBUG DIGEST` is
compared with the primary's: all 120 SET reps and all 36 80/20 reps matched.

SET only (`r1-repl-set.jsonl`, `r1b-repl-set-v3v5.jsonl`), primary SET/s, best connection count:

| cores | stock | staged | lanes, main replays ops (`80edcafda`) | lanes, lanes format frames (`9ea2a705e`) |
|---:|---:|---:|---:|---:|
| 4 | 1.32M | 1.09M | | 2.90M in sync (2.98M with the replica falling behind) |
| 8 | 1.32M | 1.18M | 1.85M | 4.64M, replica behind: applies 3.17M |
| 16 | 1.31M | 1.24M | 2.00M | 6.58M, replica behind: applies 3.22M |

- A replica costs stock nearly half its throughput (2.4M to 1.31M): main formats and copies every
  write into the backlog.
- Replaying ops on main capped lanes at 1.85-2.0M (main 71-80%); with lanes formatting the frames
  main stays at 21-37% and the primary scales with cores.
- Past about 3.2M SET/s the replica falls behind, at 8 and 16 cores alike: it applies the single
  stream with one IO thread reading (at most 25 reads of 16 KB per iteration) and parsing, and
  main sorting the SETs into apply lanes. The primary is no longer the limit.
- The highest rate a replica kept up with (`r5-repl-boundary.jsonl`, fewer writers): 3.17M SET/s at
  8 cores (32 connections; 48 connections drive 3.44M and the replica falls behind at 3.16M) and
  3.04-3.32M at 16 cores. So with a replica the sustainable write rate is 2.4x stock's, bounded by
  the replica.
- A profile of the replica while it falls behind (16 cores, `profile-replica-16c.txt`): main is the
  busiest thread at 48% of a core, in parsing the rest of each read, `replApplyDefer` and object
  creation; the 16 IO threads together use 1.4 cores. Nothing is saturated: each iteration runs
  the read-and-parse on one IO thread, then main's pass over the commands, then the short APPLY
  phase, one after another.

80/20 GET/SET (`r2-repl-mixed.jsonl`), primary GET/s + SET/s; the replica applied every SET in sync:

| cores | stock | staged | lanes |
|---:|---:|---:|---:|
| 8 | 2.05M + 0.51M | 1.93M + 0.48M | 5.89M + 1.47M |
| 16 | 2.05M + 0.51M | 2.16M + 0.54M | 9.49M + 2.37M |

With 20% writes the replica keeps up (2.37M SET/s is below the replica's SET ceiling), and lanes serve
4.6x stock at 16 cores.

### The write lanes on the replica (`866b5ba64`)

A replica applies the primary's stream through the same lanes, with T = the stream order:

1. The read phase's IO thread reads at most 1 MB of the stream, parses the first command as usual, and
   scans the rest for command boundaries (bulk lengths only). It cuts the complete commands into one chunk
   per staged thread, about equal in bytes, and notes at each cut the database and transaction state the
   commands before it leave (SELECT, MULTI, EXEC, DISCARD). A read under 16 KB is one chunk, parsed there.
2. Main opens a PARSE phase: every staged thread parses its chunk into its slice of the primary client's
   command queue (the same objects and command preparation the IO-thread parser makes) and classifies each
   command as `fpClassify` does: a lane by the shard of its keys, or a barrier (keys in several shards, a
   key a replica client blocks on or watches, a command off the lane list, everything inside MULTI/EXEC,
   anything after a SELECT of a database not created yet).
3. Main runs T segment by segment: a segment's lane commands in an APPLY phase, each on its lane's
   executor flagged as the primary client (no reply, keys never expired, the stream's ACL-free user), and
   the barrier that ends it on main through the primary client, the usual way. After each segment main
   moves the replication offset past it and proxies its bytes to the backlog and the sub-replicas, so a
   write is acknowledged (REPLCONF ACK, WAIT) and forwarded only once everything before it is applied.
4. The commands stay in the client's queue throughout, so anything main does not run this way (the gate
   turns off after a barrier, the client stops) goes on the usual way from where the plan stopped.

The lanes are off on a replica in the same cases as on a primary, except that being a replica and
proxying to sub-replicas are fine; the replica's AOF turns them off (it is fed by execution, in order).
An error a lane gets from a stream command is logged, counted and, under propagation-error-behavior
panic, panics, as the primary client's error path does. Per planned read, INFO stats has
`repl_lane_reads`, `repl_lane_read_usec` (the IO thread's read job), `repl_lane_classify_usec` (scan,
and the parse of one-chunk reads), `repl_lane_parse_usec` (PARSE phases), `repl_lane_apply_usec`
(APPLY phases), `repl_lane_plan_usec` (main's whole plan run) and `repl_lane_barriers`.

Why the parse runs in parallel: with the classification alone (`66dc786fd`, one IO thread parsing each
1 MB read, about 9,700 SETs) the replica still applied only 3.4M SET/s. Per read the IO thread spent
2,326 us reading and parsing (240 ns per command, 126 us of it classifying) and the APPLY phase 466 us,
so the serial parse was 80% of each iteration. With the parallel parse a read of about 1,700 SETs takes
66 us to read and scan, 94 us to parse and 67 us to apply.

Tests: `tests/unit/staged-loop-lanes.tcl` has replica cases for every lane type applied on the replica's
lanes, a script's MULTI/EXEC from the primary, SELECT in the stream, a replica client's WATCH aborted by
a stream write, a replica client blocked on a stream served in order, and a sub-replica fed by a lane
replica (digests and lane counters checked).

### The write lanes on the replica (measured, 2026-10-16)

Every rep runs one build on both primary and replica. SET only, P16, 100k keys, 64-byte values; all reps
valid, all replica digests matched the primary's after catch-up.

How fast a replica applies the stream when nothing else limits it (`e1-catchup-c*.jsonl`, `pwcatchup.py`,
primary metal-16, replica metal-14, writer metal-15): the replica sleeps (DEBUG SLEEP) while 192 writers
run for 5 s, then drains the backlog; the rate is commands applied per second over 10-90% of the drained
bytes. Median of 3 reps; the spread across reps is under 4%.

| cores | stock | staged | lanes |
|---:|---:|---:|---:|
| 2 | 1.82M | 2.55M | 3.32M |
| 4 | 1.83M | 2.84M | 5.61M |
| 8 | 1.82M | 3.08M | 7.44M |
| 16 | 1.83M | 3.15M | 8.85M |

End to end at 16 cores (`d1-e2e-c16.jsonl`, primary metal-5, replica metal-7, writers metal-4, 81 reps):
every cell kept its replica in sync, so the write rate is each build's primary limit. "Visible" is from a
probe on the replica host: SET a counter on the primary 200 times a second, poll the replica over loopback
until it shows; "after the reply" subtracts the probe's own SET round trip (p50 minus p50).

| writers | stock SET/s | visible p50 / p99, after the reply | staged SET/s | visible, after the reply | lanes SET/s | visible, after the reply |
|---:|---:|---|---:|---|---:|---|
| 4 | 0.77M | 0.15 / 0.22 ms, 0.08 | 0.37M | 0.43 / 0.60 ms, 0.28 | 0.57M | 0.37 / 0.57 ms, 0.26 |
| 16 | 1.19M | 0.52 / 0.73 ms, 0.33 | 0.78M | 0.60 / 0.82 ms, 0.33 | 1.98M | 0.45 / 0.62 ms, 0.34 |
| 64 | 1.30M | 1.50 / 2.52 ms, 0.70 | 1.00M | 1.27 / 1.68 ms, 0.48 | 3.78M | 0.59 / 0.81 ms, 0.39 |
| 128 | 1.31M | 4.43 / 5.67 ms, 1.19 | 1.08M | 2.16 / 2.97 ms, 0.64 | 5.78M | 0.72 / 0.96 ms, 0.45 |
| 256 | 1.32M | 8.18 / 10.26 ms, 1.71 | 1.14M | 3.97 / 5.19 ms, 1.01 | 6.51M | 1.02 / 1.30 ms, 0.52 |

- With lanes on both sides the replica keeps up with everything the primary takes (6.56M SET/s at 192
  writers); it applies up to 8.85M/s, so the primary is now the limit.
- Stock and staged replicas keep up too, because their primaries stop at 1.32M and 1.14M; their replicas
  could apply 1.83M and 3.15M.
- Past its primary's limit, more writers only queue at the primary: visible latency grows with the SET
  round trip. The part after the reply grows to 1.7 ms (stock) and 1.0 ms (staged) at 256 writers and stays
  at or under 0.52 ms with lanes at 5.8 times staged's rate.
- At 4 writers stock shows the lowest latency (0.08 ms after the reply): at light load the staged loop adds
  about 0.2 ms between the primary's reply and the write showing on the replica, with or without lanes.
- With lanes, per planned read of about 1,700 SETs at 6.6M/s: the IO thread's read job 66 us, the PARSE
  phase 94 us, APPLY 67 us; the replica's main is 19% busy.

Suites with lanes forced on (no keyspace events, `io-threads-parallel-writes-min 1`, 1024 shards,
`runs/rl3`): replication, replication-2/3/4, replication-psync, psync2 (and -reg, -pingoff,
-master-restart), block-repl, failover, expire, multi, all type suites, scripting, functions, lazyfree,
cluster replica-redirect/slot-stats/multi-slot, staged-loop, staged-loop-stress and the lane suite pass.
What fails also fails without the replica lanes: replication-buffer's four dual-channel cases (same on
`4e4d24177`), info's rehashing count (1024 shards) and hashexpire's chain-replication TTL test (its third
server inherits the notifications-off config). A TSan build (gcc 10, `-O1`) under a mixed-type load with
replica lanes reports only the bio `makeThreadKillable` race that the same load reports with lanes off.

### Replica read capacity with the lanes (offbench, 2026-10-16)

The replica page's scenario (`offbench repl-calibrate` + `repl-run`): the primary carries the page's X
writes/s and Y reads/s, and replica readers step up from 0 to 800 or 1600 connections; the capacity is the
most replica reads/s while the replica stays in sync. Same build on primary and replica; lanes runs
`keyspace-shard-bits 10` on both, and staged-pr on the replica only, as the page's staged does. Hosts are in one
placement group: primary metal-16, replica metal-14, generators metal-20 and metal-15 (0.09-0.12 ms). Run at
the page's targets so the page can join it. Each build was calibrated to X and Y on its own, then run with the
two builds interleaved, 3 reps per step. Data is in offbench-pw `data/repl/pw-lanes-c{2,4-p8,8,16}.jsonl`.

| cores | X writes/s, Y reads/s | staged on the page (`cbf29c2cf`) | staged-pr `e43737058` | lanes `866b5ba64` |
|---:|---|---:|---:|---:|
| 2 | 387k, 1.31M | 3.63M | 3.61M | 3.56M |
| 4 | 783k, 984k | 6.78M | 6.76M | 6.92M |
| 8 | 861k, 538k | 13.27M | 13.21M | 14.76M |
| 16 | 1.02M, 294k | at least 19.93M | 19.76M | at least 22.14M |

- Every build stayed in sync at every step with replica readers, so its replica applied all X writes/s
  while serving these reads.
- staged-pr reproduces the page's staged within 1%, so the rig matches the page's.
- Against staged-pr, lanes gives -1% at 2 cores, +2% at 4, +12% at 8, and at least +12% at 16. At 16 cores
  lanes leads at every step: 6.22M vs 4.91M at 50 readers, 10.19M vs 8.36M at 100, 14.13M vs 11.70M at 200,
  18.49M vs 14.98M at 400.
- 16 cores, lanes: a lower bound. The one replica-reader host ran out of cores (GEN) at 800 and 1600
  readers. A second reader host (five hosts) would measure it, as the page's 16-core stock and rrd runs did.
- The calibration needs fewer connections for lanes than for staged-pr, because the lanes primary is faster.
  For example, at 8 cores lanes needs 12 writers at P8 and staged-pr needs 41. At 4 cores five lanes
  connections at P16 were too coarse a step to reach X within 5%, so that core count runs at the P8/P8 the
  calibration suggested, for both builds.
- Invalid reps, reported without numbers:
  - staged-pr, 8 cores, 0 readers, one rep: the replica applied 98.7% of the writes with no lag growth.
  - A few reps where the primary's reads were 7-10% off Y.
  - No step lost its majority of valid reps.
- Round trips: the generator round-trip check refused several starts. Right after a run, metal-15 to metal-14
  read 0.16-0.17 ms. After the 2-core run that path read 0.17-0.29 ms for 7 minutes and then 0.28 ms, so the
  4-core run put the replica readers on metal-20 (0.09 ms). Every run that started passed the 0.15 ms check.

## Fallbacks (first version)

Main runs the whole write phase serially (today's path) unless all hold: staged on and not
nested; nlanes >= 2; standalone with shard bits > 0, or cluster mode with no slot being exported;
no client pause; no monitor; tracking idle; no keyspace notifications; `moduleObservesWrites()`
false; no bgIteration (forkless save); no throttlers; maxmemory and maxmemory-clients unset (the
prototype; batch eviction can lift it); no clients blocked on keys; no watched keys; not loading;
not a replica; no disk-error or min-replicas write denial; hotkeys sampling off. Each is a frozen
flag checked once per iteration and again after every barrier entry. Per entry (on the IO
thread): a lane command, every key in one shard, ACL allowed (acl-offload's tag, or a user with
no restrictions), auth not required, user not retired; in cluster mode, a slot served here
stably (no slot migrating or importing), else the entry goes to main for its redirect.

Found while building it (base bugs that only show once the staged loop runs in cluster mode):
`fpHarvest` left `c->slot` set after taking the current command, so the next command's slot
preparation asserted; replica parallel apply skipped the command lookup that cluster slot stats
dereference, so it is standalone-only; a client with `CLIENT CAPA redirect` lost the capability on
the executor (wrong reply on replicas), so it stays on the main path. The test framework's default
`notify-keyspace-events KEA` keeps lanes off in every suite; run them with
`--config notify-keyspace-events '""'` and `--config io-threads-parallel-writes-min 1` to exercise
lanes. With both, 79 suites (all of `unit/cluster`, the type, expire, keyspace, staged and fastpath
suites, replication and AOF) pass except tests already failing on the base or on 1024 shards
(`runs/lanes3`, `runs/lanes5`, `runs/lanes6`); gtests 928/929, the one failure also on the base.

## Milestones and measurements

1. Execution context refactor alone: no behavior change; verify no throughput loss on stock
   paths (TLS pointer cost) and the full test suites. (Done, suites run; stock-path A/B not yet.)
2. SET/GET lanes, standalone, no propagation consumers: SET-only and GET-behind-SET throughput
   and latency at 2/4/8/16 cores against staged and stock, on the two-host rig, scaling client
   count. (Done, table above.)
3. Propagation capture and replay: same with a replica attached (stream byte-identity test:
   digest of replica vs primary, and an AOF diff against a serial run of the same T). (Digest
   tests pass; the replica benchmark needs a third host.)
4. Sharding cost: staged with 0 vs 10 bits, both shard functions. (Word hash: 3-5%.)
5. Cluster mode: one node with all slots, then three nodes. (One node: done.)
6. String family, then complex types, each with its tests. (String family done.)
7. Pre-formatted RESP for propagation; per-lane key-count deltas; per-key blocked/watched checks.

## Risks

- Hot keys: a lane runs at most as fast as its hottest shard. Same limit as any slot-sharded
  design; the lane split is per segment, so the cost is bounded by the segment.
- Barrier-heavy workloads (cross-shard MGET/MSET in standalone) cut segments short; the serial
  threshold keeps them at today's cost plus the classification.
- Missed shared state. Every lane-eligible command needs an audit of what it touches outside its
  shard; debug builds should assert (`inMainThread()` replacements, a lane-thread check in
  `propagateNow` and the other main-only sinks) so a miss trips tests instead of racing.
- The execution-context rename touches about 300 sites across the tree, including modules'
  view of `current_client`; reviewers will see it as one mechanical commit.
- One phase instant per write phase changes nothing a client can observe across commands of one
  iteration, but tests that assume per-command time snapshots inside one pipeline may need care.
