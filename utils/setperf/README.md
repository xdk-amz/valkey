# Set member-TTL performance-contract suite

Deterministic, counter-based regression tests for the volatile-set feature
(`SADDEX`, `SEXPIRE`..., commit "Add member-level expiration to the SET type").
The suite asserts *work* -- lookups, probes, iterator visits, listpack steps and
bytes moved, allocations, copies, propagation volume -- not time, so a green run
means the algorithms stayed request-sized where the parent was request-sized.

## Build and run

```
# instrumented server + gtests (production builds never see the counters)
make distclean
make -j CFLAGS= LDFLAGS= SERVER_CFLAGS="-Werror -DWORK_COUNTERS"
PKG_CONFIG_PATH=$HOME/.local/lib64/pkgconfig make CFLAGS= LDFLAGS= SERVER_CFLAGS="-Werror -DWORK_COUNTERS" valkey-unit-gtests

# the Tcl contract suites (each skips itself on a server without WORK_COUNTERS)
SETPERF_RESULTS=results.jsonl ./runtest --single unit/setperf-random --single unit/setperf-memory \
    --single unit/setperf-insert --single unit/setperf-store --single unit/setperf-lifecycle \
    --durable --verbose            # --only "<test name>" or --only "/regex" for a subset
SETPERF_EXTENDED=1 ...             # adds the 1,000,000-member hashtable size
SETPERF_SEED=<n> ...               # RNG seed replayed through DEBUG WORKCTR SEED

# exact-count edge cases on the set primitives
./src/unit/valkey-unit-gtests --gtest_filter='SetWorkCounterTest.*'

# supporting latency evidence (uninstrumented binaries preferred), same fixtures
utils/setperf/bench.py --server base=<parent>/src/valkey-server --server head=<head>/src/valkey-server \
    --n 200000 --reps 200 --out utils/setperf/results/bench.json
```

`make WORK_COUNTERS=yes` also works, but only when `SERVER_CFLAGS` is not given
on the command line (a command-line `SERVER_CFLAGS` overrides the Makefile's
`+= -DWORK_COUNTERS`). Without `WORK_COUNTERS=yes` the Makefile strips the flag
from the `SERVER_CFLAGS` persisted in `.make-settings`, so a later plain `make`
rebuilds without counters; a command-line `SERVER_CFLAGS=-DWORK_COUNTERS` is
kept only for that invocation. `CFLAGS=-Werror` must not be used: it leaks into
the jemalloc configure step. CMake builds have no option for the flag (make only).

## Instrumentation (src/workctr.h, src/workctr.c)

`WC_INC/WC_ADD` macros compile to nothing without `-DWORK_COUNTERS`; with it they
bump plain `int64_t` fields of two accounts:

* `wc` -- the measured object's own work;
* `wc_index` -- everything inside a `WC_INDEX_BEGIN()/WC_INDEX_END()` bracket,
  i.e. the expiry index (vset) internals, which are themselves hashtables and
  allocations. `DEBUG WORKCTR GET` reports the second account as `idx_<name>`.
  The Tcl helper `wc_get` folds the `vset_*` family across both accounts.

Counter families (descriptions are in the X-macro list in `workctr.h`):
hashtable lookups / bucket probes / key compares / hash calls+bytes / validate
calls+rejections / iterator visits+rejections+bucket steps / scan
visits+rejections / random-sampler calls+scans / rehash steps+entries; listpack
find calls+steps / next steps / random steps / inserts / deletes / batch deletes
/ tail bytes moved / reallocs+bytes; vset adds/removes/updates/expire
calls/bucket+entry visits; set/hash/list iterator entries, random calls, expired
samples, reservoir passes, listpack members skipped as expired, members
reclaimed; zmalloc requested bytes, calls, largest single allocation, live delta
and *peak* live delta (usable bytes, main thread, reply-buffer blocks classed
apart as `reply_*`, the lazily created per-command latency histogram ignored);
sds / string-object / smember copies and payload bytes; propagation commands,
args, payload bytes, retained payload bytes and their peak, dropped
propagations (no consumer) with their size, `propagateNow` calls, replication
bytes, AOF bytes, AOF-rewrite commands/bytes, RDB members saved/loaded.

Measurement window: `DEBUG WORKCTR ARM` resets the counters at the start of the
arming client's next top-level `call()` and snapshots them after
`afterCommand()`, so the command proc, its `alsoPropagate()` queue, the
propagation flush and the AOF/replication feed are inside; argument parsing,
reply transmission and the lazily created per-command latency histogram are
outside. `ARM` replies with the generation number the snapshot will carry and
`GET` reports the snapshot's generation, so `wc_measure` refuses a snapshot when
the armed command never reached `call()` (arity/type/ACL rejection would
otherwise silently measure the `GET` itself). `RESET`/`CURRENT` give coarser
multi-command windows (active expiration, replica-side sequences). Forked
AOF-rewrite and RDB children dump their own counters to
`workctr-aofrw-child.txt` / `workctr-rdb-child.txt` in the server directory.

Allocation classes: reply-buffer blocks are booked as `reply_*`, the histogram
allocation is ignored, everything else is `mem_*`; a free or realloc is booked
to the class current at that moment, so a reply block released while a command
runs (rare) shows up as an app free. Only main-thread allocations count; the
fixtures therefore disable all four `lazyfree-lazy-*` switches.

Known limits (report them, do not work around them):
`ht_hash_calls`/`ht_insert_positions` include incremental rehash work
(`ht_rehash_entries` tells how much); `ht_bucket_probes` includes the keyspace
lookup of the command's own key; `mem_peak_live_delta` is one number for the
whole window (selection arrays, created objects and retained propagation
together -- use `mem_max_alloc`, `str_objs_created` and `prop_peak_retained_bytes`
to attribute); the hashtable seed is not covered by `DEBUG WORKCTR SEED`, so how
many expired entries a validator steps over before the first live one varies
run to run (the violations below are deterministic, their magnitude is not);
`lp_tail_bytes_moved` does not separate the volatile-count header rewrite from
member deletion (whole-blob copies by `lpDup`/`lpMerge` are `lp_blob_bytes_copied`);
the hash type's own paths are not bracketed, so hash-side index work shows up in
the main account, and a set-side assertion on the index's *internal* traversal
must read the `idx_` counters explicitly (the active-expiration tests do).

## Fixtures and contracts (tests/support/workctr.tcl)

Fixtures hold identical live members across `none` / `one` (one far-future TTL
on `m0`) / `all` (every member far-future); `mostly_expired` and `all_expired`
expire members outside any measured command with active expiration disabled
(`wc_quiesce`); `one_expired` expires only `m0`, leaving a large live population
with a single hidden member that no selection path may reclaim. `wc_fixture_buckets`
holds `n` live members spread over a requested number of distinct expiry buckets
(deadlines a minute apart, so the index's 8192 ms bucket key separates every one),
isolating the number of deadline groups from the number of members. Hashtable sizes 2,000 / 20,000 / 200,000 (+1,000,000 extended),
listpack 16 / 64 / 128, short (`m<i>`) and long (64-byte) payloads, intset
conversions. Every measurement is one command through `wc_measure`, seeded.

Contracts are derived from the parent algorithm (the `none` fixture runs the
parent code paths unchanged), from source inspection and from the legitimate
TTL overhead, never from the measured commit: `wc_assert_ratio` (target <=
baseline*factor+slack at the same size), `wc_assert_flat` (fixed request across
geometric sizes), `wc_assert_le` (absolute bound justified in the test). A
failure prints command, fixture (encoding / physical / volatile / live), seed,
both counters, the violated contract and an exact reproduction line.

## Coverage

| Family | Baseline (parent path) | Counters gated | Contract | Tests |
|---|---|---|---|---|
| SPOP key, SPOP key k (small), SRANDMEMBER key / k / -k, hashtable | `hashtableFairRandomEntry` per pick (bounded scan), CASE 2 pops | `ht_iter_visits+ht_scan_visits+ht_bucket_probes`, `set_reservoir_passes`, `mem_max_alloc`, `str_objs_created` | plain <= 4096 examined; one/all <= 4x plain + 500; no reservoir pass; flat across sizes | setperf-random: "* hashtable, one future TTL adds no population scan" (x6), long payload |
| Same, with one expired (hidden, unreclaimed) member among n live | per-pick validation rejects the one expired pick (probability 1/n) and re-samples; nothing reclaims it | same counters plus `set_members_reclaimed`, physical/volatile after, `prop_args`, `prop_cmds`, `rdb_changes_since_last_save` | same budget as one future TTL; no reservoir pass; 10 commands <= 10 x (4x plain + 500), i.e. no per-command population pass; `m0` never returned; `set_members_reclaimed` = 0 and physical = n - popped on every command, so the hidden member outlives them all; SRANDMEMBER books no keyspace change and propagates nothing; SPOP propagates no more arguments than the no-TTL pop | setperf-random: "* hashtable, one expired member adds no population scan" (x6), "repeated * with one expired member" (x2), listpack (x3) |
| SRANDMEMBER k >= card., large -k | CASE 2 stream / per-result sampling | examined, `mem_max_alloc`, `mem_peak_live_delta` | one traversal, no population array | setperf-random: "count >= cardinality", "large negative count" |
| SPOP / SRANDMEMBER listpack, incl. -100 | `lpNextRandom` + `lpBatchDelete`, `lpRandomEntries` per <=1000 results | `lp_find_steps+lp_next_steps+lp_random_steps`, `str_objs_created`, `lp_deletes`, `lp_batch_deletes`, `lp_tail_bytes_moved`, `lp_find_calls` | <= 3x plain + 2n; one batch delete; tail bytes <= 2 x listpack bytes | setperf-random listpack (x5), setperf-memory "listpack SPOP removes ... one batch", "listpack SREM" |
| Dense hidden population (primary) | per-pick rejection, then at most one fallback pass for the members still owed | `set_reservoir_passes` and examined *per command*, `set_members_reclaimed`, physical after, reply membership | <= 1 full pass and <= 2n + request budget per command; only live members returned; `set_members_reclaimed` = 0 and physical = n - popped after N commands; an all-hidden set keeps its members, its `SCARD` and its key, and re-enabling active expiration is what removes them | setperf-random "* on a mostly-hidden hashtable is bounded probes plus at most one fallback pass" (x4), "an all-hidden hashtable terminates, survives, and is left to active expiration", "listpack SPOP leaves the hidden members in place" |
| Replica reads (`SRANDMEMBER` / `-k` / no count) | hidden, and no policy there reclaims | examined, `set_reservoir_passes`, `set_members_reclaimed`, physical after, `prop_cmds` | one-TTL and one-hidden-member reads <= 4x plain + 500 and flat across sizes, no reservoir pass, nothing reclaimed, nothing propagated; dense hidden population <= 1 fallback pass per read | setperf-random replica block: "replica SRANDMEMBER key 2 on a one-TTL set", "... on a mostly-expired set", "replica * with one hidden member stays request-sized" (x3) |
| Near-total SPOP, total pop | CASE 3 remembers `remaining`; CASE 1 | `mem_max_alloc` (flat across sizes), `str_objs_created`, `prop_peak_retained_bytes` (measured) | aux memory ~ remaining | setperf-memory T1/T2/T3, T9 (propagation equivalence, inherited O(count) retention) |
| Near-total SPOP `count` = n-5 and n-1 with one hidden member | the remainder is defined by the live members returned | `set_members_reclaimed`, physical/volatile after, key existence, `mem_max_alloc`, `str_objs_created`, `prop_args` | reply is `min(count, live)` live members and never `m0`; nothing reclaimed; the hidden member and the key survive even when `count` = live; allocation and copies <= 2x the no-TTL pop; propagated arguments <= the no-TTL pop + 8 | setperf-memory "near-total SPOP with one hidden member sizes memory by the remainder and reclaims nothing" |
| SRANDMEMBER k = n/2, k = 5 | CASE 3 aux table / CASE 4 | examined, memory, copies | ratio <= 2x / request-sized | setperf-memory T4/T5 |
| Repeated small SPOP, all-TTL (nothing expired) | CASE 2 | total examined over 20 commands | <= 20 x request budget | setperf-memory T8 |
| SADD / SADDEX (EX, PX, KEEPTTL, NX/XX/MNX/MXX) absent, existing, expired; SMOVE | `hashtableFindPositionForInsert` = 1 lookup + insert | `ht_lookups`, `ht_pops`, `vset_*`, `set_members_reclaimed`, listpack `lp_find_calls`/`lp_inserts`/`lp_reallocs` | <= 2 lookups per member (1 for existing); replacement = 1 probe + 1 pop + 1 index removal | setperf-insert (x13), gtest `SetWorkCounterTest.add*`, `replacingExpired*`, `listpackAddAbsent*` |
| SISMEMBER, SMISMEMBER, STTL/SPTTL/SEXPIRETIME/SPEXPIRETIME, SPERSIST, SCARD, SEXPIRE existing | one lookup per member | `ht_lookups`, `ht_iter_visits`, `ht_scan_visits`, flat across sizes | <= members + 2, no iteration | setperf-insert "reads request-sized", "SEXPIRE of existing", gtest `setAndGetExpiry*` |
| SUNIONSTORE / SINTERSTORE / SDIFFSTORE / SORT ... STORE (no consumer, replica, AOF) | verbatim command propagation | `prop_cmds_dropped`, `prop_dropped_arg*`, `str_objs_created`, `set_iter_next`/`ht_iter_visits`/`list_iter_next`, `repl_bytes`, `aof_bytes`, `prop_now_cmds`, `prop_peak_retained_bytes` | one-TTL == none (+ small const); flat across result sizes; expired sources: one copy of the result, replica/AOF contents equal | setperf-store (3 blocks, 29 tests) |
| SUNION / SINTER / SDIFF / SINTERCARD / SORT (no STORE) | same algorithms | examined, copies, `prop_cmds` | <= 2x + 16, no propagation | setperf-store "no dst" (x5) |
| MEMORY USAGE SAMPLES 1 / 5 / 0 | bounded sampling | `ht_iter_visits`, `vset_bucket_visits`, reply value | visits <= samples + 8; all_expired >= 0.8 x none | setperf-lifecycle E (x3 + hash probe) |
| MEMORY USAGE SAMPLES 1 / 5 over 8 / 64 / 512 / 4,096 distinct expiry buckets, every member live | documented `O(N) where N is the number of samples.` | `vset_bucket_visits`, `ht_iter_visits`, reply value | buckets visited <= samples + 8 and flat across bucket counts; member sampling still <= samples + 8; reply > 0. `SAMPLES 0` asks for an exact size, so its bucket walk is recorded, not gated | setperf-lifecycle "MEMORY USAGE SAMPLES k must not scale with the number of expiry buckets" |
| First TTL on intset; SPERSIST last TTL | conversion | `set_iter_next`, `mem_max_alloc`, `smember_created`, `lp_inserts`, MEMORY USAGE before/after | one pass; no retained storage | setperf-lifecycle F1/F2 |
| COPY, DEBUG RELOAD, BGSAVE child, volatile listpack load | one pass, RDB_TYPE_SET_2 | `smember_created`, `vset_adds`, `rdb_members_*`, `lp_find_calls` on load | linear, members+TTLs equal | setperf-lifecycle F3/F4 |
| AOF rewrite (`aof-use-rdb-preamble no`) | `rewriteHashObject` uses the volatile iterator | child `ht_iter_visits`, `vset_entry_visits`, `aof_rewrite_cmds/bytes` | <= 2 passes; volatile members via index; linear bytes | setperf-lifecycle G (x4) |
| Active expiration listpack / hashtable | `hashTypeDeleteExpiredFields` | `lp_tail_bytes_moved`, `set_members_reclaimed`, `vset_entry_visits`, `ht_iter_buckets` | parity with the hash path (inherited shape) | setperf-lifecycle F6 |
| SMEMBERS, SSCAN, SCARD | one pass / bounded scan / O(1) | examined, returned count | <= 2n, returned == live | setperf-lifecycle F7 |

## Findings on da37a1d (measured with this suite; numbers from the recorded runs)

Introduced by this commit:
* SPOP key k (any k < size, incl. 1 and 2) and SRANDMEMBER key k (k >= 2) on a
  hashtable set with a single future TTL run a full reservoir pass:
  2,002 entries examined at n=2,000 vs 72 for the parent path; 20,001 vs ~145
  at n=20,000; grows linearly with n (bench: 35 us -> 6,460 us median at
  n=200,000, x183). SPOP key and SRANDMEMBER key (no count) are unaffected.
* Near-total SPOP allocates a `count`-sized array of member copies
  (8 x count bytes: 159,960 at n=20,000) instead of remembering the
  `remaining` members (parent: 8,208 flat).
* SRANDMEMBER k >= cardinality builds a 24 x n selection array instead of
  streaming (480,000 bytes at n=20,000).
* Listpack SPOP k replaces one `lpBatchDelete` with k x (`lpFind` +
  `lpDeleteRangeWithEntry`): tail bytes moved 14,966 vs 332 at n=128, k=64.
* Listpack SRANDMEMBER -k walks the whole listpack once per result (1,600
  steps for -100 at n=16 vs 30).
* Repeated SPOP/SRANDMEMBER against a mostly- or all-expired hashtable rescan the
  same unreclaimed population every time (10 commands: ~200,000-272,000
  entries at n=20,000, physical count unchanged); an all-TTL set with nothing
  expired is rescanned too (20 x `SPOP key 3`: 399,577 vs 20 x 227).
* Insertion into a volatile set probes the member three times (validating
  probe, non-validating delete attempt, insert) -- 301 lookups for 100 absent
  members vs 101 -- for SADD, every SADDEX form (MNX/MXX add a fourth), SMOVE
  (5 lookups per move), and the listpack encoding (2 `lpFind` per member).
  Replacing an expired member costs 4 lookups + 2 pops where 1 probe + 1 pop +
  1 index removal are needed. Deterministic gtests: 3 vs <= 2, 2 vs <= 1.
* STORE commands and SORT ... STORE with a future TTL in any source rebuild the
  whole result as DEL + SADD/RPUSH effects: with no consumer 3,000 string copies
  and 4 dropped commands for a 3,000-member union (parent: 0); with a replica
  349,844 replication bytes and 33 commands vs 57 bytes / 1 command; AOF the
  same; peak retained propagation payload ~2 MB for a 300,000-member result.
  Replica and AOF contents were verified equal in every case, so the cost is
  amplification, not divergence. With an expired-but-unreclaimed source the
  effects are necessary (verified: the replica still holds all physical
  members); the design question is limiting them to that case.
* MEMORY USAGE on an expired-heavy set: the validating iterator steps over the
  expired population to find live samples (828 visits for SAMPLES 1, 20,000
  for SAMPLES 5 on 5-live/20,000) and with no live sample accounts 0 bytes for
  20,000 physically allocated members (812,280 vs 1,290,928). The hash type's
  estimator has the same shape (measured), so this is a new instance of an
  inherited pattern.
* AOF rewrite of a set with one volatile member iterates the whole hashtable
  twice (40,001 visits) where `rewriteHashObject` reaches the volatile fields
  through the index (20,001 + 1).

Inherited from the parent or the hash counterpart (measured, parity gated only):
* Every queued SREM/effect argv is retained until the end of the execution unit
  (near-total SPOP: `prop_peak_retained_bytes` = full popped payload in both
  parent and head; T9).
* `SPOP count >= size` copies the whole set into a temporary union set in both.
* Compact (listpack) active expiration moves the tail once per expired member
  (x64 the listpack size at n=128; hash listpack x61).
* Hashtable active expiration restarts its bucket walk per reclaim batch
  (`ht_iter_buckets` 19.5x for 10x n; hash identical).
* AOF rewrite emits one 7-argument `SADDEX key PXAT <exp> MEMBERS 1 <m>` per
  volatile member (95 B/member vs 11) -- same shape as HSETEX; equal deadlines
  are batchable (`MEMBERS <n>`): design question, linearity gated.
* SCARD reports the physical count including expired members (HLEN too).

Legitimate TTL / representation overhead (measured, bounded):
* one index operation per TTL change (SEXPIRE/SPERSIST/SADDEX EX on existing
  members), one probe per read member on every distribution, the intset ->
  listpack/hashtable conversion (one pass), COPY / RDB / reload (one pass, one
  smember per member, one index add per volatile member), replica sampling on
  one-TTL sets, the extra metadata entry of listpack SADDEX.

Earlier hypotheses not reproduced:
* SPOP key / SRANDMEMBER key without count on a hashtable (the retry loop over
  `hashtableFairRandomEntry` is bounded) -- request-sized on all fixtures.
* SRANDMEMBER k = n/2 (CASE 3 territory): the volatile path is cheaper than
  the parent's auxiliary hashtable.
* SPOP count >= size and every non-STORE set-algebra command are TTL
  independent.

Not yet verified:
* `hashtableFindBatch` bucket probing (SMISMEMBER on hashtables) is counted as
  lookups/compares only; per-key probe depth on that path is not observed.
* Memory moved by hashtable resize/rehash during large reclaims (only
  `ht_resizes`/`ht_rehash_steps`/`ht_rehash_entries` are counted).
* Cluster-mode propagation and slot migration paths are out of scope of the
  fixtures.

## Finding on f4475160e (the fix for the above; measured with the `one_expired` fixture)

The fix restores the parent samplers for sets whose members carry only future
TTLs, but selects the reservoir path with `setTypeHasExpiredMembers()`: as soon
as one member is expired and still hidden, every `SPOP key k` / `SRANDMEMBER
key k` / `SRANDMEMBER key -k` runs a full pass (200,000 `ht_iter_visits` and
`set_reservoir_passes=1` per command at n=200,000; bench median 32.8 us ->
7,110 us for `SPOP key 1`, x217). Expired members are hidden, not reclaimed on
the selection path, so the same pass repeats on every following command until
active expiration removes the member (1,999,985 entries examined over 10
`SPOP key 1`). The no-count forms are request-sized (per-pick rejection).
Removing the two gates in a probe build made all eight hashtable tests pass
with `m0` never returned, so the budget is met by the existing per-pick
validation; the gate itself is the regression. The `one` (future-TTL) fixture
cannot see this, which is why the suite previously passed on f4475160e.

## Findings on af090ba (measured with this suite on this head, seed 12345)

Every selection path reclaims. `setTypeReclaimExpiredMembers()` runs at the top
of `spopCommand`, `spopWithCountCommand`, `srandmemberCommand` and
`srandmemberWithCountCommand`, so a hidden member is deleted, unindexed and
propagated by the command that merely sampled near it:

* One hidden member of n live: `set_members_reclaimed` = 1 on every one of the
  six forms (`SPOP key`, `SPOP key 1`, `SPOP key 2`, `SRANDMEMBER key`,
  `SRANDMEMBER key 2`, `SRANDMEMBER key -5`), at n = 2,000 / 20,000 / 200,000
  and on both encodings; the physical count drops by one more than the command
  returned. Over 10 repeated `SPOP key 1` at n = 200,000 the member is reclaimed
  once and then gone, so the *examined* budget is met -- the cost contracts alone
  cannot see this, which is why the suite needed the survival gates.
* Dense hidden population (20,000 physical, 40 live): 19,960 members reclaimed
  over 10 commands, for `SRANDMEMBER key` and `SRANDMEMBER key 2` as well as for
  the two `SPOP` forms. A read deletes 19,960 members.
* All hidden (20,000): the first `SPOP key` reclaims all 20,000 and removes the
  key (`EXISTS` = 0 with `reclaimed so far: 20000`). Before the survival gate was
  added this showed up as a harness error (`DEBUG WORKCTR SETINFO` -> `ERR no
  such key`), not as a contract violation.
* Listpack (128 physical, 8 live): one `SPOP key 2` reclaims 120, and
  `lp_deletes` counts each of them.

A single hidden member still costs a full pass where nothing can reclaim it. On
a replica (`spr:one_expired`, physical 2,000 / volatile 1 / live 1,999)
`SRANDMEMBER key 2` and `SRANDMEMBER key -5` report
`set_reservoir_passes` = 1 at the smallest fixture size, and the reply is drawn
from a reservoir over all 2,000 entries; the no-count form is request-sized
(per-pick rejection). This is the steady state for staggered TTLs: no policy on
a replica ever removes the member.

`MEMORY USAGE key SAMPLES k` is O(k+V) in the number of deadline groups. With
20,000 live members and `SAMPLES 1`, member sampling is bounded
(`ht_iter_visits` = 1) but `vset_bucket_visits` = buckets + 1: 9 over 8 buckets,
65 over 64. `vsetBucketMemUsage_RAX` walks every bucket in the index, while the
command documents `O(N) where N is the number of samples.` `SAMPLES 0` asks for
an exact size, so its walk is the price of the request and is recorded only.

A near-total pop reclaims outside its request: `SPOP spm:one_expired 19995` on a
20,000-member set with one hidden member returns 19,995 live members and leaves
4 behind instead of 5, having reclaimed the hidden member as well.

Per-suite result on this head (all failures are contract violations, no harness
errors): setperf-random 17 ok / 19 failed, setperf-memory 9 / 1,
setperf-lifecycle 19 / 1, setperf-insert 16 / 1, setperf-store 29 / 0, gtest
`SetWorkCounterTest` 9 / 0. The one insert failure is the MXX replacement path:
`SADDEX key MXX EX 1000 MEMBERS 100 <existing>` costs 302 hashtable lookups
where 204 is the bound (one validating probe plus one update per member), the
fresh-lookup-per-mutation this head adopted to close the cached-pointer
use-after-free. The store suite (propagation volume, replica and AOF content
equality) is clean, and the `MEMORY USAGE` accounting floor for
expired-but-unreclaimed members -- a failure on da37a1d -- now passes.

## Recorded run against da37a1d (instrumented build, seed 12345)

| Suite | pass | fail (all contract violations, no harness errors) |
|---|---|---|
| setperf-random | 10 | 12 |
| setperf-memory | 4 | 5 |
| setperf-insert | 4 | 13 |
| setperf-store | 11 | 18 |
| setperf-lifecycle | 16 | 3 |
| gtest SetWorkCounterTest | 6 | 3 |

Existing suites on the instrumented build: unit/type/set, unit/setexpire,
unit/type/hash, unit/hashexpire, unit/type/list, unit/memefficiency, unit/aofrw:
1,125 ok, 1 failure ("Active Defrag big hash: cluster", a 6 ms vs 5 ms latency
limit while the host was running four builds in parallel; timing-only, not
counter related). Plain build (no flag, `-Werror` clean): unit/type/set,
unit/setexpire, unit/type/hash, unit/hashexpire 785 ok, 0 failures, and the
setperf suites skip themselves.

Latency (utils/setperf/results/bench-base-vs-head.json, n=200,000, medians,
plain builds of parent 66f9618 and head): `SPOP key 1` 31.5 us -> 31.5 us (no
TTL) -> 6,463 us (one TTL, x205); `SPOP key 2` 35.3 -> 6,460 us; `SRANDMEMBER
key 2` 35.2 -> 6,514 us; `SPOP key`, `SRANDMEMBER key`, `SRANDMEMBER key -5`,
`SISMEMBER`, `SADD`, `MEMORY USAGE`, `SCARD` within noise of the parent on every
fixture; listpack (n=128) within 1.0-1.5x.

## Results

`SETPERF_RESULTS` appends one JSON record per measured command
(`{"test","cmd","seed","fixture":{...},"counters":{...}}`); the recorded head run
is `utils/setperf/results/head-da37a1d.jsonl.gz`, the latency comparison
`utils/setperf/results/bench-base-vs-head.json` (parent 66f9618 vs head, both
plain builds). Mutation checks performed and reverted: a needless full
traversal added to the parent `setTypePopRandom` path failed "SPOP key
hashtable" on the no-TTL fixture (20,074 > 4,096); a redundant probe added to
`setTypeIsMemberAux` failed "membership and TTL reads are request-sized"
(3 > 2 lookups).
