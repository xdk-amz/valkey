# SET member-TTL fuzzer

Black-box tools that drive a real `valkey-server` over a socket and check the
SET member-TTL invariants. They need no build flags (unlike the Tcl contract
suites, these do not read work counters), so any server with the member-TTL
feature works, including a plain release build.

All four scripts share `setfuzz.py`'s RESP client, disposable-server launcher
(`Server`) and helpers; the sibling scripts `import setfuzz`. Servers are
spawned under `$KIROCREW_SCRATCH/fuzz/` (falling back to `/tmp`).

## Scripts

- **`setfuzz.py`** — the model fuzzer. It keeps an in-process model of every
  member's category (persistent, live with a far-future deadline the model
  knows exactly, or hidden = expired but unreclaimed with active expiration
  disabled). Its operations grow the set as well as shrink it — `SADD`,
  `SADDEX PXAT`, `SPEXPIREAT` (plain, `NX`, `XX`), `SPERSIST`, `SREM`, `SPOP`,
  `SRANDMEMBER`, hiding members with a 1 ms TTL, and letting active expiration
  reclaim them — so a run crosses the intset/listpack/hashtable conversions,
  moves members between expiry-index buckets and lasts as long as asked. After
  every operation it checks that:
  - `SCARD` counts every physical member (live + hidden);
  - `SMEMBERS`/`SISMEMBER` expose only live members;
  - `SPEXPIRETIME` is -1 for a persistent member, the exact deadline for a live
    one, and -2 for a hidden or absent one;
  - `SPOP` removes only live members, the right count, all distinct;
  - `SRANDMEMBER` returns only live members (distinct for `+k`, repeats for
    `-k`) and never mutates the set;
  - `SADD`/`SADDEX` count a hidden member as added and replace it in place,
    `SADD` keeps an existing member's TTL and `SADDEX` sets it;
  - `SPERSIST`/`SPEXPIREAT` reply exactly per member and treat a hidden member
    as absent (-2) without reviving it;
  - `SREM` counts and removes only live members, leaving hidden ones in place;
  - active expiration reclaims every hidden member and nothing else;
  - an RDB round trip (`DEBUG RELOAD`) reclaims the hidden members while an AOF
    replay (`DEBUG LOADAOF`) keeps them, and both keep every live deadline.

  Modes: `model`, `persist`, `selftest`, `all` (default). `selftest` needs no
  server — it drives the invariant checkers against deliberately wrong replies
  and asserts each one fires (10/10), so a green run means the checkers can
  fail.

- **`dist2.py`** — uniformity. For the shapes that exercise each selection step
  (index answer, redraws, rank/selection walk), it runs many draws and applies
  a chi-square test per `(shape, command)`, failing below p = 1e-6. It also
  re-checks that only live members ever come back.

- **`repl2.py`** — replication + AOF. After every `SPOP` on a copy of each
  shape, the replica (which never reclaims) must hold exactly the primary's
  physical members, and a `DEBUG LOADAOF` must reproduce the live view and
  `SCARD`. It also asserts `SRANDMEMBER` writes nothing to the replication
  stream.

- **`bench.py`** — server-side latency (µs/call from `INFO commandstats`) of
  `SPOP`/`SRANDMEMBER` across the member-TTL shapes (#27 shared deadline, #41
  mostly-hidden, all-TTL-few-live, …), measuring several binaries in
  interleaved rounds so host load hits them alike.

## Running

```sh
# build any server with the feature (a plain build is fine)
make -j

BIN=$PWD/src/valkey-server
python3 utils/setperf/fuzz/setfuzz.py --mode selftest          # no server
python3 utils/setperf/fuzz/setfuzz.py --server "$BIN" --debug-assert
python3 utils/setperf/fuzz/dist2.py   --server "$BIN" --scale 0.1
python3 utils/setperf/fuzz/repl2.py   --server "$BIN"
python3 utils/setperf/fuzz/bench.py   --server head="$BIN" --n 1000000
```

`--seed N` fixes the fuzzer's own choices (the server's random picks still
vary); `--debug-assert` starts the server with `--enable-debug-assert yes` so
engine assertions are live during the fuzz. A default run is a quick check
(about 1,600 checks). For a campaign, give each process its own port and seed
range, for example 32 processes of 4 seeds and 2000 operations per starting
shape (about 64,000 checks each):

```sh
for i in $(seq 0 31); do
  python3 utils/setperf/fuzz/setfuzz.py --server "$BIN" --debug-assert \
      --port $((27700 + i)) --seed $((1000 * (i + 1))) --seeds 4 --ops 2000 &
done; wait
```
