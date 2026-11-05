# String interning IO-thread ablations

Fourteen ablation builds from the investigation of a SET slowdown in intern-v2. The slow cell was SET, pipeline 1, 7000 connections, io-threads 16, on a Graviton c8g.metal-24xl. Each build switches off one piece of the IO-thread sharing path to locate the cost.

Each `abl-<name>.patch` is `git format-patch --stdout <base>..abl-<name>`, which keeps author, date and message. There are two bases:

| Base | Commit | Reachable from |
|---|---|---|
| intern-v2 IO head | `7ee30d675` "Select IO-parsed values at the main sampling rate" | `intern-v2` on this fork |
| stock | `3db525ec4` "Fix RDB and replication failures for certain listpack entry lengths (#4819)" | upstream `unstable`, and this branch's own parent |

To re-create a branch:

    git checkout -b abl-x <base> && git am patches/abl-x.patch

Every patch was checked this way: applied with `git am` onto its base, the resulting tree equals the original `abl-<name>` tip's tree (14/14).

Throughput is in thousands of commands per second, counted by the server, with 3-5 interleaved reps per build. During the runs stock measured 1344-1396 and head `7ee30d675` measured 1086-1273.

| Name | Base | Commit subject(s) | What it switches off | Measured |
|---|---|---|---|---|
| a | 7ee30d675 | ablate: IO threads do not share or select | all IO-thread sharing and selection | ~1362 (1351-1395), 7/7 fast |
| a1 | 7ee30d675 | ablate: IO threads look up the hot cache but never select | IO-thread selection; hot-cache lookups kept | ~1364 (1331-1365), all fast |
| a3 | 7ee30d675 | ablate: selected IO values never become singletons | singleton creation for IO selections | ~1264 (1202-1343), partial recovery |
| b | 7ee30d675 | ablate: pending releases do not set dont_sleep | pending releases keeping the event loop awake | ~1134 (1131-1341), no change |
| c | 7ee30d675 | ablate: no read-epoch bookkeeping (retired strings leak) | per-read epoch bookkeeping | ~1228 (1126-1345), no change |
| d1 | 7ee30d675 | ablate: IO threads select and hash at a fixed 1/64, then drop the selection | hand-off of selections to main | 1331-1371, fast |
| d2 | 7ee30d675 | ablate: main observes IO selections but admits none | admission on main (lookups and shared copies) | 1065-1300, slow |
| e1 | 7ee30d675 | ablate: IO selections (fixed 1/64) reach main, which discards them | any main-thread use of the selection | 1226-1357, mixed |
| e2 | 7ee30d675 | ablate: IO selections (fixed 1/64) reach main, which discards them; ablate: main only counts and observes IO selections (fixed 1/64) | everything on main except counting and rate control | 1298-1343, mixed |
| e3 | 7ee30d675 | ablate: IO threads fill a pending selection (fixed 1/64) but never hand it over | the hand-off itself (IO side still allocates and fills the struct) | 1112-1235 then 4/5 fast on rerun, mixed |
| f1 | 7ee30d675 | ablate: pending struct fills its own cache line | (pads the pending struct to 64 B to test false sharing) | 1069-1168, no help: not false sharing |
| g1 | 3db525ec4 (stock) | ablate (stock): one random cache miss per 64 collected reads | control: stock plus a little main-thread work | 1381-1396, unaffected |
| g2 | 3db525ec4 (stock) | ablate (stock): one random cache miss per 64 collected reads; ablate (stock): one random cache miss per 8 collected reads | control: stock plus more main-thread work | 1328-1352, gradual drop only |
| y1 | 7ee30d675 | ablate: IO threads look up the hot cache but never select; ablate: no IO selection; setKey samples IO-parsed values on main | IO selection; main samples IO-parsed values in setKey instead | 1336-1354, 4/4 fast: fix candidate |

Conclusion: the cost is in handing selections from IO threads to the main thread. It is not in picking or hashing on the IO threads, not in admission work, not in queued releases, and not in false sharing. y1 recovers stock throughput and keeps the sharing fix. It became the basis of the later intern-v2 change.

Source: session notes from 2026-10-16 (author's notes). Branch heads before this collapse: `abl-a 83c38a728`, `abl-a1 311000064`, `abl-a3 1c279f359`, `abl-b c75df0f64`, `abl-c ce0108bd4`, `abl-d1 1a9a1ac9a`, `abl-d2 82baa84b1`, `abl-e1 ce91dd3ce`, `abl-e2 b695dfb6a`, `abl-e3 134dcd2a0`, `abl-f1 e03b21c8f`, `abl-g1 53f5e4295`, `abl-g2 8a5d89c52`, `abl-y1 df9f884a3`.
