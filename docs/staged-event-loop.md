# Staged main-thread-offload event loop

## Goal

Replace the free-running, fully concurrent fast path with one main-orchestrated loop
whose stages are separated by barriers. Reads run on IO threads while main is idle;
writes run on main while IO threads are idle; reply writes run on IO threads while main
is idle. Because reads and writes never overlap in time, the machinery that existed only
to let concurrent readers observe a concurrently mutating keyspace is no longer needed.

## Stages

One iteration runs five named stages in order, driven by `stagedLoopIteration()`:

1. Collect FDs. Main calls `pollerGetFDs()` once and stores the ready buffer. Client
   sockets (IO affinity) land in the IO ready buffer; listeners, cluster bus, replication
   links, and module/pipe fds (main affinity) land in the main ready list.
2. Read phase. Main opens the read barrier; every IO thread consumes IO-affinity ready
   fds from the shared buffer, reads and parses, executes the leading run of READ commands
   locally and writes their replies to the socket, and puts the first WRITE command plus
   every command pipelined behind it for that client into a `cmdBatch`. It continues across
   clients until the batch is full, then queues the batch for the write phase. When the
   buffer is drained and every IO thread is done, the threads signal and main proceeds.
3. Write phase. Main runs one readable function that walks the queued batches, executes
   each entry, and fills per-entry replies into the batch arena, exactly as
   `fastpathDrain()`/`fpExecute()` do today. Main also handles main-affinity ready fds and
   every non-fast-path client here (see Stage ownership).
4. Reply phase. Main opens the reply barrier; IO threads take the batches back and write
   the filled replies to their clients' sockets, as `fpDeliverBatch()` does today.
5. Crons. `beforeSleep`, timers, and background work run on main only.

A barrier closes the reply phase: every reply is on the wire before crons and before the
next read phase. Overlapping replies with crons is a later optimization and is not done.

## Poller interface (`src/poller.h`, `src/poller.c`)

The poller is a first-class fd registry, separate from `ae`. It is Linux epoll backed and
compiled only when `HAVE_FASTPATH_EPOLL` is defined (see `src/fastpath_no_epoll.h`). Non
epoll builds compile with the capability undefined and keep stock `ae`.

- `pollerRegister(p, fd, mask, handler, privdata, affinity)` records an fd, its interest
  mask, its handler and private data, and its affinity.
- `pollerModify(p, fd, mask)` changes the interest mask.
- `pollerUnregister(p, fd)` drops the fd.
- `pollerGetFDs(p, timeout_us)` blocks up to the timeout, then demultiplexes fired events by
  each fd's registered affinity into two ready sets and returns the total count.
- IO ready set: a threadsafe consumable buffer. Its filled `count` is written once by main
  in `pollerGetFDs`; IO threads consume entries by claiming indices with an atomic cursor
  (`pollerClaimIO`), so no two threads take the same entry and none needs a lock.
- Main ready set: a plain array main walks in the write phase (`pollerMainCount`,
  `pollerMainAt`); its handlers run on main.

Affinity:

- `POLLER_AFF_IO`: client sockets. Consumed by IO threads in the read phase.
- `POLLER_AFF_MAIN`: listeners, cluster bus, replication links, module and pipe fds. Their
  handlers run on main in the write phase.

The wait timeout is derived the way `aeProcessEvents` derives it: the nearest timer event
bounds the sleep, a zero timeout when work is already pending, and an unbounded wait only
when nothing is due.

## Stage ownership

- Poller registry and the ready buffers: written by main in the collect stage; the IO ready
  buffer is read by IO threads in the read phase through the atomic cursor only.
- A fast-path client's connection, socket, query buffer, parser state and output buffer:
  owned by its IO thread across the read and reply phases. Main never touches them; in the
  write phase main only reads a batch entry's borrowed argv and writes replies into the
  batch arena.
- The keyspace: mutated only by main, only in the write phase. Read by IO threads only in
  the read phase. The two never overlap.
- Non-fast-path clients (MULTI, blocked, pubsub, MONITOR, replicas, scripts, tracking, and
  any client whose pending command is not a plain read or write) are handled entirely on
  main in the write phase.
- Crons, timers and background jobs: main only, in the crons stage.

## Per-client command order

Within one client's read, the leading run of READ commands executes on the IO thread in the
read phase. The first WRITE command, and every command pipelined behind it (reads included),
goes into the batch and executes on main in the write phase. A read therefore never overtakes
an earlier write of the same connection, and a client is never split across a phase boundary
in a way that reorders its pipeline.

## Barrier primitive (`src/phase_barrier.h`, `src/phase_barrier.c`)

Stage transitions use one small reusable phase barrier. Main opens a phase for IO threads
`[0, n)` and waits until each reports done; each IO thread waits for a phase that includes its
index. Waiters spin briefly (bounded) and then sleep on a condition variable, so on a single
core main sleeps while the one IO thread works and the thread sleeps while main works.

- `phaseBarrierInit(b, n)` / `phaseBarrierDestroy(b)`.
- `phaseBarrierSetParticipants(b, n)` (main, between phases): the count applied at the next
  open. Threads with an index at or above it park until the count grows or shutdown, so the
  IO thread count can change between iterations.
- `phaseBarrierOpen(b, phase_id)` (main): opens a phase; refused while the previous phase has
  not completed or after shutdown. Everything main stored before the open is visible to the
  participants.
- `phaseBarrierWaitAllDone(b)` (main): returns once every participant called
  `phaseBarrierDone`; everything they stored before that is visible to main. A phase with zero
  participants completes at once.
- `phaseBarrierAwaitPhase(b, worker_id, &last_seen)` (IO thread): returns the id of the next
  phase that includes `worker_id`, or `PHASE_BARRIER_EXIT` after shutdown.
- `phaseBarrierDone(b)` (IO thread): reports the phase complete.
- `phaseBarrierShutdown(b)` (main, after the last phase completed): wakes every thread, parked
  or waiting, with `PHASE_BARRIER_EXIT`.

The sequence number, participant count and phase id of the open phase share one atomic word,
so a thread never pairs one phase's id with another phase's participant count. Phase ids and
participant counts are limited to 16 bits. The poller's IO ready set relies on this barrier
for publication: `pollerClaimIO` uses relaxed atomics because the read phase open orders main's
fill of the ready set before any claim.

## Removals and their replacing invariants

Each item below exists today only to coordinate concurrent IO-thread reads with concurrent
main writes. The staging barrier removes that concurrency, so each can go once its replacing
invariant holds. Nothing whose purpose is not read/write concurrency is removed.

- D+ speculation and the sharded seqlock (`dplusSpeculateBatch`, `dplusVersionValidate`,
  `dplusVersionBracket*`, `dplusVersionBump*`, the version array in `hashtable`).
  Replacing invariant: reads run only in the read phase and main mutates only in the write
  phase, so an IO-thread read never observes a concurrent mutation. Validation of a version
  that cannot change during the read is vacuous. IO-thread reads execute directly against the
  quiescent keyspace.
- Exclusive mode (`dplusExclusiveEnter`/`dplusExclusiveLeave` and its entry gate).
  Replacing invariant: exclusive mode forced readers to stand down while main ran an atomic
  section. With reads confined to the read phase, main's write phase is already exclusive of
  all reads; no separate gate is needed.
- Epoch/QSBR reclamation for readers (`dplusReaderWorkerOnline/Quiescent/Offline`,
  `dplusDeferFree`, `dplusReclaimRetired`, the limbo lists, reader epoch announcements).
  Replacing invariant: deferral existed so freed keyspace memory a speculating reader might
  still hold was not reclaimed until every reader passed a quiescent point. With no
  speculation, main frees in the write phase while no reader holds any keyspace pointer, so a
  free is immediately safe. Note: the general off-main object-free offload in `io_threads.c`
  is a separate mechanism and is not removed by this.
- Cross-thread submit/ret rings and their return handshake (`spscQueue submit`, `spscQueue
  ret`, `fpSubmit`, `fastpathProcessReturns`, `inflight` accounting).
  Replacing invariant: the rings decoupled two loops running at once. With batches produced
  in the read phase and consumed in the write phase behind a barrier, main knows exactly when
  production is complete and can take the batches directly; a queue handoff with no concurrent
  producer needs no ring discipline, only a per-thread batch list published at the barrier.
- Reply memory accounting on `ClientControl` (`reply_bytes_produced`/`reply_bytes_released`,
  `fpReplyChargeBatch`, `fpReplyReleaseBatch`, `fastpathReplyOutstanding`).
  Replacing invariant: the two-counter scheme let main and an IO thread agree on outstanding
  reply bytes without a lock while both ran. With the reply phase strictly after the write
  phase and before the next read, reply memory produced in one write phase is fully consumed
  in the immediately following reply phase; outstanding reply bytes at a phase boundary are
  known exactly without cross-thread counters.
- Generation-checked handles (`ClientControl.generation`, `fastpathHandleStale`, owner slots).
  Candidate only. Purpose to establish before removal: the generation check also guards
  against a slot being reused for a different connection between capture and use. If the
  batch lifetime is bounded to a single iteration and no client is freed between the read
  phase that captured the handle and the reply phase that resolves it, a direct pointer
  suffices. That freeing invariant must be established against `freeClient` before removing
  the generation check; until then it stays.
- Lifecycle request signalling (`CC_REQ_*`, `req_pending`, `fastpathControlRequest`,
  `fpDrainRequests`).
  Candidate only. Purpose to establish: these deliver close/handoff/evict intents to a
  free-running owner that main cannot otherwise reach. In the staged loop main runs between
  every read and reply phase, so it can apply lifecycle transitions at a barrier instead of
  signalling. The transition points must be re-established at the barriers before this is
  removed.

## Open risks

- Single core: barriers must sleep, never spin, so main and the one IO thread yield to each
  other. Covered by a unit test that pins main and one worker to one CPU; not yet measured
  under the real loop.
- TLS: TLS reads and writes are handled by the connection layer; a fast-path socket that
  negotiates TLS mid-connection must be excluded, as it is today.
- Blocking commands, MONITOR, pubsub, replicas: all remain main-only in the write phase.
- Module fds and pipes: main-affinity in the poller; their handlers must run on main.
