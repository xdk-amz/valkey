# Staged main-thread-offload event loop

## Goal

The fast path runs as one main-orchestrated loop whose stages are separated by a barrier.
Reads run on IO threads while main waits; writes run on main while IO threads wait; reply
writes run on IO threads while main waits. Reads and writes never overlap in time.

## When it runs

`stagedMain()` (`src/staged_loop.c`) replaces `aeMain()`. At every iteration boundary it
checks whether staged mode is wanted: `io-threads-fast-path` and `io-threads-strict-offload`
on, no cluster, and at least one running IO thread besides main. There is no other switch.
With `io-threads 1` there is no IO thread, so staged mode stays off (logged once) and the
loop is stock ae. Non-epoll builds always run stock ae.

Staged mode is the only fast-path mode: fast-path admission requires it. While it is wanted,
stock IO offload (read/write/accept/poll jobs, client partitioning, strict-offload deferral)
and the IO thread park policy are off, so every non-fast-path client is served by main
through ae. Free offload stays on: workers drain their job inboxes at the start of every
phase. On entry main waits (bounded) for in-flight stock jobs to land, then adopts clients
that were accepted meanwhile and have run nothing. On exit main hands every fast-path client
back to the main path.

## One iteration

`stagedLoopIteration()` calls, in order:

1. `collectFDs`: `beforeSleep`, one `pollerGetFDs()` wait bounded like `aeProcessEvents`
   (zero if `AE_DONT_WAIT`, else the nearest timer), `afterSleep`.
2. `readPhase`: opens `STAGED_PHASE_READ` when a client fd is ready or a job is queued.
   Each IO thread claims ready fds through the poller's atomic cursor, reads and parses,
   answers the leading run of GETs itself (`fpSpeculate`), and puts the first other
   command and everything pipelined behind it into its current `cmdBatch`, across clients
   until the batch is full. Full batches go on the thread's own batch list.
3. `writePhase`: main executes every batch (`fastpathStagedExecute`, `fpExecute` per entry,
   replies into the batch arena), then, if ae's fd was ready, runs
   `aeProcessEvents(AE_FILE_EVENTS | AE_DONT_WAIT)` for listeners, replication, cluster
   bus, module pipes and every non-fast-path client.
4. `replyPhase`: opens `STAGED_PHASE_REPLY` when batches exist. Each IO thread delivers its
   own batches in order (`fpDeliverBatch`) and changes poller interest for partial writes.
   After the barrier main hands back clients that left and unmaps clients it closed
   (`fastpathStagedFinish`).
5. `crons`: time events, main only. `beforeSleep` runs at the head of the next collect, as
   in ae, so a staged and a stock iteration can follow each other at any boundary.

Every reply is on the wire before crons. Overlapping replies with crons is a later
optimization.

## Poller and ae

Client sockets are the only IO-affinity fds (`POLLER_AFF_IO`). ae keeps every main fd; its
own epoll fd is registered once as the single `POLLER_AFF_MAIN` entry, and ae dispatches
its fds in the write phase, so fd reuse checks, `AE_BARRIER` and QoS priority handling are
unchanged. The poller's fd table grows with the highest registered fd and one wait reports
at most 1024 fds (level-triggered, the rest fire next time). `pollerRegister` is main only,
between phases; an IO thread may modify or unregister the fd it claimed during a phase.

## Workers

Main enrolls IO threads `1..n` (`n` = running threads - 1) as barrier participants between
phases. A newly enrolled thread waits from the barrier sequence main recorded at enrollment.
A thread leaves only through a `STAGED_PHASE_SYNC` phase, which main opens when the thread
count drops or staged mode ends; the thread then returns to the stock loop, where a retiring
thread drains as before. `drainIOThreadsQueue` opens `STAGED_PHASE_DRAIN`. Process exit
shuts the barrier down.

## Ownership

- Staged clients belong to the IO side as a whole, not to one thread. Their owner slots,
  registry and owned list live in `fp_threads[0]` and change only on main between phases.
- A client's fd appears once per collect, so within one iteration exactly one IO thread
  reads it, owns its entries, and writes its replies. A client's replies stay in order.
- A client's socket, query buffer, parser state and output buffer are touched only by that
  thread in the read and reply phases. Main reads a batch entry's borrowed argv and writes
  the entry's reply; it touches the client itself only at a handoff.
- The keyspace is read by IO threads only in the read phase and mutated by main only in the
  write phase and crons.

## Per-client command order

The leading GETs of what a client sent execute in the read phase. The first other command
and every command behind it, reads included, execute on main in the write phase. A read
never overtakes an earlier write of the same connection.

## Lifecycle

- A client that leaves (EOF, write error, query buffer limit, unsupported command, paused
  entries handed back) is unregistered from the poller by the IO thread that saw it and
  linked on that thread's leaving list. Main hands it back after the reply phase.
- `freeClient` on a staged client records the close on its control and in a main close list.
  `fpExecute` skips its entries; main unmaps it after the reply phase and the async free
  queue frees it.

## Blocked main

`processEventsWhileBlocked` runs one nested iteration at depth 1 (`stagedNestedIteration`):
read without executing reads on IO threads, write, reply. Clients whose outer-iteration
entries are still on main are skipped. Every nested command reaches `processCommand`, which
answers BUSY or LOADING. Leaves and closes seen at depth 1 are handled at the outer boundary.

## Machinery the barrier replaces

Staged mode no longer uses any of the following; they remain compiled for the concurrent
loop's unit tests until they are removed:

- Submit/return rings, `inflight` accounting, deferral FIFO, hold/drain timers and the worker
  quiesce roles: batches are published at the read barrier and returned at the reply barrier.
- Reply byte accounting: every reply produced in a write phase is delivered in the next reply
  phase.
- `CC_REQ_*` close records, `fp_detaching` and the detach pin: main applies closes between
  phases.
- Per-thread epoll and per-thread ownership of fast-path clients.

The D+ keyspace machinery (seqlock versions, exclusive mode, epoch reclamation) is replaced
by the same barrier and is removed separately.

## Open risks

- Single core: the barrier sleeps on a condition variable after a short spin.
- A module command that yields (`RM_Yield`) inside a batch runs the nested iteration.
- Client timeouts do not apply to staged clients (clientsCron skips them, as before).
