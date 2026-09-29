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
that were accepted meanwhile and have run nothing, plus clients the last exit handed back
that are still eligible. On exit main hands every fast-path client back to the main path and
marks it (`fp_readmit`) to rejoin on the next entry.

## One iteration

`stagedLoopIteration()` calls, in order:

1. `collectFDs`: `beforeSleep`, `pollerGetNextBatch()` (the batch the epoll thread gathered
   since the last call; it waits only when that batch is empty, bounded like `aeProcessEvents`:
   zero if `AE_DONT_WAIT` or jobs are queued, else the nearest timer), `afterSleep`.
2. `readPhase`: opens `STAGED_PHASE_READ` when a client fd is ready or a job is queued.
   Each IO thread claims ready fds through the poller's atomic cursor (PRIORITY, then IO),
   reads and parses,
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

### Epoll thread and batches

The poller owns one thread, `io_epoll`, started when staged mode starts and joined when it
stops (and at process exit). It blocks in `epoll_wait` on the poller's epoll set and appends
every fired fd to the poller's `next` buffer. Consumers read only the `active` buffer. No fd
is owned by any thread; the thread only reports.

Main never touches either buffer. At the head of each iteration it calls
`pollerGetNextBatch(p, timeout_us)`, which is safe at any time outside a read phase:

- it retires `active` (re-arming the MAIN fds it reported, see below, and emptying it),
- if `next` is empty and the timeout is not zero, it waits on a condition variable until the
  epoll thread (or a requeue) posts an fd or the timeout passes,
- it makes `next` the new `active` and hands the epoll thread the emptied buffer.

With work already queued it returns without a syscall (bar MAIN re-arms). All locking is inside
`poller.c`: one mutex guards `next`, the swap and the thread control; `active` is written only
by main in the swap and reaches IO threads through the read barrier. Without a running thread
(unit tests, or a failed start) `pollerGetNextBatch` waits on epoll itself.

`pollerResize` pauses the thread, resizes, and resumes it; queued entries are kept. Stop joins
the thread and empties both buffers, after every client fd was unregistered.

The thread is placed on the union of the enrolled IO threads' CPU masks, minus main's CPUs
when main is pinned to fewer than that union, at staged start and whenever the IO thread count
changes. It is created with that mask, so it never runs on main's core first. It may share
cores with IO threads.

### Edge-triggered IO fds

PRIORITY and IO fds are registered with `EPOLLET`: an fd is reported once per readiness edge,
so the epoll thread never re-reports an unread fd and never spins. The rules that follow:

- A handler that stops before `EAGAIN` hands the fd back with `pollerRequeue(p, fd, mask)`,
  which queues it in `next`. The staged read handler does one read per claim and requeues when
  that read filled its buffer (`readToQueryBuf` returned true); the rest of its loop consumes
  the query buffer completely or leaves the fast path. A nested iteration that skips a client
  with entries still on main requeues the edge it consumed.
- A partial write adds `POLLER_WRITABLE` interest (`EPOLL_CTL_MOD`); the kernel reports the
  edge when space frees. `EPOLL_CTL_ADD` and `EPOLL_CTL_MOD` both report current readiness,
  so attaching a client with bytes already queued, or dropping write interest, loses nothing.
- An fd may be queued in `next` while it is being served from `active`; a later claim that
  reads `EAGAIN` is harmless. Each buffer holds an fd at most once; repeat reports merge masks.

### Generations

Every register and unregister of an fd starts a new generation of its slot. The generation is
carried in the epoll tag and in every queued entry; a claim whose generation no longer matches
(the fd was dropped, or closed and reused, since it was queued) is skipped.

### Affinity classes

- `POLLER_AFF_IO`: client sockets. IO threads claim them in the read phase, any thread any fd.
- `POLLER_AFF_PRIORITY`: IO-thread fds claimed before every IO fd of the same batch. Nothing
  registers it yet. It is meant for fds whose service delays others: a replica's primary link
  if it moves to an IO thread, cluster bus links, and clients holding a large unsent reply.
- `POLLER_AFF_MAIN`: ae's own epoll fd, the only one. ae keeps every main fd (listeners,
  replication, cluster bus, module pipes, non-fast-path clients) and dispatches them in the
  write phase, so fd reuse checks, `AE_BARRIER` and QoS priority handling are unchanged. MAIN
  fds are level-triggered with `EPOLLONESHOT`, re-armed when main retires the batch that
  reported them. Edge-triggered would lose readiness: ae drains a bounded number of events per
  call, so its fd can stay readable with no new edge. Plain level-triggered would make the epoll
  thread spin on it until main runs ae. One-shot gives one report per main pass.

`pollerRegister` is main only, between phases or in the write phase; an IO thread may modify or
unregister the fd it claimed during a phase.

## Workers

Main enrolls IO threads `1..n` (`n` = running threads - 1) as barrier participants between
phases. A newly enrolled thread waits from the barrier sequence main recorded at enrollment.
A thread leaves only through a `STAGED_PHASE_SYNC` phase, which main opens when the thread
count drops or staged mode ends; the thread then returns to the stock loop, where a retiring
thread drains as before. `drainIOThreadsQueue` opens `STAGED_PHASE_DRAIN`. Process exit
shuts the barrier down.

## Ownership

- Staged clients belong to the IO side as a whole, not to one thread. Main keeps the list of
  them and changes it only between phases or in the write phase.
- A batch entry names its client by pointer. A staged client is freed only after main took it
  back following the reply phase of every iteration that produced entries for it.
- A client's fd appears at most once per batch, so within one iteration exactly one IO thread
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
- `freeClient` and `freeClientAsync` on a staged client record the close on its control and in a
  main close list. `fpExecute` skips its entries; main unmaps it after the reply phase and the
  async free queue frees it. Both run on main only.
- Unwritten replies past the normal output buffer limit, or a read past the query buffer limit,
  make the client leave; main applies the stock limit handling when it takes the client back.

## Blocked main

`processEventsWhileBlocked` runs one nested iteration at depth 1 (`stagedNestedIteration`):
take the next batch without waiting, read without executing reads on IO threads, write, reply.
Clients whose outer-iteration entries are still on main are skipped and requeued. Every nested command reaches `processCommand`, which
answers BUSY or LOADING. Leaves and closes seen at depth 1 are handled at the outer boundary.

## Removals

The barrier replaces the machinery two free-running loops needed, and none of it exists:

- Submit/return rings, in-flight accounting, the deferral FIFO, worker quiesce roles and the
  `io-batch-inflight`, `io-batch-hold-us` and `io-batch-drain-us` configs: batches are
  published at the read barrier and returned at the reply barrier.
- Reply byte accounting: every reply produced in a write phase is delivered in the next reply
  phase.
- `CC_REQ_*` lifecycle requests, detach ring records and control pins: main records a close on
  the control and applies it after the reply phase.
- Generation-checked handles and owner slots: an entry's client outlives the iteration.
- Per-thread epoll for fast-path clients: the staged poller serves them. The per-thread epoll
  stays for partitioned clients of the stock IO-thread loop.
- The D+ seqlock versions, exclusive mode, epoch reclamation and reader lifecycle hooks.

## Open risks

- Single core: the barrier sleeps on a condition variable after a short spin; the epoll thread
  blocks in the kernel.
- A signal no longer interrupts main's wait for a batch; it is handled by the next cron, at
  most one timer period later.
- A module command that yields (`RM_Yield`) inside a batch runs the nested iteration.
- Client timeouts apply to staged clients: clientsCron runs between phases and closes idle ones.
