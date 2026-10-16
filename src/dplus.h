/* D+ -- GET execution on IO threads.
 *
 * GET commands execute entirely on the IO thread that parsed them: a direct
 * keyspace lookup and a reply written into the client's output buffer by the
 * same thread. The main thread never sees a successful speculative GET.
 * Misses, expired keys, large values and everything non-GET punt to the main
 * thread via the parsed-command queue.
 *
 * Invariant: IO threads read the keyspace only in the read phase, and main
 * mutates it only outside it, so a lookup never observes a concurrent write.
 *
 * REPLY ORDERING: the IO thread replies to a contiguous prefix of GET commands
 * and punts everything from the first ineligible command on, so the reply
 * buffer accumulates in command order:
 *   [spec_reply_0]...[spec_reply_k][main_reply_k+1]...
 */

#ifndef DPLUS_H
#define DPLUS_H

#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

struct client;

#define DPLUS_CACHELINE 64
#define DPLUS_MAX_IO_THREADS 256

/* Per-IO-thread counters: IO threads never write shared server stats; main
 * folds these into them once per event-loop iteration. */
typedef struct dplusThreadStats {
    long long commands_processed; /* speculated commands consumed on this thread */
    long long usec;               /* wall time spent executing them (for commandstats) */
    long long keyspace_hits;      /* speculative GET hits (bypass main's stat_keyspace_hits) */
} __attribute__((aligned(DPLUS_CACHELINE))) dplusThreadStats;

extern dplusThreadStats dplus_thread_stats[DPLUS_MAX_IO_THREADS];

/* --- Component 6: Stats (behind -DIO_LOOKUP_OFFLOAD_STATS) --- */

#ifdef IO_LOOKUP_OFFLOAD_STATS
typedef struct {
    _Atomic(uint64_t) speculative_attempts;
    _Atomic(uint64_t) speculative_hits;
    _Atomic(uint64_t) large_value_punts;
    _Atomic(uint64_t) expired_replies;
    _Atomic(uint64_t) miss_punts;
} __attribute__((aligned(DPLUS_CACHELINE))) dplusStats;

/* One slot per thread, each on its own cache lines. A counter is written only
 * by the thread that owns the slot, so an increment is an uncontended RMW on a
 * line no other core writes; INFO sums the slots. A single shared struct would
 * put every thread's increments on one line and turn the stats build into a
 * coherence benchmark instead of a measurement of the code under test. */
extern dplusStats dplus_stats[DPLUS_MAX_IO_THREADS];

#define DPLUS_STAT_ADD(field, n) \
    atomic_fetch_add_explicit(&dplus_stats[getCurTid()].field, (n), memory_order_relaxed)

/* Sum of one counter across all thread slots (relaxed; INFO-time read). */
uint64_t dplusStatsSum(size_t field_offset);
#define DPLUS_STAT_SUM(field) dplusStatsSum(offsetof(dplusStats, field))
#endif

/* ACL/AUTH gate for speculation (main-thread writers; workers read the
 * per-client spec_acl_ok byte inside dplusSpeculateBatch). */
void dplusRecomputeSpecAclOk(struct client *c);
void dplusOnAclRulesChanged(void);
/* F6: called when module command-result SUCCESS listener count transitions. */
void dplusOnCommandResultListenersChanged(int success_listeners);
void dplusOnMonitorsChanged(void);

/* IO thread: reply to a GET prefix of the parsed commands; returns how many.
 * tid = IO thread index (1..N-1). */
int dplusSpeculateBatch(struct client *c, int tid);

/* IO thread, after dplusSpeculateBatch: free the argv of the speculated
 * commands, advance the command queue and count them per thread. */
void dplusConsumeSpeculated(struct client *c, int count, int tid);

/* Aggregate per-IO-thread command counters into server.stat_numcommands.
 * Called once per event-loop iteration from beforeSleep. O(num_io_threads). */
void dplusAggregateStats(void);

/* Maximum embedded value size for speculative copy. Larger values punt.
 * The value-size sweep showed a smooth gradient with no cliff up to 1KB. */
#define DPLUS_MAX_SPECULATIVE_VALUE_LEN 1024

/* INFO dplus: speculated command count; detailed counters need
 * -DIO_LOOKUP_OFFLOAD_STATS. */
sds dplusInfoString(sds info);

#endif /* DPLUS_H */
