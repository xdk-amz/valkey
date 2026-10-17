#ifndef EXPIRE_H
#define EXPIRE_H

/* Include feature-test macros early for unit tests that include expire.h
 * before server.h. */
#include "dict.h"
#include "fmacros.h"

#include <stdbool.h>
#include "util.h"

/* Special Expiry values */
#define EXPIRY_NONE -1

/* Flags for expireIfNeeded */
#define EXPIRE_FORCE_DELETE_EXPIRED 1
#define EXPIRE_AVOID_DELETE_EXPIRED 2

#define ACTIVE_EXPIRE_CYCLE_SLOW 0
#define ACTIVE_EXPIRE_CYCLE_FAST 1

/* Command flags for items expiration update conditions */
#define EXPIRE_NX (1 << 0)
#define EXPIRE_XX (1 << 1)
#define EXPIRE_GT (1 << 2)
#define EXPIRE_LT (1 << 3)

/* Return values for expireIfNeeded */
typedef enum {
    KEY_VALID = 0, /* Could be volatile and not yet expired, non-volatile, or even nonexistent key. */
    KEY_EXPIRED,   /* Logically expired but not yet deleted. */
    KEY_DELETED    /* The key was deleted now. */
} keyStatus;

/* Return value for getExpirationPolicy */
typedef enum {
    POLICY_IGNORE_EXPIRE, /* Ignore expiration time of items and treat them as valid. */
    POLICY_KEEP_EXPIRED,  /* Ignore items which are expired but do not actively delete them. */
    POLICY_DELETE_EXPIRED /* Delete expired keys on access. */
} expirationPolicy;

/* Types of active expiry jobs. Used to track and orchestrate
 * separate expiry mechanisms within the same database.
 *
 * KEYS:   Expiry of top-level keys via db->expires.
 * FIELDS: Expiry of hash fields stored in volatile sets (e.g., per-field TTLs).
 *
 * ACTIVE_EXPIRY_TYPE_COUNT: Number of expiry types, used for sizing arrays and iteration. */
enum activeExpiryType {
    KEYS,
    FIELDS,
    ACTIVE_EXPIRY_TYPE_COUNT
};

/* Forward declarations */
typedef struct client client;
typedef struct serverObject robj;
typedef struct serverDb serverDb;

/* return the relevant expiration policy based on the current server state and the provided flags.
 * FLAGS can indicate either:
 * EXPIRE_AVOID_DELETE_EXPIRED - which indicate the command is explicitly executed with the NO_EXPIRE flag.
 * EXPIRE_FORCE_DELETE_EXPIRED - which indicate to delete expired keys even in case of a replica (for the writable replicas case) */
expirationPolicy getExpirationPolicyWithFlags(int flags);
int parseExtendedExpireArgumentsOrReply(client *c, int *flags, int max_args);
int convertExpireArgumentToUnixTime(client *c, robj *arg, mstime_t basetime, int unit, mstime_t *unixtime);

/* Handling of expired keys and hash fields */
ustime_t activeExpireCycle(int type);
/* Main: active expire of keys runs in the MAINT phase now (lanes ready and nothing observes deletes). */
int activeExpireInLanes(void);
/* Main, before a MAINT phase: does an expired key need main's per-key replay (an observer or propagation
 * sees it), or can a lane free the value itself? Decided once; the state it reads cannot change in the
 * exclusive phase. */
int maintExpireReplayNeeded(void);
/* Main, before a MAINT phase that runs the EXPIRE job: set whether lanes free expired values inline
 * (no observer, no propagation) instead of logging them for replay. */
void activeExpireSetFreeInline(int free_inline);
/* Main, before a MAINT phase: would replay be needed ONLY because the delete propagates (AOF or
 * replica/backlog), with no other observer and no slot exporting? Then lanes record each key's DEL/UNLINK
 * for main to feed to the stream and free the value themselves. Call only when replay is already needed. */
int maintExpirePropagateOnly(void);
/* Main, before a MAINT phase that runs the EXPIRE job: set whether lanes record propagation and free the
 * value themselves instead of the full per-key replay. */
void activeExpireSetPropagateOnly(int propagate_only);
/* Main, before opening the phase: arm the shared replay-bounded budget (whole-phase budget_us and the
 * measured per-key replay cost; pass 0,0 to disable it and use the per-thread split ceiling instead). */
void activeExpireBudgetBegin(uint64_t budget_us, double replay_us);
/* Main, before a MAINT phase that runs the EXPIRE job: reset the per-thread results. */
void activeExpireLaneBegin(void);
/* Lane tid: expire keys of table didx of db within budget_us; sets *hit_limit when the budget ran out
 * with expired keys left. Returns the microseconds spent. */
uint64_t activeExpireTableInLane(int tid, serverDb *db, int didx, mstime_t now, uint64_t budget_us, int *hit_limit);
/* Lane tid: expire hash fields of table didx of db (keys_with_volatile_items) within budget_us; sets
 * *hit_limit when the budget ran out with more to do. Returns the microseconds spent. */
uint64_t activeExpireFieldsTableInLane(int tid, serverDb *db, int didx, mstime_t now, uint64_t budget_us, int *hit_limit);
/* Main, after the phase and its replay: merge the per-thread results into the server's stats. */
void activeExpireLaneEnd(uint64_t lanes_us, uint64_t phase_us, uint64_t expire_us);
void expireReplicaKeys(void);
void rememberReplicaKeyWithExpire(serverDb *db, robj *key);
void flushReplicaKeysWithExpireList(int async);
size_t getReplicaKeyWithExpireCount(void);
bool timestampIsExpired(mstime_t when);
void freeReplicaKeysWithExpireAsync(dict *replica_keys_with_expire);

#endif
