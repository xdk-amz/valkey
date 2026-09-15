/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */
#ifndef _SMEMBER_H_
#define _SMEMBER_H_

#include "fmacros.h"
#include "sds.h"
#include "util.h"
#include <stdbool.h>

/*-----------------------------------------------------------------------------
 * Set member ("smember")
 *-----------------------------------------------------------------------------
 *
 * A set member is an sds with an OPTIONAL expiration time. The smember pointer
 * IS the sds pointer, so every existing consumer that treats a hashtable set
 * entry as an sds (hash function, key compare, reply helpers) keeps working.
 *
 * Layout without TTL (unchanged from today, cost 0) - exactly what sdsnewlen()
 * produces, so the header type is whatever the length calls for (sdshdr5 for
 * the short member drawn here):
 *
 *     smember/sds
 *       |
 *       V
 *     +---------+------------+
 *     | sdshdr5 | "tag:7" \0 |
 *     +---------+------------+
 *
 * Layout with TTL (aux bit SMEMBER_SDS_AUX_BIT_HAS_EXPIRY set on sdshdr8+),
 * one allocation, the expiry immediately before the sds header:
 *
 *                   smember/sds
 *                     |
 *                     V
 *     +-----------+---------+------------+
 *     | mstime_t  | sdshdr8 | "tag:7" \0 |
 *     | expire    | aux=EXP |            |
 *     +-----------+---------+------------+
 *
 * An sdshdr5 has no aux bits, so a member with a TTL is always sdshdr8 or
 * larger. Removing the last TTL (smemberSetExpiry with EXPIRY_NONE) rebuilds
 * the member as a plain sds so that the zero-cost layout is restored.
 *
 * Ownership: a member with the prefix MUST be freed with smemberFree(), never
 * with sdsfree(). Both volatile set hashtable types in server.c destroy their
 * entries with smemberFree(); the plain setHashtableType keeps sdsfree, which
 * is only correct while the set holds zero prefixed members (see
 * t_set_volatile.c).
 */

typedef char smember; /* the pointer is an sds */

/* sds aux bit index used to mark "has expiry prefix". Same bit as entry.c. */
#define SMEMBER_SDS_AUX_BIT_HAS_EXPIRY 0

/* Create a member from a buffer. expiry == EXPIRY_NONE yields a plain sds. */
smember *smemberCreate(const char *str, size_t len, mstime_t expiry);

/* Expiration time in unix ms, or EXPIRY_NONE. O(1). */
mstime_t smemberGetExpiry(const smember *m);

/* True when the member carries the expiry prefix. O(1), aux bit test. */
bool smemberHasExpiry(const smember *m);

/* Set, change or remove (EXPIRY_NONE) the expiry. May reallocate; the
 * returned pointer replaces 'm'. Removing the expiry rebuilds a plain sds. */
smember *smemberSetExpiry(smember *m, mstime_t expiry);

/* True when the member has an expiry and it is in the past relative to
 * commandTimeSnapshot(). */
bool smemberIsExpired(const smember *m);

/* Free a member of either layout. */
void smemberFree(smember *m);

/* Total allocation size of the member (prefix + sds). */
size_t smemberMemUsage(const smember *m);

/* Defragment; returns the new pointer or NULL when not moved. */
smember *smemberDefrag(smember *m, void *(*defragfn)(void *));

#endif /* _SMEMBER_H_ */
