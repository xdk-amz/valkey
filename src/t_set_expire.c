/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Set member TTL commands: SEXPIRE SPEXPIRE SEXPIREAT SPEXPIREAT STTL SPTTL
 * SEXPIRETIME SPEXPIRETIME SPERSIST SADDEX. */

#include "server.h"
#include "expire.h"
#include "smember.h"

/* Locate the MEMBERS block of a S*EXPIRE style command, parse the optional
 * NX|XX|GT|LT flags that precede it and the member count that follows it.
 * On success returns the index of the first member and stores the count in
 * 'num_members'. On failure a reply was already emitted and -1 is returned. */
static int parseMembersBlockOrReply(client *c, int *flag, long long *num_members) {
    int members_index = 3;

    for (; members_index < c->argc - 1; members_index++) {
        if (!strcasecmp(objectGetVal(c->argv[members_index]), "members")) {
            /* checking optional flags */
            if (parseExtendedExpireArgumentsOrReply(c, flag, members_index++) != C_OK) return -1;
            if (getLongLongFromObjectOrReply(c, c->argv[members_index++], num_members, NULL) != C_OK) return -1;
            break;
        }
    }

    /* Check that the parsed members number matches the real provided number of members */
    if (!*num_members || *num_members != (c->argc - members_index)) {
        addReplyError(c, "nummembers should be greater than 0 and match the provided number of members");
        return -1;
    }
    return members_index;
}

/* High-Level Algorithm of SEXPIRE / SPEXPIRE / SEXPIREAT / SPEXPIREAT:
 *
 * - Parses the TTL, the optional NX|XX|GT|LT condition and the MEMBERS block.
 * - Applies the expiration to each member through setTypeSetExpiry() and
 *   replies with one integer per member (the SET_EXPIRY_* codes, which have
 *   the same numeric values as the HEXPIRE reply).
 * - A time in the past deletes the member (SET_EXPIRY_DELETED) and the whole
 *   command is rewritten to SREM naming only the members that were really
 *   deleted, so a replica, which does not consider the member expired, deletes
 *   them too and nothing else.
 * - Otherwise the command is rewritten to SPEXPIREAT with an absolute
 *   millisecond timestamp before propagation, naming only the members whose
 *   expiration was really written and dropping the NX|XX|GT|LT condition.
 * - Deletes the key when it was left empty.
 *
 * Keyspace Notifications (if enabled):
 * - "sexpired" - when members are immediately expired and deleted.
 * - "sexpire"  - when members receive new expiration timestamps.
 * - "del"      - when the set key becomes empty and is removed. */
static void sexpireGenericCommand(client *c, mstime_t basetime, int unit) {
    robj *key = c->argv[1], *param = c->argv[2];
    mstime_t when; /* unix time in milliseconds when the member will expire. */
    int flag = 0;
    long long num_members = 0;
    int i, expired = 0, updated = 0;
    robj **new_argv = NULL;
    int new_argc = 0;
    int *updated_members = NULL;

    int members_index = parseMembersBlockOrReply(c, &flag, &num_members);
    if (members_index < 0) return;

    if (convertExpireArgumentToUnixTime(c, param, basetime, unit, &when) == C_ERR)
        return;

    robj *obj = lookupKeyWrite(c->db, key);

    /* Non SET type return simple error */
    if (checkType(c, obj, OBJ_SET)) {
        return;
    }

    bool has_volatile_members = setTypeHasVolatileMembers(obj);

    initDeferredReplyBuffer(c);

    /* From this point we would return array reply */
    addReplyArrayLen(c, num_members);

    for (i = 0; i < num_members; i++) {
        int result = setTypeSetExpiry(obj, objectGetVal(c->argv[members_index + i]), when, flag);
        if (result == SET_EXPIRY_SET) {
            if (updated_members == NULL) updated_members = zmalloc(sizeof(int) * num_members);
            updated_members[updated++] = members_index + i;
        } else if (result == SET_EXPIRY_DELETED) {
            /* In case we are expiring members prepare a new argv since we are going to delete all of them. */
            if (new_argv == NULL) {
                new_argv = zmalloc(sizeof(robj *) * (num_members + 2));
                new_argv[new_argc++] = shared.srem;
                new_argv[new_argc++] = c->argv[1];
                incrRefCount(c->argv[1]);
            }
            /* In case we deleted the member, add it to the new srem command vector. */
            new_argv[new_argc++] = c->argv[members_index + i];
            incrRefCount(c->argv[members_index + i]);
            /* we treat this case exactly as active expiration. */
            server.stat_expiredsetmembers++;
            expired++;
        }
        addReplyLongLong(c, result);
    }

    if (expired || updated) {
        if (has_volatile_members != setTypeHasVolatileMembers(obj)) {
            dbUpdateObjectWithVolatileItemsTracking(c->db, obj);
        }
        if (expired) {
            replaceClientCommandVector(c, new_argc, new_argv);
            /* We would like to reduce the number of sexpired events in case there are potential many expired members. */
            notifyKeyspaceEvent(NOTIFY_SET, "sexpired", c->argv[1], c->db->id);
        } else if (updated) {
            /* A replica and an AOF load run under POLICY_IGNORE_EXPIRE, where an
             * expired member is still a live member and a condition evaluates
             * against a different view. Every rewrite in this file therefore
             * names only the members the primary really changed and drops the
             * conditions it already evaluated. When the original argv already
             * says exactly that, only the command name and the time change. */
            if (updated < num_members || flag != 0) {
                new_argc = 0;
                /* SPEXPIREAT + key + ms + MEMBERS + count + the members. */
                new_argv = zmalloc(sizeof(robj *) * (updated + 5));
                new_argv[new_argc++] = shared.spexpireat;
                new_argv[new_argc++] = c->argv[1];
                incrRefCount(c->argv[1]);
                new_argv[new_argc++] = createStringObjectFromLongLong(when);
                new_argv[new_argc++] = shared.members;
                new_argv[new_argc++] = createStringObjectFromLongLong(updated);
                for (i = 0; i < updated; i++) {
                    new_argv[new_argc++] = c->argv[updated_members[i]];
                    incrRefCount(c->argv[updated_members[i]]);
                }
                replaceClientCommandVector(c, new_argc, new_argv);
            } else {
                /* Only rewrite the command arg if not already SPEXPIREAT */
                if (c->cmd->proc != spexpireatCommand) {
                    rewriteClientCommandArgument(c, 0, shared.spexpireat);
                }

                /* Avoid creating a string object when it's the same as argv[2] parameter  */
                if (basetime != 0 || unit == UNIT_SECONDS) {
                    robj *when_obj = createStringObjectFromLongLong(when);
                    rewriteClientCommandArgument(c, 2, when_obj);
                    decrRefCount(when_obj);
                }
            }
            notifyKeyspaceEvent(NOTIFY_SET, "sexpire", c->argv[1], c->db->id);
        }
        server.dirty += (expired + updated); // in case there was a change increment the dirty
        signalModifiedKey(c, c->db, c->argv[1]);
        /* Delete the object in case it was left empty */
        if (setTypeSize(obj) == 0) {
            dbDelete(c->db, c->argv[1]);
            notifyKeyspaceEvent(NOTIFY_GENERIC, "del", c->argv[1], c->db->id);
        }
    }

    zfree(updated_members);
    commitDeferredReplyBuffer(c, 1);
}

void sexpireCommand(client *c) {
    sexpireGenericCommand(c, commandTimeSnapshot(), UNIT_SECONDS);
}

void sexpireatCommand(client *c) {
    sexpireGenericCommand(c, 0, UNIT_SECONDS);
}

void spexpireCommand(client *c) {
    sexpireGenericCommand(c, commandTimeSnapshot(), UNIT_MILLISECONDS);
}

void spexpireatCommand(client *c) {
    sexpireGenericCommand(c, 0, UNIT_MILLISECONDS);
}

/* High-Level Algorithm of SPERSIST Command:
 *
 * - Expects a key and a list of set members whose expiration metadata should be removed.
 * - Validates that the number of provided members matches the declared count.
 *
 * - For each specified member attempts to remove any existing expiration.
 * - Replies to the client with an array of integers, each representing the result for one member:
 *   - 1 if the expiration for the member was removed.
 *   - -1 if the member exists, but has no expiration time set.
 *   - -2 if the member does not exist, is expired but not yet removed, or the
 *     set is empty.
 *
 * An expired member is reported as missing and left alone, and is not named in
 * the propagated command.
 *
 * Keyspace Notifications (if enabled):
 * - "spersist" - emitted once if any member had its expiration removed. */
void spersistCommand(client *c) {
    int members_index = 4, result = 0, changes = 0;
    long long num_members = 0;

    if (strcasecmp(objectGetVal(c->argv[members_index - 2]), "members")) {
        addReplyErrorObject(c, shared.syntaxerr);
        return;
    }

    if (getLongLongFromObjectOrReply(c, c->argv[members_index - 1], &num_members, NULL) != C_OK) return;

    /* Check that the parsed members number matches the real provided number of members */
    if (!num_members || num_members != (c->argc - members_index)) {
        addReplyError(c, "nummembers should be greater than 0 and match the provided number of members");
        return;
    }

    robj *set = lookupKeyWrite(c->db, c->argv[1]);
    if (checkType(c, set, OBJ_SET))
        return;

    initDeferredReplyBuffer(c);

    /* From this point we would return array reply */
    addReplyArrayLen(c, num_members);

    bool has_volatile_members = setTypeHasVolatileMembers(set);
    /* argv indexes of the members that really lost a TTL. */
    int *persisted = NULL;

    for (int i = 0; i < num_members; i++, members_index++) {
        result = setTypeSetExpiry(set, objectGetVal(c->argv[members_index]), EXPIRY_NONE, 0);
        if (result == SET_EXPIRY_SET) {
            if (persisted == NULL) persisted = zmalloc(sizeof(int) * num_members);
            persisted[changes++] = members_index;
            server.dirty++;
        }
        addReplyLongLong(c, result);
    }
    if (changes) {
        if (has_volatile_members != setTypeHasVolatileMembers(set)) {
            dbUpdateObjectWithVolatileItemsTracking(c->db, set);
        }
        if (changes < num_members) {
            int new_argc = 0;
            /* SPERSIST + key + MEMBERS + count + the members. */
            robj **new_argv = zmalloc(sizeof(robj *) * (changes + 4));
            new_argv[new_argc++] = shared.spersist;
            new_argv[new_argc++] = c->argv[1];
            incrRefCount(c->argv[1]);
            new_argv[new_argc++] = shared.members;
            new_argv[new_argc++] = createStringObjectFromLongLong(changes);
            for (int i = 0; i < changes; i++) {
                new_argv[new_argc++] = c->argv[persisted[i]];
                incrRefCount(c->argv[persisted[i]]);
            }
            replaceClientCommandVector(c, new_argc, new_argv);
        }
        notifyKeyspaceEvent(NOTIFY_SET, "spersist", c->argv[1], c->db->id);
        signalModifiedKey(c, c->db, c->argv[1]);
    }
    zfree(persisted);

    commitDeferredReplyBuffer(c, 1);
}

/* High-Level Algorithm of STTL / SPTTL / SEXPIRETIME / SPEXPIRETIME Commands:
 *
 * - STTL / SPTTL return the relative TTL of each member (seconds or ms).
 * - SEXPIRETIME / SPEXPIRETIME return the absolute unix expiry time.
 *
 * For each member requested:
 *   - If the member or set does not exist, or the member is expired but not yet
 *     removed: reply with -2.
 *   - If the member exists but has no expiration: reply with -1.
 *   - Otherwise reply with the remaining TTL (clamped at 0) or the absolute
 *     expiry time, depending on the variant.
 *
 * These are READONLY commands: an expired member is reported as missing but is
 * never removed, so nothing is mutated or propagated.
 *
 * Keyspace Notifications:
 * - None emitted; this command is read-only. */
static void sttlGenericCommand(client *c, mstime_t basetime, int unit) {
    int members_index = 4;
    long long num_members = 0;
    mstime_t result = EXPIRY_NONE;

    if (strcasecmp(objectGetVal(c->argv[members_index - 2]), "members")) {
        addReplyErrorObject(c, shared.syntaxerr);
        return;
    }

    if (getLongLongFromObjectOrReply(c, c->argv[members_index - 1], &num_members, NULL) != C_OK) return;

    /* Check that the parsed members number matches the real provided number of members */
    if (!num_members || num_members != (c->argc - members_index)) {
        addReplyErrorObject(c, shared.syntaxerr);
        return;
    }

    robj *set = lookupKeyRead(c->db, c->argv[1]);

    if (checkType(c, set, OBJ_SET)) return;

    /* From this point we would return array reply */
    addReplyArrayLen(c, num_members);

    for (int i = 0; i < num_members; i++) {
        if (!set || setTypeGetExpiry(set, objectGetVal(c->argv[members_index + i]), &result) == C_ERR) {
            addReplyLongLong(c, -2);
        } else if (result == EXPIRY_NONE) {
            addReplyLongLong(c, -1);
        } else {
            result = result - basetime;
            if (result < 0) result = 0;
            addReplyLongLong(c, unit == UNIT_MILLISECONDS ? result : ((result + 500) / 1000));
        }
    }
}

void sttlCommand(client *c) {
    sttlGenericCommand(c, commandTimeSnapshot(), UNIT_SECONDS);
}

void spttlCommand(client *c) {
    sttlGenericCommand(c, commandTimeSnapshot(), UNIT_MILLISECONDS);
}

void sexpiretimeCommand(client *c) {
    sttlGenericCommand(c, 0, UNIT_SECONDS);
}

void spexpiretimeCommand(client *c) {
    sttlGenericCommand(c, 0, UNIT_MILLISECONDS);
}

/* Build the argv used to propagate SADDEX. The NX/XX/MNX/MXX conditions are
 * dropped (they were already evaluated on the primary) and a relative EX/PX
 * or a second resolution EXAT is rewritten to PXAT with the absolute
 * millisecond timestamp, as HSETEX does.
 *
 * Only the options region is scanned: argv[0] (the command), argv[1] (the key)
 * and everything from the MEMBERS token on are copied verbatim. Scanning from
 * argv[0] as hsetexCommand() does would strip a key or a MEMBERS token that
 * happens to be spelled like a condition, so that
 * "SADDEX nx EX 10 MEMBERS 1 a" would reach the replica as
 * "SADDEX PXAT <ms> MEMBERS 1 a" and write the wrong key. */
static int saddexBuildRewriteArgv(client *c, int members_index, int flags, robj *expire, mstime_t when, robj **new_argv) {
    int new_argc = 0;
    /* Index of the MEMBERS token: members_index points at the first member,
     * preceded by the member count and the MEMBERS token itself. */
    int options_end = members_index - 2;

    /* Command name and key, never a condition. */
    for (int i = 0; i < 2; i++) {
        new_argv[new_argc++] = c->argv[i];
        incrRefCount(c->argv[i]);
    }

    for (int i = 2; i < options_end; i++) {
        char *opt = objectGetVal(c->argv[i]);
        if (!strcasecmp(opt, "NX") || !strcasecmp(opt, "XX") || !strcasecmp(opt, "MNX") || !strcasecmp(opt, "MXX")) {
            continue;
        }
        /* Propagate as SADDEX key PXAT millisecond-timestamp ... if there is an
         * EX/PX/EXAT flag. */
        if (expire && !(flags & ARGS_PXAT) && c->argv[i + 1] == expire) {
            new_argv[new_argc++] = shared.pxat;
            new_argv[new_argc++] = createStringObjectFromLongLong(when);
            i++; /* skip the original expire argument */
        } else {
            new_argv[new_argc++] = c->argv[i];
            incrRefCount(c->argv[i]);
        }
    }

    /* MEMBERS, the member count and the members themselves, verbatim. */
    for (int i = options_end; i < c->argc; i++) {
        new_argv[new_argc++] = c->argv[i];
        incrRefCount(c->argv[i]);
    }
    return new_argc;
}

/* High-Level Algorithm of SADDEX Command:
 *
 * - Parses the key level NX|XX condition, the member level MNX|MXX condition,
 *   the optional expiration (EX|PX|EXAT|PXAT|KEEPTTL) and the MEMBERS block.
 * - MNX|MXX is all-or-nothing over the whole call, as HSETEX FNX|FXX is: every
 *   named member is tested before anything is written, and if a single one
 *   fails the command is a no-op that replies 0. It is not a per-member
 *   filter.
 * - Adds every member, giving it the requested expiration:
 *   - A member that does not exist yet is added with the expiration (or
 *     without one when KEEPTTL or no expiration option was given).
 *   - An existing live member keeps its TTL under KEEPTTL, otherwise its TTL
 *     is set to the new time, or removed when no expiration was given.
 *   - An existing expired member is replaced. The primary propagates SREM for
 *     it before this command, counts it as an active expiration and emits
 *     "sexpired", because a replica does not see the member as expired.
 * - An expiration already in the past adds nothing: the named members are
 *   removed if they are present (expired ones included), the command is
 *   propagated as SREM, and a missing key is not created. The reply is 0 even
 *   for a member that was deleted, since nothing was added.
 *
 * Client Reply:
 * - Integer reply: the number of members added to the set.
 *
 * Keyspace Notifications (if enabled):
 * - "sadd"     - when members were added.
 * - "sexpire"  - when an expiration was applied.
 * - "sexpired" - when expired members were replaced or removed.
 * - "del"      - when the set key becomes empty and is removed. */
void saddexCommand(client *c) {
    robj *o;
    robj *expire = NULL;
    int unit = UNIT_SECONDS;
    int flags = ARGS_NO_FLAGS;
    long long num_members = 0;
    mstime_t when = EXPIRY_NONE;
    int i, added = 0, changes = 0, num_expired = 0, deleted = 0;
    int set_expired = 0, need_rewrite_argv = 0;
    robj **new_argv = NULL;
    int new_argc = 0;
    robj **expired_members = NULL;

    /* SADDEX key [NX|XX] [MNX|MXX] [EX s|PX ms|EXAT ts|PXAT ts|KEEPTTL]
     *            MEMBERS nummembers member [member ...]
     *
     * The scan starts at the first option so that a key spelled like one is not
     * mistaken for the MEMBERS token. */
    int members_index = 2, have_members = 0;
    for (; members_index < c->argc - 1; members_index++) {
        if (!strcasecmp(objectGetVal(c->argv[members_index]), "members")) {
            if (parseExtendedCommandArgumentsOrReply(c, COMMAND_SADDEX, 2, members_index++, &flags, &unit, NULL,
                                                     &expire, NULL, NULL) != C_OK)
                return;
            if (getLongLongFromObjectOrReply(c, c->argv[members_index++], &num_members, NULL) != C_OK) return;
            have_members = 1;
            break;
        }
    }
    if (!have_members) {
        addReplyErrorObject(c, shared.syntaxerr);
        return;
    }

    /* Check that the parsed members number matches the real provided number of members */
    if (!num_members || num_members != (c->argc - members_index)) {
        addReplyError(c, "nummembers should be greater than 0 and match the provided number of members");
        return;
    }

    o = lookupKeyWrite(c->db, c->argv[1]);
    if (checkType(c, o, OBJ_SET))
        return;

    if (flags & (ARGS_SET_NX | ARGS_SET_XX | ARGS_SET_FNX | ARGS_SET_FXX | ARGS_EX | ARGS_PX | ARGS_EXAT)) {
        need_rewrite_argv = 1;
    }

    /* Check NX/XX key-level conditions before creating a new object */
    if (((flags & ARGS_SET_NX) && o != NULL) ||
        ((flags & ARGS_SET_XX) && o == NULL)) {
        addReply(c, shared.czero);
        return;
    }

    /* Handle parsing and calculating the expiration time. */
    if (expire) {
        mstime_t basetime = (flags & (ARGS_EXAT | ARGS_PXAT)) ? 0 : commandTimeSnapshot();

        if (convertExpireArgumentToUnixTime(c, expire, basetime, unit, &when) == C_ERR)
            return;

        if (checkAlreadyExpired(when)) {
            need_rewrite_argv = 1;
            set_expired = 1;
        }
    }

    /* Check MNX/MXX member-level conditions */
    if (flags & (ARGS_SET_FNX | ARGS_SET_FXX)) {
        if (o) {
            /* Key exists: check members normally */
            for (i = members_index; i < c->argc; i++) {
                if (((flags & ARGS_SET_FNX) && setTypeIsMember(o, objectGetVal(c->argv[i]))) ||
                    ((flags & ARGS_SET_FXX) && !setTypeIsMember(o, objectGetVal(c->argv[i])))) {
                    addReply(c, shared.czero);
                    return;
                }
            }
        } else if (flags & ARGS_SET_FXX) {
            /* Any MXX fails because no member exists */
            addReply(c, shared.czero);
            return;
        }
        /* MNX automatically passes if key doesn't exist, nothing to check */
    }

    if (o == NULL) {
        /* Nothing to add when the expiration is already in the past and there
         * is no member to delete either. */
        if (set_expired) {
            addReply(c, shared.czero);
            return;
        }
        o = setTypeCreate(objectGetVal(c->argv[members_index]), num_members);
        dbAdd(c->db, c->argv[1], &o);
    }

    bool has_volatile_members = setTypeHasVolatileMembers(o);

    if (set_expired) {
        /* All the named members are deleted, propagate SREM instead. */
        new_argv = zmalloc(sizeof(robj *) * (num_members + 2));
        new_argv[new_argc++] = shared.srem;
        new_argv[new_argc++] = c->argv[1];
        incrRefCount(c->argv[1]);

        /* One bracket around the whole loop: the members named here may be
         * expired, and setTypeRemove() cannot see those without it. The loop has
         * no early exit, so the bracket is closed exactly once below. */
        setTypeIgnoreTTL(o, true);
        for (i = members_index; i < c->argc; i++) {
            if (setTypeRemove(o, objectGetVal(c->argv[i]))) {
                new_argv[new_argc++] = c->argv[i];
                incrRefCount(c->argv[i]);
                /* we treat this case exactly as active expiration. */
                server.stat_expiredsetmembers++;
                deleted++;
            }
        }
        setTypeIgnoreTTL(o, false);

        if (deleted) {
            if (has_volatile_members != setTypeHasVolatileMembers(o)) {
                dbUpdateObjectWithVolatileItemsTracking(c->db, o);
            }
            replaceClientCommandVector(c, new_argc, new_argv);
            /* We would like to reduce the number of sexpired events in case there are potential many expired members. */
            notifyKeyspaceEvent(NOTIFY_SET, "sexpired", c->argv[1], c->db->id);
            signalModifiedKey(c, c->db, c->argv[1]);
            server.dirty += deleted;
        } else {
            /* Nothing changed: free the argv we prepared for the rewrite. */
            decrRefCount(c->argv[1]);
            zfree(new_argv);
        }
        if (setTypeSize(o) == 0) {
            dbDelete(c->db, c->argv[1]);
            notifyKeyspaceEvent(NOTIFY_GENERIC, "del", c->argv[1], c->db->id);
        }
        addReply(c, shared.czero);
        return;
    }

    for (i = members_index; i < c->argc; i++) {
        sds member = objectGetVal(c->argv[i]);
        bool replaced_expired = false;
        int added_now = setTypeAddWithExpiry(o, member, when, &replaced_expired);

        if (added_now) {
            added++;
            changes++;
        } else if (!(flags & ARGS_KEEPTTL)) {
            /* The member is already there and alive: move its TTL to the new
             * time, or drop it when no expiration option was given. */
            if (setTypeSetExpiry(o, member, when, 0) == SET_EXPIRY_SET) changes++;
        }

        if (replaced_expired) {
            /* Under KEEPTTL the propagated SADDEX keeps the member's expiration,
             * which on the replica is the old one, so the deletion has to be
             * propagated explicitly. Any other expiration option overwrites it
             * there anyway. */
            if (flags & ARGS_KEEPTTL) {
                if (expired_members == NULL) expired_members = zmalloc(sizeof(robj *) * num_members);
                expired_members[num_expired] = c->argv[i];
                incrRefCount(c->argv[i]);
            }
            num_expired++;
        }
    }

    if (changes) {
        if (has_volatile_members != setTypeHasVolatileMembers(o)) {
            dbUpdateObjectWithVolatileItemsTracking(c->db, o);
        }

        if (num_expired) {
            server.stat_expiredsetmembers += num_expired;
            notifyKeyspaceEvent(NOTIFY_SET, "sexpired", c->argv[1], c->db->id);
            /* propagateMembersDeletion() takes ownership of the member objects. */
            if (expired_members != NULL) {
                int idx = 0;
                while (idx < num_expired) {
                    idx += propagateMembersDeletion(c->db, o, num_expired - idx, &expired_members[idx], c->slot);
                }
                zfree(expired_members);
                expired_members = NULL;
            }
        }

        if (need_rewrite_argv) {
            /* cmd + key + options (PXAT takes 2 slots) + MEMBERS + n + members */
            new_argv = zmalloc(sizeof(robj *) * (c->argc + 1));
            new_argc = saddexBuildRewriteArgv(c, members_index, flags, expire, when, new_argv);
            replaceClientCommandVector(c, new_argc, new_argv);
        }

        signalModifiedKey(c, c->db, c->argv[1]);
        server.dirty += changes;

        if (added) notifyKeyspaceEvent(NOTIFY_SET, "sadd", c->argv[1], c->db->id);
        if (expire) notifyKeyspaceEvent(NOTIFY_SET, "sexpire", c->argv[1], c->db->id);
    }

    /* Delete the object in case it was left empty. */
    if (setTypeSize(o) == 0) {
        dbDelete(c->db, c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC, "del", c->argv[1], c->db->id);
    }
    /* make sure that if we ever allocated this it was freed */
    serverAssert(expired_members == NULL);
    addReplyLongLong(c, added);
}
