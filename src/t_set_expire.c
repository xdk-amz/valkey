/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "server.h"
#include "expire.h"

static const char *nummembers_err = "nummembers should be greater than 0 and match the provided number of members";

/* Rewrites the command as 'cmd key [when] MEMBERS n members', taking the members from
 * the argv positions in 'idx'. */
static void rewriteCommandToMembers(client *c, robj *cmd, robj *when, int *idx, int n) {
    robj **new_argv = zmalloc(sizeof(robj *) * (n + (when ? 5 : 4)));
    int new_argc = 0;

    new_argv[new_argc++] = cmd;
    new_argv[new_argc++] = c->argv[1];
    incrRefCount(c->argv[1]);
    if (when) new_argv[new_argc++] = when;
    new_argv[new_argc++] = shared.members;
    new_argv[new_argc++] = createStringObjectFromLongLong(n);
    for (int i = 0; i < n; i++) {
        new_argv[new_argc++] = c->argv[idx[i]];
        incrRefCount(c->argv[idx[i]]);
    }
    replaceClientCommandVector(c, new_argc, new_argv);
}

static void sexpireGenericCommand(client *c, mstime_t basetime, int unit) {
    robj *key = c->argv[1], *param = c->argv[2];
    mstime_t when;
    int flag = 0, members_index = 3;
    long long num_members = 0;
    int i, expired = 0, updated = 0;
    robj **new_argv = NULL;
    int new_argc = 0;

    for (; members_index < c->argc - 1; members_index++) {
        if (!strcasecmp(objectGetVal(c->argv[members_index]), "members")) {
            if (parseExtendedExpireArgumentsOrReply(c, &flag, members_index++) != C_OK) return;
            if (getLongLongFromObjectOrReply(c, c->argv[members_index++], &num_members, NULL) != C_OK) return;
            break;
        }
    }

    if (!num_members || num_members != (c->argc - members_index)) {
        addReplyError(c, nummembers_err);
        return;
    }

    if (convertExpireArgumentToUnixTime(c, param, basetime, unit, &when) == C_ERR)
        return;

    robj *obj = lookupKeyWrite(c->db, key);

    if (checkType(c, obj, OBJ_SET)) {
        return;
    }

    bool has_volatile_members = setTypeHasVolatileMembers(obj);
    int *updated_members = NULL;

    initDeferredReplyBuffer(c);

    addReplyArrayLen(c, num_members);

    for (i = 0; i < num_members; i++) {
        expiryModificationResult result = setTypeSetExpiry(obj, objectGetVal(c->argv[members_index + i]), when, flag);
        if (result == EXPIRATION_MODIFICATION_SUCCESSFUL) {
            if (has_volatile_members && updated_members == NULL) updated_members = zmalloc(sizeof(int) * num_members);
            if (updated_members) updated_members[updated] = members_index + i;
            updated++;
        } else if (result == EXPIRATION_MODIFICATION_EXPIRE_ASAP) {
            if (new_argv == NULL) {
                new_argv = zmalloc(sizeof(robj *) * (num_members + 2));
                new_argv[new_argc++] = shared.srem;
                new_argv[new_argc++] = c->argv[1];
                incrRefCount(c->argv[1]);
            }
            new_argv[new_argc++] = c->argv[members_index + i];
            incrRefCount(c->argv[members_index + i]);
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
            notifyKeyspaceEvent(NOTIFY_SET, "sexpired", c->argv[1], c->db->id);
        } else if (updated) {
            /* A TTL-free set hides no expired member, so every result here reproduces on the
             * replica and the command propagates verbatim. */
            if (has_volatile_members && updated < num_members) {
                rewriteCommandToMembers(c, shared.spexpireat, createStringObjectFromLongLong(when), updated_members,
                                        updated);
            } else {
                if (c->cmd->proc != spexpireatCommand) {
                    rewriteClientCommandArgument(c, 0, shared.spexpireat);
                }

                /* Reuse argv[2] when it already contains the absolute millisecond time. */
                if (basetime != 0 || unit == UNIT_SECONDS) {
                    robj *when_obj = createStringObjectFromLongLong(when);
                    rewriteClientCommandArgument(c, 2, when_obj);
                    decrRefCount(when_obj);
                }
            }
            notifyKeyspaceEvent(NOTIFY_SET, "sexpire", c->argv[1], c->db->id);
        }
        server.dirty += expired + updated;
        signalModifiedKey(c, c->db, c->argv[1]);
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

void spersistCommand(client *c) {
    int members_index = 4, changes = 0;
    long long num_members = 0;

    if (strcasecmp(objectGetVal(c->argv[members_index - 2]), "members")) {
        addReplyErrorObject(c, shared.syntaxerr);
        return;
    }

    if (getLongLongFromObjectOrReply(c, c->argv[members_index - 1], &num_members, NULL) != C_OK) return;

    if (!num_members || num_members != (c->argc - members_index)) {
        addReplyError(c, nummembers_err);
        return;
    }

    robj *set = lookupKeyWrite(c->db, c->argv[1]);
    if (checkType(c, set, OBJ_SET))
        return;

    initDeferredReplyBuffer(c);

    addReplyArrayLen(c, num_members);

    bool has_volatile_members = setTypeHasVolatileMembers(set);
    int *persisted = NULL;

    for (int i = 0; i < num_members; i++, members_index++) {
        expiryModificationResult result = setTypeSetExpiry(set, objectGetVal(c->argv[members_index]), EXPIRY_NONE, 0);
        if (result == EXPIRATION_MODIFICATION_SUCCESSFUL) {
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
            rewriteCommandToMembers(c, shared.spersist, NULL, persisted, changes);
        }
        notifyKeyspaceEvent(NOTIFY_SET, "spersist", c->argv[1], c->db->id);
        signalModifiedKey(c, c->db, c->argv[1]);
    }
    zfree(persisted);

    commitDeferredReplyBuffer(c, 1);
}

static void sttlGenericCommand(client *c, mstime_t basetime, int unit) {
    int members_index = 4;
    long long num_members = 0;

    if (strcasecmp(objectGetVal(c->argv[members_index - 2]), "members")) {
        addReplyErrorObject(c, shared.syntaxerr);
        return;
    }

    if (getLongLongFromObjectOrReply(c, c->argv[members_index - 1], &num_members, NULL) != C_OK) return;

    if (!num_members || num_members != (c->argc - members_index)) {
        addReplyError(c, nummembers_err);
        return;
    }

    robj *set = lookupKeyRead(c->db, c->argv[1]);

    if (checkType(c, set, OBJ_SET)) return;

    addReplyArrayLen(c, num_members);

    for (int i = 0; i < num_members; i++) {
        mstime_t result;
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

/* Scan only the options: the key and members may equal option names. */
static void rewriteSaddexCommand(client *c, int members_index, int flags, robj *expire, mstime_t when) {
    robj **new_argv = zmalloc(sizeof(robj *) * c->argc);
    int new_argc = 0;
    int options_end = members_index - 2;

    for (int i = 0; i < 2; i++) {
        new_argv[new_argc++] = c->argv[i];
        incrRefCount(c->argv[i]);
    }

    for (int i = 2; i < options_end; i++) {
        char *opt = objectGetVal(c->argv[i]);
        if (!strcasecmp(opt, "NX") || !strcasecmp(opt, "XX") || !strcasecmp(opt, "MNX") || !strcasecmp(opt, "MXX")) {
            continue;
        }
        if (expire && !(flags & ARGS_PXAT) && c->argv[i + 1] == expire) {
            new_argv[new_argc++] = shared.pxat;
            new_argv[new_argc++] = createStringObjectFromLongLong(when);
            i++;
        } else {
            new_argv[new_argc++] = c->argv[i];
            incrRefCount(c->argv[i]);
        }
    }

    for (int i = options_end; i < c->argc; i++) {
        new_argv[new_argc++] = c->argv[i];
        incrRefCount(c->argv[i]);
    }
    replaceClientCommandVector(c, new_argc, new_argv);
}

void saddexCommand(client *c) {
    robj *o;
    robj *expire = NULL;
    int unit = UNIT_SECONDS;
    int flags = ARGS_NO_FLAGS;
    long long num_members = 0;
    mstime_t when = EXPIRY_NONE;
    int i, added = 0, changes = 0, num_expired = 0;
    int set_expired = 0;
    robj **new_argv = NULL;
    int new_argc = 0;
    robj **expired_members = NULL;
    smember **mxx_cached = NULL;

    int members_index = 2;
    for (; members_index < c->argc - 1; members_index++) {
        if (!strcasecmp(objectGetVal(c->argv[members_index]), "members")) {
            if (parseExtendedCommandArgumentsOrReply(c, COMMAND_SADDEX, 2, members_index++, &flags, &unit, NULL,
                                                     &expire, NULL, NULL) != C_OK)
                return;
            if (getLongLongFromObjectOrReply(c, c->argv[members_index++], &num_members, NULL) != C_OK) return;
            break;
        }
    }

    if (!num_members || num_members != (c->argc - members_index)) {
        addReplyError(c, nummembers_err);
        return;
    }

    o = lookupKeyWrite(c->db, c->argv[1]);
    if (checkType(c, o, OBJ_SET))
        return;

    if (((flags & ARGS_SET_NX) && o != NULL) ||
        ((flags & ARGS_SET_XX) && o == NULL)) {
        addReply(c, shared.czero);
        return;
    }

    if (expire) {
        mstime_t basetime = (flags & (ARGS_EXAT | ARGS_PXAT)) ? 0 : commandTimeSnapshot();

        if (convertExpireArgumentToUnixTime(c, expire, basetime, unit, &when) == C_ERR)
            return;

        if (checkAlreadyExpired(when)) set_expired = 1;
    }

    if (flags & (ARGS_SET_FNX | ARGS_SET_FXX)) {
        bool cache_safe = (flags & ARGS_SET_FXX) && o && objectGetEncoding(o) == OBJ_ENCODING_HASHTABLE;
        if (cache_safe) mxx_cached = zmalloc(sizeof(smember *) * num_members);
        if (o) {
            for (i = members_index; i < c->argc; i++) {
                if (mxx_cached) {
                    smember *member = NULL;
                    if (!hashtableFind(objectGetVal(o), objectGetVal(c->argv[i]), (void **)&member)) {
                        zfree(mxx_cached);
                        addReply(c, shared.czero);
                        return;
                    }
                    mxx_cached[i - members_index] = member;
                    if (smemberHasExpiry(member) != (when != EXPIRY_NONE)) cache_safe = false;
                } else if (((flags & ARGS_SET_FNX) && setTypeIsMember(o, objectGetVal(c->argv[i]))) ||
                           ((flags & ARGS_SET_FXX) && !setTypeIsMember(o, objectGetVal(c->argv[i])))) {
                    addReply(c, shared.czero);
                    return;
                }
            }
            if (!cache_safe) {
                zfree(mxx_cached);
                mxx_cached = NULL;
            }
        } else if (flags & ARGS_SET_FXX) {
            addReply(c, shared.czero);
            return;
        }
    }

    if (o == NULL) {
        if (set_expired) {
            addReply(c, shared.czero);
            return;
        }
        o = setTypeCreate(objectGetVal(c->argv[members_index]), num_members);
        dbAdd(c->db, c->argv[1], &o);
    }

    bool has_volatile_members = setTypeHasVolatileMembers(o);
    size_t original_size = setTypeSize(o);
    mstime_t key_expire = objectGetExpire(o);
    bool restore_key_expire = false;

    if (set_expired) {
        setTypeIgnoreTTL(o, true);
        for (i = members_index; i < c->argc; i++) {
            if (setTypeRemove(o, objectGetVal(c->argv[i]))) {
                if (new_argv == NULL) {
                    new_argv = zmalloc(sizeof(robj *) * (num_members + 2));
                    new_argv[new_argc++] = shared.srem;
                    new_argv[new_argc++] = c->argv[1];
                    incrRefCount(c->argv[1]);
                }
                new_argv[new_argc++] = c->argv[i];
                incrRefCount(c->argv[i]);
                server.stat_expiredsetmembers++;
                changes++;
            }
        }
        setTypeIgnoreTTL(o, false);

        if (changes) {
            if (has_volatile_members != setTypeHasVolatileMembers(o)) {
                dbUpdateObjectWithVolatileItemsTracking(c->db, o);
            }
            replaceClientCommandVector(c, new_argc, new_argv);
            notifyKeyspaceEvent(NOTIFY_SET, "sexpired", c->argv[1], c->db->id);
            signalModifiedKey(c, c->db, c->argv[1]);
            server.dirty += changes;
        }
    } else {
        int add_flags = (flags & ARGS_KEEPTTL) ? SET_ADD_KEEP_EXPIRY : 0;
        for (i = members_index; i < c->argc; i++) {
            if (mxx_cached) {
                smember *member = mxx_cached[i - members_index];
                mstime_t current = smemberGetExpiry(member);
                if (!(add_flags & SET_ADD_KEEP_EXPIRY) && current != when) {
                    setTypeUpdateHashtableMemberExpiry(o, objectGetVal(o), member, current, when);
                    changes++;
                }
                continue;
            }

            bool replaced_expired = false;
            bool ttl_changed = false;
            if (setTypeAddWithExpiry(o, objectGetVal(c->argv[i]), when, add_flags, &replaced_expired, &ttl_changed)) {
                added++;
                changes++;
            }
            if (ttl_changed) changes++;

            if (replaced_expired) {
                if (flags & ARGS_KEEPTTL) {
                    if (expired_members == NULL) expired_members = zmalloc(sizeof(robj *) * num_members);
                    expired_members[num_expired] = c->argv[i];
                    incrRefCount(c->argv[i]);
                }
                num_expired++;
            }
        }
        if (mxx_cached) zfree(mxx_cached);
        mxx_cached = NULL;

        if (changes) {
            if (has_volatile_members != setTypeHasVolatileMembers(o)) {
                dbUpdateObjectWithVolatileItemsTracking(c->db, o);
            }

            if (num_expired) {
                server.stat_expiredsetmembers += num_expired;
                notifyKeyspaceEvent(NOTIFY_SET, "sexpired", c->argv[1], c->db->id);
                if (expired_members != NULL) {
                    restore_key_expire = num_expired == (int)original_size && key_expire != EXPIRY_NONE;
                    int idx = 0;
                    while (idx < num_expired) {
                        idx += propagateItemsDeletion(c->db, o, num_expired - idx, &expired_members[idx], c->slot);
                    }
                    zfree(expired_members);
                }
            }

            if (flags & (ARGS_SET_NX | ARGS_SET_XX | ARGS_SET_FNX | ARGS_SET_FXX | ARGS_EX | ARGS_PX | ARGS_EXAT)) {
                rewriteSaddexCommand(c, members_index, flags, expire, when);
            }
            if (restore_key_expire) propagateCommandAndKeyExpiration(c, c->argv[1], key_expire);

            signalModifiedKey(c, c->db, c->argv[1]);
            server.dirty += changes;

            if (added) notifyKeyspaceEvent(NOTIFY_SET, "sadd", c->argv[1], c->db->id);
            if (expire) notifyKeyspaceEvent(NOTIFY_SET, "sexpire", c->argv[1], c->db->id);
        }
    }

    if (setTypeSize(o) == 0) {
        dbDelete(c->db, c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC, "del", c->argv[1], c->db->id);
    }
    addReplyLongLong(c, added);
}
