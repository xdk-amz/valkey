/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Member expiration for the SET type. A listpack set stores one entry per
 * member, so the metadata entry follows the member entry itself.
 *
 * Replicas and AOF loading apply commands under POLICY_IGNORE_EXPIRE, so an
 * expired member hidden here is live there: commands whose effect depends on
 * liveness propagate the members they changed, and copies and encoding
 * conversions carry expired members with their expiry. */

#include "server.h"
#include "expire.h"
#include "hashtable.h"
#include "intset.h"
#include "listpack.h"
#include "vset.h"

static long long smemberGetExpiryVsetFunc(const void *m) {
    return smemberGetExpiry((const smember *)m);
}

/*-----------------------------------------------------------------------------
 * Hashtable vset
 *----------------------------------------------------------------------------*/

static vset *setTypeGetVolatileSet(robj *o) {
    serverAssert(objectGetEncoding(o) == OBJ_ENCODING_HASHTABLE);
    vset *set = (vset *)hashtableMetadata(objectGetVal(o));
    return vsetIsValid(set) ? set : NULL;
}

static vset *setTypeGetOrCreateVolatileSet(robj *o) {
    serverAssert(objectGetEncoding(o) == OBJ_ENCODING_HASHTABLE);
    hashtable *ht = objectGetVal(o);
    vset *set = (vset *)hashtableMetadata(ht);
    if (!vsetIsValid(set)) {
        vsetInit(set);
        hashtableSetType(ht, &setWithVolatileMembersHashtableType);
    }
    return set;
}

void setTypeFreeVolatileSet(robj *o) {
    vset *set = (vset *)hashtableMetadata(objectGetVal(o));
    if (vsetIsValid(set)) vsetRelease(set);
    hashtableSetType(objectGetVal(o), &setHashtableType);
}

void setTypeTrackMember(robj *o, smember *m) {
    serverAssert(vsetAddEntry(setTypeGetOrCreateVolatileSet(o), smemberGetExpiryVsetFunc, m));
}

/* Call before freeing 'm': vsetRemoveEntry reads its expiry. */
void setTypeUntrackMember(robj *o, smember *m) {
    if (!smemberHasExpiry(m)) return;
    vset *set = setTypeGetVolatileSet(o);
    debugServerAssert(set);
    serverAssert(vsetRemoveEntry(set, smemberGetExpiryVsetFunc, m));
    if (vsetIsEmpty(set)) setTypeFreeVolatileSet(o);
}

/* Include unreclaimed expired members when rebuilding the vset. */
void setTypeTrackVolatileMembers(robj *o) {
    hashtable *ht = objectGetVal(o);
    hashtableIterator iter;
    hashtableInitIterator(&iter, ht, HASHTABLE_ITER_SKIP_VALIDATION);
    void *next;
    while (hashtableNext(&iter, &next)) {
        smember *m = next;
        if (!smemberHasExpiry(m)) continue;
        setTypeTrackMember(o, m);
    }
    hashtableCleanupIterator(&iter);
}

/*-----------------------------------------------------------------------------
 * Listpack helpers
 *----------------------------------------------------------------------------*/

long long setTypeListpackGetExpiry(unsigned char *lp, unsigned char *p) {
    unsigned char *metadata_ptr = lpGetMetadata(lp, p);
    return metadata_ptr ? lpGetMetadataValue(metadata_ptr) : EXPIRY_NONE;
}

static unsigned char *setTypeListpackFind(unsigned char *lp, sds member) {
    unsigned char *p = lpFirst(lp);
    if (p == NULL) return NULL;
    return lpFind(lp, p, (unsigned char *)member, sdslen(member), 0);
}

bool setTypeHasVolatileMembers(robj *o) {
    if (o == NULL) return false;
    int encoding = objectGetEncoding(o);
    if (encoding == OBJ_ENCODING_LISTPACK) return lpIsMetadata(lpStart(objectGetVal(o)));
    if (encoding == OBJ_ENCODING_HASHTABLE) {
        vset *set = setTypeGetVolatileSet(o);
        return set && !vsetIsEmpty(set);
    }
    return false;
}

long long setTypeVolatileCount(robj *o) {
    serverAssert(objectGetType(o) == OBJ_SET);

    switch (objectGetEncoding(o)) {
    case OBJ_ENCODING_LISTPACK: {
        unsigned char *p = lpStart(objectGetVal(o));
        return lpIsMetadata(p) ? lpGetMetadataValue(p) : 0;
    }
    case OBJ_ENCODING_HASHTABLE: {
        vset *set = setTypeGetVolatileSet(o);
        return set ? (long long)vsetSize(set) : 0;
    }
    case OBJ_ENCODING_INTSET: return 0;
    default: serverPanic("Unknown set encoding");
    }
}

int setTypeGetExpiry(robj *o, sds member, mstime_t *expiry) {
    *expiry = EXPIRY_NONE;

    if (objectGetEncoding(o) == OBJ_ENCODING_INTSET) return setTypeIsMember(o, member) ? C_OK : C_ERR;

    if (objectGetEncoding(o) == OBJ_ENCODING_LISTPACK) {
        unsigned char *lp = objectGetVal(o);
        unsigned char *p = setTypeListpackFind(lp, member);
        if (p == NULL) return C_ERR;
        long long member_expiry = setTypeListpackGetExpiry(lp, p);
        if (!listpackObjectItemIsValid(member_expiry)) return C_ERR;
        *expiry = member_expiry;
        return C_OK;
    }

    serverAssert(objectGetEncoding(o) == OBJ_ENCODING_HASHTABLE);
    void *found = NULL;
    if (!hashtableFind(objectGetVal(o), member, &found)) return C_ERR;
    *expiry = smemberGetExpiry(found);
    return C_OK;
}

void setTypeIgnoreTTL(robj *o, bool ignore) {
    if (objectGetEncoding(o) == OBJ_ENCODING_LISTPACK) {
        listpackObjectIgnoreTTL(ignore);
        return;
    }
    /* A conversion inside the bracket must not leak ignore-TTL state. */
    if (!ignore) listpackObjectIgnoreTTL(false);
    if (objectGetEncoding(o) != OBJ_ENCODING_HASHTABLE) return;

    if (!ignore && setTypeGetVolatileSet(o) == NULL) ignore = true;
    hashtableSetType(objectGetVal(o), ignore ? &setHashtableType : &setWithVolatileMembersHashtableType);
}

/*-----------------------------------------------------------------------------
 * Setting a member expiry
 *----------------------------------------------------------------------------*/

/* An intset cannot hold metadata; the listpack budget adds the count header and one expiry entry. */
static void setTypeConvertForExpiry(robj *o, size_t extra, size_t memberlen) {
    serverAssert(objectGetEncoding(o) == OBJ_ENCODING_INTSET);
    int enc = setTypeIntsetFitsListpack(o, extra, memberlen, 2 * LP_METADATA_MAX_ENTRY_BYTES)
                  ? OBJ_ENCODING_LISTPACK
                  : OBJ_ENCODING_HASHTABLE;
    setTypeConvertAndExpand(o, enc, intsetLen(objectGetVal(o)) + extra, 1);
}

/* Reallocates the listpack; 'p' is invalid afterwards. */
static void setTypeListpackSetExpiry(robj *o, unsigned char *p, mstime_t expiry) {
    unsigned char *lp = objectGetVal(o);
    unsigned char *metadata_ptr = lpGetMetadata(lp, p);

    if (expiry == EXPIRY_NONE) {
        lp = lpRemoveMetadata(lp, metadata_ptr);
        objectSetVal(o, lp);
        listpackObjectUpdateVolatileCount(o, -1);
        return;
    }

    unsigned char intenc[LP_MAX_INT_ENCODING_LEN];
    uint64_t enclen;
    lpEncodeIntegerGetType(expiry, intenc, &enclen);
    if (metadata_ptr) {
        lp = lpInsertMetadata(lp, intenc, enclen, metadata_ptr, LP_REPLACE, NULL);
    } else {
        lp = lpInsertMetadata(lp, intenc, enclen, p, LP_AFTER, NULL);
    }
    objectSetVal(o, lp);
    if (!metadata_ptr) listpackObjectUpdateVolatileCount(o, 1);
}

/* Locates 'member' and reports its expiry through 'current'. */
static expiryModificationResult
setTypeSetExpiryInternal(robj *o, sds member, mstime_t expiry, int flags, mstime_t *current) {
    if (o == NULL) return EXPIRATION_MODIFICATION_NOT_EXIST;

    *current = EXPIRY_NONE;
    unsigned char *p = NULL;
    void **member_ref = NULL;
    smember *m = NULL;

    if (objectGetEncoding(o) == OBJ_ENCODING_LISTPACK) {
        p = setTypeListpackFind(objectGetVal(o), member);
        if (p == NULL) return EXPIRATION_MODIFICATION_NOT_EXIST;
        *current = setTypeListpackGetExpiry(objectGetVal(o), p);
        /* Do not resurrect an expired member; active expiration propagates its SREM. */
        if (!listpackObjectItemIsValid(*current)) return EXPIRATION_MODIFICATION_NOT_EXIST;
    } else if (objectGetEncoding(o) == OBJ_ENCODING_HASHTABLE) {
        member_ref = hashtableFindRef(objectGetVal(o), member);
        if (member_ref == NULL) return EXPIRATION_MODIFICATION_NOT_EXIST;
        m = *member_ref;
        *current = smemberGetExpiry(m);
    } else if (!setTypeIsMember(o, member)) {
        return EXPIRATION_MODIFICATION_NOT_EXIST;
    }

    /* EXPIRY_NONE is treated as +inf: GT can never beat it, LT always does. */
    if ((flags & EXPIRE_NX) && *current != EXPIRY_NONE) return EXPIRATION_MODIFICATION_FAILED_CONDITION;
    if ((flags & EXPIRE_XX) && *current == EXPIRY_NONE) return EXPIRATION_MODIFICATION_FAILED_CONDITION;
    if ((flags & EXPIRE_GT) && (*current == EXPIRY_NONE || expiry <= *current))
        return EXPIRATION_MODIFICATION_FAILED_CONDITION;
    if ((flags & EXPIRE_LT) && *current != EXPIRY_NONE && expiry >= *current)
        return EXPIRATION_MODIFICATION_FAILED_CONDITION;

    if (expiry == EXPIRY_NONE && *current == EXPIRY_NONE) return EXPIRATION_MODIFICATION_FAILED;
    if (expiry != EXPIRY_NONE && checkAlreadyExpired(expiry)) {
        serverAssert(setTypeRemove(o, member));
        return EXPIRATION_MODIFICATION_EXPIRE_ASAP;
    }

    if (objectGetEncoding(o) == OBJ_ENCODING_INTSET) {
        setTypeConvertForExpiry(o, 0, 0);
        if (objectGetEncoding(o) == OBJ_ENCODING_LISTPACK) {
            p = setTypeListpackFind(objectGetVal(o), member);
        } else {
            member_ref = hashtableFindRef(objectGetVal(o), member);
            m = *member_ref;
        }
    }

    if (p != NULL) {
        setTypeListpackSetExpiry(o, p, expiry);
        return EXPIRATION_MODIFICATION_SUCCESSFUL;
    }

    if (expiry == EXPIRY_NONE) {
        setTypeUntrackMember(o, m);
        *member_ref = smemberSetExpiry(m, EXPIRY_NONE);
        return EXPIRATION_MODIFICATION_SUCCESSFUL;
    }

    smember *updated = smemberSetExpiry(m, expiry);
    *member_ref = updated;
    if (*current == EXPIRY_NONE) {
        setTypeTrackMember(o, updated);
    } else {
        serverAssert(vsetUpdateEntry(setTypeGetVolatileSet(o), smemberGetExpiryVsetFunc, m, updated, *current, expiry));
    }
    return EXPIRATION_MODIFICATION_SUCCESSFUL;
}

expiryModificationResult setTypeSetExpiry(robj *o, sds member, mstime_t expiry, int flags) {
    mstime_t current;
    return setTypeSetExpiryInternal(o, member, expiry, flags, &current);
}

/*-----------------------------------------------------------------------------
 * Adding a member with an expiry
 *----------------------------------------------------------------------------*/

static void setTypeAddNewWithExpiry(robj *o, sds member, mstime_t expiry) {
    size_t len = sdslen(member);

    if (objectGetEncoding(o) == OBJ_ENCODING_INTSET) setTypeConvertForExpiry(o, 1, len);

    if (objectGetEncoding(o) == OBJ_ENCODING_LISTPACK) {
        unsigned char *lp = objectGetVal(o);
        size_t add_bytes = len;
        if (expiry != EXPIRY_NONE) add_bytes += 2 * LP_METADATA_MAX_ENTRY_BYTES;
        if (lpLength(lp) < server.set_max_listpack_entries && len <= server.set_max_listpack_value &&
            lpSafeToAdd(lp, add_bytes)) {
            lp = lpAppend(lp, (unsigned char *)member, len);
            objectSetVal(o, lp);
            if (expiry != EXPIRY_NONE) setTypeListpackSetExpiry(o, lpLast(lp), expiry);
            return;
        }
        setTypeConvertAndExpand(o, OBJ_ENCODING_HASHTABLE, lpLength(lp) + 1, 1);
    }

    serverAssert(objectGetEncoding(o) == OBJ_ENCODING_HASHTABLE);
    smember *m = smemberCreate(member, len, expiry);
    serverAssert(hashtableAdd(objectGetVal(o), m));
    if (expiry != EXPIRY_NONE) setTypeTrackMember(o, m);
}

int setTypeAddWithExpiry(robj *o, sds member, mstime_t expiry, int flags, bool *replaced_expired, bool *ttl_changed) {
    bool volatile_set = setTypeHasVolatileMembers(o);
    if (!volatile_set && expiry == EXPIRY_NONE) return setTypeAdd(o, member);

    if (flags & SET_ADD_KEEP_EXPIRY) {
        if (setTypeIsMember(o, member)) return 0;
    } else {
        mstime_t current;
        if (setTypeSetExpiryInternal(o, member, expiry, 0, &current) != EXPIRATION_MODIFICATION_NOT_EXIST) {
            if (current != expiry && ttl_changed) *ttl_changed = true;
            return 0;
        }
    }

    if (volatile_set) {
        setTypeIgnoreTTL(o, true);
        int removed = setTypeRemove(o, member);
        setTypeIgnoreTTL(o, false);
        if (removed) *replaced_expired = true;
    }

    setTypeAddNewWithExpiry(o, member, expiry);
    return 1;
}

/*-----------------------------------------------------------------------------
 * Active expiration
 *----------------------------------------------------------------------------*/

typedef struct {
    robj *o;
    robj **members;
    size_t nmembers;
} setExpiryContext;

static int setTypeExpireMember(void *entry, void *c) {
    setExpiryContext *ctx = c;
    smember *m = entry;
    serverAssert(hashtablePop(objectGetVal(ctx->o), m, NULL));
    if (ctx->members) ctx->members[ctx->nmembers++] = createStringObjectFromSds(m);
    smemberFree(m);
    return 1;
}

size_t setTypeDeleteExpiredMembers(robj *o, mstime_t now, unsigned long max_members, robj **out_members) {
    if (!setTypeHasVolatileMembers(o)) return 0;

    if (objectGetEncoding(o) == OBJ_ENCODING_LISTPACK) {
        unsigned char *lp = objectGetVal(o);
        unsigned char *p = lpFirst(lp);
        size_t expired = 0;
        unsigned char intbuf[LP_INTBUF_SIZE];

        while (p != NULL && expired < max_members) {
            mstime_t expiry = setTypeListpackGetExpiry(lp, p);
            if (expiry != EXPIRY_NONE && expiry <= now) {
                if (out_members) {
                    int64_t len;
                    unsigned char *str = lpGet(p, &len, intbuf);
                    out_members[expired] = createStringObject((char *)str, len);
                }
                lp = lpDeleteRangeWithEntry(lp, &p, 1);
                objectSetVal(o, lp);
                expired++;
                continue;
            }
            p = lpNext(lp, p);
        }

        listpackObjectUpdateVolatileCount(o, -(long)expired);
        server.stat_expiredsetmembers += expired;
        return expired;
    }

    serverAssert(objectGetEncoding(o) == OBJ_ENCODING_HASHTABLE);
    vset *set = setTypeGetVolatileSet(o);

    /* The pops must see the expired members they remove. */
    setTypeIgnoreTTL(o, true);
    setExpiryContext ctx = {.o = o, .members = out_members, .nmembers = 0};
    size_t expired = vsetRemoveExpired(set, smemberGetExpiryVsetFunc, setTypeExpireMember, now, max_members, &ctx);
    serverAssert(ctx.nmembers <= max_members);
    if (vsetIsEmpty(set))
        setTypeFreeVolatileSet(o);
    else
        setTypeIgnoreTTL(o, false);
    server.stat_expiredsetmembers += expired;
    return expired;
}

/*-----------------------------------------------------------------------------
 * Defrag
 *----------------------------------------------------------------------------*/

typedef struct {
    robj *o;
    void *(*defragfn)(void *);
} setDefragMemberCtx;

static void defragSetMemberCallback(void *privdata, void *element_ref) {
    setDefragMemberCtx *ctx = privdata;
    smember **member_ref = element_ref;
    smember *m = *member_ref;
    smember *new_m = smemberDefrag(m, ctx->defragfn);
    if (new_m == NULL) return;
    if (smemberHasExpiry(new_m)) {
        /* The vset indexes members by pointer. */
        mstime_t expiry = smemberGetExpiry(new_m);
        serverAssert(vsetUpdateEntry(setTypeGetVolatileSet(ctx->o), smemberGetExpiryVsetFunc, m, new_m, expiry, expiry));
    }
    *member_ref = new_m;
}

size_t setTypeScanDefrag(robj *o, size_t cursor, void *(*defragfn)(void *)) {
    /* Only one object is defragged at a time, so one static cursor state is enough. */
    static struct setDefragCursor {
        size_t cursor;
        bool in_vset_phase;
    } state;
    struct setDefragCursor *st = (struct setDefragCursor *)cursor;

    if (st == NULL) {
        st = &state;
        st->cursor = 0;
        st->in_vset_phase = false;
    }

    if (!st->in_vset_phase) {
        setDefragMemberCtx ctx = {.o = o, .defragfn = defragfn};
        st->cursor = hashtableScanDefrag(objectGetVal(o), st->cursor, defragSetMemberCallback, &ctx, defragfn,
                                         HASHTABLE_SCAN_EMIT_REF);
        if (st->cursor == 0) {
            if (setTypeGetVolatileSet(o) == NULL) return 0;
            st->in_vset_phase = true;
        }
    } else {
        vset *set = setTypeGetVolatileSet(o);
        if (set == NULL) return 0;
        st->cursor = vsetScanDefrag(set, st->cursor, defragfn);
        if (st->cursor == 0) return 0;
    }
    return (size_t)st;
}
