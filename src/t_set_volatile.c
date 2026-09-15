/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Volatile member state for the SET type: vset tracking in the hashtable
 * metadata tail, the hashtable type swap, the listpack volatile-count header,
 * expiry get/set for both encodings, active expiration, the ignore-TTL bracket
 * and vset defrag. Mirrors t_hash.c, except that a listpack set stores ONE
 * entry per member, so a member's trailing metadata entry follows the member
 * entry itself and a deletion is lpDeleteRangeWithEntry(..., 1) rather than 2.
 *
 * Every hashtable-encoded set reserves the 8 byte vset slot, so a transition is
 * a plain hashtableSetType(): bucket tables, entry references and the struct
 * pointer all survive it.
 *
 * Destructor invariant: setHashtableType frees members with sdsfree, which
 * would free from the middle of the allocation of a member carrying the expiry
 * prefix. The set therefore sits on that type if and only if it holds no
 * prefixed member. */

#include "server.h"
#include "expire.h"
#include "hashtable.h"
#include "intset.h"
#include "listpack.h"
#include "smember.h"
#include "vset.h"

/* A vsetGetExpiryFunc over smembers. */
static long long smemberGetExpiryVsetFunc(const void *m) {
    return smemberGetExpiry((const smember *)m);
}

/* Transient "ignore TTL" state for the listpack encoding, which has nowhere to
 * hang it (the hashtable encoding uses the type swap). Safe as a file-scope
 * flag because command execution is single threaded and every bracket is a
 * tight set(true)/.../set(false) pair that does not span commands. */
static bool listpack_ttl_ignored = false;

/*-----------------------------------------------------------------------------
 * Hashtable type swap and vset plumbing
 *----------------------------------------------------------------------------*/

/* The vset of a hashtable-encoded set, or NULL when the set holds no volatile
 * member. O(1): a plain set has its reserved slot zeroed, which vsetIsValid
 * reports as "not initialized". Mirror of hashTypeGetVolatileSet. */
static vset *setTypeGetVolatileSet(robj *o) {
    serverAssert(objectGetEncoding(o) == OBJ_ENCODING_HASHTABLE);
    vset *set = (vset *)hashtableMetadata(objectGetVal(o));
    return vsetIsValid(set) ? set : NULL;
}

/* The vset of a hashtable-encoded set, initializing the reserved slot and
 * moving the set onto the validating volatile type when it does not have it
 * yet. The slot is already there, so this touches neither the hashtable struct
 * nor the bucket tables: every pointer the caller holds survives it. Mirror of
 * hashTypeGetOrcreateVolatileSet. */
static vset *setTypeGetOrCreateVolatileSet(robj *o) {
    serverAssert(objectGetEncoding(o) == OBJ_ENCODING_HASHTABLE);
    hashtable *ht = objectGetVal(o);
    vset *set = (vset *)hashtableMetadata(ht);
    if (!vsetIsValid(set)) {
        vsetInit(set);
        /* Place the validating access function only now that it is needed. */
        hashtableSetType(ht, &setWithVolatileMembersHashtableType);
    }
    return set;
}

/* Release the vset and move the set back onto the plain type.
 *
 * Callers MUST have freed or rebuilt-as-plain-sds every member carrying the
 * expiry prefix first: setHashtableType's destructor is sdsfree, and a
 * prefixed member freed that way frees from the middle of an allocation. An
 * empty vset is exactly that proof, since the vset holds every prefixed
 * member, so it is asserted here. Mirror of hashTypeFreeVolatileSet. */
static void setTypeFreeVolatileSet(robj *o) {
    hashtable *ht = objectGetVal(o);
    vset *set = (vset *)hashtableMetadata(ht);
    if (vsetIsValid(set)) {
        serverAssert(vsetIsEmpty(set));
        vsetRelease(set);
    }
    hashtableSetType(ht, &setHashtableType);
}

/* Object-free hook: release the vset index of a hashtable-encoded set that is
 * about to be freed, mirror of hashTypeFreeVolatileSet. Unlike
 * setTypeFreeVolatileSet this does NOT swap the type back: hashtableRelease
 * runs right after it and must keep the volatile type's smemberFree destructor
 * for the prefixed members. Only the vset's own heap is touched, so this is
 * safe from the lazyfree thread. */
void setTypeReleaseVolatileSet(robj *o) {
    serverAssert(objectGetEncoding(o) == OBJ_ENCODING_HASHTABLE);
    vset *set = (vset *)hashtableMetadata(objectGetVal(o));
    if (vsetIsValid(set)) vsetRelease(set);
}

/* Untrack 'm' from the vset of a hashtable-encoded set and, when that empties
 * the vset, drop the volatile state. 'm' is only read, never freed; the caller
 * frees it (or rebuilds it without the prefix) BEFORE the drop can happen,
 * which is why the untrack must run before the free. */
static void setTypeUntrackMember(robj *o, smember *m) {
    if (!smemberHasExpiry(m)) return;
    vset *set = setTypeGetVolatileSet(o);
    serverAssert(set != NULL);
    serverAssert(vsetRemoveEntry(set, smemberGetExpiryVsetFunc, m));
}

/* Drop the volatile state if the vset just became empty. */
static void setTypeMaybeFreeVolatileSet(robj *o) {
    vset *set = setTypeGetVolatileSet(o);
    if (set && vsetIsEmpty(set)) setTypeFreeVolatileSet(o);
}

/*-----------------------------------------------------------------------------
 * Listpack helpers
 *----------------------------------------------------------------------------*/

/* Expiry of the listpack member entry 'p': the integer payload of its trailing
 * metadata entry, or EXPIRY_NONE when it carries none. Purely a read of what
 * is stored; callers decide how to treat expired ones. */
long long setTypeListpackGetExpiry(unsigned char *lp, unsigned char *p) {
    unsigned char *metadata_ptr = lpGetMetadata(lp, p);
    return metadata_ptr ? lpGetMetadataValue(metadata_ptr) : EXPIRY_NONE;
}

/* Locate 'member' in a listpack-encoded set. Returns the member entry, or NULL
 * when it is absent. Does not filter expired members; callers apply
 * setTypeListpackMemberIsValid() on the expiry when they need visibility. */
static unsigned char *setTypeListpackFind(unsigned char *lp, sds member) {
    unsigned char *p = lpFirst(lp);
    if (p == NULL) return NULL;
    return lpFind(lp, p, (unsigned char *)member, sdslen(member), 0);
}

/*-----------------------------------------------------------------------------
 * Volatile state introspection
 *----------------------------------------------------------------------------*/

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
    serverAssert(o != NULL && objectGetType(o) == OBJ_SET);
    if (expiry) *expiry = EXPIRY_NONE;

    /* Fast path: a set without TTLs answers with the plain membership test. */
    if (!setTypeHasVolatileMembers(o)) return setTypeIsMember(o, member) ? C_OK : C_ERR;

    if (objectGetEncoding(o) == OBJ_ENCODING_LISTPACK) {
        unsigned char *lp = objectGetVal(o);
        unsigned char *p = setTypeListpackFind(lp, member);
        if (p == NULL) return C_ERR;
        long long member_expiry = setTypeListpackGetExpiry(lp, p);
        if (!setTypeListpackMemberIsValid(member_expiry)) return C_ERR;
        if (expiry) *expiry = member_expiry;
        return C_OK;
    }

    serverAssert(objectGetEncoding(o) == OBJ_ENCODING_HASHTABLE);
    /* The volatile type's validateEntry callback hides expired members from
     * the lookup, so one reports C_ERR just like a missing member. */
    void *found = NULL;
    if (!hashtableFind(objectGetVal(o), member, &found)) return C_ERR;
    if (expiry) *expiry = smemberGetExpiry(found);
    return C_OK;
}

/*-----------------------------------------------------------------------------
 * Visibility of expired members
 *----------------------------------------------------------------------------*/

bool setTypeListpackMemberIsValid(long long expiry) {
    if (expiry == EXPIRY_NONE) return true;
    /* Inside an ignore-TTL bracket (e.g. SADD force-replacing an already
     * expired member) every member is visible, mirroring the hashtable
     * encoding's swap to the non-validating volatile type. */
    if (listpack_ttl_ignored) return true;
    if (getExpirationPolicyWithFlags(0) == POLICY_IGNORE_EXPIRE) return true;
    return !timestampIsExpired(expiry);
}

void setTypeIgnoreTTL(robj *o, bool ignore) {
    serverAssert(o != NULL && objectGetType(o) == OBJ_SET);

    if (objectGetEncoding(o) == OBJ_ENCODING_LISTPACK) {
        listpack_ttl_ignored = ignore;
        return;
    }
    /* Clearing happens regardless of encoding so that a bracket whose object
     * was converted listpack->hashtable in between cannot leak the flag. */
    if (!ignore) listpack_ttl_ignored = false;
    if (objectGetEncoding(o) != OBJ_ENCODING_HASHTABLE) return;

    hashtable *ht = objectGetVal(o);
    /* Never swap to or from setHashtableType here: that type frees members
     * with sdsfree, which is wrong for a member carrying the expiry prefix
     * (destructor invariant). A set with no volatile member already ignores
     * nothing, so there is nothing to bracket. */
    if (hashtableGetType(ht) == &setHashtableType) return;
    hashtableSetType(ht, ignore ? &setVolatileIgnoreTTLHashtableType : &setWithVolatileMembersHashtableType);
}

/*-----------------------------------------------------------------------------
 * Listpack aggregate volatile-count header
 *----------------------------------------------------------------------------*/

/* Maintain the aggregate volatile-count header of a listpack-encoded set.
 *
 * The header is a single tagged entry leading the listpack whose integer
 * payload is the number of members carrying an expiry. It exists only while
 * that count is > 0: created on the 0->1 transition, updated in place, and
 * deleted on the 1->0 transition, so sets without member TTLs pay nothing.
 *
 * Must be called after the mutation it accounts for; it may reallocate the
 * listpack, so callers must not reuse element pointers taken before it. */
void setTypeUpdateVolatileCount(robj *o, long delta) {
    if (delta == 0) return;
    serverAssert(objectGetEncoding(o) == OBJ_ENCODING_LISTPACK);
    unsigned char *lp = objectGetVal(o);
    unsigned char *head = lpStart(lp);
    int has_head = lpIsMetadata(head);
    long long count = (has_head ? lpGetMetadataValue(head) : 0) + delta;
    serverAssert(count >= 0);
    if (count == 0) {
        if (has_head) lp = lpRemoveMetadata(lp, head);
        objectSetVal(o, lp);
        return;
    }

    unsigned char intenc[LP_MAX_INT_ENCODING_LEN];
    uint64_t enclen;
    lpEncodeIntegerGetType(count, intenc, &enclen);
    /* head == lpStart(lp): replace the existing header in place, or insert a new
     * one before the first physical entry / EOF. */
    lp = lpInsertMetadata(lp, intenc, enclen, head, has_head ? LP_REPLACE : LP_BEFORE, NULL);
    objectSetVal(o, lp);
}

/*-----------------------------------------------------------------------------
 * Removing members
 *----------------------------------------------------------------------------*/

/* Remove 'member' from a set that has volatile members, whether or not the
 * member itself is volatile and whether or not it is already expired. The
 * caller decides visibility: to reach an expired member, wrap this in an
 * ignore-TTL bracket. Returns 1 if a member was removed. */
static int setTypeRemoveFromVolatileSet(robj *o, sds member) {
    if (objectGetEncoding(o) == OBJ_ENCODING_LISTPACK) {
        unsigned char *lp = objectGetVal(o);
        unsigned char *p = setTypeListpackFind(lp, member);
        if (p == NULL) return 0;
        bool was_volatile = lpGetMetadata(lp, p) != NULL;
        /* Deletes the member entry and, with it, its trailing metadata. */
        lp = lpDeleteRangeWithEntry(lp, &p, 1);
        objectSetVal(o, lp);
        if (was_volatile) setTypeUpdateVolatileCount(o, -1);
        return 1;
    }

    serverAssert(objectGetEncoding(o) == OBJ_ENCODING_HASHTABLE);
    void *popped = NULL;
    if (!hashtablePop(objectGetVal(o), member, &popped)) return 0;
    smember *m = popped;
    setTypeUntrackMember(o, m);
    smemberFree(m);
    setTypeMaybeFreeVolatileSet(o);
    return 1;
}

int setTypeRemoveVolatile(robj *o, sds member) {
    serverAssert(o != NULL && objectGetType(o) == OBJ_SET);
    serverAssert(objectGetEncoding(o) == OBJ_ENCODING_HASHTABLE);
    /* The validating volatile type hides expired members from the pop, so one
     * reports 0 and is left to active expiration, which propagates its SREM. */
    return setTypeRemoveFromVolatileSet(o, member);
}

/*-----------------------------------------------------------------------------
 * Setting a member expiry
 *----------------------------------------------------------------------------*/

/* Shared NX/XX/GT/LT evaluation, same semantics as hexpireGenericCommand.
 * 'flags' are the EXPIRE_NX / EXPIRE_XX / EXPIRE_GT / EXPIRE_LT bits from
 * expire.h, which the S*EXPIRE commands parse with the same helper the hash and
 * key commands use. EXPIRY_NONE is treated as +inf: GT can never beat it, LT
 * always does. */
static bool setTypeExpiryConditionsMet(mstime_t current, mstime_t expiry, int flags) {
    if ((flags & EXPIRE_NX) && current != EXPIRY_NONE) return false;
    if ((flags & EXPIRE_XX) && current == EXPIRY_NONE) return false;
    if ((flags & EXPIRE_GT) && (current == EXPIRY_NONE || expiry <= current)) return false;
    if ((flags & EXPIRE_LT) && current != EXPIRY_NONE && expiry >= current) return false;
    return true;
}

/* True when a set of 'size' members, the largest of which is 'maxelelen'
 * bytes, still fits the listpack thresholds. */
static bool setTypeListpackFits(unsigned long size, size_t maxelelen) {
    return size <= server.set_max_listpack_entries && maxelelen <= server.set_max_listpack_value;
}

/* Widest decimal member of an intset, used to pick the target encoding when a
 * TTL forces the conversion away from OBJ_ENCODING_INTSET. */
static size_t intsetMaxElementLen(intset *is) {
    if (intsetLen(is) == 0) return 0;
    return max(sdigits10(intsetMax(is)), sdigits10(intsetMin(is)));
}

/* An intset cannot hold metadata: convert to listpack when the set still fits
 * the listpack thresholds with 'extra' members added, else to hashtable. */
static void setTypeConvertForExpiry(robj *o, size_t extra, size_t memberlen) {
    serverAssert(objectGetEncoding(o) == OBJ_ENCODING_INTSET);
    intset *is = objectGetVal(o);
    unsigned long size = intsetLen(is) + extra;
    size_t maxelelen = max(intsetMaxElementLen(is), memberlen);
    int enc = setTypeListpackFits(size, maxelelen) ? OBJ_ENCODING_LISTPACK : OBJ_ENCODING_HASHTABLE;
    setTypeConvertAndExpand(o, enc, size, 1);
}

/* Attach, refresh or drop the expiry of the listpack member entry 'p'.
 * 'current' is the expiry stored there today. Reallocates the listpack and
 * invalidates 'p'. */
static void setTypeListpackSetExpiry(robj *o, unsigned char *p, mstime_t current, mstime_t expiry) {
    unsigned char *lp = objectGetVal(o);
    unsigned char *metadata_ptr = lpGetMetadata(lp, p);
    serverAssert((current != EXPIRY_NONE) == (metadata_ptr != NULL));

    if (expiry == EXPIRY_NONE) {
        lp = lpRemoveMetadata(lp, metadata_ptr);
        objectSetVal(o, lp);
        setTypeUpdateVolatileCount(o, -1);
        return;
    }

    unsigned char intenc[LP_MAX_INT_ENCODING_LEN];
    uint64_t enclen;
    lpEncodeIntegerGetType(expiry, intenc, &enclen);
    if (metadata_ptr) {
        /* Refresh in place; LP_REPLACE re-encodes the integer width. */
        lp = lpInsertMetadata(lp, intenc, enclen, metadata_ptr, LP_REPLACE, NULL);
        objectSetVal(o, lp);
    } else {
        lp = lpInsertMetadata(lp, intenc, enclen, p, LP_AFTER, NULL);
        objectSetVal(o, lp);
        setTypeUpdateVolatileCount(o, 1);
    }
}

int setTypeSetExpiry(robj *o, sds member, mstime_t expiry, int flags) {
    /* A missing key reports every member as missing, as hashTypeSetExpire does. */
    if (o == NULL) return SET_EXPIRY_NOT_EXIST;
    serverAssert(objectGetType(o) == OBJ_SET);

    /* An intset holds no metadata. Resolve everything that does not need the
     * new layout before paying for the conversion. */
    if (objectGetEncoding(o) == OBJ_ENCODING_INTSET) {
        if (!setTypeIsMember(o, member)) return SET_EXPIRY_NOT_EXIST;
        if (!setTypeExpiryConditionsMet(EXPIRY_NONE, expiry, flags)) return SET_EXPIRY_CONDITION_NOT_MET;
        /* No member of an intset has an expiry, so there is none to remove. */
        if (expiry == EXPIRY_NONE) return SET_EXPIRY_FAILED;
        if (checkAlreadyExpired(expiry)) {
            serverAssert(setTypeRemove(o, member));
            return SET_EXPIRY_DELETED;
        }
        setTypeConvertForExpiry(o, 0, sdslen(member));
    }

    if (objectGetEncoding(o) == OBJ_ENCODING_LISTPACK) {
        unsigned char *lp = objectGetVal(o);
        unsigned char *p = setTypeListpackFind(lp, member);
        if (p == NULL) return SET_EXPIRY_NOT_EXIST;
        mstime_t current = setTypeListpackGetExpiry(lp, p);
        /* A lazily-expired member is logically gone: report it as missing
         * instead of resurrecting it, and leave its removal (and the SREM that
         * propagates it) to active expiration. */
        if (!setTypeListpackMemberIsValid(current)) return SET_EXPIRY_NOT_EXIST;

        if (!setTypeExpiryConditionsMet(current, expiry, flags)) return SET_EXPIRY_CONDITION_NOT_MET;
        if (expiry == EXPIRY_NONE && current == EXPIRY_NONE) return SET_EXPIRY_FAILED;
        if (expiry != EXPIRY_NONE && checkAlreadyExpired(expiry)) {
            bool was_volatile = current != EXPIRY_NONE;
            lp = lpDeleteRangeWithEntry(lp, &p, 1);
            objectSetVal(o, lp);
            if (was_volatile) setTypeUpdateVolatileCount(o, -1);
            return SET_EXPIRY_DELETED;
        }
        /* Nothing has mutated the listpack since the lookup, so 'p' is live. */
        setTypeListpackSetExpiry(o, p, current, expiry);
        return SET_EXPIRY_SET;
    }

    serverAssert(objectGetEncoding(o) == OBJ_ENCODING_HASHTABLE);
    /* hashtableFindRef applies the volatile type's validateEntry callback, so
     * an expired member reports NOT_EXIST. The ignore-TTL bracket is
     * deliberately NOT used here: S*EXPIRE must not resurrect one. */
    void **member_ref = hashtableFindRef(objectGetVal(o), member);
    if (member_ref == NULL) return SET_EXPIRY_NOT_EXIST;
    smember *m = *member_ref;
    mstime_t current = smemberGetExpiry(m);

    if (!setTypeExpiryConditionsMet(current, expiry, flags)) return SET_EXPIRY_CONDITION_NOT_MET;
    if (expiry == EXPIRY_NONE && current == EXPIRY_NONE) return SET_EXPIRY_FAILED;

    if (expiry != EXPIRY_NONE && checkAlreadyExpired(expiry)) {
        void *popped = NULL;
        serverAssert(hashtablePop(objectGetVal(o), member, &popped));
        serverAssert(popped == m);
        setTypeUntrackMember(o, m);
        smemberFree(m);
        setTypeMaybeFreeVolatileSet(o);
        return SET_EXPIRY_DELETED;
    }

    if (expiry == EXPIRY_NONE) {
        /* Persist: untrack, then rebuild the member without the prefix so the
         * zero-cost layout is restored. Only once no prefixed member is left
         * may the set move back to the plain type. */
        setTypeUntrackMember(o, m);
        *member_ref = smemberSetExpiry(m, EXPIRY_NONE);
        setTypeMaybeFreeVolatileSet(o);
        return SET_EXPIRY_SET;
    }

    /* The type swap does not move the hashtable struct or its bucket tables,
     * so 'member_ref' survives it. */
    vset *set = setTypeGetOrCreateVolatileSet(o);
    smember *updated = smemberSetExpiry(m, expiry);
    *member_ref = updated;
    if (current == EXPIRY_NONE) {
        serverAssert(vsetAddEntry(set, smemberGetExpiryVsetFunc, updated));
    } else {
        serverAssert(vsetUpdateEntry(set, smemberGetExpiryVsetFunc, m, updated, current, expiry));
    }
    return SET_EXPIRY_SET;
}

/*-----------------------------------------------------------------------------
 * Adding a member with an expiry
 *----------------------------------------------------------------------------*/

/* Add 'member', known to be absent, to a listpack- or hashtable-encoded set,
 * carrying 'expiry'. Converts the encoding when the listpack thresholds are
 * exceeded. */
static void setTypeAddNewWithExpiry(robj *o, sds member, mstime_t expiry) {
    size_t len = sdslen(member);

    if (objectGetEncoding(o) == OBJ_ENCODING_INTSET) setTypeConvertForExpiry(o, 1, len);

    if (objectGetEncoding(o) == OBJ_ENCODING_LISTPACK) {
        unsigned char *lp = objectGetVal(o);
        /* The member and its metadata entry are appended together, so the
         * safe-to-add budget must cover both. */
        if (setTypeListpackFits(lpLength(lp) + 1, len) && lpSafeToAdd(lp, len + LP_METADATA_MAX_ENTRY_BYTES)) {
            lp = lpAppend(lp, (unsigned char *)member, len);
            objectSetVal(o, lp);
            /* The appended member is the last physical entry, and it has no
             * metadata yet, so lpLast() addresses it. */
            if (expiry != EXPIRY_NONE) setTypeListpackSetExpiry(o, lpLast(lp), EXPIRY_NONE, expiry);
            return;
        }
        setTypeConvertAndExpand(o, OBJ_ENCODING_HASHTABLE, lpLength(lp) + 1, 1);
    }

    serverAssert(objectGetEncoding(o) == OBJ_ENCODING_HASHTABLE);
    if (expiry == EXPIRY_NONE) {
        /* A plain member: smemberCreate() returns an unprefixed sds, which is
         * what the plain hashtable type stores and what the volatile type reads
         * through the aux bit, so either type is fine here. Replacing the last
         * expired member may already have dropped the volatile state. */
        serverAssert(hashtableAdd(objectGetVal(o), smemberCreate(member, len, EXPIRY_NONE)));
        return;
    }
    /* The member is absent from the bucket tables, so no pointer needs to be
     * reloaded here: the type swap that setTypeGetOrCreateVolatileSet may
     * perform does not move the hashtable struct. */
    vset *set = setTypeGetOrCreateVolatileSet(o);
    smember *m = smemberCreate(member, len, expiry);
    serverAssert(hashtableAdd(objectGetVal(o), m));
    serverAssert(vsetAddEntry(set, smemberGetExpiryVsetFunc, m));
}

int setTypeAddWithExpiry(robj *o, sds member, mstime_t expiry, bool *replaced_expired) {
    if (replaced_expired) *replaced_expired = false;
    serverAssert(o != NULL && objectGetType(o) == OBJ_SET);

    /* Fast path: an untouched set adding a member without a TTL is the plain
     * SADD. */
    bool volatile_set = setTypeHasVolatileMembers(o);
    if (!volatile_set && expiry == EXPIRY_NONE) return setTypeAdd(o, member);

    /* A live member wins: neither the member nor its TTL is changed. The lookup
     * applies the expired-member filter, so an expired one is not seen here. */
    if (setTypeGetExpiry(o, member, NULL) == C_OK) return 0;

    if (volatile_set) {
        /* The member may still be present as an expired one. Make it
         * visible, drop it, and tell the caller so it can propagate the SREM
         * and emit the notification. */
        setTypeIgnoreTTL(o, true);
        int removed = setTypeRemoveFromVolatileSet(o, member);
        setTypeIgnoreTTL(o, false);
        if (removed && replaced_expired) *replaced_expired = true;
    }

    if (expiry == EXPIRY_NONE) {
        /* Reaching here with no expiry means the set WAS volatile on entry
         * (an untouched set returned above), so the plain setTypeAdd() path
         * would delegate straight back here and recurse. */
        serverAssert(volatile_set);
        setTypeAddNewWithExpiry(o, member, EXPIRY_NONE);
    } else {
        setTypeAddNewWithExpiry(o, member, expiry);
    }
    return 1;
}

/*-----------------------------------------------------------------------------
 * Reaping expired members
 *----------------------------------------------------------------------------*/

typedef struct {
    robj *o;         /* the set being expired */
    robj **members;  /* caller's output array, or NULL */
    size_t nmembers; /* members written to 'members' */
} setExpiryContext;

/* A vsetExpiryFunc: drop one expired member from the hashtable. Runs inside an
 * ignore-TTL bracket so the pop can see it. */
static int setTypeExpireMember(void *entry, void *c) {
    setExpiryContext *ctx = c;
    smember *m = entry;
    serverAssert(objectGetEncoding(ctx->o) == OBJ_ENCODING_HASHTABLE);
    void *popped = NULL;
    /* The member pointer is its own lookup key: an smember IS an sds. */
    serverAssert(hashtablePop(objectGetVal(ctx->o), m, &popped));
    if (ctx->members) ctx->members[ctx->nmembers++] = createStringObject(m, sdslen((sds)m));
    smemberFree(popped);
    return 1;
}

size_t setTypeDeleteExpiredMembers(robj *o, mstime_t now, unsigned long max, robj **out) {
    serverAssert(o != NULL && objectGetType(o) == OBJ_SET);
    if (max == 0 || !setTypeHasVolatileMembers(o)) return 0;

    if (objectGetEncoding(o) == OBJ_ENCODING_LISTPACK) {
        unsigned char *lp = objectGetVal(o);
        unsigned char *p = lpFirst(lp);
        size_t expired = 0;
        unsigned char intbuf[LP_INTBUF_SIZE];

        while (p != NULL && expired < max) {
            int64_t len;
            unsigned char *str = lpGet(p, &len, intbuf);
            mstime_t expiry = setTypeListpackGetExpiry(lp, p);
            if (expiry != EXPIRY_NONE && expiry <= now) {
                /* 'str' may point into intbuf, so copy it out before the
                 * listpack is mutated under us. */
                if (out) out[expired] = createStringObject((char *)str, len);
                /* Deletes the member and its trailing metadata, and leaves 'p'
                 * on the entry that followed the range (NULL at EOF), so the
                 * walk stays linear instead of restarting. */
                lp = lpDeleteRangeWithEntry(lp, &p, 1);
                objectSetVal(o, lp);
                expired++;
                continue;
            }
            p = lpNext(lp, p);
        }

        /* Update the aggregate header once: doing it per deletion would
         * reallocate the listpack under the scan cursor. */
        setTypeUpdateVolatileCount(o, -(long)expired);
        return expired;
    }

    serverAssert(objectGetEncoding(o) == OBJ_ENCODING_HASHTABLE);
    vset *set = setTypeGetVolatileSet(o);
    if (set == NULL) return 0;
    serverAssert(!vsetIsEmpty(set));

    /* Skip TTL validation for the duration of the walk so the pops can see the
     * members they are removing. */
    setTypeIgnoreTTL(o, true);
    setExpiryContext ctx = {.o = o, .members = out, .nmembers = 0};
    size_t expired = vsetRemoveExpired(set, smemberGetExpiryVsetFunc, setTypeExpireMember, now, max, &ctx);
    serverAssert(ctx.nmembers <= max);
    /* Safe now that vsetRemoveExpired has returned: no vset walk is live, and
     * every prefixed member it held has been freed. */
    if (vsetIsEmpty(set)) setTypeFreeVolatileSet(o);
    setTypeIgnoreTTL(o, false);
    return expired;
}

/*-----------------------------------------------------------------------------
 * Defrag
 *----------------------------------------------------------------------------*/

static smember *setTypeDefragMember(robj *o, smember *m, void *(*defragfn)(void *)) {
    serverAssert(objectGetEncoding(o) == OBJ_ENCODING_HASHTABLE);
    smember *new_m = smemberDefrag(m, defragfn);
    /* Not moved: the hashtable bucket and the vset both still hold a valid
     * pointer, so there is nothing to re-point. */
    if (new_m == NULL) return NULL;
    if (smemberHasExpiry(new_m)) {
        /* The vset indexes volatile members BY POINTER, so it has to be
         * re-pointed here. 'm' is already freed and MUST NOT be dereferenced;
         * vsetUpdateEntry treats it as an opaque identity and takes the old
         * expiry from its argument instead. */
        mstime_t expiry = smemberGetExpiry(new_m);
        vset *set = setTypeGetVolatileSet(o);
        serverAssert(set != NULL);
        serverAssert(vsetUpdateEntry(set, smemberGetExpiryVsetFunc, m, new_m, expiry, expiry));
    }
    return new_m;
}

/* privdata of defragSetMemberCallback: what setTypeDefragMember needs. */
typedef struct {
    robj *o;
    void *(*defragfn)(void *);
} setDefragMemberCtx;

/* Hashtable scan callback (HASHTABLE_SCAN_EMIT_REF) for the members of a
 * volatile set: move the member and write the new pointer back into the bucket
 * slot. Mirrors defragHashTypeEntry in t_hash.c. */
static void defragSetMemberCallback(void *privdata, void *element_ref) {
    setDefragMemberCtx *ctx = (setDefragMemberCtx *)privdata;
    smember **member_ref = (smember **)element_ref;
    smember *new_member = setTypeDefragMember(ctx->o, *member_ref, ctx->defragfn);
    if (new_member) *member_ref = new_member;
}

size_t setTypeScanDefrag(robj *o, size_t cursor, void *(*defragfn)(void *)) {
    serverAssert(o != NULL && objectGetType(o) == OBJ_SET);

    /* Two cursor phases, as hashTypeScanDefrag: phase 1 walks the hashtable
     * and moves the members (re-pointing the vset for each volatile one that
     * moves), phase 2 walks the vset's own allocations. Only one object is
     * defragged at a time, so one file-scope state is enough. */
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
        /* Members done. A set without volatile members has no vset to walk. */
        if (st->cursor == 0) {
            if (setTypeGetVolatileSet(o) == NULL) return 0;
            st->in_vset_phase = true;
        }
    } else {
        vset *set = setTypeGetVolatileSet(o);
        serverAssert(set != NULL);
        st->cursor = vsetScanDefrag(set, st->cursor, defragfn);
        /* We're done with this object. */
        if (st->cursor == 0) return 0;
    }
    return (size_t)st;
}
