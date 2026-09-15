/*
 * Copyright (c) 2009-2012, Redis Ltd.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "server.h"
#include "hashtable.h"
#include "intset.h" /* Compact integer set structure */
#include "smember.h"

/*-----------------------------------------------------------------------------
 * Set Commands
 *----------------------------------------------------------------------------*/

void sunionDiffGenericCommand(client *c, robj **setkeys, int setnum, robj *dstkey, int op);

/*-----------------------------------------------------------------------------
 * Member TTL support for the setType* API
 *----------------------------------------------------------------------------*
 *
 * The volatile state itself is owned by t_set_volatile.c; this file only makes
 * the type API honour it. A set that never received a TTL must run the
 * instruction stream it ran before member TTLs existed, so every function below
 * pays at most one O(1) setTypeHasVolatileMembers() test and never a per-member
 * one. */

/* True when the listpack member at 'p' is visible in the current execution
 * context. An expired member is invisible, except inside an
 * ignore-TTL bracket or under POLICY_IGNORE_EXPIRE. */
static inline bool setTypeListpackIsValidAt(unsigned char *lp, unsigned char *p) {
    return setTypeListpackMemberIsValid(setTypeListpackGetExpiry(lp, p));
}

/* Factory method to return a set that *can* hold "value". When the object has
 * an integer-encodable value, an intset will be returned. Otherwise, a listpack
 * or a regular hash table.
 *
 * The size hint indicates approximately how many items will be added which is
 * used to determine the initial representation. */
robj *setTypeCreate(sds value, size_t size_hint) {
    if (isSdsRepresentableAsLongLong(value, NULL) == C_OK && size_hint <= server.set_max_intset_entries)
        return createIntsetObject();
    if (size_hint <= server.set_max_listpack_entries) return createSetListpackObject();

    /* We may oversize the set by using the hint if the hint is not accurate,
     * but we will assume this is acceptable to maximize performance. */
    robj *o = createSetObject();
    hashtableExpand(objectGetVal(o), size_hint);
    return o;
}

/* Check if the existing set should be converted to another encoding based off the
 * the size hint. */
void setTypeMaybeConvert(robj *set, size_t size_hint) {
    if ((set->encoding == OBJ_ENCODING_LISTPACK && size_hint > server.set_max_listpack_entries) ||
        (set->encoding == OBJ_ENCODING_INTSET && size_hint > server.set_max_intset_entries)) {
        setTypeConvertAndExpand(set, OBJ_ENCODING_HASHTABLE, size_hint, 1);
    }
}

/* Return the maximum number of entries to store in an intset. */
static size_t intsetMaxEntries(void) {
    size_t max_entries = server.set_max_intset_entries;
    /* limit to 1G entries due to intset internals. */
    if (max_entries >= 1 << 30) max_entries = 1 << 30;
    return max_entries;
}

/* Converts intset to HT if it contains too many entries. */
static void maybeConvertIntset(robj *subject) {
    serverAssert(subject->encoding == OBJ_ENCODING_INTSET);
    if (intsetLen(objectGetVal(subject)) > intsetMaxEntries()) setTypeConvert(subject, OBJ_ENCODING_HASHTABLE);
}

/* When you know all set elements are integers, call this to convert the set to
 * an intset. No conversion happens if the set contains too many entries for an
 * intset. */
static void maybeConvertToIntset(robj *set) {
    if (set->encoding == OBJ_ENCODING_INTSET) return; /* already intset */
    /* An intset has nowhere to store a member expiry, so a set with volatile
     * members must keep its current encoding. */
    if (setTypeHasVolatileMembers(set)) return;
    if (setTypeSize(set) > intsetMaxEntries()) return; /* can't use intset */
    intset *is = intsetNew();
    char *str;
    size_t len;
    int64_t llval;
    setTypeIterator *si = setTypeInitIterator(set);
    while (setTypeNext(si, &str, &len, &llval) != -1) {
        if (str) {
            /* If the element is returned as a string, we may be able to convert
             * it to integer. This happens for OBJ_ENCODING_HASHTABLE. */
            serverAssert(string2ll(str, len, (long long *)&llval));
        }
        uint8_t success = 0;
        is = intsetAdd(is, llval, &success);
        serverAssert(success);
    }
    setTypeReleaseIterator(si);
    freeSetObject(set); /* frees the internals but not robj itself */
    objectSetVal(set, is);
    set->encoding = OBJ_ENCODING_INTSET;
}

/* Add the specified sds value into a set.
 *
 * If the value was already member of the set, nothing is done and 0 is
 * returned, otherwise the new element is added and 1 is returned. */
int setTypeAdd(robj *subject, sds value) {
    return setTypeAddAux(subject, value, sdslen(value), 0, 1);
}

/* Add member. This function is optimized for the different encodings. The
 * value can be provided as an sds string (indicated by passing str_is_sds =
 * 1), as string and length (str_is_sds = 0) or as an integer in which case str
 * is set to NULL and llval is provided instead.
 *
 * The set must not have volatile members: SADD, SADDEX and SMOVE call
 * setTypeAddWithExpiry() so that they can propagate the SREM for an expired
 * member they replace, and every other caller fills a set it just created.
 *
 * Returns 1 if the value was added and 0 if it was already a member. */
int setTypeAddAux(robj *set, char *str, size_t len, int64_t llval, int str_is_sds) {
    serverAssert(!setTypeHasVolatileMembers(set));
    char tmpbuf[LONG_STR_SIZE];
    if (!str) {
        if (set->encoding == OBJ_ENCODING_INTSET) {
            uint8_t success = 0;
            objectSetVal(set, intsetAdd(objectGetVal(set), llval, &success));
            if (success) maybeConvertIntset(set);
            return success;
        }
        /* Convert int to string. */
        len = ll2string(tmpbuf, sizeof tmpbuf, llval);
        str = tmpbuf;
        str_is_sds = 0;
    }

    serverAssert(str);
    if (set->encoding == OBJ_ENCODING_HASHTABLE) {
        /* Avoid duping the string if it is an sds string. */
        sds sdsval = str_is_sds ? (sds)str : sdsnewlen(str, len);
        hashtable *ht = objectGetVal(set);
        hashtablePosition position;
        if (hashtableFindPositionForInsert(ht, sdsval, &position, NULL)) {
            /* Key doesn't already exist in the set. Add it but dup the key. */
            if (sdsval == str) sdsval = sdsdup(sdsval);
            hashtableInsertAtPosition(ht, sdsval, &position);
            return 1;
        } else if (sdsval != str) {
            /* String is already a member. Free our temporary sds copy. */
            sdsfree(sdsval);
            return 0;
        }
    } else if (set->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *lp = objectGetVal(set);
        unsigned char *p = lpFirst(lp);
        if (p != NULL) p = lpFind(lp, p, (unsigned char *)str, len, 0);
        if (p == NULL) {
            /* Not found.  */
            if (lpLength(lp) < server.set_max_listpack_entries && len <= server.set_max_listpack_value &&
                lpSafeToAdd(lp, len)) {
                if (str == tmpbuf) {
                    /* This came in as integer so we can avoid parsing it again.
                     * TODO: Create and use lpFindInteger; don't go via string. */
                    lp = lpAppendInteger(lp, llval);
                } else {
                    lp = lpAppend(lp, (unsigned char *)str, len);
                }
                objectSetVal(set, lp);
            } else {
                /* Size limit is reached. Convert to hashtable and add. The
                 * conversion carries the member expiries over. */
                setTypeConvertAndExpand(set, OBJ_ENCODING_HASHTABLE, lpLength(lp) + 1, 1);
                serverAssert(hashtableAdd(objectGetVal(set), sdsnewlen(str, len)));
            }
            return 1;
        }
    } else if (set->encoding == OBJ_ENCODING_INTSET) {
        long long value;
        if (string2ll(str, len, &value)) {
            uint8_t success = 0;
            objectSetVal(set, intsetAdd(objectGetVal(set), value, &success));
            if (success) {
                maybeConvertIntset(set);
                return 1;
            }
        } else {
            /* Check if listpack encoding is safe not to cross any threshold. */
            size_t maxelelen = 0, totsize = 0;
            unsigned long n = intsetLen(objectGetVal(set));
            if (n != 0) {
                size_t elelen1 = sdigits10(intsetMax(objectGetVal(set)));
                size_t elelen2 = sdigits10(intsetMin(objectGetVal(set)));
                maxelelen = max(elelen1, elelen2);
                size_t s1 = lpEstimateBytesRepeatedInteger(intsetMax(objectGetVal(set)), n);
                size_t s2 = lpEstimateBytesRepeatedInteger(intsetMin(objectGetVal(set)), n);
                totsize = max(s1, s2);
            }
            if (intsetLen((const intset *)objectGetVal(set)) < server.set_max_listpack_entries &&
                len <= server.set_max_listpack_value && maxelelen <= server.set_max_listpack_value &&
                lpSafeToAdd(NULL, totsize + len)) {
                /* In the "safe to add" check above we assumed all elements in
                 * the intset are of size maxelelen. This is an upper bound. */
                setTypeConvertAndExpand(set, OBJ_ENCODING_LISTPACK, intsetLen(objectGetVal(set)) + 1, 1);
                unsigned char *lp = objectGetVal(set);
                lp = lpAppend(lp, (unsigned char *)str, len);
                lp = lpShrinkToFit(lp);
                objectSetVal(set, lp);
                return 1;
            } else {
                setTypeConvertAndExpand(set, OBJ_ENCODING_HASHTABLE, intsetLen(objectGetVal(set)) + 1, 1);
                /* The set *was* an intset and this value is not integer
                 * encodable, so hashtableAdd should always work. */
                serverAssert(hashtableAdd(objectGetVal(set), sdsnewlen(str, len)));
                return 1;
            }
        }
    } else {
        serverPanic("Unknown set encoding");
    }
    return 0;
}

/* Deletes a value provided as an sds string from the set. Returns 1 if the
 * value was deleted and 0 if it was not a member of the set. */
int setTypeRemove(robj *setobj, sds value) {
    return setTypeRemoveAux(setobj, value, sdslen(value), 0, 1);
}

/* Remove a member. This function is optimized for the different encodings. The
 * value can be provided as an sds string (indicated by passing str_is_sds =
 * 1), as string and length (str_is_sds = 0) or as an integer in which case str
 * is set to NULL and llval is provided instead.
 *
 * Returns 1 if the value was deleted and 0 if it was not a member of the set. */
int setTypeRemoveAux(robj *setobj, char *str, size_t len, int64_t llval, int str_is_sds) {
    bool volatile_set = setTypeHasVolatileMembers(setobj);
    char tmpbuf[LONG_STR_SIZE];
    if (!str) {
        if (setobj->encoding == OBJ_ENCODING_INTSET) {
            int success;
            objectSetVal(setobj, intsetRemove(objectGetVal(setobj), llval, &success));
            return success;
        }
        len = ll2string(tmpbuf, sizeof tmpbuf, llval);
        str = tmpbuf;
        str_is_sds = 0;
    }

    if (setobj->encoding == OBJ_ENCODING_HASHTABLE) {
        if (volatile_set) {
            /* setTypeRemoveVolatile removes LIVE members only (an expired one is
             * invisible and only active expiration may take it), untracks the vset
             * entry, frees with smemberFree and drops the volatile type when the
             * last volatile member goes away. */
            sds member = str_is_sds ? (sds)str : sdsnewlen(str, len);
            int deleted = setTypeRemoveVolatile(setobj, member);
            if (member != str) sdsfree(member);
            return deleted;
        }
        sds sdsval = str_is_sds ? (sds)str : sdsnewlen(str, len);
        int deleted = hashtableDelete(objectGetVal(setobj), sdsval);
        if (sdsval != str) sdsfree(sdsval); /* free temp copy */
        return deleted;
    } else if (setobj->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *lp = objectGetVal(setobj);
        unsigned char *p = lpFirst(lp);
        if (p == NULL) return 0;
        p = lpFind(lp, p, (unsigned char *)str, len, 0);
        if (p != NULL) {
            if (volatile_set) {
                long long expiry = setTypeListpackGetExpiry(lp, p);
                /* SREM of an expired member returns 0 and does NOT delete: the
                 * member is invisible and only active expiration may remove it, so that
                 * the deletion is propagated exactly once. */
                if (!setTypeListpackMemberIsValid(expiry)) return 0;
                /* lpDeleteRangeWithEntry consumes the trailing metadata entry
                 * along with the member; plain lpDelete would orphan it. */
                lp = lpDeleteRangeWithEntry(lp, &p, 1);
                objectSetVal(setobj, lp);
                if (expiry != EXPIRY_NONE) setTypeUpdateVolatileCount(setobj, -1);
                return 1;
            }
            lp = lpDelete(lp, p, NULL);
            objectSetVal(setobj, lp);
            return 1;
        }
    } else if (setobj->encoding == OBJ_ENCODING_INTSET) {
        long long llval;
        if (string2ll(str, len, &llval)) {
            int success;
            objectSetVal(setobj, intsetRemove(objectGetVal(setobj), llval, &success));
            if (success) return 1;
        }
    } else {
        serverPanic("Unknown set encoding");
    }
    return 0;
}

/* Check if an sds string is a member of the set. Returns 1 if the value is a
 * member of the set and 0 if it isn't. */
int setTypeIsMember(robj *subject, sds value) {
    return setTypeIsMemberAux(subject, value, sdslen(value), 0, 1);
}

/* Membership checking optimized for the different encodings. The value can be
 * provided as an sds string (indicated by passing str_is_sds = 1), as string
 * and length (str_is_sds = 0) or as an integer in which case str is set to NULL
 * and llval is provided instead.
 *
 * Returns 1 if the value is a member of the set and 0 if it isn't. */
int setTypeIsMemberAux(robj *set, char *str, size_t len, int64_t llval, int str_is_sds) {
    char tmpbuf[LONG_STR_SIZE];
    if (!str) {
        if (set->encoding == OBJ_ENCODING_INTSET) return intsetFind(objectGetVal(set), llval);
        len = ll2string(tmpbuf, sizeof tmpbuf, llval);
        str = tmpbuf;
        str_is_sds = 0;
    }

    if (set->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *lp = objectGetVal(set);
        unsigned char *p = lpFirst(lp);
        if (p == NULL) return 0;
        p = lpFind(lp, p, (unsigned char *)str, len, 0);
        if (p == NULL) return 0;
        /* One leading-header test; only a volatile set pays the metadata read
         * needed to hide expired members. */
        return !setTypeHasVolatileMembers(set) || setTypeListpackIsValidAt(lp, p);
    } else if (set->encoding == OBJ_ENCODING_INTSET) {
        long long llval;
        return string2ll(str, len, &llval) && intsetFind(objectGetVal(set), llval);
    } else if (set->encoding == OBJ_ENCODING_HASHTABLE && str_is_sds) {
        /* No new test: the volatile hashtable type filters expired members in
         * its validateEntry callback, and the plain type has none. */
        return hashtableFind(objectGetVal(set), (sds)str, NULL);
    } else if (set->encoding == OBJ_ENCODING_HASHTABLE) {
        sds sdsval = sdsnewlen(str, len);
        int result = hashtableFind(objectGetVal(set), sdsval, NULL);
        sdsfree(sdsval);
        return result;
    } else {
        serverPanic("Unknown set encoding");
    }
}

setTypeIterator *setTypeInitIterator(robj *subject) {
    setTypeIterator *si = zmalloc(sizeof(setTypeIterator));
    si->subject = subject;
    si->encoding = subject->encoding;
    si->lp_has_volatile = false;
    if (si->encoding == OBJ_ENCODING_HASHTABLE) {
        si->hashtable_iterator = hashtableCreateIterator(objectGetVal(subject), 0);
    } else if (si->encoding == OBJ_ENCODING_INTSET) {
        si->ii = 0;
    } else if (si->encoding == OBJ_ENCODING_LISTPACK) {
        si->lpi = NULL;
        si->lp_has_volatile = setTypeHasVolatileMembers(subject);
    } else {
        serverPanic("Unknown set encoding");
    }
    return si;
}

void setTypeReleaseIterator(setTypeIterator *si) {
    if (si->encoding == OBJ_ENCODING_HASHTABLE) hashtableReleaseIterator(si->hashtable_iterator);
    zfree(si);
}

/* Move to the next entry in the set. Returns the object at the current
 * position, as a string or as an integer.
 *
 * Since set elements can be internally be stored as SDS strings, char buffers or
 * simple arrays of integers, setTypeNext returns the encoding of the
 * set object you are iterating, and will populate the appropriate pointers
 * (str and len) or (llele) depending on whether the value is stored as a string
 * or as an integer internally.
 *
 * If OBJ_ENCODING_HASHTABLE is returned, then str points to an sds string and can be
 * used as such. If OBJ_ENCODING_INTSET, then llele is populated and str is
 * pointed to NULL. If OBJ_ENCODING_LISTPACK is returned, the value can be
 * either a string or an integer. If *str is not NULL, then str and len are
 * populated with the string content and length. Otherwise, llele populated with
 * an integer value.
 *
 * Note that str, len and llele pointers should all be passed and cannot
 * be NULL since the function will try to defensively populate the non
 * used field with values which are easy to trap if misused.
 *
 * When there are no more elements -1 is returned. */
int setTypeNext(setTypeIterator *si, char **str, size_t *len, int64_t *llele) {
    if (si->encoding == OBJ_ENCODING_HASHTABLE) {
        void *next;
        /* No new test: on a volatile set the hashtable iterator filters
         * expired members through the type's validateEntry callback. */
        if (!hashtableNext(si->hashtable_iterator, &next)) return -1;
        *str = next;
        *len = sdslen(*str);
        *llele = -123456789; /* Not needed. Defensive. */
    } else if (si->encoding == OBJ_ENCODING_INTSET) {
        if (!intsetGet(objectGetVal(si->subject), si->ii++, llele)) return -1;
        *str = NULL;
    } else if (si->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *lp = objectGetVal(si->subject);
        unsigned char *lpi = si->lpi;
        lpi = (lpi == NULL) ? lpFirst(lp) : lpNext(lp, lpi);
        if (si->lp_has_volatile) {
            /* Expired members are invisible to iteration. */
            while (lpi != NULL && !setTypeListpackIsValidAt(lp, lpi)) lpi = lpNext(lp, lpi);
        }
        if (lpi == NULL) return -1;
        si->lpi = lpi;
        unsigned int l;
        *str = (char *)lpGetValue(lpi, &l, (long long *)llele);
        *len = (size_t)l;
    } else {
        serverPanic("Wrong set encoding in setTypeNext");
    }
    return si->encoding;
}

/* The not copy on write friendly version but easy to use version
 * of setTypeNext() is setTypeNextObject(), returning new SDS
 * strings. So if you don't retain a pointer to this object you should call
 * sdsfree() against it.
 *
 * This function is the way to go for write operations where COW is not
 * an issue. */
sds setTypeNextObject(setTypeIterator *si) {
    int64_t intele;
    char *str;
    size_t len;

    if (setTypeNext(si, &str, &len, &intele) == -1) return NULL;
    if (str != NULL) return sdsnewlen(str, len);
    return sdsfromlonglong(intele);
}

mstime_t setTypeIteratorExpiry(setTypeIterator *si, const char *str) {
    switch (si->encoding) {
    case OBJ_ENCODING_HASHTABLE: return smemberGetExpiry((const smember *)str);
    case OBJ_ENCODING_LISTPACK: return si->lp_has_volatile ? setTypeListpackGetExpiry(objectGetVal(si->subject), si->lpi) : EXPIRY_NONE;
    default: return EXPIRY_NONE;
    }
}

/* One live member of a set that carries member TTLs, drawn uniformly.
 *
 * Fair random picks with expired members rejected are uniform over the live
 * ones and cost O(1) as long as the expired ones are not dense. When too many
 * picks in a row land on an expired member the draw falls back to reservoir
 * sampling over a single pass of the live members: still uniform, but O(n) in
 * the set size, which is the price of a set that is mostly expired members.
 *
 * Returns the encoding, or -1 when the set holds no live member. */
static int setTypeRandomVolatileElement(robj *setobj, char **str, size_t *len, int64_t *llele) {
    int found = 0;

    if (setobj->encoding == OBJ_ENCODING_HASHTABLE) {
        /* The volatile type's validateEntry hides expired members from the
         * sampler, so without the bracket a set dense with them would spin.
         * Inside it we do the rejecting ourselves against the raw expiry. */
        setTypeIgnoreTTL(setobj, true);
        for (int tries = 0; tries < 100; tries++) {
            void *entry = NULL;
            if (!hashtableFairRandomEntry(objectGetVal(setobj), &entry)) break;
            if (smemberIsExpired(entry)) continue;
            *str = entry;
            *len = sdslen((sds)entry);
            *llele = -123456789; /* Not needed. Defensive. */
            found = 1;
            break;
        }
        setTypeIgnoreTTL(setobj, false);
    } else if (setobj->encoding == OBJ_ENCODING_LISTPACK) {
        /* No bracket here: lpSeek/lpGetValue never consult member validity, so
         * the sampler already sees expired members, and an ignore bracket would
         * instead make setTypeListpackMemberIsValid() accept them. */
        unsigned char *lp = objectGetVal(setobj);
        unsigned long total = lpLength(lp);
        for (int tries = 0; total > 0 && tries < 100; tries++) {
            unsigned char *p = lpSeek(lp, rand() % total);
            if (p == NULL) break;
            if (!setTypeListpackIsValidAt(lp, p)) continue;
            unsigned int l;
            *str = (char *)lpGetValue(p, &l, (long long *)llele);
            *len = (size_t)l;
            found = 1;
            break;
        }
    } else {
        serverPanic("Unknown set encoding");
    }
    if (found) return setobj->encoding;

    /* Sampling defeated by dense expired members, or every member is expired.
     * Reservoir-sample the live members, which the iterator already filters
     * for: the previous "return the first live member" fallback was not
     * uniform. */
    int encoding = -1;
    unsigned long seen = 0;
    char *s;
    size_t l;
    int64_t ll;
    int enc;
    setTypeIterator *si = setTypeInitIterator(setobj);
    while ((enc = setTypeNext(si, &s, &l, &ll)) != -1) {
        /* Keep the member just seen with probability 1/(seen+1), which leaves
         * every live member equally likely once the pass is over. */
        if (seen == 0 || (unsigned long)rand() % (seen + 1) == 0) {
            *str = s;
            *len = l;
            *llele = ll;
            encoding = enc;
        }
        seen++;
    }
    setTypeReleaseIterator(si);
    return encoding;
}

/* Return random element from a non empty set.
 * The returned element can be an int64_t value if the set is encoded
 * as an "intset" blob of integers, or a string.
 *
 * The caller provides three pointers to be populated with the right
 * object. The return value of the function is the object->encoding
 * field of the object and can be used by the caller to check if the
 * int64_t pointer or the str and len pointers were populated, as for
 * setTypeNext. If OBJ_ENCODING_HASHTABLE is returned, str is pointed to a
 * string which is actually an sds string and it can be used as such.
 *
 * Note that both the str, len and llele pointers should be passed and cannot
 * be NULL. If str is set to NULL, the value is an integer stored in llele.
 *
 * Returns -1 when the set has volatile members and none of them is live. */
int setTypeRandomElement(robj *setobj, char **str, size_t *len, int64_t *llele) {
    if (setobj->encoding == OBJ_ENCODING_HASHTABLE) {
        if (setTypeHasVolatileMembers(setobj)) return setTypeRandomVolatileElement(setobj, str, len, llele);
        void *entry = NULL;
        hashtableFairRandomEntry(objectGetVal(setobj), &entry);
        *str = entry;
        *len = sdslen(*str);
        *llele = -123456789; /* Not needed. Defensive. */
    } else if (setobj->encoding == OBJ_ENCODING_INTSET) {
        /* An intset never carries TTLs, so no test at all here. */
        *llele = intsetRandom(objectGetVal(setobj));
        *str = NULL; /* Not needed. Defensive. */
    } else if (setobj->encoding == OBJ_ENCODING_LISTPACK) {
        if (setTypeHasVolatileMembers(setobj)) return setTypeRandomVolatileElement(setobj, str, len, llele);
        unsigned char *lp = objectGetVal(setobj);
        int r = rand() % lpLength(lp);
        unsigned char *p = lpSeek(lp, r);
        unsigned int l;
        *str = (char *)lpGetValue(p, &l, (long long *)llele);
        *len = (size_t)l;
    } else {
        serverPanic("Unknown set encoding");
    }
    return setobj->encoding;
}

/* Pops a random element and returns it as an object. Returns NULL when the set
 * has volatile members and none of them is live: a set with a non-zero reported
 * size can still yield NULL, so every caller has to handle it. */
robj *setTypePopRandom(robj *set) {
    robj *obj;
    if (set->encoding == OBJ_ENCODING_LISTPACK && !setTypeHasVolatileMembers(set)) {
        /* Find random and delete it without re-seeking the listpack. */
        unsigned int i = 0;
        unsigned char *p = lpNextRandom(objectGetVal(set), lpFirst(objectGetVal(set)), &i, 1, 0);
        unsigned int len = 0; /* initialize to silence warning */
        long long llele = 0;  /* initialize to silence warning */
        char *str = (char *)lpGetValue(p, &len, &llele);
        if (str)
            obj = createStringObject(str, len);
        else
            obj = createStringObjectFromLongLong(llele);
        objectSetVal(set, lpDelete(objectGetVal(set), p, NULL));
    } else {
        /* A volatile listpack comes here too: lpNextRandom would pick expired
         * members, and lpDelete would orphan the picked member's metadata
         * entry. */
        char *str;
        size_t len = 0;
        int64_t llele = 0;
        int encoding = setTypeRandomElement(set, &str, &len, &llele);
        if (encoding == -1) return NULL;
        if (str)
            obj = createStringObject(str, len);
        else
            obj = createStringObjectFromLongLong(llele);
        setTypeRemoveAux(set, str, len, llele, encoding == OBJ_ENCODING_HASHTABLE);
    }
    return obj;
}

unsigned long setTypeSize(const robj *subject) {
    if (subject->encoding == OBJ_ENCODING_HASHTABLE) {
        return hashtableSize((const hashtable *)objectGetVal(subject));
    } else if (subject->encoding == OBJ_ENCODING_INTSET) {
        return intsetLen((const intset *)objectGetVal(subject));
    } else if (subject->encoding == OBJ_ENCODING_LISTPACK) {
        return lpLength((unsigned char *)objectGetVal(subject));
    } else {
        serverPanic("Unknown set encoding");
    }
}

/* Convert the set to specified encoding. The resulting hashtable (when converting
 * to a hash table) is presized to hold the number of elements in the original
 * set. */
void setTypeConvert(robj *setobj, int enc) {
    setTypeConvertAndExpand(setobj, enc, setTypeSize(setobj), 1);
}

/* Snapshot the members of a volatile set together with their expiries, so that
 * they can be rebuilt in another container. Expired members are
 * CARRIED with their expiry, not dropped (see the body for why).
 *
 * Returns the number collected. *members and *expiries are zmalloc'd arrays
 * owned by the caller; every sds in *members must be freed. */
static size_t setTypeCollectMembers(robj *setobj, sds **members, mstime_t **expiries) {
    size_t cap = setTypeSize(setobj); /* counts expired members, so an upper bound */
    sds *m = zmalloc(sizeof(sds) * (cap + 1));
    mstime_t *e = zmalloc(sizeof(mstime_t) * (cap + 1));
    size_t n = 0;
    char *str;
    size_t len;
    int64_t llele;
    int encoding;

    /* Expired members are carried across WITH their expiry rather
     * than dropped: a replica applying the same command sees them as live
     * (POLICY_IGNORE_EXPIRE), so dropping them here without a propagated SREM
     * would leave primary and replica with different SCARD / digests. They
     * stay invisible and active expiration removes and propagates them later. */
    setTypeIgnoreTTL(setobj, true);
    setTypeIterator *si = setTypeInitIterator(setobj);
    while ((encoding = setTypeNext(si, &str, &len, &llele)) != -1) {
        serverAssert(n < cap);
        if (encoding == OBJ_ENCODING_HASHTABLE) {
            /* str IS the smember pointer, so its expiry travels with it. */
            e[n] = smemberGetExpiry(str);
            m[n] = sdsnewlen(str, len);
        } else {
            e[n] = setTypeListpackGetExpiry(objectGetVal(setobj), si->lpi);
            m[n] = str ? sdsnewlen(str, len) : sdsfromlonglong(llele);
        }
        n++;
    }
    setTypeReleaseIterator(si);
    setTypeIgnoreTTL(setobj, false);

    *members = m;
    *expiries = e;
    return n;
}

/* Add a snapshot taken by setTypeCollectMembers() to 'setobj' (which must be an
 * empty container of the target encoding) and release it. setTypeAddWithExpiry
 * owns the per-encoding TTL bookkeeping: listpack metadata entry plus the
 * volatile-count header, or smember plus vset plus the volatile hashtable
 * type. */
static void setTypeAddCollectedMembers(robj *setobj, sds *members, mstime_t *expiries, size_t n) {
    for (size_t i = 0; i < n; i++) {
        setTypeAddWithExpiry(setobj, members[i], expiries[i], NULL);
        sdsfree(members[i]);
    }
    zfree(members);
    zfree(expiries);
}

/* Converts a set to the specified encoding, pre-sizing it for 'cap' elements.
 * The 'panic' argument controls whether to panic on OOM (panic=1) or return
 * C_ERR on OOM (panic=0). If panic=1 is given, this function always returns
 * C_OK. */
int setTypeConvertAndExpand(robj *setobj, int enc, unsigned long cap, int panic) {
    setTypeIterator *si;
    serverAssertWithInfo(NULL, setobj, setobj->type == OBJ_SET && setobj->encoding != enc);

    /* Only listpack and hashtable sets can be volatile (an intset has nowhere
     * to store an expiry), and a volatile set must carry its member expiries
     * over to the new encoding. */
    bool carry_expiry = setobj->encoding != OBJ_ENCODING_INTSET && setTypeHasVolatileMembers(setobj);
    sds *members = NULL;
    mstime_t *expiries = NULL;
    size_t nmembers = 0;

    if (enc == OBJ_ENCODING_HASHTABLE) {
        hashtable *ht = hashtableCreate(&setHashtableType);
        sds element;

        /* Presize the hashtable to avoid rehashing */
        if (panic) {
            hashtableExpand(ht, cap);
        } else if (!hashtableTryExpand(ht, cap)) {
            hashtableRelease(ht);
            return C_ERR;
        }

        if (carry_expiry) {
            /* Snapshot before the source is freed; the members are re-added
             * after the new container is installed. */
            nmembers = setTypeCollectMembers(setobj, &members, &expiries);
        } else {
            /* To add the elements we extract integers and create Objects */
            si = setTypeInitIterator(setobj);
            while ((element = setTypeNextObject(si)) != NULL) {
                serverAssert(hashtableAdd(ht, element));
            }
            setTypeReleaseIterator(si);
        }

        freeSetObject(setobj); /* frees the internals but not setobj itself */
        setobj->encoding = OBJ_ENCODING_HASHTABLE;
        objectSetVal(setobj, ht);
    } else if (enc == OBJ_ENCODING_LISTPACK) {
        /* Preallocate the minimum two bytes per element (enc/value + backlen) */
        size_t estcap = cap * 2;
        if (setobj->encoding == OBJ_ENCODING_INTSET && setTypeSize(setobj) > 0) {
            /* If we're converting from intset, we have a better estimate. */
            size_t s1 = lpEstimateBytesRepeatedInteger(intsetMin(objectGetVal(setobj)), cap);
            size_t s2 = lpEstimateBytesRepeatedInteger(intsetMax(objectGetVal(setobj)), cap);
            estcap = max(s1, s2);
        }
        if (carry_expiry) {
            /* Room for one metadata entry per member plus the aggregate header. */
            estcap += (cap + 1) * LP_METADATA_MAX_ENTRY_BYTES;
        }
        unsigned char *lp = lpNew(estcap);
        char *str;
        size_t len;
        int64_t llele;
        if (carry_expiry) {
            nmembers = setTypeCollectMembers(setobj, &members, &expiries);
        } else {
            si = setTypeInitIterator(setobj);
            while (setTypeNext(si, &str, &len, &llele) != -1) {
                if (str != NULL)
                    lp = lpAppend(lp, (unsigned char *)str, len);
                else
                    lp = lpAppendInteger(lp, llele);
            }
            setTypeReleaseIterator(si);
        }

        freeSetObject(setobj); /* frees the internals but not setobj itself */
        setobj->encoding = OBJ_ENCODING_LISTPACK;
        objectSetVal(setobj, lp);
    } else {
        serverPanic("Unsupported set conversion");
    }

    if (carry_expiry) setTypeAddCollectedMembers(setobj, members, expiries, nmembers);
    return C_OK;
}

/* This is a helper function for the COPY command.
 * Duplicate a set object, with the guarantee that the returned object
 * has the same encoding as the original one.
 *
 * The resulting object always has refcount set to 1
 *
 * Member TTLs are preserved, and so are expired members: a replica
 * applying the same command sees them as live (POLICY_IGNORE_EXPIRE), so
 * dropping them here without a propagated SREM would leave the two sides with
 * different SCARD and digests. They stay invisible and are removed later, on
 * the copy as on the original. The two encodings get there differently:
 * - listpack: the raw memcpy carries the metadata entries and the
 *   volatile-count header verbatim, which is what keeps the "same encoding"
 *   guarantee unconditional.
 * - hashtable: each member is its own allocation and the vset has to be
 *   rebuilt, so the copy is built member by member, expiries included. */
robj *setTypeDup(robj *o) {
    robj *set;
    setTypeIterator *si;

    serverAssert(objectGetType(o) == OBJ_SET);

    /* Create a new set object that have the same encoding as the original object's encoding */
    if (objectGetEncoding(o) == OBJ_ENCODING_INTSET) {
        intset *is = objectGetVal(o);
        size_t size = intsetBlobLen(is);
        intset *newis = zmalloc(size);
        memcpy(newis, is, size);
        set = createObject(OBJ_SET, newis);
        set->encoding = OBJ_ENCODING_INTSET;
    } else if (objectGetEncoding(o) == OBJ_ENCODING_LISTPACK) {
        unsigned char *lp = objectGetVal(o);
        size_t sz = lpBytes(lp);
        unsigned char *new_lp = zmalloc(sz);
        memcpy(new_lp, lp, sz);
        set = createObject(OBJ_SET, new_lp);
        set->encoding = OBJ_ENCODING_LISTPACK;
    } else if (objectGetEncoding(o) == OBJ_ENCODING_HASHTABLE) {
        set = createSetObject();
        hashtable *ht = objectGetVal(o);
        hashtableExpand(objectGetVal(set), hashtableSize(ht));
        /* Stream the members into the fresh object. Expired members are copied
         * with their expiry rather than dropped, so that the copy agrees with
         * the one a replica builds from the same command; the ignore-TTL
         * bracket makes the source iterator yield them. */
        bool volatile_source = setTypeHasVolatileMembers(o);
        if (volatile_source) setTypeIgnoreTTL(o, true);
        si = setTypeInitIterator(o);
        char *str;
        size_t len;
        int64_t intobj;
        while (setTypeNext(si, &str, &len, &intobj) != -1) {
            if (volatile_source) {
                /* str is the smember itself, so its expiry travels with it. */
                setTypeAddWithExpiry(set, (sds)str, smemberGetExpiry(str), NULL);
            } else {
                setTypeAdd(set, (sds)str);
            }
        }
        setTypeReleaseIterator(si);
        if (volatile_source) setTypeIgnoreTTL(o, false);
    } else {
        serverPanic("Unknown set encoding");
    }
    return set;
}

void saddCommand(client *c) {
    robj *set;
    int j, added = 0;

    set = lookupKeyWrite(c->db, c->argv[1]);
    if (checkType(c, set, OBJ_SET)) return;

    if (set == NULL) {
        set = setTypeCreate(objectGetVal(c->argv[2]), c->argc - 2);
        dbAdd(c->db, c->argv[1], &set);
    } else {
        setTypeMaybeConvert(set, c->argc - 2);
    }

    /* An expired member is still there until active expiration removes it, and
     * SADD replaces it with a plain one. A replica does not see it as expired,
     * so its SREM must reach the replication stream before this SADD does. */
    if (setTypeHasVolatileMembers(set)) {
        robj **expired_members = NULL;
        size_t num_expired = 0;
        for (j = 2; j < c->argc; j++) {
            bool replaced_expired = false;
            if (setTypeAddWithExpiry(set, objectGetVal(c->argv[j]), EXPIRY_NONE, &replaced_expired)) added++;
            if (replaced_expired) {
                if (expired_members == NULL) expired_members = zmalloc(sizeof(robj *) * (c->argc - 2));
                expired_members[num_expired++] = c->argv[j];
                incrRefCount(c->argv[j]);
            }
        }
        if (num_expired > 0) {
            /* Propagate the deletions in batches, as propagateFieldsDeletion
             * is used for hash fields. */
            size_t idx = 0;
            while (idx < num_expired) {
                idx += propagateMembersDeletion(c->db, set, num_expired - idx, &expired_members[idx], c->slot);
            }
            server.stat_expiredsetmembers += num_expired;
            notifyKeyspaceEvent(NOTIFY_SET, "sexpired", c->argv[1], c->db->id);
            /* No decrRefCount loop here: propagateMembersDeletion consumed the
             * reference taken above for every member, exactly as the hash
             * keepttl_fields path does with propagateFieldsDeletion. */
        }
        zfree(expired_members);
        /* Replacing the last expired member may have dropped the volatile
         * state. */
        if (!setTypeHasVolatileMembers(set)) dbUpdateObjectWithVolatileItemsTracking(c->db, set);
    } else {
        for (j = 2; j < c->argc; j++) {
            if (setTypeAdd(set, objectGetVal(c->argv[j]))) added++;
        }
    }
    if (added) {
        signalModifiedKey(c, c->db, c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_SET, "sadd", c->argv[1], c->db->id);
        server.dirty += added;
    }
    addReplyLongLong(c, added);
}

void sremCommand(client *c) {
    robj *set;
    int j, deleted = 0, keyremoved = 0;

    if ((set = lookupKeyWriteOrReply(c, c->argv[1], shared.czero)) == NULL || checkType(c, set, OBJ_SET)) return;

    /* An expired member named in the argv is left alone here, but it is a plain
     * member for a replica and for AOF loading (both run under
     * POLICY_IGNORE_EXPIRE), so propagating this argv verbatim would remove it
     * there. Propagate only the members really removed. */
    bool was_volatile = setTypeHasVolatileMembers(set);
    int *removed = was_volatile ? zmalloc(sizeof(int) * (c->argc - 2)) : NULL;
    if (set->encoding == OBJ_ENCODING_HASHTABLE) hashtablePauseAutoShrink(objectGetVal(set));
    for (j = 2; j < c->argc; j++) {
        if (setTypeRemove(set, objectGetVal(c->argv[j]))) {
            if (removed) removed[deleted] = j;
            deleted++;
            if (setTypeSize(set) == 0) {
                /* Removing the last member already dropped the volatile state,
                 * so dbDelete() can no longer recognize the key as tracked:
                 * untrack it here with the pre-removal flag, as HDEL does. */
                if (was_volatile) dbUntrackKeyWithVolatileItems(c->db, set);
                dbDelete(c->db, c->argv[1]);
                keyremoved = 1;
                break;
            }
        }
    }
    if (!keyremoved && set->encoding == OBJ_ENCODING_HASHTABLE) hashtableResumeAutoShrink(objectGetVal(set));
    /* Removing the last volatile member drops the volatile state, so the key
     * must stop being tracked for active expiration. */
    if (!keyremoved && was_volatile && !setTypeHasVolatileMembers(set)) dbUpdateObjectWithVolatileItemsTracking(c->db, set);

    if (deleted) {
        signalModifiedKey(c, c->db, c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_SET, "srem", c->argv[1], c->db->id);
        if (keyremoved) notifyKeyspaceEvent(NOTIFY_GENERIC, "del", c->argv[1], c->db->id);
        server.dirty += deleted;
        /* Some argument named a member the primary did not remove (an expired
         * one, one that was never there, or one after the early exit above):
         * propagate only the removals. Nothing removed needs no filtering,
         * because dirty == 0 already suppresses the propagation, and the common
         * "everything was removed" case keeps the original argv and allocates
         * nothing. */
        if (removed && deleted < c->argc - 2) {
            int new_argc = deleted + 2;
            robj **new_argv = zmalloc(sizeof(robj *) * new_argc);
            new_argv[0] = shared.srem;
            new_argv[1] = c->argv[1];
            incrRefCount(c->argv[1]);
            for (j = 0; j < deleted; j++) {
                new_argv[j + 2] = c->argv[removed[j]];
                incrRefCount(c->argv[removed[j]]);
            }
            replaceClientCommandVector(c, new_argc, new_argv);
        }
    }
    zfree(removed);
    addReplyLongLong(c, deleted);
}

/* SMOVE destination side for a member that carries a TTL: the source member's
 * expiry is written into the destination unconditionally, also when the
 * destination already holds the member. Returns 1 when the member was newly
 * added, 0 when the destination already held it live. */
static int smoveAddToDestWithExpiry(client *c, robj *dstset, robj *ele, mstime_t expiry) {
    bool replaced_expired = false;
    int added = setTypeAddWithExpiry(dstset, objectGetVal(ele), expiry, &replaced_expired);
    if (replaced_expired) {
        /* The destination copy was an expired member. A replica does not see
         * it as expired and would keep its TTL, so propagate its SREM before
         * the SMOVE itself, as saddCommand does. */
        robj *members[1] = {ele};
        /* propagateMembersDeletion consumes one reference of every member, and
         * 'ele' is c->argv[3], which the client still owns. */
        incrRefCount(ele);
        propagateMembersDeletion(c->db, dstset, 1, members, c->slot);
        server.stat_expiredsetmembers++;
        notifyKeyspaceEvent(NOTIFY_SET, "sexpired", c->argv[2], c->db->id);
    } else if (!added) {
        /* The destination already held the member live: overwrite its TTL with
         * the source's, including removing it when the source had none. */
        setTypeSetExpiry(dstset, objectGetVal(ele), expiry, 0);
    }
    /* The destination may have just gained (or lost) its volatile state. */
    dbUpdateObjectWithVolatileItemsTracking(c->db, dstset);
    return added;
}

void smoveCommand(client *c) {
    robj *srcset, *dstset, *ele;
    srcset = lookupKeyWrite(c->db, c->argv[1]);
    dstset = lookupKeyWrite(c->db, c->argv[2]);
    ele = c->argv[3];

    /* If the source key does not exist return 0 */
    if (srcset == NULL) {
        addReply(c, shared.czero);
        return;
    }

    /* If the source key has the wrong type, or the destination key
     * is set and has the wrong type, return with an error. */
    if (checkType(c, srcset, OBJ_SET) || checkType(c, dstset, OBJ_SET)) return;

    /* If srcset and dstset are equal, SMOVE is a no-op */
    if (srcset == dstset) {
        addReply(c, setTypeIsMember(srcset, objectGetVal(ele)) ? shared.cone : shared.czero);
        return;
    }

    /* SMOVE carries the member's TTL to the destination, so read it while the
     * member is still in the source. */
    mstime_t expiry = EXPIRY_NONE;
    bool src_volatile = setTypeHasVolatileMembers(srcset);
    if (src_volatile && setTypeGetExpiry(srcset, objectGetVal(ele), &expiry) != C_OK) {
        /* Missing, or expired: not visible, so there is nothing to move. */
        addReply(c, shared.czero);
        return;
    }

    /* If the element cannot be removed from the src set, return 0. */
    if (!setTypeRemove(srcset, objectGetVal(ele))) {
        addReply(c, shared.czero);
        return;
    }
    notifyKeyspaceEvent(NOTIFY_SET, "srem", c->argv[1], c->db->id);

    /* Moving out the last volatile member drops the source's volatile state. */
    if (src_volatile && !setTypeHasVolatileMembers(srcset)) dbUpdateObjectWithVolatileItemsTracking(c->db, srcset);

    /* Remove the src set from the database when empty */
    if (setTypeSize(srcset) == 0) {
        dbDelete(c->db, c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC, "del", c->argv[1], c->db->id);
    }

    /* Create the destination set when it doesn't exist */
    if (!dstset) {
        dstset = setTypeCreate(objectGetVal(ele), 1);
        dbAdd(c->db, c->argv[2], &dstset);
    }

    signalModifiedKey(c, c->db, c->argv[1]);
    server.dirty++;

    /* An extra key has changed when ele was successfully added to dstset */
    int added;
    if (expiry == EXPIRY_NONE && !setTypeHasVolatileMembers(dstset)) {
        /* Neither side carries a TTL: the plain path, one object-level branch. */
        added = setTypeAdd(dstset, objectGetVal(ele));
    } else {
        added = smoveAddToDestWithExpiry(c, dstset, ele, expiry);
    }
    if (added) {
        server.dirty++;
        signalModifiedKey(c, c->db, c->argv[2]);
        notifyKeyspaceEvent(NOTIFY_SET, "sadd", c->argv[2], c->db->id);
    }
    addReply(c, shared.cone);
}

#define SMISMEMBER_FIND_BATCH_SIZE 16
static_assert(SMISMEMBER_FIND_BATCH_SIZE <= HASHTABLE_FIND_BATCH_MAX_SIZE,
              "SMISMEMBER batch size exceeds hashtable batch lookup limit");

static void sismemberReply(client *c, robj *set, robj *member) {
    addReply(c, setTypeIsMember(set, objectGetVal(member)) ? shared.cone : shared.czero);
}

static void smismemberReplyWithHashtable(client *c, hashtable *ht, robj **members, size_t count) {
    const void *keys[SMISMEMBER_FIND_BATCH_SIZE];
    void *found_entries[SMISMEMBER_FIND_BATCH_SIZE];
    while (count) {
        size_t batch = count > SMISMEMBER_FIND_BATCH_SIZE ? SMISMEMBER_FIND_BATCH_SIZE : count;

        for (size_t i = 0; i < batch; i++) {
            keys[i] = objectGetVal(members[i]);
        }

        uint32_t result = hashtableFindBatch(ht, (int)batch, keys, found_entries);

        for (size_t i = 0; i < batch; i++) {
            addReply(c, (result >> i) & 1 ? shared.cone : shared.czero);
        }

        members += batch;
        count -= batch;
    }
}

void sismemberCommand(client *c) {
    robj *set;
    int xx = 0;

    if (c->argc == 4 && !strcasecmp(objectGetVal(c->argv[3]), "XX")) {
        xx = 1;
    } else if (c->argc > 3) {
        addReplyErrorObject(c, shared.syntaxerr);
        return;
    }

    set = lookupKeyRead(c->db, c->argv[1]);
    if (set == NULL) {
        if (xx)
            /* If key doesn't exist and XX is specified, return -1 */
            addReplyLongLong(c, -1);
        else
            /* If key doesn't exist and XX is not specified, return 0 */
            addReply(c, shared.czero);
        return;
    }

    if (checkType(c, set, OBJ_SET)) return;

    sismemberReply(c, set, c->argv[2]);
}

void smismemberCommand(client *c) {
    robj *set;

    /* Don't abort when the key cannot be found. Non-existing keys are empty
     * sets, where SMISMEMBER should respond with a series of zeros. */
    set = lookupKeyRead(c->db, c->argv[1]);
    if (set == NULL) {
        addReplyArrayLen(c, c->argc - 2);
        for (int j = 2; j < c->argc; j++) {
            addReply(c, shared.czero);
        }
        return;
    }
    if (checkType(c, set, OBJ_SET)) return;

    size_t count = c->argc - 2;
    addReplyArrayLen(c, count);

    /* Prefer hashtable batch lookup to improve performance. */
    if (set->encoding == OBJ_ENCODING_HASHTABLE && count > 1) {
        smismemberReplyWithHashtable(c, objectGetVal(set), c->argv + 2, count);
        return;
    }

    for (size_t i = 0; i < count; i++) {
        sismemberReply(c, set, c->argv[i + 2]);
    }
}

void scardCommand(client *c) {
    robj *o;

    if ((o = lookupKeyReadOrReply(c, c->argv[1], shared.czero)) == NULL || checkType(c, o, OBJ_SET)) return;

    addReplyLongLong(c, setTypeSize(o));
}

/* Handle the "SPOP key <count>" variant. The normal version of the
 * command is handled by the spopCommand() function itself. */

/* How many times bigger should be the set compared to the remaining size
 * for us to use the "create new set" strategy? Read later in the
 * implementation for more info. */
#define SPOP_MOVE_STRATEGY_MUL 5

/* SPOP <key> <count> sampler.
 *
 * No command expires members, so setTypeSize() counts the expired ones
 * and is only an upper bound on what can be popped: for a set with volatile
 * members the reply length is deferred (a short pop must become a short reply,
 * not a protocol error) and such a set takes neither the listpack fast path,
 * which deletes by position with lpBatchDelete and would orphan the metadata
 * entries, nor CASE 3, which rebuilds the remainder into a plain set. */
void spopWithCountCommand(client *c) {
    long l;
    /* Get the count argument */
    if (getPositiveLongFromObjectOrReply(c, c->argv[2], &l, NULL) != C_OK) return;
    unsigned long count = (unsigned long)l;

    /* Make sure a key with the name inputted exists, and that it's type is
     * indeed a set. Otherwise, return nil */
    robj *set;
    if ((set = lookupKeyWriteOrReply(c, c->argv[1], shared.emptyset[c->resp])) == NULL || checkType(c, set, OBJ_SET))
        return;

    /* If count is zero, serve an empty set ASAP to avoid special
     * cases later. */
    if (count == 0) {
        addReply(c, shared.emptyset[c->resp]);
        return;
    }

    unsigned long size = setTypeSize(set);
    bool volatile_set = setTypeHasVolatileMembers(set);

    /* CASE 1:
     * The number of requested elements is greater than or equal to
     * the number of elements inside the set: simply return the whole set. */
    if (count >= size) {
        /* Generate an SPOP keyspace notification */
        notifyKeyspaceEvent(NOTIFY_SET, "spop", c->argv[1], c->db->id);
        server.dirty += size;

        /* We just return the entire set */
        sunionDiffGenericCommand(c, c->argv + 1, 1, NULL, SET_OP_UNION);

        /* Delete the set as it is now empty */
        dbDelete(c->db, c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC, "del", c->argv[1], c->db->id);

        /* todo: Move the spop notification to be executed after the command logic. */

        /* Propagate this command as a DEL or UNLINK operation */
        robj *aux = server.lazyfree_lazy_server_del ? shared.unlink : shared.del;
        rewriteClientCommandVector(c, 2, aux, c->argv[1]);
        signalModifiedKey(c, c->db, c->argv[1]);
        return;
    }

    /* Case 2 and 3 require to replicate SPOP as a set of SREM commands.
     * Prepare our replication argument vector. Also send the array length
     * which is common to both the code paths. */
    unsigned long batchsize = count > 1024 ? 1024 : count;
    robj **propargv = zmalloc(sizeof(robj *) * (2 + batchsize));
    propargv[0] = shared.srem;
    propargv[1] = c->argv[1];
    unsigned long propindex = 2;
    void *replylen = NULL;
    if (volatile_set)
        replylen = addReplyDeferredLen(c);
    else
        addReplySetLen(c, count);
    unsigned long popped = 0;

    /* Common iteration vars. */
    char *str;
    size_t len;
    int64_t llele;
    unsigned long remaining = size - count; /* Elements left after SPOP. */

    /* If we are here, the number of requested elements is less than the
     * number of elements inside the set. Also we are sure that count < size.
     * Use two different strategies.
     *
     * CASE 2: The number of elements to return is small compared to the
     * set size. We can just extract random elements and return them to
     * the set. */
    if (remaining * SPOP_MOVE_STRATEGY_MUL > count && set->encoding == OBJ_ENCODING_LISTPACK && !volatile_set) {
        /* Specialized case for listpack. Traverse it only once. */
        unsigned char *lp = objectGetVal(set);
        unsigned char *p = lpFirst(lp);
        unsigned int index = 0;
        unsigned char **ps = zmalloc(sizeof(char *) * count);
        for (unsigned long i = 0; i < count; i++) {
            p = lpNextRandom(lp, p, &index, count - i, 0);
            unsigned int len;
            str = (char *)lpGetValue(p, &len, (long long *)&llele);

            if (str) {
                addReplyBulkCBuffer(c, str, len);
                propargv[propindex++] = createStringObject(str, len);
            } else {
                addReplyBulkLongLong(c, llele);
                propargv[propindex++] = createStringObjectFromLongLong(llele);
            }
            /* Replicate/AOF this command as an SREM operation */
            if (propindex == 2 + batchsize) {
                alsoPropagate(c->db->id, propargv, propindex, PROPAGATE_AOF | PROPAGATE_REPL, c->slot);
                for (unsigned long j = 2; j < propindex; j++) {
                    decrRefCount(propargv[j]);
                }
                propindex = 2;
            }

            /* Store pointer for later deletion and move to next. */
            ps[i] = p;
            p = lpNext(lp, p);
            index++;
        }
        lp = lpBatchDelete(lp, ps, count);
        zfree(ps);
        objectSetVal(set, lp);
        popped = count;
    } else if (remaining * SPOP_MOVE_STRATEGY_MUL > count || volatile_set) {
        for (unsigned long i = 0; i < count; i++) {
            robj *ele = setTypePopRandom(set);
            if (ele == NULL) break; /* only expired members remain */
            propargv[propindex] = ele;
            addReplyBulk(c, propargv[propindex]);
            propindex++;
            popped++;
            /* Replicate/AOF this command as an SREM operation */
            if (propindex == 2 + batchsize) {
                alsoPropagate(c->db->id, propargv, propindex, PROPAGATE_AOF | PROPAGATE_REPL, c->slot);
                for (unsigned long j = 2; j < propindex; j++) {
                    decrRefCount(propargv[j]);
                }
                propindex = 2;
            }
        }
        /* Popping the last volatile member drops the volatile state (see SREM). */
        if (volatile_set && !setTypeHasVolatileMembers(set)) dbUpdateObjectWithVolatileItemsTracking(c->db, set);
    } else {
        /* CASE 3: The number of elements to return is very big, approaching
         * the size of the set itself. After some time extracting random elements
         * from such a set becomes computationally expensive, so we use
         * a different strategy, we extract random elements that we don't
         * want to return (the elements that will remain part of the set),
         * creating a new set as we do this (that will be stored as the original
         * set). Then we return the elements left in the original set and
         * release it. */
        robj *newset = NULL;

        /* Create a new set with just the remaining elements. */
        if (set->encoding == OBJ_ENCODING_LISTPACK) {
            /* Specialized case for listpack. Traverse it only once. */
            newset = createSetListpackObject();
            unsigned char *lp = objectGetVal(set);
            unsigned char *p = lpFirst(lp);
            unsigned int index = 0;
            unsigned char **ps = zmalloc(sizeof(char *) * remaining);
            for (unsigned long i = 0; i < remaining; i++) {
                p = lpNextRandom(lp, p, &index, remaining - i, 0);
                unsigned int len;
                str = (char *)lpGetValue(p, &len, (long long *)&llele);
                setTypeAddAux(newset, str, len, llele, 0);
                ps[i] = p;
                p = lpNext(lp, p);
                index++;
            }
            lp = lpBatchDelete(lp, ps, remaining);
            zfree(ps);
            objectSetVal(set, lp);
        } else {
            while (remaining--) {
                int encoding = setTypeRandomElement(set, &str, &len, &llele);
                if (!newset) {
                    newset = str ? createSetListpackObject() : createIntsetObject();
                }
                setTypeAddAux(newset, str, len, llele, encoding == OBJ_ENCODING_HASHTABLE);
                setTypeRemoveAux(set, str, len, llele, encoding == OBJ_ENCODING_HASHTABLE);
            }
        }
        /* Transfer the old set to the client. */
        setTypeIterator *si;
        si = setTypeInitIterator(set);
        while (setTypeNext(si, &str, &len, &llele) != -1) {
            if (str == NULL) {
                addReplyBulkLongLong(c, llele);
                propargv[propindex++] = createStringObjectFromLongLong(llele);
            } else {
                addReplyBulkCBuffer(c, str, len);
                propargv[propindex++] = createStringObject(str, len);
            }
            /* Replicate/AOF this command as an SREM operation */
            if (propindex == 2 + batchsize) {
                alsoPropagate(c->db->id, propargv, propindex, PROPAGATE_AOF | PROPAGATE_REPL, c->slot);
                for (unsigned long i = 2; i < propindex; i++) {
                    decrRefCount(propargv[i]);
                }
                propindex = 2;
            }
        }
        setTypeReleaseIterator(si);

        /* Assign the new set as the key value. */
        dbReplaceValue(c->db, c->argv[1], &newset);
        popped = count;
    }
    if (volatile_set) setDeferredSetLen(c, replylen, popped);
    server.dirty += popped;

    /* Replicate/AOF the remaining elements as an SREM operation */
    if (propindex != 2) {
        alsoPropagate(c->db->id, propargv, propindex, PROPAGATE_AOF | PROPAGATE_REPL, c->slot);
        for (unsigned long i = 2; i < propindex; i++) {
            decrRefCount(propargv[i]);
        }
        propindex = 2;
    }
    zfree(propargv);

    /* Don't propagate the command itself even if we incremented the
     * dirty counter. We don't want to propagate an SPOP command since
     * we propagated the command as a set of SREMs operations using
     * the alsoPropagate() API. */
    preventCommandPropagation(c);
    /* A set whose remaining members are all expired pops nothing, and a command
     * that changed nothing emits no event and invalidates nothing. */
    if (popped) {
        notifyKeyspaceEvent(NOTIFY_SET, "spop", c->argv[1], c->db->id);
        signalModifiedKey(c, c->db, c->argv[1]);
    }
}

void spopCommand(client *c) {
    robj *set, *ele;

    if (c->argc == 3) {
        spopWithCountCommand(c);
        return;
    } else if (c->argc > 3) {
        addReplyErrorObject(c, shared.syntaxerr);
        return;
    }

    /* Make sure a key with the name inputted exists, and that it's type is
     * indeed a set */
    if ((set = lookupKeyWriteOrReply(c, c->argv[1], shared.null[c->resp])) == NULL || checkType(c, set, OBJ_SET))
        return;

    /* setTypePopRandom() yields NULL when no remaining member is live. */
    bool was_volatile = setTypeHasVolatileMembers(set);
    ele = setTypePopRandom(set);
    if (ele == NULL) {
        addReply(c, shared.null[c->resp]);
        return;
    }
    /* Popping the last volatile member drops the volatile state (see SREM). An
     * emptied set is untracked in the delete branch below instead. */
    if (was_volatile && setTypeSize(set) > 0 && !setTypeHasVolatileMembers(set))
        dbUpdateObjectWithVolatileItemsTracking(c->db, set);

    notifyKeyspaceEvent(NOTIFY_SET, "spop", c->argv[1], c->db->id);

    /* Replicate/AOF this command as an SREM operation */
    rewriteClientCommandVector(c, 3, shared.srem, c->argv[1], ele);

    /* Add the element to the reply */
    addReplyBulk(c, ele);
    decrRefCount(ele);

    /* Delete the set if it's empty */
    if (setTypeSize(set) == 0) {
        /* Popping the last member already dropped the volatile state, so
         * dbDelete() can no longer recognize the key as tracked: untrack it
         * here with the pre-pop flag, as HDEL does. */
        if (was_volatile) dbUntrackKeyWithVolatileItems(c->db, set);
        dbDelete(c->db, c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC, "del", c->argv[1], c->db->id);
    }

    /* Set has been modified */
    signalModifiedKey(c, c->db, c->argv[1]);
    server.dirty++;
}

/* handle the "SRANDMEMBER key <count>" variant. The normal version of the
 * command is handled by the srandmemberCommand() function itself. */

/* How many times bigger should be the set compared to the requested size
 * for us to don't use the "remove elements" strategy? Read later in the
 * implementation for more info. */
#define SRANDMEMBER_SUB_STRATEGY_MUL 3

/* If client is trying to ask for a very large number of random elements,
 * queuing may consume an unlimited amount of memory, so we want to limit
 * the number of randoms per time. */
#define SRANDFIELD_RANDOM_SAMPLE_LIMIT 1000

static void srandmemberWithCountFromVolatileSet(client *c, robj *set, unsigned long count, int uniq);
static void srandmemberWithCountFromSet(client *c, robj *set, unsigned long count, int uniq);

void srandmemberWithCountCommand(client *c) {
    long l;
    unsigned long count;
    int uniq = 1;
    robj *set;

    if (getRangeLongFromObjectOrReply(c, c->argv[2], -LONG_MAX, LONG_MAX, &l, NULL) != C_OK) return;
    if (l >= 0) {
        count = (unsigned long)l;
    } else {
        /* A negative count means: return the same elements multiple times
         * (i.e. don't remove the extracted element after every extraction). */
        count = -l;
        uniq = 0;
    }

    if ((set = lookupKeyReadOrReply(c, c->argv[1], shared.emptyarray)) == NULL || checkType(c, set, OBJ_SET)) return;

    /* If count is zero, serve it ASAP to avoid special cases later. */
    if (count == 0) {
        addReply(c, shared.emptyarray);
        return;
    }

    /* SRANDMEMBER is read-only, so it must not remove anything: the plain
     * sampler needs an exact setTypeSize() and position-indexed listpack access,
     * which expired members break, hence a sampler of its own. */
    if (setTypeHasVolatileMembers(set)) {
        srandmemberWithCountFromVolatileSet(c, set, count, uniq);
        return;
    }
    srandmemberWithCountFromSet(c, set, count, uniq);
}

/* Pointer identifying the member the iterator currently sits on, for
 * addReplyVolatileSetMember(): the smember itself for the hashtable encoding,
 * the listpack entry otherwise. Only valid while nothing mutates the set. */
static void *setTypeIteratorMemberRef(setTypeIterator *si, char *str) {
    return si->encoding == OBJ_ENCODING_HASHTABLE ? (void *)str : (void *)si->lpi;
}

/* Reply with the member 'ref' addresses, as taken from setTypeIteratorMemberRef(). */
static void addReplyVolatileSetMember(client *c, robj *set, void *ref) {
    if (set->encoding == OBJ_ENCODING_HASHTABLE) {
        addReplyBulkCBuffer(c, ref, sdslen((sds)ref));
        return;
    }
    unsigned int len;
    long long llele;
    char *str = (char *)lpGetValue((unsigned char *)ref, &len, &llele);
    if (str == NULL)
        addReplyBulkLongLong(c, llele);
    else
        addReplyBulkCBuffer(c, str, len);
}

/* Pointers to every LIVE member of a set that carries member TTLs. *out is a
 * zmalloc'd array owned by the caller; the pointers address the set's own
 * memory, so they die with the first mutation of it. Returns the count. */
static unsigned long setTypeCollectLiveMembers(robj *set, void ***out) {
    /* setTypeSize() counts the expired members too, so this is an upper bound. */
    unsigned long cap = setTypeSize(set);
    void **refs = zmalloc(sizeof(void *) * (cap + 1));
    unsigned long n = 0;
    char *str;
    size_t len;
    int64_t llele;
    setTypeIterator *si = setTypeInitIterator(set);
    while (setTypeNext(si, &str, &len, &llele) != -1) {
        serverAssert(n < cap);
        refs[n++] = setTypeIteratorMemberRef(si, str);
    }
    setTypeReleaseIterator(si);
    *out = refs;
    return n;
}

/* SRANDMEMBER key <count> on a set that carries member TTLs.
 *
 * Expired members break both invariants the plain samplers rely
 * on: setTypeSize() is only an upper bound on the live members, and a listpack
 * position no longer maps to a live one. So each variant below makes at most ONE
 * pass over the live members instead of probing repeatedly, which costs O(n) in
 * the set size and is what an exactly uniform, bounded reply is worth while
 * expired members are still there. The members are addressed by pointer into
 * the set, which is safe because SRANDMEMBER never mutates it. */
static void srandmemberWithCountFromVolatileSet(client *c, robj *set, unsigned long count, int uniq) {
    char *str;
    size_t len;
    int64_t llele;

    /* A single member is a single draw, so the shared primitive is both uniform
     * and cheaper than a pass: it rejects expired members out of fair random
     * picks and only falls back to a pass when those are dense. */
    if (count == 1) {
        if (setTypeRandomElement(set, &str, &len, &llele) == -1) {
            addReply(c, shared.emptyarray);
            return;
        }
        addReplyArrayLen(c, 1);
        if (str == NULL)
            addReplyBulkLongLong(c, llele);
        else
            addReplyBulkCBuffer(c, str, len);
        return;
    }

    if (!uniq) {
        /* A negative count allows repetition, so the draws are independent. A
         * small count draws each element on its own, which is cheaper than a
         * pass over the set; a larger one amortizes one pass by sampling the
         * collected live members with replacement. */
        if (count <= 16) {
            /* The set cannot change under a read-only command, so once one draw
             * finds a live member every further draw finds one too. */
            if (setTypeRandomElement(set, &str, &len, &llele) == -1) {
                addReply(c, shared.emptyarray);
                return;
            }
            addReplyArrayLen(c, count);
            for (unsigned long i = 0; i < count; i++) {
                if (i > 0) serverAssert(setTypeRandomElement(set, &str, &len, &llele) != -1);
                if (str == NULL)
                    addReplyBulkLongLong(c, llele);
                else
                    addReplyBulkCBuffer(c, str, len);
            }
            return;
        }
        void **live;
        unsigned long n = setTypeCollectLiveMembers(set, &live);
        if (n == 0) {
            zfree(live);
            addReply(c, shared.emptyarray);
            return;
        }
        addReplyArrayLen(c, count);
        while (count--) {
            addReplyVolatileSetMember(c, set, live[(unsigned long)rand() % n]);
            if (c->flag.close_asap) break;
        }
        zfree(live);
        return;
    }

    /* Unique members: reservoir-sample the live members in one pass. Set members
     * are distinct, so the reservoir is distinct too and no auxiliary hashtable
     * is needed; it yields a uniform subset without knowing the live count up
     * front, and degrades to "every live member" when there are at most 'count'
     * of them. The reservoir is capped by setTypeSize() so that a huge 'count'
     * cannot ask for an allocation larger than the set. */
    unsigned long size = setTypeSize(set);
    unsigned long cap = count < size ? count : size;
    void **res = zmalloc(sizeof(void *) * (cap + 1));
    unsigned long seen = 0, held = 0;
    setTypeIterator *si = setTypeInitIterator(set);
    while (setTypeNext(si, &str, &len, &llele) != -1) {
        void *ref = setTypeIteratorMemberRef(si, str);
        if (held < cap) {
            res[held++] = ref;
        } else {
            unsigned long r = (unsigned long)rand() % (seen + 1);
            if (r < cap) res[r] = ref;
        }
        seen++;
    }
    setTypeReleaseIterator(si);

    addReplyArrayLen(c, held);
    for (unsigned long i = 0; i < held; i++) addReplyVolatileSetMember(c, set, res[i]);
    zfree(res);
}

/* The SRANDMEMBER key <count> sampler proper. 'set' never received a member
 * TTL, so setTypeSize() is exact and every member it holds is visible. */
static void srandmemberWithCountFromSet(client *c, robj *set, unsigned long count, int uniq) {
    unsigned long size;
    char *str;
    size_t len;
    int64_t llele;

    size = setTypeSize(set);

    /* CASE 1: The count was negative, so the extraction method is just:
     * "return N random elements" sampling the whole set every time.
     * This case is trivial and can be served without auxiliary data
     * structures. This case is the only one that also needs to return the
     * elements in random order. */
    if (!uniq || count == 1) {
        addReplyArrayLen(c, count);

        if (set->encoding == OBJ_ENCODING_LISTPACK && count > 1) {
            /* Specialized case for listpack, traversing it only once. */
            unsigned long limit, sample_count;
            limit = count > SRANDFIELD_RANDOM_SAMPLE_LIMIT ? SRANDFIELD_RANDOM_SAMPLE_LIMIT : count;
            listpackEntry *entries = zmalloc(limit * sizeof(listpackEntry));
            while (count) {
                sample_count = count > limit ? limit : count;
                count -= sample_count;
                lpRandomEntries(objectGetVal(set), sample_count, entries);
                for (unsigned long i = 0; i < sample_count; i++) {
                    if (entries[i].sval)
                        addReplyBulkCBuffer(c, entries[i].sval, entries[i].slen);
                    else
                        addReplyBulkLongLong(c, entries[i].lval);
                }
                if (c->flag.close_asap) break;
            }
            zfree(entries);
            return;
        }

        while (count--) {
            setTypeRandomElement(set, &str, &len, &llele);
            if (str == NULL) {
                addReplyBulkLongLong(c, llele);
            } else {
                addReplyBulkCBuffer(c, str, len);
            }
            if (c->flag.close_asap) break;
        }
        return;
    }

    /* CASE 2:
     * The number of requested elements is greater than the number of
     * elements inside the set: simply return the whole set. */
    if (count >= size) {
        setTypeIterator *si;
        addReplyArrayLen(c, size);
        si = setTypeInitIterator(set);
        while (setTypeNext(si, &str, &len, &llele) != -1) {
            if (str == NULL) {
                addReplyBulkLongLong(c, llele);
            } else {
                addReplyBulkCBuffer(c, str, len);
            }
            size--;
        }
        setTypeReleaseIterator(si);
        serverAssert(size == 0);
        return;
    }

    /* CASE 2.5 listpack only. Sampling unique elements, in non-random order.
     * Listpack encoded sets are meant to be relatively small, so
     * SRANDMEMBER_SUB_STRATEGY_MUL isn't necessary and we rather not make
     * copies of the entries. Instead, we emit them directly to the output
     * buffer.
     *
     * And it is inefficient to repeatedly pick one random element from a
     * listpack in CASE 4. So we use this instead. */
    if (set->encoding == OBJ_ENCODING_LISTPACK) {
        unsigned char *lp = objectGetVal(set);
        unsigned char *p = lpFirst(lp);
        unsigned int i = 0;
        addReplyArrayLen(c, count);
        while (count) {
            p = lpNextRandom(lp, p, &i, count--, 0);
            unsigned int len;
            str = (char *)lpGetValue(p, &len, (long long *)&llele);
            if (str == NULL) {
                addReplyBulkLongLong(c, llele);
            } else {
                addReplyBulkCBuffer(c, str, len);
            }
            p = lpNext(lp, p);
            i++;
        }
        return;
    }

    /* For CASE 3 and CASE 4 we need an auxiliary hashtable. */
    hashtable *ht = hashtableCreate(&sdsReplyHashtableType);

    /* CASE 3:
     * The number of elements inside the set is not greater than
     * SRANDMEMBER_SUB_STRATEGY_MUL times the number of requested elements.
     * In this case we create a set from scratch with all the elements, and
     * subtract random elements to reach the requested number of elements.
     *
     * This is done because if the number of requested elements is just
     * a bit less than the number of elements in the set, the natural approach
     * used into CASE 4 is highly inefficient. */
    if (count * SRANDMEMBER_SUB_STRATEGY_MUL > size) {
        setTypeIterator *si;

        /* Add all the elements into the temporary hashtable. */
        si = setTypeInitIterator(set);
        hashtableExpand(ht, size);
        while (setTypeNext(si, &str, &len, &llele) != -1) {
            if (str == NULL) {
                serverAssert(hashtableAdd(ht, (void *)sdsfromlonglong(llele)));
            } else {
                serverAssert(hashtableAdd(ht, (void *)sdsnewlen(str, len)));
            }
        }
        setTypeReleaseIterator(si);
        serverAssert(hashtableSize(ht) == size);

        /* Remove random elements to reach the right count. */
        while (size > count) {
            void *element;
            hashtableFairRandomEntry(ht, &element);
            hashtableDelete(ht, element);
            sdsfree((sds)element);
            size--;
        }
    }

    /* CASE 4: We have a big set compared to the requested number of elements.
     * In this case we can simply get random elements from the set and add
     * to the temporary set, trying to eventually get enough unique elements
     * to reach the specified count. */
    else {
        unsigned long added = 0;
        sds sdsele;

        hashtableExpand(ht, count);
        while (added < count) {
            setTypeRandomElement(set, &str, &len, &llele);
            if (str == NULL) {
                sdsele = sdsfromlonglong(llele);
            } else {
                sdsele = sdsnewlen(str, len);
            }
            /* Try to add the object to the dictionary. If it already exists
             * free it, otherwise increment the number of objects we have
             * in the result dictionary. */
            if (hashtableAdd(ht, sdsele))
                added++;
            else
                sdsfree(sdsele);
        }
    }

    /* CASE 3 & 4: send the result to the user. */
    {
        hashtableIterator iter;
        hashtableInitIterator(&iter, ht, 0);

        addReplyArrayLen(c, count);
        serverAssert(count == hashtableSize(ht));
        void *element;
        while (hashtableNext(&iter, &element)) addReplyBulkSds(c, (sds)element);
        hashtableCleanupIterator(&iter);
        hashtableRelease(ht);
    }
}

/* SRANDMEMBER <key> [<count>] */
void srandmemberCommand(client *c) {
    robj *set;
    char *str;
    size_t len;
    int64_t llele;

    if (c->argc == 3) {
        srandmemberWithCountCommand(c);
        return;
    } else if (c->argc > 3) {
        addReplyErrorObject(c, shared.syntaxerr);
        return;
    }

    /* Handle variant without <count> argument. Reply with simple bulk string */
    if ((set = lookupKeyReadOrReply(c, c->argv[1], shared.null[c->resp])) == NULL || checkType(c, set, OBJ_SET)) return;

    /* setTypeRandomElement() returns -1 when no member is live. */
    if (setTypeRandomElement(set, &str, &len, &llele) == -1) {
        addReply(c, shared.null[c->resp]);
        return;
    }
    if (str == NULL) {
        addReplyBulkLongLong(c, llele);
    } else {
        addReplyBulkCBuffer(c, str, len);
    }
}

int qsortCompareSetsByCardinality(const void *s1, const void *s2) {
    if (setTypeSize(*(robj **)s1) > setTypeSize(*(robj **)s2)) return 1;
    if (setTypeSize(*(robj **)s1) < setTypeSize(*(robj **)s2)) return -1;
    return 0;
}

/* This is used by SDIFF and in this case we can receive NULL that should
 * be handled as empty sets. */
int qsortCompareSetsByRevCardinality(const void *s1, const void *s2) {
    robj *o1 = *(robj **)s1, *o2 = *(robj **)s2;
    unsigned long first = o1 ? setTypeSize(o1) : 0;
    unsigned long second = o2 ? setTypeSize(o2) : 0;

    if (first < second) return 1;
    if (first > second) return -1;
    return 0;
}

#define STORE_PROP_BATCH 1024 /* members per propagated command, as SPOP's SREM batching */

/* Propagate a *STORE destination by effect instead of verbatim: a deletion of
 * 'dstkey' followed by batched '<cmd> dstkey member ...' over 'members'.
 *
 * Needed as soon as a source carries member TTLs. An expired member is excluded
 * from the result on the primary, but a replica re-executing the same command
 * runs under POLICY_IGNORE_EXPIRE and sees it as live, so it would store a
 * different destination. Nothing later reconciles a *STORE destination, so the
 * divergence is permanent -- unlike a read, which is merely stale until the
 * primary removes the member.
 *
 * The caller keeps ownership of 'members'. */
void propagateStoreAsEffects(client *c, robj *dstkey, const char *cmd, robj **members, unsigned long count) {
    propagateDeletion(c->db, dstkey, server.lazyfree_lazy_server_del, c->slot);

    if (count > 0) {
        robj **argv = zmalloc(sizeof(robj *) * (2 + STORE_PROP_BATCH));
        argv[0] = createStringObject(cmd, strlen(cmd));
        argv[1] = dstkey;
        for (unsigned long i = 0; i < count;) {
            int n = 2;
            while (i < count && n < 2 + STORE_PROP_BATCH) argv[n++] = members[i++];
            alsoPropagate(c->db->id, argv, n, PROPAGATE_AOF | PROPAGATE_REPL, c->slot);
        }
        decrRefCount(argv[0]);
        zfree(argv);
    }
    preventCommandPropagation(c);
}

/* propagateStoreAsEffects() for a destination SET, which holds plain members
 * with no TTL of their own. 'dstset' is NULL when the result is empty, and then
 * only the deletion goes out. */
static void propagateSetStoreAsEffects(client *c, robj *dstkey, robj *dstset) {
    unsigned long size = dstset ? setTypeSize(dstset) : 0;
    robj **members = size > 0 ? zmalloc(sizeof(robj *) * size) : NULL;
    unsigned long count = 0;
    if (members) {
        char *str;
        size_t len;
        int64_t llval;
        setTypeIterator *si = setTypeInitIterator(dstset);
        while (setTypeNext(si, &str, &len, &llval) != -1)
            members[count++] = str ? createStringObject(str, len) : createStringObjectFromLongLong(llval);
        setTypeReleaseIterator(si);
    }
    propagateStoreAsEffects(c, dstkey, "SADD", members, count);
    for (unsigned long i = 0; i < count; i++) decrRefCount(members[i]);
    zfree(members);
}

/* SINTER / SMEMBERS / SINTERSTORE / SINTERCARD
 *
 * 'cardinality_only' work for SINTERCARD, only return the cardinality
 * with minimum processing and memory overheads.
 *
 * 'limit' work for SINTERCARD, stop searching after reaching the limit.
 * Passing a 0 means unlimited.
 *
 * setTypeSize() counting expired members costs at most a suboptimal iteration
 * order here, but the SINTERSTORE variant must be propagated by effect when a
 * source carries member TTLs; see propagateStoreAsEffects().
 */
void sinterGenericCommand(client *c,
                          robj **setkeys,
                          unsigned long setnum,
                          robj *dstkey,
                          int cardinality_only,
                          unsigned long limit) {
    robj **sets = zmalloc(sizeof(robj *) * setnum);
    setTypeIterator *si;
    robj *dstset = NULL;
    char *str;
    size_t len;
    int64_t intobj;
    void *replylen = NULL;
    unsigned long j, cardinality = 0;
    int encoding, empty = 0;
    int volatile_source = 0;

    for (j = 0; j < setnum; j++) {
        robj *setobj = lookupKeyRead(c->db, setkeys[j]);
        if (!setobj) {
            /* A NULL is considered an empty set */
            empty += 1;
            sets[j] = NULL;
            continue;
        }
        if (checkType(c, setobj, OBJ_SET)) {
            zfree(sets);
            return;
        }
        sets[j] = setobj;
        if (dstkey && setTypeHasVolatileMembers(setobj)) volatile_source = 1;
    }

    /* Set intersection with an empty set always results in an empty set.
     * Return ASAP if there is an empty set.
     *
     * Verbatim propagation is safe here: 'empty' counts only MISSING keys, and
     * a set holding nothing but expired members is still a key, so a replica
     * reaches the same empty result from the same command. */
    if (empty > 0) {
        zfree(sets);
        if (dstkey) {
            if (dbDelete(c->db, dstkey)) {
                signalModifiedKey(c, c->db, dstkey);
                notifyKeyspaceEvent(NOTIFY_GENERIC, "del", dstkey, c->db->id);
                server.dirty++;
            }
            addReply(c, shared.czero);
        } else if (cardinality_only) {
            addReplyLongLong(c, cardinality);
        } else {
            addReply(c, shared.emptyset[c->resp]);
        }
        return;
    }

    /* Sort sets from the smallest to largest, this will improve our
     * algorithm's performance */
    qsort(sets, setnum, sizeof(robj *), qsortCompareSetsByCardinality);

    /* The first thing we should output is the total number of elements...
     * since this is a multi-bulk write, but at this stage we don't know
     * the intersection set size, so we use a trick, append an empty object
     * to the output list and save the pointer to later modify it with the
     * right length */
    if (dstkey) {
        /* If we have a target key where to store the resulting set
         * create this key with an empty set inside */
        if (sets[0]->encoding == OBJ_ENCODING_INTSET) {
            /* The first set is an intset, so the result is an intset too. The
             * elements are inserted in ascending order which is efficient in an
             * intset. */
            dstset = createIntsetObject();
        } else if (sets[0]->encoding == OBJ_ENCODING_LISTPACK) {
            /* To avoid many reallocs, we estimate that the result is a listpack
             * of approximately the same size as the first set. Then we shrink
             * it or possibly convert it to intset in the end. */
            unsigned char *lp = lpNew(lpBytes(objectGetVal(sets[0])));
            dstset = createObject(OBJ_SET, lp);
            dstset->encoding = OBJ_ENCODING_LISTPACK;
        } else {
            /* We start off with a listpack, since it's more efficient to append
             * to than an intset. Later we can convert it to intset or a
             * hashtable. */
            dstset = createSetListpackObject();
        }
    } else if (!cardinality_only) {
        replylen = addReplyDeferredLen(c);
    }

    /* Iterate all the elements of the first (smallest) set, and test
     * the element against all the other sets, if at least one set does
     * not include the element it is discarded */
    int only_integers = 1;
    si = setTypeInitIterator(sets[0]);
    while ((encoding = setTypeNext(si, &str, &len, &intobj)) != -1) {
        for (j = 1; j < setnum; j++) {
            if (sets[j] == sets[0]) continue;
            if (!setTypeIsMemberAux(sets[j], str, len, intobj, encoding == OBJ_ENCODING_HASHTABLE)) break;
        }

        /* Only take action when all sets contain the member */
        if (j == setnum) {
            if (cardinality_only) {
                cardinality++;

                /* We stop the searching after reaching the limit. */
                if (limit && cardinality >= limit) break;
            } else if (!dstkey) {
                if (str != NULL)
                    addReplyBulkCBuffer(c, str, len);
                else
                    addReplyBulkLongLong(c, intobj);
                cardinality++;
            } else {
                if (str && only_integers) {
                    /* It may be an integer although we got it as a string. */
                    if (encoding == OBJ_ENCODING_HASHTABLE && string2ll(str, len, (long long *)&intobj)) {
                        if (dstset->encoding == OBJ_ENCODING_LISTPACK || dstset->encoding == OBJ_ENCODING_INTSET) {
                            /* Adding it as an integer is more efficient. */
                            str = NULL;
                        }
                    } else {
                        /* It's not an integer */
                        only_integers = 0;
                    }
                }
                setTypeAddAux(dstset, str, len, intobj, encoding == OBJ_ENCODING_HASHTABLE);
            }
        }
    }
    setTypeReleaseIterator(si);

    if (cardinality_only) {
        addReplyLongLong(c, cardinality);
    } else if (dstkey) {
        /* Store the resulting set into the target, if the intersection
         * is not an empty set. */
        if (setTypeSize(dstset) > 0) {
            if (only_integers) maybeConvertToIntset(dstset);
            if (dstset->encoding == OBJ_ENCODING_LISTPACK) {
                /* We allocated too much memory when we created it to avoid
                 * frequent reallocs. Therefore, we shrink it now. */
                objectSetVal(dstset, lpShrinkToFit(objectGetVal(dstset)));
            }
            setKey(c, c->db, dstkey, &dstset, 0);
            notifyKeyspaceEvent(NOTIFY_SET, "sinterstore", dstkey, c->db->id);
            server.dirty++;
            addReplyLongLong(c, setTypeSize(dstset));
            if (volatile_source) propagateSetStoreAsEffects(c, dstkey, dstset);
        } else {
            int deleted = dbDelete(c->db, dstkey);
            if (deleted) {
                server.dirty++;
                signalModifiedKey(c, c->db, dstkey);
                notifyKeyspaceEvent(NOTIFY_GENERIC, "del", dstkey, c->db->id);
            }
            addReply(c, shared.czero);
            /* Nothing stored and no destination to remove: a plain set
             * propagates nothing here (server.dirty is untouched), so neither
             * may the by-effect path. */
            if (volatile_source && deleted) propagateSetStoreAsEffects(c, dstkey, NULL);
            decrRefCount(dstset);
        }
    } else {
        setDeferredSetLen(c, replylen, cardinality);
    }
    zfree(sets);
}

/* SINTER key [key ...] */
void sinterCommand(client *c) {
    sinterGenericCommand(c, c->argv + 1, c->argc - 1, NULL, 0, 0);
}

/* SINTERCARD numkeys key [key ...] [LIMIT limit] */
void sinterCardCommand(client *c) {
    long j;
    long numkeys = 0; /* Number of keys. */
    long limit = 0;   /* 0 means not limit. */

    if (getRangeLongFromObjectOrReply(c, c->argv[1], 1, LONG_MAX, &numkeys, "numkeys should be greater than 0") != C_OK)
        return;
    if (numkeys > (c->argc - 2)) {
        addReplyError(c, "Number of keys can't be greater than number of args");
        return;
    }

    for (j = 2 + numkeys; j < c->argc; j++) {
        char *opt = objectGetVal(c->argv[j]);
        int moreargs = (c->argc - 1) - j;

        if (!strcasecmp(opt, "LIMIT") && moreargs) {
            j++;
            if (getPositiveLongFromObjectOrReply(c, c->argv[j], &limit, "LIMIT can't be negative") != C_OK) return;
        } else {
            addReplyErrorObject(c, shared.syntaxerr);
            return;
        }
    }

    sinterGenericCommand(c, c->argv + 2, numkeys, NULL, 1, limit);
}

/* SINTERSTORE destination key [key ...] */
void sinterstoreCommand(client *c) {
    sinterGenericCommand(c, c->argv + 2, c->argc - 2, c->argv[1], 0, 0);
}

/* SUNION / SUNIONSTORE / SDIFF / SDIFFSTORE
 *
 * As in sinterGenericCommand above: the set sizes only pick the SDIFF
 * algorithm, and the STORE variant is propagated by effect when a source
 * carries member TTLs. */
void sunionDiffGenericCommand(client *c, robj **setkeys, int setnum, robj *dstkey, int op) {
    robj **sets = zmalloc(sizeof(robj *) * setnum);
    setTypeIterator *si;
    robj *dstset = NULL;
    int dstset_encoding = OBJ_ENCODING_INTSET;
    char *str;
    size_t len;
    int64_t llval;
    int encoding;
    int j, cardinality = 0;
    int diff_algo = 1;
    int sameset = 0;
    int volatile_source = 0;

    for (j = 0; j < setnum; j++) {
        robj *setobj = lookupKeyRead(c->db, setkeys[j]);
        if (!setobj) {
            sets[j] = NULL;
            continue;
        }
        if (checkType(c, setobj, OBJ_SET)) {
            zfree(sets);
            return;
        }
        /* For a SET's encoding, according to the factory method setTypeCreate(), currently have 3 types:
         * 1. OBJ_ENCODING_INTSET
         * 2. OBJ_ENCODING_LISTPACK
         * 3. OBJ_ENCODING_HASHTABLE
         * 'dstset_encoding' is used to determine which kind of encoding to use when initialize 'dstset'.
         *
         * If all sets are all OBJ_ENCODING_INTSET encoding or 'dstkey' is not null, keep 'dstset'
         * OBJ_ENCODING_INTSET encoding when initialize. Otherwise, it is not efficient to create the 'dstset'
         * from intset and then convert to listpack or hashtable.
         *
         * If one of the set is OBJ_ENCODING_LISTPACK, let's set 'dstset' to hashtable default encoding,
         * the hashtable is more efficient when find and compare than the listpack. The corresponding
         * time complexity are O(1) vs O(n). */
        if (!dstkey && dstset_encoding == OBJ_ENCODING_INTSET &&
            (setobj->encoding == OBJ_ENCODING_LISTPACK || setobj->encoding == OBJ_ENCODING_HASHTABLE)) {
            dstset_encoding = OBJ_ENCODING_HASHTABLE;
        }
        sets[j] = setobj;
        if (dstkey && setTypeHasVolatileMembers(setobj)) volatile_source = 1;
        if (j > 0 && sets[0] == sets[j]) {
            sameset = 1;
        }
    }

    /* Select what DIFF algorithm to use.
     *
     * Algorithm 1 is O(N*M) where N is the size of the element first set
     * and M the total number of sets.
     *
     * Algorithm 2 is O(N) where N is the total number of elements in all
     * the sets.
     *
     * We compute what is the best bet with the current input here. */
    if (op == SET_OP_DIFF && sets[0] && !sameset) {
        long long algo_one_work = 0, algo_two_work = 0;

        for (j = 0; j < setnum; j++) {
            if (sets[j] == NULL) continue;

            algo_one_work += setTypeSize(sets[0]);
            algo_two_work += setTypeSize(sets[j]);
        }

        /* Algorithm 1 has better constant times and performs less operations
         * if there are elements in common. Give it some advantage. */
        algo_one_work /= 2;
        diff_algo = (algo_one_work <= algo_two_work) ? 1 : 2;

        if (diff_algo == 1 && setnum > 1) {
            /* With algorithm 1 it is better to order the sets to subtract
             * by decreasing size, so that we are more likely to find
             * duplicated elements ASAP. */
            qsort(sets + 1, setnum - 1, sizeof(robj *), qsortCompareSetsByRevCardinality);
        }
    }

    /* We need a temp set object to store our union/diff. If the dstkey
     * is not NULL (that is, we are inside an SUNIONSTORE/SDIFFSTORE operation) then
     * this set object will be the resulting object to set into the target key*/
    if (dstset_encoding == OBJ_ENCODING_INTSET) {
        dstset = createIntsetObject();
    } else {
        dstset = createSetObject();
    }

    if (op == SET_OP_UNION) {
        /* Union is trivial, just add every element of every set to the
         * temporary set. */
        for (j = 0; j < setnum; j++) {
            if (!sets[j]) continue; /* nonexistent keys are like empty sets */

            si = setTypeInitIterator(sets[j]);
            while ((encoding = setTypeNext(si, &str, &len, &llval)) != -1) {
                cardinality += setTypeAddAux(dstset, str, len, llval, encoding == OBJ_ENCODING_HASHTABLE);
            }
            setTypeReleaseIterator(si);
        }
    } else if (op == SET_OP_DIFF && sameset) {
        /* At least one of the sets is the same one (same key) as the first one, result must be empty. */
    } else if (op == SET_OP_DIFF && sets[0] && diff_algo == 1) {
        /* DIFF Algorithm 1:
         *
         * We perform the diff by iterating all the elements of the first set,
         * and only adding it to the target set if the element does not exist
         * into all the other sets.
         *
         * This way we perform at max N*M operations, where N is the size of
         * the first set, and M the number of sets. */
        si = setTypeInitIterator(sets[0]);
        while ((encoding = setTypeNext(si, &str, &len, &llval)) != -1) {
            for (j = 1; j < setnum; j++) {
                if (!sets[j]) continue;        /* no key is an empty set. */
                if (sets[j] == sets[0]) break; /* same set! */
                if (setTypeIsMemberAux(sets[j], str, len, llval, encoding == OBJ_ENCODING_HASHTABLE)) break;
            }
            if (j == setnum) {
                /* There is no other set with this element. Add it. */
                cardinality += setTypeAddAux(dstset, str, len, llval, encoding == OBJ_ENCODING_HASHTABLE);
            }
        }
        setTypeReleaseIterator(si);
    } else if (op == SET_OP_DIFF && sets[0] && diff_algo == 2) {
        /* DIFF Algorithm 2:
         *
         * Add all the elements of the first set to the auxiliary set.
         * Then remove all the elements of all the next sets from it.
         *
         * This is O(N) where N is the sum of all the elements in every
         * set. */
        for (j = 0; j < setnum; j++) {
            if (!sets[j]) continue; /* nonexistent keys are like empty sets */

            si = setTypeInitIterator(sets[j]);
            while ((encoding = setTypeNext(si, &str, &len, &llval)) != -1) {
                if (j == 0) {
                    cardinality += setTypeAddAux(dstset, str, len, llval, encoding == OBJ_ENCODING_HASHTABLE);
                } else {
                    cardinality -= setTypeRemoveAux(dstset, str, len, llval, encoding == OBJ_ENCODING_HASHTABLE);
                }
            }
            setTypeReleaseIterator(si);

            /* Exit if result set is empty as any additional removal
             * of elements will have no effect. */
            if (cardinality == 0) break;
        }
    }

    /* Output the content of the resulting set, if not in STORE mode */
    if (!dstkey) {
        addReplySetLen(c, cardinality);
        si = setTypeInitIterator(dstset);
        while (setTypeNext(si, &str, &len, &llval) != -1) {
            if (str)
                addReplyBulkCBuffer(c, str, len);
            else
                addReplyBulkLongLong(c, llval);
        }
        setTypeReleaseIterator(si);
        server.lazyfree_lazy_server_del ? freeObjAsync(NULL, dstset, -1) : decrRefCount(dstset);
    } else {
        /* If we have a target key where to store the resulting set
         * create this key with the result set inside */
        if (setTypeSize(dstset) > 0) {
            setKey(c, c->db, dstkey, &dstset, 0);
            notifyKeyspaceEvent(NOTIFY_SET, op == SET_OP_UNION ? "sunionstore" : "sdiffstore", dstkey, c->db->id);
            server.dirty++;
            addReplyLongLong(c, setTypeSize(dstset));
            if (volatile_source) propagateSetStoreAsEffects(c, dstkey, dstset);
        } else {
            int deleted = dbDelete(c->db, dstkey);
            if (deleted) {
                server.dirty++;
                signalModifiedKey(c, c->db, dstkey);
                notifyKeyspaceEvent(NOTIFY_GENERIC, "del", dstkey, c->db->id);
            }
            addReply(c, shared.czero);
            /* See sinterGenericCommand: nothing stored and nothing removed is a
             * no-op, and a no-op propagates nothing. */
            if (volatile_source && deleted) propagateSetStoreAsEffects(c, dstkey, NULL);
            decrRefCount(dstset);
        }
    }
    zfree(sets);
}

/* SUNION key [key ...] */
void sunionCommand(client *c) {
    sunionDiffGenericCommand(c, c->argv + 1, c->argc - 1, NULL, SET_OP_UNION);
}

/* SUNIONSTORE destination key [key ...] */
void sunionstoreCommand(client *c) {
    sunionDiffGenericCommand(c, c->argv + 2, c->argc - 2, c->argv[1], SET_OP_UNION);
}

/* SDIFF key [key ...] */
void sdiffCommand(client *c) {
    sunionDiffGenericCommand(c, c->argv + 1, c->argc - 1, NULL, SET_OP_DIFF);
}

/* SDIFFSTORE destination key [key ...] */
void sdiffstoreCommand(client *c) {
    sunionDiffGenericCommand(c, c->argv + 2, c->argc - 2, c->argv[1], SET_OP_DIFF);
}

void sscanCommand(client *c) {
    robj *set;
    unsigned long long cursor;

    if (parseScanCursorOrReply(c, objectGetVal(c->argv[2]), &cursor) == C_ERR) return;
    if ((set = lookupKeyReadOrReply(c, c->argv[1], shared.emptyscan)) == NULL || checkType(c, set, OBJ_SET)) return;
    scanGenericCommand(c, set, cursor);
}
