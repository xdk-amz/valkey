/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Unit tests for the state of src/t_set_volatile.c that the integration tests
 * cannot reach: the listpack metadata header, which hashtable type a set is on,
 * the vset in the reserved metadata tail, and defrag relocation, where the
 * point is a defragfn that always moves the allocation. */

#include "generated_wrappers.hpp"

#include <cstdio>
#include <cstring>

extern "C" {
#include "expire.h"
#include "listpack.h"
#include "server.h"
#include "smember.h"
}

#define NOW 1000000000000LL /* fixed clock, ms */

/* Fields of the shared 'server' global this code reads. They are saved and
 * restored rather than bzero'd so that no other test suite is disturbed. */
typedef struct savedServerState {
    size_t max_listpack_entries;
    size_t max_listpack_value;
    mstime_t snapshot;
    int loading;
    char *primary_host;
    int import_mode;
    client *current_client;
    uint32_t paused_actions;
} savedServerState;

static void serverStateSaveAndConfigure(savedServerState *saved) {
    saved->max_listpack_entries = server.set_max_listpack_entries;
    saved->max_listpack_value = server.set_max_listpack_value;
    saved->snapshot = server.cmd_time_snapshot;
    saved->loading = server.loading;
    saved->primary_host = server.primary_host;
    saved->import_mode = server.import_mode;
    saved->current_client = server.current_client;
    saved->paused_actions = server.paused_actions;

    server.set_max_listpack_entries = 128;
    server.set_max_listpack_value = 64;
    server.cmd_time_snapshot = NOW;
    /* Act as a primary outside loading/import so that expiry decisions are
     * taken locally (POLICY_DELETE_EXPIRED, checkAlreadyExpired active). */
    server.loading = 0;
    server.primary_host = NULL;
    server.import_mode = 0;
    server.current_client = NULL;
    server.paused_actions = 0;
}

static void serverStateRestore(const savedServerState *saved) {
    server.set_max_listpack_entries = saved->max_listpack_entries;
    server.set_max_listpack_value = saved->max_listpack_value;
    server.cmd_time_snapshot = saved->snapshot;
    server.loading = saved->loading;
    server.primary_host = saved->primary_host;
    server.import_mode = saved->import_mode;
    server.current_client = saved->current_client;
    server.paused_actions = saved->paused_actions;
}

static robj *makeListpackSet(const char **members, size_t count) {
    robj *o = createSetListpackObject();
    for (size_t i = 0; i < count; i++) {
        sds s = sdsnew(members[i]);
        EXPECT_EQ(setTypeAdd(o, s), 1);
        sdsfree(s);
    }
    EXPECT_EQ(objectGetEncoding(o), OBJ_ENCODING_LISTPACK);
    return o;
}

static robj *makeHashtableSet(const char **members, size_t count) {
    robj *o = createSetListpackObject();
    for (size_t i = 0; i < count; i++) {
        sds s = sdsnew(members[i]);
        EXPECT_EQ(setTypeAdd(o, s), 1);
        sdsfree(s);
    }
    if (objectGetEncoding(o) != OBJ_ENCODING_HASHTABLE) setTypeConvert(o, OBJ_ENCODING_HASHTABLE);
    EXPECT_EQ(objectGetEncoding(o), OBJ_ENCODING_HASHTABLE);
    return o;
}

static int setExpiry(robj *o, const char *member, mstime_t expiry, int flags = 0) {
    sds s = sdsnew(member);
    int res = setTypeSetExpiry(o, s, expiry, flags);
    sdsfree(s);
    return res;
}

/* C_OK plus the stored expiry, or C_ERR when the member is not visible. */
static int getExpiry(robj *o, const char *member, mstime_t *expiry) {
    sds s = sdsnew(member);
    int res = setTypeGetExpiry(o, s, expiry);
    sdsfree(s);
    return res;
}

/* Address of the bucket slot holding 'member', as the defrag scan sees it. */
static smember **memberRef(robj *o, const char *member) {
    sds s = sdsnew(member);
    void **ref = hashtableFindRef((hashtable *)objectGetVal(o), s);
    sdsfree(s);
    return (smember **)ref;
}

static hashtableType *setType(robj *o) {
    return hashtableGetType((hashtable *)objectGetVal(o));
}

/* True when the reserved metadata tail holds an initialized vset. */
static bool hasVset(robj *o) {
    return vsetIsValid((vset *)hashtableMetadata((hashtable *)objectGetVal(o)));
}

/* Actively expire with the clock at 'now'; returns the number of members gone. */
static size_t expireAt(robj *o, mstime_t now) {
    robj *removed[16];
    server.cmd_time_snapshot = now;
    size_t n = setTypeDeleteExpiredMembers(o, now, 16, removed);
    for (size_t i = 0; i < n; i++) decrRefCount(removed[i]);
    server.cmd_time_snapshot = NOW;
    return n;
}

class SetVolatileListpackTest : public ::testing::Test {
  protected:
    savedServerState saved;
    robj *bracketed; /* set left inside an ignore-TTL bracket, if any */

    void SetUp() override {
        serverStateSaveAndConfigure(&saved);
        bracketed = NULL;
    }

    /* The listpack bracket is process-wide state, so an assertion that fires
     * inside it must not leak it into the next test. */
    void TearDown() override {
        if (bracketed) setTypeIgnoreTTL(bracketed, false);
        serverStateRestore(&saved);
    }

    void ignoreTTL(robj *o, bool ignore) {
        setTypeIgnoreTTL(o, ignore);
        bracketed = ignore ? o : NULL;
    }
};

/* The aggregate header is created on the 0->1 transition, kept in sync, and
 * removed on the 1->0 transition, so a set without TTLs pays nothing. */
TEST_F(SetVolatileListpackTest, volatileCountHeaderLifecycle) {
    const char *members[] = {"a", "b", "c"};
    robj *o = makeListpackSet(members, sizeof(members) / sizeof(members[0]));
    ASSERT_FALSE(setTypeHasVolatileMembers(o));
    ASSERT_EQ(setTypeVolatileCount(o), 0);
    ASSERT_FALSE(lpIsMetadata(lpStart((unsigned char *)objectGetVal(o))));

    ASSERT_EQ(setExpiry(o, "a", NOW + 5000), SET_EXPIRY_SET);
    ASSERT_TRUE(setTypeHasVolatileMembers(o));
    ASSERT_EQ(setTypeVolatileCount(o), 1);
    ASSERT_TRUE(lpIsMetadata(lpStart((unsigned char *)objectGetVal(o))));
    ASSERT_EQ(setTypeSize(o), 3u); /* metadata entries are not members */

    ASSERT_EQ(setExpiry(o, "c", NOW + 9000), SET_EXPIRY_SET);
    ASSERT_EQ(setTypeVolatileCount(o), 2);

    /* Refreshing an existing expiry must not change the count. */
    ASSERT_EQ(setExpiry(o, "a", NOW + 7000), SET_EXPIRY_SET);
    ASSERT_EQ(setTypeVolatileCount(o), 2);

    ASSERT_EQ(setExpiry(o, "a", EXPIRY_NONE), SET_EXPIRY_SET);
    ASSERT_EQ(setTypeVolatileCount(o), 1);

    ASSERT_EQ(setExpiry(o, "c", EXPIRY_NONE), SET_EXPIRY_SET);
    ASSERT_EQ(setTypeVolatileCount(o), 0);
    ASSERT_FALSE(setTypeHasVolatileMembers(o));
    ASSERT_FALSE(lpIsMetadata(lpStart((unsigned char *)objectGetVal(o))));
    ASSERT_EQ(setTypeSize(o), 3u);

    decrRefCount(o);
}

/* An expired member is invisible until an ignore-TTL bracket makes it visible
 * again, and is still counted while it has not been removed. */
TEST_F(SetVolatileListpackTest, ignoreTTLBracketRevealsExpiredMembers) {
    const char *members[] = {"a", "b"};
    robj *o = makeListpackSet(members, sizeof(members) / sizeof(members[0]));
    ASSERT_EQ(setExpiry(o, "a", NOW + 1000), SET_EXPIRY_SET);
    server.cmd_time_snapshot = NOW + 2000;

    mstime_t expiry = EXPIRY_NONE;
    ASSERT_EQ(getExpiry(o, "a", &expiry), C_ERR);
    ASSERT_EQ(setTypeSize(o), 2u);
    ASSERT_EQ(setTypeVolatileCount(o), 1);

    ignoreTTL(o, true);
    ASSERT_EQ(getExpiry(o, "a", &expiry), C_OK);
    ASSERT_EQ(expiry, NOW + 1000);
    ignoreTTL(o, false);
    ASSERT_EQ(getExpiry(o, "a", &expiry), C_ERR);

    decrRefCount(o);
}

/*-----------------------------------------------------------------------------
 * Hashtable encoding
 *----------------------------------------------------------------------------*/

/* A defragfn that ALWAYS moves. It mirrors activeDefragAlloc (copy, then
 * release the old allocation) but allocates BEFORE freeing, so the new pointer
 * is guaranteed to differ and a consumer holding the stale one really is
 * looking at freed memory. */
static void *alwaysMoveDefragAlloc(void *ptr) {
    size_t size = zmalloc_usable_size(ptr);
    void *newptr = zmalloc(size);
    memcpy(newptr, ptr, size);
    zfree(ptr);
    return newptr;
}

class SetVolatileHashtableTest : public ::testing::Test {
  protected:
    savedServerState saved;

    void SetUp() override {
        serverStateSaveAndConfigure(&saved);
    }

    void TearDown() override {
        serverStateRestore(&saved);
    }
};

/* The three types are swapped in place on a live hashtable, so all of them must
 * reserve the same metadata tail. */
TEST_F(SetVolatileHashtableTest, allSetTypesReserveTheSameMetadataTail) {
    ASSERT_EQ(setHashtableType.getMetadataSize(), sizeof(vset));
    ASSERT_EQ(setWithVolatileMembersHashtableType.getMetadataSize(), sizeof(vset));
    ASSERT_EQ(setVolatileIgnoreTTLHashtableType.getMetadataSize(), sizeof(vset));
}

/* The first TTL moves the set onto the validating volatile type and initializes
 * the vset in the reserved tail; the last one moves it back and releases the
 * vset. The struct is never reallocated, so the value pointer the robj holds is
 * the same one throughout. */
TEST_F(SetVolatileHashtableTest, firstAndLastExpirySwapTheHashtableType) {
    const char *members[] = {"a", "b", "c"};
    robj *o = makeHashtableSet(members, sizeof(members) / sizeof(members[0]));
    void *val_before = objectGetVal(o);

    ASSERT_EQ(setType(o), &setHashtableType);
    ASSERT_FALSE(hasVset(o));
    ASSERT_FALSE(setTypeHasVolatileMembers(o));

    ASSERT_EQ(setExpiry(o, "a", NOW + 1000), SET_EXPIRY_SET);
    ASSERT_EQ(setType(o), &setWithVolatileMembersHashtableType);
    ASSERT_TRUE(hasVset(o));
    ASSERT_EQ(objectGetVal(o), val_before) << "a type swap must not move the hashtable struct";

    /* A second TTL changes nothing about the type. */
    ASSERT_EQ(setExpiry(o, "b", NOW + 2000), SET_EXPIRY_SET);
    ASSERT_EQ(setType(o), &setWithVolatileMembersHashtableType);

    /* One TTL left: still volatile. */
    ASSERT_EQ(setExpiry(o, "a", EXPIRY_NONE), SET_EXPIRY_SET);
    ASSERT_EQ(setType(o), &setWithVolatileMembersHashtableType);
    ASSERT_TRUE(hasVset(o));

    ASSERT_EQ(setExpiry(o, "b", EXPIRY_NONE), SET_EXPIRY_SET);
    ASSERT_EQ(setType(o), &setHashtableType);
    ASSERT_FALSE(hasVset(o));
    ASSERT_FALSE(setTypeHasVolatileMembers(o));
    ASSERT_EQ(objectGetVal(o), val_before);
    ASSERT_EQ(setTypeSize(o), 3u);

    /* And the set can become volatile again afterwards. */
    ASSERT_EQ(setExpiry(o, "c", NOW + 3000), SET_EXPIRY_SET);
    ASSERT_EQ(setType(o), &setWithVolatileMembersHashtableType);
    ASSERT_TRUE(hasVset(o));

    decrRefCount(o);
}

/* Removing the last volatile MEMBER, rather than its TTL, swaps back too. */
TEST_F(SetVolatileHashtableTest, removingLastVolatileMemberSwapsTypeBack) {
    const char *members[] = {"a", "b"};
    robj *o = makeHashtableSet(members, sizeof(members) / sizeof(members[0]));
    ASSERT_EQ(setExpiry(o, "a", NOW + 1000), SET_EXPIRY_SET);
    ASSERT_EQ(setType(o), &setWithVolatileMembersHashtableType);

    sds s = sdsnew("a");
    ASSERT_EQ(setTypeRemoveVolatile(o, s), 1);
    sdsfree(s);
    ASSERT_EQ(setType(o), &setHashtableType);
    ASSERT_FALSE(hasVset(o));
    ASSERT_EQ(setTypeSize(o), 1u);

    decrRefCount(o);
}

/* Active expiration reaches zero volatile members through the vset and swaps
 * the type back once it is empty, inside a bracket that must not leave the set
 * on the ignore-TTL type. */
TEST_F(SetVolatileHashtableTest, reclaimingTheLastVolatileMemberSwapsTypeBack) {
    const char *members[] = {"a", "b", "c"};
    robj *o = makeHashtableSet(members, sizeof(members) / sizeof(members[0]));
    ASSERT_EQ(setExpiry(o, "a", NOW + 1000), SET_EXPIRY_SET);
    ASSERT_EQ(setExpiry(o, "b", NOW + 5000), SET_EXPIRY_SET);

    /* Only "a" is due. */
    ASSERT_EQ(expireAt(o, NOW + 2000), 1u);
    ASSERT_EQ(setType(o), &setWithVolatileMembersHashtableType);
    ASSERT_TRUE(hasVset(o));

    ASSERT_EQ(expireAt(o, NOW + 6000), 1u);
    ASSERT_EQ(setType(o), &setHashtableType);
    ASSERT_FALSE(hasVset(o));
    ASSERT_EQ(setTypeSize(o), 1u);

    decrRefCount(o);
}

/* setHashtableType frees members with sdsfree and the volatile types with
 * smemberFree, so a member carries the expiry prefix only while its set is on a
 * volatile type: any other combination frees an interior pointer or leaks the
 * prefix. */
TEST_F(SetVolatileHashtableTest, memberLayoutMatchesTheDestructorOfItsType) {
    const char *members[] = {"a", "b"};
    robj *o = makeHashtableSet(members, sizeof(members) / sizeof(members[0]));
    ASSERT_FALSE(smemberHasExpiry(*memberRef(o, "a")));

    ASSERT_EQ(setExpiry(o, "a", NOW + 1000), SET_EXPIRY_SET);
    ASSERT_EQ(setType(o), &setWithVolatileMembersHashtableType);
    ASSERT_TRUE(smemberHasExpiry(*memberRef(o, "a")));
    ASSERT_FALSE(smemberHasExpiry(*memberRef(o, "b")));

    ASSERT_EQ(setExpiry(o, "a", EXPIRY_NONE), SET_EXPIRY_SET);
    ASSERT_EQ(setType(o), &setHashtableType);
    ASSERT_FALSE(smemberHasExpiry(*memberRef(o, "a")));

    decrRefCount(o);
}

/* The two cursor phases of setTypeScanDefrag: every member moves, the
 * vset internals move, and both the reads and active expiration still work. */
TEST_F(SetVolatileHashtableTest, scanDefragVolatileMovesMembersAndVset) {
    char names[200][16];
    const char *members[200];
    const size_t count = sizeof(members) / sizeof(members[0]);
    const size_t volatile_count = count / 2;

    for (size_t i = 0; i < count; i++) {
        snprintf(names[i], sizeof(names[i]), "member:%zu", i);
        members[i] = names[i];
    }
    robj *o = makeHashtableSet(members, count);

    /* Every other member becomes volatile, with staggered expiries so that the
     * vset grows past its single-pointer and vector layouts. */
    for (size_t i = 0; i < count; i += 2) {
        ASSERT_EQ(setExpiry(o, members[i], NOW + 1000 + (mstime_t)i), SET_EXPIRY_SET);
    }
    ASSERT_EQ(setTypeVolatileCount(o), (long long)volatile_count);

    size_t cursor = 0;
    int passes = 0;
    do {
        cursor = setTypeScanDefrag(o, cursor, alwaysMoveDefragAlloc);
        ASSERT_LT(++passes, 1000) << "scan did not terminate";
    } while (cursor != 0);
    ASSERT_GE(passes, 2) << "expected a member phase and a vset phase";

    /* Nothing was lost and every TTL survived the move. */
    ASSERT_EQ(setTypeSize(o), count);
    ASSERT_EQ(setTypeVolatileCount(o), (long long)volatile_count);
    for (size_t i = 0; i < count; i++) {
        mstime_t expiry = EXPIRY_NONE;
        ASSERT_EQ(getExpiry(o, members[i], &expiry), C_OK) << members[i];
        ASSERT_EQ(expiry, (i % 2 == 0) ? NOW + 1000 + (mstime_t)i : EXPIRY_NONE) << members[i];
    }

    /* The vset still indexes the moved members, so they can be reclaimed. */
    size_t removed = 0;
    for (int round = 0; round < 20 && removed < volatile_count; round++) removed += expireAt(o, NOW + 100000);
    ASSERT_EQ(removed, volatile_count);
    ASSERT_FALSE(setTypeHasVolatileMembers(o));
    ASSERT_EQ(setTypeSize(o), count - volatile_count);

    decrRefCount(o);
}
