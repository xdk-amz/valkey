/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Exact work-counter contracts on the set-type primitives. These complement
 * tests/unit/setperf-*.tcl with cases that need no RNG luck: how many logical
 * hashtable lookups a single primitive issues, and whether a random selection
 * on a live volatile set ever iterates the table.
 *
 * Only built when the server library was compiled with WORK_COUNTERS (see
 * src/workctr.h); the file is empty otherwise. */

#include "generated_wrappers.hpp"

#ifdef WORK_COUNTERS

#include <cstdio>
#include <cstring>

extern "C" {
#include "expire.h"
#include "listpack.h"
#include "server.h"
#include "smember.h"
#include "workctr.h"
robj *setTypePopRandom(robj *set); /* t_set.c, not exported by server.h */
}

#define NOW 1000000000000LL

/* Index (vset) work is counted in the idx account inside WC_INDEX_BEGIN/END
 * brackets; hashtable/memory counters in `wc` are therefore the set's own. */
#define IDX(f) (wc.f + wc_index.f)

static char names[4096][16];
static const char *namelist[4096];

static const char **memberNames(size_t count) {
    for (size_t i = 0; i < count; i++) {
        snprintf(names[i], sizeof(names[i]), "m%zu", i);
        namelist[i] = names[i];
    }
    return namelist;
}

static void addMembers(robj *o, const char **members, size_t count) {
    for (size_t i = 0; i < count; i++) {
        sds s = sdsnew(members[i]);
        EXPECT_EQ(setTypeAdd(o, s), 1);
        sdsfree(s);
    }
}

/* Convert while empty: a hashtable-encoded set never converts back. */
static robj *makeHashtableSet(size_t count) {
    robj *o = createSetListpackObject();
    setTypeConvert(o, OBJ_ENCODING_HASHTABLE);
    addMembers(o, memberNames(count), count);
    return o;
}

static robj *makeListpackSet(size_t count) {
    robj *o = createSetListpackObject();
    addMembers(o, memberNames(count), count);
    return o;
}

static expiryModificationResult setExpiry(robj *o, const char *member, mstime_t expiry) {
    sds s = sdsnew(member);
    expiryModificationResult res = setTypeSetExpiry(o, s, expiry, 0);
    sdsfree(s);
    return res;
}

static int addWithExpiry(robj *o, const char *member, mstime_t expiry, int flags, bool *replaced) {
    sds s = sdsnew(member);
    bool ttl_changed = false;
    *replaced = false;
    int res = setTypeAddWithExpiry(o, s, expiry, flags, replaced, &ttl_changed);
    sdsfree(s);
    return res;
}

class SetWorkCounterTest : public ::testing::Test {
  protected:
    static void SetUpTestSuite() {
        memset(&server, 0, sizeof(valkeyServer));
        server.set_max_listpack_entries = 128;
        server.set_max_listpack_value = 64;
        server.hz = 10;
    }

    void SetUp() override {
        server.cmd_time_snapshot = NOW;
        wcReset();
    }
};

/* --- random selection: a live volatile hashtable set is never iterated --- */

TEST_F(SetWorkCounterTest, randomElementOnOneTtlHashtableDoesNotIterate) {
    robj *o = makeHashtableSet(1000);
    ASSERT_EQ(setExpiry(o, "m0", NOW + 100000), EXPIRATION_MODIFICATION_SUCCESSFUL);
    wcReset();
    for (int i = 0; i < 50; i++) {
        char *str;
        size_t len;
        int64_t llele;
        ASSERT_EQ(setTypeRandomElement(o, &str, &len, &llele), OBJ_ENCODING_HASHTABLE);
    }
    /* Contract: sampling only, no full traversal, no reservoir pass. */
    EXPECT_EQ(wc.ht_iter_visits, 0);
    EXPECT_EQ(wc.set_reservoir_passes, 0);
    EXPECT_EQ(wc.set_random_expired_seen, 0);
    decrRefCount(o);
}

TEST_F(SetWorkCounterTest, popRandomOnOneTtlHashtableDoesNotIterate) {
    robj *o = makeHashtableSet(1000);
    ASSERT_EQ(setExpiry(o, "m0", NOW + 100000), EXPIRATION_MODIFICATION_SUCCESSFUL);
    wcReset();
    for (int i = 0; i < 20; i++) {
        robj *popped = setTypePopRandom(o);
        ASSERT_NE(popped, (robj *)NULL);
        decrRefCount(popped);
    }
    EXPECT_EQ(setTypeSize(o), 980u);
    EXPECT_EQ(wc.ht_iter_visits, 0);
    EXPECT_EQ(wc.set_reservoir_passes, 0);
    decrRefCount(o);
}

/* --- insertion: one probe to find/replace the slot plus one insert position --- */

TEST_F(SetWorkCounterTest, addAbsentMemberToVolatileHashtableUsesAtMostTwoLookups) {
    robj *o = makeHashtableSet(100);
    ASSERT_EQ(setExpiry(o, "m0", NOW + 100000), EXPIRATION_MODIFICATION_SUCCESSFUL);
    bool replaced;

    wcReset();
    ASSERT_EQ(addWithExpiry(o, "absent-1", EXPIRY_NONE, SET_ADD_KEEP_EXPIRY, &replaced), 1);
    EXPECT_FALSE(replaced);
    int64_t plain_add_lookups = wc.ht_lookups;
    EXPECT_LE(wc.ht_lookups, 2) << "SADD path: live probe + failed physical delete + insert is one lookup too many";
    EXPECT_EQ(wc.ht_pops, 0);

    wcReset();
    ASSERT_EQ(addWithExpiry(o, "absent-2", NOW + 5000, 0, &replaced), 1);
    EXPECT_FALSE(replaced);
    EXPECT_LE(wc.ht_lookups, 2) << "SADDEX path: expiry probe + failed physical delete + insert is one lookup too many";
    EXPECT_EQ(wc.ht_pops, 0);
    EXPECT_EQ(IDX(vset_adds), 1);

    (void)plain_add_lookups;
    decrRefCount(o);
}

TEST_F(SetWorkCounterTest, addExistingLiveMemberIsOneLookup) {
    robj *o = makeHashtableSet(100);
    ASSERT_EQ(setExpiry(o, "m0", NOW + 100000), EXPIRATION_MODIFICATION_SUCCESSFUL);
    bool replaced;

    wcReset();
    ASSERT_EQ(addWithExpiry(o, "m5", EXPIRY_NONE, SET_ADD_KEEP_EXPIRY, &replaced), 0);
    EXPECT_EQ(wc.ht_lookups, 1);
    EXPECT_EQ(wc.ht_inserts, 0);

    wcReset();
    ASSERT_EQ(addWithExpiry(o, "m5", NOW + 9000, 0, &replaced), 0);
    EXPECT_EQ(wc.ht_lookups, 1);
    EXPECT_EQ(IDX(vset_adds), 1); /* the TTL it did not have is legitimate index work */
    decrRefCount(o);
}

TEST_F(SetWorkCounterTest, replacingExpiredMemberIsOneProbeOnePopOneInsert) {
    robj *o = makeHashtableSet(100);
    ASSERT_EQ(setExpiry(o, "m0", NOW + 100000), EXPIRATION_MODIFICATION_SUCCESSFUL);
    ASSERT_EQ(setExpiry(o, "m7", NOW + 1000), EXPIRATION_MODIFICATION_SUCCESSFUL);
    server.cmd_time_snapshot = NOW + 2000;
    bool replaced;

    wcReset();
    ASSERT_EQ(addWithExpiry(o, "m7", EXPIRY_NONE, SET_ADD_KEEP_EXPIRY, &replaced), 1);
    EXPECT_TRUE(replaced);
    /* Legitimate: the expired physical entry is popped and unindexed once. */
    EXPECT_EQ(wc.ht_pops, 1);
    EXPECT_EQ(IDX(vset_removes), 1);
    EXPECT_EQ(wc.set_members_reclaimed, 1);
    EXPECT_EQ(wc.ht_inserts, 1);
    /* Not legitimate: probing the same member a third time. */
    EXPECT_LE(wc.ht_lookups, 2) << "validating probe + non-validating pop + insert probe is one lookup too many";
    EXPECT_EQ(setTypeSize(o), 100u);
    decrRefCount(o);
}

/* --- TTL primitives are single lookups --- */

TEST_F(SetWorkCounterTest, setAndGetExpiryAreSingleLookups) {
    robj *o = makeHashtableSet(100);
    wcReset();
    ASSERT_EQ(setExpiry(o, "m3", NOW + 100000), EXPIRATION_MODIFICATION_SUCCESSFUL);
    EXPECT_EQ(wc.ht_lookups, 1);
    EXPECT_EQ(IDX(vset_adds), 1);
    EXPECT_EQ(wc.ht_iter_visits, 0);

    wcReset();
    sds s = sdsnew("m3");
    mstime_t expiry;
    ASSERT_EQ(setTypeGetExpiry(o, s, &expiry), C_OK);
    sdsfree(s);
    EXPECT_EQ(expiry, NOW + 100000);
    EXPECT_EQ(wc.ht_lookups, 1);

    wcReset();
    ASSERT_EQ(setExpiry(o, "m3", EXPIRY_NONE), EXPIRATION_MODIFICATION_SUCCESSFUL);
    EXPECT_EQ(wc.ht_lookups, 1);
    EXPECT_EQ(IDX(vset_removes), 1);
    EXPECT_EQ(wc.ht_iter_visits, 0);
    decrRefCount(o);
}

/* --- listpack: a TTL adds validation, not extra traversals --- */

TEST_F(SetWorkCounterTest, listpackAddAbsentMemberIsOneFind) {
    robj *o = makeListpackSet(32);
    ASSERT_EQ(setExpiry(o, "m0", NOW + 100000), EXPIRATION_MODIFICATION_SUCCESSFUL);
    bool replaced;
    wcReset();
    ASSERT_EQ(addWithExpiry(o, "absent", EXPIRY_NONE, SET_ADD_KEEP_EXPIRY, &replaced), 1);
    EXPECT_LE(wc.lp_find_calls, 1) << "a second lpFind for a member that never existed";
    EXPECT_EQ(wc.lp_inserts, 1);
    decrRefCount(o);
}

TEST_F(SetWorkCounterTest, listpackRandomElementOnOneTtlSetIsOneTraversal) {
    const size_t n = 64;
    robj *o = makeListpackSet(n);
    ASSERT_EQ(setExpiry(o, "m0", NOW + 100000), EXPIRATION_MODIFICATION_SUCCESSFUL);
    wcReset();
    char *str;
    size_t len;
    int64_t llele;
    ASSERT_EQ(setTypeRandomElement(o, &str, &len, &llele), OBJ_ENCODING_LISTPACK);
    EXPECT_LE(wc.lp_next_steps + wc.lp_random_steps + wc.lp_find_steps, (int64_t)(2 * n));
    decrRefCount(o);
}

/* --- reclaim: expired members are removed exactly once, no second scan --- */

TEST_F(SetWorkCounterTest, deleteExpiredMembersVisitsEachExpiredEntryOnce) {
    const size_t n = 1000;
    robj *o = makeHashtableSet(n);
    const char **members = memberNames(n);
    for (size_t i = 0; i < n; i += 2) ASSERT_EQ(setExpiry(o, members[i], NOW + 1000), EXPIRATION_MODIFICATION_SUCCESSFUL);
    wcReset();
    ASSERT_EQ(setTypeDeleteExpiredMembers(o, NOW + 2000, n, NULL), n / 2);
    EXPECT_EQ(wc.set_members_reclaimed, (int64_t)(n / 2));
    EXPECT_EQ(wc.ht_pops, (int64_t)(n / 2));
    EXPECT_LE(IDX(vset_entry_visits), (int64_t)(n / 2 + 16));
    /* The set's own table is never iterated; the index's internal tables may be. */
    EXPECT_EQ(wc.ht_iter_visits, 0);
    EXPECT_LE(wc_index.ht_iter_visits, (int64_t)(n / 2 + 16));
    EXPECT_EQ(setTypeSize(o), n / 2);
    EXPECT_FALSE(setTypeHasVolatileMembers(o));
    decrRefCount(o);
}

#endif /* WORK_COUNTERS */
