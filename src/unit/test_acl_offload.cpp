/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Deterministic unit coverage for the acl-offload state machine: an IO thread
 * tags an ALLOW verdict + ACL epoch at admission, and the main-thread executor
 * consumes it only while the epoch still matches. DENY, unknown, dynamic and
 * epoch-moved cases fall back to the full main check. These tests drive the
 * pure helpers (consume / bump / same-read cutoff / mark-bound / quiesce) on
 * plain client/user state; the concurrent memory-safety discipline is exercised
 * by the real-engine Tcl suite and under ASan. */

#include "generated_wrappers.hpp"

#include <cstring>

extern "C" {
#include "fastpath.h"
#include "io_threads.h"
#include "module.h"
#include "server.h"
extern hashtableType commandSetType;
void createSharedObjects(void);
void ACLFreeUser(user *u);
}

class AclOffloadTest : public ::testing::Test {
  protected:
    static void SetUpTestSuite() {
        monotonicInit();
        memset(&server, 0, sizeof(server));
        server.hz = CONFIG_DEFAULT_HZ;
        server.logfile = const_cast<char *>("/dev/null");
        server.verbosity = LL_WARNING;
        server.main_thread_id = pthread_self();
        server.el = aeCreateEventLoop(64);
        createSharedObjects();
        moduleInitModulesSystem();
        server.commands = hashtableCreate(&commandSetType);
        server.orig_commands = hashtableCreate(&commandSetType);
        populateCommandTable();
        ACLInit();
        server.dbnum = 1;
        server.db = static_cast<serverDb **>(zcalloc(sizeof(serverDb *)));
        server.db[0] = static_cast<serverDb *>(zcalloc(sizeof(serverDb)));
        server.db[0]->id = 0;
        /* Feature on for most tests; io-threads>1 + fast path so aclOffloadActive(). */
        server.acl_offload = 1;
        server.io_threads_fast_path = 1;
        server.io_threads_num = 2;
    }

    /* A restricted user: read + SET on key pattern foo:* only. */
    static user *makeRestricted() {
        user *u = ACLCreateUnlinkedUser();
        EXPECT_EQ(ACLSetUser(u, "on", -1), C_OK);
        EXPECT_EQ(ACLSetUser(u, "~foo:*", -1), C_OK);
        EXPECT_EQ(ACLSetUser(u, "+@read", -1), C_OK);
        EXPECT_EQ(ACLSetUser(u, "+set", -1), C_OK);
        return u;
    }

    static void freeObjs(robj **argv, int argc) {
        for (int i = 0; i < argc; i++) decrRefCount(argv[i]);
    }
};

/* An ALLOW verdict + fresh epoch is consumed with no main-thread ACL evaluation. */
TEST_F(AclOffloadTest, AllowVerdictConsumedWhileEpochMatches) {
    user *u = makeRestricted();
    aclMarkUserBound(u);
    robj *argv[3] = {createStringObject("SET", 3), createStringObject("foo:1", 5), createStringObject("v", 1)};
    client ec;
    memset(&ec, 0, sizeof(ec));
    ec.user = u;
    ec.db = server.db[0];
    ec.argv = argv;
    ec.argc = 3;
    ec.parsed_cmd = lookupCommandByCString("set");
    ec.cmd = ec.parsed_cmd;
    ec.read_flags = READ_FLAGS_PARSING_COMPLETED | READ_FLAGS_ACL_ALLOWED;
    ec.acl_epoch_seen = aclOffloadEpoch();

    long long hits0 = server.stat_acl_offload_hits;
    int idx = 0;
    EXPECT_EQ(aclOffloadConsume(&ec, &idx), ACL_OK);
    EXPECT_EQ(server.stat_acl_offload_hits, hits0 + 1);
    EXPECT_FALSE(ec.read_flags & READ_FLAGS_ACL_ALLOWED); /* bit consumed */

    ACLFreeUser(u);
    freeObjs(argv, 3);
}

/* A verdict tagged under an old epoch is punted and re-evaluated on main. */
TEST_F(AclOffloadTest, StaleEpochVerdictIsPunted) {
    user *u = makeRestricted();
    aclMarkUserBound(u);
    robj *argv[3] = {createStringObject("SET", 3), createStringObject("foo:1", 5), createStringObject("v", 1)};
    client ec;
    memset(&ec, 0, sizeof(ec));
    ec.user = u;
    ec.db = server.db[0];
    ec.argv = argv;
    ec.argc = 3;
    ec.parsed_cmd = lookupCommandByCString("set");
    ec.cmd = ec.parsed_cmd;
    ec.read_flags = READ_FLAGS_PARSING_COMPLETED | READ_FLAGS_ACL_ALLOWED;
    ec.acl_epoch_seen = aclOffloadEpoch();

    aclOffloadBumpEpoch(); /* an ACL mutation happened after the verdict was tagged */

    long long punts0 = server.stat_acl_offload_punts;
    long long hits0 = server.stat_acl_offload_hits;
    int idx = 0;
    /* SET foo:1 is still permitted, so the re-check on main returns ACL_OK, but it was a PUNT. */
    EXPECT_EQ(aclOffloadConsume(&ec, &idx), ACL_OK);
    EXPECT_EQ(server.stat_acl_offload_punts, punts0 + 1);
    EXPECT_EQ(server.stat_acl_offload_hits, hits0); /* not counted as a hit */

    ACLFreeUser(u);
    freeObjs(argv, 3);
}

/* With no ALLOW bit set (a denial or an untagged command), consume runs the full
 * main check and produces the canonical command error. */
TEST_F(AclOffloadTest, NoVerdictBitRunsFullCheckWithCmdError) {
    user *u = makeRestricted();
    aclMarkUserBound(u);
    robj *argv[2] = {createStringObject("DEL", 3), createStringObject("foo:1", 5)};
    client ec;
    memset(&ec, 0, sizeof(ec));
    ec.user = u;
    ec.db = server.db[0];
    ec.argv = argv;
    ec.argc = 2;
    ec.parsed_cmd = lookupCommandByCString("del");
    ec.cmd = ec.parsed_cmd;
    ec.read_flags = READ_FLAGS_PARSING_COMPLETED; /* NO allow bit: a denial is never tagged */
    ec.acl_epoch_seen = aclOffloadEpoch();

    long long hits0 = server.stat_acl_offload_hits, punts0 = server.stat_acl_offload_punts;
    int idx = -1;
    EXPECT_EQ(aclOffloadConsume(&ec, &idx), ACL_DENIED_CMD);
    EXPECT_EQ(server.stat_acl_offload_hits, hits0);
    EXPECT_EQ(server.stat_acl_offload_punts, punts0);

    ACLFreeUser(u);
    freeObjs(argv, 2);
}

/* Wrong-key command is denied with the exact offending arg index preserved. */
TEST_F(AclOffloadTest, WrongKeyDeniedWithKeyError) {
    user *u = makeRestricted();
    aclMarkUserBound(u);
    robj *argv[3] = {createStringObject("SET", 3), createStringObject("other:1", 7), createStringObject("v", 1)};
    client ec;
    memset(&ec, 0, sizeof(ec));
    ec.user = u;
    ec.db = server.db[0];
    ec.argv = argv;
    ec.argc = 3;
    ec.parsed_cmd = lookupCommandByCString("set");
    ec.cmd = ec.parsed_cmd;
    ec.read_flags = READ_FLAGS_PARSING_COMPLETED;
    ec.acl_epoch_seen = aclOffloadEpoch();
    int idx = -1;
    EXPECT_EQ(aclOffloadConsume(&ec, &idx), ACL_DENIED_KEY);
    EXPECT_GE(idx, 0);
    ACLFreeUser(u);
    freeObjs(argv, 3);
}

/* A verdict is never trusted inside MULTI (tagging stops at MULTI). */
TEST_F(AclOffloadTest, VerdictNotTrustedInsideMulti) {
    user *u = makeRestricted();
    aclMarkUserBound(u);
    robj *argv[3] = {createStringObject("SET", 3), createStringObject("foo:1", 5), createStringObject("v", 1)};
    client ec;
    memset(&ec, 0, sizeof(ec));
    ec.user = u;
    ec.db = server.db[0];
    ec.argv = argv;
    ec.argc = 3;
    ec.parsed_cmd = lookupCommandByCString("set");
    ec.cmd = ec.parsed_cmd;
    ec.read_flags = READ_FLAGS_PARSING_COMPLETED | READ_FLAGS_ACL_ALLOWED;
    ec.acl_epoch_seen = aclOffloadEpoch();
    ec.flag.multi = 1;
    multiState ms;
    memset(&ms, 0, sizeof(ms));
    ms.transaction_db_id = 0;
    ec.mstate = &ms; /* ACLCheckAllPerm reads transaction_db_id under multi */
    long long hits0 = server.stat_acl_offload_hits;
    int idx = 0;
    EXPECT_EQ(aclOffloadConsume(&ec, &idx), ACL_OK); /* re-checked on main, allowed */
    EXPECT_EQ(server.stat_acl_offload_hits, hits0);  /* multi is never a fast-path hit */
    ACLFreeUser(u);
    freeObjs(argv, 3);
}

/* Same-read cutoff: identity/db-changing commands stop tagging for the rest of the read. */
TEST_F(AclOffloadTest, SameReadCutoffPredicate) {
    EXPECT_TRUE(aclOffloadShouldStopTagging(lookupCommandByCString("auth")));
    EXPECT_TRUE(aclOffloadShouldStopTagging(lookupCommandByCString("hello")));
    EXPECT_TRUE(aclOffloadShouldStopTagging(lookupCommandByCString("reset")));
    EXPECT_TRUE(aclOffloadShouldStopTagging(lookupCommandByCString("select")));
    EXPECT_TRUE(aclOffloadShouldStopTagging(lookupCommandByCString("multi")));
    EXPECT_TRUE(aclOffloadShouldStopTagging(nullptr)); /* unknown command: conservative stop */
    EXPECT_FALSE(aclOffloadShouldStopTagging(lookupCommandByCString("get")));
    EXPECT_FALSE(aclOffloadShouldStopTagging(lookupCommandByCString("set")));
}

/* Binding marks the user reachable-by-IO-thread exactly once and is idempotent. */
TEST_F(AclOffloadTest, MarkBoundIsIdempotent) {
    user *u = makeRestricted();
    EXPECT_FALSE(u->flags & USER_FLAG_BOUND);
    aclMarkUserBound(u);
    EXPECT_TRUE(u->flags & USER_FLAG_BOUND);
    uint32_t flags_after = u->flags;
    aclMarkUserBound(u);
    EXPECT_EQ(u->flags, flags_after);
    ACLFreeUser(u);
}

/* The epoch bump is observable (low 32 bits). */
TEST_F(AclOffloadTest, EpochBumpIsObservable) {
    uint32_t e0 = aclOffloadEpoch();
    aclOffloadBumpEpoch();
    EXPECT_NE(aclOffloadEpoch(), e0);
}

/* With the feature off, consume never accounts a hit. */
TEST_F(AclOffloadTest, FeatureOffNeverHits) {
    int saved = server.acl_offload;
    server.acl_offload = 0;
    user *u = makeRestricted();
    robj *argv[3] = {createStringObject("SET", 3), createStringObject("foo:1", 5), createStringObject("v", 1)};
    client ec;
    memset(&ec, 0, sizeof(ec));
    ec.user = u;
    ec.db = server.db[0];
    ec.argv = argv;
    ec.argc = 3;
    ec.parsed_cmd = lookupCommandByCString("set");
    ec.cmd = ec.parsed_cmd;
    ec.read_flags = READ_FLAGS_PARSING_COMPLETED;
    ec.acl_epoch_seen = aclOffloadEpoch();
    long long hits0 = server.stat_acl_offload_hits;
    int idx = 0;
    EXPECT_EQ(aclOffloadConsume(&ec, &idx), ACL_OK);
    EXPECT_EQ(server.stat_acl_offload_hits, hits0);
    server.acl_offload = saved;
    ACLFreeUser(u);
    freeObjs(argv, 3);
}

/* aclOffloadQuiesce is safe with no fast-path workers (fp_slots == 0): returns at once
 * and accounts one quiesce. */
TEST_F(AclOffloadTest, QuiesceIsSafeWithNoWorkers) {
    long long q0 = server.stat_acl_offload_quiesce_count;
    aclOffloadQuiesce();
    EXPECT_EQ(server.stat_acl_offload_quiesce_count, q0 + 1);
}
