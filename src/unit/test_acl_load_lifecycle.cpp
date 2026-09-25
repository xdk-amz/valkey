/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Deterministic coverage for the ACL LOAD fast-path keep-alive decision. An ACL reload takes
 * a keep-alive reference (fp_refs) on a fast-path client's principal so the user object stays
 * valid until the client hands back to main. That reference is released only for RETIRED
 * principals (ACLFastpathClientReturned). A user is retired iff the reload releases it, which is
 * exactly the old users present in old_users. A surviving user (an unlinked module user is not
 * in old_users) is never retired, so a keep-alive on it would never be released and leaks one
 * reference per reload. aclReloadRetiresPrincipal is the predicate that prevents this. */

#include "generated_wrappers.hpp"

#include <cstring>

extern "C" {
#include "server.h"
}

class AclLoadLifecycleTest : public ::testing::Test {};

/* A bare user object with only the fields the predicate reads. */
static user *stubUser(const char *name) {
    user *u = static_cast<user *>(zcalloc(sizeof(user)));
    u->name = sdsnew(name);
    u->fp_refs = 0;
    return u;
}

static void freeStub(user *u) {
    sdsfree(u->name);
    zfree(u);
}

TEST_F(AclLoadLifecycleTest, KeepAliveTakenForRetiredPrincipalOnly) {
    rax *old_users = raxNew();
    user *alice = stubUser("alice");
    raxInsert(old_users, reinterpret_cast<unsigned char *>(const_cast<char *>("alice")), 5, alice, NULL);

    /* A user present in old_users is retired by the reload and needs the keep-alive. */
    EXPECT_EQ(aclReloadRetiresPrincipal(old_users, alice), 1);

    /* A module user is unlinked from Users, absent from old_users, and survives un-retired. */
    user *muser = stubUser("module-user");
    EXPECT_EQ(aclReloadRetiresPrincipal(old_users, muser), 0);

    /* Identity, not name: a surviving user sharing a config user's name is a different object
     * and must not be treated as retired (that would strand a reference on a live user). */
    user *alice_mod = stubUser("alice");
    EXPECT_EQ(aclReloadRetiresPrincipal(old_users, alice_mod), 0);

    freeStub(alice);
    freeStub(muser);
    freeStub(alice_mod);
    raxFree(old_users);
}

TEST_F(AclLoadLifecycleTest, SurvivingModuleUserRefIsBalanced) {
    rax *old_users = raxNew();
    user *alice = stubUser("alice");
    raxInsert(old_users, reinterpret_cast<unsigned char *>(const_cast<char *>("alice")), 5, alice, NULL);
    user *muser = stubUser("module-user");

    /* Apply the reload's fast-path keep-alive decision to each principal. */
    if (aclReloadRetiresPrincipal(old_users, alice)) alice->fp_refs++;
    if (aclReloadRetiresPrincipal(old_users, muser)) muser->fp_refs++;

    /* Retired principal: kept alive (released later on handback). */
    EXPECT_EQ(alice->fp_refs, 1u);
    /* Survivor: no reference taken, so nothing leaks across the reload. */
    EXPECT_EQ(muser->fp_refs, 0u);

    freeStub(alice);
    freeStub(muser);
    raxFree(old_users);
}
