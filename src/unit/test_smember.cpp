/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "generated_wrappers.hpp"

#include <cstring>
#include <string>

extern "C" {
#include "expire.h"
#include "fmacros.h"
#include "server.h"
#include "smember.h"
}

#define SHORT_MEMBER "tag:7"

/* One member per sds header type (sdshdr5, sdshdr8, sdshdr16) plus a binary one. */
static const std::string LONG_MEMBER(200, 'y');
static const std::string LARGE_MEMBER(300, 'x');
static const std::string MEMBERS[] = {SHORT_MEMBER, std::string("a\0b", 3), LONG_MEMBER, LARGE_MEMBER};

static void *neverMoveDefragAlloc(void *) {
    return nullptr;
}

/* Allocate before freeing so relocation always changes the pointer. */
static void *alwaysMoveDefragAlloc(void *ptr) {
    size_t size = zmalloc_usable_size(ptr);
    void *newptr = zmalloc(size);
    memcpy(newptr, ptr, size);
    zfree(ptr);
    return newptr;
}

class SmemberTest : public ::testing::Test {
  protected:
    void verifyMember(const smember *m, const std::string &str, mstime_t expiry) {
        ASSERT_EQ(sdslen(m), str.size());
        ASSERT_EQ(memcmp(m, str.data(), str.size()), 0);
        ASSERT_EQ(smemberGetExpiry(m), expiry);
        ASSERT_EQ(smemberHasExpiry(m), expiry != EXPIRY_NONE);
        if (expiry == EXPIRY_NONE) {
            sds reference = sdsnewlen(str.data(), str.size());
            ASSERT_EQ(sdsType(m), sdsType(reference));
            ASSERT_EQ(smemberMemUsage(m), sdsAllocSize(reference));
            sdsfree(reference);
        } else {
            ASSERT_NE(sdsType(m), SDS_TYPE_5);
        }
    }
};

TEST_F(SmemberTest, setExpiryTransitions) {
    for (const std::string &str : MEMBERS) {
        smember *m = smemberCreate(str.data(), str.size(), EXPIRY_NONE);
        verifyMember(m, str, EXPIRY_NONE);

        m = smemberSetExpiry(m, 5000);
        verifyMember(m, str, 5000);

        ASSERT_EQ(smemberSetExpiry(m, 6000), m);
        verifyMember(m, str, 6000);

        m = smemberSetExpiry(m, EXPIRY_NONE);
        verifyMember(m, str, EXPIRY_NONE);

        smemberFree(m);
    }
}

TEST_F(SmemberTest, isExpired) {
    enterExecutionUnit(1, ustime());
    mstime_t now = commandTimeSnapshot();

    smember *plain = smemberCreate(SHORT_MEMBER, strlen(SHORT_MEMBER), EXPIRY_NONE);
    ASSERT_FALSE(smemberIsExpired(plain));

    smember *future = smemberCreate(SHORT_MEMBER, strlen(SHORT_MEMBER), now + 10000);
    ASSERT_FALSE(smemberIsExpired(future));

    smember *current = smemberCreate(SHORT_MEMBER, strlen(SHORT_MEMBER), now);
    ASSERT_FALSE(smemberIsExpired(current));

    smember *past = smemberCreate(SHORT_MEMBER, strlen(SHORT_MEMBER), now - 10000);
    ASSERT_TRUE(smemberIsExpired(past));

    smemberFree(plain);
    smemberFree(future);
    smemberFree(current);
    smemberFree(past);
    exitExecutionUnit();
}

/* Allocator slack can force a larger header than the payload requires. */
TEST_F(SmemberTest, memUsage) {
#ifndef USE_JEMALLOC
    GTEST_SKIP() << "Test requires jemalloc";
#endif
    std::string member(252, 'x');
    ASSERT_EQ(sdsReqType(member.size()), SDS_TYPE_8);
    smember *m = smemberCreate(member.c_str(), member.size(), 1000);
    ASSERT_EQ(sdsType(m), SDS_TYPE_16);
    ASSERT_EQ(smemberMemUsage(m), zmalloc_usable_size((char *)sdsAllocPtr(m) - sizeof(mstime_t)));
    smemberFree(m);
}

/* smemberDefrag() returns the moved member at the same offset into the new allocation. */
TEST_F(SmemberTest, defrag) {
    for (const std::string &str : MEMBERS) {
        smember *plain = smemberCreate(str.data(), str.size(), EXPIRY_NONE);
        ASSERT_EQ(smemberDefrag(plain, neverMoveDefragAlloc), nullptr);
        smember *moved_plain = smemberDefrag(plain, alwaysMoveDefragAlloc);
        ASSERT_NE(moved_plain, nullptr);
        verifyMember(moved_plain, str, EXPIRY_NONE);
        smemberFree(moved_plain);

        smember *prefixed = smemberCreate(str.data(), str.size(), 4242);
        smember *moved_prefixed = smemberDefrag(prefixed, alwaysMoveDefragAlloc);
        ASSERT_NE(moved_prefixed, nullptr);
        verifyMember(moved_prefixed, str, 4242);
        smemberFree(moved_prefixed);
    }
}
