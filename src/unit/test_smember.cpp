/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "generated_wrappers.hpp"

#include <climits>
#include <cstring>

extern "C" {
#include "expire.h"
#include "fmacros.h"
#include "monotonic.h"
#include "server.h"
#include "smember.h"
}

/* Short members fit in an sdshdr5 (< 32 bytes), long members do not. */
#define SHORT_MEMBER "tag:7"
#define MEDIUM_MEMBER "member:0123456789abcdef"
#define LONG_MEMBER "member:123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"

/* Size of the expiry prefix; must match smember.c. */
static const size_t EXPIRY_PREFIX_SIZE = sizeof(mstime_t);

/* Allocation size used by the moving defrag callback below. */
static size_t defrag_alloc_size = 0;

/* Mimics activeDefragAlloc(): moves the allocation and returns the new one. */
static void *test_defrag_move(void *ptr) {
    void *newptr = zmalloc(defrag_alloc_size);
    memcpy(newptr, ptr, defrag_alloc_size);
    zfree(ptr);
    return newptr;
}

/* Mimics an allocation that did not need to move. */
static void *test_defrag_no_move(void *ptr) {
    UNUSED(ptr);
    return NULL;
}

class SmemberTest : public ::testing::Test {
  protected:
    void verify_member(const smember *m, const char *str, mstime_t expiry) {
        size_t len = strlen(str);
        ASSERT_EQ(sdslen((const_sds)m), len);
        ASSERT_EQ(memcmp(m, str, len), 0);
        ASSERT_EQ(m[len], '\0');
        ASSERT_EQ(smemberGetExpiry(m), expiry);
        ASSERT_EQ(smemberHasExpiry(m), expiry != EXPIRY_NONE);
        /* The aux bit is the only marker of the prefix, and an sdshdr5 can
         * never carry it. */
        ASSERT_EQ(sdsGetAuxBit((const_sds)m, SMEMBER_SDS_AUX_BIT_HAS_EXPIRY) != 0, expiry != EXPIRY_NONE);
        if (expiry != EXPIRY_NONE) {
            ASSERT_NE(sdsType((const_sds)m), SDS_TYPE_5);
        }
    }
};

/* A member without an expiry is exactly what sdsnewlen() produces, including
 * the sdshdr5 header for short strings, and no aux bit is set. */
TEST_F(SmemberTest, createPlain) {
    smember *m1 = smemberCreate(SHORT_MEMBER, strlen(SHORT_MEMBER), EXPIRY_NONE);
    verify_member(m1, SHORT_MEMBER, EXPIRY_NONE);
    sds reference = sdsnewlen(SHORT_MEMBER, strlen(SHORT_MEMBER));
    ASSERT_EQ(sdsType((const_sds)m1), sdsType(reference));
    ASSERT_EQ(sdsType(reference), SDS_TYPE_5);
    ASSERT_EQ(smemberMemUsage(m1), sdsAllocSize(reference));
    sdsfree(reference);

    smember *m2 = smemberCreate(LONG_MEMBER, strlen(LONG_MEMBER), EXPIRY_NONE);
    verify_member(m2, LONG_MEMBER, EXPIRY_NONE);
    ASSERT_NE(sdsType((const_sds)m2), SDS_TYPE_5);

    /* A plain member is sdsfree()-compatible. */
    sdsfree(m1);
    smemberFree(m2);
}

/* A member with an expiry keeps the 8-byte prefix and an sdshdr8+ header. */
TEST_F(SmemberTest, createPrefixed) {
    smember *m1 = smemberCreate(SHORT_MEMBER, strlen(SHORT_MEMBER), 1234567);
    verify_member(m1, SHORT_MEMBER, 1234567);

    smember *m2 = smemberCreate(LONG_MEMBER, strlen(LONG_MEMBER), 42);
    verify_member(m2, LONG_MEMBER, 42);

    /* Empty member with an expiry. */
    smember *m3 = smemberCreate("", 0, 7);
    verify_member(m3, "", 7);

    smemberFree(m1);
    smemberFree(m2);
    smemberFree(m3);
}

/* Expiry values round-trip, including values that need more than 32 bits. */
TEST_F(SmemberTest, getExpiryRoundTrip) {
    const mstime_t expiries[] = {0, 1, 1000, 2147483647LL, 4294967296LL, 1893456000000LL, LLONG_MAX};
    for (size_t i = 0; i < sizeof(expiries) / sizeof(expiries[0]); i++) {
        smember *m = smemberCreate(MEDIUM_MEMBER, strlen(MEDIUM_MEMBER), expiries[i]);
        ASSERT_EQ(smemberGetExpiry(m), expiries[i]);
        ASSERT_TRUE(smemberHasExpiry(m));
        smemberFree(m);
    }
    /* A plain member reports EXPIRY_NONE. */
    smember *plain = smemberCreate(MEDIUM_MEMBER, strlen(MEDIUM_MEMBER), EXPIRY_NONE);
    ASSERT_EQ(smemberGetExpiry(plain), EXPIRY_NONE);
    ASSERT_FALSE(smemberHasExpiry(plain));
    smemberFree(plain);
}

/* plain -> prefixed -> changed -> EXPIRY_NONE returns a plain sds again. */
TEST_F(SmemberTest, setExpiryTransitions) {
    const char *strings[] = {SHORT_MEMBER, LONG_MEMBER};
    for (size_t i = 0; i < sizeof(strings) / sizeof(strings[0]); i++) {
        const char *str = strings[i];
        smember *m = smemberCreate(str, strlen(str), EXPIRY_NONE);
        verify_member(m, str, EXPIRY_NONE);

        /* Setting an expiry on a plain member rebuilds it with the prefix. */
        m = smemberSetExpiry(m, 5000);
        verify_member(m, str, 5000);

        /* Changing an existing expiry keeps the same allocation. */
        smember *before = m;
        m = smemberSetExpiry(m, 6000);
        ASSERT_EQ(m, before);
        verify_member(m, str, 6000);

        /* Removing the expiry restores the zero-cost plain layout. */
        m = smemberSetExpiry(m, EXPIRY_NONE);
        verify_member(m, str, EXPIRY_NONE);
        sds reference = sdsnewlen(str, strlen(str));
        ASSERT_EQ(sdsType((const_sds)m), sdsType(reference));
        ASSERT_EQ(sdslen((const_sds)m), sdslen(reference));
        ASSERT_EQ(sdscmp((const_sds)m, reference), 0);
        sdsfree(reference);

        /* Removing an absent expiry is a no-op. */
        before = m;
        m = smemberSetExpiry(m, EXPIRY_NONE);
        ASSERT_EQ(m, before);

        /* The rebuilt member is sdsfree()-compatible. */
        sdsfree(m);
    }
}

/* smemberIsExpired() compares against commandTimeSnapshot(). */
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

/* The prefix is accounted for in the reported memory usage. */
TEST_F(SmemberTest, memUsage) {
    /* A 22 byte member still uses an sdshdr5 when plain, and its plain
     * allocation request lands on an allocator size class boundary, so the
     * prefixed layout is guaranteed to need at least 8 more bytes. */
    char str[23];
    memset(str, 'x', sizeof(str) - 1);
    str[sizeof(str) - 1] = '\0';

    smember *plain = smemberCreate(str, strlen(str), EXPIRY_NONE);
    smember *prefixed = smemberCreate(str, strlen(str), 1000);
    ASSERT_EQ(smemberMemUsage(plain), sdsAllocSize((const_sds)plain));
    ASSERT_EQ(smemberMemUsage(prefixed), sdsAllocSize((const_sds)prefixed) + EXPIRY_PREFIX_SIZE);
    ASSERT_GE(smemberMemUsage(prefixed), smemberMemUsage(plain) + EXPIRY_PREFIX_SIZE);
    smemberFree(plain);
    smemberFree(prefixed);

    /* Same for a long member, where both layouts use an sdshdr8+. */
    smember *long_plain = smemberCreate(LONG_MEMBER, strlen(LONG_MEMBER), EXPIRY_NONE);
    smember *long_prefixed = smemberCreate(LONG_MEMBER, strlen(LONG_MEMBER), 1000);
    ASSERT_EQ(smemberMemUsage(long_prefixed), sdsAllocSize((const_sds)long_prefixed) + EXPIRY_PREFIX_SIZE);
    ASSERT_GE(smemberMemUsage(long_prefixed), smemberMemUsage(long_plain));
    smemberFree(long_plain);
    smemberFree(long_prefixed);
}

/* smemberDefrag() returns the new pointer, or NULL when nothing moved. */
TEST_F(SmemberTest, defrag) {
    smember *plain = smemberCreate(LONG_MEMBER, strlen(LONG_MEMBER), EXPIRY_NONE);
    ASSERT_EQ(smemberDefrag(plain, test_defrag_no_move), nullptr);
    defrag_alloc_size = smemberMemUsage(plain);
    smember *moved_plain = smemberDefrag(plain, test_defrag_move);
    ASSERT_NE(moved_plain, nullptr);
    verify_member(moved_plain, LONG_MEMBER, EXPIRY_NONE);
    smemberFree(moved_plain);

    smember *prefixed = smemberCreate(LONG_MEMBER, strlen(LONG_MEMBER), 4242);
    ASSERT_EQ(smemberDefrag(prefixed, test_defrag_no_move), nullptr);
    defrag_alloc_size = smemberMemUsage(prefixed);
    smember *moved_prefixed = smemberDefrag(prefixed, test_defrag_move);
    ASSERT_NE(moved_prefixed, nullptr);
    verify_member(moved_prefixed, LONG_MEMBER, 4242);
    smemberFree(moved_prefixed);
}
