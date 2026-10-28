/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "generated_wrappers.hpp"

#include <climits>
#include <cstdint>
#include <cstring>

extern "C" {
#include "intern.h"
#include "server.h"
}

/* Mirror of the private internHdr prefix in intern.c. The header sits
 * immediately before the sds header, so a shared sds is reached as
 * [ internHdr ][ sdshdrN ][ bytes ]. The test reconstructs the prefix address
 * from the public sds helpers to read and poke the refcount without touching
 * production code. */
typedef struct testInternHdr {
    uint32_t refcount;
    uint32_t size : 24;
    uint32_t where : 8;
} testInternHdr;

static testInternHdr *testHdrOf(const_sds s) {
    char *p = (char *)s - sdsHdrSize(sdsType(s)) - sizeof(testInternHdr);
    return (testInternHdr *)p;
}

class InternTest : public ::testing::Test {
  protected:
    void SetUp() override {
        /* internInit() is idempotent for the fields the tests read: it rebuilds
         * the index and probation tables and installs the release hook. */
        internInit();
    }

    /* Produces an indexed shared string equal to `val` with refcount 2.
     * The first admission makes a probation singleton; the second promotes it
     * to the index and takes a reference, so the result is shared. */
    sds makeShared(const char *val) {
        size_t len = strlen(val);
        sds a = sdsnewlen(val, len);
        a = internAdmitSlow(a);
        sds b = sdsnewlen(val, len);
        b = internAdmitSlow(b);
        EXPECT_TRUE(sdsIsShared(b));
        sdsfree(a);
        return b;
    }
};

/* A string already at INTERN_REFS_MAX references is not shared further: a new
 * holder gets a private copy and the capped count does not move. */
TEST_F(InternTest, RefcountCapHandsBackPrivateCopy) {
    const char *val = "intern-refcount-cap-boundary-value";
    sds shared = makeShared(val);
    ASSERT_TRUE(sdsIsShared(shared));

    testInternHdr *h = testHdrOf(shared);
    uint32_t capped = UINT32_MAX - 1; /* INTERN_REFS_MAX */
    h->refcount = capped;

    /* internDupSds() of a capped shared string must not take another reference;
     * it returns an unshared private copy instead. */
    sds dup = internDupSds(shared);
    EXPECT_NE(dup, shared);
    EXPECT_FALSE(sdsIsShared(dup));
    EXPECT_EQ(h->refcount, capped);
    EXPECT_EQ(internSharedRefcount(shared), capped);
    sdsfree(dup);

    /* A fresh admission of the same bytes must also decline to share with the
     * capped string and hand back its own copy. */
    sds fresh = sdsnewlen(val, strlen(val));
    sds out = internAdmitSlow(fresh);
    EXPECT_FALSE(sdsIsShared(out));
    EXPECT_EQ(h->refcount, capped);
    sdsfree(out);

    /* Restore a releasable count before freeing. */
    h->refcount = 1;
    sdsfree(shared);
}

/* A fork child owns a copy-on-write image whose keys other dumped keys may
 * still reference, so it must never release a shared string. */
TEST_F(InternTest, ForkChildReleaseAborts) {
    sds shared = makeShared("intern-fork-child-release-guard-value");
    ASSERT_TRUE(sdsIsShared(shared));

    int saved = server.in_fork_child;
    server.in_fork_child = 1;
    /* The release hook asserts it is not in a fork child; freeing a shared
     * string there aborts. */
    ASSERT_DEATH(sdsfree(shared), "");
    server.in_fork_child = saved;

    /* The death test runs the free in a forked subprocess, so the string is
     * still live here; release it normally. */
    testInternHdr *h = testHdrOf(shared);
    h->refcount = 1;
    sdsfree(shared);
}

/* A shareable string outside the index that gains a holder is indexed, so both
 * holders are counted and later writes find it. */
TEST_F(InternTest, DupOfSingletonIsIndexed) {
    const char *val = "intern-dup-of-a-probation-singleton";
    internInfo before, after;
    internGetInfo(&before);
    sds a = internAdmitSlow(sdsnew(val));
    ASSERT_TRUE(sdsIsShared(a));
    sds d = internDupSds(a);
    EXPECT_EQ(d, a);
    internGetInfo(&after);
    EXPECT_EQ(after.shared, before.shared + 1);
    EXPECT_EQ(after.refs, before.refs + 2);

    sds b = internAdmitSlow(sdsnew(val));
    EXPECT_EQ(b, a);
    EXPECT_EQ(internSharedRefcount(a), 3u);
    sdsfree(a);
    sdsfree(d);
    sdsfree(b);
}

/* Values whose shared copy fits the allocation size class of a private one: 80
 * bytes need 84 bytes privately and 92 shared, both in the 96-byte class. */
static sds loadValue(char *buf, int i) {
    snprintf(buf, 96, "load-value-%010d-padding-padding-padding-padding-padding-padding-padding", i);
    return internNewFrom(buf, 80);
}

/* While loading, a value is shared with its first copy however many other
 * values came between them, and removing singletons from the load map keeps
 * the rest findable. */
TEST_F(InternTest, LoadSharesAcrossTheWholeLoad) {
    const int n = 50000;
    char buf[96];
    sds *first = (sds *)zmalloc(sizeof(sds) * n);
    internLoadBegin();
    for (int i = 0; i < n; i++) {
        first[i] = loadValue(buf, i);
        ASSERT_TRUE(sdsIsShared(first[i]));
    }
    for (int i = 0; i < n; i += 2) sdsfree(first[i]);
    internInfo before, after;
    internGetInfo(&before);
    for (int i = 1; i < n; i += 2) {
        sds again = loadValue(buf, i);
        ASSERT_EQ(again, first[i]);
        sdsfree(again);
    }
    internGetInfo(&after);
    EXPECT_EQ(after.promotions, before.promotions + n / 2);
    internLoadEnd();
    for (int i = 1; i < n; i += 2) sdsfree(first[i]);
    zfree(first);
}

/* A value whose shared copy needs a bigger allocation stays private for its
 * first holder while loading, and is shared from the second holder on. */
TEST_F(InternTest, LoadMisfitSharesFromSecondHolder) {
    /* 40 bytes: 44 bytes privately in the 48-byte class, 52 shared in the 56. */
    const char *val = "load-misfit-value-forty-bytes-long-xxxxx";
    ASSERT_EQ(strlen(val), 40u);
    internLoadBegin();
    sds a = internNewFrom(val, 40);
    sds b = internNewFrom(val, 40);
    sds c = internNewFrom(val, 40);
    internLoadEnd();
    EXPECT_FALSE(sdsIsShared(a));
    EXPECT_TRUE(sdsIsShared(b));
    EXPECT_EQ(b, c);
    EXPECT_EQ(internSharedRefcount(b), 2u);
    sdsfree(a);
    sdsfree(b);
    sdsfree(c);
}
