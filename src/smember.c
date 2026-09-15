/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Set member with optional expiration. The layout is documented in smember.h. */

#include "server.h"
#include "serverassert.h"
#include "expire.h"
#include "smember.h"

/* Size of the optional expiry prefix, in bytes. */
#define SMEMBER_EXPIRY_SIZE (sizeof(mstime_t))

bool smemberHasExpiry(const smember *m) {
    return sdsGetAuxBit((const_sds)m, SMEMBER_SDS_AUX_BIT_HAS_EXPIRY) != 0;
}

/* The address of the member's allocation: the expiry prefix when there is one,
 * the sds header otherwise. */
static char *smemberGetAllocPtr(const smember *m) {
    char *buf = sdsAllocPtr((const_sds)m);
    if (smemberHasExpiry(m)) buf -= SMEMBER_EXPIRY_SIZE;
    return buf;
}

static mstime_t *smemberGetExpiryRef(const smember *m) {
    serverAssert(smemberHasExpiry(m));
    return (mstime_t *)smemberGetAllocPtr(m);
}

mstime_t smemberGetExpiry(const smember *m) {
    if (smemberHasExpiry(m)) return *smemberGetExpiryRef(m);
    return EXPIRY_NONE;
}

/* The sds header records only the space that follows the prefix, so
 * sdsAllocSize() plus the prefix is the size of the whole allocation. */
static smember *smemberCreatePrefixed(const char *str, size_t len, mstime_t expiry) {
    serverAssert(expiry != EXPIRY_NONE);
    char type = sdsReqType(len);
    /* An sdshdr5 has no aux bits, so it cannot encode the prefix. */
    if (type == SDS_TYPE_5) type = SDS_TYPE_8;
    int hdrlen = sdsHdrSize(type);
    size_t req = SMEMBER_EXPIRY_SIZE + hdrlen + len + 1;
    serverAssert(req > len); /* Catch size_t overflow. */

    size_t bufsize;
    char *buf = zmalloc_usable(req, &bufsize);
    *(mstime_t *)buf = expiry;
    buf += SMEMBER_EXPIRY_SIZE;
    bufsize -= SMEMBER_EXPIRY_SIZE;

    /* zmalloc_usable() may hand out more room than requested; grow the header
     * type when the usable size no longer fits it, as sdsnewlen() does. */
    size_t usable = bufsize - hdrlen - 1;
    if (usable > sdsTypeMaxSize(type)) type = sdsReqType(usable);

    smember *m = (smember *)sdswrite(buf, bufsize, type, str, len);
    /* sds aux bits are zero after sdswrite(); mark the prefix. */
    sdsSetAuxBit((sds)m, SMEMBER_SDS_AUX_BIT_HAS_EXPIRY, 1);
    debugServerAssert(smemberHasExpiry(m) && smemberGetExpiry(m) == expiry);
    return m;
}

smember *smemberCreate(const char *str, size_t len, mstime_t expiry) {
    if (expiry == EXPIRY_NONE) return sdsnewlen(str, len);
    return smemberCreatePrefixed(str, len, expiry);
}

smember *smemberSetExpiry(smember *m, mstime_t expiry) {
    bool has_expiry = smemberHasExpiry(m);
    if (expiry != EXPIRY_NONE) {
        if (has_expiry) {
            *smemberGetExpiryRef(m) = expiry;
            return m;
        }
        /* Plain sds -> prefixed layout, which needs a new allocation. */
        smember *new_member = smemberCreatePrefixed(m, sdslen((sds)m), expiry);
        sdsfree((sds)m);
        return new_member;
    }
    if (!has_expiry) return m;
    /* Prefixed -> plain sds, restoring the layout a set without TTLs has. */
    smember *new_member = sdsnewlen(m, sdslen((sds)m));
    smemberFree(m);
    return new_member;
}

bool smemberIsExpired(const smember *m) {
    /* EXPIRY_NONE is negative, which timestampIsExpired() reads as no
     * expiration. */
    return timestampIsExpired(smemberGetExpiry(m));
}

void smemberFree(smember *m) {
    if (!smemberHasExpiry(m)) {
        sdsfree(m);
        return;
    }
    zfree(smemberGetAllocPtr(m));
}

size_t smemberMemUsage(const smember *m) {
    size_t mem = sdsAllocSize((const_sds)m);
    if (smemberHasExpiry(m)) mem += SMEMBER_EXPIRY_SIZE;
    return mem;
}

smember *smemberDefrag(smember *m, void *(*defragfn)(void *)) {
    char *allocation = smemberGetAllocPtr(m);
    char *new_allocation = defragfn(allocation);
    if (new_allocation == NULL) return NULL;
    return (smember *)(new_allocation + ((const char *)m - allocation));
}
