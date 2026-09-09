#include "server.h"
#include "serverassert.h"
#include "intern.h"
#include "hashtable.h"
#include "sds.h"
#include "zmalloc.h"

/*-----------------------------------------------------------------------------
 * String interning subsystem (spike S1) -- hash VALUES only.
 *
 * Dedup index: a hashtable of internSlot*, keyed on content. The hash function
 * reads the cached slot->hash; the compare function compares len then memcmp.
 * Lookups by raw {buf,len} use a probe key whose layout matches a slot's prefix
 * (buf, len, refcount, hash) so the same callbacks operate on it.
 *----------------------------------------------------------------------------*/

/* Per-duplicate saving: an un-interned duplicate would have cost an owned
 * sdshdr8 buffer of (len + header + NUL). */
#define INTERN_PER_USE_OVERHEAD(len) ((len) + sdsHdrSize(SDS_TYPE_8) + 1)

static uint64_t internSlotHash(const void *key) {
    const internSlot *slot = key;
    return slot->hash;
}

/* Returns non-zero (true) when the two keys are equal, matching the hashtable
 * keyCompare contract (see dictSdsKeyCompare). */
static int internSlotCompare(const void *key1, const void *key2) {
    const internSlot *a = key1;
    const internSlot *b = key2;
    if (a->len != b->len) return 0;
    return memcmp(a->buf, b->buf, a->len) == 0;
}

static hashtableType internIndexType = {
    .hashFunction = internSlotHash,
    .keyCompare = internSlotCompare,
    .instant_rehashing = 1,
};

/* The dedup index. Lazily created. */
static hashtable *intern_index = NULL;

static void internInit(void) {
    if (intern_index == NULL) intern_index = hashtableCreate(&internIndexType);
}

size_t internSlotAllocSize(const internSlot *slot) {
    size_t size = zmalloc_size((void *)slot);
    if (slot->buf != slot->inline_) size += zmalloc_size((void *)slot->buf);
    return size;
}

uint32_t internSlotRefcount(const internSlot *slot) {
    return slot->refcount;
}

internSlot *internAcquire(const char *buf, size_t len) {
    internInit();

    /* Probe the index. The probe key mirrors a slot's prefix layout so the
     * type callbacks (which read len/buf/hash) work against it. */
    internSlot probe;
    probe.buf = buf;
    probe.len = len;
    probe.hash = (uint32_t)hashtableGenHashFunction(buf, len);

    void *found;
    if (hashtableFind(intern_index, &probe, &found)) {
        internSlot *slot = found;
        slot->refcount++;
        return slot;
    }

    /* Miss: allocate a new slot, copy the bytes. */
    internSlot *slot = zmalloc(sizeof(internSlot));
    slot->len = len;
    slot->refcount = 1;
    slot->hash = probe.hash;
    if (len <= INTERN_SSO_MAX) {
        memcpy(slot->inline_, buf, len);
        slot->inline_[len] = '\0';
        slot->buf = slot->inline_;
    } else {
        char *bytes = zmalloc(len + 1);
        memcpy(bytes, buf, len);
        bytes[len] = '\0';
        slot->buf = bytes;
    }
    serverAssert(hashtableAdd(intern_index, slot));
    return slot;
}

void internRelease(internSlot *slot) {
    serverAssert(slot->refcount > 0);
    if (--slot->refcount > 0) return;

    /* Last reference: remove from the index and free inline. */
    serverAssert(hashtableDelete(intern_index, slot));
    if (slot->buf != slot->inline_) zfree((void *)slot->buf);
    zfree(slot);
}

size_t internBytesSaved(void) {
    internStats stats;
    internGetStats(&stats);
    return stats.bytes_saved;
}

void internGetStats(internStats *stats) {
    memset(stats, 0, sizeof(*stats));
    if (intern_index == NULL) return;

    hashtableIterator iter;
    hashtableInitIterator(&iter, intern_index, 0);
    void *elem;
    while (hashtableNext(&iter, &elem)) {
        internSlot *slot = elem;
        stats->slots++;
        if (slot->buf == slot->inline_) {
            stats->sso_slots++;
        } else {
            stats->long_slots++;
        }
        if (slot->refcount > 1) {
            stats->bytes_saved += (size_t)(slot->refcount - 1) * INTERN_PER_USE_OVERHEAD(slot->len);
        }
    }
    hashtableCleanupIterator(&iter);
    stats->index_mem = hashtableMemUsage(intern_index);
}
