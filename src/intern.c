#include "server.h"
#include "serverassert.h"
#include "intern.h"
#include "hashtable.h"
#include "sds.h"
#include "zmalloc.h"

/*-----------------------------------------------------------------------------
 * String interning subsystem (spike S1 values, S2 hash field names).
 *
 * Two dedup indexes, one per kind: intern_index_value (hash values, S1) and
 * intern_index_field (hash field names, S2). Both are hashtables of internSlot*
 * keyed on content. The hash function reads the cached slot->hash; the compare
 * function compares len then memcmp. Lookups by raw {buf,len} use a probe key
 * whose prefix layout (buf, len, refcount, hash) matches a slot so the same
 * callbacks operate on it.
 *
 * VALUE slots: slot->buf points at raw content bytes (inline_ or a long alloc).
 * FIELD slots: inline_ (or a long alloc) is laid out as a real sdshdr8
 *   {len, alloc, flags=SDS_TYPE_8, bytes, NUL} and slot->buf points at the sds
 *   body (header + 3). This makes slot->buf a genuine sds so the hash's
 *   hashtable type callbacks (sdslen, sdsHashConfigurableSeed, dictSdsKeyCompare)
 *   operate on it with no change. Field aux bits are NOT used on this shared sds;
 *   the Type-5 entry carries the holder's aux bits in its own flags byte.
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

/* The two dedup indexes. Lazily created. */
static hashtable *intern_index_value = NULL;
static hashtable *intern_index_field = NULL;

static void internInit(void) {
    if (intern_index_value == NULL) intern_index_value = hashtableCreate(&internIndexType);
    if (intern_index_field == NULL) intern_index_field = hashtableCreate(&internIndexType);
}

/* True if the field slot stores its sdshdr8 inline (vs a long allocation). */
static inline bool internFieldIsInline(const internSlot *slot) {
    return (const char *)slot->buf - sdsHdrSize(SDS_TYPE_8) == slot->inline_;
}

size_t internSlotAllocSize(const internSlot *slot) {
    /* VALUE slot: the slot itself, plus a long byte buffer if content did not
     * fit inline. slot->buf == slot->inline_ exactly when stored inline. */
    size_t size = zmalloc_size((void *)slot);
    if (slot->buf != slot->inline_) size += zmalloc_size((void *)slot->buf);
    return size;
}

size_t internSlotFieldAllocSize(const internSlot *slot) {
    /* FIELD slot: the slot itself, plus a long sdshdr8 allocation if the field
     * did not fit inline. The header base is slot->buf - sdsHdrSize(SDS_TYPE_8). */
    size_t size = zmalloc_size((void *)slot);
    if (!internFieldIsInline(slot)) {
        size += zmalloc_size((void *)((const char *)slot->buf - sdsHdrSize(SDS_TYPE_8)));
    }
    return size;
}

uint32_t internSlotRefcount(const internSlot *slot) {
    return slot->refcount;
}

sds internSlotFieldSds(const internSlot *slot) {
    return (sds)slot->buf;
}

/* Common acquire: probe `index`, on hit bump refcount, on miss build a slot with
 * `builder` and insert. `is_field` selects the storage layout. */
static internSlot *internAcquireImpl(hashtable *index, const char *buf, size_t len, bool is_field) {
    internSlot probe;
    probe.buf = buf;
    probe.len = len;
    probe.hash = (uint32_t)hashtableGenHashFunction(buf, len);

    void *found;
    if (hashtableFind(index, &probe, &found)) {
        internSlot *slot = found;
        slot->refcount++;
        return slot;
    }

    internSlot *slot = zmalloc(sizeof(internSlot));
    slot->len = len;
    slot->refcount = 1;
    slot->hash = probe.hash;

    if (!is_field) {
        /* VALUE: raw bytes, inline or long. */
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
    } else {
        /* FIELD: lay out an sdshdr8 so slot->buf is a genuine sds. Use sdswrite,
         * which writes the header + bytes + NUL and returns the sds body. */
        size_t need = sdsReqSize(len, SDS_TYPE_8); /* hdr(3) + len + 1 */
        char *hdr;
        if (need <= sizeof(slot->inline_)) {
            hdr = slot->inline_;
        } else {
            hdr = zmalloc(need);
        }
        slot->buf = sdswrite(hdr, need, SDS_TYPE_8, buf, len);
    }
    serverAssert(hashtableAdd(index, slot));
    return slot;
}

internSlot *internAcquire(const char *buf, size_t len) {
    internInit();
    return internAcquireImpl(intern_index_value, buf, len, false);
}

internSlot *internAcquireField(const char *buf, size_t len) {
    internInit();
    return internAcquireImpl(intern_index_field, buf, len, true);
}

static void internReleaseImpl(hashtable *index, internSlot *slot, bool is_field) {
    serverAssert(slot->refcount > 0);
    if (--slot->refcount > 0) return;

    serverAssert(hashtableDelete(index, slot));
    if (!is_field) {
        if (slot->buf != slot->inline_) zfree((void *)slot->buf);
    } else {
        if (!internFieldIsInline(slot)) zfree((void *)((const char *)slot->buf - sdsHdrSize(SDS_TYPE_8)));
    }
    zfree(slot);
}

void internRetain(internSlot *slot) {
    serverAssert(slot->refcount > 0);
    slot->refcount++;
}

void internRelease(internSlot *slot) {
    internReleaseImpl(intern_index_value, slot, false);
}

void internReleaseField(internSlot *slot) {
    internReleaseImpl(intern_index_field, slot, true);
}

size_t internBytesSaved(void) {
    internStats stats;
    internGetStats(&stats);
    return stats.bytes_saved;
}

/* Length histogram bucket index: <=8, 9-16, 17-20, 21-23, 24-32, 33-64, >64. */
static int internHistBucket(size_t len) {
    if (len <= 8) return 0;
    if (len <= 16) return 1;
    if (len <= 20) return 2;
    if (len <= 23) return 3;
    if (len <= 32) return 4;
    if (len <= 64) return 5;
    return 6;
}

static void internAccumulate(hashtable *index, bool is_field, internStats *stats) {
    if (index == NULL) return;
    hashtableIterator iter;
    hashtableInitIterator(&iter, index, 0);
    void *elem;
    while (hashtableNext(&iter, &elem)) {
        internSlot *slot = elem;
        stats->slots++;
        if (is_field) {
            stats->field_slots++;
            if (internFieldIsInline(slot)) stats->sso_slots++; else stats->long_slots++;
        } else {
            stats->value_slots++;
            if (slot->buf == slot->inline_) stats->sso_slots++; else stats->long_slots++;
        }
        stats->hist[internHistBucket(slot->len)]++;
        if (slot->refcount > 1) {
            stats->bytes_saved += (size_t)(slot->refcount - 1) * INTERN_PER_USE_OVERHEAD(slot->len);
            stats->dup_refs += (slot->refcount - 1);
        }
    }
    hashtableCleanupIterator(&iter);
    stats->index_mem += hashtableMemUsage(index);
}

void internGetStats(internStats *stats) {
    memset(stats, 0, sizeof(*stats));
    internAccumulate(intern_index_value, false, stats);
    internAccumulate(intern_index_field, true, stats);
}
