#ifndef __INTERN_H
#define __INTERN_H

#include <stddef.h>
#include <stdint.h>
#include "sds.h"

/* Automatic interning of out-of-line keyspace strings.
 *
 * A shareable string is an sds allocated with an internHdr prefix and marked
 * with the SDS_AUX_SHARED header bit. It is immutable. Every holder owns one
 * reference, and sdsfree() releases one reference. Holders keep pointing at the
 * sds itself, so read paths are unchanged.
 *
 * A sampled write turns its value into a shareable singleton and remembers it in
 * a small probation table. A later write of the same bytes finds it there or in
 * the index, takes a reference and frees its own copy. The first holder never
 * needs to be found again.
 *
 * Thread contract: admission, lookup and the index are main-thread only. A
 * release on any other thread is queued and applied by internDrainDeferred(). */

typedef struct internGate {
    uint32_t tick;   /* calls left before the next sampled call */
    uint32_t period; /* sample one call in `period` */
} internGate;

extern internGate intern_gate;

sds internAdmitSlow(sds s);

/* Offers a freshly owned string that is about to be stored in the keyspace.
 * Takes ownership of `s` and returns the sds the holder must store. The result
 * is `s` itself or a shareable string that carries one reference for the
 * holder. Main thread only. */
static inline sds internAdmit(sds s) {
    if (--intern_gate.tick) return s;
    return internAdmitSlow(s);
}

void internInit(void);

/* Bio jobs that free keyspace data run between these two calls, so their
 * releases are coalesced into one message for the main thread. */
void internDeferBegin(void);
void internDeferPublish(void);

/* Applies releases queued by other threads. Main thread only. */
size_t internDrainDeferred(void);

/* Allocation size of a shareable string, including its prefix. */
size_t internSharedAllocSize(const_sds s);
uint32_t internSharedRefcount(const_sds s);

typedef struct internInfo {
    size_t shared;        /* strings in the index */
    size_t singletons;    /* strings in probation */
    size_t refs;          /* references held on indexed strings */
    size_t shared_bytes;  /* allocation bytes of indexed strings */
    size_t saved_bytes;   /* bytes not allocated thanks to sharing */
    size_t table_bytes;   /* index and probation table memory */
    size_t hits;          /* writes that took a reference on an indexed string */
    size_t promotions;    /* singletons that gained a second holder */
    size_t sampled;       /* writes that were hashed */
    size_t deferred;      /* releases applied from other threads */
    int level;            /* sampling level, 0 = every write */
} internInfo;

void internGetInfo(internInfo *info);

#endif
