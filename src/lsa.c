/* lsa.c - log-structured bump allocator for unembedded raw string values.
 * Segment pool + bump alloc + tombstone free + band cleaner.
 * Standalone: no server.h dependency, so sds.c stays linkable into the
 * cli/benchmark binaries (they never install the hooks). */
#define _GNU_SOURCE
#include "lsa.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
#include <sys/mman.h>

/* Entries are 4-byte aligned: the header's widest hard-alignment member is
 * the packed uint32 meta word, and the sds payload is byte-oriented. The
 * owner pointer sits at offset 4 inside a packed struct; the compiler emits
 * alignment-safe accesses for it (aarch64 handles these natively). */
#define LSA_ALIGN 4u

/* meta word packing: bits 0-23 total_size (max 16MB >> 1MB e_max),
 * bits 24-28 sds_off (max sds hdr is 17B), bit 31 tombstone. */
#define LSA_META_SIZE_MASK 0x00FFFFFFu
#define LSA_META_OFF_SHIFT 24
#define LSA_META_OFF_MASK 0x1Fu
#define LSA_META_TOMB 0x80000000u

/* Entry header, 12 bytes packed. data[] holds an sds buffer
 * (sdshdr + string + NUL); the sds base is data + sds_off. */
typedef struct __attribute__((packed)) lsaEntry {
    uint32_t meta;
    void *owner; /* address of owning robj's val_ptr field.
                  * Valid iff not tombstoned: sdsfree tombstones
                  * BEFORE the owning robj is freed, so a live
                  * (non-tombstoned) entry implies a live owner. */
    char data[];
} lsaEntry;

static inline uint32_t entrySize(const lsaEntry *e) {
    return e->meta & LSA_META_SIZE_MASK;
}
static inline uint32_t entrySdsOff(const lsaEntry *e) {
    return (e->meta >> LSA_META_OFF_SHIFT) & LSA_META_OFF_MASK;
}
static inline int entryTombstoned(const lsaEntry *e) {
    return (e->meta & LSA_META_TOMB) != 0;
}

typedef struct lsaSegment {
    uint32_t id;
    uint32_t bump;       /* next free offset in payload */
    uint64_t live_bytes; /* logical live bytes in this segment */
    struct lsaSegment *next_free;
    uint8_t on_free_list;
    uint8_t madvised;
    uint8_t ever_used;
    uint8_t tail_purged; /* stale pages above bump returned (frozen segs) */
    char *payload;
} lsaSegment;

static struct {
    int inited;
    char *region;
    size_t region_size;
    uint32_t nsegments;
    lsaSegment *segs;
    lsaSegment *active;    /* bump target */
    lsaSegment *free_list; /* LIFO */
    uint32_t free_count;
    int band_lo, band_hi; /* trigger below lo% live, clean to hi% live */
    /* accounting */
    uint64_t used_bytes; /* sum of bump over non-free segments */
    uint64_t live_bytes; /* logical live bytes */
    /* stats */
    uint64_t admissions, tombstones, escapes;
    uint64_t alloc_bytes, copied_bytes;
    uint64_t clean_passes, emergency_cleans, heap_fallbacks;
    uint64_t owner_mismatch_dead;
    uint64_t dead_fastpath_reclaims;
    uint64_t clean_us; /* wall time spent inside cleaner work */
} L;

/* Hooks installed into sds.c (declared in sds.h, defined in sds.c). */
static int hookIsLog(const void *allocptr) {
    return (const char *)allocptr >= L.region && (const char *)allocptr < L.region + L.region_size;
}

static inline lsaEntry *entryFromAlloc(void *allocptr) {
    return (lsaEntry *)((char *)allocptr - offsetof(lsaEntry, data));
}

static inline lsaSegment *segOf(const void *p) {
    return &L.segs[((const char *)p - L.region) / LSA_SEGMENT_SIZE];
}

static void tombstone(lsaEntry *e) {
    if (entryTombstoned(e)) return;
    e->meta |= LSA_META_TOMB;
    segOf(e)->live_bytes -= entrySize(e);
    L.live_bytes -= entrySize(e);
    L.tombstones++;
}

static void hookFreeLog(void *allocptr) {
    tombstone(entryFromAlloc(allocptr));
}

static void hookEscapeLog(void *allocptr) {
    tombstone(entryFromAlloc(allocptr));
    L.escapes++;
}

static inline uint32_t alignUp(uint32_t n) {
    return (n + (LSA_ALIGN - 1)) & ~(LSA_ALIGN - 1);
}

static lsaSegment *popFree(void) {
    lsaSegment *s = L.free_list;
    if (!s) return NULL;
    L.free_list = s->next_free;
    L.free_count--;
    s->next_free = NULL;
    s->on_free_list = 0;
    s->madvised = 0;
    s->tail_purged = 0;
    s->bump = 0;
    s->live_bytes = 0;
    s->ever_used = 1;
    return s;
}

static void pushFree(lsaSegment *s) {
    L.used_bytes -= s->bump;
    s->bump = 0;
    s->live_bytes = 0;
    s->next_free = L.free_list;
    s->on_free_list = 1;
    L.free_list = s;
    L.free_count++;
}

/* Bump-allocate raw space. Pops free segments as needed. NULL when the
 * pool is exhausted. */
static lsaEntry *rawBump(uint32_t need) {
    lsaSegment *s = L.active;
    if (!s || s->bump + need > LSA_SEGMENT_SIZE) {
        lsaSegment *ns = popFree();
        if (!ns) return NULL;
        L.active = ns;
        s = ns;
    }
    lsaEntry *e = (lsaEntry *)(s->payload + s->bump);
    s->bump += need;
    s->live_bytes += need;
    L.used_bytes += need;
    L.live_bytes += need;
    return e;
}

/* Clean one victim: the most-dead non-active used segment. Live entries are
 * copied to the bump head and their owner slot rewritten; the victim is
 * wholesale-recycled. Fully-dead segments are reclaimed without a walk
 * (zero copy). Returns 1 if a segment was reclaimed. */
static int cleanOneVictim(void) {
    lsaSegment *victim = NULL;
    uint64_t best_dead = 0;
    for (uint32_t i = 0; i < L.nsegments; i++) {
        lsaSegment *s = &L.segs[i];
        if (s->on_free_list || s == L.active || s->bump == 0) continue;
        if (s->live_bytes == 0) {
            /* Dead-share fast path: every entry is tombstoned; recycle
             * wholesale, no walk, no copying. */
            pushFree(s);
            L.dead_fastpath_reclaims++;
            L.clean_passes++;
            return 1;
        }
        uint64_t dead = s->bump - s->live_bytes;
        if (dead > best_dead) {
            best_dead = dead;
            victim = s;
        }
    }
    if (!victim) return 0;

    /* Destination capacity: active remainder + free segments. Copying pops
     * from the free list; the victim is only recycled after the walk, so the
     * walk must be fully coverable up front. */
    uint64_t cap = L.free_count * (uint64_t)LSA_SEGMENT_SIZE;
    if (L.active) cap += LSA_SEGMENT_SIZE - L.active->bump;
    if (cap < victim->live_bytes) return 0;

    uint32_t off = 0;
    while (off < victim->bump) {
        lsaEntry *e = (lsaEntry *)(victim->payload + off);
        uint32_t esize = entrySize(e);
        if (esize == 0) abort(); /* corrupt walk guard */
        if (!entryTombstoned(e)) {
            /* Owner is alive (tombstone-before-robj-free invariant), so the
             * deref is safe. Pointer equality = liveness. */
            char *sds_base = e->data + entrySdsOff(e);
            void **owner = e->owner;
            if (owner && *owner == sds_base) {
                lsaEntry *ne = rawBump(esize);
                if (!ne) abort(); /* capacity was pre-checked */
                memcpy(ne, e, esize);
                *owner = ne->data + entrySdsOff(ne);
                L.copied_bytes += esize;
                /* victim's live accounting drops with the victim below */
                L.live_bytes -= esize; /* rawBump double-counted */
                victim->live_bytes -= esize;
            } else {
                /* Slot points elsewhere: value was replaced without a free.
                 * Treated as dead. Should not happen with the sdsfree seam. */
                L.owner_mismatch_dead++;
                tombstone(e);
                L.tombstones--; /* not a client-visible free */
            }
        }
        off += esize;
    }
    pushFree(victim);
    L.clean_passes++;
    return 1;
}

static uint64_t nowUs(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000ull + ts.tv_nsec / 1000;
}

int lsaInit(size_t size_mb, int band_lo, int band_hi) {
    if (L.inited) return 1;
    size_t nseg = (size_mb * 1024 * 1024) / LSA_SEGMENT_SIZE;
    if (nseg < 2) nseg = 2;
    L.region_size = nseg * LSA_SEGMENT_SIZE;
    L.region = mmap(NULL, L.region_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    if (L.region == MAP_FAILED) {
        L.region = NULL;
        return 0;
    }
    L.nsegments = (uint32_t)nseg;
    L.segs = calloc(nseg, sizeof(lsaSegment));
    if (!L.segs) {
        munmap(L.region, L.region_size);
        L.region = NULL;
        return 0;
    }
    for (uint32_t i = 0; i < L.nsegments; i++) {
        L.segs[i].id = i;
        L.segs[i].payload = L.region + (size_t)i * LSA_SEGMENT_SIZE;
        L.segs[i].on_free_list = 1;
        L.segs[i].next_free = (i + 1 < L.nsegments) ? &L.segs[i + 1] : NULL;
    }
    L.free_list = &L.segs[0];
    L.free_count = L.nsegments;
    L.active = NULL;
    if (band_lo < 1) band_lo = 1;
    if (band_hi > 99) band_hi = 99;
    if (band_lo >= band_hi) band_lo = band_hi - 1;
    L.band_lo = band_lo;
    L.band_hi = band_hi;
    L.inited = 1;
    sdsLsaIsLog = hookIsLog;
    sdsLsaFreeLog = hookFreeLog;
    sdsLsaEscapeLog = hookEscapeLog;
    return 1;
}

int lsaEnabled(void) {
    return L.inited;
}

sds lsaAdmitSds(const_sds src) {
    if (!L.inited) return NULL;
    size_t len = sdslen(src);
    if (len > LSA_EMAX) return NULL;
    char type = sdsReqType(len);
    /* TYPE_5 has no alloc field and sdsAllocSize() would call
     * malloc_usable_size on a log pointer. Force TYPE_8 minimum. */
    if (type == SDS_TYPE_5) type = SDS_TYPE_8;
    size_t hdrlen = sdsHdrSize(type);
    size_t bufsize = hdrlen + len + 1;
    uint32_t need = alignUp((uint32_t)(sizeof(lsaEntry) + bufsize));
    lsaEntry *e = rawBump(need);
    if (!e) {
        /* Emergency inline clean, then retry once. */
        L.emergency_cleans++;
        if (cleanOneVictim()) e = rawBump(need);
        if (!e) {
            L.heap_fallbacks++;
            return NULL;
        }
    }
    e->meta = (need & LSA_META_SIZE_MASK) | ((uint32_t)hdrlen << LSA_META_OFF_SHIFT);
    e->owner = NULL; /* set by lsaSetOwner right after the robj exists */
    sds s = sdswrite(e->data, bufsize, type, src, len);
    L.admissions++;
    L.alloc_bytes += need;
    return s;
}

int lsaIsLogSds(const_sds s) {
    return L.inited && hookIsLog(sdsAllocPtr(s));
}

void lsaSetOwner(sds s, void **owner) {
    entryFromAlloc(sdsAllocPtr(s))->owner = owner;
}

/* Band cleaner: trigger when live < band_lo% of used, clean until
 * live >= band_hi% of used. No time budget: write-amp and cleaning CPU
 * are accepted by directive; the cleaner drains to the band ceiling every
 * tick (each pass reclaims one segment, so a tick is bounded by pool size). */
void lsaCleanerCron(void) {
    if (!L.inited) return;
    uint64_t t0 = nowUs();
    while (L.used_bytes * (uint64_t)L.band_lo > L.live_bytes * 100ull) { /* live < lo% of used */
        if (!cleanOneVictim()) break;
        if (L.used_bytes * (uint64_t)L.band_hi <= L.live_bytes * 100ull) break; /* live >= hi% */
    }
    L.clean_us += nowUs() - t0;
    /* Return physical pages of every free segment (decay-equivalent to
     * jemalloc's dirty-page purging; refault cost on reuse is accepted). */
    for (lsaSegment *s = L.free_list; s; s = s->next_free) {
        if (!s->madvised && s->ever_used) {
            madvise(s->payload, LSA_SEGMENT_SIZE, MADV_DONTNEED);
            s->madvised = 1;
        }
    }
    /* Frozen (non-active, in-use) segments recycled from the free list may
     * carry stale-touched pages above their final bump from an earlier
     * incarnation. Return that tail once per incarnation. */
    for (uint32_t i = 0; i < L.nsegments; i++) {
        lsaSegment *s = &L.segs[i];
        if (s->on_free_list || s == L.active || s->bump == 0 || s->tail_purged) continue;
        size_t start = (s->bump + 4095u) & ~4095u;
        if (start < LSA_SEGMENT_SIZE) madvise(s->payload + start, LSA_SEGMENT_SIZE - start, MADV_DONTNEED);
        s->tail_purged = 1;
    }
}

sds lsaInfoAppend(sds info) {
    uint64_t touched = 0;
    for (uint32_t i = 0; i < L.nsegments; i++) {
        lsaSegment *s = &L.segs[i];
        if (!s->ever_used) continue;
        if (s->on_free_list) {
            if (!s->madvised) touched += LSA_SEGMENT_SIZE;
        } else if (s->tail_purged) {
            touched += (s->bump + 4095u) & ~4095u;
        } else {
            touched += LSA_SEGMENT_SIZE;
        }
    }
    double write_amp = L.alloc_bytes ? (double)(L.alloc_bytes + L.copied_bytes) / (double)L.alloc_bytes : 1.0;
    return sdscatprintf(info,
                        "lsa_enabled:%d\r\n"
                        "lsa_band_lo:%d\r\n"
                        "lsa_band_hi:%d\r\n"
                        "lsa_committed_bytes:%llu\r\n"
                        "lsa_live_bytes:%llu\r\n"
                        "lsa_dead_bytes:%llu\r\n"
                        "lsa_physical_touched_bytes:%llu\r\n"
                        "lsa_segments_total:%u\r\n"
                        "lsa_segments_free:%u\r\n"
                        "lsa_admissions:%llu\r\n"
                        "lsa_tombstones:%llu\r\n"
                        "lsa_escapes:%llu\r\n"
                        "lsa_alloc_bytes:%llu\r\n"
                        "lsa_copied_bytes:%llu\r\n"
                        "lsa_write_amp:%.4f\r\n"
                        "lsa_clean_passes:%llu\r\n"
                        "lsa_dead_fastpath_reclaims:%llu\r\n"
                        "lsa_clean_us:%llu\r\n"
                        "lsa_emergency_cleans:%llu\r\n"
                        "lsa_heap_fallbacks:%llu\r\n"
                        "lsa_owner_mismatch_dead:%llu\r\n",
                        L.inited, L.band_lo, L.band_hi, (unsigned long long)L.used_bytes,
                        (unsigned long long)L.live_bytes, (unsigned long long)(L.used_bytes - L.live_bytes),
                        (unsigned long long)touched, L.nsegments, L.free_count,
                        (unsigned long long)L.admissions, (unsigned long long)L.tombstones,
                        (unsigned long long)L.escapes, (unsigned long long)L.alloc_bytes,
                        (unsigned long long)L.copied_bytes, write_amp, (unsigned long long)L.clean_passes,
                        (unsigned long long)L.dead_fastpath_reclaims, (unsigned long long)L.clean_us,
                        (unsigned long long)L.emergency_cleans, (unsigned long long)L.heap_fallbacks,
                        (unsigned long long)L.owner_mismatch_dead);
}
