/* Phase barrier between main and worker threads. See phase_barrier.h. */

#include "fmacros.h"
#include "phase_barrier.h"

#include <string.h>

#define PHASE_BARRIER_SPIN 1024

static inline void phaseBarrierRelax(void) {
#if defined(__x86_64__) || defined(__i386__)
    __builtin_ia32_pause();
#elif defined(__aarch64__)
    __asm__ __volatile__("isb" ::: "memory");
#endif
}

static inline uint32_t stateSeq(uint64_t st) {
    return (uint32_t)(st >> 32);
}
static inline int stateN(uint64_t st) {
    return (int)((st >> 16) & 0xffff);
}
static inline int statePhase(uint64_t st) {
    return (int)(st & 0xffff);
}

int phaseBarrierInit(phaseBarrier *b, int nparticipants) {
    if (nparticipants < 0 || nparticipants > PHASE_BARRIER_MAX_PARTICIPANTS) return -1;
    memset(b, 0, sizeof(*b));
    atomic_init(&b->state, 0);
    atomic_init(&b->pending, 0);
    atomic_init(&b->shutdown, 0);
    b->nparticipants = nparticipants;
    if (pthread_mutex_init(&b->lock, NULL) != 0) return -1;
    if (pthread_cond_init(&b->open_cv, NULL) != 0) goto err_lock;
    if (pthread_cond_init(&b->park_cv, NULL) != 0) goto err_open;
    if (pthread_cond_init(&b->done_cv, NULL) != 0) goto err_park;
    return 0;
err_park:
    pthread_cond_destroy(&b->park_cv);
err_open:
    pthread_cond_destroy(&b->open_cv);
err_lock:
    pthread_mutex_destroy(&b->lock);
    return -1;
}

void phaseBarrierDestroy(phaseBarrier *b) {
    pthread_cond_destroy(&b->done_cv);
    pthread_cond_destroy(&b->park_cv);
    pthread_cond_destroy(&b->open_cv);
    pthread_mutex_destroy(&b->lock);
}

int phaseBarrierSetParticipants(phaseBarrier *b, int nparticipants) {
    if (nparticipants < 0 || nparticipants > PHASE_BARRIER_MAX_PARTICIPANTS) return -1;
    pthread_mutex_lock(&b->lock);
    if (atomic_load_explicit(&b->pending, memory_order_acquire) != 0) {
        pthread_mutex_unlock(&b->lock);
        return -1;
    }
    b->nparticipants = nparticipants;
    if (b->park_sleepers) pthread_cond_broadcast(&b->park_cv);
    pthread_mutex_unlock(&b->lock);
    return 0;
}

int phaseBarrierOpen(phaseBarrier *b, int phase_id) {
    if (phase_id < 0 || phase_id > PHASE_BARRIER_MAX_ID) return -1;
    if (atomic_load_explicit(&b->shutdown, memory_order_relaxed)) return -1;
    if (atomic_load_explicit(&b->pending, memory_order_acquire) != 0) return -1;
    pthread_mutex_lock(&b->lock);
    int n = b->nparticipants;
    uint64_t seq = (uint64_t)(stateSeq(atomic_load_explicit(&b->state, memory_order_relaxed)) + 1u);
    /* pending must be visible before the phase is, since a participant may finish at once. */
    atomic_store_explicit(&b->pending, n, memory_order_relaxed);
    atomic_store_explicit(&b->state, seq << 32 | (uint64_t)n << 16 | (uint64_t)phase_id, memory_order_release);
    if (b->open_sleepers) pthread_cond_broadcast(&b->open_cv);
    pthread_mutex_unlock(&b->lock);
    return 0;
}

void phaseBarrierWaitAllDone(phaseBarrier *b, int spin) {
    for (int i = 0, n = spin ? PHASE_BARRIER_SPIN : 0; i < n; i++) {
        if (atomic_load_explicit(&b->pending, memory_order_acquire) == 0) return;
        phaseBarrierRelax();
    }
    pthread_mutex_lock(&b->lock);
    while (atomic_load_explicit(&b->pending, memory_order_acquire) != 0) {
        b->done_sleeper = 1;
        pthread_cond_wait(&b->done_cv, &b->lock);
        b->done_sleeper = 0;
    }
    pthread_mutex_unlock(&b->lock);
}

void phaseBarrierShutdown(phaseBarrier *b) {
    pthread_mutex_lock(&b->lock);
    atomic_store_explicit(&b->shutdown, 1, memory_order_release);
    pthread_cond_broadcast(&b->open_cv);
    pthread_cond_broadcast(&b->park_cv);
    pthread_mutex_unlock(&b->lock);
}

/* Returns 1 with *phase set if st names a new phase that includes worker_id; marks it seen either way. */
static inline int takePhase(uint64_t st, int worker_id, uint64_t *last_seen, int *phase) {
    if (stateSeq(st) == (uint32_t)*last_seen) return 0;
    *last_seen = stateSeq(st);
    if (worker_id >= stateN(st)) return 0;
    *phase = statePhase(st);
    return 1;
}

int phaseBarrierAwaitPhase(phaseBarrier *b, int worker_id, uint64_t *last_seen, int spin) {
    int phase;
    for (int i = 0, n = spin ? PHASE_BARRIER_SPIN : 0; i < n; i++) {
        if (atomic_load_explicit(&b->shutdown, memory_order_acquire)) return PHASE_BARRIER_EXIT;
        uint64_t st = atomic_load_explicit(&b->state, memory_order_acquire);
        if (takePhase(st, worker_id, last_seen, &phase)) return phase;
        if (worker_id >= stateN(st)) break; /* not a participant now: park instead of spinning */
        phaseBarrierRelax();
    }
    pthread_mutex_lock(&b->lock);
    for (;;) {
        if (atomic_load_explicit(&b->shutdown, memory_order_acquire)) {
            phase = PHASE_BARRIER_EXIT;
            break;
        }
        uint64_t st = atomic_load_explicit(&b->state, memory_order_acquire);
        if (takePhase(st, worker_id, last_seen, &phase)) break;
        if (worker_id >= b->nparticipants) {
            b->park_sleepers++;
            pthread_cond_wait(&b->park_cv, &b->lock);
            b->park_sleepers--;
        } else {
            b->open_sleepers++;
            pthread_cond_wait(&b->open_cv, &b->lock);
            b->open_sleepers--;
        }
    }
    pthread_mutex_unlock(&b->lock);
    return phase;
}

void phaseBarrierDone(phaseBarrier *b) {
    if (atomic_fetch_sub_explicit(&b->pending, 1, memory_order_acq_rel) != 1) return;
    pthread_mutex_lock(&b->lock);
    if (b->done_sleeper) pthread_cond_signal(&b->done_cv);
    pthread_mutex_unlock(&b->lock);
}
