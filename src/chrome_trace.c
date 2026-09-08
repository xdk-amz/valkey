#include "chrome_trace.h"

#ifdef CHROME_TRACE_ENABLED

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sys/time.h>
#include <unistd.h>

#define TRACE_BUFFER_SIZE (64 * 1024)
#define TRACE_MAX_THREADS 64
#define TRACE_MAX_COUNTERS 128
#define TRACE_DEFAULT_FLUSH_COUNT 5000

typedef struct {
    char data[TRACE_BUFFER_SIZE];
    int offset;
    int count;
} TraceBuffer;

typedef struct TraceThreadNode {
    TraceBuffer buffer;
    struct TraceThreadNode *next;
} TraceThreadNode;

/* Global state */
static FILE *traceFile = NULL;
static pthread_mutex_t traceMutex = PTHREAD_MUTEX_INITIALIZER;
static TraceThreadNode *traceThreadList = NULL;
static int traceThreadCount = 0;
static int traceFlushPolicy = TRACE_DEFAULT_FLUSH_COUNT;

/* Counter table */
typedef struct {
    const char *name;
    int64_t value;
} TraceCounter;
static TraceCounter traceCounters[TRACE_MAX_COUNTERS];
static int traceCounterCount = 0;
static pthread_mutex_t counterMutex = PTHREAD_MUTEX_INITIALIZER;

static __thread TraceThreadNode *tlsNode = NULL;

uint64_t chromeTraceTimestamp(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (uint64_t)tv.tv_sec * 1000000ULL + (uint64_t)tv.tv_usec;
}

static TraceThreadNode *getThreadNode(void) {
    if (tlsNode) return tlsNode;
    TraceThreadNode *node = (TraceThreadNode *)calloc(1, sizeof(TraceThreadNode));
    if (!node) return NULL;
    pthread_mutex_lock(&traceMutex);
    if (traceThreadCount < TRACE_MAX_THREADS) {
        node->next = traceThreadList;
        traceThreadList = node;
        traceThreadCount++;
    }
    pthread_mutex_unlock(&traceMutex);
    tlsNode = node;
    return node;
}

/* Flush one buffer to file. Caller must hold traceMutex. */
static void flushBufferLocked(TraceThreadNode *node) {
    if (node->buffer.offset > 0 && traceFile) {
        fwrite(node->buffer.data, 1, node->buffer.offset, traceFile);
        node->buffer.offset = 0;
        node->buffer.count = 0;
    }
}

static void appendEvent(TraceThreadNode *node, const char *event, int len) {
    /* Buffer overflow -> flush */
    if (node->buffer.offset + len > TRACE_BUFFER_SIZE) {
        pthread_mutex_lock(&traceMutex);
        flushBufferLocked(node);
        pthread_mutex_unlock(&traceMutex);
    }
    if (node->buffer.offset + len <= TRACE_BUFFER_SIZE) {
        memcpy(node->buffer.data + node->buffer.offset, event, len);
        node->buffer.offset += len;
        node->buffer.count++;
    }
    /* Flush on policy */
    if (traceFlushPolicy == 0 ||
        (traceFlushPolicy > 0 && node->buffer.count >= traceFlushPolicy)) {
        pthread_mutex_lock(&traceMutex);
        flushBufferLocked(node);
        pthread_mutex_unlock(&traceMutex);
    }
}

static void writeEvent(const char *ph, const char *name, const char *cat) {
    if (!traceFile) return;
    TraceThreadNode *node = getThreadNode();
    if (!node) return;

    uint64_t ts = chromeTraceTimestamp();
    pid_t pid = getpid();
    unsigned long tid = (unsigned long)pthread_self();
    char event[512];
    int len;

    if (cat && cat[0]) {
        len = snprintf(event, sizeof(event),
            "{\"ph\":\"%s\",\"name\":\"%s\",\"cat\":\"%s\",\"ts\":%" PRIu64 ",\"pid\":%d,\"tid\":%lu},\n",
            ph, name, cat, ts, (int)pid, tid);
    } else {
        len = snprintf(event, sizeof(event),
            "{\"ph\":\"%s\",\"name\":\"%s\",\"ts\":%" PRIu64 ",\"pid\":%d,\"tid\":%lu},\n",
            ph, name, ts, (int)pid, tid);
    }
    if (len <= 0 || len >= (int)sizeof(event)) return;
    appendEvent(node, event, len);
}

void chromeTraceInit(const char *filename) {
    chromeTraceInitWithBufferSize(filename, TRACE_DEFAULT_FLUSH_COUNT);
}

void chromeTraceInitWithBufferSize(const char *filename, int bufferSize) {
    pthread_mutex_lock(&traceMutex);
    if (traceFile) { pthread_mutex_unlock(&traceMutex); return; }
    traceFile = fopen(filename, "w");
    if (!traceFile) { pthread_mutex_unlock(&traceMutex); return; }
    traceFlushPolicy = bufferSize;
    traceThreadList = NULL;
    traceThreadCount = 0;
    traceCounterCount = 0;
    fputs("{\"traceEvents\":[\n", traceFile);
    pthread_mutex_unlock(&traceMutex);
}

void chromeTraceFlush(void) {
    pthread_mutex_lock(&traceMutex);
    TraceThreadNode *node = traceThreadList;
    while (node) {
        flushBufferLocked(node);
        node = node->next;
    }
    if (traceFile) fflush(traceFile);
    pthread_mutex_unlock(&traceMutex);
}

void chromeTraceClose(void) {
    chromeTraceFlush();
    pthread_mutex_lock(&traceMutex);
    if (traceFile) {
        /* Remove trailing ",\n" to produce valid JSON */
        long pos = ftell(traceFile);
        if (pos > 2) {
            fseek(traceFile, -2, SEEK_CUR);
            fputs("\n]}\n", traceFile);
            ftruncate(fileno(traceFile), ftell(traceFile));
        } else {
            fputs("]}\n", traceFile);
        }
        fclose(traceFile);
        traceFile = NULL;
    }
    TraceThreadNode *node = traceThreadList;
    while (node) {
        TraceThreadNode *next = node->next;
        free(node);
        node = next;
    }
    traceThreadList = NULL;
    traceThreadCount = 0;
    tlsNode = NULL;
    pthread_mutex_unlock(&traceMutex);
}

void chromeTraceBegin(const char *name, const char *cat) { writeEvent("B", name, cat); }
void chromeTraceEnd(const char *name, const char *cat) { writeEvent("E", name, cat); }
void chromeTraceInstant(const char *name, const char *cat) { writeEvent("i", name, cat); }

void chromeTraceCounter(const char *name, int64_t value) {
    if (!traceFile) return;
    TraceThreadNode *node = getThreadNode();
    if (!node) return;

    /* Update counter table */
    pthread_mutex_lock(&counterMutex);
    int idx = -1;
    for (int i = 0; i < traceCounterCount; i++) {
        if (traceCounters[i].name == name || strcmp(traceCounters[i].name, name) == 0) {
            idx = i; break;
        }
    }
    if (idx < 0 && traceCounterCount < TRACE_MAX_COUNTERS) {
        idx = traceCounterCount++;
        traceCounters[idx].name = name;
    }
    if (idx >= 0) traceCounters[idx].value = value;
    pthread_mutex_unlock(&counterMutex);

    /* Emit counter event */
    uint64_t ts = chromeTraceTimestamp();
    pid_t pid = getpid();
    unsigned long tid = (unsigned long)pthread_self();
    char event[512];
    int len;

    len = snprintf(event, sizeof(event),
        "{\"ph\":\"C\",\"name\":\"%s\",\"ts\":%" PRIu64 ",\"pid\":%d,\"tid\":%lu,\"args\":{\"%s\":%" PRId64 "}},\n",
        name, ts, (int)pid, tid, name, value);
    if (len <= 0 || len >= (int)sizeof(event)) return;
    appendEvent(node, event, len);
}

void chromeTraceCounterDelta(const char *name, int64_t delta) {
    if (!traceFile) return;
    int64_t newValue = 0;
    pthread_mutex_lock(&counterMutex);
    int idx = -1;
    for (int i = 0; i < traceCounterCount; i++) {
        if (traceCounters[i].name == name || strcmp(traceCounters[i].name, name) == 0) {
            idx = i; break;
        }
    }
    if (idx < 0 && traceCounterCount < TRACE_MAX_COUNTERS) {
        idx = traceCounterCount++;
        traceCounters[idx].name = name;
        traceCounters[idx].value = 0;
    }
    if (idx >= 0) {
        traceCounters[idx].value += delta;
        newValue = traceCounters[idx].value;
    }
    pthread_mutex_unlock(&counterMutex);

    /* Emit counter event directly (don't call chromeTraceCounter to avoid double-lock) */
    if (idx < 0) return;
    TraceThreadNode *node = getThreadNode();
    if (!node) return;
    uint64_t ts = chromeTraceTimestamp();
    pid_t pid = getpid();
    unsigned long tid = (unsigned long)pthread_self();
    char event[512];
    int len = snprintf(event, sizeof(event),
        "{\"ph\":\"C\",\"name\":\"%s\",\"ts\":%" PRIu64 ",\"pid\":%d,\"tid\":%lu,\"args\":{\"%s\":%" PRId64 "}},\n",
        name, ts, (int)pid, tid, name, newValue);
    if (len <= 0 || len >= (int)sizeof(event)) return;
    appendEvent(node, event, len);
}

#endif /* CHROME_TRACE_ENABLED */
