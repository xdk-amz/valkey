#ifndef CHROME_TRACE_H
#define CHROME_TRACE_H

#include <stdint.h>

#ifdef CHROME_TRACE_ENABLED

/* Function declarations */
void chromeTraceInit(const char *filename);
void chromeTraceInitWithBufferSize(const char *filename, int bufferSize);
void chromeTraceClose(void);
void chromeTraceFlush(void);
void chromeTraceBegin(const char *name, const char *cat);
void chromeTraceEnd(const char *name, const char *cat);
void chromeTraceInstant(const char *name, const char *cat);
void chromeTraceCounter(const char *name, int64_t value);
void chromeTraceCounterDelta(const char *name, int64_t delta);
uint64_t chromeTraceTimestamp(void);

/* Cleanup helper for TRACE_FUNCTION / TRACE_SCOPE */
typedef struct {
    const char *name;
    const char *cat;
} chromeTraceScopeGuard;

static inline void chromeTraceScopeEnd(chromeTraceScopeGuard *guard) {
    chromeTraceEnd(guard->name, guard->cat);
}

/* Public macros */
#define TRACE_START(filename) chromeTraceInit(filename)
#define TRACE_START_BUFFERED(filename, n) chromeTraceInitWithBufferSize(filename, n)
#define TRACE_STOP() chromeTraceClose()
#define TRACE_FLUSH() chromeTraceFlush()

#define TRACE_FUNCTION() \
    chromeTraceBegin(__func__, "function"); \
    chromeTraceScopeGuard __attribute__((cleanup(chromeTraceScopeEnd))) \
        _trace_func_guard_ = { __func__, "function" }

#define TRACE_SCOPE_PASTE(a, b) a##b
#define TRACE_SCOPE_NAME(b) TRACE_SCOPE_PASTE(_trace_scope_guard_, b)

#define TRACE_SCOPE(name) \
    chromeTraceBegin(name, "scope"); \
    chromeTraceScopeGuard __attribute__((cleanup(chromeTraceScopeEnd))) \
        TRACE_SCOPE_NAME(__COUNTER__) = { name, "scope" }

#define TRACE_BEGIN(name) chromeTraceBegin(name, "")
#define TRACE_END(name) chromeTraceEnd(name, "")
#define TRACE_INSTANT(name) chromeTraceInstant(name, "")
#define TRACE_COUNTER_SET(name, value) chromeTraceCounter(name, (int64_t)(value))
#define TRACE_COUNTER_INC(name) chromeTraceCounterDelta(name, 1)
#define TRACE_COUNTER_DEC(name) chromeTraceCounterDelta(name, -1)

#else /* CHROME_TRACE_ENABLED not defined */

#define TRACE_START(filename) ((void)0)
#define TRACE_START_BUFFERED(filename, n) ((void)0)
#define TRACE_STOP() ((void)0)
#define TRACE_FLUSH() ((void)0)
#define TRACE_FUNCTION() ((void)0)
#define TRACE_SCOPE(name) ((void)0)
#define TRACE_BEGIN(name) ((void)0)
#define TRACE_END(name) ((void)0)
#define TRACE_INSTANT(name) ((void)0)
#define TRACE_COUNTER_SET(name, value) ((void)0)
#define TRACE_COUNTER_INC(name) ((void)0)
#define TRACE_COUNTER_DEC(name) ((void)0)

#endif /* CHROME_TRACE_ENABLED */

#endif /* CHROME_TRACE_H */
