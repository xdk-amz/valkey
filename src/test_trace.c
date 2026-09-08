#include "chrome_trace.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

void test_function(void) {
    TRACE_FUNCTION();
    {
        TRACE_SCOPE("inner");
        TRACE_INSTANT("inside_scope");
    }
}

void *thread_worker(void *arg) {
    (void)arg;
    for (int i = 0; i < 1000; i++) {
        TRACE_BEGIN("thread_event");
        TRACE_END("thread_event");
    }
    return NULL;
}

int main(void) {
    const char *path = "/tmp/test_trace.json";

    /* Remove any leftover trace file */
    remove(path);

    TRACE_START(path);

    /* (3) TRACE_FUNCTION via test_function */
    test_function();

    /* (5) Manual BEGIN/END */
    TRACE_BEGIN("manual_span");
    TRACE_END("manual_span");

    /* (6) Instant event */
    TRACE_INSTANT("checkpoint");

    /* (7) Counters */
    TRACE_COUNTER_SET("my_counter", 42);
    TRACE_COUNTER_INC("my_counter");
    TRACE_COUNTER_DEC("my_counter");

    /* (8) Spawn 4 threads each writing 1000 events */
    pthread_t threads[4];
    for (int i = 0; i < 4; i++)
        pthread_create(&threads[i], NULL, thread_worker, NULL);
    for (int i = 0; i < 4; i++)
        pthread_join(threads[i], NULL);

    TRACE_STOP();

#ifdef CHROME_TRACE_ENABLED
    /* Validate output only when tracing is enabled */
    int pass = 1;
    FILE *f = fopen(path, "r");
    if (!f) { printf("FAIL: cannot open %s\n", path); return 1; }

    fseek(f, 0, SEEK_END);
    long len = ftell(f);
    fseek(f, 0, SEEK_SET);
    char *buf = malloc(len + 1);
    fread(buf, 1, len, f);
    buf[len] = '\0';
    fclose(f);

    if (buf[0] != '{') { printf("FAIL: does not start with '{'\n"); pass = 0; }
    /* Check for traceEvents array */
    if (!strstr(buf, "\"traceEvents\"")) { printf("FAIL: missing traceEvents key\n"); pass = 0; }

    const char *expected[] = {"test_function", "inner", "manual_span", "checkpoint",
                              "my_counter", "thread_event", NULL};
    for (int i = 0; expected[i]; i++) {
        if (!strstr(buf, expected[i])) {
            printf("FAIL: missing event '%s'\n", expected[i]);
            pass = 0;
        }
    }

    free(buf);
    printf("%s\n", pass ? "PASS" : "FAIL");
    return pass ? 0 : 1;
#else
    /* When disabled, verify no trace file was created */
    FILE *f = fopen(path, "r");
    if (f) {
        fclose(f);
        printf("FAIL: trace file created when tracing disabled\n");
        return 1;
    }
    printf("PASS (no-op)\n");
    return 0;
#endif
}
