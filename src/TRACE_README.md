# Chrome Tracing for Valkey

Compile-time tracing that outputs [Chrome Trace Event Format](https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU) JSON, viewable in `chrome://tracing` or [Perfetto](https://ui.perfetto.dev).

Zero runtime cost when disabled — all macros expand to `((void)0)`.

## Build

```bash
# With tracing
make CFLAGS="-DCHROME_TRACE_ENABLED"

# Without (default) — no-ops, zero overhead
make
```

## API

```c
#include "chrome_trace.h"

TRACE_START("/tmp/trace.json");       // Open trace file
TRACE_START_BUFFERED("/tmp/t.json", 1000); // Custom flush every N events

TRACE_FUNCTION();                     // Auto begin/end for current function
TRACE_SCOPE("name");                  // Auto begin/end for a named block

TRACE_BEGIN("name");                  // Manual duration begin
TRACE_END("name");                    // Manual duration end
TRACE_INSTANT("name");                // Point-in-time event

TRACE_COUNTER_SET("clients", n);      // Set counter value
TRACE_COUNTER_INC("clients");         // +1
TRACE_COUNTER_DEC("clients");         // -1

TRACE_FLUSH();                        // Force flush all buffers
TRACE_STOP();                         // Flush + close file
```

## Flush Policies

| `TRACE_START_BUFFERED(file, N)` | Behavior |
|-|--|
| N > 0 | Flush every N events per thread (default: 5000) |
| N == 0 | Unbuffered — flush every event |
| N == -1 | Fully buffered — flush only on `TRACE_STOP()` |

## Architecture

- **Thread-local 64KB buffers** — event writes need no mutex
- **Mutex only on flush** — taken when writing buffer to disk
- **Up to 64 threads** tracked via linked list
- **Counter table** — 128 named counters with delta support

## Viewing

1. Run your instrumented binary
2. Open `chrome://tracing` in Chrome (or https://ui.perfetto.dev)
3. Load the JSON file
4. Filter by category (`function`, `scope`, or custom)

## Files

| File | Purpose |
|------|---------|
| `chrome_trace.h` | Public API — macros + function declarations |
| `chrome_trace.c` | Implementation (compiled only when `CHROME_TRACE_ENABLED`) |
| `test_trace.c` | Validation test (see below) |

## test_trace.c

Standalone test that exercises every API macro and validates the output:

1. Calls every trace macro (FUNCTION, SCOPE, BEGIN/END, INSTANT, COUNTER_SET/INC/DEC)
2. Spawns 4 threads each writing 1000 events (tests thread-safety)
3. Verifies the output file is valid JSON (`[` ... `]`)
4. Checks all expected event names appear in the output
5. When compiled *without* `CHROME_TRACE_ENABLED`, verifies no file is created

```bash
# Run with tracing
gcc -DCHROME_TRACE_ENABLED -I. -o test_trace test_trace.c chrome_trace.c -lpthread
./test_trace  # prints PASS

# Run without (no-op verification)
gcc -I. -o test_trace_noop test_trace.c -lpthread
./test_trace_noop  # prints PASS (no-op)
```
