/* dplus no-op stubs for standalone tools (valkey-cli, valkey-benchmark).
 *
 * hashtable.c calls the dplus exclusive-mode hooks (dplusExclusiveEnter/Leave),
 * whose real definitions live in dplus.c -- which depends on the server core
 * and is not linked into the tools. The tools have no speculative walkers, so
 * no-ops are correct.
 *
 * This MUST be a separate translation unit linked ONLY into the tools:
 * same-TU weak stubs get inlined by LTO at hashtable.c's call sites before
 * strong-symbol resolution, silently no-op'ing the server's drains. */

void dplusExclusiveEnter(void) {}
void dplusExclusiveLeave(void) {}
