/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Helpers for test suites that bring up parts of the server in SetUpTestSuite.
 *
 * All suites run in one process, and --gtest_repeat runs SetUpTestSuite again
 * for every iteration. Some server init functions are written to run once per
 * process: calling them a second time deadlocks or aborts. The helpers below
 * make the second and later calls safe. Every helper is an inline function, so
 * its function-local static is shared by all test files (C++17 [dcl.inline]).
 */

#ifndef VALKEY_UNIT_TEST_SERVER_INIT_HPP
#define VALKEY_UNIT_TEST_SERVER_INIT_HPP

extern "C" {
#include "allocator_defrag.h"
#include "module.h"
#include "server.h"
}

#include <unistd.h>

/* moduleInitModulesSystem() ends by locking the module GIL. The GIL is a plain
 * (non-recursive) mutex that the server releases only when it enters the event
 * loop. A second call in the same process blocks forever on that lock. Release
 * the lock taken by the previous call before initialising again. */
inline void unitTestInitModulesSystem(void) {
    static bool gil_locked = false;
    static int module_pipe[2] = {-1, -1};
    if (gil_locked) moduleReleaseGIL();
    /* Each call opens a new server.module_pipe. The next suite's memset(&server)
     * forgets the two fds, so without this they stay open for the rest of the
     * process, one pair per suite per --gtest_repeat iteration. */
    if (module_pipe[0] >= 0) {
        close(module_pipe[0]);
        close(module_pipe[1]);
    }
    moduleInitModulesSystem();
    module_pipe[0] = server.module_pipe[0];
    module_pipe[1] = server.module_pipe[1];
    gil_locked = true;
}

/* aeCreateEventLoop() opens one epoll fd. A suite that creates server.el in
 * SetUpTestSuite and never deletes it leaks that fd on every iteration, and
 * the next suite's memset(&server) drops the pointer. Tests that build a small
 * event loop (aeCreateEventLoop(64)) then fail once the lowest free fd is 64
 * or more, because aeCreateFileEvent rejects fd >= setsize. Delete the loop
 * at the end of the suite and leave server.el NULL. */
inline void unitTestDeleteEventLoop(void) {
    if (server.el == NULL) return;
    aeDeleteEventLoop(server.el);
    server.el = NULL;
}

extern "C" {
extern struct serverCommand serverCommandTable[];
}

/* populateCommandTable() fills the static serverCommandTable entries in place
 * (fullname, key specs, ACL id, subcommand hashtable) and adds them to
 * server.commands and server.orig_commands. The filling step cannot run twice:
 * commandAddSubcommand() asserts that a subcommand is not already present in
 * the parent's subcommands_ht, so the second call aborts the process. The two
 * hashtables, however, are cleared by every suite's memset(&server), and some
 * suites release them in TearDownTestSuite, so they must be rebuilt.
 *
 * The caller creates server.commands and server.orig_commands first, as it
 * does for populateCommandTable(). The first call runs the real function;
 * later calls only re-add the already filled entries to the new tables. */
inline void unitTestPopulateCommandTable(void) {
    static bool populated = false;
    if (!populated) {
        populateCommandTable();
        populated = true;
        return;
    }
    for (int j = 0; serverCommandTable[j].declared_name != NULL; j++) {
        struct serverCommand *c = serverCommandTable + j;
        /* Same skip rule as populateCommandStructure() with sentinel_mode == 0. */
        if (c->flags & CMD_ONLY_SENTINEL) continue;
        serverAssert(hashtableAdd(server.commands, c));
        serverAssert(hashtableAdd(server.orig_commands, c));
    }
}

/* connTypeInitialize() registers each connection type in the static connTypes[]
 * array, and connTypeRegister() asserts that the slot is still empty. A second
 * call in the same process aborts. The registry does not live in the server
 * struct, so memset(&server) does not clear it: register once per process. */
inline void unitTestConnTypeInitialize(void) {
    static bool initialized = false;
    if (initialized) return;
    connTypeInitialize();
    initialized = true;
}

/* ACLInit() calls ACLInitCommandCategories(), which allocates a fresh
 * ACLCommandCategories array but keeps the static write index
 * nextCommandCategory. The second call therefore writes the default
 * categories after a block of zeroed slots, so a lookup such as "+@read"
 * stops at the first empty slot and fails, and the fourth call runs past
 * ACL_MAX_CATEGORIES (64) and asserts. All ACL state ACLInit() creates is
 * static (not in the server struct), so one call per process is enough. */
inline void unitTestACLInit(void) {
    static bool initialized = false;
    if (initialized) return;
    ACLInit();
    initialized = true;
}

/* allocatorDefragInit() asserts that it has not run before (the jemalloc
 * query keys it resolves are process state). A second call in the same
 * process aborts, and the abort then crashes in serverLogRaw because the
 * suite has no server.logfile. Initialise once per process. */
inline void unitTestAllocatorDefragInit(void) {
    static bool initialized = false;
    if (initialized) return;
    allocatorDefragInit();
    initialized = true;
}

#endif
