/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* An IO thread takes a partitioned client's read only from IDLE. A rearm done by another IO thread
 * registers the socket before it publishes IDLE, so readiness seen during the rearm must be claimed once
 * the rearm lands: the one-shot event that reported it does not repeat. */

#include "generated_wrappers.hpp"

#include <chrono>
#include <thread>

extern "C" {
#include "io_threads.h"
#include "server.h"
}

TEST(PartitionedReadTest, ReadinessDuringARearmIsClaimedOnceTheRearmLands) {
    client *c = static_cast<client *>(zcalloc(sizeof(client)));
    c->io_read_state = CLIENT_ARMING_IO;
    std::thread rearm([c] {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        __atomic_store_n(&c->io_read_state, CLIENT_IDLE, __ATOMIC_RELEASE);
    });
    EXPECT_EQ(partitionedClientClaimRead(c), 1);
    EXPECT_EQ(c->io_read_state, CLIENT_PENDING_IO);
    rearm.join();
    zfree(c);
}

TEST(PartitionedReadTest, NoClaimWhileMainHoldsOrOwnsTheRead) {
    client *c = static_cast<client *>(zcalloc(sizeof(client)));
    for (uint8_t state : {CLIENT_PENDING_IO, CLIENT_COMPLETED_IO, CLIENT_CLOSING_IO, CLIENT_HELD_IO}) {
        c->io_read_state = state;
        EXPECT_EQ(partitionedClientClaimRead(c), 0);
        EXPECT_EQ(c->io_read_state, state);
    }
    c->io_read_state = CLIENT_IDLE;
    EXPECT_EQ(partitionedClientClaimRead(c), 1);
    EXPECT_EQ(c->io_read_state, CLIENT_PENDING_IO);
    zfree(c);
}
