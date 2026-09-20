/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "generated_wrappers.hpp"

#include <arpa/inet.h>
#include <cstddef>
#include <cstring>
#include <sys/socket.h>
#include <sys/un.h>

extern "C" {
#include "fastpath.h"
#include "server.h"
}

class CommandOriginTest : public ::testing::Test {};

static void fillV4(struct sockaddr_in *sa, const char *ip, int port) {
    memset(sa, 0, sizeof(*sa));
    sa->sin_family = AF_INET;
    sa->sin_port = htons(port);
    ASSERT_EQ(inet_pton(AF_INET, ip, &sa->sin_addr), 1);
}

static void fillV6(struct sockaddr_in6 *sa, const char *ip, int port) {
    memset(sa, 0, sizeof(*sa));
    sa->sin6_family = AF_INET6;
    sa->sin6_port = htons(port);
    ASSERT_EQ(inet_pton(AF_INET6, ip, &sa->sin6_addr), 1);
}

/* The text getClientPeerId() would produce for the same address. */
static void expectedText(int family, const void *addr, int port, char *out, size_t out_len) {
    char ip[NET_IP_STR_LEN];
    ASSERT_NE(inet_ntop(family, addr, ip, sizeof(ip)), nullptr);
    formatAddr(out, out_len, ip, port);
}

TEST_F(CommandOriginTest, LayoutIsFixedSizeAndByValue) {
    /* No sds, no connection: the identity is plain data that outlives the client.
     * The one pointer is the principal, which main resolves against live ACL state. */
    EXPECT_EQ(sizeof(PeerIdentity), 20u);
    EXPECT_EQ(sizeof(CommandOrigin), 64u);
    EXPECT_EQ(sizeof(cmdEntry), 168u);
    EXPECT_EQ(offsetof(cmdEntry, handle), 0u);
    EXPECT_EQ(offsetof(cmdEntry, origin) % alignof(CommandOrigin), 0u);
    EXPECT_EQ(sizeof(cmdBatch), offsetof(cmdBatch, e) + IO_BATCH_MAX * sizeof(cmdEntry));
}

TEST_F(CommandOriginTest, Ipv4RoundTrip) {
    const char *cases[][2] = {{"127.0.0.1", "6379"}, {"10.0.0.1", "1"}, {"255.255.255.255", "65535"}, {"0.0.0.0", "0"}};
    for (auto &tc : cases) {
        struct sockaddr_in sa;
        fillV4(&sa, tc[0], atoi(tc[1]));
        PeerIdentity peer;
        ASSERT_EQ(peerIdentityFromSockaddr(&peer, (struct sockaddr *)&sa, sizeof(sa)), C_OK);
        EXPECT_EQ(peer.family, AF_INET);
        EXPECT_EQ(peer.port, atoi(tc[1]));
        char got[CONN_ADDR_STR_LEN], want[CONN_ADDR_STR_LEN];
        ASSERT_GT(peerIdentityFormat(&peer, got, sizeof(got)), 0);
        expectedText(AF_INET, &sa.sin_addr, atoi(tc[1]), want, sizeof(want));
        EXPECT_STREQ(got, want);
    }
}

TEST_F(CommandOriginTest, Ipv6RoundTrip) {
    const char *cases[][2] = {{"::1", "6379"}, {"2001:db8::1", "443"}, {"fe80::1ff:fe23:4567:890a", "65535"}, {"::ffff:192.0.2.1", "80"}, {"::", "0"}};
    for (auto &tc : cases) {
        struct sockaddr_in6 sa;
        fillV6(&sa, tc[0], atoi(tc[1]));
        PeerIdentity peer;
        ASSERT_EQ(peerIdentityFromSockaddr(&peer, (struct sockaddr *)&sa, sizeof(sa)), C_OK);
        EXPECT_EQ(peer.family, AF_INET6);
        EXPECT_EQ(peer.port, atoi(tc[1]));
        char got[CONN_ADDR_STR_LEN], want[CONN_ADDR_STR_LEN];
        ASSERT_GT(peerIdentityFormat(&peer, got, sizeof(got)), 0);
        expectedText(AF_INET6, &sa.sin6_addr, atoi(tc[1]), want, sizeof(want));
        EXPECT_STREQ(got, want);
        EXPECT_EQ(got[0], '[');
    }
}

TEST_F(CommandOriginTest, UnsupportedTransportsAreRejected) {
    PeerIdentity peer;
    struct sockaddr_un un;
    memset(&un, 0, sizeof(un));
    un.sun_family = AF_UNIX;
    strcpy(un.sun_path, "/tmp/valkey.sock");
    EXPECT_EQ(peerIdentityFromSockaddr(&peer, (struct sockaddr *)&un, sizeof(un)), C_ERR);

    struct sockaddr_storage ss;
    memset(&ss, 0, sizeof(ss));
    ss.ss_family = AF_UNSPEC;
    EXPECT_EQ(peerIdentityFromSockaddr(&peer, (struct sockaddr *)&ss, sizeof(ss)), C_ERR);
    ss.ss_family = AF_PACKET;
    EXPECT_EQ(peerIdentityFromSockaddr(&peer, (struct sockaddr *)&ss, sizeof(ss)), C_ERR);

    /* A truncated INET sockaddr is not trusted either. */
    struct sockaddr_in sa;
    fillV4(&sa, "127.0.0.1", 1);
    EXPECT_EQ(peerIdentityFromSockaddr(&peer, (struct sockaddr *)&sa, sizeof(sa) - 1), C_ERR);

    /* A rejected identity has no formatting. */
    memset(&peer, 0, sizeof(peer));
    char buf[CONN_ADDR_STR_LEN];
    EXPECT_EQ(peerIdentityFormat(&peer, buf, sizeof(buf)), -1);
}

TEST_F(CommandOriginTest, ExecutorReportsOriginNotItself) {
    client ec;
    memset(&ec, 0, sizeof(ec));
    ec.id = 7;
    ec.flag.executor = 1;

    /* Without a bound entry the executor is itself. */
    EXPECT_EQ(getClientOriginId(&ec), 7u);

    struct sockaddr_in6 sa;
    fillV6(&sa, "2001:db8::42", 40000);
    struct sockaddr_in la;
    fillV4(&la, "10.1.2.3", 6379);
    CommandOrigin origin;
    memset(&origin, 0, sizeof(origin));
    origin.client_id = 123456789;
    ASSERT_EQ(peerIdentityFromSockaddr(&origin.peer, (struct sockaddr *)&sa, sizeof(sa)), C_OK);
    ASSERT_EQ(peerIdentityFromSockaddr(&origin.local, (struct sockaddr *)&la, sizeof(la)), C_OK);
    ec.origin = &origin;
    EXPECT_EQ(getClientOriginId(&ec), 123456789u);
    EXPECT_STREQ(getClientPeerId(&ec), "[2001:db8::42]:40000");
    EXPECT_STREQ(getClientSockname(&ec), "10.1.2.3:6379");
    EXPECT_EQ(isClientConnIpV6(&ec), 1);

    /* The copy in the entry is independent of the client it was taken from. */
    CommandOrigin copy = origin;
    memset(&origin, 0xff, sizeof(origin));
    ec.origin = &copy;
    EXPECT_EQ(getClientOriginId(&ec), 123456789u);
    EXPECT_STREQ(getClientPeerId(&ec), "[2001:db8::42]:40000");
    ec.origin = NULL;
    EXPECT_EQ(getClientOriginId(&ec), 7u);
    sdsfree(ec.peerid);
    sdsfree(ec.sockname);
}

TEST_F(CommandOriginTest, PeerIdentityIpAndPort) {
    struct sockaddr_in sa;
    fillV4(&sa, "192.0.2.9", 4242);
    PeerIdentity peer;
    ASSERT_EQ(peerIdentityFromSockaddr(&peer, (struct sockaddr *)&sa, sizeof(sa)), C_OK);
    char ip[NET_IP_STR_LEN];
    int port = 0;
    ASSERT_EQ(peerIdentityToIp(&peer, ip, sizeof(ip), &port), C_OK);
    EXPECT_STREQ(ip, "192.0.2.9");
    EXPECT_EQ(port, 4242);
    memset(&peer, 0, sizeof(peer));
    EXPECT_EQ(peerIdentityToIp(&peer, ip, sizeof(ip), &port), C_ERR);
}
