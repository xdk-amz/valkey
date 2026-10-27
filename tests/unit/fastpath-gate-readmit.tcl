# A transient server-wide gate (client pause, forkless save, replication throttler) sends fast-path
# clients to main and turns new connections away. Once the gate lifts, both kinds of client must
# come back to the fast path; before this fix they stayed on main for the life of the connection.
# CLIENT PAUSE is the deterministic stand-in for the three gates, which share one check.

proc fpgr_client {} {
    return [valkey [srv 0 host] [srv 0 port] 1 $::tls]
}

proc fpgr_wait_fastpath_clients {n} {
    wait_for_condition 100 20 {
        [getInfoProperty [r info fastpath] fastpath_clients] == $n
    } else {
        fail "expected $n fast-path clients: [r info fastpath]"
    }
}

start_server {tags {"fastpath gate readmit external:skip tls:skip"} overrides {io-threads 4}} {
    assert_equal {io-threads-fast-path yes} [r config get io-threads-fast-path]
    r client setname fpgr-control
    r select 0

    test {Fast path gate: an admitted client sent to main by a pause rejoins when the pause lifts} {
        set a [fpgr_client]
        $a ping
        assert_equal PONG [$a read]
        fpgr_wait_fastpath_clients 1

        r client pause 30000 write
        # The next command runs on main (the gate is up); the client leaves the fast path.
        $a ping
        assert_equal PONG [$a read]
        fpgr_wait_fastpath_clients 0
        # Still gated: a further command keeps it on main and must not drop the readmit mark.
        $a ping
        assert_equal PONG [$a read]
        assert_equal 0 [getInfoProperty [r info fastpath] fastpath_clients]

        r client unpause
        $a ping
        assert_equal PONG [$a read]
        fpgr_wait_fastpath_clients 1
        $a close
        fpgr_wait_fastpath_clients 0
    }

    test {Fast path gate: a client accepted during a pause joins when the pause lifts} {
        r client pause 30000 write
        set b [fpgr_client]
        $b ping
        assert_equal PONG [$b read]
        assert_equal 0 [getInfoProperty [r info fastpath] fastpath_clients]

        r client unpause
        $b ping
        assert_equal PONG [$b read]
        fpgr_wait_fastpath_clients 1

        # A client that left for a reason of its own stays on main after the gate lifts.
        set c [fpgr_client]
        $c client setname stays-on-main
        assert_equal OK [$c read]
        r client pause 30000 write
        $c ping
        assert_equal PONG [$c read]
        r client unpause
        $c ping
        assert_equal PONG [$c read]
        after 100
        assert_equal 1 [getInfoProperty [r info fastpath] fastpath_clients]
        $b close
        $c close
        fpgr_wait_fastpath_clients 0
    }
}
