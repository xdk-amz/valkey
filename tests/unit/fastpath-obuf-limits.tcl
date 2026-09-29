# Client-output-buffer-limit enforcement for IO-owned fast-path clients. Fast-path clients are
# authenticated CLIENT_TYPE_NORMAL, so the normal class limit applies; enforcement runs on main
# through the limit registry, never by touching the IO-owned client.

# A deferring client that sends nothing before its first command, so it stays on the fast path
# (a SELECT would move it to the main path).
proc fpol_client {} {
    return [valkey [srv 0 host] [srv 0 port] 1 $::tls]
}

proc fpol_wait_fastpath_clients {n} {
    wait_for_condition 100 20 {
        [getInfoProperty [r info fastpath] fastpath_clients] == $n
    } else {
        fail "expected $n fast-path clients: [r info fastpath]"
    }
}

proc fpol_disconnections {} {
    return [getInfoProperty [r info stats] client_output_buffer_limit_disconnections]
}

start_server {tags {"fastpath obuf-limits external:skip tls:skip"} overrides {io-threads 2 io-batch-hold-us 10000}} {
    assert_equal {io-threads-fast-path yes} [r config get io-threads-fast-path]
    r client setname fp-control
    r select 0 ;# the same db as the raw fast-path clients

    test {Fast path COB: a large reply over the hard limit closes the client before delivery} {
        r config set client-output-buffer-limit "normal 100kb 0 0"
        r set cob:big [string repeat x 1048576] ;# 1 MiB value, its GET reply exceeds the 100 KiB hard cap
        set base [fpol_disconnections]
        set a [fpol_client]
        fpol_wait_fastpath_clients 1

        # Never read the reply: it is discarded at the hard-limit check, so the connection closes.
        $a get cob:big
        $a flush
        wait_for_condition 100 50 {
            [fpol_disconnections] == $base + 1
        } else {
            fail "hard-limit disconnection was not counted: [r info stats]"
        }
        fpol_wait_fastpath_clients 0
        catch {$a read} e
        assert_match {*I/O error*} $e
        $a close
        r config set client-output-buffer-limit "normal 0 0 0"
    }

    test {Fast path COB: a client under the limit is untouched and stays on the fast path} {
        r config set client-output-buffer-limit "normal 100kb 0 0"
        set base [fpol_disconnections]
        set a [fpol_client]
        fpol_wait_fastpath_clients 1

        # Small replies never approach the cap: normal request/reply parity is unchanged.
        $a set cob:small v
        assert_equal OK [$a read]
        $a get cob:small
        assert_equal v [$a read]
        $a ping
        assert_equal PONG [$a read]
        assert_equal 1 [getInfoProperty [r info fastpath] fastpath_clients]
        assert_equal $base [fpol_disconnections]
        $a close
        fpol_wait_fastpath_clients 0
        r config set client-output-buffer-limit "normal 0 0 0"
    }

    test {Fast path COB: parity holds after enforcement, a fresh client still serves normally} {
        r config set client-output-buffer-limit "normal 0 0 0"
        set a [fpol_client]
        fpol_wait_fastpath_clients 1
        $a set cob:after v
        assert_equal OK [$a read]
        $a get cob:after
        assert_equal v [$a read]
        $a close
        fpol_wait_fastpath_clients 0
    }
}
