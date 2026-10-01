# Idle-timeout enforcement for IO-owned fast-path clients. Fast-path clients are authenticated
# CLIENT_TYPE_NORMAL, so the normal `timeout` (maxidletime) applies; enforcement runs on main through
# the limit registry against an IO-owner-written stamp, never by touching the IO-owned client. An idle
# close is its own reason and counts through expired_clients even though it publishes a CLOSE.

# A deferring client that sends nothing before its first command, so it stays on the fast path
# (a SELECT would move it to the main path).
proc fpit_client {} {
    return [valkey [srv 0 host] [srv 0 port] 1 $::tls]
}

proc fpit_wait_fastpath_clients {n} {
    wait_for_condition 100 20 {
        [getInfoProperty [r info fastpath] fastpath_clients] == $n
    } else {
        fail "expected $n fast-path clients: [r info fastpath]"
    }
}

proc fpit_expired {} {
    return [getInfoProperty [r info stats] client_idle_timeout_disconnections]
}

start_server {tags {"fastpath idle-timeout external:skip tls:skip"} overrides {io-threads 2 io-batch-hold-us 10000}} {
    assert_equal {io-threads-fast-path yes} [r config get io-threads-fast-path]
    r client setname fp-control
    r select 0 ;# the same db as the raw fast-path clients

    test {Fast path idle timeout: an idle client closes and increments expired_clients} {
        r config set timeout 1
        set base [fpit_expired]
        set a [fpit_client]
        $a ping ;# one command puts it on the fast path
        assert_equal PONG [$a read]
        fpit_wait_fastpath_clients 1

        # Stay idle: the amortized cron closes it once idle strictly longer than the timeout.
        wait_for_condition 100 100 {
            [fpit_expired] == $base + 1
        } else {
            fail "idle disconnection was not counted: [r info stats]"
        }
        fpit_wait_fastpath_clients 0
        catch {$a read} e
        assert_match {*I/O error*} $e
        $a close
        r config set timeout 0
    }

    test {Fast path idle timeout: a periodic PING keeps the client alive} {
        r config set timeout 1
        set base [fpit_expired]
        set a [fpit_client]
        $a ping
        assert_equal PONG [$a read]
        fpit_wait_fastpath_clients 1

        # Interact faster than the timeout: each successful read advances the stamp, so it never expires.
        # Ping the control connection too, so it is not itself idle-reaped while this loop only drives $a.
        for {set i 0} {$i < 6} {incr i} {
            after 300
            r ping
            $a ping
            assert_equal PONG [$a read]
        }
        assert_equal 1 [getInfoProperty [r info fastpath] fastpath_clients]
        assert_equal $base [fpit_expired]
        $a close
        fpit_wait_fastpath_clients 0
        r config set timeout 0
    }

    test {Fast path idle timeout: zero timeout never closes an idle client} {
        r config set timeout 0
        set base [fpit_expired]
        set a [fpit_client]
        $a ping
        assert_equal PONG [$a read]
        fpit_wait_fastpath_clients 1

        # A quiet stretch well past the earlier one-second window: disabled, so nothing closes.
        after 2500
        assert_equal 1 [getInfoProperty [r info fastpath] fastpath_clients]
        assert_equal $base [fpit_expired]
        $a ping ;# still serving normally
        assert_equal PONG [$a read]
        $a close
        fpit_wait_fastpath_clients 0
    }

    test {Idle timeout parity: a normal main-path client still times out} {
        r config set timeout 1
        set base_clients [getInfoProperty [r info clients] connected_clients]
        set b [valkey [srv 0 host] [srv 0 port] 0 $::tls]
        assert_equal OK [$b select 9] ;# selecting a db keeps it on the normal path
        assert_equal [expr {$base_clients + 1}] [getInfoProperty [r info clients] connected_clients]

        # Never touch b during the wait so its timer is not reset; connected_clients returns to base once
        # the normal clientsCron closes it, showing idle-timeout parity with the fast path.
        wait_for_condition 100 100 {
            [getInfoProperty [r info clients] connected_clients] == $base_clients
        } else {
            fail "normal client was not closed on idle timeout: [r info clients]"
        }
        catch {$b close}
        r config set timeout 0
    }

    test {Fast path idle timeout: parity holds after enforcement, a fresh client serves normally} {
        r config set timeout 0
        set a [fpit_client]
        $a ping
        assert_equal PONG [$a read]
        fpit_wait_fastpath_clients 1
        $a set it:after v
        assert_equal OK [$a read]
        $a get it:after
        assert_equal v [$a read]
        $a close
        fpit_wait_fastpath_clients 0
    }
}
