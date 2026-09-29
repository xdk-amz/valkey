# maxmemory-clients enforcement for IO-owned fast-path clients. A fast-path client is accounted into the
# CLIENT_TYPE_NORMAL memory total through the main-only limit registry (attach-time base allocation plus
# its outstanding reply bytes), so client eviction sees and evicts it exactly like a normal client, while
# only its IO owner ever closes the transport.

# A client that sends nothing before its first command stays on the fast path (a SELECT would move it to
# the main path).
proc fpmm_client {} {
    return [valkey [srv 0 host] [srv 0 port] 1 $::tls]
}

proc fpmm_wait_fastpath_clients {n} {
    wait_for_condition 100 20 {
        [getInfoProperty [r info fastpath] fastpath_clients] == $n
    } else {
        fail "expected $n fast-path clients: [r info fastpath]"
    }
}

proc fpmm_evicted {} {
    return [getInfoProperty [r info stats] evicted_clients]
}

start_server {tags {"fastpath maxmemory-clients external:skip tls:skip"} overrides {io-threads 2 io-batch-hold-us 10000}} {
    assert_equal {io-threads-fast-path yes} [r config get io-threads-fast-path]
    r client setname fpmm-control
    r select 0 ;# the same db as the raw fast-path clients

    test {maxmemory-clients evicts a stuck large-reply fast-path client and keeps a healthy one} {
        r eval {return redis.call('set', KEYS[1], string.rep('x', 4194304))} 1 mmc:big
        r config set maxmemory-clients 1mb
        set base [fpmm_evicted]

        # A healthy small client that reads its replies stays well under the limit.
        set healthy [fpmm_client]
        fpmm_wait_fastpath_clients 1
        $healthy set mmc:s v
        assert_equal OK [$healthy read]

        set stuck [fpmm_client]
        fpmm_wait_fastpath_clients 2

        # Multiple unread replies exceed transport buffering, leaving retained output visible to eviction.
        for {set i 0} {$i < 8} {incr i} {
            $stuck get mmc:big
        }
        $stuck flush

        wait_for_condition 200 50 {
            [fpmm_evicted] == $base + 1
        } else {
            fail "stuck fast-path client was not evicted: [r info stats] [r info fastpath]"
        }
        fpmm_wait_fastpath_clients 1
        $stuck close

        # The healthy client is untouched and still serves on the fast path.
        $healthy ping
        assert_equal PONG [$healthy read]
        $healthy close
        fpmm_wait_fastpath_clients 0
        r config set maxmemory-clients 0
    }

    test {maxmemory-clients enable and disable with active fast-path clients does not crash} {
        set a [fpmm_client]
        fpmm_wait_fastpath_clients 1
        $a set mmc:e v
        assert_equal OK [$a read]

        # Toggle the limit at runtime while a fast-path client is owned; the config apply skips the
        # IO-owned client and rebuckets or clears the registry synchronously without asserting.
        r config set maxmemory-clients 4mb
        $a get mmc:e
        assert_equal v [$a read]
        r config set maxmemory-clients 0
        $a get mmc:e
        assert_equal v [$a read]
        r config set maxmemory-clients 2mb
        $a ping
        assert_equal PONG [$a read]
        r config set maxmemory-clients 0

        assert_equal 1 [getInfoProperty [r info fastpath] fastpath_clients]
        $a close
        fpmm_wait_fastpath_clients 0
    }

    test {maxmemory-clients parity: a stuck normal-path client is still evicted} {
        r eval {return redis.call('set', KEYS[1], string.rep('x', 4194304))} 1 mmc:np
        r config set maxmemory-clients 1mb
        set base [fpmm_evicted]

        set nc [fpmm_client]
        fpmm_wait_fastpath_clients 1
        $nc select 0 ;# SELECT moves it off the fast path onto the normal main path
        assert_equal OK [$nc read]
        fpmm_wait_fastpath_clients 0

        for {set i 0} {$i < 8} {incr i} {
            $nc get mmc:np
        }
        $nc flush
        wait_for_condition 200 50 {
            [fpmm_evicted] == $base + 1
        } else {
            fail "stuck normal-path client was not evicted: [r info stats]"
        }
        $nc close
        r config set maxmemory-clients 0
    }
}
