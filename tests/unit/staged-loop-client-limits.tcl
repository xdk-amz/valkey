# Client limits with the staged loop running: a main-path client whose replies wait for the socket.

start_server {tags {"staged-loop external:skip tls:skip"} overrides {io-threads 2}} {
    r select 0

    test {Staged: maxmemory-clients evicts a main-path client with unread replies} {
        r eval {return redis.call('set', KEYS[1], string.rep('x', 4194304))} 1 cl:big
        r config set maxmemory-clients 1mb
        set base [getInfoProperty [r info stats] evicted_clients]
        set nc [valkey [srv 0 host] [srv 0 port] 1 $::tls]
        $nc client tracking on ;# keeps it on the main path
        assert_equal OK [$nc read]
        for {set i 0} {$i < 8} {incr i} { $nc get cl:big }
        $nc flush
        # Eviction reads the client's state, which must not wait on a write no phase will run.
        wait_for_condition 100 50 {
            [getInfoProperty [r info stats] evicted_clients] == $base + 1
        } else {
            fail "stuck main-path client was not evicted: [r info stats]"
        }
        $nc close
        r config set maxmemory-clients 0
    }
}
