# Active expire in the MAINT phase: with keyspace-shard-bits > 0 and IO threads, keys expire on every
# staged thread and main replays each delete's effects after the phase. Clients, replicas and the AOF
# must see what they see when main expires keys itself; cron-lanes no is the serial reference.

proc me_cron_calls {r job} {
    set v [getInfoProperty [$r info cronstats] cron_$job]
    if {[regexp {calls=(\d+)} $v -> n]} { return $n }
    return 0
}

proc me_load_volatile {r prefix n ttl_ms} {
    $r eval {for i=1,tonumber(ARGV[1]) do redis.call('set', KEYS[1]..i, i, 'px', tonumber(ARGV[2]) + (i % 100)) end return 1} 1 $prefix $n $ttl_ms
}

# DBSIZE counts keys still in the tables, so waiting on it never expires a key lazily.
proc me_wait_empty_except {r n} {
    wait_for_condition 300 50 {
        [$r dbsize] == $n
    } else {
        fail "keys did not expire: dbsize [$r dbsize], want $n"
    }
}

proc me_wait_empty {r} {
    me_wait_empty_except $r 0
}

set me_overrides {
    io-threads 4
    io-threads-fast-path yes
    keyspace-shard-bits 6
    save ""
    enable-debug-command yes
}

foreach lanes {yes no} {
start_server [list tags {"maint expire external:skip tls:skip"} overrides [concat $me_overrides [list cron-lanes $lanes]]] {
    r select 0

    test "Active expire with cron-lanes $lanes removes every key once and counts it" {
        r config resetstat
        me_load_volatile r k: 20000 100
        me_wait_empty r
        assert_equal 20000 [s expired_keys]
        set lane_calls [me_cron_calls r maint_expire]
        if {$lanes eq "yes"} { assert_morethan $lane_calls 0 } else { assert_equal 0 $lane_calls }
    }

    test "Active expire with cron-lanes $lanes sends one expired event per key" {
        r config set notify-keyspace-events Ex
        set rd [valkey_deferring_client]
        $rd select 0
        $rd read
        $rd subscribe __keyevent@0__:expired
        $rd read
        me_load_volatile r ev: 2000 100
        set got {}
        for {set i 0} {$i < 2000} {incr i} {
            lappend got [lindex [$rd read] 2]
        }
        assert_equal 2000 [llength [lsort -unique $got]]
        $rd close
        r config set notify-keyspace-events ""
    }

    test "Active expire with cron-lanes $lanes aborts a transaction watching the key" {
        r flushdb
        r set w:1 v px 200
        set rd [valkey_client]
        $rd select 0
        $rd watch w:1
        me_wait_empty r
        $rd multi
        $rd set w:1 new
        assert_equal {} [$rd exec]
        $rd close
    }

    test "Active expire with cron-lanes $lanes unblocks a stream reader whose key expires" {
        r del s:1
        r xgroup create s:1 g $ MKSTREAM
        set rd [valkey_deferring_client]
        $rd select 0
        $rd read
        $rd xreadgroup GROUP g c BLOCK 0 STREAMS s:1 >
        wait_for_blocked_clients_count 1
        r pexpire s:1 100
        assert_error "*NOGROUP*" {$rd read}
        $rd close
    }
}
}

foreach lanes {yes no} {
start_server [list tags {"maint expire repl external:skip tls:skip"} overrides [concat $me_overrides [list cron-lanes $lanes]]] {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]
    start_server [list overrides [concat $me_overrides [list cron-lanes $lanes]]] {
        set replica [srv 0 client]

        test "Active expire with cron-lanes $lanes reaches the replica as one delete per key" {
            $replica replicaof $primary_host $primary_port
            wait_for_sync $replica
            $primary select 0
            $replica select 0
            $primary config resetstat
            me_load_volatile $primary r: 20000 300
            $primary eval {for i=1,500 do redis.call('set', 'keep:'..i, i) end return 1} 0
            me_wait_empty_except $primary 500
            wait_for_ofs_sync $primary $replica
            assert_equal 500 [$replica dbsize]
            assert_equal [$primary debug digest] [$replica debug digest]
            assert_equal 20000 [status $primary expired_keys]
            assert_equal 0 [status $replica expired_keys]
        }
    }
}
}

foreach lanes {yes no} {
start_server [list tags {"maint expire aof external:skip tls:skip"} overrides [concat $me_overrides [list cron-lanes $lanes appendonly yes appendfsync everysec]]] {
    r select 0
    test "Active expire with cron-lanes $lanes leaves an AOF that reloads to the same dataset" {
        me_load_volatile r a: 20000 200
        r eval {for i=1,300 do redis.call('set', 'stay:'..i, i) end return 1} 0
        me_wait_empty_except r 300
        set digest [r debug digest]
        r debug loadaof
        r select 0
        assert_equal 300 [r dbsize]
        assert_equal $digest [r debug digest]
    }
}
}
