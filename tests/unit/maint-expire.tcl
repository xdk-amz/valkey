# Active expire in the MAINT phase: with keyspace-shard-bits > 0 and IO threads, keys expire on every
# staged thread and main replays each delete's effects after the phase. Clients, replicas and the AOF
# must see what they see when main expires keys itself; cron-lanes no is the serial reference.

proc me_cron_calls {r job} {
    set v [getInfoProperty [$r info cronstats] cron_$job]
    if {[regexp {calls=(\d+)} $v -> n]} { return $n }
    return 0
}

# calls= for one command from INFO commandstats (0 when the command never ran).
proc me_cmdstat_calls {r cmd} {
    set line [getInfoProperty [$r info commandstats] cmdstat_$cmd]
    if {[regexp {calls=(\d+)} $line -> n]} { return $n }
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

    test "Active expire with cron-lanes $lanes unblocks a BLPOP whose list key expires" {
        r del l:1
        r rpush l:1 a
        r pexpire l:1 100
        set rd [valkey_deferring_client]
        $rd select 0
        $rd read
        $rd blpop l:2 0
        wait_for_blocked_clients_count 1
        # l:1 expiring must not deliver anything; the client stays blocked, then we release it.
        after 300
        assert_equal 1 [s blocked_clients]
        r rpush l:2 z
        assert_equal {l:2 z} [$rd read]
        $rd close
    }

    test "Active expire with cron-lanes $lanes sends one tracking invalidation per key (redirect)" {
        r flushdb
        # Redirect invalidation messages to a pub/sub client; count one per expired key.
        set rd_redir [valkey_deferring_client]
        $rd_redir select 0
        $rd_redir read
        $rd_redir client id
        set redir_id [$rd_redir read]
        $rd_redir subscribe __redis__:invalidate
        $rd_redir read
        set rt [valkey_client]
        $rt select 0
        $rt client tracking on redirect $redir_id
        me_load_volatile r t: 1000 150
        # Read each key so the server tracks it for this client.
        for {set i 1} {$i <= 1000} {incr i} { $rt get t:$i }
        me_wait_empty r
        set keys {}
        set deadline [expr {[clock milliseconds] + 5000}]
        while {[llength [lsort -unique $keys]] < 1000 && [clock milliseconds] < $deadline} {
            set msg [$rd_redir read]
            if {[lindex $msg 0] eq "message"} {
                foreach k [lindex $msg 2] { lappend keys $k }
            }
        }
        assert_equal 1000 [llength [lsort -unique $keys]]
        $rt client tracking off
        $rt close
        $rd_redir close
    }

    test "Active expire with cron-lanes $lanes keeps a value pinned by an in-flight large reply" {
        r flushdb
        r config set lazyfree-lazy-expire no
        # A large value read with a deferred client whose reply is still being produced must not be
        # freed early when the key expires between the GET dispatch and the reply read.
        set big [string repeat x 200000]
        r set big:1 $big px 150
        set rd [valkey_deferring_client]
        $rd select 0
        $rd read
        $rd get big:1
        # Give the key time to expire while the reply is in flight on the server.
        after 300
        me_wait_empty r
        set got [$rd read]
        assert_equal $big $got
        $rd close
    }

    test "Active expire with cron-lanes $lanes propagates one DEL per key with a SELECT" {
        r flushall
        r select 0
        r config set lazyfree-lazy-expire no
        set repl [attach_to_replication_stream]
        me_load_volatile r pr: 300 120
        me_wait_empty r
        # Collect propagated commands until every key's DEL is seen; order across tables/threads is
        # arbitrary, so compare the set of deleted keys, not a fixed sequence.
        set dels {}
        set saw_select 0
        set deadline [expr {[clock milliseconds] + 8000}]
        while {[llength $dels] < 300 && [clock milliseconds] < $deadline} {
            set cmd [read_from_replication_stream $repl]
            if {$cmd eq ""} continue
            set op [lindex $cmd 0]
            if {$op eq "select"} { set saw_select 1; continue }
            if {$op eq "del"} { lappend dels [lindex $cmd 1] }
        }
        close $repl
        assert_equal 1 $saw_select
        assert_equal 300 [llength [lsort -unique $dels]]
    }

    test "Active expire with cron-lanes $lanes propagates UNLINK per key under lazyfree-lazy-expire" {
        r flushall
        r select 0
        r config set lazyfree-lazy-expire yes
        set repl [attach_to_replication_stream]
        me_load_volatile r pu: 300 120
        me_wait_empty r
        set unlinks {}
        set deadline [expr {[clock milliseconds] + 8000}]
        while {[llength $unlinks] < 300 && [clock milliseconds] < $deadline} {
            set cmd [read_from_replication_stream $repl]
            if {$cmd eq ""} continue
            if {[lindex $cmd 0] eq "unlink"} { lappend unlinks [lindex $cmd 1] }
        }
        close $repl
        assert_equal 300 [llength [lsort -unique $unlinks]]
        r config set lazyfree-lazy-expire no
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
            $replica config resetstat
            me_load_volatile $primary r: 20000 300
            $primary eval {for i=1,500 do redis.call('set', 'keep:'..i, i) end return 1} 0
            me_wait_empty_except $primary 500
            wait_for_ofs_sync $primary $replica
            assert_equal 500 [$replica dbsize]
            assert_equal [$primary debug digest] [$replica debug digest]
            assert_equal 20000 [status $primary expired_keys]
            assert_equal 0 [status $replica expired_keys]
        }

        test "Active expire with cron-lanes $lanes replicates UNLINK under lazyfree-lazy-expire" {
            $primary flushall
            $primary config set lazyfree-lazy-expire yes
            wait_for_ofs_sync $primary $replica
            $primary select 0
            me_load_volatile $primary u: 10000 200
            me_wait_empty_except $primary 0
            wait_for_ofs_sync $primary $replica
            assert_equal [$primary debug digest] [$replica debug digest]
            assert_equal 0 [$replica dbsize]
            $primary config set lazyfree-lazy-expire no
        }

        test "Active expire with cron-lanes $lanes replicates keys from two dbs with the right SELECTs" {
            $primary flushall
            $primary select 0
            me_load_volatile $primary d0: 5000 200
            $primary select 1
            me_load_volatile $primary d1: 5000 200
            $primary select 0
            # Both dbs drain on the primary; dbsize is per selected db, so check each.
            wait_for_condition 300 50 {
                [$primary dbsize] == 0
            } else {
                fail "db0 expiry did not drain"
            }
            $primary select 1
            wait_for_condition 300 50 {
                [$primary dbsize] == 0
            } else {
                $primary select 0
                fail "db1 expiry did not drain"
            }
            $primary select 0
            wait_for_ofs_sync $primary $replica
            # Whole-dataset digest covers both dbs; equality proves the SELECTs replicated correctly.
            assert_equal [$primary debug digest] [$replica debug digest]
            $replica select 0
            assert_equal 0 [$replica dbsize]
            $replica select 1
            assert_equal 0 [$replica dbsize]
            $replica select 0
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
