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

proc me_seq {n} {
    set l {}
    for {set i 1} {$i <= $n} {incr i} { lappend l $i }
    return $l
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
        # One MGET tracks every key for this client well before the first one can expire, even on a slow
        # (sanitizer) build; a key that expired before it was read would never be invalidated.
        me_load_volatile r t: 1000 1000
        $rt mget {*}[lmap i [me_seq 1000] {format t:%d $i}]
        me_wait_empty r
        # Every invalidation was queued before this sentinel, so reading up to it cannot block forever.
        r publish __redis__:invalidate me-done
        set keys {}
        while 1 {
            set msg [$rd_redir read]
            if {[lindex $msg 0] ne "message"} continue
            if {[lindex $msg 2] eq "me-done"} break
            foreach k [lindex $msg 2] { lappend keys $k }
        }
        assert_equal 1000 [llength [lsort -unique $keys]]
        $rt client tracking off
        $rt close
        $rd_redir close
    }

    test "Active expire with cron-lanes $lanes removes tracked keys after their client disconnects" {
        r flushdb
        set rt [valkey_client]
        $rt select 0
        $rt client tracking on
        # Active expiry stays off until the client has gone, so every key expires with no tracking client.
        r debug set-active-expire 0
        me_load_volatile r st: 1000 300
        $rt mget {*}[lmap i [me_seq 1000] {format st:%d $i}]
        assert_morethan_equal [s tracking_total_keys] 1000
        $rt close
        wait_for_condition 50 20 {[s tracking_clients] == 0} else { fail "tracking client still connected" }
        r debug set-active-expire 1
        me_wait_empty r
        # With no client left, the tracking table still drops each expired key's entry.
        assert_equal 0 [s tracking_total_keys]
    }

    test "Active expire with cron-lanes $lanes keeps keys and hash fields within one cycle budget" {
        r flushdb
        r config resetstat
        r latency reset
        r config set latency-monitor-threshold 1
        # Enough expiring keys and fields that each alone would use a whole cycle budget.
        me_load_volatile r bk: 600000 400
        r eval {
            for h=1,tonumber(ARGV[1]) do
                for f=1,tonumber(ARGV[2]) do
                    redis.call('hsetex', 'bh:'..h, 'px', 400 + f % 50, 'fields', 1, 'f'..f, 'v')
                end
            end
            return 1
        } 0 400 1000
        wait_for_condition 400 50 {[r dbsize] == 0} else { fail "keys and fields did not expire: [r dbsize]" }
        # One expire-cycle sample per second covers the whole MAINT phase (keys and fields both run in the
        # lanes). Spending the budget twice per tick puts every busy second near 50 ms or more; a loaded
        # host can stretch a single tick, so allow one sample past 45 ms. Slow builds skip it (--no-latency).
        if {!$::no_latency} {
            set samples [r latency history expire-cycle]
            set over [lmap s $samples {expr {[lindex $s 1] > 45 ? [lindex $s 1] : [continue]}}]
            assert_morethan [llength $samples] 1
            assert {[llength $over] <= 1}
        }
        r config set latency-monitor-threshold 0
    }

    test "Active expire with cron-lanes $lanes expires hash fields in the phase, not on main" {
        r flushdb
        r config resetstat
        # A hash whose fields expire but which keeps a non-volatile field, and one that empties.
        r hset keeph f1 1 f2 2 stay 9
        r hpexpire keeph 100 FIELDS 2 f1 f2
        r hset goneh only 1
        r hpexpire goneh 100 FIELDS 1 only
        wait_for_condition 300 50 {[r hlen keeph] == 1 && [r exists goneh] == 0} else {
            fail "fields did not expire: keeph [r hgetall keeph] goneh [r exists goneh]"
        }
        assert_equal {stay 9} [r hgetall keeph]
        if {$lanes eq "yes"} {
            # Field expiry ran in the EXPIRE lane job; main's serial field pass did not run.
            assert_morethan [me_cron_calls r maint_expire] 0
        }
    }

    test "Active expire with cron-lanes $lanes counts every expired field exactly in free-inline mode" {
        r flushdb
        r config resetstat
        # Dense: many hashes across tables, every field expiring in a tight window, no observer so the
        # lanes delete and count fields themselves. A per-field global increment in the lane would race
        # and lose counts; main bulk-adds a per-lane total instead, so expired_fields is exact.
        set loaded 0
        for {set h 0} {$h < 400} {incr h} {
            set args [list hset dh:$h]
            for {set f 0} {$f < 100} {incr f} { lappend args f$f $f }
            lappend args keep 1
            r {*}$args
            set fields {}
            for {set f 0} {$f < 100} {incr f} { lappend fields f$f }
            r hpexpire dh:$h 300 FIELDS 100 {*}$fields
            incr loaded 100
        }
        wait_for_condition 400 50 {[r hlen dh:0] == 1 && [r hlen dh:399] == 1} else {
            fail "dense field expiry did not drain"
        }
        assert_equal $loaded [s expired_fields]
        # The field stale estimate follows the lanes as it follows the serial job.
        assert_morethan [s expired_keys_with_volatile_items_stale_perc] 0
    }

    test "Active expire with cron-lanes $lanes opens no phase per loop iteration while nothing is due" {
        r flushdb
        # Keys and hash fields whose TTLs are an hour away: no phase stops on its budget, so a fast cycle
        # must not open a MAINT phase on every event loop iteration; at hz 10 slow cycles open about 10
        # phases a second, against hundreds with one phase per iteration.
        set hz [lindex [r config get hz] 1]
        r config set hz 10
        me_load_volatile r far: 20000 3600000
        r eval {for h=1,2000 do redis.call('hsetex', 'farh:'..h, 'EX', 3600, 'FIELDS', 2, 'a', 1, 'b', 2) end return 1} 0
        after 300
        set before [me_cron_calls r maint]
        set t0 [clock milliseconds]
        set n 0
        while {[clock milliseconds] - $t0 < 1000} { r ping; incr n }
        set phases [expr {[me_cron_calls r maint] - $before}]
        r config set hz $hz
        assert_morethan $n 200
        assert_lessthan $phases 60
        r flushdb
    }

    test "Active expire with cron-lanes $lanes shrinks keys_with_volatile_items when a hash empties" {
        r flushdb
        r set anchor 1
        for {set i 0} {$i < 200} {incr i} { r hset vh:$i only 1; r hpexpire vh:$i 100 FIELDS 1 only }
        assert_match "*keys_with_volatile_items=200*" [r info keyspace]
        wait_for_condition 300 50 {[r dbsize] == 1} else { fail "hashes did not empty: [r dbsize]" }
        assert_match "*keys_with_volatile_items=0*" [r info keyspace]
    }

    test "Active expire with cron-lanes $lanes sends hexpired per hash and del when it empties" {
        r flushdb
        r config set notify-keyspace-events Exg
        set rd [valkey_deferring_client]
        $rd select 0
        $rd read
        $rd psubscribe __keyevent@0__:hexpired __keyevent@0__:del
        $rd read
        $rd read
        r hset eh a 1 b 2
        r hpexpire eh 120 FIELDS 2 a b
        # Expect one hexpired (fields a,b in one batch) and one del (hash emptied).
        set hexpired 0
        set del 0
        set deadline [expr {[clock milliseconds] + 5000}]
        while {($hexpired == 0 || $del == 0) && [clock milliseconds] < $deadline} {
            set msg [$rd read]
            if {[lindex $msg 0] ne "pmessage"} continue
            if {[lindex $msg 1] eq "__keyevent@0__:hexpired"} { incr hexpired }
            if {[lindex $msg 1] eq "__keyevent@0__:del"} { incr del }
        }
        assert_equal 1 $hexpired
        assert_equal 1 $del
        $rd close
        r config set notify-keyspace-events ""
    }

    test "Active expire with cron-lanes $lanes aborts a transaction watching a hash whose fields expire" {
        r flushdb
        r hset wh a 1 b 2
        r hpexpire wh 150 FIELDS 2 a b
        set rd [valkey_client]
        $rd select 0
        $rd watch wh
        wait_for_condition 300 50 {[r exists wh] == 0} else { fail "watched hash did not empty" }
        $rd multi
        $rd set wh new
        assert_equal {} [$rd exec]
        $rd close
    }

    test "Active expire with cron-lanes $lanes propagates HDEL per hash with a SELECT" {
        r flushall
        r select 0
        r config set lazyfree-lazy-expire no
        set repl [attach_to_replication_stream]
        # 200 hashes, 2 fields each expiring, keep 1 field so the hash survives (HDEL, no DEL).
        for {set i 0} {$i < 200} {incr i} { r hset ph:$i a 1 b 2 keep 9; r hpexpire ph:$i 120 FIELDS 2 a b }
        wait_for_condition 300 50 {[r hlen ph:0] == 1 && [r hlen ph:199] == 1} else { fail "fields did not expire" }
        set hdels {}
        set saw_select 0
        set deadline [expr {[clock milliseconds] + 8000}]
        while {[llength $hdels] < 200 && [clock milliseconds] < $deadline} {
            set cmd [read_from_replication_stream $repl]
            if {$cmd eq ""} continue
            set op [lindex $cmd 0]
            if {$op eq "select"} { set saw_select 1; continue }
            if {$op eq "hdel"} { lappend hdels [lindex $cmd 1] }
        }
        close $repl
        assert_equal 1 $saw_select
        assert_equal 200 [llength [lsort -unique $hdels]]
        r config set lazyfree-lazy-expire no
    }

    test "Active expire with cron-lanes $lanes frees large values off the MAINT phase" {
        r flushdb
        r config resetstat
        # One value whose free takes far longer than an expire cycle's budget.
        r eval {for i=1,tonumber(ARGV[1]) do redis.call('hset', KEYS[1], i, i) end return 1} 1 bigh 1000000
        for {set i 0} {$i < 50} {incr i} { r hset smallh:$i f v }
        r pexpire bigh 100
        for {set i 0} {$i < 50} {incr i} { r pexpire smallh:$i 100 }
        me_wait_empty r
        assert_equal 51 [s expired_keys]
        if {$lanes eq "yes"} {
            # The large value's free is handed off the phase: whichever mode is active (full replay with
            # no observer, or the propagate-only feed once a repl backlog exists), main logs the expensive
            # value and frees it after the barrier, so no phase ran for the length of its free.
            assert_morethan [expr {[me_cron_calls r maint_replay] + [me_cron_calls r maint_feed]}] 0
            regexp {max_usec=(\d+)} [getInfoProperty [r info cronstats] cron_maint] -> maxus
            assert_lessthan $maxus 100000
        }
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
            # Offset parity: the replica consumed exactly the stream the primary produced.
            assert_equal [status $primary master_repl_offset] [status $replica master_repl_offset]
            if {$lanes eq "yes"} {
                # A replica is the only observer, so the lanes fed the deletes instead of replaying them.
                assert_morethan [me_cron_calls $primary maint_feed] 0
                assert_equal 0 [me_cron_calls $primary maint_replay]
            }
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

        test "Active expire with cron-lanes $lanes keeps a replica attached mid-expiry in sync" {
            # Detach, load a large volatile set on the primary alone, then attach mid-expiry so the
            # replica joins by full sync while keys are still expiring. It must converge and match.
            $replica replicaof no one
            $primary flushall
            $primary select 0
            me_load_volatile $primary m: 40000 1500
            $primary eval {for i=1,300 do redis.call('set', 'keepm:'..i, i) end return 1} 0
            after 300
            # Attach while expiry is in flight.
            $replica replicaof $primary_host $primary_port
            wait_for_sync $replica
            me_wait_empty_except $primary 300
            wait_for_ofs_sync $primary $replica
            assert_equal [$primary debug digest] [$replica debug digest]
            assert_equal [status $primary master_repl_offset] [status $replica master_repl_offset]
            assert_equal 300 [$replica dbsize]
        }

        test "Active expire with cron-lanes $lanes replicates hash-field expiry to a replica" {
            $primary flushall
            $primary select 0
            $replica select 0
            # Hashes whose fields expire (some empty, some survive); replica must match field for field.
            for {set i 0} {$i < 2000} {incr i} {
                $primary hset fh:$i a 1 b 2 keep 9
                $primary hpexpire fh:$i 200 FIELDS 2 a b
            }
            for {set i 0} {$i < 2000} {incr i} {
                $primary hset fe:$i only 1
                $primary hpexpire fe:$i 200 FIELDS 1 only
            }
            wait_for_condition 400 50 {
                [$primary hlen fh:0] == 1 && [$primary exists fe:0] == 0 && [$primary exists fe:1999] == 0
            } else { fail "field expiry did not drain on the primary" }
            wait_for_ofs_sync $primary $replica
            assert_equal [$primary debug digest] [$replica debug digest]
            assert_equal [status $primary master_repl_offset] [status $replica master_repl_offset]
            assert_equal 1 [$replica hlen fh:0]
            assert_equal 0 [$replica exists fe:0]
        }

        test "Active expire with cron-lanes $lanes keeps the per-tick stall bounded under dense field expiry with a replica" {
            $primary flushall
            $primary select 0
            $primary config resetstat
            $primary latency reset
            $primary config set latency-monitor-threshold 1
            # Many hashes whose fields all expire at once: a dense burst that main must feed to the replica.
            # Charged per field, no MAINT phase runs past the 25 ms budget; charging a hash as one unit
            # let phases run to about twice the budget. A loaded host can stretch one phase.
            $primary eval {
                for h=0,tonumber(ARGV[1])-1 do
                    local a = {}
                    for f=0,99 do a[#a+1]='f'..f; a[#a+1]=f end
                    a[#a+1]='keep'; a[#a+1]=1
                    redis.call('hset', 'dh:'..h, unpack(a))
                end
                return 1
            } 0 16000
            $primary config resetstat
            $primary eval {
                local a = {}
                for f=0,99 do a[#a+1]='f'..f end
                for h=0,tonumber(ARGV[1])-1 do
                    redis.call('hpexpire', 'dh:'..h, 300, 'FIELDS', 100, unpack(a))
                end
                return 1
            } 0 16000
            wait_for_condition 600 50 {[$primary hlen dh:0] == 1 && [$primary hlen dh:15999] == 1} else {
                fail "dense field expiry did not drain"
            }
            wait_for_ofs_sync $primary $replica
            assert_equal [$primary debug digest] [$replica debug digest]
            # Phases of 32 ms or more: histogram buckets 15 and up (bucket i counts [2^i, 2^(i+1)) us).
            regexp {hist=([0-9/]+)} [getInfoProperty [$primary info cronstats] cron_maint] -> hist
            set long 0
            set i 0
            foreach n [split $hist /] {
                if {$i >= 15} { incr long $n }
                incr i
            }
            # Slow builds skip the timing check (--no-latency).
            if {!$::no_latency} { assert {$long <= 1} }
            $primary config set latency-monitor-threshold 0
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

    test "Active expire with cron-lanes $lanes leaves an AOF with hash-field deletes that reloads consistently" {
        r flushall
        r select 0
        for {set i 0} {$i < 2000} {incr i} { r hset ah:$i a 1 b 2 keep 9; r hpexpire ah:$i 150 FIELDS 2 a b }
        for {set i 0} {$i < 2000} {incr i} { r hset ae:$i only 1; r hpexpire ae:$i 150 FIELDS 1 only }
        # Wait until the whole burst has settled: the 2000 ae:* hashes are gone and only the 2000 ah:*
        # survive, so dbsize is stable before the reload (a partial drain would carry extra keys over).
        wait_for_condition 600 50 {
            [r dbsize] == 2000 && [r hlen ah:0] == 1 && [r hlen ah:1999] == 1
        } else { fail "field expiry did not fully drain: dbsize [r dbsize]" }
        set dbsize [r dbsize]
        r debug loadaof
        r select 0
        # The HDEL/DEL the lanes fed are in the AOF, so the reload keeps the surviving field and the
        # emptied hashes stay gone. (DEBUG DIGEST across an AOF reload differs for field-TTL hashes on
        # the base too, so compare the observable data, not the digest.)
        assert_equal $dbsize [r dbsize]
        assert_equal {keep 9} [r hgetall ah:0]
        assert_equal 0 [r exists ae:0]
    }

    test "Active expire with cron-lanes $lanes feeds propagate-only deletes and the AOF DEL count equals expired keys" {
        r flushall
        r select 0
        r config resetstat
        r config set lazyfree-lazy-expire no
        # AOF on, no replica, and no other observer -> propagation is the only observer.
        set repl [attach_to_replication_stream]
        me_load_volatile r af: 2000 120
        me_wait_empty r
        assert_equal 2000 [s expired_keys]
        set dels {}
        set deadline [expr {[clock milliseconds] + 8000}]
        while {[llength $dels] < 2000 && [clock milliseconds] < $deadline} {
            set cmd [read_from_replication_stream $repl]
            if {$cmd eq ""} continue
            if {[lindex $cmd 0] eq "del"} { lappend dels [lindex $cmd 1] }
        }
        close $repl
        # Exactly one DEL per expired key reached the stream (which the AOF shares framing with).
        assert_equal 2000 [llength [lsort -unique $dels]]
        if {$lanes eq "yes"} {
            # The lanes took the propagate-only path: main fed the deletes, it did not run the per-key replay.
            assert_morethan [me_cron_calls r maint_feed] 0
            assert_equal 0 [me_cron_calls r maint_replay]
        } else {
            assert_equal 0 [me_cron_calls r maint_feed]
        }
    }

    test "Active expire with cron-lanes $lanes falls back to full replay when a keyspace-event subscriber exists" {
        r flushall
        r select 0
        r config resetstat
        r config set notify-keyspace-events Ex
        set rd [valkey_deferring_client]
        $rd select 0
        $rd read
        $rd subscribe __keyevent@0__:expired
        $rd read
        me_load_volatile r ke: 2000 120
        set got {}
        for {set i 0} {$i < 2000} {incr i} { lappend got [lindex [$rd read] 2] }
        assert_equal 2000 [llength [lsort -unique $got]]
        $rd close
        if {$lanes eq "yes"} {
            # An event subscriber forces the full per-key replay, not the propagate-only feed.
            assert_morethan [me_cron_calls r maint_replay] 0
            assert_equal 0 [me_cron_calls r maint_feed]
        }
        r config set notify-keyspace-events ""
    }
}
}
