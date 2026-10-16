# MAINT phase in cluster mode: a node's tables are its slots, so table maintenance fans out to the
# staged threads without keyspace-shard-bits. cron-lanes no runs the same jobs on main.

proc mc_cron_calls {idx job} {
    set v [getInfoProperty [R $idx info cronstats] cron_$job]
    if {[regexp {calls=(\d+)} $v -> n]} { return $n }
    return 0
}

# n keys spread over every slot, each with a TTL a little above ttl_ms.
proc mc_load_volatile {idx prefix n ttl_ms} {
    set rd [valkey_deferring_client [expr {-$idx}]]
    for {set i 0} {$i < $n} {incr i} {
        $rd set $prefix$i $i px [expr {$ttl_ms + $i % 100}]
    }
    for {set i 0} {$i < $n} {incr i} { $rd read }
    $rd close
}

# n keys per tag for ntags hash tags, so ntags slot tables each grow through several rehashes.
proc mc_load_tagged {idx ntags n} {
    set rd [valkey_deferring_client [expr {-$idx}]]
    for {set t 0} {$t < $ntags} {incr t} {
        for {set i 0} {$i < $n} {incr i} { $rd set "{t$t}:$i" $i }
    }
    for {set i 0} {$i < $ntags * $n} {incr i} { $rd read }
    $rd close
}

proc mc_del_tagged {idx ntags from to} {
    set rd [valkey_deferring_client [expr {-$idx}]]
    for {set t 0} {$t < $ntags} {incr t} {
        for {set i $from} {$i < $to} {incr i} { $rd del "{t$t}:$i" }
    }
    for {set i 0} {$i < $ntags * ($to - $from)} {incr i} { $rd read }
    $rd close
}

proc mc_wait_no_rehashing {idx} {
    wait_for_condition 200 50 {
        [getInfoProperty [R $idx info memory] mem_overhead_db_hashtable_rehashing] == 0
    } else {
        fail "rehashing did not finish on node $idx"
    }
}

foreach ::mc_lanes {yes no} {
start_cluster 1 1 [list tags {external:skip cluster maint tls:skip logreqres:skip} overrides [list io-threads 4 save "" enable-debug-command yes activerehashing yes cron-lanes $::mc_lanes]] {

    test "Cluster active expire with cron-lanes $::mc_lanes removes every key once and reaches the replica" {
        wait_for_cluster_state ok
        wait_for_log_messages 0 {"*Staged event loop started*"} 0 100 50
        wait_for_condition 100 50 {[status [Rn 1] master_link_status] eq "up"} else { fail "replica not linked" }
        R 0 config resetstat
        R 1 config resetstat
        mc_load_volatile 0 k: 20000 300
        for {set i 0} {$i < 500} {incr i} { R 0 set keep:$i $i }
        wait_for_condition 300 50 {[R 0 dbsize] == 500} else { fail "keys did not expire: dbsize [R 0 dbsize]" }
        assert_equal 20000 [getInfoProperty [R 0 info stats] expired_keys]
        set lane_calls [mc_cron_calls 0 maint_expire]
        if {$::mc_lanes eq "yes"} { assert_morethan $lane_calls 0 } else { assert_equal 0 $lane_calls }
        wait_for_ofs_sync [Rn 0] [Rn 1]
        assert_equal 500 [R 1 dbsize]
        assert_equal [R 0 debug digest] [R 1 debug digest]
        assert_equal 0 [getInfoProperty [R 1 info stats] expired_keys]
    }

    test "Cluster active expire with cron-lanes $::mc_lanes sends one expired event per key" {
        R 0 flushall
        wait_for_ofs_sync [Rn 0] [Rn 1]
        R 0 config set notify-keyspace-events Ex
        set rd [valkey_deferring_client 0]
        $rd psubscribe __keyevent@0__:expired
        $rd read
        mc_load_volatile 0 ev: 2000 150
        set got {}
        for {set i 0} {$i < 2000} {incr i} {
            lappend got [lindex [$rd read] 3]
        }
        assert_equal 2000 [llength [lsort -unique $got]]
        $rd close
        R 0 config set notify-keyspace-events ""
    }

    test "Cluster active expire with cron-lanes $::mc_lanes aborts a transaction watching the key" {
        R 0 flushall
        wait_for_ofs_sync [Rn 0] [Rn 1]
        R 0 set w:1 v px 200
        set rd [valkey_client 0]
        $rd watch w:1
        wait_for_condition 300 50 {[R 0 dbsize] == 0} else { fail "watched key did not expire" }
        $rd multi
        $rd set w:1 new
        assert_equal {} [$rd exec]
        $rd close
    }

    test "Cluster rehash and shrink with cron-lanes $::mc_lanes keep slot tables consistent" {
        R 0 flushall
        wait_for_ofs_sync [Rn 0] [Rn 1]
        R 0 config resetstat
        mc_load_tagged 0 64 3000
        assert_equal 192000 [R 0 dbsize]
        mc_wait_no_rehashing 0
        mc_del_tagged 0 64 0 2900
        assert_equal 6400 [R 0 dbsize]
        mc_wait_no_rehashing 0
        assert_equal 100 [R 0 cluster countkeysinslot [R 0 cluster keyslot "{t7}:x"]]
        assert_match "*keys=6400*" [R 0 info keyspace]
        if {$::mc_lanes eq "yes"} { assert_morethan [mc_cron_calls 0 maint] 0 } else { assert_equal 0 [mc_cron_calls 0 maint] }
        wait_for_ofs_sync [Rn 0] [Rn 1]
        assert_equal [R 0 debug digest] [R 1 debug digest]
    }
}
}
