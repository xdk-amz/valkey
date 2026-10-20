start_server {tags {"lazyfree"}} {
    test "UNLINK can reclaim memory in background" {
        # make sure the previous test is really done before sampling used_memory
        wait_lazyfree_done r

        set orig_mem [s used_memory]
        set args {}
        for {set i 0} {$i < 100000} {incr i} {
            lappend args $i
        }
        r sadd myset {*}$args
        assert {[r scard myset] == 100000}
        set peak_mem [s used_memory]
        assert {[r unlink myset] == 1}
        assert {$peak_mem > $orig_mem+1000000}
        wait_for_condition 50 100 {
            [s used_memory] < $peak_mem &&
            [s used_memory] < $orig_mem*2
        } else {
            fail "Memory is not reclaimed by UNLINK"
        }
    }

    test "FLUSHDB ASYNC can reclaim memory in background" {
        # make sure the previous test is really done before sampling used_memory
        wait_lazyfree_done r

        set orig_mem [s used_memory]
        set args {}
        for {set i 0} {$i < 100000} {incr i} {
            lappend args $i
        }
        r sadd myset {*}$args
        assert {[r scard myset] == 100000}
        set peak_mem [s used_memory]
        r flushdb async
        assert {$peak_mem > $orig_mem+1000000}
        wait_for_condition 50 100 {
            [s used_memory] < $peak_mem &&
            [s used_memory] < $orig_mem*2
        } else {
            fail "Memory is not reclaimed by FLUSHDB ASYNC"
        }
    }

    test "lazy free a stream with all types of metadata" {
        # make the previous test is really done before doing RESETSTAT
        wait_for_condition 50 100 {
            [s lazyfree_pending_objects] == 0
        } else {
            fail "lazyfree isn't done"
        }

        r config resetstat
        r config set stream-node-max-entries 5
        for {set j 0} {$j < 1000} {incr j} {
            if {rand() < 0.9} {
                r xadd stream * foo $j
            } else {
                r xadd stream * bar $j
            }
        }
        r xgroup create stream mygroup 0
        set records [r xreadgroup GROUP mygroup Alice COUNT 2 STREAMS stream >]
        r xdel stream [lindex [lindex [lindex [lindex $records 0] 1] 1] 0]
        r xack stream mygroup [lindex [lindex [lindex [lindex $records 0] 1] 0] 0]
        r unlink stream

        # make sure it was lazy freed
        wait_for_condition 50 100 {
            [s lazyfree_pending_objects] == 0
        } else {
            fail "lazyfree isn't done"
        }
        assert_equal [s lazyfreed_objects] 1
    } {} {needs:config-resetstat}

    test "lazy free a stream with deleted cgroup" {
        r config resetstat
        r xadd s * a b
        r xgroup create s bla $
        r xgroup destroy s bla
        r unlink s

        # make sure it was not lazy freed
        wait_for_condition 50 100 {
            [s lazyfree_pending_objects] == 0
        } else {
            fail "lazyfree isn't done"
        }
        assert_equal [s lazyfreed_objects] 0
    } {} {needs:config-resetstat}
}

# Queues one lazy free job behind the ones already queued and waits until
# lazyfreed_objects reaches 'expected'. The BIO thread runs jobs in order, so
# every job queued before the fence has run once it returns.
proc lazyfree_fence {r expected} {
    set members {}
    for {set i 0} {$i < 200} {incr i} {lappend members m$i}
    $r sadd lazyfree-fence {*}$members
    $r unlink lazyfree-fence
    wait_for_condition 50 100 {
        [status $r lazyfreed_objects] >= $expected
    } else {
        fail "lazyfree fence isn't done"
    }
}

proc get_calls {r} {
    set calls 0
    regexp {calls=([0-9]+)} [cmdrstat get $r] _ calls
    return $calls
}

# Pipelines 'n' GETs of 'key' on 'rd' without reading them, waits until the
# server ran them, and checks that some of the replies are still unwritten.
proc lazyfree_fill_reader {r rd id key n total_calls} {
    for {set i 0} {$i < $n} {incr i} {$rd get $key}
    $rd flush
    wait_for_condition 100 100 {
        [get_calls $r] >= $total_calls
    } else {
        fail "GETs were not processed"
    }
    assert_morethan [lindex [regexp -inline {omem=([0-9]+)} [$r client list id $id]] 1] 0
}

proc lazyfree_client_id {rd} {
    $rd client id
    return [$rd read]
}

proc lazyfree_read_replies {rd n val} {
    for {set i 0} {$i < $n} {incr i} {
        if {[$rd read] ne $val} {fail "wrong reply $i"}
    }
}

proc lazyfree_setup_keyspace {r val} {
    wait_lazyfree_done $r
    $r flushall sync
    $r config resetstat
    for {set i 0} {$i < 10} {incr i} {$r set filler$i v}
    $r set big $val
}

foreach {flush} {FLUSHALL FLUSHDB} {
    start_server {tags {"lazyfree external:skip"}} {
        test "$flush ASYNC waits for unwritten replies that reference the old keyspace" {
            set val [string repeat x 262144]
            lazyfree_setup_keyspace r $val
            set rd [valkey_deferring_client]
            set id [lazyfree_client_id $rd]
            lazyfree_fill_reader r $rd $id big 200 200

            r $flush async
            lazyfree_fence r 1
            assert_equal 1 [s lazyfreed_objects]
            assert_equal 11 [s lazyfree_pending_objects]

            lazyfree_read_replies $rd 200 $val
            $rd ping
            assert_equal PONG [$rd read]
            $rd close
            wait_lazyfree_done r
            assert_equal 12 [s lazyfreed_objects]
        } {} {needs:config-resetstat}
    }
}

start_server {tags {"lazyfree external:skip"}} {
    test "FLUSHALL ASYNC frees the old keyspace when a client holding its replies is killed" {
        set val [string repeat x 262144]
        lazyfree_setup_keyspace r $val
        set rd [valkey_deferring_client]
        set id [lazyfree_client_id $rd]
        lazyfree_fill_reader r $rd $id big 200 200

        r flushall async
        lazyfree_fence r 1
        assert_equal 11 [s lazyfree_pending_objects]

        assert_equal 1 [r client kill id $id]
        $rd close
        wait_lazyfree_done r
        assert_equal 12 [s lazyfreed_objects]
    } {} {needs:config-resetstat}

    test "Two async flushes waiting on one client are each freed once" {
        set val [string repeat x 262144]
        lazyfree_setup_keyspace r $val
        r select 10
        for {set i 0} {$i < 4} {incr i} {r set filler$i v}
        r set big $val
        r select 9

        set rd [valkey_deferring_client]
        set id [lazyfree_client_id $rd]
        $rd select 9
        for {set i 0} {$i < 100} {incr i} {$rd get big}
        $rd select 10
        lazyfree_fill_reader r $rd $id big 100 200

        r flushdb async
        r select 10
        r flushdb async
        r select 9
        lazyfree_fence r 1
        assert_equal 1 [s lazyfreed_objects]
        assert_equal 16 [s lazyfree_pending_objects]

        assert_equal OK [$rd read]
        lazyfree_read_replies $rd 100 $val
        assert_equal OK [$rd read]
        lazyfree_read_replies $rd 100 $val
        $rd ping
        assert_equal PONG [$rd read]
        $rd close
        wait_lazyfree_done r
        assert_equal 17 [s lazyfreed_objects]
    } {} {needs:config-resetstat cluster:skip}

    test "Replies added after an async flush do not delay its free" {
        set old [string repeat x 20480]
        set new [string repeat y 20480]
        lazyfree_setup_keyspace r $old
        set rd1 [valkey_deferring_client]
        set rd2 [valkey_deferring_client]
        set id1 [lazyfree_client_id $rd1]
        set id2 [lazyfree_client_id $rd2]
        lazyfree_fill_reader r $rd1 $id1 big 2000 2000

        r flushall async
        r set big $new
        lazyfree_fill_reader r $rd1 $id1 big 3000 5000
        lazyfree_fill_reader r $rd2 $id2 big 3000 8000
        lazyfree_fence r 1
        assert_equal 1 [s lazyfreed_objects]
        assert_equal 11 [s lazyfree_pending_objects]

        # rd1's later replies and all of rd2's reference the new keyspace.
        lazyfree_read_replies $rd1 2000 $old
        set read 0
        while {$read < 3000 && [s lazyfree_pending_objects] != 0} {
            lazyfree_read_replies $rd1 100 $new
            incr read 100
        }
        assert_lessthan $read 3000
        wait_lazyfree_done r
        assert_equal 12 [s lazyfreed_objects]

        lazyfree_read_replies $rd1 [expr {3000 - $read}] $new
        lazyfree_read_replies $rd2 3000 $new
        $rd1 close
        $rd2 close
        assert_equal 12 [s lazyfreed_objects]
    } {} {needs:config-resetstat}

    test "Async flush with no unwritten replies is freed right away" {
        set val [string repeat x 262144]
        lazyfree_setup_keyspace r $val
        assert_equal $val [r get big]
        r flushall async
        lazyfree_fence r 12
        assert_equal 12 [s lazyfreed_objects]
        assert_equal 0 [s lazyfree_pending_objects]
    } {} {needs:config-resetstat}
}

start_server {tags {"lazyfree external:skip"} overrides {io-threads 4 io-threads-always-active yes}} {
    test "FLUSHALL ASYNC waits for unwritten replies with IO threads" {
        set val [string repeat x 262144]
        lazyfree_setup_keyspace r $val
        set rd [valkey_deferring_client]
        set id [lazyfree_client_id $rd]
        lazyfree_fill_reader r $rd $id big 200 200

        r flushall async
        lazyfree_fence r 1
        assert_equal 1 [s lazyfreed_objects]
        assert_equal 11 [s lazyfree_pending_objects]

        lazyfree_read_replies $rd 200 $val
        assert_morethan [s io_threaded_writes_processed] 0
        $rd ping
        assert_equal PONG [$rd read]
        $rd close
        wait_lazyfree_done r
        assert_equal 12 [s lazyfreed_objects]
    } {} {needs:config-resetstat}
}
