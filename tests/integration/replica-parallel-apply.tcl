# Replica parallel apply: with keyspace-shard-bits set on the replica, eligible replicated SETs
# are applied on the staged threads by shard. The replica must stay identical to the primary, and
# any feature that observes individual writes must see them.

proc parallel_applied {r} {
    return [getInfoProperty [$r info stats] repl_parallel_applied]
}

proc assert_in_sync {primary replica} {
    wait_for_ofs_sync $primary $replica
    assert_equal [$primary debug digest] [$replica debug digest]
    assert_equal [$primary dbsize] [$replica dbsize]
}

start_server {tags {"repl external:skip"} overrides {io-threads 4 io-threads-fast-path yes}} {
    start_server {overrides {io-threads 4 io-threads-fast-path yes keyspace-shard-bits 8}} {
        set primary [srv -1 client]
        set primary_host [srv -1 host]
        set primary_port [srv -1 port]
        set replica [srv 0 client]
        # The test default turns on keyspace events, which keep every write serial.
        $replica config set notify-keyspace-events ""

        $replica replicaof $primary_host $primary_port
        wait_for_sync $replica
        wait_for_ofs_sync $primary $replica

        test {Parallel apply: pipelined SETs converge and run in APPLY phases} {
            set before [parallel_applied $replica]
            set rd [valkey_deferring_client -1]
            for {set i 0} {$i < 20000} {incr i} {
                $rd set key:[expr {$i % 3000}] v$i
            }
            for {set i 0} {$i < 20000} {incr i} { $rd read }
            $rd close
            assert_in_sync $primary $replica
            assert_morethan [expr {[parallel_applied $replica] - $before}] 0
        }

        test {Parallel apply: TTLs, serial commands and other dbs keep stream order} {
            set rd [valkey_deferring_client -1]
            for {set i 0} {$i < 3000} {incr i} {
                $rd set k:$i a$i px 100000
                $rd incr ctr
                $rd set k:$i b$i
                $rd select [expr {$i % 3}]
                $rd set k:$i c$i ex 1000
                $rd del k:[expr {$i - 1}]
                $rd select 9
            }
            for {set i 0} {$i < 21000} {incr i} { $rd read }
            $rd close
            assert_in_sync $primary $replica
            $replica select 1
            assert_equal [$replica get k:1] c1
            assert_range [$replica ttl k:1] 900 1000
            $replica select 9
            assert_equal [$replica get k:2999] b2999
            assert_equal [$replica ttl k:2999] -1
        }

        test {Parallel apply: MULTI/EXEC from the primary stays atomic} {
            set rd [valkey_deferring_client -1]
            for {set i 0} {$i < 500} {incr i} {
                $rd set m:$i x
                $rd multi
                $rd set m:$i y
                $rd set n:$i y
                $rd exec
                $rd set n:$i z
            }
            for {set i 0} {$i < 3000} {incr i} { $rd read }
            $rd close
            assert_in_sync $primary $replica
            assert_equal [$replica get m:499] y
            assert_equal [$replica get n:499] z
        }

        test {Parallel apply: WATCH on the replica sees a replicated SET} {
            set rr [valkey_client]
            $rr readonly
            $rr watch w:1
            $primary set w:1 changed
            wait_for_ofs_sync $primary $replica
            $rr multi
            $rr get w:1
            assert_equal [$rr exec] {}
            $rr close
        }

        test {Parallel apply: MONITOR on the replica sees every replicated SET} {
            set before [parallel_applied $replica]
            set mon [valkey_deferring_client]
            $mon monitor
            assert_equal [$mon read] OK
            $primary set mon:1 a
            $primary set mon:2 b
            wait_for_ofs_sync $primary $replica
            assert_match {*"set"*"mon:1"*} [$mon read]
            assert_match {*"set"*"mon:2"*} [$mon read]
            $mon close
            assert_equal [parallel_applied $replica] $before
        }

        test {Parallel apply: keyspace notifications on the replica fire for replicated SETs} {
            $replica config set notify-keyspace-events KEA
            set sub [valkey_deferring_client]
            $sub psubscribe __keyspace@9__:note
            $sub read
            $primary set note v
            assert_equal [$sub read] {pmessage __keyspace@9__:note __keyspace@9__:note set}
            $sub close
            $replica config set notify-keyspace-events ""
            assert_in_sync $primary $replica
            set before [parallel_applied $replica]
            $primary set note w
            assert_in_sync $primary $replica
            assert_equal [parallel_applied $replica] [expr {$before + 1}]
        }

        test {Parallel apply: off by config} {
            $replica config set replica-parallel-apply no
            set before [parallel_applied $replica]
            for {set i 0} {$i < 100} {incr i} { $primary set off:$i v }
            assert_in_sync $primary $replica
            assert_equal [parallel_applied $replica] $before
            $replica config set replica-parallel-apply yes
        }

        test {Parallel apply: sharded replica survives a full resync and SCAN sees every key} {
            $replica replicaof no one
            $primary set post-resync 1
            $replica replicaof $primary_host $primary_port
            wait_for_sync $replica
            assert_in_sync $primary $replica
            $replica select 9
            set cursor 0
            set keys {}
            while 1 {
                set res [$replica scan $cursor count 500]
                set cursor [lindex $res 0]
                lappend keys {*}[lindex $res 1]
                if {$cursor == 0} break
            }
            assert_equal [llength [lsort -unique $keys]] [$replica dbsize]
        }
    }
}
