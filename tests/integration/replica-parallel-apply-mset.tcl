source tests/support/benchmark.tcl

# Replica parallel apply of multi-key writes. A superset primary propagates a split MSET as
# MULTI, one MSET part per lane, EXEC; a stock primary (or a main-thread client) propagates one MSET.
# The replica's classifier must make both lane work: pieces per lane, and a transparent MULTI/EXEC
# when the whole body has lanes. The replica must stay identical to the primary and transactions
# must stay atomic to its readers.

proc lane_stat {r name} {
    return [getInfoProperty [$r info stats] $name]
}

proc assert_in_sync {primary replica} {
    wait_for_ofs_sync $primary $replica
    if {[$primary debug digest] ne [$replica debug digest]} {
        set n 0
        foreach k [$primary keys *] {
            set vp [$primary debug digest-value $k]
            set vr [$replica debug digest-value $k]
            if {$vp ne $vr} {
                puts "DIFF $k primary=[$primary get $k] replica=[$replica get $k] exists=[$replica exists $k]"
                if {[incr n] >= 15} break
            }
        }
        puts "dbsize primary=[$primary dbsize] replica=[$replica dbsize]"
    }
    assert_equal [$primary debug digest] [$replica debug digest]
    assert_equal [$primary dbsize] [$replica dbsize]
}

start_server {tags {"repl external:skip"} overrides {io-threads 4 io-threads-fast-path yes keyspace-shard-bits 8}} {
    start_server {overrides {io-threads 3 io-threads-fast-path yes keyspace-shard-bits 8}} {
        set primary [srv -1 client]
        set primary_host [srv -1 host]
        set primary_port [srv -1 port]
        set replica [srv 0 client]
        # The test default turns on keyspace events, which keep every write serial on both sides.
        $primary config set notify-keyspace-events ""
        $replica config set notify-keyspace-events ""

        $replica replicaof $primary_host $primary_port
        wait_for_sync $replica
        wait_for_ofs_sync $primary $replica

        # A client that never SELECTs stays on the primary's fast path (db 0): its MSETs are split per
        # lane and propagated as MULTI/parts/EXEC. The test clients are on db 9; db 0 is read raw.
        set r0 [valkey [srv 0 host] [srv 0 port] 0 $::tls]

        test {MSET apply: split transactions from the primary run on the lanes} {
            set tx0 [lane_stat $replica repl_lane_tx_transparent]
            set pa0 [lane_stat $replica repl_parallel_applied]
            # The primary splits and wraps an MSET only when its lanes run it, which needs many
            # concurrently readable clients: a Tcl client's pipeline runs as serial segments.
            set cmd [valkeybenchmark $primary_host $primary_port "-c 50 -P 16 -n 30000 -t mset -r 5000 -d 8 -q"]
            exec {*}$cmd
            assert_in_sync $primary $replica
            assert_morethan [expr {[lane_stat $replica repl_lane_tx_transparent] - $tx0}] 10000
            assert_morethan [expr {[lane_stat $replica repl_parallel_applied] - $pa0}] 100000
        }

        test {MSET apply: whole MSETs from a main-thread client are split into lane pieces} {
            set sp0 [lane_stat $replica repl_lane_splits]
            set rd [valkey_deferring_client -1]
            $rd select 9
            $rd read
            for {set i 0} {$i < 3000} {incr i} {
                $rd mset w:a:$i $i w:b:$i $i w:c:$i $i w:d:$i $i w:e:$i $i w:f:$i $i
            }
            for {set i 0} {$i < 3000} {incr i} { $rd read }
            $rd close
            assert_in_sync $primary $replica
            assert_equal [$replica mget w:a:2999 w:f:2999] {2999 2999}
            assert_morethan [expr {[lane_stat $replica repl_lane_splits] - $sp0}] 2000
        }

        test {MSET apply: a transaction with a non-lane command keeps the barrier path} {
            set tx0 [lane_stat $replica repl_lane_tx_transparent]
            set rd [valkey_deferring_client -1]
            for {set i 0} {$i < 300} {incr i} {
                $rd multi
                $rd mset t:a:$i $i t:b:$i $i t:c:$i $i
                $rd setbit t:bit:$i 7 1
                $rd exec
            }
            for {set i 0} {$i < 1200} {incr i} { $rd read }
            $rd close
            assert_in_sync $primary $replica
            assert_equal [$replica mget t:a:299 t:c:299] {299 299}
            assert_equal [$replica getbit t:bit:299 7] 1
            assert_equal [lane_stat $replica repl_lane_tx_transparent] $tx0
        }

        test {MSET apply: split transactions stay atomic to a replica reader} {
            set p0 [valkey $primary_host $primary_port 0 $::tls]
            $p0 mset at:x 0 at:y 0 at:z 0
            wait_for_ofs_sync $primary $replica
            set rd [valkey_deferring_client_by_addr $primary_host $primary_port]
            set rr [valkey [srv 0 host] [srv 0 port] 0 $::tls]
            $rr readonly
            set torn 0
            for {set round 0} {$round < 40} {incr round} {
                for {set i 0} {$i < 200} {incr i} {
                    set v [expr {$round * 200 + $i + 1}]
                    $rd mset at:x $v at:y $v at:z $v ms:[expr {$i % 300}] $v
                }
                for {set i 0} {$i < 50} {incr i} {
                    set got [$rr mget at:x at:y at:z]
                    if {[lindex $got 0] ne [lindex $got 1] || [lindex $got 1] ne [lindex $got 2]} { incr torn }
                }
                for {set i 0} {$i < 200} {incr i} { $rd read }
            }
            $rd close
            $rr close
            $p0 close
            assert_equal $torn 0
            assert_in_sync $primary $replica
            assert_equal [$r0 get at:x] 8000
        }

        test {MSET apply: split transactions are phase-atomic to a speculating replica reader} {
            # The reader above runs on main. A replica has no client write lanes (fpLanesAllowed is off
            # with a primary), so its only off-main read path is speculation: a fast-path client whose
            # read is all GETs is answered on its IO thread in the read phase. That phase and the APPLY
            # phase running the transparent transactions share one barrier, so no GET observes an
            # APPLY phase part-way. Four GETs are four commands and may straddle two read phases with
            # an APPLY between them, so the invariant is monotone: a later GET never reads a value older
            # than an earlier one, and each value is a whole transaction's value (the same v for every
            # key, written together). A reader that could see inside a phase would read a key still at
            # the previous transaction after a sibling already shows the next one, in either order.
            set p0 [valkey $primary_host $primary_port 0 $::tls]
            $p0 mset fa:x 0 fa:y 0 fa:z 0 fa:w 0
            wait_for_ofs_sync $primary $replica
            set rd [valkey_deferring_client_by_addr $primary_host $primary_port]
            set fr [valkey_deferring_client_by_addr [srv 0 host] [srv 0 port]]
            set spec0 [getInfoProperty [$replica info fastpath] fastpath_speculated]
            set backward 0
            set last 0
            for {set round 0} {$round < 40} {incr round} {
                for {set i 0} {$i < 200} {incr i} {
                    set v [expr {$round * 200 + $i + 1}]
                    $rd mset fa:x $v fa:y $v fa:z $v fa:w $v ms:[expr {$i % 300}] $v
                }
                for {set i 0} {$i < 50} {incr i} {
                    $fr get fa:x
                    $fr get fa:y
                    $fr get fa:z
                    $fr get fa:w
                    $fr flush
                    set prev $last
                    foreach got [list [$fr read] [$fr read] [$fr read] [$fr read]] {
                        if {$got < $prev} { incr backward }
                        set prev $got
                    }
                    set last $prev
                }
                for {set i 0} {$i < 200} {incr i} { $rd read }
            }
            $rd close
            $fr close
            $p0 close
            assert_equal $backward 0
            # The reads were answered on the replica's IO thread, not on main.
            set spec [expr {[getInfoProperty [$replica info fastpath] fastpath_speculated] - $spec0}]
            assert_morethan $spec 4000
            assert_in_sync $primary $replica
            assert_equal [$r0 get fa:x] 8000
        }

        test {MSET apply: split transactions stay atomic to a fast-path client's MGET} {
            # One MGET is one command, so one moment. A fast-path client's MGET is not speculated (only
            # pure-GET reads are) and reaches main between phases, never inside an APPLY phase.
            set p0 [valkey $primary_host $primary_port 0 $::tls]
            $p0 mset fm:x 0 fm:y 0 fm:z 0 fm:w 0
            wait_for_ofs_sync $primary $replica
            set rd [valkey_deferring_client_by_addr $primary_host $primary_port]
            set fr [valkey_deferring_client_by_addr [srv 0 host] [srv 0 port]]
            set torn 0
            for {set round 0} {$round < 40} {incr round} {
                for {set i 0} {$i < 200} {incr i} {
                    set v [expr {$round * 200 + $i + 1}]
                    $rd mset fm:x $v fm:y $v fm:z $v fm:w $v ms:[expr {$i % 300}] $v
                }
                for {set i 0} {$i < 50} {incr i} {
                    $fr mget fm:x fm:y fm:z fm:w
                    set got [$fr read]
                    foreach f [lrange $got 1 end] {
                        if {$f ne [lindex $got 0]} { incr torn; break }
                    }
                }
                for {set i 0} {$i < 200} {incr i} { $rd read }
            }
            $rd close
            $fr close
            $p0 close
            assert_equal $torn 0
            assert_in_sync $primary $replica
            assert_equal [$r0 get fm:x] 8000
        }

        test {MSET apply: SETs and MSETs interleaved converge} {
            set rd [valkey_deferring_client -1]
            for {set i 0} {$i < 3000} {incr i} {
                $rd set il:$i a
                $rd mset il:$i b il:[expr {$i + 1}] b il:[expr {$i + 2}] b
                $rd set il:[expr {$i + 1}] c
            }
            for {set i 0} {$i < 9000} {incr i} { $rd read }
            $rd close
            assert_in_sync $primary $replica
            assert_equal [$replica get il:3000] c
            assert_equal [$replica get il:3001] b
        }
    }
}
