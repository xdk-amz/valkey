source tests/support/benchmark.tcl

# Chained replication through a replica that lane-applies multi-key writes. The replica's classifier
# runs an MSET as per-lane pieces and a lane-only MULTI/EXEC as a transparent transaction, but it must
# forward the primary's stream to its own sub-replicas byte for byte (rpAdvance proxies the applied
# bytes, not what the lanes ran). So a sub-replica sees the same MULTI/parts/EXEC the replica saw, a
# stock sub-replica can apply it, and every level ends identical.

proc chain_stat {r name} {
    return [getInfoProperty [$r info stats] $name]
}

proc assert_chain_in_sync {primary replica sub} {
    wait_for_ofs_sync $primary $replica
    wait_for_ofs_sync $replica $sub
    foreach {name r} [list replica $replica sub-replica $sub] {
        if {[$primary debug digest] ne [$r debug digest]} {
            set n 0
            foreach k [$primary keys *] {
                if {[$primary debug digest-value $k] ne [$r debug digest-value $k]} {
                    puts "DIFF $name $k primary=[$primary get $k] $name=[$r get $k] exists=[$r exists $k]"
                    if {[incr n] >= 15} break
                }
            }
            puts "dbsize primary=[$primary dbsize] $name=[$r dbsize]"
        }
        assert_equal [$primary debug digest] [$r debug digest]
        assert_equal [$primary dbsize] [$r dbsize]
    }
}

start_server {tags {"repl external:skip"} overrides {io-threads 4 io-threads-fast-path yes keyspace-shard-bits 8}} {
    start_server {overrides {io-threads 3 io-threads-fast-path yes keyspace-shard-bits 8}} {
        start_server {overrides {io-threads 3 io-threads-fast-path yes keyspace-shard-bits 8}} {
            set primary [srv -2 client]
            set primary_host [srv -2 host]
            set primary_port [srv -2 port]
            set replica [srv -1 client]
            set replica_host [srv -1 host]
            set replica_port [srv -1 port]
            set sub [srv 0 client]
            foreach r [list $primary $replica $sub] {
                # The test default turns on keyspace events, which keep every write serial.
                $r config set notify-keyspace-events ""
            }

            $replica replicaof $primary_host $primary_port
            wait_for_sync $replica
            $sub replicaof $replica_host $replica_port
            wait_for_sync $sub
            assert_chain_in_sync $primary $replica $sub

            test {MSET chain: split transactions are forwarded as the primary sent them} {
                # The replica makes the MULTI/parts/EXEC transparent. If it forwarded what its lanes ran
                # (pieces, no wrap) the sub-replica would see plain MSETs and classify none as a
                # transaction; it must see the same stream and make the same transactions transparent.
                set rtx0 [chain_stat $replica repl_lane_tx_transparent]
                set stx0 [chain_stat $sub repl_lane_tx_transparent]
                set spa0 [chain_stat $sub repl_parallel_applied]
                set cmd [valkeybenchmark $primary_host $primary_port "-c 50 -P 16 -n 30000 -t mset -r 5000 -d 8 -q"]
                exec {*}$cmd
                assert_chain_in_sync $primary $replica $sub
                set rtx [expr {[chain_stat $replica repl_lane_tx_transparent] - $rtx0}]
                set stx [expr {[chain_stat $sub repl_lane_tx_transparent] - $stx0}]
                assert_morethan $rtx 10000
                assert_morethan $stx 10000
                assert_morethan [expr {[chain_stat $sub repl_parallel_applied] - $spa0}] 100000
                # Both levels see the same transactions; small reads fall to main at either level, so
                # the counts need not be equal, only of the same order.
                assert_morethan $stx [expr {$rtx / 2}]
            }

            test {MSET chain: whole MSETs are split into lane pieces at both levels} {
                set rsp0 [chain_stat $replica repl_lane_splits]
                set ssp0 [chain_stat $sub repl_lane_splits]
                set rd [valkey_deferring_client -2]
                $rd select 9
                $rd read
                for {set i 0} {$i < 3000} {incr i} {
                    $rd mset w:a:$i $i w:b:$i $i w:c:$i $i w:d:$i $i w:e:$i $i w:f:$i $i
                }
                for {set i 0} {$i < 3000} {incr i} { $rd read }
                $rd close
                assert_chain_in_sync $primary $replica $sub
                assert_equal [$sub mget w:a:2999 w:f:2999] {2999 2999}
                assert_morethan [expr {[chain_stat $replica repl_lane_splits] - $rsp0}] 2000
                assert_morethan [expr {[chain_stat $sub repl_lane_splits] - $ssp0}] 2000
            }

            test {MSET chain: a transaction with a non-lane command stays a barrier at both levels} {
                set rtx0 [chain_stat $replica repl_lane_tx_transparent]
                set stx0 [chain_stat $sub repl_lane_tx_transparent]
                set rd [valkey_deferring_client -2]
                for {set i 0} {$i < 300} {incr i} {
                    $rd multi
                    $rd mset t:a:$i $i t:b:$i $i t:c:$i $i
                    $rd setbit t:bit:$i 7 1
                    $rd exec
                }
                for {set i 0} {$i < 1200} {incr i} { $rd read }
                $rd close
                assert_chain_in_sync $primary $replica $sub
                assert_equal [$sub mget t:a:299 t:c:299] {299 299}
                assert_equal [$sub getbit t:bit:299 7] 1
                assert_equal [chain_stat $replica repl_lane_tx_transparent] $rtx0
                assert_equal [chain_stat $sub repl_lane_tx_transparent] $stx0
            }

            test {MSET chain: split transactions stay atomic to a sub-replica fast-path MGET} {
                set p0 [valkey $primary_host $primary_port 0 $::tls]
                $p0 mset fm:x 0 fm:y 0 fm:z 0 fm:w 0
                wait_for_ofs_sync $primary $replica
                wait_for_ofs_sync $replica $sub
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
                assert_chain_in_sync $primary $replica $sub
                set s0 [valkey [srv 0 host] [srv 0 port] 0 $::tls]
                assert_equal [$s0 get fm:x] 8000
                $s0 close
            }

            test {MSET chain: a stock sub-replica applies the forwarded stream} {
                # With parallel apply off the sub-replica runs the stream the stock way: the MULTI,
                # parts and EXEC the replica forwarded must be a valid transaction to it.
                $sub config set replica-parallel-apply no
                set spa0 [chain_stat $sub repl_parallel_applied]
                set rtx0 [chain_stat $replica repl_lane_tx_transparent]
                set cmd [valkeybenchmark $primary_host $primary_port "-c 50 -P 16 -n 30000 -t mset -r 5000 -d 8 -q"]
                exec {*}$cmd
                assert_chain_in_sync $primary $replica $sub
                # The replica saw (and forwarded) wrapped transactions; the sub-replica ran them on main.
                assert_morethan [expr {[chain_stat $replica repl_lane_tx_transparent] - $rtx0}] 10000
                assert_equal [chain_stat $sub repl_parallel_applied] $spa0
                $sub config set replica-parallel-apply yes
            }

            test {MSET chain: a sub-replica partially resyncing mid-stream converges} {
                # Drop the chain's lower link while split transactions flow. The sub-replica reconnects
                # with the replica's replid and resumes from the replica's backlog, which holds the
                # primary's bytes, so the resumed stream is valid where it restarts.
                set pok0 [chain_stat $replica sync_partial_ok]
                set cmd [valkeybenchmark $primary_host $primary_port "-c 50 -P 16 -n 60000 -t mset -r 5000 -d 8 -q"]
                set bench [open "|[join $cmd]" r]
                after 100
                $replica client kill type replica
                read $bench
                close $bench
                wait_for_condition 100 50 {
                    [chain_stat $replica sync_partial_ok] > $pok0
                } else {
                    fail "sub-replica did not partially resync"
                }
                wait_for_sync $sub
                assert_chain_in_sync $primary $replica $sub
            }
        }
    }
}
