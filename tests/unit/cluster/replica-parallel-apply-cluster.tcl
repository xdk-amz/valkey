# Replica parallel apply in cluster mode: a cluster replica applies eligible replicated SETs on the
# staged threads, lanes by slot, and stays identical to its primary, per slot as well as in total.

proc rpac_parallel_applied {idx} {
    return [getInfoProperty [[srv $idx client] info stats] repl_parallel_applied]
}

proc rpac_in_sync {primary replica} {
    wait_for_ofs_sync $primary $replica
    assert_equal [$primary debug digest] [$replica debug digest]
    assert_equal [$primary dbsize] [$replica dbsize]
}

start_cluster 1 1 {tags {external:skip cluster repl staged-loop} overrides {io-threads 4 cluster-slot-stats-enabled no}} {
    set primary [srv 0 client]
    set replica [srv -1 client]
    # The test default turns on keyspace events, which keep every write serial.
    $replica config set notify-keyspace-events ""

    test {Cluster replica parallel apply: pipelined SETs over many slots converge in APPLY phases} {
        wait_for_cluster_state ok
        wait_for_ofs_sync $primary $replica
        set before [rpac_parallel_applied -1]
        set rd [valkey_deferring_client 0]
        for {set i 0} {$i < 20000} {incr i} {
            $rd set key:[expr {$i % 3000}] v$i
        }
        for {set i 0} {$i < 20000} {incr i} { $rd read }
        $rd close
        rpac_in_sync $primary $replica
        assert_morethan [expr {[rpac_parallel_applied -1] - $before}] 0
    }

    test {Cluster replica parallel apply: per-slot key counts and TTLs match the primary} {
        set rd [valkey_deferring_client 0]
        for {set i 0} {$i < 3000} {incr i} {
            $rd set "{t$i}a" a$i px 100000
            $rd incr ctr
            $rd set "{t$i}b" b$i
            $rd del "{t[expr {$i - 1}]}a"
        }
        for {set i 0} {$i < 12000} {incr i} { $rd read }
        $rd close
        rpac_in_sync $primary $replica
        foreach k [list "{t1}b" "{t2999}a" "{t500}b"] {
            set slot [$primary cluster keyslot $k]
            assert_equal [$primary cluster countkeysinslot $slot] [$replica cluster countkeysinslot $slot]
        }
        $replica readonly
        assert_equal b1 [$replica get "{t1}b"]
        assert_range [$replica pttl "{t2999}a"] 1 100000
        assert_equal -1 [$replica ttl "{t1}b"]
    }
}
