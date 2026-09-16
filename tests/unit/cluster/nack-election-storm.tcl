# Regression test for https://github.com/madolson/valkey-agents/issues/20.
# Persistent voter-state NACKs must not bypass auth_retry_time and repeatedly
# restart an election that cannot succeed.

proc nack_storm_current_epoch {idx} {
    set info [R $idx CLUSTER INFO]
    if {![regexp {cluster_current_epoch:(\d+)} $info -> epoch]} {
        fail "could not parse cluster_current_epoch from CLUSTER INFO of node $idx"
    }
    return $epoch
}

start_cluster 3 1 {tags {external:skip cluster} overrides {
    cluster-node-timeout 3000
    cluster-ping-interval 100
    cluster-replica-validity-factor 0
    cluster-blacklist-ttl 300
}} {
    set primary0_id [R 0 CLUSTER MYID]
    set replica_id [R 3 CLUSTER MYID]
    set paused_pid [srv 0 pid]
    set auth_retry_time 12000
    set window_ms 10000
    set intended_attempts [expr {int(ceil(double($window_ms) / $auth_retry_time))}]

    test "Cluster is up and node 3 replicates node 0" {
        wait_for_cluster_state ok
        assert_equal "slave" [s -3 role]
        assert_equal $primary0_id [dict get [cluster_get_myself 3] slaveof]
    }

    test "Hold the replica while establishing a persistent NACK condition" {
        R 3 CONFIG SET cluster-replica-no-failover yes
    }

    test "Pause primary 0 and wait for all surviving nodes to mark it FAIL" {
        pause_process $paused_pid
        wait_for_condition 100 100 {
            [cluster_all_see_flag {1 2 3} [list $primary0_id] fail]
        } else {
            fail "not every surviving node marked primary 0 as FAIL"
        }
    }

    test "Voters forget the failed primary while the candidate retains it" {
        R 1 CLUSTER FORGET $primary0_id
        R 2 CLUSTER FORGET $primary0_id
        wait_for_condition 100 100 {
            [dict get [cluster_get_node_by_id 1 $replica_id] slaveof] eq "-" &&
            [dict get [cluster_get_node_by_id 2 $replica_id] slaveof] eq "-"
        } else {
            fail "voters still associate the candidate with a primary"
        }
        assert_equal $primary0_id [dict get [cluster_get_myself 3] slaveof]
    }

    test "Persistent NO_PRIMARY NACKs obey the normal retry cadence" {
        set epoch_before [nack_storm_current_epoch 1]
        set elections_before [count_log_message -3 "Starting a failover election for epoch"]
        set fastfail_before [count_log_message -3 "cannot reach quorum"]
        set denied1_before [count_log_message -1 "Failover auth denied"]
        set denied2_before [count_log_message -2 "Failover auth denied"]

        set t0 [clock milliseconds]
        R 3 CONFIG SET cluster-replica-no-failover no
        after $window_ms
        set elapsed [expr {[clock milliseconds] - $t0}]

        set epoch_delta [expr {[nack_storm_current_epoch 1] - $epoch_before}]
        set elections [expr {[count_log_message -3 "Starting a failover election for epoch"] - $elections_before}]
        set fastfails [expr {[count_log_message -3 "cannot reach quorum"] - $fastfail_before}]
        set denied [expr {
            [count_log_message -1 "Failover auth denied"] - $denied1_before +
            [count_log_message -2 "Failover auth denied"] - $denied2_before
        }]

        puts "=========================================================="
        puts "Issue #20 NACK election cadence measurement"
        puts "  window                              : $elapsed ms"
        puts "  auth_retry_time                     : $auth_retry_time ms"
        puts "  allowed attempts in window          : $intended_attempts"
        puts "  cluster_current_epoch delta         : $epoch_delta"
        puts "  elections                           : $elections"
        puts "  persistent-reason fast-fails        : $fastfails"
        puts "  denied votes                        : $denied"
        puts "=========================================================="

        assert {$elections <= $intended_attempts}
        assert {$epoch_delta <= $intended_attempts}
        assert_equal 0 $fastfails
        assert {$denied >= 1}
        assert_equal "slave" [s -3 role]
    }

    test "At most one normal retry starts after the measurement window" {
        set epoch_before [nack_storm_current_epoch 1]
        after 3000
        set epoch_delta [expr {[nack_storm_current_epoch 1] - $epoch_before}]
        puts "  further 3000 ms epoch delta         : $epoch_delta"
        assert {$epoch_delta <= 1}
    }

    test "Resume the paused primary" {
        resume_process $paused_pid
    }
}
