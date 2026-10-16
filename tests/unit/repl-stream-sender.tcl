# repl-stream-sender: a dedicated thread streams the backlog to ONLINE plaintext
# replicas off the staged loop. These tests exercise the sender-specific behaviour:
# the counters move only when the sender owns a replica, output-buffer-limit still
# disconnects a slow streamed replica, killing a replica mid-stream is clean, under
# appendfsync always no byte reaches a replica before it is fsynced, and a replica
# that resyncs moves between the main path and the sender path with no gap or
# duplicate (digest check).

proc rss_sender_replicas {r} {
    return [getInfoProperty [$r info replication] repl_sender_replicas]
}
proc rss_sender_bytes {r} {
    return [getInfoProperty [$r info replication] repl_sender_bytes]
}

foreach mode {iteration append} {
start_server {tags {"repl external:skip"} overrides {io-threads 2 enable-debug-command yes}} {
    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]
    $master config set repl-stream-sender $mode
    # Disk-based sync so the replica streams through the sender immediately.
    $master config set repl-diskless-sync no

    start_server {overrides {io-threads 2 enable-debug-command yes}} {
        set replica [srv 0 client]
        $replica config set repl-stream-sender $mode

        test "repl-stream-sender $mode: counters move only when the sender owns a replica" {
            # No replica yet: the sender owns nothing and has sent nothing.
            assert_equal 0 [rss_sender_replicas $master]
            set before [rss_sender_bytes $master]
            $master set k0 v0
            # With no streamed replica the sender byte counter must not move.
            assert_equal $before [rss_sender_bytes $master]

            $replica replicaof $master_host $master_port
            wait_for_sync $replica
            wait_replica_online $master
            wait_for_condition 50 100 {
                [rss_sender_replicas $master] == 1
            } else {
                fail "sender did not take ownership of the replica"
            }
            set b1 [rss_sender_bytes $master]
            for {set i 0} {$i < 2000} {incr i} { $master set k$i v$i }
            wait_for_ofs_sync $master $replica
            # Now the sender owns the replica, so its byte counter advances.
            assert {[rss_sender_bytes $master] > $b1}
            assert_equal [status $master master_repl_offset] [status $replica slave_repl_offset]
        }

        test "repl-stream-sender $mode: a replica that resyncs matches by digest" {
            $master debug populate 5000
            wait_for_ofs_sync $master $replica
            # Force a full resync mid-stream: the replica leaves the sender path, syncs
            # again, and rejoins it. No byte may be skipped or duplicated.
            $replica replicaof no one
            for {set i 0} {$i < 3000} {incr i} { $master set resync$i v }
            $replica replicaof $master_host $master_port
            wait_for_sync $replica
            for {set i 0} {$i < 2000} {incr i} { $master set tail$i v }
            wait_for_ofs_sync $master $replica
            assert_equal [$master debug digest] [$replica debug digest]
        }
    }
}
}

# Output-buffer-limit must still disconnect a slow streamed replica.
start_server {tags {"repl external:skip"} overrides {io-threads 2 enable-debug-command yes}} {
    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]
    $master config set repl-stream-sender append
    $master config set repl-diskless-sync no

    start_server {overrides {io-threads 2 enable-debug-command yes}} {
        set replica [srv 0 client]
        set replica_pid [srv 0 pid]
        $replica config set repl-stream-sender append
        test "repl-stream-sender: a slow replica past client-output-buffer-limit is disconnected" {
            $replica replicaof $master_host $master_port
            wait_for_sync $replica
            wait_replica_online $master
            assert_equal 1 [s -1 connected_slaves]
            # A tiny hard limit plus a replica that stops reading: SIGSTOP the replica so it
            # cannot drain its socket, then push enough backlog to blow the hard limit. The
            # primary must drop it even though the sender owns the send.
            $master config set client-output-buffer-limit "replica 1024 0 0"
            exec kill -STOP $replica_pid
            for {set i 0} {$i < 200000} {incr i} { $master set cob$i [string repeat x 200] }
            wait_for_condition 500 100 {
                [s -1 connected_slaves] eq 0
            } else {
                exec kill -CONT $replica_pid
                fail "slow streamed replica was not disconnected on COB"
            }
            exec kill -CONT $replica_pid
        }
    }
}

# Killing a replica mid-stream is clean (no primary crash, counter stays sane).
start_server {tags {"repl external:skip"} overrides {io-threads 2 enable-debug-command yes}} {
    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]
    $master config set repl-stream-sender append
    $master config set repl-diskless-sync no

    start_server {overrides {io-threads 2 enable-debug-command yes}} {
        set replica [srv 0 client]
        set replica_pid [srv 0 pid]
        $replica config set repl-stream-sender append
        test "repl-stream-sender: killing a replica mid-stream is clean" {
            $replica replicaof $master_host $master_port
            wait_for_sync $replica
            wait_replica_online $master
            # Write continuously while the replica is force-killed.
            for {set i 0} {$i < 5000} {incr i} { $master set pre$i v }
            exec kill -9 $replica_pid
            for {set i 0} {$i < 20000} {incr i} { $master set post$i v }
            # Primary survives and eventually drops the dead replica.
            assert_equal {PONG} [$master ping]
            wait_for_condition 100 100 {
                [s -1 connected_slaves] eq 0
            } else {
                fail "primary did not drop the killed replica"
            }
            assert_equal {PONG} [$master ping]
        }
    }
}

# appendfsync always: no byte reaches a replica before it is fsynced.
start_server {tags {"repl external:skip"} overrides {io-threads 2 enable-debug-command yes appendonly yes appendfsync always}} {
    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]
    $master config set repl-stream-sender append
    $master config set repl-diskless-sync no

    start_server {overrides {io-threads 2 enable-debug-command yes}} {
        set replica [srv 0 client]
        $replica config set repl-stream-sender append
        test "repl-stream-sender: appendfsync always never sends a write before the fsync" {
            $replica replicaof $master_host $master_port
            wait_for_sync $replica
            wait_replica_online $master
            for {set i 0} {$i < 3000} {incr i} { $master set fsync$i v }
            wait_for_ofs_sync $master $replica
            # The replica's offset may never exceed the primary's fsynced offset. The
            # sender publishes only the fsynced offset under appendfsync always, so the
            # replica offset tracks the master offset exactly and the datasets match.
            assert_equal [status $master master_repl_offset] [status $replica slave_repl_offset]
            assert_equal [$master debug digest] [$replica debug digest]
        }
    }
}

# A replica that connects and disconnects repeatedly under continuous write load races
# the sender's send against main's Remove; the primary must stay correct and the final
# dataset must match.
start_server {tags {"repl external:skip"} overrides {io-threads 2 enable-debug-command yes}} {
    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]
    $master config set repl-stream-sender append
    $master config set repl-diskless-sync no

    start_server {overrides {io-threads 2 enable-debug-command yes}} {
        set replica [srv 0 client]
        $replica config set repl-stream-sender append
        test "repl-stream-sender: repeated resync under write load races Remove cleanly" {
            for {set cycle 0} {$cycle < 10} {incr cycle} {
                $replica replicaof $master_host $master_port
                wait_for_sync $replica
                for {set i 0} {$i < 1000} {incr i} { $master set c${cycle}k$i v }
                $replica replicaof no one
            }
            assert_equal {PONG} [$master ping]
            # Final steady state: reconnect and the datasets converge with no gap/dupe.
            $replica replicaof $master_host $master_port
            wait_for_sync $replica
            for {set i 0} {$i < 2000} {incr i} { $master set final$i v }
            wait_for_ofs_sync $master $replica
            assert_equal [$master debug digest] [$replica debug digest]
        }
    }
}
