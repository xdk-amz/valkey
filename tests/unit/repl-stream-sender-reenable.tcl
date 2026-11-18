# repl-stream-sender: turning the sender off and on again must re-adopt every replica
# that is already ONLINE. The only production call that hands a replica to the sender
# used to be replicaPutOnline, so after CONFIG SET repl-stream-sender no, then on again,
# an existing replica stayed on the main path for its lifetime (audit finding REV1-3).
# The sender now walks server.replicas when it starts and adopts every eligible one.
#
# The assertion that fails without the fix is repl_sender_replicas == 1 after the
# re-enable; repl_sender_bytes must then grow for new writes, which shows the sender
# really sends to that replica, and the digests must match (no gap, no double send).

proc rsr_sender_replicas {r} {
    return [getInfoProperty [$r info replication] repl_sender_replicas]
}
proc rsr_sender_bytes {r} {
    return [getInfoProperty [$r info replication] repl_sender_bytes]
}

# One round: off, writes flow on the main path; on, the sender owns the replica again
# and its byte counter moves for new writes. $prefix keeps the keys of each round apart.
proc rsr_toggle_round {master replica mode prefix} {
    $master config set repl-stream-sender no
    wait_for_condition 50 100 {
        [rsr_sender_replicas $master] == 0
    } else {
        fail "sender did not hand the replica back on disable"
    }
    # Main path: writes still reach the replica (F9 hand-back).
    for {set i 0} {$i < 1000} {incr i} { $master set ${prefix}off$i v }
    wait_for_ofs_sync $master $replica
    assert_equal [$master debug digest] [$replica debug digest]

    $master config set repl-stream-sender $mode
    # Without the fix the sender starts but owns nothing: this times out.
    wait_for_condition 50 100 {
        [rsr_sender_replicas $master] == 1
    } else {
        fail "sender did not re-adopt the ONLINE replica after re-enable"
    }
    set b1 [rsr_sender_bytes $master]
    for {set i 0} {$i < 2000} {incr i} { $master set ${prefix}on$i v }
    wait_for_ofs_sync $master $replica
    # The sender, not main, carried the new writes.
    assert {[rsr_sender_bytes $master] > $b1}
    assert_equal [status $master master_repl_offset] [status $replica slave_repl_offset]
    assert_equal [$master debug digest] [$replica debug digest]
}

# plain: io-threads 2 and the ae loop. lanes: the staged loop with lanes forced, so the
# re-adopt also runs from the staged iteration's tick.
foreach {variant primary_overrides} {
    plain {io-threads 2 enable-debug-command yes}
    lanes {io-threads 5 keyspace-shard-bits 10 io-threads-parallel-writes-min 1 enable-debug-command yes}
} {
foreach mode {iteration append} {
start_server [list tags {"repl external:skip"} overrides $primary_overrides] {
    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]
    $master config set repl-stream-sender $mode
    $master config set repl-diskless-sync no

    start_server {overrides {io-threads 2 enable-debug-command yes}} {
        set replica [srv 0 client]

        test "repl-stream-sender $mode $variant: re-enable adopts the ONLINE replica again" {
            $replica replicaof $master_host $master_port
            wait_for_sync $replica
            wait_replica_online $master
            wait_for_condition 50 100 {
                [rsr_sender_replicas $master] == 1
            } else {
                fail "sender did not take ownership of the new replica"
            }
            # Toggle twice: the second round starts from a replica that was already
            # re-adopted once, so a one-shot re-adopt would be caught too.
            rsr_toggle_round $master $replica $mode r1
            rsr_toggle_round $master $replica $mode r2
        }

        test "repl-stream-sender $mode $variant: re-enable while the replica has pending main-path data" {
            $master config set repl-stream-sender no
            wait_for_condition 50 100 {
                [rsr_sender_replicas $master] == 0
            } else {
                fail "sender did not hand the replica back on disable"
            }
            # One pipeline: many writes, the re-enable, more writes. The config change is
            # applied while the replica still has unsent main-path bytes, so the adopt must
            # settle main's write first and seed the sender from the cursor: no byte may be
            # skipped or sent twice.
            set pipe {}
            for {set i 0} {$i < 3000} {incr i} { append pipe "set busy_a$i [string repeat x 100]\r\n" }
            append pipe "config set repl-stream-sender $mode\r\n"
            for {set i 0} {$i < 3000} {incr i} { append pipe "set busy_b$i [string repeat y 100]\r\n" }
            $master write $pipe
            $master flush
            for {set i 0} {$i < 6001} {incr i} { $master read }
            wait_for_condition 50 100 {
                [rsr_sender_replicas $master] == 1
            } else {
                fail "sender did not re-adopt the replica after a re-enable under load"
            }
            set b1 [rsr_sender_bytes $master]
            for {set i 0} {$i < 1000} {incr i} { $master set busy_c$i v }
            wait_for_ofs_sync $master $replica
            assert {[rsr_sender_bytes $master] > $b1}
            assert_equal [status $master master_repl_offset] [status $replica slave_repl_offset]
            assert_equal [$master debug digest] [$replica debug digest]
        }
    }
}
}
}
