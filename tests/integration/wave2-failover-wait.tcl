# Wave 2, worker w4: replication guarantees seen from fast-path clients.
#   WAIT / WAITAOF after lane writes, split MSET, a write behind a barrier and serial writes (CB-16,
#   TXS-26, TXS-27, PER-06, CLU-16); FAILOVER under pipelined INCR from fast-path clients (PLAT-09,
#   REPL-21); a replica with replica-parallel-apply while io-threads is resized (PLAT-02); SIGTERM
#   and SHUTDOWN under fast-path load (PLAT-08, REPL-02).
# Fast-path clients are raw db-0 sockets without a client name. Every test asserts the fast path,
# the lanes or the parallel apply moved with an INFO counter. Synchronisation uses SIGSTOP/SIGCONT
# of the replica, offsets, log lines and wait_for_condition; no sleeps.

proc w4_resp {args} {
    set cmd "*[llength $args]\r\n"
    foreach a $args { append cmd "$[string length $a]\r\n$a\r\n" }
    return $cmd
}

proc w4_read_reply {fd} {
    set line [gets $fd]
    if {[eof $fd]} { return {EOF} }
    switch -- [string index $line 0] {
        {$} {
            if {$line eq {$-1}} { return {(nil)} }
            return [gets $fd]
        }
        {*} {
            set n [string range $line 1 end]
            if {$n < 0} { return {(nil)} }
            set out {}
            for {set i 0} {$i < $n} {incr i} { lappend out [w4_read_reply $fd] }
            return $out
        }
        {_} { return {(nil)} }
        default { return $line }
    }
}

proc w4_read_replies {fd n} {
    set out {}
    for {set i 0} {$i < $n} {incr i} { lappend out [w4_read_reply $fd] }
    return $out
}

proc w4_fp {c field} {
    return [getInfoProperty [$c info fastpath] $field]
}

# A raw socket admitted to the fast path of the server behind $c (its PING is answered, then the
# server counts one more fast-path client).
proc w4_fast_socket {c host port} {
    set before [w4_fp $c fastpath_clients]
    set fd [socket $host $port]
    fconfigure $fd -translation {crlf binary} -blocking 1 -buffering full
    puts -nonewline $fd [w4_resp ping]
    flush $fd
    assert_equal {+PONG} [w4_read_reply $fd]
    wait_for_condition 200 20 {
        [w4_fp $c fastpath_clients] > $before
    } else {
        fail "the raw socket was not admitted to the fast path: [$c info fastpath]"
    }
    return $fd
}

# Close a fast-path socket and wait until the server no longer counts it, so the next
# w4_fast_socket sees a stable baseline.
proc w4_close {c fd} {
    set before [w4_fp $c fastpath_clients]
    close $fd
    wait_for_condition 200 20 {
        [w4_fp $c fastpath_clients] < $before
    } else {
        fail "the server still counts the closed fast-path socket: [$c info fastpath]"
    }
}

proc w4_send {fd buf} {
    puts -nonewline $fd $buf
    flush $fd
}

# A pipeline of lane writes over many shards, replies checked, on an open fast socket.
proc w4_lane_burst {fd prefix n} {
    set buf ""
    for {set i 0} {$i < $n} {incr i} { append buf [w4_resp set $prefix:k$i v$i] }
    w4_send $fd $buf
    set got [w4_read_replies $fd $n]
    assert_equal [lrepeat $n {+OK}] $got
}

# WAIT on the SAME fast socket that did the writes (WAIT counts the caller's own write offset).
proc w4_wait {fd numreplicas timeout} {
    w4_send $fd [w4_resp wait $numreplicas $timeout]
    set r [w4_read_reply $fd]
    assert_match {:*} $r
    return [string range $r 1 end]
}

proc w4_delta {c field before} {
    return [expr {[w4_fp $c $field] - $before}]
}

# SHUTDOWN NOW in the same pipeline as lane writes, replica running (no wait): every SET the
# primary acknowledged must reach the replica. With repl-stream-sender on this is F10 (REPL-02):
# finishShutdown stops the sender and the final flush skips replicas it still marks as owned.
foreach sender {no yes} {
    start_server {tags {"wave2 repl shutdown external:skip tls:skip"} overrides {io-threads 4}} {
        start_server [list overrides [list io-threads 5 keyspace-shard-bits 10 io-threads-parallel-writes-min 1 repl-diskless-sync no repl-stream-sender $sender]] {
            set primary [srv 0 client]
            set primary_host [srv 0 host]
            set primary_port [srv 0 port]
            set replica [srv -1 client]
            $primary client setname w4-control
            $replica client setname w4-control
            $primary select 0
            $replica select 0
            # The harness default.conf turns keyspace notifications on, which keeps writes off the lanes.
            $primary config set notify-keyspace-events ""
            $replica replicaof $primary_host $primary_port
            wait_for_sync $replica
            wait_for_ofs_sync $primary $replica

            test "SHUTDOWN NOW in a pipeline after fast-path SETs keeps every acked SET on the replica, repl-stream-sender $sender" {
                set n 500
                set fd [w4_fast_socket $primary $primary_host $primary_port]
                assert_equal [expr {$sender eq "yes" ? 1 : 0}] [getInfoProperty [$primary info replication] repl_sender_replicas]
                # Engagement: the same pipeline shape without SHUTDOWN uses the lanes.
                set lanes [w4_fp $primary fastpath_write_lane_entries]
                w4_lane_burst $fd w4:warm $n
                set lane_delta [w4_delta $primary fastpath_write_lane_entries $lanes]
                assert_morethan $lane_delta 0
                wait_for_ofs_sync $primary $replica
                set buf ""
                for {set i 0} {$i < $n} {incr i} { append buf [w4_resp set w4:now:$i [string repeat v 100]] }
                append buf [w4_resp shutdown now]
                w4_send $fd $buf
                set acked {}
                for {set i 0} {$i < $n} {incr i} {
                    if {[w4_read_reply $fd] ne {+OK}} break
                    lappend acked $i
                }
                catch {close $fd}
                wait_for_condition 200 50 { ![is_alive [srv 0 pid]] } else { fail "primary still alive" }
                wait_for_condition 200 50 {
                    [status $replica master_link_status] eq "down"
                } else { fail "replica link still up" }
                set missing 0
                foreach i $acked {
                    if {[$replica exists w4:now:$i] != 1} { incr missing }
                }
                puts "    repl-stream-sender $sender, lane entries +$lane_delta, acked [llength $acked] of $n, missing on replica $missing"
                assert_morethan [llength $acked] 0
                assert_equal 0 $missing
            }
        }
    }
}
