# The replica drain in finishShutdown must not re-offload. It waits for the ONE replica write an
# IO thread owns when shutdown starts and accounts it (so the final flush includes its bytes, the
# N3 guarantee), then does a single best-effort flush. Without the guard, accounting a completed
# write hands the rest of the replica's backlog back to an IO thread and the loop repeats until the
# whole backlog is on the wire, so SHUTDOWN blocks for as long as a slow live replica takes.
#
# Deterministic setup. The replica is a raw socket (tests/helpers/w4_fake_replica.tcl) that reads
# nothing until a trigger file appears, then drains SLOWLY (a small read, then a short sleep),
# modelling a slow live replica. Steps:
#  1. Build a ~5 MB backlog with NO write delay while the replica is PAUSED, so the kernel socket
#     buffers overflow and a real backlog is left pending in the replica output buffer (omem). We
#     build it first, with no delay, because the delay (next step) stops the main thread from
#     reading the burst connection while that connection's own reply write is in flight, which would
#     throttle how much of the burst is applied.
#  2. Arm DEBUG IO-WRITE-DONE-DELAY (one second per IO write report) and release the replica. The
#     slow drain keeps the socket mostly full, so the primary always has one offloaded replica write
#     in flight (it writes a chunk, waits out the one-second report delay, re-offloads) with the
#     multi-MB backlog still pending behind it.
#  3. Confirm a write is in flight, then run SHUTDOWN NOW on a SECOND, clean connection - never the
#     burst connection, whose own delayed reply write would keep the main thread from reading a
#     SHUTDOWN on it until long after the window closed.
# finishShutdown then finds one in-flight write with the backlog pending behind it:
#   fixed:  accounts the one in-flight write (if one is in flight at that instant), does one
#           best-effort flush, drops the rest -> accounted 0 or 1.
#   broken: re-offloads the pending backlog and keeps accounting write after write -> accounted 2+.
# The "Accounted N in-flight replica write(s)" log line carries N, so the mutation shows as N >= 2.

proc w4sb_resp {args} {
    set cmd "*[llength $args]\r\n"
    foreach a $args { append cmd "$[string length $a]\r\n$a\r\n" }
    return $cmd
}

proc w4sb_read_line {fd} {
    set line [gets $fd]
    if {[eof $fd]} { return {EOF} }
    return $line
}

proc w4sb_socket {host port} {
    set fd [socket $host $port]
    fconfigure $fd -translation {crlf binary} -blocking 1 -buffering full
    puts -nonewline $fd [w4sb_resp ping]
    flush $fd
    assert_equal {+PONG} [w4sb_read_line $fd]
    return $fd
}

# n pipelined SETs of size bytes; replies are not read (see the header note).
proc w4sb_burst {fd prefix n size} {
    set v [string repeat v $size]
    set buf ""
    for {set i 0} {$i < $n} {incr i} { append buf [w4sb_resp set $prefix:$i $v] }
    puts -nonewline $fd $buf
    flush $fd
}

start_server {tags {"wave2 repl shutdown external:skip tls:skip"} overrides {io-threads 5 io-threads-fast-path no repl-diskless-sync no shutdown-timeout 10 save ""}} {
    set primary [srv 0 client]
    set host [srv 0 host]
    set port [srv 0 port]
    set dir [lindex [$primary config get dir] 1]
    $primary client setname w4-control
    $primary config set repl-ping-replica-period 3600

    test {finishShutdown accounts the one in-flight replica write and does not re-offload the backlog} {
        set trigger [file join $dir w4_fake_replica.go]
        set out [file join $dir w4_fake_replica.out]
        set ready [file join $dir w4_fake_replica.ready]
        file delete -force $trigger $out $ready
        set tclsh [info nameofexecutable]
        exec $tclsh tests/helpers/w4_fake_replica.tcl $host $port $trigger $out $ready &
        wait_for_condition 500 20 { [file exists $ready] } else { fail "fake replica did not finish the full sync" }
        wait_for_condition 100 50 {
            [regexp {slave0:.*state=online} [$primary info replication]]
        } else { fail "fake replica not online" }

        # Force IO threads on now that the replica streams, so burst A's replica write is offloaded
        # for sure. Without this the pool stays idle on a quiet host and the main thread writes the
        # backlog inline, so no write is ever in flight and the test has nothing to account. We set
        # it here, not in the overrides, because an active pool during the raw-socket handshake can
        # leave an offloaded REPLCONF reply pending and make PSYNC fall back (see repl-compression).
        $primary config set io-threads-always-active yes

        # Two raw connections. fd carries the write burst; its replies are never read. fdx carries
        # only SHUTDOWN NOW. They must be separate: a reply write to fd is offloaded under the delay
        # below, and while that write is in flight the main thread will not read fd's input, so a
        # SHUTDOWN sent on fd would not be processed until fd's whole reply backlog drained - long
        # after burst A's in-flight window closed. fdx has no pending replies, so its SHUTDOWN is
        # read and run immediately, while burst A is still in flight.
        set fd [w4sb_socket $host $port]
        set fdx [w4sb_socket $host $port]
        # Build the replica backlog FIRST, with NO write delay, so it is not throttled (once the
        # delay is on, the main thread stops reading fd while fd's own reply write is in flight). The
        # replica is PAUSED (trigger not set), so the ~5 MB of burst A overflows the kernel socket
        # buffers and leaves a real backlog pending in the replica output buffer (omem).
        w4sb_burst $fd a 5000 1024
        # Wait until that backlog is really queued on the replica (omem = output buffer memory; a
        # replica uses the reply list, so obl stays 0). This read is on $primary with no delay yet,
        # so it is cheap; a paused replica cannot drain it.
        wait_for_condition 100 20 {
            [regexp {omem=(\d+)} [$primary client list type replica] _ omem] && $omem > 200000
        } else { fail "replica backlog did not build" }
        # Arm the one-second write-done delay.
        $primary debug io-write-done-delay 1000000
        # Release the replica so it starts draining SLOWLY. The socket now accepts a little at a
        # time, so beforeSleep keeps one offloaded write in flight (it writes a chunk, waits out the
        # one-second report delay, re-offloads), while the multi-MB backlog stays pending behind it
        # for seconds.
        set tf [open $trigger w]
        close $tf
        # Wait until a replica write is actually in flight, then shut down right away. With a slow
        # replica the primary keeps a write offloaded continuously, so this stays true and the
        # shutdown reliably catches one in flight. io_threaded_writes_pending lives in the Debug INFO
        # section (a replica never shows obl); its reply is delayed too, but the continuous stream of
        # in-flight writes means one is still in flight when the poll returns. A missed setup instead
        # fails loudly here rather than as a silent "accounted 0".
        wait_for_condition 50 100 {
            [getInfoProperty [$primary info debug] io_threaded_writes_pending] >= 1
        } else { fail "no IO write went in flight after release" }
        # SHUTDOWN NOW on the clean connection. finishShutdown finds one in-flight write with the
        # rest of the backlog pending behind it, against a slow replica:
        #   fixed:  accounts the one in-flight write, does one best-effort flush, drops the rest, and
        #           exits in about a second -> accounted 1.
        #   broken: re-offloads the pending backlog and keeps accounting write after write as the
        #           slow replica dribbles it out -> accounted 2+ (and a much slower exit).
        set t0 [clock milliseconds]
        puts -nonewline $fdx [w4sb_resp shutdown now]
        flush $fdx
        wait_for_condition 800 50 { ![is_alive [srv 0 pid]] } else { fail "primary still alive after 40 s" }
        set elapsed [expr {[clock milliseconds] - $t0}]
        catch {close $fd}
        catch {close $fdx}
        wait_for_condition 200 50 { [file exists $out] } else { fail "fake replica did not stop" }
        set fh [open [srv 0 stdout] r]
        set log [read $fh]
        close $fh
        set accounted 0
        regexp {Accounted (\d+) in-flight replica write} $log -> accounted
        puts "    SHUTDOWN NOW to exit: $elapsed ms, accounted in-flight writes: $accounted"
        assert_match {*ready to exit*} $log
        # The fix accounts at most the one write that was in flight and does not re-offload the
        # backlog, so the drain is bounded by one in-flight write per replica. The broken loop
        # re-offloads the pending backlog and keeps accounting writes while the slow replica
        # dribbles it out, so 2 or more is the mutation signal here. Whether a write is in flight
        # at the exact shutdown instant depends on how much of the backlog the kernel send buffer
        # absorbed, so 0 is a valid outcome of a correct server and is not a failure; the exact
        # once-per-replica count is pinned without timing by the ShutdownBoundTest unit tests
        # (src/unit/test_shutdown_bound.cpp). Elapsed is dominated by the write delay, so only a
        # loose hang guard is asserted on it.
        assert_lessthan $accounted 2
        assert_lessthan $elapsed 30000
    }
}
