# A replication throttler used to be a gate: while one was installed, fast-path clients were sent to
# main for the life of the connection and new connections were turned away. Now it is a brake: main
# hands each lane a share of the throttler's tokens before every read phase; a write command parsed
# with no budget left sends its client to main, where the throttler queues it in FIFO order like any
# main-path client, and the client rejoins the fast path once main has run it. These tests drive the
# budget path with DEBUG THROTTLE-RATE, a fixed-rate write throttler over the same criteria as the
# replication one, so no lagging replica is needed.
#
# Lanes only run when several clients are readable in the same iteration, so load is sent in small
# interleaved rounds across clients.

proc fpt_client {} {
    return [valkey [srv 0 host] [srv 0 port] 1 $::tls]
}

proc fpt_info {field} {
    set v [getInfoProperty [r info fastpath] $field]
    if {$v eq {}} { set v 0 }
    return $v
}

# Write commands executed on the fast path: on a lane, or by the reading thread outside a lane phase.
proc fpt_fastpath_writes {} {
    expr {[fpt_info fastpath_writes] + [fpt_info fastpath_write_lane_entries]}
}

proc fpt_wait_fastpath_clients {n} {
    wait_for_condition 200 20 {
        [getInfoProperty [r info fastpath] fastpath_clients] == $n
    } else {
        fail "expected $n fast-path clients: [r info fastpath]"
    }
}

# Send $n SETs per client in rounds of $chunk, interleaving clients, reading each round's replies
# before the next (a pipelining client of depth $chunk: a budget stop costs it at most one round on
# main before it rejoins). Returns the elapsed ms.
proc fpt_set_round {clients n tag {chunk 50}} {
    set t0 [clock milliseconds]
    for {set base 0} {$base < $n} {incr base $chunk} {
        foreach c $clients {
            for {set i $base} {$i < $base + $chunk && $i < $n} {incr i} { $c set $tag:$i v }
        }
        foreach c $clients { $c flush }
        foreach c $clients {
            for {set i $base} {$i < $base + $chunk && $i < $n} {incr i} { assert_equal OK [$c read] }
        }
    }
    return [expr {[clock milliseconds] - $t0}]
}

start_server {tags {"fastpath throttle lanes external:skip tls:skip"} overrides {io-threads 4 enable-debug-command yes}} {
    assert_equal {io-threads-fast-path yes} [r config get io-threads-fast-path]
    assert_equal {io-threads-parallel-writes yes} [r config get io-threads-parallel-writes]
    r client setname fpt-control
    r select 0

    # Each test opens its own fast-path clients and starts from a clean server: a failed test leaves
    # neither unread replies nor a throttler behind for the next one.
    proc fpt_open {n} {
        catch {r debug throttle-rate off}
        r client kill type normal skipme yes
        fpt_wait_fastpath_clients 0
        set clients {}
        for {set i 0} {$i < $n} {incr i} {
            set c [fpt_client]
            $c ping
            assert_equal PONG [$c read]
            lappend clients $c
        }
        fpt_wait_fastpath_clients $n
        return $clients
    }
    proc fpt_close {clients} {
        foreach c $clients { catch {$c close} }
        catch {r debug throttle-rate off}
        fpt_wait_fastpath_clients 0
    }

    test {Fast path throttle: writes under a throttler keep flowing on the fast path near the target rate} {
        set clients [fpt_open 4]
        # Unthrottled reference: 4 x 2000 SETs run on the fast path.
        set w0 [fpt_fastpath_writes]
        set ms_free [fpt_set_round $clients 2000 free]
        set free_writes [expr {[fpt_fastpath_writes] - $w0}]
        assert_morethan $free_writes 6000

        # 8000 writes at 2000/s: ~3.9 s after the bucket's 0.1 s burst. Both bounds are 15% wide.
        assert_equal OK [r debug throttle-rate 2000]
        set stops0 [fpt_info fastpath_throttle_stops]
        set parked0 [fpt_info fastpath_throttle_parked_reads]
        set ms [fpt_set_round $clients 2000 thr]
        set rate [expr {8000.0 * 1000 / $ms}]
        assert_morethan $rate 1700
        assert_lessthan $rate 2300

        # The pacing happened on the fast path: writers were parked for budget, and at most the first
        # round (fresh clients meeting an empty bucket) went through main's queue. Nobody is stranded
        # on main. (Lane-entry counts are not used: a budget-sized batch is under the parallel-writes
        # minimum and runs as a serial segment.)
        assert_morethan [expr {[fpt_info fastpath_throttle_parked_reads] - $parked0}] 20
        assert_lessthan_equal [expr {[fpt_info fastpath_throttle_stops] - $stops0}] 4
        fpt_wait_fastpath_clients 4
        assert_morethan $ms [expr {$ms_free * 2}]
        fpt_close $clients
    }

    test {Fast path throttle: a write with no budget waits in the throttle queue with the h flag, then the client rejoins} {
        set clients [fpt_open 2]
        set a [lindex $clients 0]
        set b [lindex $clients 1]
        assert_equal OK [r debug throttle-rate 10]
        # At 10/s the lanes' window is one token, so shares are zero and an admitted writer may not
        # overshoot: the first write of this burst with no budget goes to main's queue, which releases
        # it at the rate, whatever the pipeline depth of the read. Meanwhile b's writes queue behind it.
        set stops0 [fpt_info fastpath_throttle_stops]
        for {set i 0} {$i < 6} {incr i} { $a set burst:$i v }
        $a flush
        $b set other v
        $b flush
        wait_for_condition 100 20 {
            [regexp {flags=[A-Za-z]*h} [r client list]]
        } else {
            fail "no client waits in the throttle queue: [r client list]"
        }
        assert_morethan [fpt_info fastpath_throttle_stops] $stops0
        # Lifting the throttler releases the queue; both clients finish and are back on the fast path.
        assert_equal OK [r debug throttle-rate off]
        for {set i 0} {$i < 6} {incr i} { assert_equal OK [$a read] }
        assert_equal OK [$b read]
        fpt_wait_fastpath_clients 2
        fpt_close $clients
    }

    test {Fast path throttle: a connection that only reads is not held up by parked writers} {
        set clients [fpt_open 2]
        set w [lindex $clients 0]
        set ro [lindex $clients 1]
        r set ro-key 1
        assert_equal OK [r debug throttle-rate 10]
        # 20 SETs at 10/s: the first few run on budget, the next stops the client into main's queue for
        # one release, then it rejoins as a writer and is parked between tokens for the rest.
        set parked0 [fpt_info fastpath_throttle_parked_reads]
        for {set i 0} {$i < 20} {incr i} { $w set q:$i v }
        $w flush
        wait_for_condition 100 20 {
            [fpt_info fastpath_throttle_parked_reads] > $parked0
        } else {
            fail "writer was not parked: [r info fastpath]"
        }
        # Reads on another fast-path connection, and the control connection itself, answer at once.
        set t0 [clock milliseconds]
        for {set i 0} {$i < 20} {incr i} {
            $ro get ro-key
            $ro flush
            assert_equal 1 [$ro read]
        }
        assert_lessthan [expr {[clock milliseconds] - $t0}] 500
        assert_equal OK [r debug throttle-rate off]
        for {set i 0} {$i < 20} {incr i} { assert_equal OK [$w read] }
        fpt_wait_fastpath_clients 2
        fpt_close $clients
    }

    test {Fast path throttle: one connection's pipeline keeps its order across budget stops} {
        set clients [fpt_open 4]
        assert_equal OK [r debug throttle-rate 2000]
        r del ctr
        # INCRs interleaved with reads that depend on them, sent in rounds so the client stops and
        # rejoins several times: every reply must match its position.
        set c [lindex $clients 1]
        for {set i 1} {$i <= 800} {incr i} {
            $c incr ctr
            if {$i % 50 == 0} {
                $c get ctr
                $c flush
            }
        }
        for {set i 1} {$i <= 800} {incr i} {
            assert_equal $i [$c read]
            if {$i % 50 == 0} { assert_equal $i [$c read] }
        }
        # Four clients appending to their own lists in interleaved rounds: each list in send order.
        set n 300
        for {set j 0} {$j < 4} {incr j} { r del lst:$j }
        for {set base 0} {$base < $n} {incr base 50} {
            set j 0
            foreach c $clients {
                for {set i $base} {$i < $base + 50} {incr i} { $c rpush lst:$j $i }
                incr j
            }
            foreach c $clients { $c flush }
        }
        foreach c $clients {
            for {set i 0} {$i < $n} {incr i} { assert_equal [expr {$i + 1}] [$c read] }
        }
        set want {}
        for {set i 0} {$i < $n} {incr i} { lappend want $i }
        for {set j 0} {$j < 4} {incr j} { assert_equal $want [r lrange lst:$j 0 -1] }
        fpt_wait_fastpath_clients 4
        fpt_close $clients
    }

    test {Fast path throttle: a main-path writer queued first is released before a fast-path write} {
        set clients [fpt_open 1]
        set c [lindex $clients 0]
        assert_equal OK [r debug throttle-rate 10]
        # Empty the bucket (3 tokens at 10/s) with a fast-path burst, then queue a main-path write (a
        # SELECTed connection writes on main), then send a fast-path write: it must not overtake.
        for {set i 0} {$i < 3} {incr i} { $c set drain:$i v }
        $c flush
        for {set i 0} {$i < 3} {incr i} { assert_equal OK [$c read] }
        set m [valkey_deferring_client]
        $m select 0
        assert_equal OK [$m read]
        $m client id
        set mid [$m read]
        $m set main-first 1
        $m flush
        wait_for_condition 100 20 {
            [regexp "id=$mid \[^\n\]*flags=\[A-Za-z\]*h" [r client list]]
        } else {
            fail "main-path writer did not queue in the throttler: [r client list]"
        }
        $c incr fp-second
        $c flush
        # The next token (100 ms away) goes to the main-path writer; the fast-path write follows.
        assert_equal OK [$m read]
        assert_equal 1 [r get main-first]
        assert_equal 1 [$c read]
        assert_equal 1 [r get fp-second]
        $m close
        fpt_wait_fastpath_clients 1
        fpt_close $clients
    }

    test {Fast path throttle: removing the throttler restores full speed on the fast path at once} {
        set clients [fpt_open 4]
        assert_equal OK [r debug throttle-rate 2000]
        fpt_set_round $clients 200 warm
        assert_equal OK [r debug throttle-rate off]
        fpt_wait_fastpath_clients 4
        set w0 [fpt_fastpath_writes]
        set ms [fpt_set_round $clients 2000 off]
        # 8000 writes would take ~4 s throttled; unthrottled they take a fraction of a second, and the
        # bulk of them on lanes (the tail of each round is a small serial segment).
        assert_lessthan $ms 1500
        assert_morethan [expr {[fpt_fastpath_writes] - $w0}] 6000
        fpt_close $clients
    }

    test {Fast path throttle: a client accepted under a throttler joins the fast path} {
        fpt_open 0
        assert_equal OK [r debug throttle-rate 2000]
        set d [fpt_client]
        $d set accepted v
        assert_equal OK [$d read]
        fpt_wait_fastpath_clients 1
        assert_equal OK [r debug throttle-rate off]
        $d close
        fpt_wait_fastpath_clients 0
    }
}
