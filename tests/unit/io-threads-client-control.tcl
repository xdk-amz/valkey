# ClientControl lifecycle and reply-accounting integration coverage.
#
# The connection stays IO-owned (a ClientConnection); its shared, stable state
# lives in a ClientControl reached through a generation-checked ClientHandle.
# Lifecycle intent (QUIESCE/HANDOFF/EVICT/CLOSE) is published by an authorized
# domain and executed only by the owner, and every external reply is charged to
# the control on produce and released only on reclaim. Reply-accounting counters
# are internal, so these tests prove the invariants through observable behavior:
# replies arrive exactly once and in order across a lifecycle transition, charged
# reply memory is never dropped mid-flight, a control slot is reusable only under a
# fresh generation, and no teardown ever asserts or corrupts the heap.

# A deferring raw db-0 client. SELECT would hand it off the fast path, so the
# lifecycle tests must never use a helper that issues one.
proc cc_client {} {
    return [valkey [srv 0 host] [srv 0 port] 1 $::tls]
}

proc cc_fastpath_clients {} {
    return [getInfoProperty [r info fastpath] fastpath_clients]
}

proc cc_workers_open {} {
    return [getInfoProperty [r info fastpath] fastpath_workers_open]
}

proc cc_running {} {
    return [getInfoProperty [r info server] io_threads_running]
}

proc cc_output_bytes {} {
    return [getInfoProperty [r info fastpath] fastpath_net_output_bytes]
}

proc cc_batches {} {
    return [getInfoProperty [r info fastpath] fastpath_batches]
}

proc cc_wait_fastpath_clients {n} {
    wait_for_condition 500 20 {
        [cc_fastpath_clients] == $n
    } else {
        fail "expected $n fast-path clients: [r info fastpath]"
    }
}

proc cc_wait_running {n} {
    wait_for_condition 500 20 {
        [cc_running] == $n
    } else {
        fail "expected $n running IO threads: [r info server]"
    }
}

proc cc_wait_workers_open {n} {
    wait_for_condition 500 20 {
        [cc_workers_open] == $n
    } else {
        fail "expected $n open fast-path workers: [r info fastpath]"
    }
}

# Every retirement finished and no worker is still quiescing.
proc cc_wait_settled {} {
    wait_for_condition 500 20 {
        [getInfoProperty [r info server] io_threads_retiring] == 0 &&
        [getInfoProperty [r info fastpath] fastpath_workers_quiescing] == 0
    } else {
        fail "workers did not settle: [r info server] [r info fastpath]"
    }
}

# Opens a raw db-0 fast-path client and seeds its counter key at 0.
proc cc_open_counter {key} {
    set c [cc_client]
    $c set $key 0
    assert_equal OK [$c read]
    return $c
}

# Encodes a RESP command so several can be written back-to-back in one payload.
proc cc_resp {args} {
    set cmd "*[llength $args]\r\n"
    foreach a $args { append cmd "$[string length $a]\r\n$a\r\n" }
    return $cmd
}

# The server has not crashed or emitted an assertion.
proc cc_assert_healthy {} {
    assert_equal PONG [r ping]
    set log [exec cat [srv 0 stdout]]
    assert_equal 0 [string match {*VALKEY BUG REPORT*} $log]
    assert_equal 0 [string match {*=== ASSERTION FAILED ===*} $log]
}

start_server {tags {"io-threads-client-control external:skip tls:skip"} overrides {io-threads 4 io-threads-always-active yes enable-debug-command yes}} {
    assert_equal {io-threads 4} [r config get io-threads]
    r select 0

    test {Control: a client disconnecting in flight drops its work without asserting} {
        set c [cc_open_counter cc:a]
        cc_wait_fastpath_clients 1
        # A deep pipeline nobody will read, then close mid-flight so entries carrying
        # the client's ClientHandle are still detached when the connection is gone.
        for {set j 0} {$j < 2000} {incr j} { $c incr cc:a }
        $c flush
        $c close
        cc_wait_fastpath_clients 0
        cc_wait_settled
        cc_assert_healthy
        # The control slot is reclaimed: a fresh client is admitted normally.
        set d [cc_open_counter cc:a2]
        cc_wait_fastpath_clients 1
        $d incr cc:a2
        assert_equal 1 [$d read]
        $d close
        cc_wait_fastpath_clients 0
    }

    test {Control: disconnect with charged, unwritten replies releases cleanly} {
        r set cc:big [string repeat x 1048576]
        set c [cc_client]
        $c set cc:b v
        assert_equal OK [$c read]
        cc_wait_fastpath_clients 1
        # Large replies with nobody reading: the socket fills, the worker retains
        # (charges) the rest. Closing now must release the charged reply memory.
        for {set j 0} {$j < 64} {incr j} { $c get cc:big }
        $c flush
        wait_for_condition 100 20 {
            [cc_output_bytes] > 1048576
        } else {
            fail "worker never wrote a reply"
        }
        $c close
        cc_wait_fastpath_clients 0
        cc_wait_settled
        cc_assert_healthy
        r del cc:big
    }

    test {Control: a reply produced before a close request still reaches the wire} {
        set c [cc_open_counter cc:c]
        cc_wait_fastpath_clients 1
        # Pipeline work, then disable the fast path (publishes the handoff/close
        # transition) right behind it. Every already-produced reply must be delivered.
        for {set j 0} {$j < 200} {incr j} { $c incr cc:c }
        $c flush
        assert_equal OK [r config set io-threads-fast-path no]
        for {set j 1} {$j <= 200} {incr j} { assert_equal $j [$c read] }
        assert_equal 200 [r get cc:c]
        cc_wait_fastpath_clients 0
        cc_wait_workers_open 0
        # The connection survives the transition and is served by main.
        $c ping
        assert_equal PONG [$c read]
        $c close
        assert_equal OK [r config set io-threads-fast-path yes]
        cc_wait_workers_open 3
    }

    test {Control: a batch published just before a close request executes exactly once} {
        set c [cc_open_counter cc:d]
        cc_wait_fastpath_clients 1
        set ctl [valkey_deferring_client]
        $ctl client setname cc-ctl
        assert_equal OK [$ctl read]
        set batches [cc_batches]
        # DEBUG SLEEP holds main; the client's batch publishes while it sleeps, then
        # the reconfigure lands. The late batch must run once, in order, before close.
        set payload [cc_resp debug sleep 0.4]
        append payload [cc_resp config set io-threads 1]
        $ctl write $payload
        $ctl flush
        after 50
        for {set j 0} {$j < 40} {incr j} { $c incr cc:d }
        $c flush
        wait_for_condition 100 20 {
            [cc_batches] > $batches
        } else {
            fail "no batch published while main slept"
        }
        assert_equal OK [$ctl read]
        assert_equal OK [$ctl read]
        for {set j 1} {$j <= 40} {incr j} { assert_equal $j [$c read] }
        assert_equal 40 [r get cc:d]
        cc_wait_fastpath_clients 0
        cc_wait_settled
        $c close
        $ctl close
        assert_equal OK [r config set io-threads 4]
        cc_wait_running 3
        cc_wait_workers_open 3
    }

    test {Control: a control slot is reused only under a fresh generation} {
        # Churn many short-lived clients through the same worker slots. If a stale
        # ClientHandle were resolved after its slot was reused, a reply would land on
        # the wrong connection or the server would assert. Each client must see only
        # its own replies.
        for {set round 0} {$round < 40} {incr round} {
            set c [cc_client]
            $c set cc:e $round
            assert_equal OK [$c read]
            $c incr cc:e:$round
            assert_equal 1 [$c read]
            $c get cc:e
            assert_equal $round [$c read]
            $c close
        }
        cc_wait_fastpath_clients 0
        cc_wait_settled
        cc_assert_healthy
    }

    test {Control: handoff carrying commands and replies loses nothing} {
        set clients {}
        for {set i 0} {$i < 6} {incr i} {
            set c [cc_open_counter cc:f$i]
            lappend clients $c
        }
        cc_wait_fastpath_clients 6
        # Each client has produced replies in flight when strict-offload is disabled,
        # which hands every worker-owned connection back to main (HANDOFF).
        set i 0
        foreach c $clients { for {set j 0} {$j < 100} {incr j} { $c incr cc:f$i }; incr i }
        foreach c $clients { $c flush }
        assert_equal OK [r config set io-threads-strict-offload no]
        set i 0
        foreach c $clients {
            for {set j 1} {$j <= 100} {incr j} { assert_equal $j [$c read] }
            assert_equal 100 [r get cc:f$i]
            $c ping
            assert_equal PONG [$c read]
            incr i
        }
        cc_wait_fastpath_clients 0
        cc_wait_workers_open 0
        foreach c $clients { $c close }
        assert_equal OK [r config set io-threads-strict-offload yes]
        cc_wait_workers_open 3
    }

    test {Control: a close during quiesce supersedes the pending handoff} {
        set c [cc_open_counter cc:g]
        cc_wait_fastpath_clients 1
        set ctl [valkey_deferring_client]
        $ctl client setname cc-ctl2
        assert_equal OK [$ctl read]
        # Begin a quiesce (io-threads down) while main sleeps, then close the client:
        # a CLOSE arriving after a HANDOFF started still frees the connection, and the
        # server must stay healthy with no leaked slot.
        set payload [cc_resp debug sleep 0.4]
        append payload [cc_resp config set io-threads 1]
        $ctl write $payload
        $ctl flush
        after 50
        for {set j 0} {$j < 20} {incr j} { $c incr cc:g }
        $c flush
        after 50
        $c close
        assert_equal OK [$ctl read]
        assert_equal OK [$ctl read]
        cc_wait_fastpath_clients 0
        cc_wait_settled
        cc_assert_healthy
        assert {[r get cc:g] <= 20}
        $ctl close
        assert_equal OK [r config set io-threads 4]
        cc_wait_running 3
        cc_wait_workers_open 3
    }

    test {Control: repeated and conflicting lifecycle requests are idempotent} {
        set clients {}
        for {set i 0} {$i < 4} {incr i} {
            set c [cc_open_counter cc:h$i]
            lappend clients $c
        }
        cc_wait_fastpath_clients 4
        set i 0
        foreach c $clients { for {set j 0} {$j < 30} {incr j} { $c incr cc:h$i }; incr i }
        foreach c $clients { $c flush }
        # A storm of overlapping quiesce/handoff/close-shaped requests: duplicates and
        # conflicts must collapse to one deterministic transition per client.
        assert_equal OK [r config set io-threads-fast-path no]
        assert_equal OK [r config set io-threads-fast-path no]
        assert_equal OK [r config set io-threads-strict-offload no]
        assert_equal OK [r config set io-threads 2]
        assert_equal OK [r config set io-threads 1]
        set i 0
        foreach c $clients {
            for {set j 1} {$j <= 30} {incr j} { assert_equal $j [$c read] }
            assert_equal 30 [r get cc:h$i]
            $c ping
            assert_equal PONG [$c read]
            incr i
        }
        cc_wait_settled
        cc_assert_healthy
        foreach c $clients { $c close }
        assert_equal OK [r config set io-threads-strict-offload yes]
        assert_equal OK [r config set io-threads-fast-path yes]
        assert_equal OK [r config set io-threads 4]
        cc_wait_running 3
        cc_wait_workers_open 3
    }

    test {Control: a protocol error with a lifecycle pending closes without asserting} {
        set c [cc_open_counter cc:i]
        cc_wait_fastpath_clients 1
        # Publish a quiesce while main sleeps, then send a garbage inline line that
        # trips a protocol error while the lifecycle transition is pending.
        set ctl [valkey_deferring_client]
        $ctl client setname cc-ctl3
        assert_equal OK [$ctl read]
        set payload [cc_resp debug sleep 0.3]
        append payload [cc_resp config set io-threads 1]
        $ctl write $payload
        $ctl flush
        after 30
        $c write "*1\r\n\$3\r\nfoo\r\n*abc\r\n"
        $c flush
        catch {$c read} e
        catch {$c close}
        assert_equal OK [$ctl read]
        assert_equal OK [$ctl read]
        cc_wait_fastpath_clients 0
        cc_wait_settled
        cc_assert_healthy
        $ctl close
        assert_equal OK [r config set io-threads 4]
        cc_wait_running 3
        cc_wait_workers_open 3
    }

    test {Control: queue backpressure during a lifecycle transition does not deadlock} {
        assert_equal OK [r config set io-batch-inflight 1]
        set clients {}
        for {set i 0} {$i < 8} {incr i} {
            set c [cc_open_counter cc:j$i]
            lappend clients $c
        }
        cc_wait_fastpath_clients 8
        set i 0
        foreach c $clients { for {set j 0} {$j < 200} {incr j} { $c incr cc:j$i }; incr i }
        foreach c $clients { $c flush }
        # Handoff every client to main while the in-flight window is one batch deep:
        # publication cannot outrun return, so the transition must still drain.
        assert_equal OK [r config set io-threads 1]
        set i 0
        foreach c $clients {
            for {set j 1} {$j <= 200} {incr j} { assert_equal $j [$c read] }
            assert_equal 200 [r get cc:j$i]
            incr i
        }
        cc_wait_settled
        cc_assert_healthy
        foreach c $clients { $c close }
        assert_equal OK [r config set io-batch-inflight 16]
        assert_equal OK [r config set io-threads 4]
        cc_wait_running 3
        cc_wait_workers_open 3
    }

    test {Control: a slow reader cannot stall a lifecycle transition and loses no reply} {
        r set cc:big [string repeat x 1048576]
        set c [cc_client]
        $c set cc:k v
        assert_equal OK [$c read]
        cc_wait_fastpath_clients 1
        for {set j 0} {$j < 64} {incr j} { $c get cc:big }
        $c flush
        wait_for_condition 100 20 {
            [cc_output_bytes] > 1048576
        } else {
            fail "worker never wrote"
        }
        assert_equal OK [r config set io-threads 1]
        cc_wait_fastpath_clients 0
        cc_wait_settled
        # Every charged reply is handed to main and delivered in order.
        for {set j 0} {$j < 64} {incr j} { assert_equal 1048576 [string length [$c read]] }
        $c ping
        assert_equal PONG [$c read]
        $c close
        r del cc:big
        assert_equal OK [r config set io-threads 4]
        cc_wait_running 3
        cc_wait_workers_open 3
    }
}

start_server {tags {"io-threads-client-control external:skip tls:skip"} overrides {io-threads 4 io-threads-always-active yes}} {
    test {Control: shutdown while reconfiguring under load exits with no worker-destruction assert} {
        r select 0
        set clients {}
        for {set i 0} {$i < 8} {incr i} {
            set c [cc_open_counter cc:s$i]
            lappend clients $c
        }
        cc_wait_fastpath_clients 8
        # A deep unread pipeline: workers hold charged replies. Shrink the worker set
        # (begin quiescing) and immediately shut down, so teardown must reclaim every
        # ClientControl with outstanding replies and no worker-destruction assertion.
        foreach c $clients {
            for {set j 0} {$j < 2000} {incr j} { $c incr cc:s }
            $c flush
        }
        set pid [s process_id]
        catch {r config set io-threads 1}
        catch {r shutdown nosave}
        wait_for_log_messages 0 {"*ready to exit, bye bye*"} 0 200 10
        wait_for_condition 200 10 {
            ![process_is_alive $pid]
        } else {
            fail "server did not exit"
        }
        set log [exec cat [srv 0 stdout]]
        assert_equal 0 [string match {*VALKEY BUG REPORT*} $log]
        assert_equal 0 [string match {*=== ASSERTION FAILED ===*} $log]
        assert_equal 0 [string match {*did not stop before exit*} $log]
        foreach c $clients { catch {$c close} }
    }
}
