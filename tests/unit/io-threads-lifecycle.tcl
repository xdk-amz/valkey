# IO worker lifecycle: clients an IO thread owns survive, or close deterministically,
# when the worker set shrinks, the fast path or strict offload is disabled, the
# server shuts down, or a scale-up fails part way.

# A deferring raw db-0 client: SELECT would move it off the fast path.
proc lc_client {} {
    return [valkey [srv 0 host] [srv 0 port] 1 $::tls]
}

proc lc_fastpath_clients {} {
    return [getInfoProperty [r info fastpath] fastpath_clients]
}

proc lc_workers_open {} {
    return [getInfoProperty [r info fastpath] fastpath_workers_open]
}

proc lc_running {} {
    return [getInfoProperty [r info server] io_threads_running]
}

proc lc_retiring {} {
    return [getInfoProperty [r info server] io_threads_retiring]
}

proc lc_wait_fastpath_clients {n} {
    wait_for_condition 500 20 {
        [lc_fastpath_clients] == $n
    } else {
        fail "expected $n fast-path clients: [r info fastpath]"
    }
}

# Every retirement finished and every running worker's role matches the configuration.
proc lc_wait_settled {} {
    wait_for_condition 500 20 {
        [lc_retiring] == 0 && [getInfoProperty [r info fastpath] fastpath_workers_quiescing] == 0
    } else {
        fail "workers did not settle: [r info server] [r info fastpath]"
    }
}

proc lc_wait_running {n} {
    wait_for_condition 500 20 {
        [lc_running] == $n
    } else {
        fail "expected $n running IO threads: [r info server]"
    }
}

proc lc_wait_workers_open {n} {
    wait_for_condition 500 20 {
        [lc_workers_open] == $n
    } else {
        fail "expected $n open fast-path workers: [r info fastpath]"
    }
}

# Opens $n fast-path clients, each with its own counter key at 0.
proc lc_open_counters {n prefix} {
    set clients {}
    for {set i 0} {$i < $n} {incr i} {
        set c [lc_client]
        $c set $prefix$i 0
        assert_equal OK [$c read]
        lappend clients $c
    }
    lc_wait_fastpath_clients $n
    return $clients
}

proc lc_pipeline_incr {clients prefix count} {
    set i 0
    foreach c $clients {
        for {set j 0} {$j < $count} {incr j} { $c incr $prefix$i }
        $c flush
        incr i
    }
}

# Each client must see exactly the replies 1..$total, in order.
proc lc_assert_incr_replies {clients prefix total} {
    set i 0
    foreach c $clients {
        for {set j 1} {$j <= $total} {incr j} { assert_equal $j [$c read] }
        assert_equal $total [r get $prefix$i]
        incr i
    }
}

proc lc_resp {args} {
    set cmd "*[llength $args]\r\n"
    foreach a $args { append cmd "$[string length $a]\r\n$a\r\n" }
    return $cmd
}

# A named deferring control connection: named clients stay on the main path.
proc lc_ctl_client {} {
    set c [valkey_deferring_client]
    $c client setname lc-ctl
    assert_equal OK [$c read]
    return $c
}

# Runs $cmds right behind a DEBUG SLEEP from one read, so batches published
# meanwhile are still unexecuted when $cmds run.
proc lc_run_behind_sleep {ctl secs cmds} {
    set payload [lc_resp debug sleep $secs]
    foreach cmd $cmds { append payload [lc_resp {*}$cmd] }
    $ctl write $payload
    $ctl flush
}

start_server {tags {"io-threads-lifecycle external:skip tls:skip"} overrides {io-threads 16 io-threads-always-active yes enable-debug-command yes}} {
    assert_equal {io-threads 16} [r config get io-threads]
    r select 0

    test {Lifecycle: io-threads 16 -> 1 keeps active fast-path clients and their reply order} {
        set clients [lc_open_counters 8 lc:a]
        lc_wait_running 15
        lc_pipeline_incr $clients lc:a 100
        assert_equal OK [r config set io-threads 1]
        lc_pipeline_incr $clients lc:a 100
        lc_assert_incr_replies $clients lc:a 200
        lc_wait_settled
        assert_equal 0 [lc_running]
        assert_equal 0 [lc_fastpath_clients]
        # The connections are still open and served by main.
        foreach c $clients {
            $c ping
            assert_equal PONG [$c read]
        }
        set ::lc_clients $clients
    }

    test {Lifecycle: IO threads can be increased again after 16 -> 1 -> 16} {
        assert_equal OK [r config set io-threads 16]
        lc_wait_running 15
        lc_wait_settled
        lc_wait_workers_open 15
        set c [lc_client]
        $c set lc:b v
        assert_equal OK [$c read]
        lc_wait_fastpath_clients 1
        # Clients handed off by the previous shrink keep working on the main path.
        foreach old $::lc_clients {
            $old incr lc:c
            assert_match {[0-9]*} [$old read]
            $old close
        }
        $c close
        lc_wait_fastpath_clients 0
    }

    test {Lifecycle: overlapping io-threads changes converge with clients pipelining throughout} {
        set clients [lc_open_counters 6 lc:d]
        set total 0
        foreach n {8 3 1 5 16 2 16} {
            lc_pipeline_incr $clients lc:d 40
            incr total 40
            assert_equal OK [r config set io-threads $n]
            assert_equal "io-threads $n" [r config get io-threads]
        }
        lc_assert_incr_replies $clients lc:d $total
        lc_wait_running 15
        lc_wait_settled
        foreach c $clients { $c close }
    }
}

start_server {tags {"io-threads-lifecycle external:skip tls:skip"} overrides {io-threads 4 io-threads-always-active yes enable-debug-command yes}} {
    assert_equal {io-threads 4} [r config get io-threads]
    r select 0

    test {Lifecycle: disabling the fast path transitions existing fast-path clients} {
        set clients [lc_open_counters 3 lc:e]
        lc_pipeline_incr $clients lc:e 50
        assert_equal OK [r config set io-threads-fast-path no]
        lc_pipeline_incr $clients lc:e 50
        lc_assert_incr_replies $clients lc:e 100
        lc_wait_fastpath_clients 0
        lc_wait_workers_open 0
        assert_equal 3 [lc_running]
        # New connections are not admitted while the fast path is off.
        set c [lc_client]
        $c set lc:f v
        assert_equal OK [$c read]
        after 100
        assert_equal 0 [lc_fastpath_clients]
        $c close
        foreach c $clients { $c close }
    }

    test {Lifecycle: re-enabling the fast path reopens the workers} {
        assert_equal OK [r config set io-threads-fast-path yes]
        lc_wait_workers_open 3
        set c [lc_client]
        $c set lc:g v
        assert_equal OK [$c read]
        lc_wait_fastpath_clients 1
        $c close
        lc_wait_fastpath_clients 0
    }

    test {Lifecycle: disabling strict offload hands worker-owned clients to main} {
        set clients [lc_open_counters 3 lc:h]
        lc_pipeline_incr $clients lc:h 50
        assert_equal OK [r config set io-threads-strict-offload no]
        lc_pipeline_incr $clients lc:h 50
        lc_assert_incr_replies $clients lc:h 100
        lc_wait_fastpath_clients 0
        lc_wait_workers_open 0
        foreach c $clients {
            $c ping
            assert_equal PONG [$c read]
            $c close
        }
        assert_equal OK [r config set io-threads-strict-offload yes]
        lc_wait_workers_open 3
    }

    test {Lifecycle: commands published before quiescing complete exactly once, in order} {
        foreach setting {{io-threads 1} {io-threads-fast-path no}} {
            set c [lc_client]
            $c set lc:i 0
            assert_equal OK [$c read]
            lc_wait_fastpath_clients 1
            set ctl [lc_ctl_client]
            set batches [getInfoProperty [r info fastpath] fastpath_batches]
            lc_run_behind_sleep $ctl 0.4 [list [list config set {*}$setting]]
            after 50
            for {set j 0} {$j < 40} {incr j} { $c incr lc:i }
            $c flush
            wait_for_condition 100 20 {
                [getInfoProperty [r info fastpath] fastpath_batches] > $batches
            } else {
                fail "commands were not published while main slept"
            }
            assert_equal OK [$ctl read]
            assert_equal OK [$ctl read]
            for {set j 1} {$j <= 40} {incr j} { assert_equal $j [$c read] }
            assert_equal 40 [r get lc:i]
            lc_wait_fastpath_clients 0
            lc_wait_settled
            $c close
            $ctl close
            assert_equal OK [r config set io-threads 4]
            assert_equal OK [r config set io-threads-fast-path yes]
            lc_wait_running 3
            lc_wait_workers_open 3
        }
    }

    test {Lifecycle: a client disconnecting with a batch in flight during quiescing is harmless} {
        set c [lc_client]
        $c set lc:j 0
        assert_equal OK [$c read]
        lc_wait_fastpath_clients 1
        set ctl [lc_ctl_client]
        lc_run_behind_sleep $ctl 0.4 [list [list config set io-threads 1]]
        after 50
        for {set j 0} {$j < 20} {incr j} { $c incr lc:j }
        $c flush
        after 50
        $c close
        assert_equal OK [$ctl read]
        assert_equal OK [$ctl read]
        lc_wait_fastpath_clients 0
        lc_wait_settled
        assert_equal PONG [r ping]
        assert {[r get lc:j] <= 20}
        $ctl close
        assert_equal OK [r config set io-threads 4]
        lc_wait_running 3
        lc_wait_workers_open 3
    }

    test {Lifecycle: a slow reader cannot block reconfiguration and loses no reply} {
        r set lc:big [string repeat x 1048576]
        set c [lc_client]
        $c set lc:k v
        assert_equal OK [$c read]
        lc_wait_fastpath_clients 1
        # Sixty-four 1MB replies with nobody reading: the socket fills and the
        # worker buffers the rest.
        for {set j 0} {$j < 64} {incr j} { $c get lc:big }
        $c flush
        wait_for_condition 100 20 {
            [getInfoProperty [r info fastpath] fastpath_net_output_bytes] > 1048576
        } else {
            fail "worker never wrote"
        }
        assert_equal OK [r config set io-threads 1]
        lc_wait_fastpath_clients 0
        lc_wait_settled
        # Everything comes out, in order, through the main path now.
        for {set j 0} {$j < 64} {incr j} { assert_equal 1048576 [string length [$c read]] }
        $c ping
        assert_equal PONG [$c read]
        $c close
        r del lc:big
        assert_equal OK [r config set io-threads 4]
        lc_wait_running 3
        lc_wait_workers_open 3
    }

    test {Lifecycle: batch backpressure during quiescing does not deadlock} {
        assert_equal OK [r config set io-batch-inflight 1]
        set clients [lc_open_counters 8 lc:l]
        lc_pipeline_incr $clients lc:l 200
        assert_equal OK [r config set io-threads 1]
        lc_pipeline_incr $clients lc:l 100
        lc_assert_incr_replies $clients lc:l 300
        lc_wait_settled
        foreach c $clients { $c close }
        assert_equal OK [r config set io-batch-inflight 16]
        assert_equal OK [r config set io-threads 4]
        lc_wait_running 3
        lc_wait_workers_open 3
    }

    test {Lifecycle: repeated and overlapping quiesce requests are idempotent} {
        set clients [lc_open_counters 4 lc:m]
        lc_pipeline_incr $clients lc:m 30
        assert_equal OK [r config set io-threads-fast-path no]
        assert_equal OK [r config set io-threads-fast-path no]
        assert_equal OK [r config set io-threads-strict-offload no]
        assert_equal OK [r config set io-threads 2]
        assert_equal OK [r config set io-threads 1]
        assert_equal OK [r config set io-threads-fast-path yes]
        lc_pipeline_incr $clients lc:m 30
        lc_assert_incr_replies $clients lc:m 60
        lc_wait_settled
        assert_equal 0 [lc_running]
        foreach c $clients {
            $c ping
            assert_equal PONG [$c read]
            $c close
        }
        assert_equal OK [r config set io-threads-strict-offload yes]
        assert_equal OK [r config set io-threads 4]
        lc_wait_running 3
        lc_wait_workers_open 3
    }

    test {Lifecycle: a partial scale-up failure rolls back the threads it started} {
        set clients [lc_open_counters 2 lc:n]
        lc_pipeline_incr $clients lc:n 20
        assert_equal OK [r debug io-threads-fail-create 6]
        catch {r config set io-threads 8} e
        assert_match {*Can't create IO threads*} $e
        assert_equal {io-threads 4} [r config get io-threads]
        lc_wait_running 3
        lc_wait_settled
        verify_log_message 0 "*IO thread 6: creation failure injected*" 0
        lc_assert_incr_replies $clients lc:n 20
        # The slots the failed attempt used are reusable.
        assert_equal OK [r config set io-threads 8]
        lc_wait_running 7
        lc_wait_workers_open 7
        foreach c $clients { $c close }
        assert_equal OK [r config set io-threads 4]
        lc_wait_running 3
        lc_wait_settled
    }

    test {Lifecycle: fast-path attribution is intact after a shrink and regrow} {
        assert_equal OK [r config set io-threads 1]
        lc_wait_settled
        assert_equal OK [r config set io-threads 4]
        lc_wait_running 3
        lc_wait_workers_open 3
        r acl log reset
        r acl setuser default resetkeys ~allowed:*
        set c [lc_client]
        $c set allowed:x 1
        assert_equal OK [$c read]
        lc_wait_fastpath_clients 1
        set sock [fconfigure [$c channel] -sockname]
        set peer "[lindex $sock 0]:[lindex $sock 2]"
        set id ""
        foreach line [split [string trim [r client list]] "\n"] {
            if {[string match "*addr=$peer *" $line]} { regexp {id=(\d+)} $line -> id }
        }
        assert {$id ne ""}
        $c get denied:y
        assert_error {*NOPERM*} {$c read}
        set entry [lindex [r acl log] 0]
        assert_equal denied:y [dict get $entry object]
        assert_match "id=$id addr=$peer *" [dict get $entry client-info]
        $c close
        r acl setuser default resetkeys allkeys
    }
}

start_server {tags {"io-threads-lifecycle external:skip tls:skip"} overrides {io-threads 4 io-threads-always-active yes}} {
    test {Lifecycle: shutdown under active fast-path traffic exits cleanly} {
        r select 0
        set clients {}
        for {set i 0} {$i < 8} {incr i} {
            set c [lc_client]
            $c set lc:s$i 0
            assert_equal OK [$c read]
            lappend clients $c
        }
        lc_wait_fastpath_clients 8
        # A deep pipeline nobody reads: the workers are publishing when SHUTDOWN lands.
        foreach c $clients {
            for {set j 0} {$j < 2000} {incr j} { $c incr lc:s }
            $c flush
        }
        set pid [s process_id]
        catch {r shutdown nosave}
        wait_for_log_messages 0 {"*ready to exit, bye bye*"} 0 200 10
        wait_for_condition 200 10 {
            ![process_is_alive $pid]
        } else {
            fail "server did not exit"
        }
        set log [exec cat [srv 0 stdout]]
        assert_equal 0 [string match {*VALKEY BUG REPORT*} $log]
        assert_equal 0 [string match {*did not stop before exit*} $log]
        foreach c $clients { catch {$c close} }
    }
}
