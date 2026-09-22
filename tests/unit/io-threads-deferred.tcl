# Clients an IO thread cannot read because its in-flight batch cap is reached wait in a
# per-thread FIFO and are served oldest first as returned batches free capacity. These tests
# force the cap to one batch so every pipelined client is deferred, then check that the
# deferred clients all progress, that a deferred client can be closed by either side, and
# that a lifecycle transition with a populated FIFO drains without asserting.

proc df_client {} {
    return [valkey [srv 0 host] [srv 0 port] 1 $::tls]
}

proc df_info {field} {
    return [getInfoProperty [r info fastpath] $field]
}

proc df_wait_fastpath_clients {n} {
    wait_for_condition 500 20 {
        [df_info fastpath_clients] == $n
    } else {
        fail "expected $n fast-path clients: [r info fastpath]"
    }
}

proc df_wait_settled {} {
    wait_for_condition 500 20 {
        [getInfoProperty [r info server] io_threads_retiring] == 0 &&
        [df_info fastpath_workers_quiescing] == 0
    } else {
        fail "workers did not settle: [r info server] [r info fastpath]"
    }
}

proc df_assert_healthy {} {
    assert_equal PONG [r ping]
    set log [exec cat [srv 0 stdout]]
    assert_equal 0 [string match {*VALKEY BUG REPORT*} $log]
    assert_equal 0 [string match {*=== ASSERTION FAILED ===*} $log]
}

# Opens n raw db-0 fast-path clients, each with its own counter key at 0.
proc df_open_clients {n prefix} {
    set clients {}
    for {set i 0} {$i < $n} {incr i} {
        set c [df_client]
        $c set $prefix$i 0
        assert_equal OK [$c read]
        lappend clients $c
    }
    df_wait_fastpath_clients $n
    return $clients
}

start_server {tags {"io-threads-deferred external:skip tls:skip"} overrides {io-threads 2 io-threads-always-active yes enable-debug-command yes}} {
    r select 0

    test {Deferred: capped clients are deferred and all complete in order} {
        assert_equal OK [r config set io-batch-inflight 1]
        set before [df_info fastpath_deferrals]
        set clients [df_open_clients 16 df:a]
        set i 0
        foreach c $clients { for {set j 0} {$j < 100} {incr j} { $c incr df:a$i }; incr i }
        foreach c $clients { $c flush }
        set i 0
        foreach c $clients {
            for {set j 1} {$j <= 100} {incr j} { assert_equal $j [$c read] }
            assert_equal 100 [r get df:a$i]
            incr i
        }
        # With one batch in flight per thread and sixteen pipelined clients, the cap must have
        # turned clients away; a run with no deferrals did not exercise the FIFO.
        assert_morethan [df_info fastpath_deferrals] $before
        foreach c $clients { $c close }
        df_wait_fastpath_clients 0
        df_assert_healthy
        assert_equal OK [r config set io-batch-inflight 16]
    }

    test {Deferred: a client killed while deferred leaves the FIFO without asserting} {
        assert_equal OK [r config set io-batch-inflight 1]
        set clients [df_open_clients 16 df:b]
        set victim [lindex $clients 0]
        $victim client id
        set vid [$victim read]
        set i 0
        foreach c $clients { for {set j 0} {$j < 100} {incr j} { $c incr df:b$i }; incr i }
        foreach c $clients { $c flush }
        assert_equal 1 [r client kill id $vid]
        set i 0
        foreach c [lrange $clients 1 end] {
            incr i
            for {set j 1} {$j <= 100} {incr j} { assert_equal $j [$c read] }
            assert_equal 100 [r get df:b$i]
        }
        catch {$victim read}
        foreach c $clients { catch {$c close} }
        df_wait_fastpath_clients 0
        df_assert_healthy
        assert_equal OK [r config set io-batch-inflight 16]
    }

    test {Deferred: a client disconnecting while deferred is reclaimed cleanly} {
        assert_equal OK [r config set io-batch-inflight 1]
        set clients [df_open_clients 16 df:c]
        set i 0
        foreach c $clients { for {set j 0} {$j < 100} {incr j} { $c incr df:c$i }; incr i }
        foreach c $clients { $c flush }
        # Half the clients hang up with their reads still queued behind the cap.
        foreach c [lrange $clients 0 7] { $c close }
        set i 8
        foreach c [lrange $clients 8 end] {
            for {set j 1} {$j <= 100} {incr j} { assert_equal $j [$c read] }
            assert_equal 100 [r get df:c$i]
            incr i
        }
        foreach c [lrange $clients 8 end] { $c close }
        df_wait_fastpath_clients 0
        df_assert_healthy
        assert_equal OK [r config set io-batch-inflight 16]
    }

    test {Deferred: handoff with a populated FIFO drains every client} {
        assert_equal OK [r config set io-batch-inflight 1]
        set clients [df_open_clients 16 df:d]
        set i 0
        foreach c $clients { for {set j 0} {$j < 100} {incr j} { $c incr df:d$i }; incr i }
        foreach c $clients { $c flush }
        assert_equal OK [r config set io-threads 1]
        set i 0
        foreach c $clients {
            for {set j 1} {$j <= 100} {incr j} { assert_equal $j [$c read] }
            assert_equal 100 [r get df:d$i]
            incr i
        }
        df_wait_settled
        df_assert_healthy
        foreach c $clients { $c close }
        assert_equal OK [r config set io-batch-inflight 16]
        assert_equal OK [r config set io-threads 2]
        wait_for_condition 500 20 {
            [df_info fastpath_workers_open] == 1
        } else {
            fail "worker did not reopen: [r info fastpath]"
        }
    }
}
