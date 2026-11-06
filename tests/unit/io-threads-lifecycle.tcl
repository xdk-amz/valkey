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

# Every retirement finished.
proc lc_wait_settled {} {
    wait_for_condition 500 20 {
        [lc_retiring] == 0
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

start_server {tags {"io-threads-lifecycle external:skip tls:skip logreqres:skip"} overrides {io-threads 16 io-threads-always-active yes enable-debug-command yes io-threads-epoll-thread no}} {
    assert_equal {io-threads 16} [r config get io-threads]
    r client setname fp-control
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
        # Clients handed off by the previous shrink rejoin the fast path with the new one.
        lc_wait_fastpath_clients 9
        set total 0
        foreach old $::lc_clients {
            $old incr lc:c
            assert_equal [incr total] [$old read]
        }
        assert_equal 9 [lc_fastpath_clients]
        foreach old $::lc_clients { $old close }
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
        # Every client handed back on the way down is on the fast path again.
        lc_wait_fastpath_clients 6
        lc_pipeline_incr $clients lc:d 10
        set i 0
        foreach c $clients {
            for {set j 1} {$j <= 10} {incr j} { assert_equal [expr {$total + $j}] [$c read] }
            assert_equal [expr {$total + 10}] [r get lc:d$i]
            incr i
        }
        assert_equal 6 [lc_fastpath_clients]
        foreach c $clients { $c close }
        lc_wait_fastpath_clients 0
    }

    test {Lifecycle: raising io-threads while workers are still retiring recreates them} {
        # One EXEC runs both changes before any retirement can finish.
        r multi
        r config set io-threads 1
        r config set io-threads 4
        assert_equal {OK OK} [r exec]
        lc_wait_running 3
        lc_wait_settled
        set c [lc_client]
        $c set lc:r v
        assert_equal OK [$c read]
        $c close
        assert_equal OK [r config set io-threads 16]
        lc_wait_running 15
        lc_wait_settled
    }
}

start_server {tags {"io-threads-lifecycle external:skip tls:skip logreqres:skip"} overrides {io-threads 4 io-threads-always-active yes enable-debug-command yes io-threads-epoll-thread no}} {
    assert_equal {io-threads 4} [r config get io-threads]
    r client setname fp-control
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
            # IO threads read only between main's write phases: the pipeline is read after
            # the reconfigure, by whichever path owns the client then, and runs once in order.
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
        set clients [lc_open_counters 8 lc:l]
        lc_pipeline_incr $clients lc:l 200
        assert_equal OK [r config set io-threads 1]
        lc_pipeline_incr $clients lc:l 100
        lc_assert_incr_replies $clients lc:l 300
        lc_wait_settled
        foreach c $clients { $c close }
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
        r client setname fp-control
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

start_server {tags {"io-threads-lifecycle external:skip tls:skip"} overrides {io-threads 1 io-threads-always-active yes enable-debug-command yes}} {
    test {Lifecycle: each IO thread added at runtime gets its own allocator arena, kept across restarts} {
        if {![string match {*jemalloc*} [s mem_allocator]]} {
            return
        }
        set base [r debug mallctl arenas.narenas]
        r config set io-threads 4
        wait_for_condition 500 20 {
            [lc_running] == 3
        } else {
            fail "IO threads did not start: [r info server]"
        }
        r set lc:arena x
        assert_equal [expr {$base + 3}] [r debug mallctl arenas.narenas]
        r config set io-threads 1
        lc_wait_settled
        r config set io-threads 4
        wait_for_condition 500 20 {
            [lc_running] == 3
        } else {
            fail "IO threads did not restart: [r info server]"
        }
        assert_equal [expr {$base + 3}] [r debug mallctl arenas.narenas]
        assert_equal x [r get lc:arena]
    }
}

start_server {tags {"io-threads-lifecycle external:skip tls:skip"} overrides {io-threads 1 io-threads-always-active yes}} {
    r client setname fp-control
    test {Lifecycle: a client whose command is still arriving joins the fast path on scale-up} {
        set fd [socket [srv 0 host] [srv 0 port]]
        fconfigure $fd -translation binary -buffering none
        puts -nonewline $fd "*3\r\n\$3\r\nSET\r\n\$6\r\nlc:mid\r\n\$5\r\nva"
        flush $fd
        wait_for_condition 100 10 {
            [getInfoProperty [r info clients] connected_clients] == 2
        } else {
            fail "partial client not connected"
        }
        after 50
        r config set io-threads 2
        wait_for_condition 500 20 {
            [lc_running] == 1
        } else {
            fail "IO thread did not start: [r info server]"
        }
        wait_for_condition 500 20 {
            [lc_fastpath_clients] == 1
        } else {
            fail "the client with a command in flight did not join the fast path: [r info fastpath]"
        }
        puts -nonewline $fd "lue\r\n"
        flush $fd
        assert_equal "+OK" [string trim [gets $fd]]
        puts -nonewline $fd "*2\r\n\$3\r\nGET\r\n\$6\r\nlc:mid\r\n"
        flush $fd
        assert_equal "\$5" [string trim [gets $fd]]
        assert_equal value [string trim [gets $fd]]
        close $fd
    }
}

proc lc_epoll {} {
    return [getInfoProperty [r info fastpath] staged_epoll_thread]
}

# io-threads-epoll-threads: the collector threads the epoll thread runs as.
proc lc_collectors {} {
    return [lindex [r config get io-threads-epoll-threads] 1]
}

# The smallest io-threads the epoll thread can run at: main, one IO thread and the collectors.
proc lc_epoll_min {} {
    return [expr {2 + [lc_collectors]}]
}

# io-threads N is N threads: main plus N-1 IO threads, or main, N-1-C IO threads and C collector
# threads when the epoll thread runs.
proc lc_wait_budget {io_threads epoll} {
    set running [expr {$io_threads - 1 - ($epoll ? [lc_collectors] : 0)}]
    wait_for_condition 500 20 {
        [lc_running] == $running && [lc_retiring] == 0 && [lc_epoll] == $epoll
    } else {
        fail "expected $running IO threads and epoll thread $epoll: [r info server] [r info fastpath]"
    }
}

start_server {tags {"io-threads-lifecycle external:skip tls:skip logreqres:skip"} overrides {io-threads 4 io-threads-always-active yes}} {
    r client setname fp-control
    r select 0

    if {[lsearch -exact $::global_overrides io-threads-epoll-thread] < 0} {
        test {Thread budget: the epoll thread is off by default} {
            assert_equal {io-threads-epoll-thread no} [r config get io-threads-epoll-thread]
            assert_equal {io-threads-epoll-thread-cpu -1} [r config get io-threads-epoll-thread-cpu]
            lc_wait_budget 4 0
        }
    }

    test {Thread budget: the epoll thread takes the place of one IO thread} {
        assert_equal OK [r config set io-threads-epoll-thread yes]
        lc_wait_budget 4 1
        assert_equal OK [r config set io-threads 5]
        lc_wait_budget 5 1
        assert_equal OK [r config set io-threads-epoll-thread no]
        lc_wait_budget 5 0
        assert_equal OK [r config set io-threads 4]
        lc_wait_budget 4 0
        # The smallest io-threads that can give up IO threads: main, one IO thread and the collectors.
        set min [lc_epoll_min]
        assert_equal OK [r config set io-threads $min]
        assert_equal OK [r config set io-threads-epoll-thread yes]
        lc_wait_budget $min 1
        # Without the fast path the staged loop and its epoll thread do not run.
        assert_equal OK [r config set io-threads-fast-path no]
        lc_wait_budget $min 0
        assert_equal OK [r config set io-threads-fast-path yes]
        lc_wait_budget $min 1
    }

    test {Thread budget: the epoll thread is refused below its smallest io-threads} {
        set min [lc_epoll_min]
        assert_error {*io-threads*or more*} {r config set io-threads [expr {$min - 1}]}
        assert_equal "io-threads $min" [r config get io-threads]
        assert_equal OK [r config set io-threads-epoll-thread no io-threads 2]
        lc_wait_budget 2 0
        assert_error {*io-threads*or more*} {r config set io-threads-epoll-thread yes}
        assert_equal {io-threads-epoll-thread no} [r config get io-threads-epoll-thread]
        assert_equal OK [r config set io-threads $min io-threads-epoll-thread yes]
        lc_wait_budget $min 1
    }

    test {Thread budget: reply order holds while the epoll thread starts and stops} {
        assert_equal OK [r config set io-threads 4]
        set clients [lc_open_counters 8 lc:ep]
        for {set round 0} {$round < 20} {incr round} {
            lc_pipeline_incr $clients lc:ep 50
            assert_equal OK [r config set io-threads-epoll-thread [expr {$round % 2 ? "yes" : "no"}]]
        }
        lc_assert_incr_replies $clients lc:ep 1000
        foreach c $clients { $c close }
    }
}

start_server {tags {"io-threads-lifecycle external:skip tls:skip logreqres:skip"} overrides {io-threads 4 io-threads-always-active yes enable-debug-command yes io-threads-epoll-thread yes}} {
    test {Thread budget: an epoll thread that exits is logged and its IO thread comes back} {
        r client setname fp-control
        r select 0
        lc_wait_budget 4 1
        set lines [count_log_lines 0]
        assert_equal OK [r debug staged-epoll-thread-fail]
        wait_for_log_messages 0 {"*the epoll thread exited (epoll_wait: Input/output error)*"} $lines 100 50
        lc_wait_budget 4 0
        # Failed under this configuration, it stays off: an IO thread holds its slot.
        after 300
        assert_equal 0 [lc_epoll]
        r set lc:ep:k v
        assert_equal v [r get lc:ep:k]
        # A configuration change tries again.
        assert_equal OK [r config set io-threads 5]
        lc_wait_budget 5 1
    }
}

proc lc_thread_tid {pid name} {
    foreach task [glob -nocomplain /proc/$pid/task/*] {
        if {[catch {set comm [string trim [exec cat $task/comm]]}]} continue
        if {$comm eq $name} { return [file tail $task] }
    }
    return {}
}

proc lc_thread_cpus {tid} {
    return [lindex [split [exec taskset -pc $tid] :] end]
}

proc lc_cpu_list {spec} {
    set cpus {}
    foreach part [split [string trim $spec] ,] {
        set range [split $part -]
        for {set c [lindex $range 0]} {$c <= [lindex $range end]} {incr c} { lappend cpus $c }
    }
    return $cpus
}

if {$::tcl_platform(os) eq "Linux" && ![catch {exec taskset -pc [pid]}] && [llength [lc_cpu_list [lc_thread_cpus [pid]]]] >= 2} {
start_server {tags {"io-threads-lifecycle external:skip tls:skip logreqres:skip"} overrides {io-threads 4 io-threads-always-active yes io-threads-epoll-thread yes}} {
    test {Thread budget: io-threads-epoll-thread-cpu pins the epoll thread and refuses a taken CPU} {
        lc_wait_budget 4 1
        set pid [srv 0 pid]
        set allowed [lc_thread_cpus $pid]
        lassign [lc_cpu_list $allowed] cpu_a cpu_b
        assert_error {*not one of the CPUs*} {r config set io-threads-epoll-thread-cpu 1023}
        assert_equal OK [r config set io-threads-epoll-thread-cpu $cpu_a]
        set epoll [lc_thread_tid $pid io_epoll]
        assert {$epoll ne {}}
        wait_for_condition 100 10 {
            [string trim [lc_thread_cpus $epoll]] eq $cpu_a
        } else {
            fail "epoll thread not on CPU $cpu_a: [lc_thread_cpus $epoll]"
        }
        # A CPU an IO thread is pinned to alone is refused, and the setting keeps its value.
        set io1 [lc_thread_tid $pid io_thd_1]
        assert {$io1 ne {}}
        exec taskset -pc $cpu_b $io1
        assert_error {*an I/O thread is pinned to*} {r config set io-threads-epoll-thread-cpu $cpu_b}
        assert_equal "io-threads-epoll-thread-cpu $cpu_a" [r config get io-threads-epoll-thread-cpu]
        exec taskset -pc $allowed $io1
        assert_equal OK [r config set io-threads-epoll-thread-cpu -1]
        assert_equal PONG [r ping]
    }
}
}

start_server {tags {"io-threads-lifecycle external:skip tls:skip"}} {
    test {Thread budget: the server refuses to start with the epoll thread on below io-threads 3} {
        catch {exec $::VALKEY_SERVER_BIN --port 0 --io-threads 2 --io-threads-epoll-thread yes 2>@1} msg
        assert_match {*FATAL CONFIG*io-threads 3 or more*} $msg
    }
}
