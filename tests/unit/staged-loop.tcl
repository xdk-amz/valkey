# Staged event loop: per-client command order across the read and write phases, many
# clients sharing IO threads, teardown mid-pipeline, and fast-path replies while main is
# blocked. Every fast-path client here is a raw db-0 socket: the test framework's client
# sends SELECT first, which moves a connection off the fast path.

proc sl_fastpath_clients {} {
    return [getInfoProperty [r info fastpath] fastpath_clients]
}

proc sl_socket {} {
    set fd [socket [srv 0 host] [srv 0 port]]
    fconfigure $fd -translation {crlf binary} -blocking 1 -buffering full
    return $fd
}

proc sl_resp {args} {
    set cmd "*[llength $args]\r\n"
    foreach a $args { append cmd "$[string length $a]\r\n$a\r\n" }
    return $cmd
}

# Reads one RESP reply of a simple type, a bulk string or a null, as one Tcl value.
proc sl_read_reply {fd} {
    set line [gets $fd]
    switch -- [string index $line 0] {
        {$} {
            if {$line eq {$-1}} { return {(nil)} }
            return [gets $fd]
        }
        default { return $line }
    }
}

proc sl_read_replies {fd n} {
    set out {}
    for {set i 0} {$i < $n} {incr i} { lappend out [sl_read_reply $fd] }
    return $out
}

proc sl_wait_fastpath_clients {n} {
    wait_for_condition 200 20 {
        [sl_fastpath_clients] == $n
    } else {
        fail "expected $n fast-path clients: [r info fastpath]"
    }
}

start_server {tags {"staged-loop external:skip tls:skip logreqres:skip"} overrides {io-threads 3 enable-debug-command yes}} {
    r client setname sl-control
    r select 0

    test {Staged loop: reads before the first write run in order with the writes behind them} {
        set fd [sl_socket]
        puts -nonewline $fd [sl_resp set sl:k old]
        flush $fd
        assert_equal {+OK} [sl_read_reply $fd]
        sl_wait_fastpath_clients 1
        # GET, SET, GET in one packet: the leading GET runs on the IO thread, the SET and
        # the GET behind it on main; replies keep the order the commands were sent.
        puts -nonewline $fd "[sl_resp get sl:k][sl_resp set sl:k new][sl_resp get sl:k]"
        flush $fd
        assert_equal {old +OK new} [sl_read_replies $fd 3]
        # A longer mixed pipeline, reads after writes included.
        set payload ""
        set expect {}
        for {set i 0} {$i < 50} {incr i} {
            append payload [sl_resp get sl:k] [sl_resp set sl:k v$i] [sl_resp get sl:k] [sl_resp incr sl:n]
            lappend expect [expr {$i == 0 ? "new" : "v[expr {$i - 1}]"}] {+OK} v$i ":[expr {$i + 1}]"
        }
        puts -nonewline $fd $payload
        flush $fd
        assert_equal $expect [sl_read_replies $fd 200]
        assert_equal 1 [sl_fastpath_clients]
        close $fd
        sl_wait_fastpath_clients 0
    }

    test {Staged loop: many clients mixing reads and writes each see their own order} {
        set n 24
        set fds {}
        for {set c 0} {$c < $n} {incr c} {
            set fd [sl_socket]
            puts -nonewline $fd [sl_resp set sl:c$c 0]
            flush $fd
            assert_equal {+OK} [sl_read_reply $fd]
            lappend fds $fd
        }
        sl_wait_fastpath_clients $n
        # Every client pipelines its rounds in one write before any reply is read, so the
        # IO threads claim many ready clients per read phase and batch across them.
        set rounds 100
        for {set c 0} {$c < $n} {incr c} {
            set payload ""
            for {set i 0} {$i < $rounds} {incr i} {
                append payload [sl_resp get sl:c$c] [sl_resp incr sl:c$c] [sl_resp get sl:shared] [sl_resp get sl:c$c]
            }
            puts -nonewline [lindex $fds $c] $payload
            flush [lindex $fds $c]
        }
        for {set c 0} {$c < $n} {incr c} {
            set fd [lindex $fds $c]
            for {set i 0} {$i < $rounds} {incr i} {
                set got [sl_read_replies $fd 4]
                assert_equal [list $i ":[expr {$i + 1}]" {(nil)} [expr {$i + 1}]] $got
            }
        }
        for {set c 0} {$c < $n} {incr c} { assert_equal $rounds [r get sl:c$c] }
        foreach fd $fds { close $fd }
        sl_wait_fastpath_clients 0
    }

    test {Staged loop: a client closing mid-pipeline is torn down without disturbing others} {
        set keep [sl_socket]
        puts -nonewline $keep [sl_resp set sl:keep 0]
        flush $keep
        assert_equal {+OK} [sl_read_reply $keep]
        for {set round 0} {$round < 10} {incr round} {
            set fd [sl_socket]
            set payload ""
            for {set i 0} {$i < 2000} {incr i} { append payload [sl_resp incr sl:gone] }
            puts -nonewline $fd $payload
            flush $fd
            close $fd
            puts -nonewline $keep [sl_resp incr sl:keep]
            flush $keep
            assert_equal ":[expr {$round + 1}]" [sl_read_reply $keep]
        }
        sl_wait_fastpath_clients 1
        assert {[r get sl:gone] <= 20000}
        assert_equal 10 [r get sl:keep]
        close $keep
        sl_wait_fastpath_clients 0
        assert_equal PONG [r ping]
    }

    test {Staged loop: a pipeline longer than one read is served in full with no further input} {
        set fd [sl_socket]
        puts -nonewline $fd [sl_resp set sl:big:k v]
        flush $fd
        assert_equal {+OK} [sl_read_reply $fd]
        sl_wait_fastpath_clients 1
        # Hundreds of KB in one burst, then silence: every read after the first is driven by the
        # poller, not by a new readiness edge.
        set n 20000
        set payload ""
        for {set i 0} {$i < $n} {incr i} { append payload [sl_resp incr sl:big:n] }
        for {set i 0} {$i < 2000} {incr i} { append payload [sl_resp get sl:big:k] }
        set bigval [string repeat y 300000]
        append payload [sl_resp set sl:big:v $bigval] [sl_resp strlen sl:big:v]
        puts -nonewline $fd $payload
        flush $fd
        set replies [sl_read_replies $fd [expr {$n + 2002}]]
        assert_equal ":$n" [lindex $replies [expr {$n - 1}]]
        assert_equal v [lindex $replies [expr {$n + 1999}]]
        assert_equal {+OK} [lindex $replies end-1]
        assert_equal {:300000} [lindex $replies end]
        assert_equal 1 [sl_fastpath_clients]
        close $fd
        sl_wait_fastpath_clients 0
    }

    test {Staged loop: replies larger than the socket buffer are completed on writable events} {
        r set sl:wide [string repeat z 1000000]
        set fd [sl_socket]
        puts -nonewline $fd [sl_resp set sl:wide:x 1]
        flush $fd
        assert_equal {+OK} [sl_read_reply $fd]
        sl_wait_fastpath_clients 1
        # 16 MB of replies to a peer that does not read yet: the server's writes stop short, and
        # the rest goes out only as the socket drains.
        set payload ""
        for {set i 0} {$i < 8} {incr i} { append payload [sl_resp get sl:wide] }
        append payload [sl_resp incr sl:wide:x]
        for {set i 0} {$i < 8} {incr i} { append payload [sl_resp get sl:wide] }
        puts -nonewline $fd $payload
        flush $fd
        after 300
        set replies [sl_read_replies $fd 17]
        for {set i 0} {$i < 17} {incr i} {
            if {$i == 8} {
                assert_equal {:2} [lindex $replies $i]
            } else {
                assert_equal 1000000 [string length [lindex $replies $i]]
            }
        }
        assert_equal 1 [sl_fastpath_clients]
        puts -nonewline $fd [sl_resp get sl:wide:x]
        flush $fd
        assert_equal 2 [sl_read_reply $fd]
        close $fd
        sl_wait_fastpath_clients 0
    }

    test {Staged loop: fast-path clients get BUSY while a script blocks main} {
        r config set busy-reply-threshold 100
        set fd [sl_socket]
        puts -nonewline $fd [sl_resp set sl:busy v]
        flush $fd
        assert_equal {+OK} [sl_read_reply $fd]
        sl_wait_fastpath_clients 1
        set rd [valkey_deferring_client]
        $rd eval {while true do end} 0
        after 300
        puts -nonewline $fd "[sl_resp get sl:busy][sl_resp set sl:busy w]"
        flush $fd
        set replies [sl_read_replies $fd 2]
        assert_match {-BUSY*} [lindex $replies 0]
        assert_match {-BUSY*} [lindex $replies 1]
        r script kill
        assert_error {*killed*} {$rd read}
        $rd close
        puts -nonewline $fd [sl_resp get sl:busy]
        flush $fd
        assert_equal {v} [sl_read_reply $fd]
        close $fd
        r config set busy-reply-threshold 5000
    }
}

# Thread id of the server thread named $name, or "" when there is none.
proc sl_thread_tid {pid name} {
    foreach dir [glob -nocomplain /proc/$pid/task/*] {
        if {[catch {open $dir/comm} f]} continue
        set comm [string trim [read $f]]
        close $f
        if {$comm eq $name} { return [file tail $dir] }
    }
    return ""
}

# CPU list a thread may run on, as taskset prints it ("pid N's current affinity list: 0-7").
proc sl_affinity {tid} {
    return [lindex [exec taskset -pc $tid] end]
}

if {$::tcl_platform(os) eq "Linux" && ![catch {exec taskset -pc [pid]}]} {
start_server {tags {"staged-loop external:skip tls:skip logreqres:skip"} overrides {io-threads 3}} {
    test {Staged loop: main and an IO thread pinned to its CPU sleep at the barrier instead of spinning} {
        set pid [srv 0 pid]
        set io1 [sl_thread_tid $pid io_thd_1]
        assert {$io1 ne ""}
        set main_cpus [sl_affinity $pid]
        set io1_cpus [sl_affinity $io1]
        set cpu [lindex [split [lindex [split $main_cpus ,] 0] -] 0]
        set loglines [count_log_lines 0]
        exec taskset -pc $cpu $pid
        exec taskset -pc $cpu $io1
        wait_for_log_messages 0 {"*1 IO thread is pinned to main's CPU*sleep at the barrier*"} $loglines 100 50
        # Fast-path reads and writes still complete, in order, while neither spins.
        set fd [sl_socket]
        set payload ""
        set expect {}
        for {set i 0} {$i < 200} {incr i} {
            append payload [sl_resp set sl:pin:$i v$i] [sl_resp get sl:pin:$i]
            lappend expect {+OK} v$i
        }
        puts -nonewline $fd $payload
        flush $fd
        assert_equal $expect [sl_read_replies $fd 400]
        close $fd
        set loglines [count_log_lines 0]
        exec taskset -pc $io1_cpus $io1
        exec taskset -pc $main_cpus $pid
        wait_for_log_messages 0 {"*no IO thread is pinned to main's CPU*"} $loglines 100 50
    }

    test {Staged loop: main takes its share of each phase again once its CPU is its own} {
        set pid [srv 0 pid]
        set io1 [sl_thread_tid $pid io_thd_1]
        set io2 [sl_thread_tid $pid io_thd_2]
        set main_cpus [sl_affinity $pid]
        set io1_cpus [sl_affinity $io1]
        set io2_cpus [sl_affinity $io2]
        set cpus {}
        foreach range [split $main_cpus ,] {
            lassign [split $range -] lo hi
            if {$hi eq ""} { set hi $lo }
            for {set c $lo} {$c <= $hi} {incr c} { lappend cpus $c }
        }
        if {[llength $cpus] < 2} {
            puts "skipped: the server may run on only one CPU"
        } else {
            lassign $cpus a b
            # Pinned after start, as a benchmark harness pins: main and both IO threads share CPU a.
            set loglines [count_log_lines 0]
            exec taskset -pc $a $pid
            exec taskset -pc $a $io1
            exec taskset -pc $a $io2
            wait_for_log_messages 0 {"*main shares a CPU with an IO thread and leaves the phases*"} $loglines 100 50
            set loglines [count_log_lines 0]
            exec taskset -pc $b $io1
            exec taskset -pc $b $io2
            wait_for_log_messages 0 {"*main works a share of each phase*"} $loglines 100 50
            set fd [sl_socket]
            set payload ""
            set expect {}
            for {set i 0} {$i < 200} {incr i} {
                append payload [sl_resp set sl:lane:$i v$i] [sl_resp get sl:lane:$i]
                lappend expect {+OK} v$i
            }
            puts -nonewline $fd $payload
            flush $fd
            assert_equal $expect [sl_read_replies $fd 400]
            close $fd
            exec taskset -pc $io2_cpus $io2
            exec taskset -pc $io1_cpus $io1
            exec taskset -pc $main_cpus $pid
        }
    }
}
}

# The crash report's memory test cancels and joins every IO thread; staged workers wait on the barrier.
tags {"valgrind:skip"} {
start_server {tags {"staged-loop external:skip tls:skip"} overrides {io-threads 4 enable-debug-command yes crash-memcheck-enabled yes}} {
    test {Staged loop: a crash with several workers at the barrier finishes the report and exits} {
        r client setname sl-control
        r select 0
        set fd [sl_socket]
        puts -nonewline $fd [sl_resp set sl:crash v]
        flush $fd
        assert_equal {+OK} [sl_read_reply $fd]
        sl_wait_fastpath_clients 1
        set pid [srv 0 pid]
        catch {r debug segfault}
        wait_for_condition 300 100 {
            ![is_alive $pid]
        } else {
            fail "server did not exit after the crash report"
        }
        assert_equal 1 [count_log_message 0 "Fast memory test PASSED"]
        close $fd
    }
}
}
