# Staged event loop reply streaming (staged-reply-overlap): the STREAM phase delivers completed
# runs while main still executes. These tests exercise the config none|exec|loop under pipelined
# load, exact per-connection reply order, teardown (CLIENT KILL and output-buffer-limit) while
# replies stream, appendfsync always with the config on, and flipping the mode mid-load.
# Every fast-path client is a raw db-0 socket; the framework's SELECT would move it off the path.

proc sls_fastpath_clients {} {
    return [getInfoProperty [r info fastpath] fastpath_clients]
}

proc sls_wait_fastpath_clients {n} {
    wait_for_condition 200 25 {
        [sls_fastpath_clients] == $n
    } else {
        fail "expected $n fast-path clients: [r info fastpath]"
    }
}

proc sls_socket {} {
    set fd [socket [srv 0 host] [srv 0 port]]
    fconfigure $fd -translation binary -blocking 1 -buffering full
    return $fd
}

proc sls_resp {args} {
    set cmd "*[llength $args]\r\n"
    foreach a $args { append cmd "$[string length $a]\r\n$a\r\n" }
    return $cmd
}

proc sls_read_reply {fd} {
    if {[gets $fd line] < 0} { error "EOF" }
    set line [string trimright $line "\r"]
    switch -- [string index $line 0] {
        {$} {
            if {$line eq {$-1}} { return {(nil)} }
            set n [string range $line 1 end]
            set v [read $fd [expr {$n + 2}]]
            if {[string length $v] != $n + 2} { error "EOF" }
            return [string range $v 0 end-2]
        }
        {*} {
            if {$line eq {*-1}} { return {(nil)} }
            set out {}
            for {set i 0} {$i < [string range $line 1 end]} {incr i} { lappend out [sls_read_reply $fd] }
            return $out
        }
        default { return $line }
    }
}

proc sls_read_replies {fd n} {
    set out {}
    for {set i 0} {$i < $n} {incr i} { lappend out [sls_read_reply $fd] }
    return $out
}

# A raw socket that ran one command and sits on the fast path.
proc sls_fp_client {} {
    set before [sls_fastpath_clients]
    set fd [sls_socket]
    puts -nonewline $fd [sls_resp set sls:probe 1]
    flush $fd
    assert_equal {+OK} [sls_read_reply $fd]
    wait_for_condition 200 25 {
        [sls_fastpath_clients] >= $before + 1
    } else {
        fail "the socket did not join the fast path: [r info fastpath]"
    }
    return $fd
}

proc sls_open {n} {
    set fds {}
    for {set i 0} {$i < $n} {incr i} { lappend fds [sls_fp_client] }
    return $fds
}

start_server {tags {"staged-loop external:skip tls:skip"} overrides {io-threads 3 enable-debug-command yes}} {
    r client setname sls-control
    r select 0

    # Each mode drives the same workload: 32 connections, each INCR its own key depth-16 pipelined,
    # many rounds. The reply sequence of each connection must be exactly 1,2,3,... with no gap,
    # reorder or duplicate, whether or not replies stream while main executes.
    foreach mode {none exec loop} {
        test "Stream $mode: pipelined per-key INCR keep exact per-connection sequence (P16)" {
            r config set staged-reply-overlap $mode
            r flushall
            r config resetstat
            set n 32
            set fds [sls_open $n]
            set rounds 40
            set depth 16
            set base 0
            for {set round 0} {$round < $rounds} {incr round} {
                set i 0
                foreach fd $fds {
                    set p ""
                    for {set k 0} {$k < $depth} {incr k} { append p [sls_resp incr sls:seq:$i] }
                    puts -nonewline $fd $p
                    flush $fd
                    incr i
                }
                set i 0
                foreach fd $fds {
                    for {set k 1} {$k <= $depth} {incr k} {
                        assert_equal ":[expr {$base + $k}]" [sls_read_reply $fd]
                    }
                    incr i
                }
                incr base $depth
            }
            for {set i 0} {$i < $n} {incr i} { assert_equal [expr {$rounds * $depth}] [r get sls:seq:$i] }
            assert_equal $n [sls_fastpath_clients]
            # The streaming modes opened STREAM phases during the workload; none did not. Read before
            # closing the clients so the number reflects only this test's traffic.
            set phases [getInfoProperty [r info fastpath] staged_stream_phases]
            if {$mode eq {none}} {
                assert_equal 0 $phases
            } else {
                assert {$phases > 0}
            }
            foreach fd $fds { close $fd }
            sls_wait_fastpath_clients 0
        }
    }

    # A reader-heavy mix at P16: GETs stream behind the writers' INCRs. Each reader's view of its
    # own counter is monotone, and the final values are exact, in exec and loop.
    foreach mode {exec loop} {
        test "Stream $mode: readers streaming behind writers stay monotone and exact" {
            r config set staged-reply-overlap $mode
            r flushall
            set writers [sls_open 8]
            set readers [sls_open 8]
            r set sls:mono 0
            for {set round 0} {$round < 25} {incr round} {
                foreach fd $writers {
                    set p ""
                    for {set k 0} {$k < 64} {incr k} { append p [sls_resp incr sls:mono] }
                    puts -nonewline $fd $p
                    flush $fd
                }
                foreach fd $readers {
                    set p ""
                    for {set k 0} {$k < 128} {incr k} { append p [sls_resp get sls:mono] }
                    puts -nonewline $fd $p
                    flush $fd
                }
                foreach fd $writers { sls_read_replies $fd 64 }
                foreach fd $readers {
                    set last -1
                    foreach v [sls_read_replies $fd 128] {
                        assert {$v >= $last}
                        set last $v
                    }
                }
            }
            assert_equal [expr {8 * 25 * 64}] [r get sls:mono]
            foreach fd [concat $writers $readers] { close $fd }
            sls_wait_fastpath_clients 0
            r config resetstat
        }
    }

    # Flip the mode (none<->exec<->loop) between every in-flight burst. A CONFIG SET of the mode is
    # read at the next iteration boundary; replies in flight across the change stay exact.
    test {Stream: CONFIG SET of the mode under load keeps every reply exact} {
        r flushall
        set n 16
        set fds [sls_open $n]
        set modes {exec loop none exec none loop}
        set base 0
        set depth 20
        set mi 0
        for {set round 0} {$round < 48} {incr round} {
            r config set staged-reply-overlap [lindex $modes [expr {$mi % [llength $modes]}]]
            incr mi
            set i 0
            foreach fd $fds {
                set p ""
                for {set k 0} {$k < $depth} {incr k} { append p [sls_resp incr sls:cfg:$i] }
                puts -nonewline $fd $p
                flush $fd
                incr i
            }
            set i 0
            foreach fd $fds {
                for {set k 1} {$k <= $depth} {incr k} {
                    assert_equal ":[expr {$base + $k}]" [sls_read_reply $fd]
                }
                incr i
            }
            incr base $depth
        }
        for {set i 0} {$i < $n} {incr i} { assert_equal $base [r get sls:cfg:$i] }
        foreach fd $fds { close $fd }
        sls_wait_fastpath_clients 0
        r config set staged-reply-overlap exec
    }

    # CLIENT KILL while a large burst streams: the killed clients vanish, the survivors stay exact.
    foreach mode {exec loop} {
        test "Stream $mode: CLIENT KILL while replies stream leaves others exact" {
            r config set staged-reply-overlap $mode
            r flushall
            r acl setuser sls-victim on >pw ~* &* +@all
            set keep [sls_open 4]
            set seen 0
            for {set round 0} {$round < 10} {incr round} {
                set victims {}
                for {set i 0} {$i < 6} {incr i} {
                    set fd [sls_socket]
                    puts -nonewline $fd [sls_resp auth sls-victim pw]
                    flush $fd
                    assert_equal {+OK} [sls_read_reply $fd]
                    puts -nonewline $fd [sls_resp set sls:vp 1]
                    flush $fd
                    assert_equal {+OK} [sls_read_reply $fd]
                    lappend victims $fd
                }
                foreach fd $victims {
                    set p ""
                    for {set k 0} {$k < 3000} {incr k} { append p [sls_resp incr sls:kv] [sls_resp get sls:absent] }
                    puts -nonewline $fd $p
                    flush $fd
                }
                foreach fd $keep {
                    set p ""
                    for {set k 0} {$k < 200} {incr k} { append p [sls_resp incr sls:kk:$fd] }
                    puts -nonewline $fd $p
                    flush $fd
                }
                assert_equal 6 [r client kill user sls-victim]
                foreach fd $keep {
                    for {set k 1} {$k <= 200} {incr k} { assert_equal ":[expr {$seen + $k}]" [sls_read_reply $fd] }
                }
                incr seen 200
                foreach fd $victims { catch {close $fd} }
            }
            sls_wait_fastpath_clients 4
            foreach fd $keep { close $fd }
            sls_wait_fastpath_clients 0
            r acl deluser sls-victim
            assert_equal PONG [r ping]
            r config resetstat
        }
    }

    # An output-buffer-limit kill while replies stream closes the offender; the others are exact.
    foreach mode {exec loop} {
        test "Stream $mode: output buffer limit disconnects a streaming client, others unaffected" {
            r config set staged-reply-overlap $mode
            r flushall
            r config set client-output-buffer-limit "normal 1mb 0 0"
            r set sls:ob [string repeat x 65536]
            set closed [getInfoProperty [r info stats] client_output_buffer_limit_disconnections]
            set keep [sls_open 3]
            set fd [sls_fp_client]
            set p ""
            for {set k 0} {$k < 600} {incr k} { append p [sls_resp incr sls:ob:n] [sls_resp get sls:ob] }
            fconfigure $fd -blocking 0
            catch {
                puts -nonewline $fd $p
                flush $fd
            }
            wait_for_condition 200 25 {
                [getInfoProperty [r info stats] client_output_buffer_limit_disconnections] == $closed + 1
            } else {
                fail "output buffer limit did not disconnect: [r client list]"
            }
            foreach k $keep {
                puts -nonewline $k [sls_resp incr sls:ob:keep:$k]
                flush $k
                assert_equal {:1} [sls_read_reply $k]
            }
            sls_wait_fastpath_clients 3
            catch {close $fd}
            foreach k $keep { close $k }
            r config set client-output-buffer-limit "normal 0 0 0"
            sls_wait_fastpath_clients 0
            r config resetstat
        }
    }
}

# appendfsync always: a reply must not leave before its write reaches the AOF, even with streaming
# requested. Streaming is disabled in that configuration (eligibility excludes it), so the base
# reply-after-fsync ordering holds; this test asserts it directly with the config on.
start_server {tags {"staged-loop external:skip tls:skip"} overrides {io-threads 3 appendonly yes appendfsync always enable-debug-command yes}} {
    r select 0

    foreach mode {exec loop} {
        test "Stream $mode: appendfsync always never replies before the write reaches the AOF" {
            r config set staged-reply-overlap $mode
            r flushall
            set fd [sls_fp_client]
            for {set i 0} {$i < 10} {incr i} {
                r debug aof-flush-sleep 0
                r setrange sls:af:big [expr {int(rand() * 5000000) + 5000000}] x
                r debug aof-flush-sleep 300000
                set aof [get_last_incr_aof_path r]
                set size1 [file size $aof]
                puts -nonewline $fd "[sls_resp get sls:af:big][sls_resp incr sls:af:n]"
                flush $fd
                sls_read_reply $fd
                assert_equal ":[expr {$i + 1}]" [sls_read_reply $fd]
                assert {[file size $aof] > $size1}
            }
            r debug aof-flush-sleep 0
            close $fd
            sls_wait_fastpath_clients 0
        }
    }
}
