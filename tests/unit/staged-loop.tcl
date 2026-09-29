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

start_server {tags {"staged-loop external:skip tls:skip"} overrides {io-threads 3 enable-debug-command yes}} {
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
