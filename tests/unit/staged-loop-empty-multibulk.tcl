# Fast-path reads that contain an empty multibulk (a count of zero or less).

proc ss_socket {} {
    set fd [socket [srv 0 host] [srv 0 port]]
    fconfigure $fd -translation binary -blocking 1 -buffering full
    return $fd
}

proc ss_resp {args} {
    set cmd "*[llength $args]\r\n"
    foreach a $args { append cmd "$[string length $a]\r\n$a\r\n" }
    return $cmd
}

# One reply of a simple type, a bulk string, a null, or a flat array of those.
proc ss_read_reply {fd} {
    set line [string trimright [gets $fd] "\r"]
    switch -- [string index $line 0] {
        {$} {
            if {$line eq {$-1}} { return {(nil)} }
            set n [string range $line 1 end]
            set v [read $fd [expr {$n + 2}]]
            return [string range $v 0 end-2]
        }
        {*} - {>} {
            set out {}
            for {set i 0} {$i < [string range $line 1 end]} {incr i} { lappend out [ss_read_reply $fd] }
            return $out
        }
        default { return $line }
    }
}

proc ss_read_replies {fd n} {
    set out {}
    for {set i 0} {$i < $n} {incr i} { lappend out [ss_read_reply $fd] }
    return $out
}

proc ss_fastpath_clients {} {
    return [getInfoProperty [r info fastpath] fastpath_clients]
}

proc ss_wait_fastpath_clients {n} {
    wait_for_condition 200 25 {
        [ss_fastpath_clients] == $n
    } else {
        fail "expected $n fast-path clients: [r info fastpath]"
    }
}

# A raw client that ran one command on the fast path.
proc ss_fp_client {key} {
    set fd [ss_socket]
    puts -nonewline $fd [ss_resp set $key 1]
    flush $fd
    assert_equal {+OK} [ss_read_reply $fd]
    return $fd
}

proc ss_open {n prefix} {
    set fds {}
    for {set i 0} {$i < $n} {incr i} { lappend fds [ss_fp_client $prefix:$i] }
    wait_for_condition 200 25 {
        [ss_fastpath_clients] >= $n
    } else {
        fail "expected $n fast-path clients: [r info fastpath]"
    }
    return $fds
}

# Every client pipelines rounds of INCR on its own counter and INCR on a shared one; checks the
# per-client sequence. start is the counter value the client last saw.
proc ss_incr_round {fds prefix start rounds} {
    set i 0
    foreach fd $fds {
        set p ""
        for {set r 0} {$r < $rounds} {incr r} { append p [ss_resp incr $prefix:c$i] [ss_resp incr $prefix:shared] }
        puts -nonewline $fd $p
        flush $fd
        incr i
    }
    set i 0
    foreach fd $fds {
        for {set r 1} {$r <= $rounds} {incr r} {
            assert_equal ":[expr {$start + $r}]" [ss_read_reply $fd]
            assert_match {:[0-9]*} [ss_read_reply $fd]
        }
        incr i
    }
}

start_server {tags {"staged-loop external:skip tls:skip"} overrides {io-threads 2}} {
    r select 0

    # An empty multibulk (count <= 0) is skipped and the commands around it still run, wherever
    # it sits in the read and whatever protocol follows it.
    foreach {name payload key want} [list \
        lead-neg    "*-10\r\n*2\r\n\$4\r\nincr\r\n\$4\r\nem:a\r\n" em:a 1 \
        lead-zero   "*0\r\n*2\r\n\$4\r\nincr\r\n\$4\r\nem:b\r\n" em:b 1 \
        mid-neg     "*2\r\n\$4\r\nincr\r\n\$4\r\nem:c\r\n*-1\r\n*2\r\n\$4\r\nincr\r\n\$4\r\nem:c\r\n" em:c 2 \
        then-inline "*-1\r\nincr em:d\r\n" em:d 1 \
        mixed       "*-10\r\n*0\r\n*2\r\n\$4\r\nincr\r\n\$4\r\nem:e\r\n*-1\r\nincr em:e\r\n" em:e 2] {
        test "Staged: an empty multibulk does not stall a fast-path read ($name)" {
            set fd [ss_socket]
            puts -nonewline $fd [ss_resp set em:warm 1]
            flush $fd
            assert_equal {+OK} [ss_read_reply $fd]
            puts -nonewline $fd $payload
            flush $fd
            for {set i 1} {$i <= $want} {incr i} { assert_equal :$i [ss_read_reply $fd] }
            assert_equal $want [r get $key]
            close $fd
        }
    }
}
