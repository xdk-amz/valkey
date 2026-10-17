# Replies sent during the write phase: with one command per batch, workers send each batch's replies as
# soon as main has executed it, including the batches main read itself. Every client's replies must arrive
# in order and match what it ran, also when clients disconnect mid-pipeline and when commands are handed
# back to main while their batches are being sent.

proc sd_socket {} {
    set fd [socket [srv 0 host] [srv 0 port]]
    fconfigure $fd -translation binary -blocking 1 -buffering full
    return $fd
}

proc sd_resp {args} {
    set cmd "*[llength $args]\r\n"
    foreach a $args { append cmd "$[string length $a]\r\n$a\r\n" }
    return $cmd
}

proc sd_read_reply {fd} {
    set line [string trimright [gets $fd] "\r"]
    if {[string index $line 0] eq {$}} {
        if {$line eq {$-1}} { return {(nil)} }
        set v [read $fd [expr {[string range $line 1 end] + 2}]]
        return [string range $v 0 end-2]
    }
    return $line
}

proc sd_fastpath_clients {} {
    return [getInfoProperty [r info fastpath] fastpath_clients]
}

# n raw db-0 clients, each on the fast path.
proc sd_open {n prefix} {
    set fds {}
    for {set i 0} {$i < $n} {incr i} {
        set fd [sd_socket]
        puts -nonewline $fd [sd_resp set $prefix:init:$i 1]
        flush $fd
        assert_equal {+OK} [sd_read_reply $fd]
        lappend fds $fd
    }
    wait_for_condition 200 25 {
        [sd_fastpath_clients] >= $n
    } else {
        fail "expected $n fast-path clients: [r info fastpath]"
    }
    return $fds
}

# Each client pipelines rounds of six commands on its own keys plus a shared counter.
proc sd_send_rounds {fds prefix rounds} {
    set i 0
    foreach fd $fds {
        set p ""
        for {set r 0} {$r < $rounds} {incr r} {
            append p [sd_resp incr $prefix:c$i] [sd_resp set $prefix:s$i v$i.$r] [sd_resp get $prefix:s$i] \
                [sd_resp incr $prefix:shared] [sd_resp append $prefix:a$i x] [sd_resp strlen $prefix:a$i]
        }
        puts -nonewline $fd $p
        flush $fd
        incr i
    }
}

proc sd_check_rounds {fd i rounds} {
    for {set r 1} {$r <= $rounds} {incr r} {
        assert_equal ":$r" [sd_read_reply $fd]
        assert_equal {+OK} [sd_read_reply $fd]
        assert_equal v$i.[expr {$r - 1}] [sd_read_reply $fd]
        assert_match {:[0-9]*} [sd_read_reply $fd]
        assert_equal ":$r" [sd_read_reply $fd]
        assert_equal ":$r" [sd_read_reply $fd]
    }
}

start_server {tags {"staged-loop external:skip tls:skip logreqres:skip"} overrides {io-threads 3 io-batch-commands 1}} {
    r client setname sd-control
    r select 0

    test {Write-phase delivery: one-command batches keep every client's replies in order} {
        set n 24
        set rounds 200
        set fds [sd_open $n sd:a]
        sd_send_rounds $fds sd:a $rounds
        set i 0
        foreach fd $fds {
            sd_check_rounds $fd $i $rounds
            incr i
        }
        assert_equal [expr {$n * $rounds}] [r get sd:a:shared]
        foreach fd $fds { close $fd }
        wait_for_condition 200 25 {[sd_fastpath_clients] == 0} else { fail "clients still on the fast path" }
    }

    test {Write-phase delivery: clients closing mid-pipeline do not disturb the others} {
        set n 24
        set rounds 150
        set fds [sd_open $n sd:b]
        sd_send_rounds $fds sd:b $rounds
        set kept {}
        set i 0
        foreach fd $fds {
            if {$i % 2} { close $fd } else { lappend kept [list $i $fd] }
            incr i
        }
        foreach item $kept {
            lassign $item i fd
            sd_check_rounds $fd $i $rounds
            close $fd
        }
        wait_for_condition 200 25 {[sd_fastpath_clients] == 0} else { fail "clients still on the fast path" }
        assert_equal {PONG} [r ping]
        # A closed client's commands that were read ran in order: a cut round may have run its INCR only.
        for {set i 1} {$i < $n} {incr i 2} {
            set c [r get sd:b:c$i]
            if {$c eq {}} continue
            assert_range [expr {$c - [r strlen sd:b:a$i]}] 0 1
        }
    }

    test {Write-phase delivery: commands handed back to main mid-delivery run in order after a pause} {
        set n 12
        set rounds 100
        set fds [sd_open $n sd:p]
        r client pause 300 write
        sd_send_rounds $fds sd:p $rounds
        set i 0
        foreach fd $fds {
            sd_check_rounds $fd $i $rounds
            incr i
        }
        assert_equal [expr {$n * $rounds}] [r get sd:p:shared]
        foreach fd $fds { close $fd }
    }
}
