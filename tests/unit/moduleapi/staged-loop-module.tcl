# Staged event loop: MODULE LOAD and UNLOAD while fast-path clients pipeline the module command.
proc slo_fastpath_clients {} {
    return [getInfoProperty [r info fastpath] fastpath_clients]
}

proc slo_wait_fastpath_clients {n} {
    wait_for_condition 200 25 {
        [slo_fastpath_clients] == $n
    } else {
        fail "expected $n fast-path clients: [r info fastpath]"
    }
}

proc slo_socket {{host {}} {port {}}} {
    if {$host eq {}} { set host [srv 0 host] }
    if {$port eq {}} { set port [srv 0 port] }
    set fd [socket $host $port]
    fconfigure $fd -translation binary -blocking 1 -buffering full
    return $fd
}

proc slo_resp {args} {
    set cmd "*[llength $args]\r\n"
    foreach a $args { append cmd "$[string length $a]\r\n$a\r\n" }
    return $cmd
}

# One reply of a simple type, a bulk string, a null, or an array of those. Raises on EOF.
proc slo_read_reply {fd} {
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
            for {set i 0} {$i < [string range $line 1 end]} {incr i} { lappend out [slo_read_reply $fd] }
            return $out
        }
        default { return $line }
    }
}

proc slo_read_replies {fd n} {
    set out {}
    for {set i 0} {$i < $n} {incr i} { lappend out [slo_read_reply $fd] }
    return $out
}

# A raw socket that authenticated (optionally), ran one command and sits on the fast path.
proc slo_fp_client {{auth {}}} {
    set before [slo_fastpath_clients]
    set fd [slo_socket]
    if {$auth ne {}} {
        puts -nonewline $fd [slo_resp auth {*}$auth]
        flush $fd
        assert_equal {+OK} [slo_read_reply $fd]
    }
    puts -nonewline $fd [slo_resp set slo:probe 1]
    flush $fd
    assert_equal {+OK} [slo_read_reply $fd]
    wait_for_condition 200 25 {
        [slo_fastpath_clients] >= $before + 1
    } else {
        fail "the socket did not join the fast path: [r info fastpath]"
    }
    return $fd
}

proc slo_open {n {auth {}}} {
    set fds {}
    for {set i 0} {$i < $n} {incr i} { lappend fds [slo_fp_client $auth] }
    return $fds
}

# Writes payload in random-sized pieces with a flush after each, so commands straddle reads.
proc slo_send_chunked {fd payload} {
    set len [string length $payload]
    set pos 0
    while {$pos < $len} {
        set n [expr {1 + int(rand() * 700)}]
        puts -nonewline $fd [string range $payload $pos [expr {$pos + $n - 1}]]
        flush $fd
        incr pos $n
        if {rand() < 0.05} { after 1 }
    }
}

# The value stored under version ver of a large key: a version prefix then one repeated byte.
proc slo_big_value {ver size} {
    set prefix "V$ver:"
    set ch [string index "abcdefghijklmnop" [expr {$ver % 16}]]
    return "$prefix[string repeat $ch [expr {$size - [string length $prefix]}]]"
}

# A reply to GET of the large key is valid if it is null or exactly one of the states a single
# version passes through: as set, after APPEND, after APPEND and SETRANGE.
proc slo_check_big_reply {reply size} {
    if {$reply eq {(nil)}} return
    if {![regexp {^V(\d+):} $reply -> ver]} {
        fail "large reply has no version prefix: [string range $reply 0 40]... ([string length $reply] bytes)"
    }
    set s0 [slo_big_value $ver $size]
    set s1 "$s0!!!!"
    set mid [expr {$size / 2}]
    set s2 [string replace $s1 $mid [expr {$mid + 3}] ZZZZ]
    if {$reply ne $s0 && $reply ne $s1 && $reply ne $s2} {
        set first -1
        for {set i 0} {$i < [string length $reply]} {incr i} {
            if {[string index $reply $i] ne [string index $s0 $i]} { set first $i; break }
        }
        fail "large reply corrupted: version $ver, [string length $reply] bytes, first difference at $first"
    }
}

start_server {tags {"modules staged-loop external:skip tls:skip"} overrides {io-threads 3 enable-debug-command yes}} {
    r client setname slo-control
    r select 0

    test {Staged overlap: MODULE LOAD and UNLOAD while clients pipeline its command and core commands} {
        set module [file normalize tests/modules/zset.so]
        r del slo:mz
        r zadd slo:mz 0 a 0 b 0 c
        # Module clients mix the module command with core commands; core clients send only core
        # commands and stay on the fast path while the command table changes.
        set fds [slo_open 3]
        set core [slo_open 3]
        set ctl [valkey_deferring_client]
        $ctl select 0
        $ctl read
        set loaded 0
        set counts {0 0 0}
        set core_counts {0 0 0}
        for {set round 0} {$round < 30} {incr round} {
            set i 0
            foreach fd $fds {
                set p ""
                for {set k 0} {$k < 100} {incr k} {
                    append p [slo_resp zset.rangebylex slo:mz - +] [slo_resp incr slo:mc:$i] [slo_resp get slo:mc:$i]
                }
                puts -nonewline $fd $p
                flush $fd
                incr i
            }
            set i 0
            foreach fd $core {
                set p ""
                for {set k 0} {$k < 200} {incr k} { append p [slo_resp incr slo:mk:$i] [slo_resp get slo:mk:$i] }
                puts -nonewline $fd $p
                flush $fd
                incr i
            }
            if {$loaded} { $ctl module unload zset } else { $ctl module load $module }
            set i 0
            foreach fd $fds {
                set c [lindex $counts $i]
                for {set k 0} {$k < 100} {incr k} {
                    set got [slo_read_replies $fd 3]
                    set z [lindex $got 0]
                    if {$z ne {a b c} && ![string match {-ERR unknown command*} $z]} {
                        fail "unexpected module command reply: $z"
                    }
                    incr c
                    assert_equal [list ":$c" $c] [lrange $got 1 2]
                }
                lset counts $i $c
                incr i
            }
            set i 0
            foreach fd $core {
                set c [lindex $core_counts $i]
                for {set k 0} {$k < 200} {incr k} {
                    incr c
                    assert_equal [list ":$c" $c] [slo_read_replies $fd 2]
                }
                lset core_counts $i $c
                incr i
            }
            assert_equal OK [$ctl read]
            set loaded [expr {!$loaded}]
            # A command sent after the acknowledgement sees the new command table.
            foreach fd $fds {
                puts -nonewline $fd [slo_resp zset.rangebylex slo:mz - +]
                flush $fd
                set z [slo_read_reply $fd]
                if {$loaded} {
                    assert_equal {a b c} $z
                } else {
                    assert_match {-ERR unknown command*} $z
                }
            }
        }
        if {$loaded} { r module unload zset }
        $ctl close
        assert {[slo_fastpath_clients] >= [llength $core]}
        foreach fd [concat $fds $core] { close $fd }
        slo_wait_fastpath_clients 0
    }
}
