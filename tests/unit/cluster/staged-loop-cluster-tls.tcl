# CLU-06: with tls-cluster yes and a plain TCP port open, a plain-TCP client must get MOVED and ASK
# with the plain TCP port of the target node (stock: shouldReturnTlsInfo looks at the client's
# connection). Fast-path clients may be plain TCP or TLS, and their redirects are built on the
# executor client, whose conn is NULL, so shouldReturnTlsInfo follows the origin client's conn_type.
# This file only runs with --tls.

proc sct_socket {idx} {
    set fd [socket [srv $idx host] [srv $idx pport]]
    fconfigure $fd -translation binary -blocking 1 -buffering none
    return $fd
}

proc sct_resp {args} {
    set cmd "*[llength $args]\r\n"
    foreach a $args { append cmd "$[string length $a]\r\n$a\r\n" }
    return $cmd
}

proc sct_read_raw {fd} {
    set line [gets $fd]
    append line "\n"
    if {[string index $line 0] eq "$"} {
        set n [string range [string trimright $line "\r\n"] 1 end]
        if {$n >= 0} { append line [read $fd [expr {$n + 2}]] }
    }
    return $line
}

proc sct_pipeline_raw {fd cmds} {
    set out ""
    foreach c $cmds { append out [sct_resp {*}$c] }
    puts -nonewline $fd $out
    set replies {}
    foreach c $cmds { lappend replies [sct_read_raw $fd] }
    return $replies
}

proc sct_info {idx section field} {
    return [getInfoProperty [[srv $idx client] info $section] $field]
}

proc sct_fast_client {idx} {
    set before [sct_info $idx fastpath fastpath_clients]
    set fd [sct_socket $idx]
    assert_equal "+PONG\r\n" [lindex [sct_pipeline_raw $fd {{PING}}] 0]
    wait_for_condition 100 20 {[sct_info $idx fastpath fastpath_clients] > $before} else {
        fail "plain TCP client did not join the fast path"
    }
    return $fd
}

proc sct_main_client {idx} {
    set fd [sct_socket $idx]
    assert_equal "+OK\r\n" [lindex [sct_pipeline_raw $fd {{CLIENT SETNAME mainpath}}] 0]
    return $fd
}

proc sct_key_on {idx prefix} {
    set myid [[srv $idx client] cluster myid]
    for {set i 0} {$i < 10000} {incr i} {
        set key "$prefix$i"
        set slot [[srv $idx client] cluster keyslot $key]
        foreach n [get_cluster_nodes [expr {abs($idx)}]] {
            foreach r [dict get $n slots] {
                set lo [lindex [split $r -] 0]
                set hi [lindex [split $r -] end]
                if {[string is integer -strict $lo] && $slot >= $lo && $slot <= $hi && [dict get $n id] eq $myid} {
                    return $key
                }
            }
        }
    }
    fail "no key found on node $idx"
}

if {!$::tls} {
    puts "staged-loop-cluster-tls: needs --tls (a TLS build with tls-cluster yes); skipped"
} else {
start_cluster 2 0 {tags {external:skip cluster staged-loop logreqres:skip} overrides {io-threads 5 io-threads-parallel-writes-min 1 cluster-slot-stats-enabled no}} {

    test {TLS cluster: staged loop runs and plain TCP ports are open} {
        wait_for_cluster_state ok
        foreach idx {0 -1} {
            wait_for_log_messages $idx {"*Staged event loop started*"} 0 100 50
            [srv $idx client] config set notify-keyspace-events ""
        }
        assert {[srv -1 pport] != [srv -1 port]}
    }

    set local [sct_key_on 0 "local"]
    set remote [sct_key_on -1 "remote"]
    set tcp1 [srv -1 pport]
    set tls1 [srv -1 port]

    test {TLS cluster: a plain TCP main-path client gets MOVED with the plain TCP port} {
        set main [sct_main_client 0]
        set r [sct_pipeline_raw $main [list [list GET $remote] [list SET $remote x]]]
        close $main
        foreach x $r {
            assert_match "-MOVED * 127.0.0.1:$tcp1\r\n" $x
        }
    }

    test {TLS cluster: a plain TCP fast-path client gets MOVED with the plain TCP port (lanes and fast GET)} {
        set lane0 [sct_info 0 fastpath fastpath_write_lane_entries]
        set spec0 [sct_info 0 fastget fastget_speculated_commands]
        set fast [sct_fast_client 0]
        set main [sct_main_client 0]
        set cmds {}
        for {set i 0} {$i < 5} {incr i} { lappend cmds [list SET $local v] [list SET $remote x] [list GET $remote] }
        for {set i 0} {$i < 5} {incr i} { lappend cmds [list GET $local] [list GET $remote] }
        set rf [sct_pipeline_raw $fast $cmds]
        set rm [sct_pipeline_raw $main $cmds]
        close $fast
        close $main
        assert {[sct_info 0 fastpath fastpath_write_lane_entries] > $lane0}
        assert {[sct_info 0 fastget fastget_speculated_commands] > $spec0}
        for {set i 0} {$i < [llength $cmds]} {incr i} {
            if {[lindex $rf $i] ne [lindex $rm $i]} {
                fail "reply $i differs for [lindex $cmds $i]: fast {[lindex $rf $i]} main {[lindex $rm $i]}"
            }
        }
        foreach idx {1 2 16} {
            assert_match "-MOVED * 127.0.0.1:$tcp1\r\n" [lindex $rf $idx]
        }
    }

    test {TLS cluster: a plain TCP fast-path client gets ASK with the plain TCP port} {
        set src [srv 0 client]
        set dst [srv -1 client]
        set missing "{$local}missing"
        set slot [$src cluster keyslot $missing]
        $src del $missing
        assert_equal OK [$dst cluster setslot $slot importing [$src cluster myid]]
        assert_equal OK [$src cluster setslot $slot migrating [$dst cluster myid]]
        set fast [sct_fast_client 0]
        set main [sct_main_client 0]
        set cmds [list [list GET $missing] [list SET $missing x] [list GET $missing]]
        set rf [sct_pipeline_raw $fast $cmds]
        set rm [sct_pipeline_raw $main $cmds]
        close $fast
        close $main
        assert_equal OK [$src cluster setslot $slot stable]
        assert_equal OK [$dst cluster setslot $slot stable]
        foreach x $rm { assert_equal "-ASK $slot 127.0.0.1:$tcp1\r\n" $x }
        foreach x $rf { assert_equal "-ASK $slot 127.0.0.1:$tcp1\r\n" $x }
    }
}
}
