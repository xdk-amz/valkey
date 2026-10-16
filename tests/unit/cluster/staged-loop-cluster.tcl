# Staged event loop in cluster mode: fast-path clients get the same redirects and cluster errors as
# main-path clients, for commands main executes and for GETs answered on IO threads. Fast-path
# clients are raw sockets: they send nothing before their commands, so they join the fast path.

proc scl_socket {idx} {
    set fd [socket [srv $idx host] [srv $idx port]]
    fconfigure $fd -translation binary -blocking 1 -buffering none
    return $fd
}

proc scl_resp {args} {
    set cmd "*[llength $args]\r\n"
    foreach a $args { append cmd "$[string length $a]\r\n$a\r\n" }
    return $cmd
}

# One RESP2 reply: simple strings, errors and integers as their line, bulks as their payload.
proc scl_read {fd} {
    set line [string trimright [gets $fd] "\r"]
    switch -- [string index $line 0] {
        {$} {
            set n [string range $line 1 end]
            if {$n < 0} { return {(nil)} }
            set v [read $fd [expr {$n + 2}]]
            return [string range $v 0 end-2]
        }
        default { return $line }
    }
}

proc scl_pipeline {fd cmds} {
    set out ""
    foreach c $cmds { append out [scl_resp {*}$c] }
    puts -nonewline $fd $out
    set replies {}
    foreach c $cmds { lappend replies [scl_read $fd] }
    return $replies
}

proc scl_fastpath_clients {idx} {
    return [getInfoProperty [[srv $idx client] info fastpath] fastpath_clients]
}

proc scl_speculated {idx} {
    return [getInfoProperty [[srv $idx client] info fastget] fastget_speculated_commands]
}

# A key with hash tag $tag on a slot node $idx serves.
proc scl_key_on {idx prefix} {
    set myid [[srv $idx client] cluster myid]
    for {set i 0} {$i < 10000} {incr i} {
        set key "$prefix$i"
        set slot [[srv $idx client] cluster keyslot $key]
        set owner [dict get [cluster_get_node_by_slot [expr {-$idx}] $slot] id]
        if {$owner eq $myid} { return $key }
    }
    fail "no key found on node $idx"
}

proc cluster_get_node_by_slot {idx slot} {
    foreach n [get_cluster_nodes $idx] {
        foreach r [dict get $n slots] {
            set lo [lindex [split $r -] 0]
            set hi [lindex [split $r -] end]
            if {[string is integer -strict $lo] && $slot >= $lo && $slot <= $hi} { return $n }
        }
    }
    fail "slot $slot not served"
}

start_cluster 2 1 {tags {external:skip cluster staged-loop tls:skip logreqres:skip} overrides {io-threads 3 cluster-slot-stats-enabled no}} {

    test {Staged loop runs in cluster mode} {
        wait_for_cluster_state ok
        foreach idx {0 -1 -2} {
            wait_for_log_messages $idx {"*Staged event loop started*"} 0 100 50
        }
    }

    set local [scl_key_on 0 "fpl"]
    set remote [scl_key_on -1 "fpr"]
    set port1 [srv -1 port]

    test {Fast path: a pipeline of local and remote keys gets values and MOVED in order} {
        set fd [scl_socket 0]
        assert_equal +PONG [lindex [scl_pipeline $fd {{PING}}] 0]
        wait_for_condition 100 20 {[scl_fastpath_clients 0] >= 1} else { fail "client did not join the fast path" }
        set r [scl_pipeline $fd [list [list SET $local v1] [list GET $local] [list GET $remote] [list SET $remote x] \
                                       [list GET $local]]]
        assert_equal {+OK} [lindex $r 0]
        assert_equal v1 [lindex $r 1]
        assert_match "-MOVED * *:$port1" [lindex $r 2]
        assert_match "-MOVED * *:$port1" [lindex $r 3]
        assert_equal v1 [lindex $r 4]
        assert {[scl_fastpath_clients 0] >= 1}
        close $fd
    }

    test {Fast path: GETs answered on IO threads in cluster mode, remote keys redirected} {
        [srv 0 client] set $local v2
        set fd [scl_socket 0]
        assert_equal +PONG [lindex [scl_pipeline $fd {{PING}}] 0]
        set before [scl_speculated 0]
        set cmds {}
        for {set i 0} {$i < 10} {incr i} { lappend cmds [list GET $local] }
        foreach r [scl_pipeline $fd $cmds] { assert_equal v2 $r }
        assert {[scl_speculated 0] > $before}
        set cmds {}
        for {set i 0} {$i < 10} {incr i} { lappend cmds [list GET $local] [list GET $remote] }
        set i 0
        foreach r [scl_pipeline $fd $cmds] {
            if {$i % 2 == 0} { assert_equal v2 $r } else { assert_match "-MOVED * *:$port1" $r }
            incr i
        }
        close $fd
    }

    test {Fast path on a replica: GET is MOVED until READONLY, answered after it, MOVED after READWRITE} {
        set primary [srv 0 client]
        set replica [srv -2 client]
        $primary set $local onreplica
        wait_for_ofs_sync $primary $replica
        set fd [scl_socket -2]
        assert_match {-MOVED *} [lindex [scl_pipeline $fd [list [list GET $local]]] 0]
        assert_equal {+OK} [lindex [scl_pipeline $fd {{READONLY}}] 0]
        wait_for_condition 100 20 {[scl_fastpath_clients -2] >= 1} else { fail "replica client did not rejoin the fast path" }
        set before [scl_speculated -2]
        set cmds {}
        for {set i 0} {$i < 10} {incr i} { lappend cmds [list GET $local] }
        foreach r [scl_pipeline $fd $cmds] { assert_equal onreplica $r }
        assert {[scl_speculated -2] > $before}
        # Reads main executes for a fast-path client keep its READONLY too.
        $primary hset "{$local}h" f hv
        wait_for_ofs_sync $primary $replica
        assert_equal hv [lindex [scl_pipeline $fd [list [list HGET "{$local}h" f]]] 0]
        assert_match {-MOVED *} [lindex [scl_pipeline $fd [list [list SET $local w]]] 0]
        assert_equal {+OK} [lindex [scl_pipeline $fd {{READWRITE}}] 0]
        set r [scl_pipeline $fd [list [list GET $local] [list GET $local]]]
        foreach x $r { assert_match {-MOVED *} $x }
        close $fd
    }

    test {Fast path on a slot being migrated: present keys served, missing keys ASK, ASKING on the target} {
        set src [srv 0 client]
        set dst [srv -1 client]
        set present "{$local}present"
        set missing "{$local}missing"
        set slot [$src cluster keyslot $present]
        $src set $present here
        $src del $missing
        set src_id [$src cluster myid]
        set dst_id [$dst cluster myid]
        assert_equal OK [$dst cluster setslot $slot importing $src_id]
        assert_equal OK [$src cluster setslot $slot migrating $dst_id]

        set fd [scl_socket 0]
        assert_equal +PONG [lindex [scl_pipeline $fd {{PING}}] 0]
        wait_for_condition 100 20 {[scl_fastpath_clients 0] >= 1} else { fail "client did not join the fast path" }
        set r [scl_pipeline $fd [list [list GET $present] [list GET $missing] [list SET $present again] \
                                       [list SET $missing x] [list GET $present]]]
        assert_equal here [lindex $r 0]
        assert_equal "-ASK $slot 127.0.0.1:$port1" [lindex $r 1]
        assert_equal {+OK} [lindex $r 2]
        assert_equal "-ASK $slot 127.0.0.1:$port1" [lindex $r 3]
        assert_equal again [lindex $r 4]
        close $fd

        set fd [scl_socket -1]
        assert_match {-MOVED *} [lindex [scl_pipeline $fd [list [list GET $missing]]] 0]
        set r [scl_pipeline $fd [list {ASKING} [list GET $missing] [list GET $missing]]]
        assert_equal {+OK} [lindex $r 0]
        assert_equal {(nil)} [lindex $r 1]
        assert_match {-MOVED *} [lindex $r 2]
        close $fd

        assert_equal OK [$src cluster setslot $slot stable]
        assert_equal OK [$dst cluster setslot $slot stable]
    }

    test {Fast path: CROSSSLOT for keys on different slots} {
        set fd [scl_socket 0]
        set r [scl_pipeline $fd [list [list MSET "{a}1" x "{b}2" y] [list MGET "{a}1" "{b}2"]]]
        foreach x $r { assert_match {-CROSSSLOT *} $x }
        close $fd
    }

    test {Fast path: CLUSTER SLOT-STATS counts every fast-path GET} {
        set p [srv 0 client]
        $p config set cluster-slot-stats-enabled yes
        $p config resetstat
        $p set $local counted
        set slot [$p cluster keyslot $local]
        set fd [scl_socket 0]
        assert_equal +PONG [lindex [scl_pipeline $fd {{PING}}] 0]
        wait_for_condition 100 20 {[scl_fastpath_clients 0] >= 1} else { fail "client did not join the fast path" }
        set cmds {}
        for {set i 0} {$i < 20} {incr i} { lappend cmds [list GET $local] }
        foreach r [scl_pipeline $fd $cmds] { assert_equal counted $r }
        set stats [lindex [$p cluster slot-stats slotsrange $slot $slot] 0 1]
        assert {[dict get $stats network-bytes-in] >= 20 * [string length [scl_resp GET $local]]}
        assert {[dict get $stats network-bytes-out] >= 20 * [string length "\$7\r\ncounted\r\n"]}
        $p config set cluster-slot-stats-enabled no
        close $fd
    }
}
