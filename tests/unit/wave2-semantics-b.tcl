# Wave 2 semantics tests, part B: item 21.
# Same conventions as wave2-semantics.tcl: fast-path clients are unnamed raw sockets on db 0,
# control clients select db 0 and keep a name, notify-keyspace-events is cleared at runtime,
# and every test asserts that an INFO fastpath counter moved.

set ::w6b_open {}

proc w6b_socket {host port} {
    set fd [socket $host $port]
    fconfigure $fd -translation binary -blocking 1 -buffering full
    lappend ::w6b_open $fd
    return $fd
}

proc w6b_resp {args} {
    set cmd "*[llength $args]\r\n"
    foreach a $args { append cmd "$[string length $a]\r\n$a\r\n" }
    return $cmd
}

proc w6b_line {fd} {
    set line ""
    while {1} {
        set ch [read $fd 1]
        if {$ch eq ""} { error "connection closed" }
        if {$ch eq "\n"} { return [string range $line 0 end-1] }
        append line $ch
    }
}

proc w6b_read_reply {fd} {
    set line [w6b_line $fd]
    switch -- [string index $line 0] {
        {$} {
            if {$line eq {$-1}} { return {(nil)} }
            set data [read $fd [expr {[string range $line 1 end] + 2}]]
            return [string range $data 0 end-2]
        }
        {*} {
            set n [string range $line 1 end]
            if {$n < 0} { return {(nil)} }
            set out {}
            for {set i 0} {$i < $n} {incr i} { lappend out [w6b_read_reply $fd] }
            return $out
        }
        {:} { return [string range $line 1 end] }
        default { return $line }
    }
}

proc w6b_read_replies {fd n} {
    set out {}
    for {set i 0} {$i < $n} {incr i} { lappend out [w6b_read_reply $fd] }
    return $out
}

proc w6b_send {fd buf} {
    puts -nonewline $fd $buf
    flush $fd
}

proc w6b_info {c field} {
    return [getInfoProperty [$c info fastpath] $field]
}

proc w6b_stat {c field} {
    return [getInfoProperty [$c info stats] $field]
}

proc w6b_control {c} {
    $c client setname w6-control
    $c select 0
    $c config set notify-keyspace-events {}
}

# ---------------------------------------------------------------------------------------------
# Item 21 (EV-07): maxmemory-eviction-batch yes, noeviction, the server is over maxmemory.
# FLUSHALL SYNC and a SET in the same read: the SET must succeed, as it does with batch off
# and on stock. The cached OOM verdict must not outlive the memory that caused it.
# ---------------------------------------------------------------------------------------------
proc w6b_evict_case {host port batch} {
    r flushall sync
    r config set maxmemory 0
    r config set maxmemory-policy noeviction
    r config set maxmemory-eviction-batch $batch
    set fd [w6b_socket $host $port]
    set b0 [w6b_info r fastpath_batches]
    set buf ""
    set v [string repeat x 1000]
    for {set i 0} {$i < 20000} {incr i} { append buf [w6b_resp set fill:$i $v] }
    w6b_send $fd $buf
    w6b_read_replies $fd 20000
    assert_morethan [w6b_info r fastpath_batches] $b0
    r config set maxmemory [expr {[s used_memory] / 2}]
    # one round trip while over the limit, so the OOM verdict is computed and cached
    w6b_send $fd [w6b_resp set probe v]
    assert_match {*OOM*} [w6b_read_reply $fd]
    w6b_send $fd "[w6b_resp flushall sync][w6b_resp set after v]"
    set got [w6b_read_replies $fd 2]
    close $fd
    r config set maxmemory 0
    return $got
}

start_server {tags {"wave2 external:skip tls:skip"} overrides {io-threads 5 keyspace-shard-bits 10 io-threads-parallel-writes-min 1 save {}}} {
    w6b_control r
    set host [srv 0 host]
    set port [srv 0 port]

    test {Wave2 #21: noeviction, batch off: FLUSHALL then SET in the same read succeeds (baseline)} {
        assert_equal {+OK +OK} [w6b_evict_case $host $port no]
        assert_equal v [r get after]
    }

    test {Wave2 #21: noeviction, maxmemory-eviction-batch yes: FLUSHALL then SET in the same read succeeds} {
        assert_equal {+OK +OK} [w6b_evict_case $host $port yes]
        assert_equal v [r get after]
    }
    r config set maxmemory-eviction-batch no
}
