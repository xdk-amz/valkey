# TLS clients are served by the staged fast path: the IO threads read, decrypt, encrypt and write
# for them as they do for TCP clients. Runs only with --tls.

proc tfp_client {} {
    # Sends nothing before its first command, so it stays on the fast path.
    return [valkey [srv 0 host] [srv 0 port] 1 $::tls]
}

proc tfp_info {field} {
    return [getInfoProperty [r info fastpath] $field]
}

# Fast-path clients beyond those already there when the test started (a failed test leaves its own).
proc tfp_wait_clients {n} {
    set want [expr {$::tfp_base + $n}]
    wait_for_condition 100 20 {
        [tfp_info fastpath_clients] == $want
    } else {
        fail "expected $want fast-path clients: [r info fastpath]"
    }
}

proc tfp_resp {args} {
    set cmd "*[llength $args]\r\n"
    foreach a $args { append cmd "$[string length $a]\r\n$a\r\n" }
    return $cmd
}

proc tfp_value {len seed} {
    set chunk [string repeat [format %08x $seed] 128]
    set v [string repeat $chunk [expr {$len / [string length $chunk] + 1}]]
    return [string range $v 0 [expr {$len - 1}]]
}

if {$::tls} {
start_server {tags {"staged-loop tls external:skip"} overrides {io-threads 4 save ""}} {
    set ::tfp_base [tfp_info fastpath_clients]
    test {TLS clients join the fast path} {
        set cs {}
        for {set i 0} {$i < 3} {incr i} {
            set c [tfp_client]
            $c ping
            assert_equal PONG [$c read]
            lappend cs $c
        }
        tfp_wait_clients 3
        set reads [tfp_info fastpath_reads]
        set in [tfp_info fastpath_net_input_bytes]
        foreach c $cs {
            $c set tfp:k:$c v
            assert_equal OK [$c read]
            $c get tfp:k:$c
            assert_equal v [$c read]
        }
        assert_morethan [tfp_info fastpath_reads] $reads
        assert_morethan [tfp_info fastpath_net_input_bytes] $in
        tfp_wait_clients 3
        foreach c $cs { $c close }
        tfp_wait_clients 0
    }

    set ::tfp_base [tfp_info fastpath_clients]
    test {Pipelined TLS commands are read, run and answered on the fast path} {
        set c [tfp_client]
        $c ping
        assert_equal PONG [$c read]
        tfp_wait_clients 1
        set n 500
        set payload ""
        for {set i 0} {$i < $n} {incr i} { append payload [tfp_resp set tfp:p:$i v$i] }
        for {set i 0} {$i < $n} {incr i} { append payload [tfp_resp get tfp:p:$i] }
        set in [tfp_info fastpath_net_input_bytes]
        $c write $payload
        $c flush
        for {set i 0} {$i < $n} {incr i} { assert_equal OK [$c read] }
        for {set i 0} {$i < $n} {incr i} { assert_equal v$i [$c read] }
        assert {[tfp_info fastpath_net_input_bytes] - $in >= [string length $payload]}
        # A pipeline of reads alone is answered by the IO thread that read it.
        set payload ""
        for {set i 0} {$i < $n} {incr i} { append payload [tfp_resp get tfp:p:$i] }
        set spec [tfp_info fastpath_speculated]
        $c write $payload
        $c flush
        for {set i 0} {$i < $n} {incr i} { assert_equal v$i [$c read] }
        assert_morethan [tfp_info fastpath_speculated] $spec
        tfp_wait_clients 1
        $c close
        tfp_wait_clients 0
    }

    set ::tfp_base [tfp_info fastpath_clients]
    test {Large TLS values spanning many records, in both directions} {
        set c [tfp_client]
        $c ping
        assert_equal PONG [$c read]
        tfp_wait_clients 1
        set in [tfp_info fastpath_net_input_bytes]
        set out [tfp_info fastpath_net_output_bytes]
        # One value far past a 16KB record, then a pipeline of several, all in one write.
        set big [tfp_value 1000000 1]
        set payload [tfp_resp set tfp:big $big]
        for {set i 0} {$i < 8} {incr i} { append payload [tfp_resp set tfp:big:$i [tfp_value 200000 [expr {$i + 2}]]] }
        $c write $payload
        $c flush
        for {set i 0} {$i < 9} {incr i} { assert_equal OK [$c read] }
        set payload [tfp_resp get tfp:big]
        for {set i 0} {$i < 8} {incr i} { append payload [tfp_resp get tfp:big:$i] }
        $c write $payload
        $c flush
        assert_equal $big [$c read]
        for {set i 0} {$i < 8} {incr i} { assert_equal [tfp_value 200000 [expr {$i + 2}]] [$c read] }
        assert {[tfp_info fastpath_net_input_bytes] - $in >= 2600000}
        assert {[tfp_info fastpath_net_output_bytes] - $out >= 2600000}
        tfp_wait_clients 1
        $c close
        tfp_wait_clients 0
    }

    set ::tfp_base [tfp_info fastpath_clients]
    test {TLS replies a slow reader cannot take yet are written when it reads} {
        set c [tfp_client]
        # The fast-path client is on db 0, the test client on db 9.
        $c set tfp:slow [tfp_value 1000000 7]
        assert_equal OK [$c read]
        tfp_wait_clients 1
        set out [tfp_info fastpath_net_output_bytes]
        set payload ""
        for {set i 0} {$i < 32} {incr i} { append payload [tfp_resp get tfp:slow] }
        $c write $payload
        $c flush
        # 32MB of replies against kernel buffers of a few MB: writes block until the client reads.
        after 500
        set v [tfp_value 1000000 7]
        for {set i 0} {$i < 32} {incr i} { assert_equal $v [$c read] }
        assert {[tfp_info fastpath_net_output_bytes] - $out >= 32000000}
        $c ping
        assert_equal PONG [$c read]
        tfp_wait_clients 1
        $c close
        tfp_wait_clients 0
    }

    set ::tfp_base [tfp_info fastpath_clients]
    test {A TLS client that runs a main-path command rejoins the fast path} {
        set c [tfp_client]
        $c ping
        assert_equal PONG [$c read]
        tfp_wait_clients 1
        # DBSIZE runs on main: the client leaves, then rejoins once main is done with it.
        $c write "[tfp_resp set tfp:r 1][tfp_resp dbsize][tfp_resp incr tfp:r][tfp_resp get tfp:r]"
        $c flush
        assert_equal OK [$c read]
        assert_morethan [$c read] 0
        assert_equal 2 [$c read]
        assert_equal 2 [$c read]
        tfp_wait_clients 1
        set reads [tfp_info fastpath_reads]
        $c incr tfp:r
        assert_equal 3 [$c read]
        assert_morethan [tfp_info fastpath_reads] $reads
        $c close
        tfp_wait_clients 0
    }

    set ::tfp_base [tfp_info fastpath_clients]
    test {A TLS client closing on the fast path is freed} {
        set before [s connected_clients]
        set cs {}
        for {set i 0} {$i < 4} {incr i} {
            set c [tfp_client]
            $c ping
            assert_equal PONG [$c read]
            lappend cs $c
        }
        tfp_wait_clients 4
        # One closes with a reply still unread and a command half sent.
        set c [lindex $cs 0]
        $c write "[tfp_resp get tfp:slow]*2\r\n\$3\r\nGET"
        $c flush
        foreach c $cs { $c close }
        tfp_wait_clients 0
        wait_for_condition 100 20 {
            [s connected_clients] == $before
        } else {
            fail "TLS clients were not freed: [r client list]"
        }
    }
}
}
