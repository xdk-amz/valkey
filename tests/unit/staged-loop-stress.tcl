# Staged event loop under stress: raw db-0 clients (the framework client SELECTs 9 and leaves
# the fast path) mixing writes and reads on shared keys, kills and disconnects under load,
# features that move a client off the fast path, replication, eviction, big values, idle
# timeouts, reload, and IO thread count changes.

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

start_server {tags {"staged-loop external:skip tls:skip logreqres:skip"} overrides {io-threads 3 enable-debug-command yes}} {
    r client setname ss-control
    r select 0

    test {Staged stress: interleaved writes and reads on shared keys keep per-client order and totals} {
        set n 16
        set fds [ss_open $n ss:a]
        set rounds 60
        set depth 6
        for {set r 0} {$r < $rounds} {incr r} {
            set i 0
            foreach fd $fds {
                set p ""
                for {set k 0} {$k < $depth} {incr k} {
                    set v v$i.$r.$k
                    append p [ss_resp incr ss:shared] [ss_resp set ss:own$i $v] [ss_resp get ss:own$i] \
                        [ss_resp incr ss:cnt$i] [ss_resp expire ss:own$i 100] [ss_resp del ss:tmp$i] \
                        [ss_resp get ss:sk] [ss_resp set ss:sk $v] [ss_resp get ss:own$i]
                }
                puts -nonewline $fd $p
                flush $fd
                incr i
            }
            set i 0
            foreach fd $fds {
                for {set k 0} {$k < $depth} {incr k} {
                    set v v$i.$r.$k
                    set got [ss_read_replies $fd 9]
                    assert_match {:[0-9]*} [lindex $got 0]
                    assert_equal [list {+OK} $v ":[expr {$r * $depth + $k + 1}]" {:1} {:0}] [lrange $got 1 5]
                    assert_equal [list {+OK} $v] [lrange $got 7 8]
                }
                incr i
            }
        }
        assert_equal [expr {$n * $rounds * $depth}] [r get ss:shared]
        assert_equal $n [ss_fastpath_clients]
        foreach fd $fds { close $fd }
        ss_wait_fastpath_clients 0
    }

    test {Staged stress: CLIENT KILL of fast-path clients under load drops only them} {
        set keep [ss_open 4 ss:k]
        set seen 0
        for {set round 0} {$round < 10} {incr round} {
            set victims {}
            for {set v 0} {$v < 4} {incr v} {
                set fd [ss_fp_client ss:victim]
                set p ""
                for {set i 0} {$i < 500} {incr i} { append p [ss_resp incr ss:victim] }
                puts -nonewline $fd $p
                flush $fd
                lappend victims $fd
            }
            foreach fd $victims {
                set port [lindex [fconfigure $fd -sockname] 2]
                catch {r client kill addr 127.0.0.1:$port}
            }
            foreach fd $victims { close $fd }
            ss_incr_round $keep ss:k $seen 20
            incr seen 20
        }
        ss_wait_fastpath_clients 4
        foreach fd $keep { close $fd }
        ss_wait_fastpath_clients 0
        assert_equal PONG [r ping]
    }

    test {Staged stress: BUSY replies while main is blocked, then a normal service} {
        set fds [ss_open 4 ss:b]
        r config set busy-reply-threshold 50
        set rd [valkey_deferring_client]
        $rd eval {while true do end} 0
        after 200
        foreach fd $fds {
            puts -nonewline $fd "[ss_resp incr ss:b][ss_resp get ss:b]"
            flush $fd
        }
        foreach fd $fds { foreach reply [ss_read_replies $fd 2] { assert_match {-BUSY*} $reply } }
        set k [valkey_client]
        $k script kill
        $k close
        catch {$rd read}
        $rd close
        ss_incr_round $fds ss:b 0 50
        foreach fd $fds { close $fd }
        ss_wait_fastpath_clients 0
    }

    test {Staged stress: MULTI, SUBSCRIBE and CLIENT TRACKING move a fast-path client off it} {
        set fd [ss_fp_client ss:m]
        ss_wait_fastpath_clients 1
        puts -nonewline $fd "[ss_resp get ss:m][ss_resp multi][ss_resp incr ss:m][ss_resp get ss:m][ss_resp exec][ss_resp get ss:m]"
        flush $fd
        assert_equal {1 +OK +QUEUED +QUEUED {:2 2} 2} [ss_read_replies $fd 6]
        close $fd

        set fd [ss_fp_client ss:p]
        ss_wait_fastpath_clients 1
        puts -nonewline $fd "[ss_resp get ss:p][ss_resp subscribe ss:ch]"
        flush $fd
        assert_equal {1 {subscribe ss:ch :1}} [ss_read_replies $fd 2]
        assert_equal 1 [r publish ss:ch hello]
        assert_equal {message ss:ch hello} [ss_read_reply $fd]
        close $fd

        set fd [ss_fp_client ss:t]
        ss_wait_fastpath_clients 1
        puts -nonewline $fd "[ss_resp client tracking on][ss_resp get ss:t]"
        flush $fd
        assert_equal {+OK 1} [ss_read_replies $fd 2]
        set rd [valkey_client]
        $rd select 0
        $rd set ss:t 2
        $rd close
        # RESP2 invalidations need a redirect; the connection must still answer in order.
        puts -nonewline $fd [ss_resp get ss:t]
        flush $fd
        assert_equal 2 [ss_read_reply $fd]
        close $fd
        ss_wait_fastpath_clients 0
    }

    test {Staged stress: BLPOP on main is woken by a fast-path LPUSH} {
        set rd [valkey_deferring_client]
        $rd select 0
        $rd read
        $rd blpop ss:list 5
        wait_for_blocked_clients_count 1
        set fd [ss_fp_client ss:l]
        ss_wait_fastpath_clients 1
        puts -nonewline $fd [ss_resp lpush ss:list hello]
        flush $fd
        assert_equal {:1} [ss_read_reply $fd]
        assert_equal {ss:list hello} [$rd read]
        $rd close
        close $fd
        ss_wait_fastpath_clients 0
    }

    test {Staged stress: MONITOR sees fast-path commands} {
        set mon [valkey_deferring_client]
        $mon monitor
        assert_equal OK [$mon read]
        set fd [ss_fp_client ss:mon]
        puts -nonewline $fd "[ss_resp set ss:mon1 v][ss_resp get ss:mon1]"
        flush $fd
        assert_equal {+OK v} [ss_read_replies $fd 2]
        set seen {}
        while {[llength $seen] < 3} {
            set line [$mon read]
            if {[string match {*"ss:mon*} $line]} { lappend seen $line }
        }
        assert_match {*"set" "ss:mon"*} [lindex $seen 0]
        assert_match {*"get" "ss:mon1"*} [lindex $seen 2]
        $mon close
        close $fd
    }

    test {Staged stress: 64KiB to 4MiB values round-trip on several fast-path clients} {
        set fds [ss_open 4 ss:bv]
        foreach size {65536 65537 262144 1048576 4194304} {
            set i 0
            foreach fd $fds {
                set v [string repeat [format %c [expr {65 + $i}]] $size]
                puts -nonewline $fd "[ss_resp set ss:bv$i $v][ss_resp get ss:bv$i][ss_resp strlen ss:bv$i]"
                flush $fd
                incr i
            }
            set i 0
            foreach fd $fds {
                set v [string repeat [format %c [expr {65 + $i}]] $size]
                assert_equal {+OK} [ss_read_reply $fd]
                assert {[ss_read_reply $fd] eq $v}
                assert_equal ":$size" [ss_read_reply $fd]
                incr i
            }
        }
        assert_equal 4 [ss_fastpath_clients]
        foreach fd $fds { close $fd }
        ss_wait_fastpath_clients 0
    }

    test {Staged stress: maxmemory evicts under fast-path writes} {
        r flushall sync
        r config set maxmemory-policy allkeys-lru
        set limit [expr {[s used_memory] + 4 * 1024 * 1024}]
        r config set maxmemory $limit
        set fds [ss_open 4 ss:mm]
        set val [string repeat x 1000]
        for {set r 0} {$r < 40} {incr r} {
            set i 0
            foreach fd $fds {
                set p ""
                for {set k 0} {$k < 200} {incr k} { append p [ss_resp set ss:mm:$i:$r:$k $val] }
                puts -nonewline $fd $p
                flush $fd
                incr i
            }
            foreach fd $fds { for {set k 0} {$k < 200} {incr k} { assert_equal {+OK} [ss_read_reply $fd] } }
        }
        assert {[s evicted_keys] > 0}
        assert {[s used_memory] < $limit + 1024 * 1024}
        r config set maxmemory 0
        foreach fd $fds { close $fd }
        ss_wait_fastpath_clients 0
        r flushall
    }

    test {Staged stress: the idle timeout closes an idle fast-path client} {
        set fd [ss_fp_client ss:to]
        ss_wait_fastpath_clients 1
        r config set timeout 1
        # The control client must not idle out itself.
        wait_for_condition 100 50 {
            [ss_fastpath_clients] == 0
        } else {
            r config set timeout 0
            close $fd
            fail "idle fast-path client was not closed"
        }
        r config set timeout 0
        puts -nonewline $fd [ss_resp ping]
        catch {flush $fd}
        assert_equal {} [gets $fd]
        assert {[eof $fd]}
        close $fd
    }

    test {Staged stress: DEBUG RELOAD with fast-path clients pipelining} {
        r flushall
        r debug populate 100000 ss:rl 10
        set fds [ss_open 4 ss:r]
        set expect {}
        foreach fd $fds {
            set p ""
            for {set k 0} {$k < 200} {incr k} { append p [ss_resp incr ss:rl:n] }
            puts -nonewline $fd $p
            flush $fd
        }
        assert_equal OK [r debug reload]
        set ok 0
        foreach fd $fds {
            for {set k 0} {$k < 200} {incr k} {
                set reply [ss_read_reply $fd]
                if {[string match {:*} $reply]} { incr ok } else { assert_match {-LOADING*} $reply }
            }
        }
        assert_equal $ok [r get ss:rl:n]
        assert {[r dbsize] >= 100000}
        ss_incr_round $fds ss:r 0 10
        foreach fd $fds { close $fd }
        ss_wait_fastpath_clients 0
        r flushall
    }
}

start_server {tags {"staged-loop external:skip tls:skip logreqres:skip"} overrides {io-threads 3}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]
    start_server {overrides {io-threads 3}} {
        set replica [srv 0 client]
        test {Staged stress: fast-path writes replicate} {
            $replica replicaof $primary_host $primary_port
            wait_for_condition 100 100 {
                [s 0 master_link_status] eq {up}
            } else {
                fail "replica did not connect"
            }
            set fds {}
            for {set i 0} {$i < 4} {incr i} {
                set fd [socket $primary_host $primary_port]
                fconfigure $fd -translation binary -blocking 1 -buffering full
                lappend fds $fd
            }
            set i 0
            foreach fd $fds {
                set p ""
                for {set k 0} {$k < 1000} {incr k} { append p [ss_resp set ss:rp:$i:$k v$k] [ss_resp incr ss:rp:c$i] }
                puts -nonewline $fd $p
                flush $fd
                incr i
            }
            foreach fd $fds { ss_read_replies $fd 2000 }
            assert {[getInfoProperty [$primary info fastpath] fastpath_clients] >= 4}
            wait_for_ofs_sync $primary $replica
            $replica select 0
            for {set i 0} {$i < 4} {incr i} {
                assert_equal 1000 [$replica get ss:rp:c$i]
                assert_equal v999 [$replica get ss:rp:$i:999]
            }
            $primary select 0
            assert_equal [$primary dbsize] [$replica dbsize]
            foreach fd $fds { close $fd }
        }
    }
}

start_server {tags {"staged-loop external:skip tls:skip logreqres:skip"} overrides {io-threads 16 io-threads-always-active yes}} {
    test {Staged stress: raising io-threads while lowered slots still retire converges} {
        # Lowered to 1 and raised to 5 before any retiring thread stopped: the slots below the
        # new target must finish retiring and be recreated instead of blocking the server.
        r multi
        r config set io-threads 1
        r config set io-threads 5
        assert_equal {OK OK} [r exec]
        wait_for_condition 100 50 {
            [s io_threads_running] == 4 && [s io_threads_retiring] == 0
        } else {
            fail "IO threads did not converge: running [s io_threads_running] retiring [s io_threads_retiring]"
        }
        set fd [socket [srv 0 host] [srv 0 port]]
        fconfigure $fd -translation binary -blocking 1 -buffering full
        puts -nonewline $fd [ss_resp incr ss:iot]
        flush $fd
        assert_equal {:1} [ss_read_reply $fd]
        close $fd
    }
}

start_server {tags {"staged-loop external:skip tls:skip logreqres:skip"} overrides {io-threads 4 io-threads-always-active yes}} {
    test {Staged stress: clients handed back when staged mode stops rejoin when it resumes} {
        r client setname ss-control
        r select 0
        set fds [ss_open 12 ss:rj]
        # Clients main must keep: one inside MULTI, one subscribed, one named.
        set multi [ss_fp_client ss:rjm]
        puts -nonewline $multi [ss_resp multi]
        flush $multi
        assert_equal {+OK} [ss_read_reply $multi]
        set sub [ss_fp_client ss:rjs]
        puts -nonewline $sub [ss_resp subscribe ss:rjch]
        flush $sub
        assert_equal {subscribe ss:rjch :1} [ss_read_reply $sub]
        set named [ss_fp_client ss:rjn]
        puts -nonewline $named [ss_resp client setname ss-named]
        flush $named
        assert_equal {+OK} [ss_read_reply $named]
        ss_wait_fastpath_clients 12
        set seen 0
        foreach {off on} {{io-threads 1} {io-threads 4} {io-threads-fast-path no} {io-threads-fast-path yes}
                          {io-threads 1} {io-threads 4} {io-threads-strict-offload no} {io-threads-strict-offload yes}
                          {io-threads 1} {io-threads 4} {io-threads-fast-path no} {io-threads-fast-path yes}} {
            # A pipeline in flight across the stop and one across the restart.
            set i 0
            foreach fd $fds {
                set p ""
                for {set k 0} {$k < 20} {incr k} { append p [ss_resp incr ss:rj:c$i] }
                puts -nonewline $fd $p
                flush $fd
                incr i
            }
            assert_equal OK [r config set {*}$off]
            ss_wait_fastpath_clients 0
            set i 0
            foreach fd $fds {
                for {set k 1} {$k <= 20} {incr k} { assert_equal ":[expr {$seen + $k}]" [ss_read_reply $fd] }
                incr i
            }
            incr seen 20
            assert_equal OK [r config set {*}$on]
            ss_wait_fastpath_clients 12
            ss_incr_round $fds ss:rj $seen 5
            incr seen 5
            assert_equal 12 [ss_fastpath_clients]
        }
        for {set i 0} {$i < 12} {incr i} { assert_equal $seen [r get ss:rj:c$i] }
        # The kept clients still work on main and keep their state.
        puts -nonewline $multi "[ss_resp incr ss:rjm][ss_resp exec]"
        flush $multi
        assert_equal {+QUEUED :2} [ss_read_replies $multi 2]
        assert_equal 1 [r publish ss:rjch hi]
        assert_equal {message ss:rjch hi} [ss_read_reply $sub]
        assert_match {*name=ss-named*} [r client list]
        foreach fd [concat $fds [list $multi $sub $named]] { close $fd }
        ss_wait_fastpath_clients 0
    }
}
