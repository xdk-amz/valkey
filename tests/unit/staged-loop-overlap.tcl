# Staged event loop: hazards of running socket IO and keyspace reads concurrently with main's
# command execution. Per-connection order on long mixed pipelines, large replies whose key is
# changed right after, ACL changes under load, durability of acknowledged writes,
# WAIT, teardown mid-pipeline and buffer limits. Every fast-path client is a raw db-0 socket:
# the test framework's client sends SELECT first, which moves a connection off the fast path.

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

start_server {tags {"staged-loop external:skip tls:skip"} overrides {io-threads 3 enable-debug-command yes}} {
    r client setname slo-control
    r select 0

    test {Staged overlap: long mixed pipelines on one key keep exact per-connection order} {
        set n 32
        set fds [slo_open $n]
        set rounds 12
        set depth 40
        for {set r 0} {$r < $rounds} {incr r} {
            set i 0
            foreach fd $fds {
                set p ""
                for {set k 0} {$k < $depth} {incr k} {
                    set key slo:o:$i
                    append p [slo_resp set $key $k] [slo_resp incr $key] [slo_resp get $key] \
                        [slo_resp del $key] [slo_resp get $key] [slo_resp incr $key] \
                        [slo_resp get $key] [slo_resp set $key x$r.$k] [slo_resp get $key] \
                        [slo_resp del $key] [slo_resp del $key] [slo_resp incr $key]
                }
                if {$r % 2} {
                    slo_send_chunked $fd $p
                } else {
                    puts -nonewline $fd $p
                    flush $fd
                }
                incr i
            }
            set i 0
            foreach fd $fds {
                for {set k 0} {$k < $depth} {incr k} {
                    set want [list {+OK} ":[expr {$k + 1}]" [expr {$k + 1}] {:1} {(nil)} {:1} \
                        1 {+OK} x$r.$k {:1} {:0} {:1}]
                    assert_equal $want [slo_read_replies $fd 12]
                }
                incr i
            }
        }
        for {set i 0} {$i < $n} {incr i} { assert_equal 1 [r get slo:o:$i] }
        assert_equal $n [slo_fastpath_clients]
        foreach fd $fds { close $fd }
        slo_wait_fastpath_clients 0
    }

    test {Staged overlap: reads right behind writes of other clients see a consistent key} {
        # Writers bump a shared counter and a shadow with the same value in one pipeline; a
        # reader's GET never sees the counter behind a value it already saw.
        set writers [slo_open 8]
        set readers [slo_open 8]
        r set slo:mono 0
        for {set r 0} {$r < 20} {incr r} {
            foreach fd $writers {
                set p ""
                for {set k 0} {$k < 50} {incr k} { append p [slo_resp incr slo:mono] }
                puts -nonewline $fd $p
                flush $fd
            }
            foreach fd $readers {
                set p ""
                for {set k 0} {$k < 100} {incr k} { append p [slo_resp get slo:mono] }
                puts -nonewline $fd $p
                flush $fd
            }
            foreach fd $writers { slo_read_replies $fd 50 }
            foreach fd $readers {
                set last -1
                foreach v [slo_read_replies $fd 100] {
                    assert {$v >= $last}
                    set last $v
                }
            }
        }
        assert_equal [expr {8 * 20 * 50}] [r get slo:mono]
        foreach fd [concat $writers $readers] { close $fd }
        slo_wait_fastpath_clients 0
    }

    foreach size {65536 1048576} {
        test "Staged overlap: GET of a $size byte value changed right after returns intact bytes" {
            set readers [slo_open 3]
            set writer [slo_fp_client]
            set ver 0
            r set slo:big [slo_big_value $ver $size]
            set gets [expr {$size > 100000 ? 12 : 60}]
            for {set round 0} {$round < 8} {incr round} {
                foreach fd $readers {
                    set p ""
                    for {set k 0} {$k < $gets} {incr k} { append p [slo_resp get slo:big] }
                    puts -nonewline $fd $p
                    flush $fd
                }
                # Overwrite, grow, modify in place and delete the key while the replies are in flight.
                set p ""
                set nops 0
                for {set k 0} {$k < 6} {incr k} {
                    incr ver
                    set mid [expr {$size / 2}]
                    append p [slo_resp set slo:big [slo_big_value $ver $size]] \
                        [slo_resp append slo:big !!!!] [slo_resp setrange slo:big $mid ZZZZ]
                    incr nops 3
                    if {$k % 3 == 2} {
                        append p [slo_resp del slo:big]
                        incr nops
                    }
                }
                incr ver
                append p [slo_resp set slo:big [slo_big_value $ver $size]]
                incr nops
                puts -nonewline $writer $p
                flush $writer
                foreach fd $readers {
                    for {set k 0} {$k < $gets} {incr k} { slo_check_big_reply [slo_read_reply $fd] $size }
                }
                slo_read_replies $writer $nops
                # A GET after the writer's acknowledgement sees the final version exactly.
                set fd [lindex $readers 0]
                puts -nonewline $fd [slo_resp get slo:big]
                flush $fd
                assert_equal [slo_big_value $ver $size] [slo_read_reply $fd]
            }
            foreach fd $readers { close $fd }
            close $writer
            slo_wait_fastpath_clients 0
        }
    }

    test {Staged overlap: ACL SETUSER narrowing a pipelining user refuses every later command} {
        r acl setuser slo-u on >pw ~* &* +@all
        r set slo:acl:k v
        r set slo:acl:other w
        foreach narrowing {{-get} {resetkeys ~slo:acl:allowed*}} {
            r acl setuser slo-u reset on >pw ~* &* +@all
            set fds [slo_open 4 {slo-u pw}]
            # A burst in flight across the change: each connection sees accepted replies, then
            # only refusals, never an accepted reply after a refusal.
            foreach fd $fds {
                set p ""
                for {set k 0} {$k < 3000} {incr k} { append p [slo_resp get slo:acl:k] }
                puts -nonewline $fd $p
                flush $fd
            }
            r acl setuser slo-u {*}$narrowing
            foreach fd $fds {
                set p ""
                for {set k 0} {$k < 200} {incr k} { append p [slo_resp get slo:acl:k] [slo_resp get slo:acl:other] }
                puts -nonewline $fd $p
                flush $fd
            }
            foreach fd $fds {
                set refused 0
                foreach reply [slo_read_replies $fd 3000] {
                    if {[string match {-NOPERM*} $reply]} {
                        set refused 1
                    } elseif {$refused} {
                        fail "accepted reply after a refusal with $narrowing: $reply"
                    } else {
                        assert_equal v $reply
                    }
                }
                foreach reply [slo_read_replies $fd 400] { assert_match {-NOPERM*} $reply }
            }
            foreach fd $fds { close $fd }
            slo_wait_fastpath_clients 0
        }
        r acl deluser slo-u
    }

    test {Staged overlap: CLIENT KILL of pipelining clients leaves the others exact} {
        r acl setuser slo-victim on >pw ~* &* +@all
        set keep [slo_open 4]
        set seen 0
        for {set round 0} {$round < 10} {incr round} {
            set victims [slo_open 6 {slo-victim pw}]
            foreach fd $victims {
                set p ""
                for {set k 0} {$k < 2000} {incr k} { append p [slo_resp incr slo:kv] [slo_resp get slo:big:none] }
                # A partial command at the end of the pipeline.
                append p "*3\r\n\$3\r\nSET\r\n\$6\r\nslo:kv\r\n\$100\r\nabc"
                puts -nonewline $fd $p
                flush $fd
            }
            foreach fd $keep {
                set p ""
                for {set k 0} {$k < 200} {incr k} { append p [slo_resp incr slo:kk:$fd] }
                puts -nonewline $fd $p
                flush $fd
            }
            assert_equal 6 [r client kill user slo-victim]
            foreach fd $keep {
                for {set k 1} {$k <= 200} {incr k} { assert_equal ":[expr {$seen + $k}]" [slo_read_reply $fd] }
            }
            incr seen 200
            foreach fd $victims { catch {close $fd} }
        }
        slo_wait_fastpath_clients 4
        foreach fd $keep { close $fd }
        slo_wait_fastpath_clients 0
        r acl deluser slo-victim
        assert_equal PONG [r ping]
    }

    test {Staged overlap: EOF mid-pipeline, mid-command and mid-reply leaves the others exact} {
        r set slo:eof:big [string repeat q 1000000]
        set keep [slo_open 4]
        set seen 0
        for {set round 0} {$round < 15} {incr round} {
            set gone {}
            for {set v 0} {$v < 3} {incr v} {
                set fd [slo_socket]
                switch $v {
                    0 {
                        set p ""
                        for {set k 0} {$k < 3000} {incr k} { append p [slo_resp incr slo:eof:n] }
                    }
                    1 { set p "[slo_resp incr slo:eof:n]*2\r\n\$3\r\nGET\r\n\$9\r\nslo:e" }
                    2 {
                        set p ""
                        for {set k 0} {$k < 20} {incr k} { append p [slo_resp get slo:eof:big] }
                    }
                }
                puts -nonewline $fd $p
                flush $fd
                lappend gone $fd
            }
            if {$round % 2} { after 2 }
            foreach fd $gone { close $fd }
            foreach fd $keep {
                set p ""
                for {set k 0} {$k < 50} {incr k} { append p [slo_resp incr slo:eof:k:$fd] [slo_resp get slo:eof:k:$fd] }
                puts -nonewline $fd $p
                flush $fd
            }
            foreach fd $keep {
                for {set k 1} {$k <= 50} {incr k} {
                    set c [expr {$seen + $k}]
                    assert_equal [list ":$c" $c] [slo_read_replies $fd 2]
                }
            }
            incr seen 50
        }
        slo_wait_fastpath_clients 4
        foreach fd $keep { close $fd }
        slo_wait_fastpath_clients 0
        assert {[r get slo:eof:n] <= 15 * 3001}
        assert_equal PONG [r ping]
    }

    test {Staged overlap: output buffer limit disconnects a pipelining client, others unaffected} {
        r config set client-output-buffer-limit "normal 1mb 0 0"
        r set slo:ob [string repeat x 65536]
        set closed [getInfoProperty [r info stats] client_output_buffer_limit_disconnections]
        set keep [slo_open 3]
        set fd [slo_fp_client]
        set p ""
        for {set k 0} {$k < 600} {incr k} { append p [slo_resp incr slo:ob:n] [slo_resp get slo:ob] }
        fconfigure $fd -blocking 0
        catch {
            puts -nonewline $fd $p
            flush $fd
        }
        set seen 0
        wait_for_condition 200 25 {
            [getInfoProperty [r info stats] client_output_buffer_limit_disconnections] == $closed + 1
        } else {
            fail "output buffer limit did not disconnect: [r client list]"
        }
        foreach k $keep {
            puts -nonewline $k [slo_resp incr slo:ob:keep:$k]
            flush $k
            assert_equal {:1} [slo_read_reply $k]
        }
        slo_wait_fastpath_clients 3
        catch {close $fd}
        foreach k $keep { close $k }
        r config set client-output-buffer-limit "normal 0 0 0"
        slo_wait_fastpath_clients 0
    }

    test {Staged overlap: query buffer limit disconnects a pipelining client, others unaffected} {
        r config set client-query-buffer-limit 1mb
        set closed [getInfoProperty [r info stats] client_query_buffer_limit_disconnections]
        set keep [slo_open 3]
        set fd [slo_fp_client]
        set p ""
        for {set k 0} {$k < 1000} {incr k} { append p [slo_resp incr slo:qb:n] }
        append p "*3\r\n\$3\r\nSET\r\n\$6\r\nslo:qb\r\n\$4000000\r\n" [string repeat y 1500000]
        fconfigure $fd -blocking 0
        catch {
            puts -nonewline $fd $p
            flush $fd
        }
        wait_for_condition 200 25 {
            [getInfoProperty [r info stats] client_query_buffer_limit_disconnections] == $closed + 1
        } else {
            fail "query buffer limit did not disconnect: [r client list]"
        }
        assert_equal 1000 [r get slo:qb:n]
        assert_equal 0 [r exists slo:qb]
        foreach k $keep {
            puts -nonewline $k [slo_resp incr slo:qb:keep:$k]
            flush $k
            assert_equal {:1} [slo_read_reply $k]
        }
        slo_wait_fastpath_clients 3
        catch {close $fd}
        foreach k $keep { close $k }
        r config set client-query-buffer-limit 1gb
        slo_wait_fastpath_clients 0
    }
}

set slo_acl_dir [tmpdir "server.slo-acl"]
set slo_acl_file [file join $slo_acl_dir slo.acl]
proc slo_write_acl {path rules} {
    set f [open $path w]
    puts $f "user default on nopass ~* &* +@all"
    puts $f "user slo-l on >pw $rules"
    close $f
}
slo_write_acl $slo_acl_file "~* &* +@all"

start_server [list overrides [list dir $slo_acl_dir aclfile slo.acl io-threads 3] tags {"staged-loop external:skip tls:skip"}] {
    r select 0

    test {Staged overlap: ACL LOAD narrowing a pipelining user refuses every later command} {
        r set slo:al:k v
        foreach narrowed {{~* &* +@all -get} {~slo:al:allowed* &* +@all}} {
            slo_write_acl $slo_acl_file "~* &* +@all"
            r acl load
            set fds [slo_open 4 {slo-l pw}]
            foreach fd $fds {
                set p ""
                for {set k 0} {$k < 3000} {incr k} { append p [slo_resp get slo:al:k] }
                puts -nonewline $fd $p
                flush $fd
            }
            slo_write_acl $slo_acl_file $narrowed
            r acl load
            foreach fd $fds {
                set p ""
                for {set k 0} {$k < 200} {incr k} { append p [slo_resp get slo:al:k] }
                catch {
                    puts -nonewline $fd $p
                    flush $fd
                }
            }
            # A connection either stays and is refused from some point on, or is closed; it never
            # gets an accepted reply after a refusal or after the reload.
            foreach fd $fds {
                set refused 0
                set replies {}
                catch {
                    for {set k 0} {$k < 3200} {incr k} { lappend replies [slo_read_reply $fd] }
                }
                foreach reply $replies {
                    if {[string match {-NOPERM*} $reply]} {
                        set refused 1
                    } elseif {$refused} {
                        fail "accepted reply after a refusal with $narrowed: $reply"
                    } else {
                        assert_equal v $reply
                    }
                }
                if {[llength $replies] == 3200} {
                    foreach reply [lrange $replies 3000 end] {
                        if {![string match {-NOPERM*} $reply]} {
                            fail "accepted reply to a command sent after ACL LOAD with $narrowed: $reply"
                        }
                    }
                }
                catch {close $fd}
            }
            slo_wait_fastpath_clients 0
        }
    }
}

start_server {tags {"staged-loop external:skip tls:skip"} overrides {io-threads 3 appendonly yes appendfsync always enable-debug-command yes}} {
    r select 0

    test {Staged overlap: with appendfsync always a reply never leaves before its write reaches the AOF} {
        set fd [slo_fp_client]
        for {set i 0} {$i < 10} {incr i} {
            r debug aof-flush-sleep 0
            r setrange slo:af:big [expr {int(rand() * 5000000) + 5000000}] x
            r debug aof-flush-sleep 300000
            set aof [get_last_incr_aof_path r]
            set size1 [file size $aof]
            puts -nonewline $fd "[slo_resp get slo:af:big][slo_resp incr slo:af:n]"
            flush $fd
            slo_read_reply $fd
            assert_equal ":[expr {$i + 1}]" [slo_read_reply $fd]
            assert {[file size $aof] > $size1}
        }
        r debug aof-flush-sleep 0
        close $fd
    }

    test {Staged overlap: with appendfsync always acknowledged writes survive a kill -9 restart} {
        r flushall
        set n 20000
        set fds [slo_open 4]
        # Each flush waits, so the kill lands with many replies still unsent.
        r debug aof-flush-sleep 2000
        set i 0
        foreach fd $fds {
            set p ""
            for {set k 0} {$k < $n} {incr k} { append p [slo_resp incr slo:dur:$i] }
            puts -nonewline $fd $p
            flush $fd
            incr i
        }
        after 50
        # Killed without a chance to flush: only what already reached the AOF survives.
        exec kill -9 [srv 0 pid]
        set last {}
        foreach fd $fds {
            set v 0
            while {![catch {slo_read_reply $fd} reply]} {
                if {![regexp {^:(\d+)$} $reply -> v]} { fail "unexpected reply: $reply" }
            }
            lappend last $v
            catch {close $fd}
        }
        restart_server 0 true false
        wait_done_loading r
        r select 0
        set sum [expr [join $last +]]
        # The kill must have interrupted the stream for the check to mean anything.
        assert {$sum > 0 && $sum < 4 * $n}
        for {set i 0} {$i < 4} {incr i} {
            set got [r get slo:dur:$i]
            if {$got eq {}} { set got 0 }
            assert {$got >= [lindex $last $i]}
        }
    }
}

start_server {tags {"staged-loop external:skip tls:skip"} overrides {io-threads 3}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]
    start_server {overrides {io-threads 3}} {
        set replica [srv 0 client]

        test {Staged overlap: WAIT returns the replica count and the writes before it are on the replica} {
            $replica replicaof $primary_host $primary_port
            wait_for_condition 100 100 {
                [s 0 master_link_status] eq {up}
            } else {
                fail "replica did not connect"
            }
            $replica select 0
            set joined 0
            for {set round 1} {$round <= 30} {incr round} {
                # Fresh connections each round: their writes run on the fast path until WAIT.
                set fds {}
                for {set i 0} {$i < 6} {incr i} { lappend fds [slo_socket $primary_host $primary_port] }
                set i 0
                foreach fd $fds {
                    set p ""
                    for {set k 0} {$k < 20} {incr k} { append p [slo_resp set slo:w:$i:$k r$round] [slo_resp incr slo:w:c$i] }
                    puts -nonewline $fd $p
                    flush $fd
                    incr i
                }
                set i 0
                foreach fd $fds {
                    for {set k 1} {$k <= 20} {incr k} {
                        assert_equal [list {+OK} ":[expr {($round - 1) * 25 + $k}]"] [slo_read_replies $fd 2]
                    }
                    incr i
                }
                set joined [expr {max($joined, [getInfoProperty [$primary info fastpath] fastpath_clients])}]
                set i 0
                foreach fd $fds {
                    set p ""
                    for {set k 0} {$k < 5} {incr k} { append p [slo_resp set slo:w:$i:x$k r$round] [slo_resp incr slo:w:c$i] }
                    append p [slo_resp wait 1 5000] [slo_resp get slo:w:c$i]
                    puts -nonewline $fd $p
                    flush $fd
                    incr i
                }
                set i 0
                foreach fd $fds {
                    for {set k 1} {$k <= 5} {incr k} {
                        assert_equal [list {+OK} ":[expr {($round - 1) * 25 + 20 + $k}]"] [slo_read_replies $fd 2]
                    }
                    assert_equal {:1} [slo_read_reply $fd]
                    set c [expr {$round * 25}]
                    assert_equal $c [slo_read_reply $fd]
                    # WAIT acknowledged: the replica already applied these writes.
                    assert_equal $c [$replica get slo:w:c$i]
                    assert_equal r$round [$replica get slo:w:$i:x4]
                    assert_equal r$round [$replica get slo:w:$i:19]
                    incr i
                }
                foreach fd $fds { close $fd }
            }
            assert {$joined >= 1}
        }
    }
}
