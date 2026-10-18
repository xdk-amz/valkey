# Parallel write lanes (docs/parallel-write-stage.md): the staged write phase runs lane-eligible
# fast-path commands on main and the IO threads at once, by keyspace shard. Fast-path clients here
# are raw db-0 sockets; io-threads-parallel-writes-min 1 sends every segment to the lanes.

proc pwl_socket {host port} {
    set fd [socket $host $port]
    fconfigure $fd -translation {crlf binary} -blocking 1 -buffering full
    return $fd
}

proc pwl_resp {args} {
    set cmd "*[llength $args]\r\n"
    foreach a $args { append cmd "$[string length $a]\r\n$a\r\n" }
    return $cmd
}

proc pwl_read_reply {fd} {
    set line [gets $fd]
    switch -- [string index $line 0] {
        {$} {
            if {$line eq {$-1}} { return {(nil)} }
            return [gets $fd]
        }
        {*} {
            set n [string range $line 1 end]
            if {$n < 0} { return {(nil)} }
            set out {}
            for {set i 0} {$i < $n} {incr i} { lappend out [pwl_read_reply $fd] }
            return $out
        }
        default { return $line }
    }
}

proc pwl_read_replies {fd n} {
    set out {}
    for {set i 0} {$i < $n} {incr i} { lappend out [pwl_read_reply $fd] }
    return $out
}

proc pwl_fastpath_clients {c} {
    return [getInfoProperty [$c info fastpath] fastpath_clients]
}

proc pwl_lane_entries {c} {
    return [getInfoProperty [$c info fastpath] fastpath_write_lane_entries]
}

proc pwl_speculated {c} {
    return [getInfoProperty [$c info fastpath] fastpath_speculated]
}

# Commands of the primary's stream a replica applied on its lanes.
proc pwl_repl_applied {c} {
    return [getInfoProperty [$c info stats] repl_parallel_applied]
}

proc pwl_calls {c cmd} {
    set v [getInfoProperty [$c info commandstats] cmdstat_$cmd]
    if {![regexp {calls=([0-9]+)} $v -> n]} { return 0 }
    return $n
}

# A pipeline of lane writes over many shards on a fast-path socket of the primary, replies read.
proc pwl_lane_load {primary host port prefix n} {
    set fd [pwl_fast_socket $primary $host $port]
    set buf ""
    for {set i 0} {$i < $n} {incr i} {
        switch [expr {$i % 5}] {
            0 { append buf [pwl_resp set $prefix:k$i v$i] }
            1 { append buf [pwl_resp hset $prefix:h[expr {$i % 97}] f$i $i] }
            2 { append buf [pwl_resp sadd $prefix:s[expr {$i % 89}] m$i] }
            3 { append buf [pwl_resp zadd $prefix:z[expr {$i % 83}] $i m$i] }
            4 { append buf [pwl_resp rpush $prefix:l[expr {$i % 79}] $i] }
        }
    }
    puts -nonewline $fd $buf
    flush $fd
    pwl_read_replies $fd $n
    close $fd
}

proc pwl_wait_fastpath_clients {c n} {
    wait_for_condition 200 20 {
        [pwl_fastpath_clients $c] == $n
    } else {
        fail "expected $n fast-path clients: [$c info fastpath]"
    }
}

# SET pipelines on nsock fast-path sockets sent at once, so the read phase spreads them over the IO
# threads; replies read.
proc pwl_multi_load {c host port prefix nsock n} {
    pwl_wait_fastpath_clients $c 0
    set fds {}
    for {set s 0} {$s < $nsock} {incr s} {
        set fd [pwl_socket $host $port]
        puts -nonewline $fd [pwl_resp ping]
        flush $fd
        assert_equal {+PONG} [pwl_read_reply $fd]
        lappend fds $fd
    }
    pwl_wait_fastpath_clients $c $nsock
    set s 0
    foreach fd $fds {
        set buf ""
        for {set i 0} {$i < $n} {incr i} { append buf [pwl_resp set $prefix:$s:$i $i] }
        puts -nonewline $fd $buf
        flush $fd
        incr s
    }
    foreach fd $fds {
        pwl_read_replies $fd $n
        close $fd
    }
}

# A raw db-0 socket on the fast path, the only one open on that server.
proc pwl_fast_socket {c host port} {
    pwl_wait_fastpath_clients $c 0
    set fd [pwl_socket $host $port]
    puts -nonewline $fd [pwl_resp ping]
    flush $fd
    assert_equal {+PONG} [pwl_read_reply $fd]
    pwl_wait_fastpath_clients $c 1
    return $fd
}

start_server {tags {"staged-loop external:skip tls:skip"} overrides {io-threads 5 keyspace-shard-bits 10 io-threads-parallel-writes-min 1 enable-debug-command yes}} {
    r client setname pwl-control
    r select 0
    r config set notify-keyspace-events ""
    set host [srv 0 host]
    set port [srv 0 port]

    test {Write lanes: a pipeline's writes and reads on many shards run on the lanes, replies in order} {
        set fd [pwl_fast_socket r $host $port]
        set before [expr {[pwl_lane_entries r] + [pwl_speculated r]}]
        set buf ""
        set expect {}
        for {set i 0} {$i < 200} {incr i} {
            append buf [pwl_resp set pwl:k$i v$i]
            append buf [pwl_resp get pwl:k$i]
            lappend expect {+OK} v$i
        }
        puts -nonewline $fd $buf
        flush $fd
        assert_equal $expect [pwl_read_replies $fd 400]
        # A read that ends a packet before a GET lets the IO thread answer that GET in the read phase.
        assert {[pwl_lane_entries r] + [pwl_speculated r] - $before >= 400}
        close $fd
    }

    test {Write lanes: a client's reads after its writes of other keys see the order it sent} {
        set fd [pwl_fast_socket r $host $port]
        set buf ""
        for {set i 0} {$i < 100} {incr i} {
            append buf [pwl_resp set pwl:a $i]
            append buf [pwl_resp set pwl:b $i]
            append buf [pwl_resp get pwl:a]
            append buf [pwl_resp get pwl:b]
        }
        puts -nonewline $fd $buf
        flush $fd
        set got [pwl_read_replies $fd 400]
        for {set i 0} {$i < 100} {incr i} {
            assert_equal [list {+OK} {+OK} $i $i] [lrange $got [expr {4 * $i}] [expr {3 + 4 * $i}]]
        }
        close $fd
    }

    test {Write lanes: a command only main runs splits the segment and keeps its place} {
        set barriers [getInfoProperty [r info fastpath] fastpath_write_barriers]
        set fd [pwl_fast_socket r $host $port]
        set buf ""
        append buf [pwl_resp set pwl:n 1]
        append buf [pwl_resp setbit pwl:bits 7 1]
        append buf [pwl_resp get pwl:n]
        append buf [pwl_resp set pwl:n 10]
        append buf [pwl_resp bitcount pwl:bits]
        append buf [pwl_resp get pwl:n]
        puts -nonewline $fd $buf
        flush $fd
        assert_equal {+OK :0 1 +OK :1 10} [pwl_read_replies $fd 6]
        assert {[getInfoProperty [r info fastpath] fastpath_write_barriers] >= $barriers + 2}
        close $fd
    }

    test {Write lanes: string and key commands run on the lanes with stock semantics} {
        r flushall
        set fd [pwl_fast_socket r $host $port]
        set before [pwl_lane_entries r]
        set buf ""
        append buf [pwl_resp incr pwl:i]
        append buf [pwl_resp incrby pwl:i 10]
        append buf [pwl_resp decr pwl:i]
        append buf [pwl_resp incrbyfloat pwl:f 1.5]
        append buf [pwl_resp append pwl:s abc]
        append buf [pwl_resp setrange pwl:s 1 XY]
        append buf [pwl_resp getrange pwl:s 0 -1]
        append buf [pwl_resp strlen pwl:s]
        append buf [pwl_resp getset pwl:s new]
        append buf [pwl_resp getdel pwl:s]
        append buf [pwl_resp exists pwl:s]
        append buf [pwl_resp setex pwl:t 100 v]
        append buf [pwl_resp persist pwl:t]
        append buf [pwl_resp ttl pwl:t]
        append buf [pwl_resp expire pwl:t 50]
        append buf [pwl_resp pexpire pwl:t -1]
        append buf [pwl_resp exists pwl:t]
        append buf [pwl_resp type pwl:i]
        append buf [pwl_resp del pwl:i pwl:f]
        append buf [pwl_resp get pwl:i]
        puts -nonewline $fd $buf
        flush $fd
        set got [pwl_read_replies $fd 20]
        assert_equal {:1 :11 :10 1.5 :3 :3 aXY :3 aXY new :0 +OK :1 :-1 :1 :1 :0 +string} [lrange $got 0 17]
        # DEL of two keys runs on a lane only if they share a shard; either way the result is the same.
        assert_equal {(nil)} [lindex $got 19]
        assert_equal {:2} [lindex $got 18]
        assert {[pwl_lane_entries r] - $before >= 17}
        close $fd
    }

    test {Write lanes: MSET and MGET across shards run on main and stay atomic} {
        set fd [pwl_fast_socket r $host $port]
        set barriers [getInfoProperty [r info fastpath] fastpath_write_barriers]
        set buf ""
        for {set i 0} {$i < 20} {incr i} {
            append buf [pwl_resp mset pwl:m$i a pwl:mm$i b pwl:mmm$i c]
            append buf [pwl_resp mget pwl:m$i pwl:mm$i pwl:mmm$i]
        }
        puts -nonewline $fd $buf
        flush $fd
        for {set i 0} {$i < 20} {incr i} {
            assert_equal {+OK} [pwl_read_reply $fd]
            assert_equal {*3} [gets $fd]
            assert_equal {a b c} [pwl_read_replies $fd 3]
        }
        assert {[getInfoProperty [r info fastpath] fastpath_write_barriers] > $barriers}
        close $fd
    }

    test {Write lanes: commandstats, errorstats, keyspace stats and latency histograms count lane commands} {
        r del pwl:list
        r rpush pwl:list a
        set fd [pwl_fast_socket r $host $port]
        r config resetstat
        set buf ""
        for {set i 0} {$i < 50} {incr i} { append buf [pwl_resp set pwl:s$i x] }
        for {set i 0} {$i < 50} {incr i} { append buf [pwl_resp get pwl:s$i] }
        append buf [pwl_resp get pwl:missing]
        append buf [pwl_resp get pwl:list]
        puts -nonewline $fd $buf
        flush $fd
        set got [pwl_read_replies $fd 102]
        assert_equal {(nil)} [lindex $got 100]
        assert_match {-WRONGTYPE*} [lindex $got 101]
        close $fd
        set cs [r info commandstats]
        assert_match {*cmdstat_set:calls=50,*} $cs
        assert_match {*cmdstat_get:calls=52,*failed_calls=1*} $cs
        assert_match {*errorstat_WRONGTYPE:count=1*} [r info errorstats]
        assert_equal 1 [getInfoProperty [r info stats] keyspace_misses]
        assert_equal 51 [getInfoProperty [r info stats] keyspace_hits]
        assert_match {*calls 50 *} [r latency histogram set]
    }

    test {Write lanes: SET options run on the lanes with stock semantics} {
        set fd [pwl_fast_socket r $host $port]
        set buf ""
        append buf [pwl_resp set pwl:o 1 NX]
        append buf [pwl_resp set pwl:o 2 NX]
        append buf [pwl_resp set pwl:o 3 XX GET]
        append buf [pwl_resp set pwl:x 1 XX]
        append buf [pwl_resp set pwl:o 4 EX 1000]
        append buf [pwl_resp set pwl:o 5 KEEPTTL]
        append buf [pwl_resp get pwl:o]
        puts -nonewline $fd $buf
        flush $fd
        assert_equal {+OK (nil) 1 (nil) +OK +OK 5} [pwl_read_replies $fd 7]
        assert_range [r ttl pwl:o] 900 1000
        assert_equal 0 [r exists pwl:x]
        close $fd
    }

    test {Write lanes: a key a lane finds expired is deleted there and counted} {
        r debug set-active-expire 0
        set fd [pwl_fast_socket r $host $port]
        r set pwl:e v PX 1
        after 20
        set expired [getInfoProperty [r info stats] expired_keys]
        set buf ""
        for {set i 0} {$i < 40} {incr i} { append buf [pwl_resp set pwl:f$i x] }
        append buf [pwl_resp set pwl:e w NX GET]
        puts -nonewline $fd $buf
        flush $fd
        set got [pwl_read_replies $fd 41]
        assert_equal {(nil)} [lindex $got 40]
        assert_equal [expr {$expired + 1}] [getInfoProperty [r info stats] expired_keys]
        assert_equal w [r get pwl:e]
        close $fd
        r debug set-active-expire 1
    }

    test {Write lanes: lanes stay off while a MONITOR is attached} {
        set fd [pwl_fast_socket r $host $port]
        set rd [valkey_deferring_client]
        $rd monitor
        assert_match {*OK*} [$rd read]
        set before [pwl_lane_entries r]
        set buf ""
        for {set i 0} {$i < 50} {incr i} { append buf [pwl_resp set pwl:m$i x] }
        puts -nonewline $fd $buf
        flush $fd
        pwl_read_replies $fd 50
        assert_equal $before [pwl_lane_entries r]
        set seen {}
        while {![string match {*"pwl:m0"*} $seen]} { set seen [$rd read] }
        close $fd
        $rd close
    }

    test {Write lanes: io-threads-parallel-writes no runs the write phase on main} {
        r config set io-threads-parallel-writes no
        set fd [pwl_fast_socket r $host $port]
        set before [pwl_lane_entries r]
        set buf ""
        for {set i 0} {$i < 50} {incr i} { append buf [pwl_resp set pwl:p$i x] }
        puts -nonewline $fd $buf
        flush $fd
        pwl_read_replies $fd 50
        assert_equal $before [pwl_lane_entries r]
        close $fd
        r config set io-threads-parallel-writes yes
    }

    test {Write lanes: replies stream only while the write phase runs on main} {
        r config set staged-reply-overlap exec
        set lanes [getInfoProperty [r info fastpath] fastpath_write_lane_phases]
        set streams [getInfoProperty [r info fastpath] staged_stream_phases]
        pwl_multi_load r $host $port pwl:st 8 200
        assert {[getInfoProperty [r info fastpath] fastpath_write_lane_phases] > $lanes}
        assert_equal $streams [getInfoProperty [r info fastpath] staged_stream_phases]

        r config set io-threads-parallel-writes no
        set lanes [getInfoProperty [r info fastpath] fastpath_write_lane_phases]
        pwl_multi_load r $host $port pwl:st 8 200
        assert_equal $lanes [getInfoProperty [r info fastpath] fastpath_write_lane_phases]
        assert {[getInfoProperty [r info fastpath] staged_stream_phases] > $streams}
        r config set io-threads-parallel-writes yes
    }

    test {Write lanes: hash, set, sorted set and list commands run on the lanes with stock semantics} {
        r flushall
        set fd [pwl_fast_socket r $host $port]
        set before [pwl_lane_entries r]
        set buf ""
        set expect {}
        for {set i 0} {$i < 50} {incr i} {
            append buf [pwl_resp hset pwl:h$i a 1 b 2]
            append buf [pwl_resp hincrby pwl:h$i a 5]
            append buf [pwl_resp hdel pwl:h$i b]
            append buf [pwl_resp hget pwl:h$i a]
            append buf [pwl_resp sadd pwl:s$i x y z]
            append buf [pwl_resp srem pwl:s$i y]
            append buf [pwl_resp scard pwl:s$i]
            append buf [pwl_resp zadd pwl:z$i 1 a 2 b]
            append buf [pwl_resp zincrby pwl:z$i 5 a]
            append buf [pwl_resp zrange pwl:z$i 0 -1]
            append buf [pwl_resp rpush pwl:l$i a b c]
            append buf [pwl_resp lpop pwl:l$i]
            append buf [pwl_resp lrange pwl:l$i 0 -1]
            lappend expect :2 :6 :1 6 :3 :1 :2 :2 6 {b a} :3 a {b c}
        }
        puts -nonewline $fd $buf
        flush $fd
        assert_equal $expect [pwl_read_replies $fd 650]
        assert {[pwl_lane_entries r] - $before >= 600}
        close $fd
        assert_equal {a 6} [r hgetall pwl:h7]
        assert_equal {x z} [lsort [r smembers pwl:s7]]
        assert_equal {b 2 a 6} [r zrange pwl:z7 0 -1 withscores]
        assert_equal {b c} [r lrange pwl:l7 0 -1]
    }

    test {Write lanes: a command that empties a key deletes it on the lane} {
        set fd [pwl_fast_socket r $host $port]
        set buf ""
        for {set i 0} {$i < 40} {incr i} {
            append buf [pwl_resp rpush pwl:e$i a]
            append buf [pwl_resp rpop pwl:e$i]
            append buf [pwl_resp sadd pwl:es$i a]
            append buf [pwl_resp spop pwl:es$i]
            append buf [pwl_resp exists pwl:e$i pwl:es$i]
        }
        puts -nonewline $fd $buf
        flush $fd
        for {set i 0} {$i < 40} {incr i} {
            assert_equal {:1 a :1 a :0} [pwl_read_replies $fd 5]
        }
        close $fd
    }

    test {Write lanes: hash field expiry runs on the lanes and counts expired fields} {
        r debug set-active-expire 0
        set fd [pwl_fast_socket r $host $port]
        set fields [getInfoProperty [r info stats] expired_fields]
        set buf ""
        for {set i 0} {$i < 40} {incr i} { append buf [pwl_resp hsetex pwl:x$i PX 1 FIELDS 1 f v] }
        puts -nonewline $fd $buf
        flush $fd
        pwl_read_replies $fd 40
        after 10
        set buf ""
        for {set i 0} {$i < 40} {incr i} {
            append buf [pwl_resp hset pwl:x$i f w]
            append buf [pwl_resp hexpire pwl:x$i 1000 FIELDS 1 f]
            append buf [pwl_resp httl pwl:x$i FIELDS 1 f]
        }
        puts -nonewline $fd $buf
        flush $fd
        for {set i 0} {$i < 40} {incr i} {
            assert_equal {:1} [pwl_read_reply $fd]
            assert_equal {*1} [gets $fd]
            assert_equal {:1} [pwl_read_reply $fd]
            assert_equal {*1} [gets $fd]
            assert_range [string range [pwl_read_reply $fd] 1 end] 900 1000
        }
        # Each HSET overwrote a field past its expiry: stock counts it as expired.
        assert_equal [expr {$fields + 40}] [getInfoProperty [r info stats] expired_fields]
        close $fd
        r debug set-active-expire 1
    }

    test {Write lanes: lanes keep running while a client is blocked; a push to its key serves it in order} {
        set rd [valkey_deferring_client]
        $rd select 0
        $rd read
        $rd blpop pwl:bl 0
        wait_for_blocked_clients_count 1
        set fd [pwl_fast_socket r $host $port]
        set before [pwl_lane_entries r]
        set barriers [getInfoProperty [r info fastpath] fastpath_write_barriers]
        set buf ""
        for {set i 0} {$i < 60} {incr i} { append buf [pwl_resp rpush pwl:q$i x] }
        append buf [pwl_resp rpush pwl:bl first second]
        append buf [pwl_resp lrange pwl:bl 0 -1]
        for {set i 0} {$i < 60} {incr i} { append buf [pwl_resp rpush pwl:r$i x] }
        puts -nonewline $fd $buf
        flush $fd
        pwl_read_replies $fd 60
        assert_equal {:2} [pwl_read_reply $fd]
        # The blocked client was served right after the push, before the LRANGE behind it.
        assert_equal {*1} [gets $fd]
        assert_equal {second} [pwl_read_reply $fd]
        pwl_read_replies $fd 60
        assert_equal {pwl:bl first} [$rd read]
        assert {[pwl_lane_entries r] - $before >= 120}
        assert {[getInfoProperty [r info fastpath] fastpath_write_barriers] > $barriers}
        close $fd
        $rd close
    }

    test {Write lanes: lanes keep running while a client watches keys; a write to a watched key aborts its EXEC} {
        set rd [valkey_deferring_client]
        $rd select 0
        $rd read
        $rd watch pwl:w
        $rd read
        $rd multi
        $rd read
        $rd set pwl:wset y
        $rd read
        set fd [pwl_fast_socket r $host $port]
        set before [pwl_lane_entries r]
        set buf ""
        for {set i 0} {$i < 60} {incr i} { append buf [pwl_resp set pwl:wo$i x] }
        append buf [pwl_resp set pwl:w changed]
        puts -nonewline $fd $buf
        flush $fd
        pwl_read_replies $fd 61
        assert {[pwl_lane_entries r] - $before >= 60}
        $rd exec
        assert_equal {} [$rd read]
        assert_equal 0 [r exists pwl:wset]
        close $fd
        $rd close
    }
}

start_server {tags {"staged-loop repl external:skip tls:skip"} overrides {io-threads 5 keyspace-shard-bits 10 io-threads-parallel-writes-min 1 enable-debug-command yes}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]
    $primary client setname pwl-control
    $primary select 0
    $primary config set notify-keyspace-events ""
    start_server {overrides {io-threads 3 keyspace-shard-bits 10 enable-debug-command yes}} {
        set replica [srv 0 client]
        $replica config set notify-keyspace-events ""
        $replica replicaof $primary_host $primary_port
        wait_for_condition 100 50 {
            [s 0 master_link_status] eq {up}
        } else {
            fail "replica did not connect"
        }

        test {Write lanes: the replication stream applies to the same dataset} {
            set fds {}
            for {set c 0} {$c < 8} {incr c} {
                set fd [pwl_socket $primary_host $primary_port]
                puts -nonewline $fd [pwl_resp ping]
                flush $fd
                assert_equal {+PONG} [pwl_read_reply $fd]
                lappend fds $fd
            }
            pwl_wait_fastpath_clients $primary 8
            set before [pwl_lane_entries $primary]
            set c 0
            foreach fd $fds {
                set buf ""
                for {set i 0} {$i < 300} {incr i} {
                    set k pwl:r[expr {($c * 7919 + $i * 31) % 500}]
                    set n [expr {($c * 131 + $i * 17) % 400}]
                    switch [expr {$i % 14}] {
                        0 { append buf [pwl_resp set $k v$c.$i] }
                        1 { append buf [pwl_resp set $k e$c.$i EX 1000] }
                        2 { append buf [pwl_resp set $k p$c.$i PX 5] }
                        3 { append buf [pwl_resp set $k n$c.$i NX] }
                        4 { append buf [pwl_resp set $k x$c.$i XX GET] }
                        5 { append buf [pwl_resp get $k] }
                        6 { append buf [pwl_resp hset pwl:rh$n f$i v$c] }
                        7 { append buf [pwl_resp hincrbyfloat pwl:rh$n g 1.5] }
                        8 { append buf [pwl_resp sadd pwl:rs$n m$i] }
                        9 { append buf [pwl_resp srem pwl:rs$n m[expr {$i - 1}]] }
                        10 { append buf [pwl_resp zadd pwl:rz$n $i m$c] }
                        11 { append buf [pwl_resp zpopmin pwl:rz$n] }
                        12 { append buf [pwl_resp rpush pwl:rl$n $c.$i] }
                        13 { append buf [pwl_resp lpop pwl:rl$n] }
                    }
                }
                puts -nonewline $fd $buf
                flush $fd
                incr c
            }
            foreach fd $fds {
                pwl_read_replies $fd 300
                close $fd
            }
            assert {[pwl_lane_entries $primary] - $before > 1000}
            # Active expire on the primary deletes what PX 5 left and replicates the deletes.
            wait_for_condition 100 50 {
                [$primary debug digest] eq [$replica debug digest]
            } else {
                fail "primary and replica differ: [$primary dbsize] vs [$replica dbsize]"
            }
        }

        test {Write lanes: the replication stream has the bytes the serial write phase produces} {
            proc pwl_stream_run {primary host port} {
                $primary flushall
                $primary debug set-active-expire 0
                $primary set pwl:lz v PX 1
                after 5
                set fd [pwl_fast_socket $primary $host $port]
                set before [s -1 master_repl_offset]
                set buf ""
                for {set i 0} {$i < 50} {incr i} {
                    append buf [pwl_resp set pwl:a$i $i]
                    append buf [pwl_resp set pwl:b$i x EX 100]
                    append buf [pwl_resp incrbyfloat pwl:f$i 1.5]
                    append buf [pwl_resp hincrbyfloat pwl:h$i f 2.5]
                    append buf [pwl_resp rpush pwl:l$i a b]
                    append buf [pwl_resp zadd pwl:z$i $i m]
                }
                append buf [pwl_resp set pwl:lz w NX]
                puts -nonewline $fd $buf
                flush $fd
                pwl_read_replies $fd 301
                close $fd
                $primary debug set-active-expire 1
                return [expr {[s -1 master_repl_offset] - $before}]
            }
            set entries [pwl_lane_entries $primary]
            set lanes [pwl_stream_run $primary $primary_host $primary_port]
            assert {[pwl_lane_entries $primary] - $entries >= 250}
            wait_for_ofs_sync $primary $replica
            assert_equal [$primary debug digest] [$replica debug digest]
            $primary config set io-threads-parallel-writes no
            set entries [pwl_lane_entries $primary]
            set serial [pwl_stream_run $primary $primary_host $primary_port]
            assert_equal $entries [pwl_lane_entries $primary]
            $primary config set io-threads-parallel-writes yes
            assert_equal $serial $lanes
            wait_for_ofs_sync $primary $replica
            assert_equal [$primary debug digest] [$replica debug digest]
        }

        test {Write lanes: WAIT after a lane write waits for that write's offset} {
            set fd [pwl_fast_socket $primary $primary_host $primary_port]
            set buf ""
            for {set i 0} {$i < 40} {incr i} { append buf [pwl_resp set pwl:w$i x] }
            append buf [pwl_resp wait 1 2000]
            puts -nonewline $fd $buf
            flush $fd
            set got [pwl_read_replies $fd 41]
            assert_equal {:1} [lindex $got 40]
            close $fd
        }

        $replica config set io-threads-parallel-writes-min 1
        $replica select 0

        test {Replica lanes: the replica applies the stream's lane commands on its lanes} {
            $replica config resetstat
            set before [pwl_repl_applied $replica]
            pwl_lane_load $primary $primary_host $primary_port pwl:ra 2000
            wait_for_ofs_sync $primary $replica
            assert_equal [$primary debug digest] [$replica debug digest]
            assert {[pwl_repl_applied $replica] - $before >= 1900}
            set stats [$replica info commandstats]
            foreach cmd {set hset sadd zadd rpush} {
                assert_match "*cmdstat_$cmd:calls=400,*" $stats
            }
        }

        test {Replica lanes: a MULTI/EXEC from the primary applies in its place on main} {
            set before [pwl_calls $replica exec]
            set fd [pwl_fast_socket $primary $primary_host $primary_port]
            set buf ""
            for {set i 0} {$i < 200} {incr i} {
                append buf [pwl_resp set pwl:rm$i a$i]
                if {$i % 20 == 0} {
                    # Propagated as MULTI, the effects and EXEC.
                    append buf [pwl_resp eval {redis.call('set', KEYS[1], ARGV[1]); redis.call('append', KEYS[2], ARGV[1])} 2 pwl:rm$i pwl:rq$i b$i]
                }
                append buf [pwl_resp append pwl:rm$i c]
            }
            puts -nonewline $fd $buf
            flush $fd
            pwl_read_replies $fd 410
            close $fd
            wait_for_ofs_sync $primary $replica
            assert_equal [$primary debug digest] [$replica debug digest]
            assert_equal b0c [$replica get pwl:rm0]
            assert_equal a1c [$replica get pwl:rm1]
            assert_equal 10 [expr {[pwl_calls $replica exec] - $before}]
        }

        test {Replica lanes: SELECT in the stream sends the commands after it to their database} {
            set fd [pwl_fast_socket $primary $primary_host $primary_port]
            set other [valkey $primary_host $primary_port]
            $other select 9
            for {set round 0} {$round < 10} {incr round} {
                set buf ""
                for {set i 0} {$i < 50} {incr i} { append buf [pwl_resp set pwl:db$i r$round] }
                puts -nonewline $fd $buf
                flush $fd
                pwl_read_replies $fd 50
                $other set pwl:db[expr {$round * 3}] nine$round
                $other hset pwl:dbh f$round $round
            }
            close $fd
            $other close
            wait_for_ofs_sync $primary $replica
            assert_equal [$primary debug digest] [$replica debug digest]
            $replica select 9
            assert_equal nine9 [$replica get pwl:db27]
            assert_equal 10 [$replica hlen pwl:dbh]
            $replica select 0
            assert_equal r9 [$replica get pwl:db27]
        }

        test {Replica lanes: a write from the stream to a key a replica client watches aborts its EXEC} {
            set before [pwl_repl_applied $replica]
            set rd [valkey [srv 0 host] [srv 0 port]]
            $rd select 0
            $rd watch pwl:rw
            $rd multi
            $rd get pwl:rw
            set fd [pwl_fast_socket $primary $primary_host $primary_port]
            set buf ""
            for {set i 0} {$i < 300} {incr i} {
                append buf [pwl_resp set pwl:rx$i $i]
                if {$i == 150} { append buf [pwl_resp set pwl:rw changed] }
            }
            puts -nonewline $fd $buf
            flush $fd
            pwl_read_replies $fd 301
            close $fd
            wait_for_ofs_sync $primary $replica
            assert_equal {} [$rd exec]
            $rd close
            assert {[pwl_repl_applied $replica] - $before >= 290}
            assert_equal changed [$replica get pwl:rw]
        }

        test {Replica lanes: a replica client blocked on a stream is served by the stream's write in order} {
            set before [pwl_repl_applied $replica]
            set rd [valkey_deferring_client]
            $rd select 0
            assert_equal OK [$rd read]
            $rd xread block 0 streams pwl:rxs $
            wait_for_blocked_client
            set fd [pwl_fast_socket $primary $primary_host $primary_port]
            set buf ""
            for {set i 0} {$i < 300} {incr i} { append buf [pwl_resp set pwl:ry$i $i] }
            puts -nonewline $fd $buf
            flush $fd
            pwl_read_replies $fd 300
            close $fd
            $primary xadd pwl:rxs 1-1 f v
            assert_match {*pwl:rxs*1-1*} [$rd read]
            $rd close
            wait_for_ofs_sync $primary $replica
            assert_equal [$primary debug digest] [$replica debug digest]
            assert {[pwl_repl_applied $replica] - $before >= 290}
        }

        test {Replica lanes: a sub-replica gets the stream the replica applied} {
            set replica_host [srv 0 host]
            set replica_port [srv 0 port]
            start_server {overrides {io-threads 3 keyspace-shard-bits 10 io-threads-parallel-writes-min 1 enable-debug-command yes}} {
                set sub [srv 0 client]
                $sub config set notify-keyspace-events ""
                $sub replicaof $replica_host $replica_port
                wait_for_condition 100 50 {
                    [s 0 master_link_status] eq {up}
                } else {
                    fail "sub-replica did not connect"
                }
                set before [pwl_repl_applied $sub]
                pwl_lane_load $primary $primary_host $primary_port pwl:rsub 3000
                wait_for_ofs_sync $primary $replica
                wait_for_ofs_sync $replica $sub
                assert_equal [$primary debug digest] [$sub debug digest]
                assert {[pwl_repl_applied $sub] - $before >= 2800}
            }
        }
    }
}

start_cluster 1 1 {tags {"staged-loop cluster external:skip tls:skip"} overrides {io-threads 5 io-threads-parallel-writes-min 1}} {
    set host [srv 0 host]
    set port [srv 0 port]
    R 0 config set notify-keyspace-events ""
    R 1 config set notify-keyspace-events ""
    R 0 client setname pwl-control

    test {Write lanes (cluster): a pipeline over many slots runs on slot lanes, replies in order} {
        set fd [pwl_fast_socket [srv 0 client] $host $port]
        set before [expr {[pwl_lane_entries [srv 0 client]] + [pwl_speculated [srv 0 client]]}]
        set buf ""
        set expect {}
        for {set i 0} {$i < 300} {incr i} {
            append buf [pwl_resp set pwl:c$i v$i]
            append buf [pwl_resp incr pwl:n$i]
            append buf [pwl_resp get pwl:c$i]
            lappend expect {+OK} {:1} v$i
        }
        puts -nonewline $fd $buf
        flush $fd
        assert_equal $expect [pwl_read_replies $fd 900]
        assert {[pwl_lane_entries [srv 0 client]] + [pwl_speculated [srv 0 client]] - $before >= 900}
        close $fd
    }

    test {Write lanes (cluster): the replica applies the same dataset} {
        wait_for_condition 100 50 {
            [R 0 debug digest] eq [R 1 debug digest]
        } else {
            fail "primary and replica differ: [R 0 dbsize] vs [R 1 dbsize]"
        }
        assert {[pwl_repl_applied [srv -1 client]] >= 500}
    }

    test {Write lanes (cluster): CLUSTER SLOT-STATS counts a lane command against its slot} {
        set slot [R 0 cluster keyslot pwl:c7]
        set stats [R 0 cluster slot-stats slotsrange $slot $slot]
        set entry [dict get [lindex [lindex $stats 0] 1]]
        assert_equal 1 [dict get $entry key-count]
        assert {[dict get $entry cpu-usec] > 0}
        assert {[dict get $entry network-bytes-in] > 0}
    }

    test {Write lanes (cluster): one lane running many distinct commands in a phase counts each of them} {
        R 0 config resetstat
        set fd [pwl_fast_socket [srv 0 client] $host $port]
        set cmds {
            {set k v} {get k} {setnx k v} {getset k v} {strlen k} {append k x} {getrange k 0 1} {incr n}
            {decr n} {incrby n 2} {decrby n 2} {incrbyfloat f 1} {exists k} {touch k} {type k} {ttl k}
            {pttl k} {expire k 100} {pexpire k 100000} {expiretime k} {pexpiretime k} {persist k}
            {hset h a 1} {hsetnx h b 2} {hget h a} {hmget h a b} {hlen h} {hstrlen h a} {hexists h a}
            {hincrby h a 1} {hincrbyfloat h c 1} {hkeys h} {hvals h} {hgetall h} {hdel h c}
            {sadd s a b} {srem s b} {sismember s a} {smismember s a} {smembers s} {scard s}
            {zadd z 1 a} {zincrby z 1 a} {zscore z a} {zmscore z a} {zcard z} {zcount z 0 10}
            {zlexcount z - +} {zrange z 0 -1} {zrangebyscore z 0 10} {zrank z a} {zrevrank z a}
            {zrem z b} {rpush l a b c} {lpush l z} {lindex l 0} {llen l} {lrange l 0 -1} {lpos l a}
            {lset l 0 y} {linsert l before a q} {lrem l 1 q} {ltrim l 0 10} {rpop l} {lpop l}
            {rpushx l w} {lpushx l w}
        }
        set buf ""
        foreach c $cmds {
            # Every key carries one hash tag: one slot, one lane.
            set c [lreplace $c 1 1 "{pwl}[lindex $c 1]"]
            append buf [pwl_resp {*}$c]
        }
        puts -nonewline $fd $buf
        flush $fd
        pwl_read_replies $fd [llength $cmds]
        close $fd
        set stats [R 0 info commandstats]
        foreach c $cmds {
            assert_match "*cmdstat_[lindex $c 0]:calls=*" $stats
        }
        assert {[llength $cmds] > 64}
    }

    test {Write lanes (cluster): a CROSSSLOT command gets its error from main} {
        set fd [pwl_fast_socket [srv 0 client] $host $port]
        puts -nonewline $fd "[pwl_resp set pwl:x 1][pwl_resp mset pwl:x 1 pwl:y 2][pwl_resp get pwl:x]"
        flush $fd
        set got [pwl_read_replies $fd 3]
        assert_equal {+OK} [lindex $got 0]
        assert_match {-CROSSSLOT*} [lindex $got 1]
        assert_equal 1 [lindex $got 2]
        close $fd
    }
}
