# Independent review of the appendfsync group (Phase 24 of the superset audit): the routes its own
# tests leave out, and two measurements that document what the group does NOT promise.
#
# Method, as in fastpath-aof-order.tcl: DEBUG AOF-FLUSH-SLEEP delays each AOF flush that has data
# just before its write(2). A reply that arrives sooner than that delay left before the write. A reply
# that arrives after it waited for the write (and, under always, for the fsync that follows it).

proc far_fastpath_clients {} {
    return [getInfoProperty [r info fastpath] fastpath_clients]
}

proc far_fp_info {field} {
    return [getInfoProperty [r info fastpath] $field]
}

proc far_persist {field} {
    return [getInfoProperty [r info persistence] $field]
}

proc far_resp {args} {
    set cmd "*[llength $args]\r\n"
    foreach a $args { append cmd "$[string length $a]\r\n$a\r\n" }
    return $cmd
}

proc far_read_line {fd} {
    if {[gets $fd line] < 0} { error "EOF" }
    set line [string trimright $line "\r"]
    if {[string index $line 0] eq {$} && $line ne {$-1}} {
        set n [string range $line 1 end]
        set v [read $fd [expr {$n + 2}]]
        return [string range $v 0 end-2]
    }
    return $line
}

# A raw db-0 socket that ran one command and sits on the fast path.
proc far_fp_client {} {
    set before [far_fastpath_clients]
    set fd [socket [srv 0 host] [srv 0 port]]
    fconfigure $fd -translation binary -blocking 1 -buffering full
    puts -nonewline $fd [far_resp ping]
    flush $fd
    assert_equal {+PONG} [far_read_line $fd]
    wait_for_condition 200 25 {
        [far_fastpath_clients] >= $before + 1
    } else {
        fail "client did not join the fast path: [r info fastpath]"
    }
    return $fd
}

proc far_wait_no_bio_fsync {} {
    wait_for_condition 500 10 {
        [far_persist aof_pending_bio_fsync] == 0
    } else {
        fail "background AOF fsync did not finish"
    }
}

# Milliseconds from sending cmd on fd until its reply, with every AOF flush delayed by sleep_ms.
proc far_reply_ms {fd sleep_ms args} {
    r debug aof-flush-sleep [expr {$sleep_ms * 1000}]
    set t0 [clock milliseconds]
    puts -nonewline $fd [far_resp {*}$args]
    flush $fd
    set reply [far_read_line $fd]
    set ms [expr {[clock milliseconds] - $t0}]
    r debug aof-flush-sleep 0
    return [list $ms $reply]
}

set far_sleep 400

# ---------------------------------------------------------------------------------------------------
# appendfsync always: the write-lane route and MULTI/EXEC. The group's own tests cover the hand-back,
# the forkless save, the module write and the first rewrite under always, and the lane route under
# everysec and no only.
# ---------------------------------------------------------------------------------------------------
start_server {tags {"fastpath aof external:skip tls:skip logreqres:skip"} overrides {io-threads 4 keyspace-shard-bits 10 io-threads-parallel-writes-min 1 appendonly yes appendfsync always enable-debug-command yes save ""}} {
    r client setname far-control
    r select 0
    r config set notify-keyspace-events "" ;# the test default (KEA) turns the write lanes off
    waitForBgrewriteaof r

    test {AOF review: always, replies of a write-lane phase follow the AOF write and fsync, and the AOF keeps program order across lanes} {
        set fd [far_fp_client]
        set phases0 [far_fp_info fastpath_write_lane_phases]
        far_wait_no_bio_fsync
        r debug aof-flush-sleep [expr {$far_sleep * 1000}]
        set t0 [clock milliseconds]
        set p {}
        set keys {}
        for {set i 0} {$i < 200} {incr i} {
            set k [format far:lane:%03d $i]
            lappend keys $k
            append p [far_resp incr $k]
        }
        puts -nonewline $fd $p
        flush $fd
        assert_equal {:1} [far_read_line $fd]
        assert_morethan_equal [expr {[clock milliseconds] - $t0}] [expr {$far_sleep - 50}]
        for {set i 2} {$i <= 200} {incr i} { assert_equal {:1} [far_read_line $fd] }
        r debug aof-flush-sleep 0
        # The 200 entries, spread over the lanes by key, ran as lane phases, not serially on main.
        assert_morethan [far_fp_info fastpath_write_lane_phases] $phases0
        # The INCR AOF file lists them in the order the client sent them (the replay order T).
        set incr_file [glob -directory [file join [lindex [r config get dir] 1] [lindex [r config get appenddirname] 1]] *.incr.aof]
        set fh [open [lindex $incr_file 0] rb]
        set aof [read $fh]
        close $fh
        set seen [regexp -all -inline {far:lane:\d{3}} $aof]
        assert_equal $keys $seen
        close $fd
    }

    test {AOF review: always, a MULTI/EXEC reply on the main path follows the AOF write and fsync} {
        set rd [valkey_deferring_client]
        far_wait_no_bio_fsync
        r debug aof-flush-sleep [expr {$far_sleep * 1000}]
        set t0 [clock milliseconds]
        $rd multi
        $rd set far:multi 1
        $rd incr far:multi
        $rd exec
        assert_equal OK [$rd read]
        assert_equal QUEUED [$rd read]
        assert_equal QUEUED [$rd read]
        assert_equal {OK 2} [$rd read]
        assert_morethan_equal [expr {[clock milliseconds] - $t0}] [expr {$far_sleep - 50}]
        r debug aof-flush-sleep 0
        $rd close
    }
}

# ---------------------------------------------------------------------------------------------------
# everysec and no: the hand-back route (an EVAL that writes, run on main after the reply phase) and
# MULTI/EXEC. The group's own hand-back test runs under always only.
# ---------------------------------------------------------------------------------------------------
foreach policy {everysec no} {
    start_server {tags {"fastpath aof external:skip tls:skip logreqres:skip"} overrides {io-threads 4 appendonly yes enable-debug-command yes save ""}} {
        r config set appendfsync $policy
        r client setname far-control
        r select 0
        waitForBgrewriteaof r

        test "AOF review: $policy, a readmitted fast-path client gets an EVAL write's reply only after the AOF write" {
            set fd [far_fp_client]
            for {set i 0} {$i < 3} {incr i} {
                far_wait_no_bio_fsync
                lassign [far_reply_ms $fd $far_sleep eval {return redis.call('set', KEYS[1], ARGV[1])} 1 far:eval $i] ms reply
                assert_equal {+OK} $reply
                assert_morethan_equal $ms [expr {$far_sleep - 50}]
                wait_for_condition 100 20 {
                    [far_fastpath_clients] >= 1
                } else {
                    fail "client did not rejoin the fast path"
                }
            }
            assert_equal 2 [r get far:eval]
            close $fd
        }

        test "AOF review: $policy, a MULTI/EXEC reply on the main path follows the AOF write" {
            set rd [valkey_deferring_client]
            far_wait_no_bio_fsync
            r debug aof-flush-sleep [expr {$far_sleep * 1000}]
            set t0 [clock milliseconds]
            $rd multi
            $rd set far:multi 1
            $rd exec
            assert_equal OK [$rd read]
            assert_equal QUEUED [$rd read]
            assert_equal {OK} [$rd read]
            assert_morethan_equal [expr {[clock milliseconds] - $t0}] [expr {$far_sleep - 50}]
            r debug aof-flush-sleep 0
            $rd close
        }
    }
}

# ---------------------------------------------------------------------------------------------------
# What the group does not promise, measured. Under everysec a flush that finds a background fsync
# still running postpones the write(2) (for up to two seconds) and aofHoldsReplies() turns off, so the
# replies of that iteration leave with the write still in the AOF buffer. This is the stock loop's
# behavior too; the staged loop and the plain IO-thread loop are measured side by side.
# ---------------------------------------------------------------------------------------------------
foreach {fastpath label} {yes "staged loop" no "IO threads without the staged loop"} {
    start_server {tags {"fastpath aof external:skip tls:skip logreqres:skip"} overrides {io-threads 4 appendonly yes appendfsync everysec enable-debug-command yes save ""}} {
        r config set io-threads-fast-path $fastpath
        r client setname far-control
        r select 0
        waitForBgrewriteaof r

        test "AOF review: everysec, $label: a write postponed behind a background fsync is acknowledged before its write(2)" {
            if {$fastpath eq {yes}} {
                set fd [far_fp_client]
            } else {
                set fd [socket [srv 0 host] [srv 0 port]]
                fconfigure $fd -translation binary -blocking 1 -buffering full
                puts -nonewline $fd [far_resp ping]
                flush $fd
                assert_equal {+PONG} [far_read_line $fd]
            }
            far_wait_no_bio_fsync
            # Every background fsync created from now on takes at least 1.5 s. The next flush with
            # unfsynced data that is due for its once-a-second fsync creates one.
            r debug aof-bio-fsync-sleep 1500000
            r set far:fs 1
            wait_for_condition 300 10 {
                [far_persist aof_pending_bio_fsync] == 1
            } else {
                fail "no background fsync was started: [r info persistence]"
            }
            # The flush that follows this SET finds the fsync running and postpones the write(2), so
            # the sleep before write(2) never runs and the reply comes back at once.
            lassign [far_reply_ms $fd $far_sleep set far:es postponed] ms reply
            assert_equal {+OK} $reply
            assert_lessthan $ms 200
            # The acknowledged write is still in the process's buffer, not in the file.
            assert_equal 1 [far_persist aof_pending_bio_fsync]
            assert_morethan [far_persist aof_buffer_length] 0
            r debug aof-bio-fsync-sleep 0
            # Once the fsync completes the postponed buffer is written.
            wait_for_condition 500 10 {
                [far_persist aof_pending_bio_fsync] == 0 && [far_persist aof_buffer_length] == 0
            } else {
                fail "postponed AOF buffer was not written: [r info persistence]"
            }
            close $fd
        }
    }
}

# ---------------------------------------------------------------------------------------------------
# The fsync sleep cap (collectFDs): while a WAIT or WAITAOF client waits and the everysec fsync is
# behind, main sleeps at most 1 ms per poll. With no-appendfsync-on-rewrite yes no fsync runs while a
# rewrite child is alive, so the cap holds for the whole rewrite: a few hundred iterations a second
# (measured about 230 per second on the audit host) against the idle timer rate.
# ---------------------------------------------------------------------------------------------------
start_server {tags {"fastpath aof external:skip tls:skip logreqres:skip"} overrides {io-threads 4 appendonly yes appendfsync everysec no-appendfsync-on-rewrite yes enable-debug-command yes save "" hz 10}} {
    r client setname far-control
    r select 0
    waitForBgrewriteaof r

    test {AOF review: everysec, a WAITAOF waiter during a no-fsync rewrite wakes the staged loop about every millisecond} {
        populate 4000 far:pop: 10
        far_wait_no_bio_fsync
        r config set rdb-key-save-delay 1000
        r bgrewriteaof
        wait_for_condition 50 20 {
            [s aof_rewrite_in_progress] == 1
        } else {
            fail "rewrite did not start"
        }
        # Idle rate: timers only.
        set a [far_fp_info staged_iterations]
        after 500
        set idle [expr {[far_fp_info staged_iterations] - $a}]
        # A write the rewrite's child blocks from being fsynced, and a client waiting for that fsync.
        set rd [valkey_deferring_client]
        $rd set far:wa 1
        assert_equal OK [$rd read]
        $rd waitaof 1 0 1500
        after 100
        set a [far_fp_info staged_iterations]
        after 500
        set waiting [expr {[far_fp_info staged_iterations] - $a}]
        assert_equal {0 0} [$rd read]
        $rd close
        if {$::verbose} { puts "staged iterations per 500 ms: idle $idle, with a WAITAOF waiter $waiting" }
        assert_lessthan $idle 100
        assert_morethan $waiting 50
        assert_morethan $waiting [expr {3 * $idle}]
        r config set rdb-key-save-delay 0
        waitForBgrewriteaof r
    }
}
