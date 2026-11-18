# OBS-08 / OBS-01 / OBS-07: the stock INFO stats counters must count fast-path traffic.
# The same fixed workload is sent on a raw socket to a server with io-threads 4 (fast path
# engaged, proven by the fastpath counters) and to a server with io-threads 1 (stock path).
# The deltas of the stock counters must be equal. CONFIG RESETSTAT must zero both the stock
# counters and the fast-path counters.

proc sic_resp {args} {
    set cmd "*[llength $args]\r\n"
    foreach a $args { append cmd "$[string length $a]\r\n$a\r\n" }
    return $cmd
}

proc sic_read_line_reply {fd} {
    set line [gets $fd]
    switch -- [string index $line 0] {
        {$} {
            if {$line eq {$-1}} { return {(nil)} }
            return [gets $fd]
        }
        default { return $line }
    }
}

proc sic_stats {c} {
    set info [$c info stats]
    set out [dict create]
    foreach f {total_net_input_bytes total_net_output_bytes total_reads_processed total_writes_processed
               keyspace_hits keyspace_misses total_error_replies total_commands_processed} {
        dict set out $f [getInfoProperty $info $f]
    }
    return $out
}

proc sic_delta {before after} {
    set out [dict create]
    dict for {k v} $after { dict set out $k [expr {$v - [dict get $before $k]}] }
    return $out
}

# The fixed workload: N round trips (one command per write, reply read before the next), so
# every server reads exactly N requests from this socket. Half the GETs hit, half miss, and
# one command in ten is an unknown command (an error reply).
proc sic_workload {host port n} {
    set fd [socket $host $port]
    fconfigure $fd -translation {crlf binary} -blocking 1 -buffering full
    puts -nonewline $fd [sic_resp set sic:hit v]
    flush $fd
    assert_equal {+OK} [sic_read_line_reply $fd]
    for {set i 0} {$i < $n} {incr i} {
        switch [expr {$i % 10}] {
            0 { set cmd [sic_resp sicnosuchcommand x] }
            1 - 2 - 3 - 4 { set cmd [sic_resp get sic:hit] }
            default { set cmd [sic_resp get sic:miss$i] }
        }
        puts -nonewline $fd $cmd
        flush $fd
        sic_read_line_reply $fd
    }
    close $fd
}

proc sic_fastpath_total {c} {
    set info [$c info fastpath]
    set sum 0
    foreach f {fastpath_reads fastpath_writes fastpath_batches fastpath_speculated fastpath_net_input_bytes
               fastpath_net_output_bytes fastpath_write_lane_entries} {
        incr sum [getInfoProperty $info $f]
    }
    return $sum
}

set ::sic_n 300

start_server {tags {"info superset external:skip tls:skip"} overrides {io-threads 1}} {
    r select 0
    set host [srv 0 host]
    set port [srv 0 port]
    r config resetstat
    set before [sic_stats r]
    sic_workload $host $port $::sic_n
    set ::sic_stock_delta [sic_delta $before [sic_stats r]]

    test {INFO counters: the stock path counts the fixed workload} {
        assert_equal $::sic_n [expr {[dict get $::sic_stock_delta keyspace_hits] + [dict get $::sic_stock_delta keyspace_misses] + [dict get $::sic_stock_delta total_error_replies]}]
        assert_morethan [dict get $::sic_stock_delta total_net_input_bytes] 0
    }
}

start_server {tags {"info superset external:skip tls:skip"} overrides {io-threads 4 io-threads-parallel-writes-min 1}} {
    r select 0
    set host [srv 0 host]
    set port [srv 0 port]

    test {INFO counters: fast-path traffic is counted in the stock stats (OBS-08)} {
        r config resetstat
        # fp_before first: its INFO request must not fall inside the stock-stats window.
        set fp_before [sic_fastpath_total r]
        set before [sic_stats r]
        sic_workload $host $port $::sic_n
        set delta [sic_delta $before [sic_stats r]]
        # Engagement: the raw socket ran on the fast path.
        assert_morethan [sic_fastpath_total r] $fp_before
        assert_morethan_equal [getInfoProperty [r info fastpath] fastpath_reads] $::sic_n
        foreach f {total_net_input_bytes total_reads_processed total_writes_processed
                   keyspace_hits keyspace_misses total_error_replies total_commands_processed} {
            assert_equal [dict get $::sic_stock_delta $f] [dict get $delta $f] "stat $f io-threads 1 vs 4"
        }
        # The window also holds the reply to the first INFO stats, whose length differs by a few bytes
        # between the two servers (its numbers differ); the workload replies are about 6.4 KB.
        set out1 [dict get $::sic_stock_delta total_net_output_bytes]
        assert_range [dict get $delta total_net_output_bytes] [expr {$out1 - 64}] [expr {$out1 + 64}] "stat total_net_output_bytes io-threads 1 ($out1) vs 4"

    }

    test {INFO counters: CONFIG RESETSTAT zeroes the fast-path counters (OBS-01/OBS-07)} {
        assert_morethan [sic_fastpath_total r] 0
        r config resetstat
        # The only fast-path traffic after the reset is the INFO request below itself: at most one
        # read of that request, no reply bytes counted yet, and no batches, writes or lane entries.
        set info [r info fastpath]
        set req_len [string length [sic_resp info fastpath]]
        assert_range [getInfoProperty $info fastpath_reads] 0 1 "fastpath_reads after RESETSTAT: $info"
        assert_range [getInfoProperty $info fastpath_net_input_bytes] 0 $req_len "input bytes after RESETSTAT: $info"
        foreach f {fastpath_writes fastpath_batches fastpath_speculated fastpath_net_output_bytes fastpath_write_lane_entries} {
            assert_equal 0 [getInfoProperty $info $f] "$f after RESETSTAT: $info"
        }
        assert_equal 0 [getInfoProperty [r info stats] keyspace_hits]
    }
}
