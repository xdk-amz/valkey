# Clients served while a busy script blocks main with the staged loop running.

proc slb_read_line_within {rd ms} {
    set fd [$rd channel]
    fconfigure $fd -blocking 0
    set line ""
    set deadline [expr {[clock milliseconds] + $ms}]
    while {[clock milliseconds] < $deadline} {
        set line [gets $fd]
        if {$line ne ""} break
        after 10
    }
    fconfigure $fd -blocking 1
    return [string trimright $line "\r"]
}

proc slb_kill_busy_script {} {
    set k [valkey [srv 0 host] [srv 0 port] 0 $::tls]
    catch {$k script kill}
    $k close
}

proc slb_raw_socket {} {
    set fd [socket [srv 0 host] [srv 0 port]]
    fconfigure $fd -translation binary -blocking 1 -buffering none
    return $fd
}

proc slb_raw_resp {args} {
    set cmd "*[llength $args]\r\n"
    foreach a $args { append cmd "$[string length $a]\r\n$a\r\n" }
    return $cmd
}

proc slb_raw_line {fd ms} {
    fconfigure $fd -blocking 0
    set deadline [expr {[clock milliseconds] + $ms}]
    set line ""
    while {[clock milliseconds] < $deadline} {
        set line [gets $fd]
        if {$line ne ""} break
        after 5
    }
    fconfigure $fd -blocking 1
    return [string trimright $line "\r"]
}

start_server {tags {"staged-loop external:skip"} overrides {enable-debug-command yes io-threads 2 lua-time-limit 10}} {
    test {Staged: SCRIPT KILL ends a busy script} {
        set a [valkey_deferring_client]
        set b [valkey_deferring_client]
        $a eval {while true do end} 0
        after 300
        $b script kill
        set kill [slb_read_line_within $b 3000]
        set script [slb_read_line_within $a 3000]
        if {$script eq ""} slb_kill_busy_script
        assert_equal +OK $kill
        assert_match {*killed by user*} $script
        $a close
        $b close
    }

    test {Staged: SCRIPT KILL read while main sleeps ends the script that follows} {
        set a [valkey_deferring_client]
        set b [valkey_deferring_client]
        set s [valkey_deferring_client]
        $s debug sleep 0.5
        after 100
        $a eval {while true do end} 0
        after 20
        $b script kill
        assert_equal OK [$s read]
        set script [slb_read_line_within $a 3000]
        if {$script eq ""} slb_kill_busy_script
        assert_match {*killed by user*} $script
        assert_equal +OK [slb_read_line_within $b 3000]
        $a close
        $b close
        $s close
    }
}

start_server {tags {"staged-loop external:skip"} overrides {enable-debug-command yes io-threads 2 lua-time-limit 10}} {
    test {A busy script and the SCRIPT KILL behind it, read in one phase, both reach main} {
        set s [slb_raw_socket]
        set a [slb_raw_socket]
        set b [slb_raw_socket]
        foreach fd [list $s $a $b] { puts -nonewline $fd [slb_raw_resp PING]; assert_equal +PONG [slb_raw_line $fd 2000] }
        wait_for_condition 100 20 {[getInfoProperty [r info fastpath] fastpath_clients] >= 3} else {
            fail "clients did not join the fast path"
        }
        # Main sleeps, so A's EVAL and then B's SCRIPT KILL are read in the same phase and leave together.
        puts -nonewline $s [slb_raw_resp DEBUG SLEEP 0.5]
        after 100
        puts -nonewline $a [slb_raw_resp EVAL {while true do end} 0]
        after 20
        puts -nonewline $b [slb_raw_resp SCRIPT KILL]
        assert_equal +OK [slb_raw_line $s 3000]
        set reply [slb_raw_line $a 3000]
        if {$reply eq ""} { catch {r script kill} }
        assert_match {*killed by user*} $reply
        assert_equal +OK [slb_raw_line $b 3000]
        assert_equal PONG [r ping]
        foreach fd [list $s $a $b] { close $fd }
    }
}
