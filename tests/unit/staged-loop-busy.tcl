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
