# A RESP3 tracking REDIRECT target can be a fast-path client (no name, not
# subscribed). Each invalidation must reach it exactly once. Main wrote the push into
# the client's output buffer while fast GET on an IO thread also sent that buffer, so
# the push went out twice (audit items SPT-14 / SPT-15).

start_server {tags {"tracking fastget tls:skip"} overrides {io-threads 4 io-threads-always-active yes save {}}} {
    test {Tracking invalidation to a fast-path REDIRECT target is delivered once} {
        r set g [string repeat x 100]
        r set tk 0
        set t [valkey_deferring_client]
        $t hello 3
        $t read
        $t client id
        set tid [$t read]
        for {set i 0} {$i < 20} {incr i} { $t get g }
        for {set i 0} {$i < 20} {incr i} { $t read }

        set w [valkey_client]
        $w client tracking on redirect $tid
        $w get tk
        set x [valkey_client]

        set n 50
        set replies 0
        set pushes 0
        for {set i 1} {$i <= $n} {incr i} {
            # GETs in flight on the fast path while the push is produced on main
            for {set j 0} {$j < 20} {incr j} { $t get g }
            $x set tk $i
            $w get tk
            set got 0
            while {$got < 20} {
                set reply [$t read]
                if {[lindex $reply 0] eq {invalidate}} {
                    incr pushes
                } else {
                    incr got
                    incr replies
                }
            }
        }
        $t ping
        while 1 {
            set reply [$t read]
            if {$reply eq {PONG}} break
            if {[lindex $reply 0] eq {invalidate}} { incr pushes }
        }
        regexp {fastpath_speculated:(\d+)} [r info fastpath] -> spec
        assert_morethan $spec 0
        $t close
        $w close
        $x close
        assert_equal $n $pushes
    }
}
