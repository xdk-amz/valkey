# D+ read correctness: GETs executed on IO threads must return complete,
# current values while writes, expiry churn and rehashing run on main.

proc dplus_speculated {r} {
    set payload [$r info dplus]
    if {![regexp {dplus_speculated_commands:(\d+)} $payload -> value]} {
        fail "missing dplus_speculated_commands in INFO dplus"
    }
    return $value
}

# A deferring client that sends nothing before its first command: a SELECT would move
# it off the fast path, and only fast-path clients execute reads on their IO thread.
proc dplus_client {} {
    return [valkey_deferring_client_by_addr [srv 0 host] [srv 0 port]]
}

# Pump pipelined GETs until the IO-thread read path demonstrably engages for this client.
proc ensure_engaged {r rd key} {
    set before [dplus_speculated $r]
    for {set round 0} {$round < 20} {incr round} {
        for {set i 0} {$i < 10} {incr i} { $rd get $key }
        for {set i 0} {$i < 10} {incr i} { $rd read }
        if {[dplus_speculated $r] > $before} return
    }
    fail "IO-thread read path never engaged for test client"
}

start_server {tags {"dplus-correctness"} overrides {io-threads 4 save {}}} {
    r select 0 ;# the same db as the fast-path clients

    test {T-RACE-SET-COHERENCE: reads racing writes always return complete values} {
        set big1 [string repeat A 64]
        set big2 [string repeat B 64]
        r set race:a $big1
        set rd [dplus_client]
        ensure_engaged r $rd race:a
        for {set round 0} {$round < 200} {incr round} {
            for {set i 0} {$i < 8} {incr i} { $rd get race:a }
            if {$round % 2} { r set race:a $big1 } else { r set race:a $big2 }
            for {set i 0} {$i < 8} {incr i} {
                set got [$rd read]
                assert {$got eq $big1 || $got eq $big2}
            }
        }
        $rd close
    }

    test {T-RACE-EXPIRE-SAFETY: reads racing first-time expiry churn never crash or tear} {
        set rd [dplus_client]
        r set race:e keepme
        ensure_engaged r $rd race:e
        for {set round 0} {$round < 100} {incr round} {
            for {set i 0} {$i < 8} {incr i} { $rd get race:e }
            r expire race:e 1000
            r persist race:e         ;# next round's expire is first-time again
            for {set i 0} {$i < 8} {incr i} { assert_equal "keepme" [$rd read] }
        }
        assert_equal "PONG" [r ping]
        $rd close
    }

    test {T-MIXED-LOAD: reads still engage and stay correct after mixed writes} {
        for {set i 0} {$i < 500} {incr i} {
            r set p:$i v$i
            r expire p:$i 1000
            r incr ctr
            r append astr xx
            r setrange astr 0 yy
            r del p:$i
        }
        r set final fval
        set before [dplus_speculated r]
        set rd [dplus_client]
        for {set i 0} {$i < 50} {incr i} { $rd get final }
        for {set i 0} {$i < 50} {incr i} { assert_equal "fval" [$rd read] }
        $rd get astr
        assert_equal 1000 [string length [$rd read]]
        $rd close
        wait_for_condition 100 50 {
            [dplus_speculated r] > $before
        } else {
            fail "IO-thread reads did not engage after mixed load"
        }
    }

    test {T-REHASH-WINDOW: reads racing active rehash serve current values, never crash} {
        r flushall
        for {set i 0} {$i < 20000} {incr i} { r set rh:$i v$i }
        set rd [dplus_client]
        for {set round 0} {$round < 20} {incr round} {
            set base [expr {$round * 200}]
            for {set i 0} {$i < 200} {incr i} { $rd get rh:[expr {$base + $i}] }
            for {set i 0} {$i < 200} {incr i} {
                set k [expr {$base + $i}]
                set got [$rd read]
                # Keys below round*500 were deleted by earlier rounds.
                if {$k < $round * 500} { assert_equal {} $got } else { assert_equal v$k $got }
            }
            for {set i 0} {$i < 500} {incr i} { r del rh:[expr {$round * 500 + $i}] }
        }
        $rd close
        assert_equal "PONG" [r ping]
    }
}
