# Fast GET read correctness: GETs executed on IO threads must return complete,
# current values while writes, expiry churn and rehashing run on main.

proc fastget_speculated {r} {
    set payload [$r info fastget]
    if {![regexp {fastget_speculated_commands:(\d+)} $payload -> value]} {
        fail "missing fastget_speculated_commands in INFO fastget"
    }
    return $value
}

# A deferring client that sends nothing before its first command: a SELECT would move
# it off the fast path, and only fast-path clients execute reads on their IO thread.
proc fastget_client {} {
    return [valkey_deferring_client_by_addr [srv 0 host] [srv 0 port]]
}

# Pump pipelined GETs until the IO-thread read path demonstrably engages for this client.
proc ensure_engaged {r rd key} {
    set before [fastget_speculated $r]
    for {set round 0} {$round < 20} {incr round} {
        for {set i 0} {$i < 10} {incr i} { $rd get $key }
        for {set i 0} {$i < 10} {incr i} { $rd read }
        if {[fastget_speculated $r] > $before} return
    }
    fail "IO-thread read path never engaged for test client"
}

start_server {tags {"fastget-correctness"} overrides {io-threads 4 save {}}} {
    r select 0 ;# the same db as the fast-path clients

    test {T-RACE-SET-COHERENCE: reads racing writes always return complete values} {
        set big1 [string repeat A 64]
        set big2 [string repeat B 64]
        r set race:a $big1
        set rd [fastget_client]
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
        set rd [fastget_client]
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
        set before [fastget_speculated r]
        set rd [fastget_client]
        for {set i 0} {$i < 50} {incr i} { $rd get final }
        for {set i 0} {$i < 50} {incr i} { assert_equal "fval" [$rd read] }
        $rd get astr
        assert_equal 1000 [string length [$rd read]]
        $rd close
        wait_for_condition 100 50 {
            [fastget_speculated r] > $before
        } else {
            fail "IO-thread reads did not engage after mixed load"
        }
    }

    test {T-REHASH-WINDOW: reads racing active rehash serve current values, never crash} {
        r flushall
        for {set i 0} {$i < 20000} {incr i} { r set rh:$i v$i }
        set rd [fastget_client]
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

# SISMEMBER and HGET answered on the reading thread match main's replies in both protocols, and engage only
# with io-threads-speculate-reads.
start_server {tags {"fastget-correctness"} overrides {io-threads 4 save {}}} {
    r select 0

    proc spec_reads_round {rd} {
        set out {}
        foreach cmd {
            {sismember s:small a} {sismember s:small zz} {sismember s:small 3} {sismember s:big m7}
            {sismember s:big none} {sismember s:none a}
        } { $rd {*}$cmd }
        for {set i 0} {$i < 6} {incr i} { lappend out [$rd read] }
        foreach cmd {
            {hget h:small f} {hget h:small n} {hget h:small none} {hget h:big f9} {hget h:none f}
        } { $rd {*}$cmd }
        for {set i 0} {$i < 5} {incr i} { lappend out [$rd read] }
        $rd hget s:small x
        lappend out [catch {$rd read} e] [string range $e 0 8]
        return $out
    }

    test {SISMEMBER and HGET on the reading thread match main in RESP2 and RESP3} {
        r sadd s:small a b 3
        for {set i 0} {$i < 300} {incr i} { r sadd s:big m$i; r hset h:big f$i v$i }
        r hset h:small f v n 42
        set want {1 0 1 1 0 0 v 42 {} v9 {} 1 WRONGTYPE}
        foreach proto {2 3} {
            foreach mode {no yes} {
                r config set io-threads-speculate-reads $mode
                set rd [fastget_client]
                $rd hello $proto
                $rd read
                set before [fastget_speculated r]
                for {set round 0} {$round < 20} {incr round} { assert_equal $want [spec_reads_round $rd] }
                set moved [expr {[fastget_speculated r] > $before}]
                assert_equal [expr {$mode eq "yes"}] $moved
                $rd close
            }
        }
        assert_match {*calls=*} [cmdrstat sismember r]
    }

    test {HGET on a hash with expiring fields goes to main and stays correct} {
        r config set io-threads-speculate-reads yes
        r del h:ttl
        r hset h:ttl f v g w
        r hexpire h:ttl 100000 FIELDS 1 g
        set rd [fastget_client]
        set before [fastget_speculated r]
        for {set round 0} {$round < 20} {incr round} {
            for {set i 0} {$i < 8} {incr i} { $rd hget h:ttl f }
            for {set i 0} {$i < 8} {incr i} { assert_equal v [$rd read] }
        }
        assert_equal $before [fastget_speculated r]
        $rd close
    }
}

# MGET answered on the reading thread matches main's replies, including keys that send it to main.
start_server {tags {"fastget-correctness"} overrides {io-threads 4 save {}}} {
    r select 0

    test {MGET on the reading thread matches main in RESP2 and RESP3} {
        for {set i 0} {$i < 20} {incr i} { r set mg:$i v$i }
        r set mg:int 12345
        r set mg:big [string repeat x 2000]
        r rpush mg:list a
        set cmds {
            {mget mg:0 mg:1 mg:2} {mget mg:3 mg:int mg:4} {mget mg:5 mg:none mg:6} {mget mg:7 mg:list}
            {mget mg:8 mg:big} {mget mg:0 mg:1 mg:2 mg:3 mg:4 mg:5 mg:6 mg:7 mg:8 mg:9 mg:10 mg:11 mg:12 mg:13 mg:14 mg:15 mg:16}
            {mget mg:9}
        }
        foreach proto {2 3} {
            set ref {}
            foreach mode {no yes} {
                r config set io-threads-speculate-reads $mode
                set rd [fastget_client]
                $rd hello $proto
                $rd read
                set before [fastget_speculated r]
                set out {}
                for {set round 0} {$round < 20} {incr round} {
                    foreach c $cmds { $rd {*}$c }
                    foreach c $cmds { lappend out [$rd read] }
                    for {set i 0} {$i < 8} {incr i} { $rd mget mg:$i mg:[expr {$i + 1}] }
                    for {set i 0} {$i < 8} {incr i} { lappend out [$rd read] }
                }
                if {$mode eq "no"} { set ref $out } else { assert_equal $ref $out }
                assert_equal [expr {$mode eq "yes"}] [expr {[fastget_speculated r] > $before}]
                $rd close
            }
        }
        assert_equal {v0 v1 {}} [r mget mg:0 mg:1 mg:none]
    }
}

# Keys answered on the reading thread are touched as main touches them: LRU idle time and LFU frequency
# follow the reads, so eviction keeps a key that is only ever read there. Hot-key detection on sends
# reads to main, which counts them.
proc fastget_fill {prefix n} {
    set val [string repeat c 1000]
    for {set i 0} {$i < $n} {incr i} { r set $prefix:$i $val }
}

# Write new keys until at least min_evicted keys are evicted; returns the count.
proc fastget_evict {min_evicted} {
    set val [string repeat n 1000]
    set base [s evicted_keys]
    for {set i 0} {$i < 20000} {incr i} {
        r set new:$i $val
        if {$i % 100 == 99 && [s evicted_keys] - $base >= $min_evicted} break
    }
    return [expr {[s evicted_keys] - $base}]
}

start_server {tags {"fastget-correctness"} overrides {io-threads 4 save {} maxmemory-policy allkeys-lfu lfu-log-factor 0 lfu-decay-time 0}} {
    r select 0
    r config set io-threads-speculate-reads yes

    test {LFU: GET, SISMEMBER, HGET and MGET on the reading thread raise the key's frequency} {
        r set lfu:hot v
        r set lfu:cold v
        r sadd lfu:set a b
        r hset lfu:hash f v
        r set lfu:m1 v
        r set lfu:m2 v
        r set lfu:warm v
        set rd [fastget_client]
        ensure_engaged r $rd lfu:warm
        set before [fastget_speculated r]
        foreach cmd {{get lfu:hot} {sismember lfu:set a} {hget lfu:hash f} {mget lfu:m1 lfu:m2}} {
            for {set round 0} {$round < 10} {incr round} {
                for {set i 0} {$i < 20} {incr i} { $rd {*}$cmd }
                for {set i 0} {$i < 20} {incr i} { $rd read }
            }
        }
        $rd close
        # Every command of the reads above ran on the reading thread.
        assert_equal 800 [expr {[fastget_speculated r] - $before}]
        # lfu-log-factor 0: each touch adds one, from the initial 5.
        foreach key {lfu:hot lfu:set lfu:hash lfu:m1 lfu:m2} { assert_equal 205 [r object freq $key] }
        assert_equal 5 [r object freq lfu:cold]
    }

    test {LFU: a key read only on the reading thread survives allkeys-lfu eviction} {
        fastget_fill lfuc 2000
        r config set maxmemory [expr {[s used_memory] + 100000}]
        set evicted [fastget_evict 1000]
        r config set maxmemory 0
        assert_range $evicted 1000 1999
        assert_equal 1 [r exists lfu:hot]
        assert_equal 1 [r exists lfu:set]
        assert_equal 1 [r exists lfu:hash]
    }

    test {CLIENT NO-TOUCH reads leave the frequency alone} {
        r set lfu:nt v
        set rd [fastget_client]
        $rd client no-touch on
        $rd read
        for {set i 0} {$i < 50} {incr i} { $rd get lfu:nt }
        for {set i 0} {$i < 50} {incr i} { $rd read }
        $rd close
        assert_equal 5 [r object freq lfu:nt]
    }
}

start_server {tags {"fastget-correctness"} overrides {io-threads 4 save {} maxmemory-policy allkeys-lru}} {
    r select 0
    r config set io-threads-speculate-reads yes

    test {LRU: reads on the reading thread reset idle time, and the key survives allkeys-lru eviction} {
        r set lru:hot v
        r sadd lru:set a
        fastget_fill lruc 2000
        r set lru:warm v
        set rd [fastget_client]
        ensure_engaged r $rd lru:warm
        after 2100
        set before [fastget_speculated r]
        foreach cmd {{get lru:hot} {sismember lru:set a}} {
            for {set i 0} {$i < 50} {incr i} { $rd {*}$cmd }
            for {set i 0} {$i < 50} {incr i} { $rd read }
        }
        $rd close
        assert_equal 100 [expr {[fastget_speculated r] - $before}]
        assert_range [r object idletime lru:hot] 0 1
        assert_range [r object idletime lru:set] 0 1
        assert {[r object idletime lruc:0] >= 2}
        r config set maxmemory [expr {[s used_memory] + 100000}]
        set evicted [fastget_evict 1000]
        r config set maxmemory 0
        assert_range $evicted 1000 1999
        assert_equal 1 [r exists lru:hot]
        assert_equal 1 [r exists lru:set]
    }
}

start_server {tags {"fastget-correctness"} overrides {io-threads 4 save {}}} {
    r select 0

    test {With hot-key detection on, reads go to main and the hot key is reported} {
        r config set hotkeys-top-k 16
        r config set hotkeys-sampling-percentage 100
        r config set hotkeys-window-seconds 1
        r hotkeys reset
        r set hk:hot v
        set rd [fastget_client]
        set before [fastget_speculated r]
        for {set round 0} {$round < 10} {incr round} {
            for {set i 0} {$i < 30} {incr i} { $rd get hk:hot }
            for {set i 0} {$i < 30} {incr i} { assert_equal v [$rd read] }
        }
        assert_equal $before [fastget_speculated r]
        wait_for_condition 50 100 {
            [string match {*hk:hot*} [r hotkeys get]]
        } else {
            fail "hk:hot was not reported by HOTKEYS GET"
        }
        # Detection off: the same client's reads run on the reading thread again.
        r config set hotkeys-top-k 0
        ensure_engaged r $rd hk:hot
        $rd close
    }
}
