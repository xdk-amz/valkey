# Parallel MAINT phase: rehash and resize run on the staged IO threads instead of inline in
# serverCron. With keyspace-shard-bits > 0 and io-threads > 1 the staged loop opens a MAINT phase
# when table work is due; cron-lanes toggles it against the inline path for an A/B in one binary.

proc maint_calls {} {
    set v [getInfoProperty [r info cronstats] cron_maint]
    if {[regexp {calls=(\d+)} $v -> n]} { return $n }
    return 0
}

proc rehashing_overhead {} {
    return [status r mem_overhead_db_hashtable_rehashing]
}

# No table is mid-rehash: INFO memory reports zero rehashing overhead.
proc wait_no_rehashing {} {
    wait_for_condition 200 50 {
        [rehashing_overhead] == 0
    } else {
        fail "rehashing did not finish: overhead [rehashing_overhead]"
    }
}

proc load_keys {n} {
    r eval {for i=1,tonumber(ARGV[1]) do redis.call('set', KEYS[1]..i, i) end return 'ok'} 1 k: $n
}

proc del_keys {from to} {
    r eval {for i=tonumber(ARGV[1]),tonumber(ARGV[2]) do redis.call('del', KEYS[1]..i) end return 'ok'} 1 k: $from $to
}

foreach lanes {yes no} {
start_server {tags {"maint external:skip tls:skip"} overrides {
    io-threads 4
    io-threads-fast-path yes
    io-threads-strict-offload yes
    keyspace-shard-bits 6
    activerehashing yes
    save ""
    enable-debug-command yes
}} {
    r config set cron-lanes $lanes

    test "MAINT ($lanes): rehashing finishes after growth under a read load" {
        r flushall
        load_keys 200000
        assert_equal 200000 [r dbsize]
        # Drive reads while the tables settle; rehashing must reach zero either way.
        for {set i 0} {$i < 200} {incr i} { r get k:[expr {$i*100+1}] }
        wait_no_rehashing
        assert_equal 200000 [r dbsize]
    }

    test "MAINT ($lanes): tables shrink after a mass delete and sizes stay consistent" {
        set before [status r used_memory]
        del_keys 1 180000
        assert_equal 20000 [r dbsize]
        wait_no_rehashing
        # DBSIZE and the keyspace line agree, and memory dropped as tables shrank.
        assert_equal 20000 [r dbsize]
        assert_match "*keys=20000*" [r info keyspace]
        assert {[status r used_memory] < $before}
    }

    test "MAINT ($lanes): phase counters move only when work is due" {
        r flushall
        r config resetstat
        after 500 ;# a few idle cron ticks with an empty keyspace
        if {$lanes eq "yes"} {
            # With one dirty table at most, an empty keyspace yields no due unit, so no phase opens.
            set idle [maint_calls]
            load_keys 150000
            wait_no_rehashing
            # Growth makes work due, so the MAINT phase opened at least once more.
            assert {[maint_calls] > $idle}
        } else {
            # Lanes off: the MAINT phase never opens, the inline resize/rehash counters move instead.
            assert_equal 0 [maint_calls]
            load_keys 150000
            wait_no_rehashing
            assert_equal 0 [maint_calls]
            assert {[getInfoProperty [r info cronstats] cron_rehash] ne ""}
        }
    }

    test "MAINT ($lanes): FLUSHDB ASYNC frees a large database while phases run on another" {
        r config set hz 100
        r config resetstat
        for {set i 0} {$i < 4} {incr i} {
            # db 0 holds keys expiring over the next second, so MAINT phases run back to back while
            # the bio thread releases db 9's old keyspace.
            r select 0
            r eval {for i=1,200000 do redis.call('set', 'v:'..i, i, 'px', 300 + i % 700) end return 1} 0
            r select 9
            r debug populate 1000000 big: 16
            after 300
            r flushdb async
            assert_equal 0 [r dbsize]
            wait_for_condition 100 50 {[status r lazyfree_pending_objects] == 0} else { fail "lazyfree did not drain" }
            r select 0
            wait_for_condition 100 50 {[r dbsize] == 0} else { fail "db 0 keys did not expire" }
            r select 9
        }
        r debug populate 1000 after: 16
        assert_equal 1000 [r dbsize]
        assert_match "*db9:keys=1000*" [r info keyspace]
        r config set hz 10
    }
}
}
