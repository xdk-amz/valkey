# io-threads-speculation-replica-only: with the flag set, a primary or
# standalone server never speculates reads on IO threads (stock read path);
# a replica does. The observable is fastget_speculated_commands in INFO fastget.

proc speculated {r} {
    set payload [$r info fastget]
    if {![regexp {fastget_speculated_commands:(\d+)} $payload -> value]} {
        fail "missing fastget_speculated_commands in INFO fastget"
    }
    return $value
}

# Pipeline GETs from a fresh client of server $level and return the change in
# speculated commands. The client sends nothing before its first GET: a SELECT would move it off the
# fast path, and only fast-path clients execute reads on their IO thread.
proc pump_gets {r key {level 0}} {
    set rd [valkey_deferring_client_by_addr [srv $level host] [srv $level port]]
    set before [speculated $r]
    for {set round 0} {$round < 20} {incr round} {
        for {set i 0} {$i < 10} {incr i} { $rd get $key }
        for {set i 0} {$i < 10} {incr i} { $rd read }
    }
    $rd close
    return [expr {[speculated $r] - $before}]
}

start_server {tags {"fastget-replica-only tls:skip"} overrides {io-threads 4 io-threads-always-active yes save {}}} {
    r select 0 ;# the same db as the fast-path clients
    r set ro:key v1

    test {REPLICA-ONLY: default (flag off) speculates on a standalone server} {
        assert_equal no [lindex [r config get io-threads-speculation-replica-only] 1]
        assert_morethan [pump_gets r ro:key] 0
    }

    test {REPLICA-ONLY: flag on, a standalone server does not speculate} {
        r config set io-threads-speculation-replica-only yes
        assert_equal 0 [pump_gets r ro:key]
        assert_equal v1 [r get ro:key]
    }

    start_server {overrides {io-threads 4 io-threads-always-active yes save {} io-threads-speculation-replica-only yes}} {
        set replica [srv 0 client]
        $replica select 0
        set primary_host [srv -1 host]
        set primary_port [srv -1 port]

        test {REPLICA-ONLY: flag on, a replica speculates} {
            $replica replicaof $primary_host $primary_port
            wait_for_condition 50 100 {
                [status $replica master_link_status] eq {up}
            } else {
                fail "replication link never came up"
            }
            wait_for_condition 50 100 {
                [$replica get ro:key] eq {v1}
            } else {
                fail "key never replicated"
            }
            assert_morethan [pump_gets $replica ro:key 0] 0
        }

        test {REPLICA-ONLY: promoted to primary, speculation stops} {
            $replica replicaof no one
            assert_equal 0 [pump_gets $replica ro:key]
            assert_equal v1 [$replica get ro:key]
        }
    }
}
