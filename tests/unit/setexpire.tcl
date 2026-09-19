set ::live_member_ttl 100

proc flush_and_disable_active_expiry {} {
    r FLUSHALL
    r DEBUG SET-ACTIVE-EXPIRE 0
}

proc create_numbered_set {key n} {
    set members {}
    for {set i 0} {$i < $n} {incr i} { lappend members m$i }
    r SADD $key {*}$members
}

proc assert_none_of {got forbidden} {
    foreach m $got {
        assert_equal -1 [lsearch -exact $forbidden $m] "unexpected member $m"
    }
}

# Test members fit set-max-listpack-value, so this limit selects the encoding.
proc use_set_encoding {encoding} {
    switch $encoding {
        hashtable { r config set set-max-listpack-entries 0 }
        listpack { r config set set-max-listpack-entries 128 }
        default { error "unknown set encoding $encoding" }
    }
}

proc set_member_ttl {r key member} {
    return [lindex [$r STTL $key MEMBERS 1 $member] 0]
}

proc set_member_pttl {r key member} {
    return [lindex [$r SPTTL $key MEMBERS 1 $member] 0]
}

proc set_member_pexpiretime {r key member} {
    return [lindex [$r SPEXPIRETIME $key MEMBERS 1 $member] 0]
}

# Requires active expiry off, so the expired members stay in the set.
proc make_members_expired {r key members} {
    $r SPEXPIRE $key 1 MEMBERS [llength $members] {*}$members
    foreach member $members {
        wait_for_condition 100 10 {
            [$r SISMEMBER $key $member] == 0
        } else {
            fail "member $member of $key did not expire"
        }
    }
}

proc wait_for_set_active_expiry {r key expected_card initial_expired expected_increment} {
    wait_for_condition 100 100 {
        [$r SCARD $key] == $expected_card &&
        [status $r expired_set_members] == ($initial_expired + $expected_increment)
    } else {
        set got [status $r expired_set_members]
        set want [expr {$initial_expired + $expected_increment}]
        fail "Active expiry did not happen: SCARD [$r SCARD $key] (want $expected_card), expired_set_members $got (want $want)"
    }
}

# Return {events dirty_delta watch_fired}.
proc capture_mutation_signals {key body} {
    set rd [setup_single_keyspace_notification r]
    set watcher [valkey_client]
    $watcher watch $key
    $watcher multi
    $watcher ping

    set dirty [s rdb_changes_since_last_save]
    uplevel 1 $body
    set dirty_delta [expr {[s rdb_changes_since_last_save] - $dirty}]
    set watch_fired [expr {[$watcher exec] eq {}}]
    $watcher close

    # A marker event tells the reader where the events of $body end.
    r SADD mutation-marker x
    set events {}
    while 1 {
        set event [$rd read]
        set name [lindex [split [lindex $event 2] ":"] end]
        set event_key [lindex $event 3]
        if {$name eq "sadd" && $event_key eq "mutation-marker"} break
        lappend events "$name $event_key"
    }
    $rd close
    r DEL mutation-marker
    return [list $events $dirty_delta $watch_fired]
}

# The first byte of a DUMP payload is the RDB type.
proc dump_payload_type {payload} {
    binary scan [string index $payload 0] cu type
    return $type
}

proc seed_expired_member {r key} {
    $r SADDEX $key EX $::live_member_ttl MEMBERS 1 live
    $r SADD $key expired
    make_members_expired $r $key expired
}

# Build a set whose expired members are hidden but not yet reclaimable. Above 127
# volatile members the expiry index is a RAX whose bucket key is the deadline
# rounded up to an 8192 ms window, and it reports that key as the earliest
# expiry, so for the rest of the window the index claims nothing has expired
# while every hidden member is still there. Returns the live member names.
proc make_rax_window_set {r key hidden live} {
    # Enter a window with room left for the deadline to pass and the test body to
    # run before the window ends, which is what keeps the index's answer stale.
    while {[clock milliseconds] % 8192 >= 1024} { after 50 }

    set hidden_members {}
    for {set i 0} {$i < $hidden} {incr i} { lappend hidden_members h$i }
    set deadline [expr {[clock milliseconds] + 300}]
    # The index files the deadline under the end of the window containing it.
    set ::rax_window_end [expr {($deadline / 8192) * 8192 + 8192}]
    $r SADDEX $key PXAT $deadline MEMBERS [llength $hidden_members] {*}$hidden_members

    set live_members {}
    for {set i 0} {$i < $live} {incr i} { lappend live_members live$i }
    if {$live > 0} { $r SADD $key {*}$live_members }

    wait_for_condition 100 10 {
        [$r SISMEMBER $key [lindex $hidden_members 0]] == 0 &&
        [$r SISMEMBER $key [lindex $hidden_members end]] == 0
    } else {
        fail "the hidden members of $key did not expire"
    }
    assert_equal [expr {$hidden + $live}] [$r SCARD $key]
    assert_equal $live [llength [$r SMEMBERS $key]]
    assert_inside_rax_window
    return $live_members
}

# Outside the window the index answers correctly and proves nothing, so a run
# that drifts past it must fail rather than pass for the wrong reason.
proc assert_inside_rax_window {} {
    set remaining [expr {$::rax_window_end - [clock milliseconds]}]
    assert_equal 1 [expr {$remaining > 0}] \
        "the expiry-index window ended [expr {-$remaining}] ms ago, so this run proves nothing"
}

# Every returned member must be one of the live ones the fixture created.
proc assert_all_live {got live_members} {
    foreach m $got {
        assert_equal 1 [expr {[lsearch -exact $live_members $m] != -1}] "returned hidden member $m"
    }
}

# The primary stays populated so pre-sync setup is included in the full sync.
proc attach_replica {primary replica primary_host primary_port} {
    $replica replicaof $primary_host $primary_port
    wait_for_sync $replica
    wait_for_ofs_sync $primary $replica
}

# Hand $key's slot from node $from to node $to, moving the key with it.
proc migrate_key_slot {from to key} {
    set slot [R $from CLUSTER KEYSLOT $key]
    set from_id [R $from CLUSTER MYID]
    set to_id [R $to CLUSTER MYID]

    assert_equal {OK} [R $from CLUSTER SETSLOT $slot MIGRATING $to_id]
    assert_equal {OK} [R $to CLUSTER SETSLOT $slot IMPORTING $from_id]
    assert_equal {OK} [R $from MIGRATE [srv [expr {-$to}] host] [srv [expr {-$to}] port] $key 0 5000]
    assert_equal {OK} [R $to CLUSTER SETSLOT $slot NODE $to_id]
    assert_equal {OK} [R $from CLUSTER SETSLOT $slot NODE $to_id]
}

start_server {tags {"setexpire"}} {
    test {SEXPIRE - reply 1 when the TTL is applied} {
        r FLUSHALL
        r SADD myset a b c
        assert_equal {1 1 1} [r SEXPIRE myset $::live_member_ttl MEMBERS 3 a b c]
    }

    test {SEXPIRE - reply -2 for a missing key} {
        r FLUSHALL
        assert_equal {-2} [r SEXPIRE nosuchkey $::live_member_ttl MEMBERS 1 a]
        assert_equal 0 [r EXISTS nosuchkey]
    }

    test {SEXPIRE - reply -2 for a missing member of an existing key} {
        r FLUSHALL
        r SADD myset a b c
        assert_equal {1 -2} [r SEXPIRE myset $::live_member_ttl MEMBERS 2 a nomember]
        assert_equal 3 [r SCARD myset]
    }

    test {SEXPIRE - NX applies only to a member without a TTL} {
        r FLUSHALL
        r SADD myset a b
        assert_equal {1} [r SEXPIRE myset $::live_member_ttl MEMBERS 1 a]
        set original [set_member_pexpiretime r myset a]
        assert_equal {0 1} [r SEXPIRE myset $::live_member_ttl NX MEMBERS 2 a b]
        assert_equal $original [set_member_pexpiretime r myset a]
        assert_morethan [set_member_ttl r myset b] 0
    }

    test {SEXPIRE - XX applies only to a member that has a TTL} {
        r FLUSHALL
        r SADD myset a b
        assert_equal {1} [r SEXPIRE myset $::live_member_ttl MEMBERS 1 a]
        set original [set_member_pexpiretime r myset a]
        assert_equal {1 0} [r SEXPIRE myset [expr {2 * $::live_member_ttl}] XX MEMBERS 2 a b]
        assert_morethan [set_member_pexpiretime r myset a] $original
        assert_equal -1 [set_member_ttl r myset b]
    }

    test {SEXPIRE - GT applies only a greater expiration} {
        r FLUSHALL
        r SADD myset a
        assert_equal {1} [r SEXPIRE myset $::live_member_ttl MEMBERS 1 a]
        set original [set_member_pexpiretime r myset a]
        assert_equal {0} [r SEXPIRE myset 1 GT MEMBERS 1 a]
        assert_equal $original [set_member_pexpiretime r myset a]
        assert_equal {1} [r SEXPIRE myset [expr {2 * $::live_member_ttl}] GT MEMBERS 1 a]
        assert_morethan [set_member_pexpiretime r myset a] $original
    }

    test {SEXPIRE - LT applies only a smaller expiration} {
        r FLUSHALL
        r SADD myset a
        assert_equal {1} [r SEXPIRE myset $::live_member_ttl MEMBERS 1 a]
        set original [set_member_pexpiretime r myset a]
        assert_equal {0} [r SEXPIRE myset [expr {2 * $::live_member_ttl}] LT MEMBERS 1 a]
        assert_equal $original [set_member_pexpiretime r myset a]
        assert_equal {1} [r SEXPIRE myset 1 LT MEMBERS 1 a]
        assert_lessthan [set_member_pexpiretime r myset a] $original
    }

    test {SEXPIRE - GT and LT treat a member with no TTL as infinite} {
        r FLUSHALL
        r SADD myset a b
        assert_equal {0} [r SEXPIRE myset $::live_member_ttl GT MEMBERS 1 a]
        assert_equal -1 [set_member_ttl r myset a]
        assert_equal {1} [r SEXPIRE myset $::live_member_ttl LT MEMBERS 1 b]
        assert_morethan [set_member_ttl r myset b] 0
    }

    test {SEXPIRE - reply 2 and member removal for a time in the past} {
        r FLUSHALL
        r SADD myset a b c
        assert_equal {2} [r SEXPIRE myset 0 MEMBERS 1 a]
        assert_equal -2 [set_member_ttl r myset a]
        assert_equal {b c} [lsort [r SMEMBERS myset]]
    }

    test {SEXPIRE - the key is deleted when a past expiration removes the last member} {
        r FLUSHALL
        r SADD myset a
        assert_equal {2} [r SEXPIRE myset 0 MEMBERS 1 a]
        assert_equal 0 [r EXISTS myset]
        assert_equal 0 [get_keys_with_volatile_items r]
    }

    test {SEXPIRE - a negative relative TTL is rejected} {
        r FLUSHALL
        r SADD myset a
        assert_error "*invalid expire time*" {r SEXPIRE myset -10 MEMBERS 1 a}
        assert_equal 1 [r SISMEMBER myset a]
    }

    test {SEXPIRE - duplicate members get one reply element each} {
        r FLUSHALL
        r SADD myset a
        assert_equal {1 1} [r SEXPIRE myset $::live_member_ttl MEMBERS 2 a a]
    }

    test {SEXPIRE - WRONGTYPE against a non-set key} {
        r FLUSHALL
        r SET mystr hello
        assert_error "WRONGTYPE*" {r SEXPIRE mystr $::live_member_ttl MEMBERS 1 a}
    }

    test {SEXPIRE - MEMBERS count validation} {
        r FLUSHALL
        r SADD myset a b c
        assert_error "*greater than 0 and match the provided number*" \
            {r SEXPIRE myset $::live_member_ttl MEMBERS 2 a}
        assert_error "*greater than 0 and match the provided number*" \
            {r SEXPIRE myset $::live_member_ttl MEMBERS 0 a}
        assert_error "*not an integer or out of range*" \
            {r SEXPIRE myset $::live_member_ttl MEMBERS notanumber a}
    }

    test {SPEXPIRE - the argument is relative milliseconds} {
        r FLUSHALL
        r SADD myset a
        set before_ms [clock milliseconds]
        assert_equal {1} [r SPEXPIRE myset 100000 MEMBERS 1 a]
        set after_ms [clock milliseconds]
        assert_range [set_member_pexpiretime r myset a] \
            [expr {$before_ms + 100000}] [expr {$after_ms + 100000}]
    }

    test {SEXPIREAT - the argument is an absolute unix time in seconds} {
        r FLUSHALL
        r SADD myset a
        set at_s [expr {[clock seconds] + 100}]
        assert_equal {1} [r SEXPIREAT myset $at_s MEMBERS 1 a]
        assert_equal [expr {$at_s * 1000}] [set_member_pexpiretime r myset a]
    }

    test {SPEXPIREAT - the argument is an absolute unix time in milliseconds} {
        r FLUSHALL
        r SADD myset a
        set at_ms [expr {[clock milliseconds] + 100000}]
        assert_equal {1} [r SPEXPIREAT myset $at_ms MEMBERS 1 a]
        assert_equal $at_ms [set_member_pexpiretime r myset a]
    }

    test {STTL - mixed -2, -1 and value replies keep member order} {
        r FLUSHALL
        assert_equal {-2} [r STTL nosuchkey MEMBERS 1 a]
        r SADD myset a b
        assert_equal {1} [r SEXPIRE myset $::live_member_ttl MEMBERS 1 a]
        set res [r STTL myset MEMBERS 3 a b nomember]
        assert_equal 3 [llength $res]
        assert_morethan [lindex $res 0] 0
        assert_equal -1 [lindex $res 1]
        assert_equal -2 [lindex $res 2]
    }

    test {STTL and SPTTL - value ranges after a relative expiration} {
        r FLUSHALL
        r SADD myset a
        assert_equal {1} [r SEXPIRE myset 100 MEMBERS 1 a]
        assert_range [set_member_ttl r myset a] 90 100
        assert_range [set_member_pttl r myset a] 90000 100000
    }

    test {SEXPIRETIME rounds to the nearest second} {
        r FLUSHALL
        r SADD myset a
        # A .7s stamp, so the rounded second differs from the truncated one.
        set at_ms [expr {([clock milliseconds] + 100000) / 1000 * 1000 + 700}]
        assert_equal {1} [r SPEXPIREAT myset $at_ms MEMBERS 1 a]
        assert_equal [expr {$at_ms / 1000 + 1}] [lindex [r SEXPIRETIME myset MEMBERS 1 a] 0]
    }

    test {Set TTL readers - WRONGTYPE against a non-set key} {
        r FLUSHALL
        r SET mystr hello
        assert_error "WRONGTYPE*" {r STTL mystr MEMBERS 1 a}
    }

    test {Set TTL readers - MEMBERS count validation} {
        r FLUSHALL
        r SADD myset a b
        foreach reader {STTL SPTTL SEXPIRETIME SPEXPIRETIME} {
            assert_error "*greater than 0 and match the provided number*" {r $reader myset MEMBERS 1 a b}
            assert_error "*greater than 0 and match the provided number*" {r $reader myset MEMBERS 0 a}
            assert_error "*not an integer or out of range" {r $reader myset MEMBERS a b}
        }
    }

    test {SPERSIST - -2 for a missing key} {
        r FLUSHALL
        assert_equal {-2} [r SPERSIST nosuchkey MEMBERS 1 a]
    }

    test {SPERSIST - 1 when a TTL is removed and the readers report -1 afterwards} {
        r FLUSHALL
        r SADD myset a b c
        assert_equal {1 1} [r SEXPIRE myset $::live_member_ttl MEMBERS 2 a b]
        assert_equal {1} [r SPERSIST myset MEMBERS 1 a]
        assert_equal -1 [set_member_ttl r myset a]
        assert_morethan [set_member_ttl r myset b] 0
        assert_equal 3 [r SCARD myset]
    }

    test {SPERSIST - mixed replies keep member order} {
        r FLUSHALL
        r SADD myset a b
        assert_equal {1} [r SEXPIRE myset $::live_member_ttl MEMBERS 1 a]
        assert_equal {1 -1 -2} [r SPERSIST myset MEMBERS 3 a b nomember]
    }

    test {SPERSIST - the key stops being tracked when its last TTL is removed} {
        r FLUSHALL
        r SADD myset a b
        assert_equal {1 1} [r SEXPIRE myset $::live_member_ttl MEMBERS 2 a b]
        assert_equal 1 [get_keys_with_volatile_items r]
        assert_equal {1} [r SPERSIST myset MEMBERS 1 a]
        assert_equal 1 [get_keys_with_volatile_items r]
        assert_equal {1} [r SPERSIST myset MEMBERS 1 b]
        assert_equal 0 [get_keys_with_volatile_items r]
    }

    test {SPERSIST - WRONGTYPE and argument errors} {
        r FLUSHALL
        r SET mystr hello
        r SADD myset a b
        assert_error "WRONGTYPE*" {r SPERSIST mystr MEMBERS 1 a}
        assert_error "*ERR syntax error" {r SPERSIST myset a 1 b}
        assert_error "*not an integer or out of range" {r SPERSIST myset MEMBERS a b}
        assert_error "*greater than 0 and match the provided number*" \
            {r SPERSIST myset MEMBERS 3 a b}
        assert_error "*greater than 0 and match the provided number*" \
            {r SPERSIST myset MEMBERS 0 a}
    }

    test {SADDEX EX - the reply is the added count and the TTL is set} {
        r FLUSHALL
        assert_equal 2 [r SADDEX myset EX $::live_member_ttl MEMBERS 2 a b]
        assert_morethan [set_member_ttl r myset a] 0
        assert_morethan [set_member_ttl r myset b] 0
        assert_equal 1 [get_keys_with_volatile_items r]
    }

    test {SADDEX EX - an existing member is not counted but its TTL is updated} {
        r FLUSHALL
        r SADD myset a
        assert_equal 1 [r SADDEX myset EX $::live_member_ttl MEMBERS 2 a b]
        assert_morethan [set_member_ttl r myset a] 0
        assert_equal 0 [r SADDEX myset EX [expr {2 * $::live_member_ttl}] MEMBERS 2 a b]
        assert_morethan [set_member_ttl r myset a] $::live_member_ttl
    }

    test {SADDEX EX - an existing intset member receives the TTL} {
        r FLUSHALL
        use_set_encoding listpack
        r SADD myset 1
        assert_encoding intset myset
        assert_equal 0 [r SADDEX myset EX $::live_member_ttl MEMBERS 1 1]
        assert_encoding listpack myset
        assert_morethan [set_member_ttl r myset 1] 0
    }

    test {SADDEX EX - a past expiration removes existing members and adds none} {
        r FLUSHALL
        assert_equal 0 [r SADDEX myset EX 0 MEMBERS 1 a]
        assert_equal 0 [r EXISTS myset]
        r SADD myset a
        assert_equal 0 [r SADDEX myset EX 0 MEMBERS 2 a b]
        assert_equal 0 [r EXISTS myset]
    }

    foreach {expiration value reader} [list PX 100000000 SPTTL \
            EXAT [expr {[clock seconds] + 100000}] SEXPIRETIME \
            PXAT [expr {[clock milliseconds] + 100000000}] SPEXPIRETIME] {
        test "SADDEX $expiration - the expiration is read back in its own unit and frame" {
            r FLUSHALL
            assert_equal 1 [r SADDEX myset $expiration $value MEMBERS 1 a]
            if {[string match "*AT" $expiration]} {
                assert_equal $value [lindex [r $reader myset MEMBERS 1 a] 0]
            } else {
                assert_range [lindex [r $reader myset MEMBERS 1 a] 0] \
                    [expr {$value - 5000}] $value
            }
        }
    }

    test {SADDEX - without an expiration token the members are plain} {
        r FLUSHALL
        assert_equal 2 [r SADDEX myset MEMBERS 2 a b]
        assert_equal {-1 -1} [r STTL myset MEMBERS 2 a b]
        assert_equal 0 [get_keys_with_volatile_items r]
    }

    test {SADDEX KEEPTTL - keeps an existing member TTL and adds new members plain} {
        r FLUSHALL
        assert_equal 1 [r SADDEX myset EX $::live_member_ttl MEMBERS 1 a]
        set original [set_member_pexpiretime r myset a]
        assert_equal 1 [r SADDEX myset KEEPTTL MEMBERS 2 a b]
        assert_equal $original [set_member_pexpiretime r myset a]
        assert_equal -1 [set_member_ttl r myset b]
    }

    test {SADDEX MNX EX - all or nothing over the whole call} {
        r FLUSHALL
        r SADD myset a
        assert_equal 0 [r SADDEX myset MNX EX $::live_member_ttl MEMBERS 2 a b]
        assert_equal -1 [set_member_ttl r myset a]
        assert_equal {a} [r SMEMBERS myset]
        assert_equal 2 [r SADDEX myset MNX EX $::live_member_ttl MEMBERS 2 c d]
        assert_equal {a c d} [lsort [r SMEMBERS myset]]
        assert_morethan [set_member_ttl r myset c] 0
        assert_morethan [set_member_ttl r myset d] 0
    }

    test {SADDEX MXX EX - all or nothing over the whole call} {
        r FLUSHALL
        r SADD myset a
        assert_equal 0 [r SADDEX myset MXX EX $::live_member_ttl MEMBERS 2 a b]
        assert_equal -1 [set_member_ttl r myset a]
        assert_equal {a} [r SMEMBERS myset]
        r SADD myset b
        assert_equal 0 [r SADDEX myset MXX EX $::live_member_ttl MEMBERS 2 a b]
        assert_morethan [set_member_ttl r myset a] 0
        assert_morethan [set_member_ttl r myset b] 0
    }

    test {SADDEX NX - the key condition is honoured} {
        r FLUSHALL
        assert_equal 1 [r SADDEX myset NX EX $::live_member_ttl MEMBERS 1 a]
        assert_equal 1 [r EXISTS myset]
        assert_morethan [set_member_ttl r myset a] 0
        assert_equal 0 [r SADDEX myset NX EX $::live_member_ttl MEMBERS 1 b]
        assert_equal 0 [r SISMEMBER myset b]
    }

    test {SADDEX XX - the key condition is honoured} {
        r FLUSHALL
        assert_equal 0 [r SADDEX myset XX EX $::live_member_ttl MEMBERS 1 a]
        assert_equal 0 [r EXISTS myset]
        r SADD myset a
        assert_equal 1 [r SADDEX myset XX EX $::live_member_ttl MEMBERS 1 b]
        assert_morethan [set_member_ttl r myset b] 0
    }

    test {SADDEX - WRONGTYPE and argument errors} {
        r FLUSHALL
        r SET mystr hello
        assert_error "WRONGTYPE*" {r SADDEX mystr EX 100 MEMBERS 1 a}
        assert_error "*ERR syntax error" {r SADDEX myset EX 100 PX 1000 MEMBERS 1 a}
        assert_error "*ERR syntax error" {r SADDEX myset MNX MXX EX 100 MEMBERS 1 a}
        assert_error "*ERR syntax error" {r SADDEX myset NX XX EX 100 MEMBERS 1 a}
        assert_error "*greater than 0 and match the provided number*" \
            {r SADDEX myset EX 100 MEMBERS 2 a}
        assert_error "*greater than 0 and match the provided number*" \
            {r SADDEX myset EX 100 MEMBERS 0 a}
        assert_error "*not an integer or out of range" {r SADDEX myset EX notanumber MEMBERS 1 a}
    }

    test {Set TTL commands - MEMBERS token missing} {
        r FLUSHALL
        r SADD myset a
        assert_error "ERR wrong number of arguments for 'sexpire' command" \
            {r SEXPIRE myset 100 1 a}
        foreach command {STTL SPERSIST} {
            assert_error "ERR wrong number of arguments for '[string tolower $command]' command" \
                {r $command myset 1 a}
        }
    }

    test "SINTER SUNION SDIFF never return an expired member" {
        flush_and_disable_active_expiry
        r SADD s1{t} a b c d
        r SADD s2{t} a b c e
        make_members_expired r s1{t} {b}
        make_members_expired r s2{t} {c}
        assert_equal {a} [lsort [r SINTER s1{t} s2{t}]]
        assert_equal {a b c d e} [lsort [r SUNION s1{t} s2{t}]]
        assert_equal {c d} [lsort [r SDIFF s1{t} s2{t}]]
        assert_equal 4 [r SCARD s1{t}]
        assert_equal 4 [r SCARD s2{t}]
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    test "SINTERCARD ignores expired members" {
        flush_and_disable_active_expiry
        r SADD s1{t} a b c d
        r SADD s2{t} a b c
        make_members_expired r s1{t} {b}
        make_members_expired r s2{t} {c}
        assert_equal 1 [r SINTERCARD 2 s1{t} s2{t}]
        # A single-key SINTERCARD is the live cardinality, not SCARD.
        assert_equal 3 [r SINTERCARD 1 s1{t}]
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    test "SUNIONSTORE writes a plain set when a source is volatile" {
        flush_and_disable_active_expiry
        r SADD s1{t} a b c
        r SADD s2{t} d e
        make_members_expired r s1{t} {c}
        assert_equal {1} [r SEXPIRE s1{t} $::live_member_ttl MEMBERS 1 a]
        assert_equal 4 [r SUNIONSTORE dst{t} s1{t} s2{t}]
        assert_equal {a b d e} [lsort [r SMEMBERS dst{t}]]
        assert_equal {-1 -1 -1 -1} [r STTL dst{t} MEMBERS 4 a b d e]
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    test "SMOVE carries the member TTL to the destination" {
        r FLUSHALL
        r SADD src{t} a b
        r SADD dst{t} z
        assert_equal {1} [r SEXPIRE src{t} $::live_member_ttl MEMBERS 1 a]
        set expiry [set_member_pexpiretime r src{t} a]
        assert_equal 1 [r SMOVE src{t} dst{t} a]
        assert_equal $expiry [set_member_pexpiretime r dst{t} a]
    }

    test "SMOVE of a plain member into a volatile destination keeps it plain" {
        r FLUSHALL
        r SADD src{t} a
        r SADD dst{t} z y
        assert_equal {1} [r SEXPIRE dst{t} $::live_member_ttl MEMBERS 1 z]
        assert_equal 1 [r SMOVE src{t} dst{t} a]
        assert_equal {-1} [r STTL dst{t} MEMBERS 1 a]
    }

    test "SMOVE of an expired member returns 0 and leaves the destination alone" {
        flush_and_disable_active_expiry
        r SADD src{t} a b
        r SADD dst{t} z
        make_members_expired r src{t} {a}
        assert_equal 0 [r SMOVE src{t} dst{t} a]
        assert_equal {z} [r SMEMBERS dst{t}]
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    test "SPOP without a count leaves an all-expired set to active expiry" {
        flush_and_disable_active_expiry
        r SADD myset a b c
        make_members_expired r myset {a b c}
        assert_equal {{} 0 0} [capture_mutation_signals myset {
            assert_equal {} [r SPOP myset]
        }]
        assert_equal 3 [r SCARD myset]
        assert_equal 1 [r EXISTS myset]

        r DEBUG SET-ACTIVE-EXPIRE 1
        wait_for_condition 100 100 {
            [r EXISTS myset] == 0
        } else {
            fail "active expiry did not delete the all-expired set"
        }
    } {} {needs:debug}

    test "SPOP with a count leaves an all-expired set to active expiry" {
        flush_and_disable_active_expiry
        r SADD myset a b c
        make_members_expired r myset {a b c}
        assert_equal {{} 0 0} [capture_mutation_signals myset {
            assert_equal {} [r SPOP myset 2]
        }]
        assert_equal 3 [r SCARD myset]
        assert_equal 1 [r EXISTS myset]

        r DEBUG SET-ACTIVE-EXPIRE 1
        wait_for_condition 100 100 {
            [r EXISTS myset] == 0
        } else {
            fail "active expiry did not delete the all-expired set"
        }
    } {} {needs:debug}

    test "SPOP removes only the live member it returns" {
        flush_and_disable_active_expiry
        r SADD myset a b c
        make_members_expired r myset {a b c}
        r SADD myset live1 live2

        set signals [capture_mutation_signals myset {
            set popped [r SPOP myset]
        }]
        assert {[lsearch -exact {live1 live2} $popped] != -1}
        assert_equal {{spop myset}} [lindex $signals 0]
        assert_equal 1 [lindex $signals 1]
        assert_equal 1 [lindex $signals 2]
        assert_equal 4 [r SCARD myset]

        set signals [capture_mutation_signals myset {
            set popped [r SPOP myset 1]
        }]
        assert {[lsearch -exact {live1 live2} $popped] != -1}
        assert_equal {{spop myset}} [lindex $signals 0]
        assert_equal 1 [lindex $signals 1]
        assert_equal 1 [lindex $signals 2]
        assert_equal 3 [r SCARD myset]
        assert_equal {} [r SMEMBERS myset]
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    foreach command {SINTERSTORE SUNIONSTORE SDIFFSTORE} {
        test "$command over only expired members raises no mutation signal" {
            flush_and_disable_active_expiry
            r SADD s1{t} a b
            make_members_expired r s1{t} {a b}
            assert_equal {{} 0 0} [capture_mutation_signals dst{t} {
                assert_equal 0 [r $command dst{t} s1{t}]
            }]
            assert_equal 0 [r EXISTS dst{t}]
            r DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}
    }

    test "SORT STORE over only expired members raises no mutation signal" {
        flush_and_disable_active_expiry
        r SADD src{t} 1 2 3
        make_members_expired r src{t} {1 2 3}
        assert_equal {{} 0 0} [capture_mutation_signals dst{t} {
            assert_equal 0 [r SORT src{t} STORE dst{t}]
        }]
        assert_equal 0 [r EXISTS dst{t}]
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    test {A member TTL does not change a listpack set's encoding} {
        r FLUSHALL
        use_set_encoding listpack
        r SADD myset a b c
        assert_encoding listpack myset
        assert_equal {1} [r SEXPIRE myset $::live_member_ttl MEMBERS 1 a]
        assert_encoding listpack myset
        assert_equal {1} [r SEXPIRE myset [expr {2 * $::live_member_ttl}] MEMBERS 1 a]
        assert_encoding listpack myset
        assert_equal {1} [r SPERSIST myset MEMBERS 1 a]
        assert_encoding listpack myset
        assert_equal 1 [r SADDEX myset EX $::live_member_ttl MEMBERS 1 d]
        assert_encoding listpack myset
    }

    # Active expiry is disabled to expose logically expired members.
    foreach encoding {listpack hashtable} {
        test "SADD over an expired member returns 1 and clears the TTL - $encoding" {
            flush_and_disable_active_expiry
            r config resetstat
            use_set_encoding $encoding
            r SADD myset gone live1
            make_members_expired r myset {gone}
            set rd [setup_single_keyspace_notification r]
            assert_equal 1 [r SADD myset gone]
            assert_keyevent_patterns $rd myset sexpired sadd
            assert_equal 1 [status r expired_set_members]
            r SADD dummy dummy
            assert_keyevent_patterns $rd dummy sadd
            $rd close
            assert_equal -1 [set_member_ttl r myset gone]
            assert_equal 2 [r SCARD myset]
            r DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}

        test "SEXPIRE on an expired member replies -2 and does not resurrect it - $encoding" {
            flush_and_disable_active_expiry
            use_set_encoding $encoding
            r SADD myset gone live1
            make_members_expired r myset {gone}
            assert_equal {-2} [r SEXPIRE myset $::live_member_ttl MEMBERS 1 gone]
            assert_equal 0 [r SISMEMBER myset gone]
            r DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}
    }

    test "SADD over a live volatile member returns 0 and keeps the TTL" {
        r FLUSHALL
        r SADD myset a
        assert_equal {1} [r SEXPIRE myset $::live_member_ttl MEMBERS 1 a]
        set original [set_member_pexpiretime r myset a]
        assert_equal 0 [r SADD myset a]
        assert_equal $original [set_member_pexpiretime r myset a]
    }

    test "SADDEX MNX over an expired member treats it as absent" {
        flush_and_disable_active_expiry
        r SADD myset gone live1
        make_members_expired r myset {gone}
        assert_equal 1 [r SADDEX myset MNX EX $::live_member_ttl MEMBERS 1 gone]
        assert_morethan [set_member_ttl r myset gone] 0
        assert_equal 2 [r SCARD myset]
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    test "SADDEX MXX over an expired member treats it as absent" {
        flush_and_disable_active_expiry
        r SADD myset gone live1
        make_members_expired r myset {gone}
        assert_equal 0 [r SADDEX myset MXX EX $::live_member_ttl MEMBERS 2 gone live1]
        assert_equal 0 [r SISMEMBER myset gone]
        assert_equal -1 [set_member_ttl r myset live1]
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    test {SEXPIRE emits a single sexpire notification} {
        r FLUSHALL
        r SADD myset a b c
        set rd [setup_single_keyspace_notification r]
        r SEXPIRE myset $::live_member_ttl MEMBERS 3 a b c
        assert_keyevent_patterns $rd myset sexpire
        # Nothing else may be pending: the next event is the marker.
        r SADD dummy dummy
        assert_keyevent_patterns $rd dummy sadd
        $rd close
    }

    test {SEXPIRE on a missing member emits nothing} {
        r FLUSHALL
        r SADD myset a
        set rd [setup_single_keyspace_notification r]
        r SEXPIRE myset $::live_member_ttl MEMBERS 1 nomember
        r SADD dummy dummy
        assert_keyevent_patterns $rd dummy sadd
        $rd close
    }

    test {A past SEXPIRE emits sexpired and del and increments expired_set_members} {
        r FLUSHALL
        r config resetstat
        r SADD myset a
        set rd [setup_single_keyspace_notification r]
        assert_equal {2} [r SEXPIRE myset 0 MEMBERS 1 a]
        assert_keyevent_patterns $rd myset sexpired del
        assert_equal 1 [status r expired_set_members]
        r SADD dummy dummy
        assert_keyevent_patterns $rd dummy sadd
        $rd close
    }

    test {SPERSIST emits an spersist notification} {
        r FLUSHALL
        r SADD myset a
        r SEXPIRE myset $::live_member_ttl MEMBERS 1 a
        set rd [setup_single_keyspace_notification r]
        r SPERSIST myset MEMBERS 1 a
        assert_keyevent_patterns $rd myset spersist
        r SADD dummy dummy
        assert_keyevent_patterns $rd dummy sadd
        $rd close
    }

    test {SPERSIST on a member without a TTL emits nothing} {
        r FLUSHALL
        r SADD myset a
        set rd [setup_single_keyspace_notification r]
        r SPERSIST myset MEMBERS 1 a
        r SADD dummy dummy
        assert_keyevent_patterns $rd dummy sadd
        $rd close
    }

    test {SADDEX emits sadd and sexpire} {
        r FLUSHALL
        set rd [setup_single_keyspace_notification r]
        r SADDEX myset EX $::live_member_ttl MEMBERS 1 a
        assert_keyevent_patterns $rd myset sadd sexpire
        r SADD dummy dummy
        assert_keyevent_patterns $rd dummy sadd
        $rd close
    }

    foreach encoding {listpack hashtable} {
        test "Active expiry emits sexpired and increments expired_set_members - $encoding" {
            r FLUSHALL
            r config resetstat
            use_set_encoding $encoding
            r SADD myset a b c live1
            set rd [setup_single_keyspace_notification r]
            r SPEXPIRE myset 50 MEMBERS 3 a b c
            assert_keyevent_patterns $rd myset sexpire
            wait_for_set_active_expiry r myset 1 0 3
            assert_keyevent_patterns $rd myset sexpired
            r SADD dummy dummy
            assert_keyevent_patterns $rd dummy sadd
            $rd close
        }

        test "Active expiry of the last member deletes the key - $encoding" {
            r FLUSHALL
            r config resetstat
            use_set_encoding $encoding
            r SADD myset a
            set rd [setup_single_keyspace_notification r]
            r SPEXPIRE myset 50 MEMBERS 1 a
            assert_keyevent_patterns $rd myset sexpire
            wait_for_set_active_expiry r myset 0 0 1
            assert_keyevent_patterns $rd myset sexpired del
            assert_equal 0 [get_keys_with_volatile_items r]
            r SADD dummy dummy
            assert_keyevent_patterns $rd dummy sadd
            $rd close
        }
    }
# Member TTL metadata forces intsets to convert; set mutations never convert
# them back.
    test "A member TTL converts a small intset to listpack" {
        r FLUSHALL
        use_set_encoding listpack
        r SADD myset 1 2 3
        assert_encoding intset myset
        assert_equal {1} [r SEXPIRE myset $::live_member_ttl MEMBERS 1 2]
        assert_encoding listpack myset
        assert_equal {1 2 3} [lsort [r SMEMBERS myset]]
        assert_range [set_member_ttl r myset 2] 1 $::live_member_ttl
        assert_equal {-1 -1} [r STTL myset MEMBERS 2 1 3]
    }

    test "A member TTL converts an intset over the listpack limit to hashtable" {
        r FLUSHALL
        use_set_encoding listpack
        set ints {}
        for {set i 0} {$i < 200} {incr i} { lappend ints $i }
        r SADD myset {*}$ints
        assert_encoding intset myset
        assert_equal {1} [r SEXPIRE myset $::live_member_ttl MEMBERS 1 7]
        assert_encoding hashtable myset
        assert_equal 200 [r SCARD myset]
    }

    test "SPERSIST of the last volatile member keeps the converted encoding" {
        r FLUSHALL
        use_set_encoding listpack
        r SADD myset 1 2 3
        assert_encoding intset myset
        assert_equal {1} [r SEXPIRE myset $::live_member_ttl MEMBERS 1 2]
        assert_encoding listpack myset

        assert_equal {1} [r SPERSIST myset MEMBERS 1 2]
        assert_encoding listpack myset
        assert_equal {1 2 3} [lsort [r SMEMBERS myset]]
        assert_equal {-1 -1 -1} [r STTL myset MEMBERS 3 1 2 3]
    }

    test "A TTL command that applies no TTL keeps the intset encoding" {
        r FLUSHALL
        use_set_encoding listpack
        r SADD myset 1 2 3
        assert_encoding intset myset
        assert_equal {-2} [r SEXPIRE myset $::live_member_ttl MEMBERS 1 9]
        assert_encoding intset myset
        assert_equal {2} [r SEXPIRE myset 0 MEMBERS 1 2]
        assert_equal {1 3} [lsort [r SMEMBERS myset]]
        assert_encoding intset myset
    }

    test "SMOVE of a volatile integer member converts an intset destination" {
        r FLUSHALL
        use_set_encoding listpack
        r SADD src{t} 7
        r SADD dst{t} 1 2 3
        assert_encoding intset dst{t}
        assert_equal {1} [r SEXPIRE src{t} $::live_member_ttl MEMBERS 1 7]
        assert_equal 1 [r SMOVE src{t} dst{t} 7]
        assert_encoding listpack dst{t}
        assert_range [set_member_ttl r dst{t} 7] 1 $::live_member_ttl
        assert_equal {1 2 3 7} [lsort [r SMEMBERS dst{t}]]
        assert_equal {-1 -1 -1} [r STTL dst{t} MEMBERS 3 1 2 3]
    }

    test {Listpack to hashtable conversion keeps an expired member} {
        flush_and_disable_active_expiry
        r CONFIG SET set-max-listpack-entries 4
        r SADD myset expired l1 l2
        make_members_expired r myset {expired}
        assert_encoding listpack myset

        r SADD myset l3 l4 l5
        assert_encoding hashtable myset
        assert_equal 6 [r SCARD myset]
        assert_equal 0 [r SISMEMBER myset expired]

        use_set_encoding listpack
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    foreach encoding {listpack hashtable} {
        test "SPOP with a count above the live size returns exactly the live members - $encoding" {
            flush_and_disable_active_expiry
            use_set_encoding $encoding
            create_numbered_set myset 10
            make_members_expired r myset {m0 m1 m2 m3}
            assert_encoding $encoding myset
            assert_equal 10 [r SCARD myset]

            set popped [r SPOP myset 8]
            assert_equal {m4 m5 m6 m7 m8 m9} [lsort $popped]
            assert_equal 4 [r SCARD myset]
            assert_equal {} [r SMEMBERS myset]
            r DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}

        test "SPOP with a count below the live size returns live members only - $encoding" {
            flush_and_disable_active_expiry
            use_set_encoding $encoding
            create_numbered_set myset 10
            make_members_expired r myset {m0 m1 m2 m3}
            set popped [r SPOP myset 3]
            assert_equal 3 [llength $popped]
            assert_equal 3 [llength [lsort -unique $popped]]
            assert_none_of $popped {m0 m1 m2 m3}
            assert_equal 7 [r SCARD myset]
            assert_equal 3 [llength [r SMEMBERS myset]]
            r DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}

        test "SPOP on a set of only expired members mutates nothing - $encoding" {
            flush_and_disable_active_expiry
            use_set_encoding $encoding
            r SADD myset a b c
            make_members_expired r myset {a b c}
            assert_equal 3 [r SCARD myset]
            assert_equal {{} 0 0} [capture_mutation_signals myset {
                assert_equal {} [r SPOP myset 2]
                assert_equal {} [r SPOP myset]
                assert_equal {} [r SPOP myset 3]
            }]
            assert_equal 3 [r SCARD myset]
            assert_equal 1 [r EXISTS myset]

            r DEBUG SET-ACTIVE-EXPIRE 1
            wait_for_condition 100 100 {
                [r EXISTS myset] == 0
            } else {
                fail "active expiry did not delete the all-expired set"
            }
        } {} {needs:debug}

        test "SPOP with a huge count returns the live members and keeps the hidden ones - $encoding" {
            flush_and_disable_active_expiry
            use_set_encoding $encoding
            r SADD myset live1 live2 live3 expired
            make_members_expired r myset {expired}
            assert_equal {live1 live2 live3} [lsort [r SPOP myset 1000000000]]
            assert_equal 1 [r SCARD myset]
            assert_equal {} [r SMEMBERS myset]
            r DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}

        test "SPOP with a count untracks the last volatile member - $encoding" {
            flush_and_disable_active_expiry
            use_set_encoding $encoding
            r SADD myset plain1 plain2 volatile
            assert_equal {1} [r SEXPIRE myset $::live_member_ttl MEMBERS 1 volatile]
            for {set i 0} {$i < 100 && [r SISMEMBER myset volatile]} {incr i} {
                set popped [lindex [r SPOP myset 1] 0]
                if {$popped ne "volatile"} { r SADD myset $popped }
            }
            assert_equal 0 [r SISMEMBER myset volatile]
            assert_equal 0 [get_keys_with_volatile_items r]
            r DEBUG SET-ACTIVE-EXPIRE 1
            assert_equal PONG [r PING]
        } {} {needs:debug}

        test "SPOP without a count never returns an expired member - $encoding" {
            flush_and_disable_active_expiry
            use_set_encoding $encoding
            create_numbered_set myset 10
            make_members_expired r myset {m0 m1 m2 m3}
            for {set i 0} {$i < 6} {incr i} {
                set m [r SPOP myset]
                assert {$m ne {}}
                assert_none_of [list $m] {m0 m1 m2 m3}
            }
            assert_equal {} [r SPOP myset]
            r DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}

        test "SPOP with a count keeps the TTLs of the members that remain - $encoding" {
            r FLUSHALL
            use_set_encoding $encoding
            create_numbered_set myset 10
            assert_equal {1 1 1 1 1 1 1 1 1 1} \
                [r SEXPIRE myset $::live_member_ttl MEMBERS 10 m0 m1 m2 m3 m4 m5 m6 m7 m8 m9]
            set popped [r SPOP myset 9]
            assert_equal 9 [llength $popped]
            assert_equal 1 [r SCARD myset]
            foreach m [r SMEMBERS myset] {
                assert_range [set_member_ttl r myset $m] 1 $::live_member_ttl
            }
        }

        test "SRANDMEMBER without a count is a read - $encoding" {
            flush_and_disable_active_expiry
            use_set_encoding $encoding
            create_numbered_set myset 10
            make_members_expired r myset {m0 m1 m2 m3}
            set expired_before [s expired_set_members]

            set got {}
            set signals [capture_mutation_signals myset {
                for {set i 0} {$i < 20} {incr i} { lappend got [r SRANDMEMBER myset] }
            }]
            assert_equal 20 [llength $got]
            assert_none_of $got {m0 m1 m2 m3}
            assert_equal {{} 0 0} $signals
            assert_equal 10 [r SCARD myset]
            assert_equal $expired_before [s expired_set_members]
            r DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}

        test "SRANDMEMBER with a positive count is a read - $encoding" {
            flush_and_disable_active_expiry
            use_set_encoding $encoding
            create_numbered_set myset 10
            make_members_expired r myset {m0 m1 m2 m3}
            set expired_before [s expired_set_members]

            set signals [capture_mutation_signals myset {
                # Above the live size every live member comes back exactly once.
                set above [r SRANDMEMBER myset 20]
                # Below it, still without duplicates.
                set below [r SRANDMEMBER myset 4]
                set single [r SRANDMEMBER myset 1]
            }]
            assert_equal {m4 m5 m6 m7 m8 m9} [lsort $above]
            assert_equal 4 [llength [lsort -unique $below]]
            assert_none_of $below {m0 m1 m2 m3}
            assert_equal 1 [llength $single]
            assert_none_of $single {m0 m1 m2 m3}
            assert_equal {{} 0 0} $signals
            assert_equal 10 [r SCARD myset]
            assert_equal $expired_before [s expired_set_members]
            r DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}

        test "SRANDMEMBER with a negative count is a read - $encoding" {
            flush_and_disable_active_expiry
            use_set_encoding $encoding
            create_numbered_set myset 10
            make_members_expired r myset {m0 m1 m2 m3}
            set expired_before [s expired_set_members]

            set signals [capture_mutation_signals myset {
                # A negative count returns exactly the requested number of
                # elements, repeats allowed, drawn only from the live members.
                set got [r SRANDMEMBER myset -50]
            }]
            assert_equal 50 [llength $got]
            assert_none_of $got {m0 m1 m2 m3}
            assert_equal {{} 0 0} $signals
            assert_equal 10 [r SCARD myset]
            assert_equal $expired_before [s expired_set_members]
            r DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}

        test "SRANDMEMBER finds the one live member of a mostly-expired set - $encoding" {
            flush_and_disable_active_expiry
            use_set_encoding $encoding
            create_numbered_set myset 10
            make_members_expired r myset {m0 m1 m2 m3 m4 m5 m6 m7 m8}
            for {set i 0} {$i < 20} {incr i} {
                assert_equal m9 [r SRANDMEMBER myset]
            }
            assert_equal 10 [r SCARD myset]
            r DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}

        test "SRANDMEMBER on a set of only expired members returns nothing - $encoding" {
            flush_and_disable_active_expiry
            use_set_encoding $encoding
            r SADD myset a b c
            make_members_expired r myset {a b c}
            assert_equal {{} 0 0} [capture_mutation_signals myset {
                assert_equal {} [r SRANDMEMBER myset]
                assert_equal {} [r SRANDMEMBER myset 20]
                assert_equal {} [r SRANDMEMBER myset 2]
                assert_equal {} [r SRANDMEMBER myset -20]
            }]
            assert_equal 3 [r SCARD myset]
            assert_equal 1 [r EXISTS myset]

            r DEBUG SET-ACTIVE-EXPIRE 1
            wait_for_condition 100 100 {
                [r EXISTS myset] == 0
            } else {
                fail "active expiry did not delete the all-expired set"
            }
        } {} {needs:debug}

        test "SSCAN skips expired members - $encoding" {
            flush_and_disable_active_expiry
            use_set_encoding $encoding
            r SADD myset a b c d
            make_members_expired r myset {a b}
            # SSCAN may duplicate and reorder members.
            set cursor 0
            set found {}
            while 1 {
                set res [r SSCAN myset $cursor]
                set cursor [lindex $res 0]
                foreach m [lindex $res 1] { lappend found $m }
                if {$cursor == 0} break
            }
            assert_equal {c d} [lsort -unique $found]
            r DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}

        test "SREM ignores expired members - $encoding" {
            flush_and_disable_active_expiry
            use_set_encoding $encoding
            r SADD myset a b c
            make_members_expired r myset {b}
            assert_equal 1 [r SREM myset a b]
            assert_equal {c} [r SMEMBERS myset]
            r DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}

        test "SREM of the last live member leaves the key to active expiry - $encoding" {
            flush_and_disable_active_expiry
            use_set_encoding $encoding
            r SADD myset a b c
            make_members_expired r myset {b c}
            assert_equal 1 [r SREM myset a]
            assert_equal 2 [r SCARD myset]
            assert_equal {} [r SMEMBERS myset]

            r DEBUG SET-ACTIVE-EXPIRE 1
            wait_for_condition 100 100 {
                [r EXISTS myset] == 0
            } else {
                fail "active expiry did not delete the set of expired members"
            }
            assert_equal 0 [get_keys_with_volatile_items r]
        } {} {needs:debug}

        test "Set membership and TTL reads hide expired members - $encoding" {
            flush_and_disable_active_expiry
            use_set_encoding $encoding
            r SADD myset live1 gone1 live2 gone2
            make_members_expired r myset {gone1 gone2}
            assert_equal {1 0 1 0 0} [r SMISMEMBER myset live1 gone1 live2 gone2 missing]
            assert_equal {-2 -2} [r STTL myset MEMBERS 2 gone1 missing]
            r DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}
    }

    test "SRANDMEMBER with a negative count samples the full population" {
        flush_and_disable_active_expiry
        use_set_encoding hashtable
        set members {}
        for {set i 0} {$i < 1500} {incr i} { lappend members m$i }
        r SADD myset {*}$members expired
        make_members_expired r myset {expired}

        set got [r SRANDMEMBER myset -20000]
        assert_equal 20000 [llength $got]
        assert_none_of $got {expired}
        assert {[llength [lsort -unique $got]] > 1200}
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    test "SINTERSTORE with the destination equal to a volatile source" {
        flush_and_disable_active_expiry
        r SADD k{t} a b c
        make_members_expired r k{t} {c}
        assert_equal {1} [r SEXPIRE k{t} $::live_member_ttl MEMBERS 1 a]
        assert_equal 2 [r SINTERSTORE k{t} k{t}]
        assert_equal {a b} [lsort [r SMEMBERS k{t}]]
        assert_equal {-1 -1} [r STTL k{t} MEMBERS 2 a b]
        assert_equal 0 [get_keys_with_volatile_items r]
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

# Removing the last volatile member flips the object to non-volatile before the
# key is deleted, so the deletion must still untrack it.
    foreach {what members empty} {
        SREM                        {a}   {assert_equal 1 [r SREM myset a]}
        SPOP                        {a}   {assert_equal a [r SPOP myset]}
        {SPOP with a count}         {a b} {assert_equal {a b} [lsort [r SPOP myset 2]]}
        {an expiration in the past} {a}   {assert_equal {2} [r SPEXPIREAT myset 1 MEMBERS 1 a]}
    } {
        test "$what empties a volatile set, deleting and untracking the key" {
            r FLUSHALL
            assert_equal [llength $members] \
                [r SADDEX myset PX 60000 MEMBERS [llength $members] {*}$members]
            assert_equal 1 [get_keys_with_volatile_items r]
            eval $empty
            assert_equal 0 [r EXISTS myset]
            assert_equal 0 [get_keys_with_volatile_items r]
        }
    }

    test "SMOVE of the only volatile member deletes the source and untracks it" {
        r FLUSHALL
        assert_equal 1 [r SADDEX src{t} PX 60000 MEMBERS 1 a]
        assert_equal 1 [get_keys_with_volatile_items r]
        assert_equal 1 [r SMOVE src{t} dst{t} a]
        assert_equal 0 [r EXISTS src{t}]
        assert_equal 1 [get_keys_with_volatile_items r]
    }

    # SORT sizes its vector from SCARD, which counts expired members it must skip.
    test "SORT skips an expired member" {
        flush_and_disable_active_expiry
        r SADD myset 1 2 3 4 5
        make_members_expired r myset {3}
        assert_equal 5 [r SCARD myset]

        assert_equal {1 2 4 5} [r SORT myset]
        assert_equal {4 5} [r SORT myset LIMIT 2 10]
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    test "SORT of only expired members returns an empty list" {
        flush_and_disable_active_expiry
        r SADD myset 1 2 3
        make_members_expired r myset {1 2 3}
        assert_equal {} [r SORT myset]
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    test "SORT STORE writes only the live members" {
        flush_and_disable_active_expiry
        r SADD src{t} 1 2 3 4 5
        make_members_expired r src{t} {3}

        assert_equal 4 [r SORT src{t} STORE dst{t}]
        assert_equal {1 2 4 5} [r LRANGE dst{t} 0 -1]
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    test {MEMORY USAGE of a volatile set exceeds a plain set with the same members} {
        r FLUSHALL
        use_set_encoding hashtable

        set members {}
        for {set i 0} {$i < 64} {incr i} {
            lappend members m$i
        }
        r SADD plain {*}$members
        r SADD volat {*}$members

        set plain_mem [r MEMORY USAGE plain]
        r SEXPIRE volat 100000 MEMBERS 64 {*}$members
        set volat_mem [r MEMORY USAGE volat]

        assert_morethan $volat_mem $plain_mem

        r SPERSIST volat MEMBERS 64 {*}$members
        assert_morethan $volat_mem [r MEMORY USAGE volat]

        use_set_encoding listpack
    } {OK}

    test {Active expiry keeps working after the key is deleted and re-created} {
        r FLUSHALL
        r SADDEX myset PX 50 MEMBERS 1 m1
        wait_for_condition 100 100 {
            [r EXISTS myset] == 0
        } else {
            fail "first generation of the key was not reclaimed"
        }

        set initial_expired [status r expired_set_members]
        r SADD myset survivor
        r SADDEX myset PX 50 MEMBERS 1 doomed
        wait_for_set_active_expiry r myset 1 $initial_expired 1
        assert_equal {survivor} [r SMEMBERS myset]
    }

    test {DEL of a volatile set untracks the key} {
        r FLUSHALL
        r SADDEX myset PX 60000 MEMBERS 1 a
        assert_equal 1 [get_keys_with_volatile_items r]
        r DEL myset
        assert_equal 0 [get_keys_with_volatile_items r]
    }
}

# Replicas and AOF loading retain expired members, so rewrites name only members changed by the primary.
start_server {tags {"setexpire external:skip"}} {
    test {SEXPIRE, SPEXPIRE and SEXPIREAT are propagated as SPEXPIREAT in milliseconds} {
        r FLUSHALL
        set at_sec [expr {[clock seconds] + 100}]
        set at_ms [expr {[clock milliseconds] + 100000}]
        set repl [attach_to_replication_stream]

        r SADD myset m1 m2 m3 m4
        r SEXPIRE myset 100 MEMBERS 1 m1
        r SPEXPIRE myset 100000 MEMBERS 1 m2
        r SEXPIREAT myset $at_sec MEMBERS 1 m3
        r SPEXPIREAT myset $at_ms MEMBERS 1 m4
        set m1_at [set_member_pexpiretime r myset m1]
        set m2_at [set_member_pexpiretime r myset m2]

        assert_replication_stream $repl [subst {
            {select *}
            {sadd myset m1 m2 m3 m4}
            {spexpireat myset $m1_at MEMBERS 1 m1}
            {spexpireat myset $m2_at MEMBERS 1 m2}
            {spexpireat myset [expr {$at_sec * 1000}] MEMBERS 1 m3}
            {spexpireat myset $at_ms MEMBERS 1 m4}
        }]
        assert_equal {} [read_from_replication_stream $repl]
        close_replication_stream $repl
    }

    test {SADDEX EX, PX and EXAT are propagated as PXAT} {
        r FLUSHALL
        set at_sec [expr {[clock seconds] + 100}]
        set repl [attach_to_replication_stream]

        r SADDEX myset EX 100 MEMBERS 1 m1
        r SADDEX myset PX 100000 MEMBERS 1 m2
        r SADDEX myset EXAT $at_sec MEMBERS 1 m3
        set m1_at [set_member_pexpiretime r myset m1]
        set m2_at [set_member_pexpiretime r myset m2]

        assert_replication_stream $repl [subst {
            {select *}
            {saddex myset PXAT $m1_at MEMBERS 1 m1}
            {saddex myset PXAT $m2_at MEMBERS 1 m2}
            {saddex myset PXAT [expr {$at_sec * 1000}] MEMBERS 1 m3}
        }]
        assert_equal {} [read_from_replication_stream $repl]
        close_replication_stream $repl
    }

    test {SADDEX does not propagate condition arguments} {
        r FLUSHALL
        set at_ms [expr {[clock milliseconds] + 100000}]
        r SADD myset m1
        set repl [attach_to_replication_stream]

        r SADDEX myset XX MXX PXAT $at_ms MEMBERS 1 m1
        r SADDEX myset MNX PXAT $at_ms MEMBERS 1 m2
        r SADDEX nx NX PXAT $at_ms MEMBERS 1 a

        assert_replication_stream $repl [subst {
            {select *}
            {saddex myset PXAT $at_ms MEMBERS 1 m1}
            {saddex myset PXAT $at_ms MEMBERS 1 m2}
            {saddex nx PXAT $at_ms MEMBERS 1 a}
        }]
        assert_equal {} [read_from_replication_stream $repl]
        close_replication_stream $repl
    }

    test {SADDEX KEEPTTL is propagated as KEEPTTL} {
        r FLUSHALL
        r SADDEX myset EX 100 MEMBERS 1 m1
        set repl [attach_to_replication_stream]

        # Adding m2 makes the otherwise no-op KEEPTTL command propagate.
        r SADDEX myset KEEPTTL MEMBERS 2 m1 m2

        assert_replication_stream $repl {
            {select *}
            {saddex myset KEEPTTL MEMBERS 2 m1 m2}
        }
        assert_equal {} [read_from_replication_stream $repl]
        close_replication_stream $repl
    }

    test {SEXPIRE with a time in the past is propagated as SREM} {
        r FLUSHALL
        set repl [attach_to_replication_stream]

        r SADD myset m1 m2
        r SEXPIRE myset 0 MEMBERS 2 m1 nomember
        r SEXPIREAT myset 1 MEMBERS 1 m2

        # Replaying SREM deletes the empty set, so no separate key deletion is needed.
        assert_replication_stream $repl {
            {select *}
            {sadd myset m1 m2}
            {srem myset m1}
            {srem myset m2}
        }
        assert_equal {} [read_from_replication_stream $repl]
        close_replication_stream $repl

        assert_equal 0 [r EXISTS myset]
    }

    test {SADDEX with a time in the past is propagated as SREM} {
        r FLUSHALL
        r SADD myset m1 m2
        set repl [attach_to_replication_stream]

        r SADDEX myset EX 0 MEMBERS 2 m1 nomember
        r SADDEX myset PXAT 1 MEMBERS 1 m2

        assert_replication_stream $repl {
            {select *}
            {srem myset m1}
            {srem myset m2}
        }
        assert_equal {} [read_from_replication_stream $repl]
        close_replication_stream $repl
    }

    test {SREM with expired and live arguments propagates only removed members} {
        flush_and_disable_active_expiry
        r SADD myset live expired keep
        make_members_expired r myset {expired}
        set repl [attach_to_replication_stream]

        assert_equal 1 [r SREM myset live expired missing]
        assert_replication_stream $repl {
            {select *}
            {srem myset live}
        }
        assert_equal {} [read_from_replication_stream $repl]
        close_replication_stream $repl
        assert_equal {keep} [r SMEMBERS myset]
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    test {SADD over an expired member propagates SREM before SADD} {
        flush_and_disable_active_expiry
        r SADD myset m1 keepme
        make_members_expired r myset {m1}

        set repl [attach_to_replication_stream]
        r SADD myset m1

        # alsoPropagate() opens MULTI before SELECT.
        assert_replication_stream $repl {
            {multi}
            {select *}
            {srem myset m1}
            {sadd myset m1}
            {exec}
        }
        assert_equal {} [read_from_replication_stream $repl]
        close_replication_stream $repl
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    test {SADDEX KEEPTTL over an expired member propagates SREM first} {
        flush_and_disable_active_expiry
        r SADD myset m1 keepme
        make_members_expired r myset {m1}

        set repl [attach_to_replication_stream]
        r SADDEX myset KEEPTTL MEMBERS 1 m1

        assert_replication_stream $repl {
            {multi}
            {select *}
            {srem myset m1}
            {saddex myset KEEPTTL MEMBERS 1 m1}
            {exec}
        }
        assert_equal {} [read_from_replication_stream $repl]
        close_replication_stream $repl
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    test {SMOVE onto an expired destination member propagates SREM first} {
        flush_and_disable_active_expiry
        r SADD src{t} m1
        r SADD dst{t} m1 other
        make_members_expired r dst{t} {m1}

        set repl [attach_to_replication_stream]
        r SMOVE src{t} dst{t} m1

        assert_replication_stream $repl {
            {multi}
            {select *}
            {srem dst{t} m1}
            {smove src{t} dst{t} m1}
            {exec}
        }
        assert_equal {} [read_from_replication_stream $repl]
        close_replication_stream $repl
        assert_equal 2 [r SCARD dst{t}]
        assert_equal {m1 other} [lsort [r SMEMBERS dst{t}]]
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    test {SADD replacement preserves the containing key TTL during replay} {
        flush_and_disable_active_expiry
        r SADD myset m1
        r PEXPIRE myset 60000
        make_members_expired r myset {m1}
        set repl [attach_to_replication_stream]

        assert_equal 1 [r SADD myset m1]
        assert_replication_stream $repl {
            {multi}
            {select *}
            {srem myset m1}
            {sadd myset m1}
            {pexpireat myset *}
            {exec}
        }
        assert_equal {} [read_from_replication_stream $repl]
        close_replication_stream $repl
        assert_morethan [r PTTL myset] 0
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    test {SADDEX KEEPTTL replacement preserves the containing key TTL during replay} {
        flush_and_disable_active_expiry
        r SADD myset m1
        r PEXPIRE myset 60000
        make_members_expired r myset {m1}
        set repl [attach_to_replication_stream]

        assert_equal 1 [r SADDEX myset KEEPTTL MEMBERS 1 m1]
        assert_replication_stream $repl {
            {multi}
            {select *}
            {srem myset m1}
            {saddex myset KEEPTTL MEMBERS 1 m1}
            {pexpireat myset *}
            {exec}
        }
        assert_equal {} [read_from_replication_stream $repl]
        close_replication_stream $repl
        assert_morethan [r PTTL myset] 0
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    test {SMOVE replacement preserves the destination key TTL during replay} {
        flush_and_disable_active_expiry
        r SADD src{t} m1
        r SADD dst{t} m1
        r PEXPIRE dst{t} 60000
        make_members_expired r dst{t} {m1}
        set repl [attach_to_replication_stream]

        assert_equal 1 [r SMOVE src{t} dst{t} m1]
        assert_replication_stream $repl {
            {multi}
            {select *}
            {srem dst{t} m1}
            {smove src{t} dst{t} m1}
            {pexpireat dst{t} *}
            {exec}
        }
        assert_equal {} [read_from_replication_stream $repl]
        close_replication_stream $repl
        assert_morethan [r PTTL dst{t}] 0
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    test {Active expiry propagates one SREM per batch and no DEL while members remain} {
        r FLUSHALL
        r SADD myset keepme
        set repl [attach_to_replication_stream]

        set at_ms [expr {[clock milliseconds] + 500}]
        r SADDEX myset PXAT $at_ms MEMBERS 2 m1 m2
        wait_for_condition 100 100 {
            [r SCARD myset] == 1
        } else {
            fail "active expiry did not reclaim m1/m2"
        }

        assert_replication_stream $repl [subst {
            {select *}
            {saddex myset PXAT $at_ms MEMBERS 2 m1 m2}
            {srem myset m1 m2}
        }]
        assert_equal {} [read_from_replication_stream $repl]
        close_replication_stream $repl
    }

    test {Active expiry of the last member propagates SREM then UNLINK} {
        r FLUSHALL
        set repl [attach_to_replication_stream]

        set at_ms [expr {[clock milliseconds] + 500}]
        r SADDEX myset PXAT $at_ms MEMBERS 1 m1
        wait_for_condition 100 100 {
            [r EXISTS myset] == 0
        } else {
            fail "active expiry did not delete the emptied key"
        }

        # lazyfree-lazy-expire (default yes) makes the key deletion an UNLINK.
        assert_replication_stream $repl [subst {
            {select *}
            {saddex myset PXAT $at_ms MEMBERS 1 m1}
            {multi}
            {srem myset m1}
            {unlink myset}
            {exec}
        }]
        assert_equal {} [read_from_replication_stream $repl]
        close_replication_stream $repl
    }

    test {SRANDMEMBER propagates nothing} {
        flush_and_disable_active_expiry
        r SADD myset live1 live2 e1 e2
        make_members_expired r myset {e1 e2}

        set repl [attach_to_replication_stream]
        r SRANDMEMBER myset
        r SRANDMEMBER myset 2
        r SRANDMEMBER myset 20
        r SRANDMEMBER myset -20
        r SADD marker x

        assert_replication_stream $repl {
            {select *}
            {sadd marker x}
        }
        close_replication_stream $repl
        assert_equal 4 [r SCARD myset]
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    test {SPOP without a count propagates only the member it returned} {
        flush_and_disable_active_expiry
        r SADD myset live1 live2 e1 e2
        make_members_expired r myset {e1 e2}

        set repl [attach_to_replication_stream]
        set popped [r SPOP myset]
        r SADD marker x

        assert_replication_stream $repl [subst {
            {select *}
            {srem myset $popped}
            {sadd marker x}
        }]
        close_replication_stream $repl
        assert {[lsearch -exact {live1 live2} $popped] != -1}
        assert_equal 3 [r SCARD myset]
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    foreach encoding {listpack hashtable} {
        test "SPOP with a count propagates only the live members it returned - $encoding" {
            flush_and_disable_active_expiry
            use_set_encoding $encoding
            r SADD myset live1 live2 live3 e1 e2
            make_members_expired r myset {e1 e2}

            set repl [attach_to_replication_stream]
            set popped [r SPOP myset 2]
            r SADD marker x

            assert_replication_stream $repl {{select *}}
            set line [read_from_replication_stream $repl]
            assert_equal {srem myset} [lrange $line 0 1]
            assert_equal [lsort $popped] [lsort [lrange $line 2 end]]
            assert_equal {sadd marker x} [read_from_replication_stream $repl]
            close_replication_stream $repl

            assert_equal 2 [llength [lsort -unique $popped]]
            assert_none_of $popped {e1 e2}
            assert_equal 3 [r SCARD myset]
            r DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}
    }

    # Every replaced expired member's SREM precedes the single SADD, however
    # many members one SADD replaces.
    test {SADD over 1025 expired members propagates the SREM of every one before the SADD} {
        flush_and_disable_active_expiry
        create_numbered_set myset 1025
        set members [r SMEMBERS myset]
        make_members_expired r myset $members

        set repl [attach_to_replication_stream]
        assert_equal 1025 [r SADD myset {*}$members]
        assert_equal {multi} [read_from_replication_stream $repl]
        assert_match {select *} [read_from_replication_stream $repl]
        assert_equal [list srem myset {*}[lrange $members 0 1023]] [read_from_replication_stream $repl]
        assert_equal [list srem myset [lindex $members 1024]] [read_from_replication_stream $repl]
        assert_equal [list sadd myset {*}$members] [read_from_replication_stream $repl]
        assert_equal {exec} [read_from_replication_stream $repl]
        assert_equal {} [read_from_replication_stream $repl]
        close_replication_stream $repl
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    # A STORE result depends on the live view, so the result is propagated, not the command.
    foreach {cmd expected} {SUNIONSTORE {a b c} SINTERSTORE {a} SDIFFSTORE {c}} {
        test "$cmd with an expired member in a source propagates UNLINK plus SADD" {
            flush_and_disable_active_expiry
            r SADD s1{t} a expired c
            r SADD s2{t} a b
            r SADD dst{t} stale
            make_members_expired r s1{t} {expired}

            set repl [attach_to_replication_stream]
            r $cmd dst{t} s1{t} s2{t}

            assert_equal {multi} [read_from_replication_stream $repl]
            assert_match {select *} [read_from_replication_stream $repl]
            assert_equal [list unlink dst{t}] [read_from_replication_stream $repl]
            # Member order is the destination's iteration order.
            set line [read_from_replication_stream $repl]
            assert_equal sadd [lindex $line 0]
            assert_equal dst{t} [lindex $line 1]
            assert_equal $expected [lsort [lrange $line 2 end]]
            assert_equal {exec} [read_from_replication_stream $repl]
            assert_equal {} [read_from_replication_stream $repl]
            close_replication_stream $repl
            r DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}
    }

    foreach cmd {SINTERSTORE SDIFFSTORE} {
        test "$cmd with a volatile source and an empty result propagates only UNLINK" {
            flush_and_disable_active_expiry
            r SADD s1{t} expired
            r SADD s2{t} b
            r SADD dst{t} stale
            make_members_expired r s1{t} {expired}

            set repl [attach_to_replication_stream]
            assert_equal 0 [r $cmd dst{t} s1{t} s2{t}]

            assert_replication_stream $repl {
                {select *}
                {unlink dst{t}}
            }
            assert_equal {} [read_from_replication_stream $repl]
            close_replication_stream $repl
            r DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}
    }

    # propagateStoreAsEffects flushes the stored items in batches of 1024.
    test {A STORE of 1025 items propagates them in two batches} {
        flush_and_disable_active_expiry
        create_numbered_set s1{t} 1025
        r SADD s1{t} expired
        make_members_expired r s1{t} {expired}
        r SADD dst{t} stale
        r RPUSH l{t} stale

        set repl [attach_to_replication_stream]
        assert_equal 1025 [r SUNIONSTORE dst{t} s1{t}]
        assert_equal 1025 [r SORT s1{t} ALPHA STORE l{t}]

        assert_equal {multi} [read_from_replication_stream $repl]
        assert_match {select *} [read_from_replication_stream $repl]
        assert_equal [list unlink dst{t}] [read_from_replication_stream $repl]
        set line [read_from_replication_stream $repl]
        assert_equal {sadd dst{t}} [lrange $line 0 1]
        assert_equal 1024 [llength [lrange $line 2 end]]
        set line [read_from_replication_stream $repl]
        assert_equal {sadd dst{t}} [lrange $line 0 1]
        assert_equal 1 [llength [lrange $line 2 end]]
        assert_equal {exec} [read_from_replication_stream $repl]

        assert_equal {multi} [read_from_replication_stream $repl]
        assert_equal [list unlink l{t}] [read_from_replication_stream $repl]
        set line [read_from_replication_stream $repl]
        assert_equal {rpush l{t}} [lrange $line 0 1]
        assert_equal 1024 [llength [lrange $line 2 end]]
        set line [read_from_replication_stream $repl]
        assert_equal {rpush l{t}} [lrange $line 0 1]
        assert_equal 1 [llength [lrange $line 2 end]]
        assert_equal {exec} [read_from_replication_stream $repl]
        assert_equal {} [read_from_replication_stream $repl]
        close_replication_stream $repl
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    test {A STORE with no volatile source still propagates the command verbatim} {
        r FLUSHALL
        r SADD s1{t} a b c
        r SADD s2{t} b c d
        set repl [attach_to_replication_stream]

        r SUNIONSTORE u{t} s1{t} s2{t}
        r SINTERSTORE i{t} s1{t} s2{t}
        r SDIFFSTORE d{t} s1{t} s2{t}

        assert_replication_stream $repl {
            {select *}
            {sunionstore u{t} s1{t} s2{t}}
            {sinterstore i{t} s1{t} s2{t}}
            {sdiffstore d{t} s1{t} s2{t}}
        }
        assert_equal {} [read_from_replication_stream $repl]
        close_replication_stream $repl
    }

    test {SORT STORE with a volatile source propagates UNLINK plus RPUSH} {
        flush_and_disable_active_expiry
        r SADD src{t} 1 2 3 4 5
        r RPUSH dst{t} stale
        make_members_expired r src{t} {3}

        set repl [attach_to_replication_stream]
        assert_equal 4 [r SORT src{t} STORE dst{t}]

        assert_replication_stream $repl {
            {multi}
            {select *}
            {unlink dst{t}}
            {rpush dst{t} 1 2 4 5}
            {exec}
        }
        assert_equal {} [read_from_replication_stream $repl]
        close_replication_stream $repl
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    test {SORT STORE over only expired members propagates only the destination's UNLINK} {
        flush_and_disable_active_expiry
        r SADD src{t} 1 2 3
        make_members_expired r src{t} {1 2 3}

        set repl [attach_to_replication_stream]
        assert_equal 0 [r SORT src{t} STORE dst{t}]
        r RPUSH dst{t} stale
        assert_equal 0 [r SORT src{t} STORE dst{t}]

        assert_replication_stream $repl {
            {select *}
            {rpush dst{t} stale}
            {unlink dst{t}}
        }
        assert_equal {} [read_from_replication_stream $repl]
        close_replication_stream $repl
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    test {SPERSIST naming an expired member propagates only the members persisted} {
        flush_and_disable_active_expiry
        seed_expired_member r myset

        set repl [attach_to_replication_stream]
        assert_equal {1 -2} [r SPERSIST myset MEMBERS 2 live expired]

        assert_replication_stream $repl {
            {select *}
            {spersist myset MEMBERS 1 live}
        }
        assert_equal {} [read_from_replication_stream $repl]
        close_replication_stream $repl
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    test {SEXPIRE naming an expired member propagates only the members updated} {
        flush_and_disable_active_expiry
        seed_expired_member r myset

        set repl [attach_to_replication_stream]
        assert_equal {1 -2} [r SEXPIRE myset 300 MEMBERS 2 live expired]
        set at [set_member_pexpiretime r myset live]

        assert_replication_stream $repl [subst {
            {select *}
            {spexpireat myset $at MEMBERS 1 live}
        }]
        assert_equal {} [read_from_replication_stream $repl]
        close_replication_stream $repl
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    foreach encoding {listpack hashtable} {
        test "DEBUG RELOAD keeps member TTLs - $encoding" {
            r FLUSHALL
            use_set_encoding $encoding

            r SADD myset m1 m2 m3
            set exp [expr {[clock milliseconds] + 100000}]
            r SPEXPIREAT myset $exp MEMBERS 1 m1
            assert_encoding $encoding myset
            assert_equal 1 [get_keys_with_volatile_items r]

            r DEBUG RELOAD

            assert_encoding $encoding myset
            assert_equal 3 [r SCARD myset]
            assert_equal $exp [set_member_pexpiretime r myset m1]
            assert_equal -1 [set_member_ttl r myset m3]
            assert_equal 1 [get_keys_with_volatile_items r]

            use_set_encoding listpack
        } {OK} {needs:debug}
    }

    test {DEBUG RELOAD of a set whose members are all expired drops the key} {
        flush_and_disable_active_expiry

        r SADD myset m1 m2 m3
        make_members_expired r myset {m1 m2 m3}

        r DEBUG RELOAD

        assert_equal 0 [r EXISTS myset]
        assert_equal 0 [get_keys_with_volatile_items r]

        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    test {DUMP/RESTORE keeps member TTLs} {
        r FLUSHALL

        r SADD myset m1 m2 m3
        set exp [expr {[clock milliseconds] + 200000}]
        r SPEXPIREAT myset $exp MEMBERS 1 m1
        set serialized [r DUMP myset]

        r RESTORE rstr 0 $serialized

        assert_equal 3 [r SCARD rstr]
        assert_equal $exp [set_member_pexpiretime r rstr m1]
        assert_equal -1 [set_member_ttl r rstr m2]
        assert_equal 2 [get_keys_with_volatile_items r]
        assert_encoding listpack rstr
    } {}

    test {RESTORE of an all-expired payload loads the expired members} {
        flush_and_disable_active_expiry

        r SADDEX myset PX 1 MEMBERS 3 m1 m2 m3
        set serialized [r DUMP myset]
        wait_for_condition 50 100 {
            [r STTL myset MEMBERS 3 m1 m2 m3] eq {-2 -2 -2}
        } else {
            fail "set members did not expire"
        }
        r DEL myset
        r RESTORE myset 0 $serialized

        assert_equal 1 [r EXISTS myset]
        assert_equal 3 [r SCARD myset]
        assert_equal {} [r SMEMBERS myset]

        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    test {COPY of a hashtable set keeps an expired member and its TTL} {
        flush_and_disable_active_expiry
        use_set_encoding hashtable
        r SADD src{t} expired l1 l2
        make_members_expired r src{t} {expired}
        assert_encoding hashtable src{t}

        r COPY src{t} dst{t}
        assert_equal 3 [r SCARD dst{t}]
        assert_equal 0 [r SISMEMBER dst{t} expired]

        use_set_encoding listpack
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    foreach cmd {COPY RENAME} {
        test "$cmd keeps member TTLs" {
            r FLUSHALL

            r SADD myset{t} m1 m2 m3
            set exp [expr {[clock milliseconds] + 200000}]
            r SPEXPIREAT myset{t} $exp MEMBERS 1 m1

            if {$cmd eq "COPY"} {
                assert_equal 1 [r COPY myset{t} newset{t}]
                assert_equal $exp [set_member_pexpiretime r myset{t} m1]
                assert_equal 2 [get_keys_with_volatile_items r]
            } else {
                r RENAME myset{t} newset{t}
                assert_equal 0 [r EXISTS myset{t}]
                assert_equal 1 [get_keys_with_volatile_items r]
            }

            assert_equal 3 [r SCARD newset{t}]
            assert_equal $exp [set_member_pexpiretime r newset{t} m1]
            assert_equal -1 [set_member_ttl r newset{t} m3]
        } {}
    }

    test {MOVE keeps member TTLs and moves the volatile tracking} {
        r FLUSHALL

        r SADD myset m1 m2
        set exp [expr {[clock milliseconds] + 200000}]
        r SPEXPIREAT myset $exp MEMBERS 1 m1
        assert_equal 1 [get_keys_with_volatile_items r]

        assert_equal 1 [r MOVE myset 10]
        assert_equal 0 [r EXISTS myset]
        assert_equal 0 [get_keys_with_volatile_items r 9]

        r select 10
        assert_equal 2 [r SCARD myset]
        assert_equal $exp [set_member_pexpiretime r myset m1]
        assert_equal -1 [set_member_ttl r myset m2]
        assert_equal 1 [get_keys_with_volatile_items r 10]
        r FLUSHDB
        r select 9
    } {OK} {singledb:skip}

    set RDB_TYPE_SET 2
    set RDB_TYPE_SET_LISTPACK 20
    # Type 23 is upstream's RDB_TYPE_PATH_HASH, so volatile sets take the next one.
    set RDB_TYPE_SET_2 24

    foreach {encoding rdb_type} [list listpack $RDB_TYPE_SET_LISTPACK hashtable $RDB_TYPE_SET] {
        test "DUMP of a $encoding set uses SET_2 only while a member has a TTL" {
            r FLUSHALL
            use_set_encoding $encoding
            r SADD myset a b c
            assert_encoding $encoding myset
            assert_equal $rdb_type [dump_payload_type [r DUMP myset]]

            assert_equal {1 1 1} [r SEXPIRE myset $::live_member_ttl MEMBERS 3 a b c]
            assert_equal $RDB_TYPE_SET_2 [dump_payload_type [r DUMP myset]]

            assert_equal {1 1 1} [r SPERSIST myset MEMBERS 3 a b c]
            assert_equal $rdb_type [dump_payload_type [r DUMP myset]]
        }
    }
    use_set_encoding listpack

    start_server {tags {needs:repl external:skip}} {
        set primary [srv -1 client]
        set primary_host [srv -1 host]
        set primary_port [srv -1 port]
        set replica [srv 0 client]

        test {Full sync carries member TTLs} {
            $primary FLUSHALL

            set now [clock milliseconds]
            set m1_exp [expr {$now + 50000}]
            set m2_exp [expr {$now + 70000}]

            $primary SADD myset m1 m2 m3
            $primary SPEXPIREAT myset $m1_exp MEMBERS 1 m1
            $primary SPEXPIREAT myset $m2_exp MEMBERS 1 m2

            attach_replica $primary $replica $primary_host $primary_port

            assert_equal $m1_exp [set_member_pexpiretime $replica myset m1]
            assert_equal $m2_exp [set_member_pexpiretime $replica myset m2]
            assert_equal -1 [set_member_ttl $replica myset m3]
            assert_equal 3 [$replica SCARD myset]
            assert_equal 1 [get_keys_with_volatile_items $replica]
        }

        test {A replica never reclaims an expired member on its own} {
            $primary FLUSHALL
            $primary DEBUG SET-ACTIVE-EXPIRE 0

            $primary SADD myset m1 keepme
            make_members_expired $primary myset {m1}
            wait_for_ofs_sync $primary $replica

            assert_equal 2 [$replica SCARD myset]
            assert_equal {keepme} [$replica SMEMBERS myset]
            assert_equal 1 [get_keys_with_volatile_items $replica]

            $primary DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}

        test {A promoted replica reclaims its expired members itself} {
            $primary FLUSHALL
            $primary DEBUG SET-ACTIVE-EXPIRE 0

            $primary SADD myset m1 m2 keepme
            make_members_expired $primary myset {m1 m2}
            wait_for_ofs_sync $primary $replica

            assert_equal 3 [$replica SCARD myset]

            $replica replicaof no one
            wait_for_condition 100 100 {
                [status $replica role] eq "master"
            } else {
                fail "Replica didn't become master"
            }

            wait_for_condition 100 100 {
                [$replica SCARD myset] == 1
            } else {
                fail "promoted replica did not reclaim its expired members"
            }
            assert_equal {keepme} [$replica SMEMBERS myset]

            $primary DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}

        test {SREM, SPERSIST and SPEXPIRE preserve member expiry state on replicas} {
            $primary FLUSHALL
            $primary DEBUG SET-ACTIVE-EXPIRE 0
            attach_replica $primary $replica $primary_host $primary_port

            foreach key {kr kp kx} {
                seed_expired_member $primary $key
            }

            assert_equal 1 [$primary SREM kr live]
            assert_equal {-2 1} [$primary SPERSIST kp MEMBERS 2 expired live]
            assert_equal {-2 1} [$primary SPEXPIRE kx 30000 XX MEMBERS 2 expired live]
            wait_for_ofs_sync $primary $replica

            foreach key {kr kp kx} {
                assert_equal [$primary SCARD $key] [$replica SCARD $key]
                assert_equal [$primary SPEXPIRETIME $key MEMBERS 2 expired live] \
                    [$replica SPEXPIRETIME $key MEMBERS 2 expired live]
            }

            $primary DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}

        # Startup RDB loading propagates skipped members; DEBUG RELOAD does not.
        test {A primary loading an RDB at startup feeds SREM for the expired members it skips} {
            $primary FLUSHALL
            $primary DEBUG SET-ACTIVE-EXPIRE 0
            $primary SADD myset live1 live2 expired
            make_members_expired $primary myset {expired}
            assert_equal 3 [$primary SCARD myset]
            wait_for_ofs_sync $primary $replica
            # The shutdown save records the final replication offset, so the
            # replica can partially resync and receive the fed SREM.
            $primary config set save "3600 1"

            restart_server -1 true false
            set primary [srv -1 client]
            wait_for_condition 50 100 {
                [status $replica master_link_status] eq {up} && [status $primary sync_partial_ok] == 1
            } else {
                fail "the replica did not partially resync with the restarted primary"
            }

            assert_equal 2 [$primary SCARD myset]
            assert_equal {live1 live2} [lsort [$primary SMEMBERS myset]]
            assert_equal 0 [get_keys_with_volatile_items $primary]
            wait_for_condition 50 100 {
                [$replica SCARD myset] == 2
            } else {
                fail "the replica never received SREM for the skipped member"
            }
            assert_equal {live1 live2} [lsort [$replica SMEMBERS myset]]
        } {} {needs:debug}
    }
}

tags {"aof external:skip"} {
    foreach rdb_preamble {"yes" "no"} {
        set defaults {appendonly {yes} appendfilename {appendonly.aof} appenddirname {appendonlydir} auto-aof-rewrite-percentage {0}}
        set server_path [tmpdir server.setexpire.aof]
        start_server_aof [list dir $server_path aof-use-rdb-preamble $rdb_preamble] {
            # start_server_aof runs this body in its own frame, so re-read the loop value.
            set rdb_preamble [lindex [r config get aof-use-rdb-preamble] 1]
            foreach encoding {listpack hashtable} {
                test "Member TTLs survive AOF rewrite and restart, preamble $rdb_preamble - $encoding" {
                    flush_and_disable_active_expiry
                    use_set_encoding $encoding
                    assert_equal 0 [get_keys_with_volatile_items r]

                    set long_expire [expr {[clock milliseconds] + 1000000000}]

                    for {set i 1} {$i <= 10} {incr i} {
                        r SADD myset p$i
                        r SADDEX myset PXAT $long_expire MEMBERS 1 m$i
                    }
                    for {set i 11} {$i <= 20} {incr i} {
                        r SADDEX myset PX 20 MEMBERS 1 m$i
                    }

                    for {set i 11} {$i <= 20} {incr i} {
                        wait_for_condition 100 100 {
                            [set_member_ttl r myset m$i] eq "-2"
                        } else {
                            fail "member m$i did not expire"
                        }
                    }
                    assert_equal 1 [get_keys_with_volatile_items r]

                    waitForBgrewriteaof r
                    r BGREWRITEAOF
                    waitForBgrewriteaof r
                    if {"$rdb_preamble" eq "no"} {
                        validate_aof_content [get_base_aof_path r] 20 0 SREM
                    }

                    restart_server 0 true false

                    assert_equal 20 [llength [r SMEMBERS myset]]
                    for {set i 1} {$i <= 10} {incr i} {
                        assert_equal $long_expire [set_member_pexpiretime r myset m$i]
                        assert_equal -1 [set_member_ttl r myset p$i]
                    }
                    assert_equal 1 [get_keys_with_volatile_items r]
                } {} {needs:debug}

                test "SPOP keeps AOF framing valid, preamble $rdb_preamble - $encoding" {
                    flush_and_disable_active_expiry
                    use_set_encoding $encoding
                    set long_expire [expr {[clock milliseconds] + 1000000000}]
                    assert_equal 3 [r SADDEX myset PXAT $long_expire MEMBERS 3 a b c]
                    assert_equal 1 [llength [r SPOP myset 1]]
                    r SADD myset persistent
                    set before [lsort [r SMEMBERS myset]]

                    waitForBgrewriteaof r
                    r BGREWRITEAOF
                    waitForBgrewriteaof r
                    restart_server 0 true false

                    assert_equal $before [lsort [r SMEMBERS myset]]
                    assert_equal -1 [set_member_ttl r myset persistent]
                    assert_equal 1 [get_keys_with_volatile_items r]
                } {} {needs:debug}

                test "An expired-only AOF base preserves the key TTL for later writes, preamble $rdb_preamble - $encoding" {
                    flush_and_disable_active_expiry
                    use_set_encoding $encoding
                    r SADD myset expired
                    r PEXPIRE myset 600000
                    make_members_expired r myset {expired}

                    waitForBgrewriteaof r
                    r BGREWRITEAOF
                    waitForBgrewriteaof r
                    r SADD myset persistent
                    restart_server 0 true false

                    assert_equal {persistent} [r SMEMBERS myset]
                    assert_morethan [r PTTL myset] 0
                } {} {needs:debug}
            }
        }
    }
}

start_cluster 2 0 {tags {external:skip cluster}} {
    set key myset

    test "MIGRATE carries member TTLs to the importing node" {
        R 0 FLUSHALL
        R 1 FLUSHALL

        set exp [expr {[clock milliseconds] + 300000}]
        R 0 SADD $key volatile1 volatile2 persistent
        R 0 SPEXPIREAT $key $exp MEMBERS 2 volatile1 volatile2
        assert_equal 1 [get_keys_with_volatile_items [Rn 0]]

        migrate_key_slot 0 1 $key

        assert_equal 3 [R 1 SCARD $key]
        assert_equal [list $exp $exp -1] \
            [R 1 SPEXPIRETIME $key MEMBERS 3 volatile1 volatile2 persistent]
        assert_equal 1 [get_keys_with_volatile_items [Rn 1]]
        assert_equal 0 [get_keys_with_volatile_items [Rn 0]]
        migrate_key_slot 1 0 $key
    }

    test "MIGRATE carries an expired member the exporting node had not reclaimed" {
        R 0 FLUSHALL
        R 1 FLUSHALL
        R 0 DEBUG SET-ACTIVE-EXPIRE 0
        R 1 DEBUG SET-ACTIVE-EXPIRE 0

        R 0 SADD $key live expired
        make_members_expired [Rn 0] $key {expired}
        assert_equal 2 [R 0 SCARD $key]

        migrate_key_slot 0 1 $key

        # MIGRATE preserves unreclaimed members for destination-side expiration.
        assert_equal 2 [R 1 SCARD $key]
        assert_equal {live} [R 1 SMEMBERS $key]

        R 1 DEBUG SET-ACTIVE-EXPIRE 1
        wait_for_condition 100 100 {
            [R 1 SCARD $key] == 1
        } else {
            fail "the importing node did not remove the expired member"
        }
        assert_equal 0 [get_keys_with_volatile_items [Rn 1]]

        R 0 DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}
}

start_server {overrides {set-max-listpack-entries 0}} {
    test {SADDEX MXX safely applies a TTL to duplicate persistent members} {
        r SADD ttl-review member
        assert_equal 0 [r SADDEX ttl-review MXX EX 60 MEMBERS 2 member member]
        assert_equal 1 [r SCARD ttl-review]
        assert_morethan [set_member_ttl r ttl-review member] 0
    }

    test {SADDEX MXX safely removes a TTL from duplicate volatile members} {
        r FLUSHALL
        assert_equal 1 [r SADDEX ttl-review EX 60 MEMBERS 1 member]
        assert_equal 0 [r SADDEX ttl-review MXX MEMBERS 2 member member]
        assert_equal 1 [r SCARD ttl-review]
        assert_equal -1 [set_member_ttl r ttl-review member]
    }
}

# The blocks below all run inside one 8192 ms expiry-index window, where the
# index reports the earliest expiry as the window end while every hidden member
# is still present and still unreclaimable. Each block is separate because a
# path that trusts the index either aborts the server or never returns.
start_server {tags {"setexpire needs:repl external:skip"} overrides {set-max-listpack-entries 0}} {
    start_server {overrides {set-max-listpack-entries 0}} {
        set primary [srv -1 client]
        set primary_host [srv -1 host]
        set primary_port [srv -1 port]
        set replica [srv 0 client]

        test "SPOP with a count keeps hidden members while removing live ones" {
            $primary FLUSHALL
            $primary DEBUG SET-ACTIVE-EXPIRE 0
            $replica DEBUG SET-ACTIVE-EXPIRE 0
            attach_replica $primary $replica $primary_host $primary_port
            set live_members [make_rax_window_set $primary myset 200 60]
            wait_for_ofs_sync $primary $replica
            assert_equal 260 [$replica SCARD myset]

            # Volatile sets remove live members in place so hidden members stay untouched.
            set popped [$primary SPOP myset 230]
            assert_inside_rax_window
            assert_equal [llength $popped] [llength [lsort -unique $popped]]
            assert_all_live $popped $live_members

            # Only the returned members left the set, so the hidden ones are
            # still there, and the replica was told exactly the same.
            assert_equal [expr {260 - [llength $popped]}] [$primary SCARD myset]
            wait_for_ofs_sync $primary $replica
            assert_equal [$primary SCARD myset] [$replica SCARD myset]
            assert_equal [lsort [$primary SMEMBERS myset]] [lsort [$replica SMEMBERS myset]]

            $primary DEBUG SET-ACTIVE-EXPIRE 1
            $replica DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}

        foreach cmd {SINTERSTORE SUNIONSTORE SDIFFSTORE} {
            test "$cmd over a source with hidden members stores the same set on the replica" {
                $primary FLUSHALL
                $primary DEBUG SET-ACTIVE-EXPIRE 0
                $replica DEBUG SET-ACTIVE-EXPIRE 0
                attach_replica $primary $replica $primary_host $primary_port
                make_rax_window_set $primary src{t} 200 60
                # Live and hidden members of the source, so every set operation
                # has a result that depends on which of them are visible.
                $primary SADD other{t} live0 live1 live2 h0 h1
                wait_for_ofs_sync $primary $replica
                # The comparison only means something if the replica still holds
                # the members the primary is hiding.
                assert_equal 260 [$replica SCARD src{t}]

                $primary $cmd dst{t} src{t} other{t}
                assert_inside_rax_window
                # A STORE reads its sources, so both sides must still hold them.
                assert_equal 260 [$primary SCARD src{t}]
                wait_for_ofs_sync $primary $replica
                assert_equal 260 [$replica SCARD src{t}]
                assert_equal [$primary SCARD dst{t}] [$replica SCARD dst{t}]
                assert_equal [lsort [$primary SMEMBERS dst{t}]] [lsort [$replica SMEMBERS dst{t}]]

                $primary DEBUG SET-ACTIVE-EXPIRE 1
                $replica DEBUG SET-ACTIVE-EXPIRE 1
            } {OK} {needs:debug}
        }

    }
}

start_server {tags {"setexpire external:skip"} overrides {set-max-listpack-entries 0}} {
    test "SRANDMEMBER with a count below the physical size returns unique live members" {
        r FLUSHALL
        r DEBUG SET-ACTIVE-EXPIRE 0
        set live_members [make_rax_window_set r myset 200 150]

        # Three times the count exceeds the physical size, which selects the
        # path that copies the set and subtracts down to the count.
        set got [r SRANDMEMBER myset 120]
        assert_inside_rax_window
        assert_equal 120 [llength [lsort -unique $got]]
        assert_all_live $got $live_members
        assert_equal 350 [r SCARD myset]
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    # Inside the window the hidden members' bucket key is still ahead of the
    # clock, so it is the one bucket a census cannot settle from its key alone.
    test "SRANDMEMBER with a negative count draws only live members inside the window" {
        r FLUSHALL
        r DEBUG SET-ACTIVE-EXPIRE 0
        set live_members [make_rax_window_set r myset 200 150]

        set got [r SRANDMEMBER myset -600]
        assert_inside_rax_window
        assert_equal 600 [llength $got]
        assert_all_live $got $live_members
        assert_equal 350 [r SCARD myset]
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}
}

# A hidden-dense hashtable set: 'hidden' members kept past their deadline, plus
# live members that either carry a TTL of their own or carry none. Those are the
# two halves a draw is split into -- the expiry index can name the first by rank
# and holds nothing at all about the second -- so both must be covered, together
# with the mix that takes one of each within a single reply.
proc make_hidden_dense_set {r key hidden live live_has_ttl} {
    $r DEL $key
    set hidden_members {}
    for {set i 0} {$i < $hidden} {incr i} { lappend hidden_members h$i }
    $r SADD $key {*}$hidden_members
    make_members_expired $r $key $hidden_members

    set live_members {}
    for {set i 0} {$i < $live} {incr i} { lappend live_members live$i }
    if {$live > 0} {
        if {$live_has_ttl} {
            $r SADDEX $key EX 3600 MEMBERS $live {*}$live_members
        } else {
            $r SADD $key {*}$live_members
        }
    }
    assert_equal [expr {$hidden + $live}] [$r SCARD $key]
    assert_equal $live [llength [$r SMEMBERS $key]]
    return $live_members
}

start_server {tags {"setexpire external:skip"} overrides {set-max-listpack-entries 0}} {
    foreach {live_kind live_has_ttl} {volatile 1 persistent 0} {
        test "SRANDMEMBER draws only live members of a hidden-dense set - $live_kind live" {
            flush_and_disable_active_expiry
            set live_members [make_hidden_dense_set r myset 400 20 $live_has_ttl]
            assert_encoding hashtable myset

            for {set i 0} {$i < 50} {incr i} {
                assert_all_live [list [r SRANDMEMBER myset]] $live_members
                assert_all_live [r SRANDMEMBER myset 1] $live_members
                assert_all_live [r SRANDMEMBER myset -1] $live_members
            }
            foreach count {2 7 19 20 25 500} {
                assert_all_live [r SRANDMEMBER myset $count] $live_members
                set got [r SRANDMEMBER myset -$count]
                assert_equal $count [llength $got]
                assert_all_live $got $live_members
            }

            # A read reclaims nothing, so every hidden member is still stored.
            assert_equal 420 [r SCARD myset]
            assert_equal 20 [llength [r SMEMBERS myset]]
            assert_equal 1 [get_keys_with_volatile_items r]
            r DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}

        test "SRANDMEMBER with a negative count is unbiased over a hidden-dense set - $live_kind live" {
            flush_and_disable_active_expiry
            set live_members [make_hidden_dense_set r myset 400 20 $live_has_ttl]

            set draws 4000
            set got [r SRANDMEMBER myset -$draws]
            assert_equal $draws [llength $got]
            assert_all_live $got $live_members

            # 200 expected per member, sd ~14. The band is wide enough that no run
            # trips it by chance and narrow enough that a rank the draw cannot
            # reach, or one it favours, fails.
            foreach m $live_members {
                set seen [llength [lsearch -all -exact $got $m]]
                assert_equal 1 [expr {$seen >= 100 && $seen <= 320}] \
                    "member $m drawn $seen times out of $draws over 20 live members"
            }
            r DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}
    }

    test "SRANDMEMBER draws from both halves of a hidden-dense set" {
        flush_and_disable_active_expiry
        r DEL myset
        set hidden_members {}
        for {set i 0} {$i < 400} {incr i} { lappend hidden_members h$i }
        r SADD myset {*}$hidden_members
        make_members_expired r myset $hidden_members
        set with_ttl {}
        set without_ttl {}
        for {set i 0} {$i < 10} {incr i} {
            lappend with_ttl t$i
            lappend without_ttl p$i
        }
        r SADDEX myset EX 3600 MEMBERS 10 {*}$with_ttl
        r SADD myset {*}$without_ttl

        set got [r SRANDMEMBER myset -3000]
        assert_equal 3000 [llength $got]
        assert_all_live $got [concat $with_ttl $without_ttl]
        foreach m [concat $with_ttl $without_ttl] {
            assert_equal 1 [expr {[lsearch -exact $got $m] != -1}] "member $m was never drawn"
        }
        assert_equal 420 [r SCARD myset]
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    test "SRANDMEMBER returns the one live member of a hidden-dense set" {
        flush_and_disable_active_expiry
        foreach live_has_ttl {1 0} {
            set live_members [make_hidden_dense_set r myset 400 1 $live_has_ttl]
            assert_equal {live0} [r SRANDMEMBER myset]
            assert_equal {live0} [r SRANDMEMBER myset 1]
            assert_equal {live0} [r SRANDMEMBER myset 5]
            assert_equal [lrepeat 5 live0] [r SRANDMEMBER myset -5]
            assert_equal 401 [r SCARD myset]
        }
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    test "SRANDMEMBER over an all-hidden set answers empty without reclaiming" {
        flush_and_disable_active_expiry
        make_hidden_dense_set r myset 400 0 0
        assert_equal {} [r SRANDMEMBER myset]
        assert_equal {} [r SRANDMEMBER myset 1]
        assert_equal {} [r SRANDMEMBER myset 500]
        assert_equal {} [r SRANDMEMBER myset -500]
        assert_equal 400 [r SCARD myset]
        assert_equal 1 [get_keys_with_volatile_items r]
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    test "SRANDMEMBER with a count at or above the live population returns exactly it" {
        flush_and_disable_active_expiry
        foreach live_has_ttl {1 0} {
            set live_members [make_hidden_dense_set r myset 400 20 $live_has_ttl]
            foreach count {20 21 420 500} {
                assert_equal [lsort $live_members] [lsort [r SRANDMEMBER myset $count]]
            }
        }
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    foreach {live_kind live_has_ttl} {volatile 1 persistent 0} {
        test "SPOP with a small count over a hidden-dense set pops live members only - $live_kind live" {
            flush_and_disable_active_expiry
            foreach count {1 2} {
                set live_members [make_hidden_dense_set r myset 400 20 $live_has_ttl]
                set popped [r SPOP myset $count]
                assert_equal $count [llength $popped]
                assert_equal $count [llength [lsort -unique $popped]]
                assert_all_live $popped $live_members

                # Only the returned live members left; the hidden ones are still
                # stored and still hidden.
                assert_equal [expr {420 - $count}] [r SCARD myset]
                assert_equal [expr {20 - $count}] [llength [r SMEMBERS myset]]
                foreach m $popped { assert_equal 0 [r SISMEMBER myset $m] }
                assert_equal 1 [r EXISTS myset]
                assert_equal 1 [get_keys_with_volatile_items r]
            }
            r DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}
    }

    test "SPOP over an all-hidden set returns empty and keeps every hidden member" {
        flush_and_disable_active_expiry
        make_hidden_dense_set r myset 400 0 0
        assert_equal {{} 0 0} [capture_mutation_signals myset {
            assert_equal {} [r SPOP myset 1]
            assert_equal {} [r SPOP myset 2]
            assert_equal {} [r SPOP myset 500]
        }]
        assert_equal 400 [r SCARD myset]
        assert_equal 1 [r EXISTS myset]
        assert_equal 1 [get_keys_with_volatile_items r]
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    test "SPOP with a count of 1 pops the one live member of a hidden-dense set" {
        flush_and_disable_active_expiry
        foreach live_has_ttl {1 0} {
            make_hidden_dense_set r myset 400 1 $live_has_ttl
            assert_equal {live0} [r SPOP myset 1]
            assert_equal 400 [r SCARD myset]
            assert_equal {} [r SMEMBERS myset]
            assert_equal {} [r SPOP myset 1]
        }
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    # Pops taken from both halves of the census must leave the other half, and the
    # hidden members, exactly as they were.
    test "SPOP over a hidden-dense set keeps the remaining TTLs and hidden members" {
        flush_and_disable_active_expiry
        r DEL myset
        set hidden_members {}
        for {set i 0} {$i < 400} {incr i} { lappend hidden_members h$i }
        r SADD myset {*}$hidden_members
        make_members_expired r myset $hidden_members
        set with_ttl {}
        set without_ttl {}
        for {set i 0} {$i < 10} {incr i} {
            lappend with_ttl t$i
            lappend without_ttl p$i
        }
        r SADDEX myset EX 3600 MEMBERS 10 {*}$with_ttl
        r SADD myset {*}$without_ttl

        set popped [r SPOP myset 12]
        assert_equal 12 [llength $popped]
        assert_equal 12 [llength [lsort -unique $popped]]
        assert_all_live $popped [concat $with_ttl $without_ttl]
        assert_equal 408 [r SCARD myset]
        set left [r SMEMBERS myset]
        assert_equal 8 [llength $left]
        foreach m $left {
            if {[lsearch -exact $with_ttl $m] != -1} {
                assert_range [set_member_ttl r myset $m] 1 3600
            } else {
                assert_equal -1 [set_member_ttl r myset $m]
            }
        }
        # Only live members left, so every hidden member is still stored.
        assert_equal 400 [expr {408 - [llength $left]}]
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}
}

start_server {tags {"setexpire needs:repl external:skip"} overrides {set-max-listpack-entries 0}} {
    start_server {overrides {set-max-listpack-entries 0}} {
        set primary [srv -1 client]
        set primary_host [srv -1 host]
        set primary_port [srv -1 port]
        set replica [srv 0 client]

        test "SRANDMEMBER on a replica draws only live members and reclaims nothing" {
            $primary FLUSHALL
            $primary DEBUG SET-ACTIVE-EXPIRE 0
            $replica DEBUG SET-ACTIVE-EXPIRE 0
            set live_members [make_hidden_dense_set $primary myset 400 20 1]
            attach_replica $primary $replica $primary_host $primary_port

            # A replica read sees POLICY_KEEP_EXPIRED, so the members are hidden
            # there too and only the primary's SREM ever removes them.
            assert_equal 420 [$replica SCARD myset]
            set got [$replica SRANDMEMBER myset -200]
            assert_equal 200 [llength $got]
            assert_all_live $got $live_members
            assert_all_live [$replica SRANDMEMBER myset 20] $live_members
            assert_equal [lsort $live_members] [lsort [$replica SMEMBERS myset]]
            assert_equal 420 [$replica SCARD myset]

            $primary DEBUG SET-ACTIVE-EXPIRE 1
            $replica DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}
    }
}

# A set operand of a sorted set operation is iterated, and its members are handed
# to the other operands as lookup keys. Both are places where the set's own
# member metadata and its hidden members can leak out.

# One live and one hidden member, plus the sorted set operands that leave each
# store command with exactly the live member to store. None of them names the
# hidden member, so a command that sees it stores it.
proc seed_hidden_member_store_sources {r} {
    $r SADD s{t} live gone
    make_members_expired $r s{t} {gone}
    # Larger than the set, so the set is the operand the intersection iterates.
    $r ZADD z{t} 1 live 1 gone 1 extra
    $r ZADD other{t} 1 extra
    $r ZADD dst{t} 1 stale
}

# {store command, the score its single stored member gets, its operands}
set ::hidden_member_stores {
    ZUNIONSTORE 1 {1 s{t}}
    ZINTERSTORE 2 {2 s{t} z{t}}
    ZDIFFSTORE 1 {2 s{t} other{t}}
}

start_server {tags {"setexpire external:skip"}} {
    foreach encoding {listpack hashtable} {
        test "A set source keeps its member TTLs across the sorted set operations - $encoding" {
            r FLUSHALL
            use_set_encoding $encoding
            r SADDEX s{t} EX $::live_member_ttl MEMBERS 2 m1 m2
            assert_encoding $encoding s{t}
            r ZADD z{t} 1 m1 1 m2 1 other

            r ZUNION 1 s{t}
            r ZUNIONSTORE u{t} 1 s{t}
            r ZINTERSTORE i{t} 2 s{t} z{t}
            r ZINTERCARD 2 s{t} z{t}
            r ZDIFF 2 s{t} z{t}

            assert_range [set_member_ttl r s{t} m1] 90 $::live_member_ttl
            assert_range [set_member_ttl r s{t} m2] 90 $::live_member_ttl
            # Freeing a member whose expiry was lost reads the wrong allocation,
            # once through the member's own removal and once through the key's.
            assert_equal 1 [r SREM s{t} m1]
            assert_equal 1 [r DEL s{t}]
            assert_equal 0 [get_keys_with_volatile_items r]
        }

        test "A set source hides its expired members from the sorted set operations - $encoding" {
            flush_and_disable_active_expiry
            use_set_encoding $encoding
            r SADD s{t} live gone
            make_members_expired r s{t} {gone}
            assert_encoding $encoding s{t}
            r ZADD z{t} 1 live 1 gone 1 extra
            r ZADD one{t} 1 live

            # The set is the operand being iterated: the only one, or the smallest.
            assert_equal {live} [r ZUNION 1 s{t}]
            assert_equal 1 [r ZUNIONSTORE u{t} 1 s{t}]
            assert_equal {live} [r ZRANGE u{t} 0 -1]
            assert_equal 1 [r ZINTERCARD 2 s{t} z{t}]
            assert_equal 1 [r ZINTERSTORE i{t} 2 s{t} z{t}]
            assert_equal {live} [r ZRANGE i{t} 0 -1]
            assert_equal {} [r ZDIFF 2 s{t} one{t}]

            # The set is the larger operand: the one searched, not the one iterated.
            assert_equal {live} [r ZUNION 2 one{t} s{t}]
            assert_equal 1 [r ZINTERCARD 2 one{t} s{t}]
            assert_equal 1 [r ZINTERSTORE i{t} 2 one{t} s{t}]
            assert_equal {live} [r ZRANGE i{t} 0 -1]
            assert_equal {} [r ZDIFF 2 one{t} s{t}]
            r DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}

        foreach {cmd score operands} $::hidden_member_stores {
            test "$cmd over a set source with hidden members propagates its result - $encoding" {
                flush_and_disable_active_expiry
                use_set_encoding $encoding
                seed_hidden_member_store_sources r
                assert_encoding $encoding s{t}

                set repl [attach_to_replication_stream]
                assert_equal 1 [r $cmd dst{t} {*}$operands]

                # Replaying the command would read the hidden member, which is
                # live on a replica, so only the result itself may be sent.
                assert_replication_stream $repl [subst {
                    {multi}
                    {select *}
                    {unlink dst{t}}
                    {zadd dst{t} $score live}
                    {exec}
                }]
                close_replication_stream $repl
                assert_equal {live} [r ZRANGE dst{t} 0 -1]
                r DEBUG SET-ACTIVE-EXPIRE 1
            } {OK} {needs:debug}
        }

        test "ZUNIONSTORE over a set source with hidden members can store into it - $encoding" {
            flush_and_disable_active_expiry
            use_set_encoding $encoding
            seed_hidden_member_store_sources r

            set repl [attach_to_replication_stream]
            # The destination overwrites the source, so the result is the only
            # description of it left.
            assert_equal 1 [r ZUNIONSTORE s{t} 1 s{t}]

            assert_replication_stream $repl [subst {
                {multi}
                {select *}
                {unlink s{t}}
                {zadd s{t} 1 live}
                {exec}
            }]
            close_replication_stream $repl
            assert_equal {live} [r ZRANGE s{t} 0 -1]
            r DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}

        test "ZINTERSTORE over a set source of only hidden members propagates one UNLINK - $encoding" {
            flush_and_disable_active_expiry
            use_set_encoding $encoding
            r SADD s{t} gone
            make_members_expired r s{t} {gone}
            r ZADD z{t} 1 gone 1 extra
            r ZADD dst{t} 1 stale

            set repl [attach_to_replication_stream]
            assert_equal 0 [r ZINTERSTORE dst{t} 2 s{t} z{t}]

            assert_replication_stream $repl {
                {select *}
                {unlink dst{t}}
            }
            close_replication_stream $repl
            assert_equal 0 [r EXISTS dst{t}]
            r DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}
    }

    # MXX caches the members it looked up, and only members whose expiry state
    # already matches the one being applied stay in the cache. The past
    # expiration then takes a path of its own, which the cache must not outlive,
    # so a sanitizer build fails here if it does.
    test {SADDEX MXX with a past expiration removes volatile members} {
        r FLUSHALL
        use_set_encoding hashtable
        r SADDEX s{t} EX $::live_member_ttl MEMBERS 2 m1 m2
        set repl [attach_to_replication_stream]

        assert_equal 0 [r SADDEX s{t} MXX EX 0 MEMBERS 2 m1 m2]

        assert_replication_stream $repl {
            {select *}
            {srem s{t} m1 m2}
        }
        close_replication_stream $repl
        assert_equal 0 [r EXISTS s{t}]
        assert_equal 0 [get_keys_with_volatile_items r]
    }

    test {SADDEX MXX with a past expiration removes duplicate volatile members once} {
        r FLUSHALL
        use_set_encoding hashtable
        r SADDEX s{t} EX $::live_member_ttl MEMBERS 1 m1
        set repl [attach_to_replication_stream]

        # One cache slot per argument, both naming the same member.
        assert_equal 0 [r SADDEX s{t} MXX EX 0 MEMBERS 2 m1 m1]

        # The second argument names a member the first one already removed.
        assert_replication_stream $repl {
            {select *}
            {srem s{t} m1}
        }
        close_replication_stream $repl
        assert_equal 0 [r EXISTS s{t}]
        assert_equal 0 [get_keys_with_volatile_items r]
    }
}

# A replica holds the members the primary hides, so a store command it recomputes
# reads members the primary did not.
start_server {tags {"setexpire needs:repl external:skip"}} {
    start_server {} {
        set primary [srv -1 client]
        set primary_host [srv -1 host]
        set primary_port [srv -1 port]
        set replica [srv 0 client]

        foreach encoding {listpack hashtable} {
            foreach {cmd score operands} $::hidden_member_stores {
                test "$cmd over a set source with hidden members stores the same result on the replica - $encoding" {
                    $primary FLUSHALL
                    $primary DEBUG SET-ACTIVE-EXPIRE 0
                    $replica DEBUG SET-ACTIVE-EXPIRE 0
                    $primary config set set-max-listpack-entries [expr {$encoding eq "hashtable" ? 0 : 128}]
                    $replica config set set-max-listpack-entries [expr {$encoding eq "hashtable" ? 0 : 128}]
                    attach_replica $primary $replica $primary_host $primary_port
                    seed_hidden_member_store_sources $primary
                    wait_for_ofs_sync $primary $replica
                    # The comparison only means something while the replica still
                    # holds the member the primary is hiding.
                    assert_equal 2 [$replica SCARD s{t}]

                    assert_equal 1 [$primary $cmd dst{t} {*}$operands]
                    assert_equal {live} [$primary ZRANGE dst{t} 0 -1]
                    wait_for_ofs_sync $primary $replica
                    assert_equal [$primary ZRANGE dst{t} 0 -1 WITHSCORES] \
                        [$replica ZRANGE dst{t} 0 -1 WITHSCORES]

                    $primary DEBUG SET-ACTIVE-EXPIRE 1
                    $replica DEBUG SET-ACTIVE-EXPIRE 1
                } {OK} {needs:debug}
            }
        }
    }
}

# Build a set of live volatile members, numerous enough for the expiry index to be
# a RAX. The index reports the start of the window holding its earliest deadline,
# so while that window is the current one it claims an expired member that the set
# does not have. Returns the member names.
proc make_rax_live_set {r key members} {
    # Enter a window with room for the whole test body before the members expire,
    # which is what keeps the index's claim wrong rather than right.
    while {[clock milliseconds] % 8192 >= 1024} { after 50 }

    set names {}
    for {set i 0} {$i < $members} {incr i} { lappend names live$i }
    set ::rax_live_deadline [expr {[clock milliseconds] + 6000}]
    $r SADDEX $key PXAT $::rax_live_deadline MEMBERS [llength $names] {*}$names

    assert_equal $members [$r SCARD $key]
    assert_equal $members [llength [$r SMEMBERS $key]]
    assert_before_rax_deadline
    return $names
}

# A member that expired mid-test makes the run prove nothing, so it must fail.
proc assert_before_rax_deadline {} {
    set remaining [expr {$::rax_live_deadline - [clock milliseconds]}]
    assert_equal 1 [expr {$remaining > 0}] \
        "the members expired [expr {-$remaining}] ms ago, so this run proves nothing"
}

# Separate blocks because a path that trusts the index leaves a key that later
# reads of the same key then sample.
start_server {tags {"setexpire needs:repl external:skip"} overrides {set-max-listpack-entries 0}} {
    start_server {overrides {set-max-listpack-entries 0}} {
        set primary [srv -1 client]
        set primary_host [srv -1 host]
        set primary_port [srv -1 port]
        set replica [srv 0 client]

        foreach count {200 260} {
            test "SPOP with a count of $count over 200 live volatile members leaves no key" {
                $primary FLUSHALL
                $primary DEBUG SET-ACTIVE-EXPIRE 0
                $replica DEBUG SET-ACTIVE-EXPIRE 0
                attach_replica $primary $replica $primary_host $primary_port
                set members [make_rax_live_set $primary myset 200]
                wait_for_ofs_sync $primary $replica

                # A count at or above the size of a set the index calls partly
                # expired, of members it is all wrong about.
                set popped [$primary SPOP myset $count]
                assert_before_rax_deadline
                assert_equal [lsort $members] [lsort $popped]

                # Every member left, so no key may remain on either side.
                assert_equal 0 [$primary EXISTS myset]
                assert_equal 0 [$primary SCARD myset]
                assert_equal 0 [get_keys_with_volatile_items $primary]
                # Reads that sample a key left behind sample nothing.
                assert_equal {} [$primary SRANDMEMBER myset 3]
                assert_equal {} [$primary SPOP myset 3]
                wait_for_ofs_sync $primary $replica
                assert_equal 0 [$replica EXISTS myset]

                $primary DEBUG SET-ACTIVE-EXPIRE 1
                $replica DEBUG SET-ACTIVE-EXPIRE 1
            } {OK} {needs:debug}
        }
    }
}

start_server {tags {"setexpire needs:repl external:skip"} overrides {set-max-listpack-entries 0}} {
    start_server {overrides {set-max-listpack-entries 0}} {
        set primary [srv -1 client]
        set primary_host [srv -1 host]
        set primary_port [srv -1 port]
        set replica [srv 0 client]

        test "SPOP with a count over every live member keeps the hidden ones" {
            $primary FLUSHALL
            $primary DEBUG SET-ACTIVE-EXPIRE 0
            $replica DEBUG SET-ACTIVE-EXPIRE 0
            attach_replica $primary $replica $primary_host $primary_port
            set live_members [make_rax_window_set $primary myset 200 60]
            wait_for_ofs_sync $primary $replica
            assert_equal 260 [$replica SCARD myset]

            # The set the index is right about: popping its whole live remainder
            # still leaves the hidden members, so the key stays.
            set popped [$primary SPOP myset 60]
            assert_inside_rax_window
            assert_equal 60 [llength $popped]
            assert_all_live $popped $live_members
            assert_equal 1 [$primary EXISTS myset]
            assert_equal 200 [$primary SCARD myset]
            assert_equal {} [$primary SMEMBERS myset]
            wait_for_ofs_sync $primary $replica
            assert_equal [$primary SCARD myset] [$replica SCARD myset]

            $primary DEBUG SET-ACTIVE-EXPIRE 1
            $replica DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}
    }
}
