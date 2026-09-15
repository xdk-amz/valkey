# What the set commands see, write, convert and leave alone when members carry
# a TTL.

# TTL, in seconds, for a member that must stay live for a whole test.
set ::live_member_ttl 100

# Flush and disable active expiry, so that expired members linger.
proc flush_and_disable_active_expiry {} {
    r flushall
    r debug set-active-expire 0
}

# Build a set of $n members named m0..m<n-1>.
proc create_numbered_set {key n} {
    r del $key
    set members {}
    for {set i 0} {$i < $n} {incr i} {
        lappend members m$i
        if {[llength $members] == 500} {
            r sadd $key {*}$members
            set members {}
        }
    }
    if {[llength $members] > 0} {
        r sadd $key {*}$members
    }
}

# Assert that no element of $got is one of $forbidden.
proc assert_none_of {got forbidden} {
    foreach m $got {
        assert_equal -1 [lsearch -exact $forbidden $m] "unexpected member $m"
    }
}

# The three signals a mutating command must raise and a command that changes
# nothing must not: the keyspace notifications it emitted, how much it moved
# the change counter that drives persistence, and whether it invalidated a
# WATCH on $key. Returns {events dirty_delta watch_fired}, where events is a
# list of "<event> <key>" pairs.
proc capture_mutation_signals {key body} {
    r config set notify-keyspace-events KEA
    set rd [valkey_deferring_client]
    assert_equal {1} [psubscribe $rd __keyevent@*]
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
    r sadd mutation-marker x
    set events {}
    while 1 {
        set event [$rd read]
        set name [lindex [split [lindex $event 2] ":"] end]
        set event_key [lindex $event 3]
        if {$name eq "sadd" && $event_key eq "mutation-marker"} break
        lappend events "$name $event_key"
    }
    $rd close
    r del mutation-marker
    return [list $events $dirty_delta $watch_fired]
}

# Encoding conversion. An intset cannot carry per-member metadata, so the
# first member TTL has to convert it; dropping the last one may take it back.
start_server {
    tags {"setexpire"}
    overrides {
        "set-max-intset-entries" 512
        "set-max-listpack-entries" 128
        "set-max-listpack-value" 64
    }
} {
    test "A member TTL converts an intset to whatever the limits allow" {
        r flushall
        # Small set, listpacks enabled: listpack.
        use_set_encoding listpack
        r sadd myset 1 2 3
        assert_encoding intset myset
        assert_equal {1} [r sexpire myset $::live_member_ttl members 1 2]
        assert_encoding listpack myset
        assert_equal {1 2 3} [lsort [r smembers myset]]
        assert_range [set_member_ttl r myset 2] 1 $::live_member_ttl
        assert_equal {-1 -1} [r sttl myset members 2 1 3]

        # Listpacks disabled: hashtable. An all-integer set is still created as
        # an intset, because intset creation is governed by
        # set-max-intset-entries and not by the listpack limit.
        use_set_encoding hashtable
        r del myset
        r sadd myset 1 2 3
        assert_encoding intset myset
        assert_equal {1} [r sexpire myset $::live_member_ttl members 1 2]
        assert_encoding hashtable myset
        assert_range [set_member_ttl r myset 2] 1 $::live_member_ttl

        # More members than the listpack limit: hashtable again.
        use_set_encoding listpack
        r del myset
        for {set i 0} {$i < 200} {incr i} { r sadd myset $i }
        assert_encoding intset myset
        assert_equal {1} [r sexpire myset $::live_member_ttl members 1 7]
        assert_encoding hashtable myset
        assert_equal 200 [r scard myset]
    }

    test "SPERSIST of the last volatile member keeps the converted encoding" {
        r flushall
        use_set_encoding listpack
        r sadd myset 1 2 3
        assert_encoding intset myset
        assert_equal {1} [r sexpire myset $::live_member_ttl members 1 2]
        assert_equal {1} [r sexpire myset $::live_member_ttl members 1 3]
        assert_encoding listpack myset

        assert_equal {1} [r spersist myset members 1 2]
        assert_encoding listpack myset
        assert_equal {-1} [r sttl myset members 1 2]

        # Losing the last TTL does not convert the set back, as no other
        # shrinking mutation of a set does either.
        assert_equal {1} [r spersist myset members 1 3]
        assert_encoding listpack myset
        assert_equal {1 2 3} [lsort [r smembers myset]]
        assert_equal {-1 -1 -1} [r sttl myset members 3 1 2 3]
    }

    test "A TTL command that applies no TTL keeps the intset encoding" {
        r flushall
        use_set_encoding listpack
        r sadd myset 1 2 3
        assert_encoding intset myset
        # No member matched, so there is no metadata to store.
        assert_equal {-2} [r sexpire myset $::live_member_ttl members 1 9]
        assert_encoding intset myset
        assert_equal {-2 -2} [r sexpire myset $::live_member_ttl members 2 9 10]
        assert_encoding intset myset
        # A time in the past only removes the member, so again no TTL is
        # stored and the intset survives.
        assert_equal {2} [r sexpire myset 0 members 1 2]
        assert_equal {1 3} [lsort [r smembers myset]]
        assert_encoding intset myset
        # A mix converts, because one of the members does get a TTL.
        assert_equal {-2 1} [r sexpire myset $::live_member_ttl members 2 9 1]
        assert_no_match intset [r object encoding myset]
    }

    test "A member TTL on a non-integer member does not convert a listpack set" {
        r flushall
        use_set_encoding listpack
        r sadd myset a 1 2
        assert_encoding listpack myset
        assert_equal {1} [r sexpire myset $::live_member_ttl members 1 1]
        assert_encoding listpack myset
    }

    test "SEXPIRE on a missing key returns -2 and does not create the key" {
        r flushall
        assert_equal {-2} [r sexpire nosuchset $::live_member_ttl members 1 1]
        assert_equal {-2 -2} [r sttl nosuchset members 2 1 2]
        assert_equal 0 [r exists nosuchset]
    }

    test "SMOVE of a volatile integer member converts an intset destination" {
        r flushall
        use_set_encoding listpack
        r sadd src{t} 7
        r sadd dst{t} 1 2 3
        assert_encoding intset dst{t}
        assert_equal {1} [r sexpire src{t} $::live_member_ttl members 1 7]
        assert_equal 1 [r smove src{t} dst{t} 7]
        assert_no_match intset [r object encoding dst{t}]
        assert_range [set_member_ttl r dst{t} 7] 1 $::live_member_ttl
        assert_equal {1 2 3 7} [lsort [r smembers dst{t}]]
        assert_equal {-1 -1 -1} [r sttl dst{t} members 3 1 2 3]
    }

    test "SMOVE of a plain integer member leaves an intset destination alone" {
        r flushall
        use_set_encoding listpack
        r sadd src{t} 7
        r sadd dst{t} 1 2 3
        assert_encoding intset dst{t}
        assert_equal 1 [r smove src{t} dst{t} 7]
        # No TTL travels with the member, so there is nothing to store.
        assert_encoding intset dst{t}
        assert_equal {1 2 3 7} [lsort [r smembers dst{t}]]
    }

    test "SADD over the last expired member keeps the converted encoding" {
        flush_and_disable_active_expiry
        use_set_encoding listpack
        r sadd myset 1 2 3
        assert_encoding intset myset
        # The TTL converts the set; the expired member keeps it converted.
        make_members_expired r myset {3}
        assert_encoding listpack myset
        assert_equal 3 [r scard myset]

        # SADD replaces the expired member with a plain one, leaving nothing
        # volatile behind; the set still keeps the encoding it converted to.
        r sadd myset 3
        assert_encoding listpack myset
        assert_equal {1 2 3} [lsort [r smembers myset]]
        assert_equal 3 [r scard myset]
        assert_equal {-1 -1 -1} [r sttl myset members 3 1 2 3]
        r debug set-active-expire 1
    } {OK} {needs:debug}

    test "SMOVE of an integer over the last expired member keeps the encoding" {
        flush_and_disable_active_expiry
        use_set_encoding listpack
        r sadd src{t} 3
        r sadd dst{t} 1 2 3
        assert_encoding intset dst{t}
        make_members_expired r dst{t} {3}

        # The moved member carries no TTL, so the destination is plain again
        # but stays in the encoding the TTL converted it to.
        assert_equal 1 [r smove src{t} dst{t} 3]
        assert_encoding listpack dst{t}
        assert_equal {1 2 3} [lsort [r smembers dst{t}]]
        assert_equal 3 [r scard dst{t}]
        assert_equal {-1 -1 -1} [r sttl dst{t} members 3 1 2 3]
        assert_equal 0 [r exists src{t}]
        r debug set-active-expire 1
    } {OK} {needs:debug}
}

# What the set commands see while a member is expired but not yet removed. Both
# representations that can hold a TTL are exercised: the listpack walk and the
# hashtable walk are separate code paths.
start_server {
    tags {"setexpire"}
    overrides {
        "set-max-intset-entries" 512
        "set-max-listpack-entries" 128
        "set-max-listpack-value" 64
    }
} {
    foreach encoding {listpack hashtable} {
        test "SPOP with a count above the live size returns exactly the live members - $encoding" {
            flush_and_disable_active_expiry
            use_set_encoding $encoding
            create_numbered_set myset 10
            make_members_expired r myset {m0 m1 m2 m3}
            assert_encoding $encoding myset
            # SCARD is O(1) and still counts the four expired members.
            assert_equal 10 [r scard myset]

            set popped [r spop myset 10]
            assert_equal {m4 m5 m6 m7 m8 m9} [lsort $popped]
            assert_equal 6 [llength [lsort -unique $popped]]
            # The reply length matched its element count: a header of 10
            # followed by 6 elements would leave the connection out of sync and
            # this PING would read a leftover element instead of PONG.
            assert_equal {PONG} [r ping]
            # count >= SCARD is SPOP's "return the whole set" case: every live
            # member is returned and the key is deleted, expired members and
            # all, exactly as for a set without TTLs.
            assert_equal 0 [r exists myset]

            # Far more than the live size, on a set large enough to need more
            # than one sampling strategy.
            create_numbered_set myset 100
            set expired {}
            for {set i 10} {$i < 100} {incr i} { lappend expired m$i }
            make_members_expired r myset $expired
            assert_equal 100 [r scard myset]
            set popped [r spop myset 1000]
            assert_equal 10 [llength $popped]
            assert_equal 10 [llength [lsort -unique $popped]]
            assert_none_of $popped $expired
            assert_equal {PONG} [r ping]
            assert_equal 0 [r exists myset]
            r debug set-active-expire 1
        } {OK} {needs:debug}

        test "SPOP with a count below the live size returns live members only - $encoding" {
            flush_and_disable_active_expiry
            use_set_encoding $encoding
            create_numbered_set myset 10
            make_members_expired r myset {m0 m1 m2 m3}
            set popped [r spop myset 3]
            assert_equal 3 [llength $popped]
            assert_equal 3 [llength [lsort -unique $popped]]
            assert_none_of $popped {m0 m1 m2 m3}
            assert_equal {PONG} [r ping]
            # SPOP does not remove expired members, so the four are still
            # counted alongside the three live ones that were not popped.
            assert_equal 7 [r scard myset]
            assert_equal 3 [llength [r smembers myset]]
            assert_equal 1 [r exists myset]
            r debug set-active-expire 1
        } {OK} {needs:debug}

        test "SPOP with a count equal to the live size leaves the expired members behind - $encoding" {
            flush_and_disable_active_expiry
            use_set_encoding $encoding
            create_numbered_set myset 10
            make_members_expired r myset {m0 m1 m2 m3}
            assert_equal {m4 m5 m6 m7 m8 m9} [lsort [r spop myset 6]]
            assert_equal {PONG} [r ping]
            # Nothing live is left, but SPOP removes no expired member, so the key survives
            # with its four expired members until active expiry runs.
            assert_equal 1 [r exists myset]
            assert_equal 4 [r scard myset]
            assert_equal {} [r smembers myset]
            r debug set-active-expire 1
        } {OK} {needs:debug}

        test "SPOP on a set of only expired members returns nothing - $encoding" {
            flush_and_disable_active_expiry
            use_set_encoding $encoding
            r sadd myset a b c
            make_members_expired r myset {a b c}
            assert_equal 3 [r scard myset]
            # count < SCARD: nothing live to pop, and the key stays.
            assert_equal {} [r spop myset 2]
            # The single-member form must reply nil promptly rather than loop
            # looking for a live member.
            assert_equal {} [r spop myset]
            assert_equal {} [r spop myset]
            assert_equal {PONG} [r ping]
            assert_equal 1 [r exists myset]
            assert_equal 3 [r scard myset]
            assert_equal {-2 -2 -2} [r sttl myset members 3 a b c]
            # count >= SCARD: the whole-set case deletes the key.
            assert_equal {} [r spop myset 3]
            assert_equal 0 [r exists myset]
            r debug set-active-expire 1
        } {OK} {needs:debug}

        test "SPOP without a count never returns an expired member - $encoding" {
            flush_and_disable_active_expiry
            use_set_encoding $encoding
            create_numbered_set myset 10
            make_members_expired r myset {m0 m1 m2 m3}
            for {set i 0} {$i < 6} {incr i} {
                set m [r spop myset]
                assert {$m ne {}}
                assert_none_of [list $m] {m0 m1 m2 m3}
            }
            # All six live members are popped, so a further SPOP finds only
            # expired ones.
            assert_equal {} [r spop myset]
            assert_equal {PONG} [r ping]
            r debug set-active-expire 1
        } {OK} {needs:debug}

        test "SPOP with a count keeps the TTLs of the members that remain - $encoding" {
            r flushall
            r debug set-active-expire 1
            use_set_encoding $encoding
            r sadd s a b c d e f g h i j
            assert_equal {1 1} [r sexpire s 5000 MEMBERS 2 a b]
            # A count close to the set size takes the "rebuild the remainder"
            # strategy, which must carry the TTLs over.
            set popped [r spop s 8]
            assert_equal 8 [llength $popped]
            assert_equal 2 [r scard s]
            foreach m [r smembers s] {
                if {$m in {a b}} {
                    assert_range [set_member_ttl r s $m] 4990 5000
                } else {
                    assert_equal {-1} [r sttl s MEMBERS 1 $m]
                }
            }
        } {} {needs:debug}

        test "SRANDMEMBER never returns an expired member - $encoding" {
            flush_and_disable_active_expiry
            use_set_encoding $encoding
            create_numbered_set myset 10
            make_members_expired r myset {m0 m1 m2 m3}

            # A positive count above the live size returns every live member
            # once, with no duplicates and no expired member padding the reply.
            set got [r srandmember myset 20]
            assert_equal {m4 m5 m6 m7 m8 m9} [lsort $got]
            assert_equal 6 [llength [lsort -unique $got]]

            # Below the live size: no duplicates either.
            set got [r srandmember myset 4]
            assert_equal 4 [llength $got]
            assert_equal 4 [llength [lsort -unique $got]]
            assert_none_of $got {m0 m1 m2 m3}

            # A negative count returns exactly the requested number of
            # elements, repeats allowed, drawn only from the live members.
            set got [r srandmember myset -50]
            assert_equal 50 [llength $got]
            assert_none_of $got {m0 m1 m2 m3}

            for {set i 0} {$i < 200} {incr i} {
                set m [r srandmember myset]
                assert {$m ne {}}
                assert_none_of [list $m] {m0 m1 m2 m3}
            }
            assert_equal {PONG} [r ping]
            # SRANDMEMBER is read-only: it removes nothing, so the
            # expired members are all still counted.
            assert_equal 10 [r scard myset]
            assert_equal 6 [llength [r smembers myset]]

            # 10 live members among 90 expired ones. Rejection sampling would
            # need ~10 draws per hit here, so this is what makes the exact
            # counts above hold at all.
            create_numbered_set myset 100
            set expired {}
            set live {}
            for {set i 0} {$i < 100} {incr i} {
                if {$i < 10} { lappend live m$i } else { lappend expired m$i }
            }
            make_members_expired r myset $expired
            for {set i 0} {$i < 100} {incr i} {
                set got [r srandmember myset 1]
                assert_equal 1 [llength $got]
                assert_none_of $got $expired
            }
            set got [r srandmember myset -20]
            assert_equal 20 [llength $got]
            assert_none_of $got $expired
            set got [r srandmember myset 1000]
            assert_equal [lsort $live] [lsort $got]
            set got [r srandmember myset 7]
            assert_equal 7 [llength [lsort -unique $got]]
            assert_none_of $got $expired
            assert_equal {PONG} [r ping]
            assert_equal 100 [r scard myset]
            r debug set-active-expire 1
        } {OK} {needs:debug}

        test "SRANDMEMBER finds the one live member of a mostly-expired set - $encoding" {
            flush_and_disable_active_expiry
            use_set_encoding $encoding
            create_numbered_set myset 10
            make_members_expired r myset {m0 m1 m2 m3 m4 m5 m6 m7 m8}
            # Only m9 is live. A bounded reject loop over a 10-member set will
            # usually miss it, so this is the fallback path.
            for {set i 0} {$i < 200} {incr i} {
                assert_equal m9 [r srandmember myset]
            }
            assert_equal {PONG} [r ping]
            r debug set-active-expire 1
        } {OK} {needs:debug}

        test "SRANDMEMBER on a set of only expired members returns nothing - $encoding" {
            flush_and_disable_active_expiry
            use_set_encoding $encoding
            r sadd myset a b c
            make_members_expired r myset {a b c}
            assert_equal {} [r srandmember myset -20]
            assert_equal {} [r srandmember myset 20]
            assert_equal {} [r srandmember myset]
            assert_equal {PONG} [r ping]
            r debug set-active-expire 1
        } {OK} {needs:debug}

        test "SCARD counts expired members while SMEMBERS and SSCAN do not - $encoding" {
            flush_and_disable_active_expiry
            use_set_encoding $encoding
            r sadd myset a b c d
            make_members_expired r myset {a b}
            # SCARD is O(1) and reports the physical member count, so the two
            # lengths legitimately differ until active expiry runs.
            assert_equal 4 [r scard myset]
            assert_equal {c d} [lsort [r smembers myset]]

            # An SSCAN cursor may report a member more than once and gives no
            # order guarantee, so only the set of members it reports is
            # contractual: it must never include an expired one.
            set cursor 0
            set found {}
            while 1 {
                set res [r sscan myset $cursor]
                set cursor [lindex $res 0]
                foreach m [lindex $res 1] { lappend found $m }
                if {$cursor == 0} break
            }
            assert_equal {c d} [lsort -unique $found]
            r debug set-active-expire 1
        } {OK} {needs:debug}

        test "SREM of an expired member returns 0 - $encoding" {
            flush_and_disable_active_expiry
            use_set_encoding $encoding
            r sadd myset a b c
            make_members_expired r myset {b}
            assert_equal 0 [r srem myset b]
            assert_equal 0 [r srem myset b nosuchmember]
            # A batch counts only the live members it removed.
            assert_equal 1 [r srem myset a b nosuchmember]
            assert_equal 1 [r exists myset]
            assert_equal {c} [r smembers myset]
            r debug set-active-expire 1
        } {OK} {needs:debug}

        test "SREM of the last live member leaves the key to active expiry - $encoding" {
            flush_and_disable_active_expiry
            use_set_encoding $encoding
            r sadd myset a b c
            make_members_expired r myset {b c}
            assert_equal 1 [r srem myset a]
            # Only active expiry may delete a key that still holds members.
            assert_equal 1 [r exists myset]
            assert_equal 2 [r scard myset]
            assert_equal {} [r smembers myset]

            r debug set-active-expire 1
            wait_for_condition 100 100 {
                [r exists myset] == 0
            } else {
                fail "active expiry did not delete the set of expired members"
            }
            assert_equal 0 [get_keys_with_volatile_items r]
        } {} {needs:debug}

        test "SMISMEMBER over a mix of live, expired and missing members - $encoding" {
            flush_and_disable_active_expiry
            use_set_encoding $encoding
            r sadd myset live1 gone1 live2 gone2
            make_members_expired r myset {gone1 gone2}
            assert_equal {1 0 1 0 0} [r smismember myset live1 gone1 live2 gone2 missing]
            assert_equal {0 0} [r smismember myset gone1 missing]
            assert_equal {1} [r smismember myset live1]
            assert_equal 1 [r sismember myset live1]
            assert_equal 0 [r sismember myset gone1]
            assert_equal 0 [r sismember myset missing]
            # An expired member and a missing one are indistinguishable to a
            # reader.
            assert_equal {-2 -2} [r sttl myset members 2 gone1 missing]
            r debug set-active-expire 1
        } {OK} {needs:debug}

        test "SINTERSTORE with the destination equal to a volatile source - $encoding" {
            flush_and_disable_active_expiry
            use_set_encoding $encoding
            r sadd k{t} a b c
            make_members_expired r k{t} {c}
            assert_equal {1} [r sexpire k{t} $::live_member_ttl members 1 a]
            # A single source copies the live members only, and the result is
            # a plain set even though it overwrote a volatile one.
            assert_equal 2 [r sinterstore k{t} k{t}]
            assert_equal {a b} [lsort [r smembers k{t}]]
            assert_equal 2 [r scard k{t}]
            assert_equal {-1 -1} [r sttl k{t} members 2 a b]
            assert_equal {-2} [r sttl k{t} members 1 c]
            assert_equal {PONG} [r ping]
            assert_equal 0 [get_keys_with_volatile_items r]
            r debug set-active-expire 1
        } {OK} {needs:debug}

        test "SMOVE onto a destination that holds the member as expired - $encoding" {
            flush_and_disable_active_expiry
            use_set_encoding $encoding
            r sadd src{t} a
            r sadd dst{t} a b
            make_members_expired r dst{t} {a}
            assert_equal {1} [r sexpire src{t} $::live_member_ttl members 1 a]
            assert_equal 0 [r sismember dst{t} a]
            assert_equal {-2} [r sttl dst{t} members 1 a]
            assert_equal 1 [r smove src{t} dst{t} a]
            assert_equal 1 [r sismember dst{t} a]
            assert_range [set_member_ttl r dst{t} a] 1 $::live_member_ttl
            assert_equal {a b} [lsort [r smembers dst{t}]]
            # The expired member was replaced, not duplicated.
            assert_equal 2 [r scard dst{t}]
            assert_equal 0 [r exists src{t}]
            r debug set-active-expire 1
        } {OK} {needs:debug}
    }
}

# SINTER, SUNION, SDIFF, SINTERCARD and the *STORE forms: an expired member is
# never an input, and a destination written by a *STORE is always plain.
start_server {
    tags {"setexpire"}
    overrides {
        "set-max-intset-entries" 512
        "set-max-listpack-entries" 128
        "set-max-listpack-value" 64
    }
} {
    test "SINTER SUNION SDIFF never return an expired member" {
        flush_and_disable_active_expiry
        r sadd s1{t} a b c d
        r sadd s2{t} a b c e
        make_members_expired r s1{t} {b}
        make_members_expired r s2{t} {c}
        # Live s1 = {a c d}, live s2 = {a b e}.
        assert_equal {a} [lsort [r sinter s1{t} s2{t}]]
        assert_equal {a b c d e} [lsort [r sunion s1{t} s2{t}]]
        assert_equal {c d} [lsort [r sdiff s1{t} s2{t}]]
        assert_equal {b e} [lsort [r sdiff s2{t} s1{t}]]
        # A read must not remove anything.
        assert_equal 4 [r scard s1{t}]
        assert_equal 4 [r scard s2{t}]
        r debug set-active-expire 1
    } {OK} {needs:debug}

    test "SINTERCARD ignores expired members" {
        flush_and_disable_active_expiry
        r sadd s1{t} a b c d
        r sadd s2{t} a b c
        make_members_expired r s1{t} {b}
        make_members_expired r s2{t} {c}
        # Live s1 = {a c d}, live s2 = {a b}.
        assert_equal 1 [r sintercard 2 s1{t} s2{t}]
        assert_equal 1 [r sintercard 2 s1{t} s2{t} limit 0]
        assert_equal 1 [r sintercard 2 s1{t} s2{t} limit 5]
        assert_equal 1 [r sintercard 2 s1{t} s2{t} limit 1]
        # A single-key SINTERCARD is the live cardinality, not SCARD.
        assert_equal 3 [r sintercard 1 s1{t}]
        assert_equal 2 [r sintercard 1 s2{t}]
        assert_equal 4 [r scard s1{t}]
        r debug set-active-expire 1
    } {OK} {needs:debug}

    test "SUNIONSTORE writes a plain set when a source is volatile" {
        flush_and_disable_active_expiry
        r sadd s1{t} a b c
        r sadd s2{t} d e
        make_members_expired r s1{t} {c}
        assert_equal {1} [r sexpire s1{t} $::live_member_ttl members 1 a]
        assert_equal 4 [r sunionstore dst{t} s1{t} s2{t}]
        assert_equal {a b d e} [lsort [r smembers dst{t}]]
        # The destination carries no member TTL at all.
        assert_equal {-1 -1 -1 -1} [r sttl dst{t} members 4 a b d e]
        # The expired member was not an input, so it is not in the destination.
        assert_equal {-2} [r sttl dst{t} members 1 c]
        assert_equal 0 [r sismember dst{t} c]
        # The volatile source is untouched.
        assert_range [set_member_ttl r s1{t} a] 1 $::live_member_ttl
        r debug set-active-expire 1
    } {OK} {needs:debug}

    test "SUNIONSTORE with the destination equal to a volatile source" {
        flush_and_disable_active_expiry
        r sadd k{t} a b c
        r sadd other{t} d
        make_members_expired r k{t} {b}
        assert_equal {1} [r sexpire k{t} $::live_member_ttl members 1 a]
        assert_equal 3 [r sunionstore k{t} k{t} other{t}]
        assert_equal {a c d} [lsort [r smembers k{t}]]
        assert_equal 3 [r scard k{t}]
        assert_equal {-1 -1 -1} [r sttl k{t} members 3 a c d]
        assert_equal {-2} [r sttl k{t} members 1 b]
        assert_equal 0 [get_keys_with_volatile_items r]
        assert_equal {PONG} [r ping]
        r debug set-active-expire 1
    } {OK} {needs:debug}

    test "SINTERSTORE dst src with the destination also a volatile source" {
        flush_and_disable_active_expiry
        r sadd k{t} a b c
        r sadd other{t} a b
        make_members_expired r k{t} {b}
        assert_equal {1} [r sexpire k{t} $::live_member_ttl members 1 a]
        # Live k = {a c}, other = {a b}, so the intersection is {a}.
        assert_equal 1 [r sinterstore k{t} k{t} other{t}]
        assert_equal {a} [r smembers k{t}]
        assert_equal 1 [r scard k{t}]
        assert_equal {-1} [r sttl k{t} members 1 a]
        assert_equal {PONG} [r ping]
        r debug set-active-expire 1
    } {OK} {needs:debug}

    test "SDIFFSTORE with volatile inputs writes a plain set" {
        flush_and_disable_active_expiry
        r sadd s1{t} a b c d
        r sadd s2{t} c
        make_members_expired r s1{t} {b}
        make_members_expired r s2{t} {c}
        assert_equal {1} [r sexpire s1{t} $::live_member_ttl members 1 a]
        # s2 has no live member left, so c stays in the difference even though
        # s2 still holds it as an expired member.
        assert_equal 3 [r sdiffstore dst{t} s1{t} s2{t}]
        assert_equal {a c d} [lsort [r smembers dst{t}]]
        assert_equal {-1 -1 -1} [r sttl dst{t} members 3 a c d]
        assert_equal {-2} [r sttl dst{t} members 1 b]
        r debug set-active-expire 1
    } {OK} {needs:debug}

    test "SDIFFSTORE with the destination equal to a volatile source" {
        flush_and_disable_active_expiry
        r sadd k{t} a b c
        r sadd other{t} c
        make_members_expired r k{t} {b}
        assert_equal {1} [r sexpire k{t} $::live_member_ttl members 1 a]
        # Live k = {a c}, minus other = {c}, leaves {a} with no TTL.
        assert_equal 1 [r sdiffstore k{t} k{t} other{t}]
        assert_equal {a} [r smembers k{t}]
        assert_equal 1 [r scard k{t}]
        assert_equal {-1} [r sttl k{t} members 1 a]
        assert_equal {PONG} [r ping]
        r debug set-active-expire 1
    } {OK} {needs:debug}

    test "A *STORE whose only source has no live member deletes the destination" {
        flush_and_disable_active_expiry
        r sadd s1{t} a b
        r sadd s2{t} a b
        make_members_expired r s1{t} {a b}

        r sadd dst{t} keepme
        assert_equal 0 [r sinterstore dst{t} s1{t} s2{t}]
        assert_equal 0 [r exists dst{t}]

        r sadd dst{t} keepme
        assert_equal 0 [r sdiffstore dst{t} s1{t} s2{t}]
        assert_equal 0 [r exists dst{t}]

        # SUNIONSTORE still has s2's live members.
        r sadd dst{t} keepme
        assert_equal 2 [r sunionstore dst{t} s1{t} s2{t}]
        assert_equal {a b} [lsort [r smembers dst{t}]]
        assert_equal {-1 -1} [r sttl dst{t} members 2 a b]
        r debug set-active-expire 1
    } {OK} {needs:debug}

    # SMOVE: the TTL travels with the member and overwrites whatever the
    # destination had.
    test "SMOVE carries the member TTL to the destination" {
        r flushall
        r sadd src{t} a b
        r sadd dst{t} z
        assert_equal {1} [r sexpire src{t} $::live_member_ttl members 1 a]
        assert_equal 1 [r smove src{t} dst{t} a]
        assert_range [set_member_ttl r dst{t} a] 1 $::live_member_ttl
        assert_equal 1 [r sismember dst{t} a]
        # Gone from the source entirely, not left behind as an expired member.
        assert_equal {-2} [r sttl src{t} members 1 a]
        assert_equal 0 [r sismember src{t} a]
        assert_equal {b} [r smembers src{t}]
        # A plain destination member is unaffected.
        assert_equal {-1} [r sttl dst{t} members 1 z]
    }

    test "SMOVE of a plain member into a volatile destination keeps it plain" {
        r flushall
        r sadd src{t} a
        r sadd dst{t} z y
        assert_equal {1} [r sexpire dst{t} $::live_member_ttl members 1 z]
        assert_equal 1 [r smove src{t} dst{t} a]
        assert_equal {-1} [r sttl dst{t} members 1 a]
        assert_equal {-1} [r sttl dst{t} members 1 y]
        assert_range [set_member_ttl r dst{t} z] 1 $::live_member_ttl
        # The source is emptied and deleted.
        assert_equal 0 [r exists src{t}]
    }

    test "SMOVE overwrites a destination TTL with the source member's TTL" {
        r flushall
        r sadd src{t} a
        r sadd dst{t} a b
        assert_equal {1} [r sexpire src{t} $::live_member_ttl members 1 a]
        assert_equal {1} [r sexpire dst{t} 100000 members 1 a]
        assert_morethan [set_member_ttl r dst{t} a] $::live_member_ttl
        # The write is unconditional, so the much longer destination TTL is
        # replaced by the shorter source TTL.
        assert_equal 1 [r smove src{t} dst{t} a]
        assert_range [set_member_ttl r dst{t} a] 1 $::live_member_ttl
        assert_equal {a b} [lsort [r smembers dst{t}]]
        assert_equal 2 [r scard dst{t}]
        assert_equal 0 [r exists src{t}]
    }

    test "SMOVE of a plain member clears an existing destination TTL" {
        r flushall
        r sadd src{t} a
        r sadd dst{t} a
        assert_equal {1} [r sexpire dst{t} 100000 members 1 a]
        # The source member's TTL is written unconditionally, and it has none.
        assert_equal 1 [r smove src{t} dst{t} a]
        assert_equal {-1} [r sttl dst{t} members 1 a]
        assert_equal 1 [r scard dst{t}]
        assert_equal 0 [get_keys_with_volatile_items r]
    }

    test "SMOVE of an expired member returns 0 and leaves the destination alone" {
        flush_and_disable_active_expiry
        r sadd src{t} a b
        r sadd dst{t} z
        make_members_expired r src{t} {a}
        assert_equal 0 [r smove src{t} dst{t} a]
        assert_equal 0 [r sismember dst{t} a]
        assert_equal {z} [r smembers dst{t}]
        assert_equal {-2} [r sttl dst{t} members 1 a]
        assert_equal 2 [r scard src{t}]
        # A missing source member and a missing source key behave the same.
        assert_equal 0 [r smove src{t} dst{t} nosuchmember]
        assert_equal 0 [r smove nosuchsrc{t} dst{t} a]
        assert_equal {z} [r smembers dst{t}]
        r debug set-active-expire 1
    } {OK} {needs:debug}

    test "SMOVE of the last live member leaves the source to active expiry" {
        flush_and_disable_active_expiry
        r sadd src{t} a b
        r sadd dst{t} z
        make_members_expired r src{t} {b}
        assert_equal 1 [r smove src{t} dst{t} a]
        # Only active expiry may delete the key, so it survives holding nothing
        # but the expired member.
        assert_equal 1 [r exists src{t}]
        assert_equal 1 [r scard src{t}]
        assert_equal {} [r smembers src{t}]
        assert_equal {a z} [lsort [r smembers dst{t}]]
        r debug set-active-expire 1
    } {OK} {needs:debug}
}

# A command that mutates nothing must look like a command that mutates
# nothing: no keyspace notification, no WATCH invalidation and no change to
# the counter that drives persistence. The plain-set behaviour is measured in
# the same test and is the contract the volatile set has to match.
start_server {
    tags {"setexpire"}
    overrides {
        "set-max-intset-entries" 512
        "set-max-listpack-entries" 128
        "set-max-listpack-value" 64
    }
} {
    test "SPOP without a count that pops nothing raises no mutation signal" {
        flush_and_disable_active_expiry
        # Baseline: a missing key, where SPOP cannot mutate anything.
        r del myset
        assert_equal {{} 0 0} [capture_mutation_signals myset {
            assert_equal {} [r spop myset]
        }]

        # A set whose members are all expired: SPOP returns nothing and
        # nothing about the key has changed.
        r sadd myset a b c
        make_members_expired r myset {a b c}
        assert_equal {{} 0 0} [capture_mutation_signals myset {
            assert_equal {} [r spop myset]
            assert_equal {} [r spop myset]
        }]
        assert_equal 1 [r exists myset]
        assert_equal 3 [r scard myset]
        r debug set-active-expire 1
    } {OK} {needs:debug}

    test "SPOP with a count that pops nothing raises no mutation signal" {
        flush_and_disable_active_expiry
        r del myset
        assert_equal {{} 0 0} [capture_mutation_signals myset {
            assert_equal {} [r spop myset 3]
        }]

        # count < SCARD over a set of nothing but expired members: the reply is
        # empty and the key is untouched, so the command is a no-op like the
        # missing-key case above.
        r sadd myset a b c
        make_members_expired r myset {a b c}
        assert_equal {{} 0 0} [capture_mutation_signals myset {
            assert_equal {} [r spop myset 2]
        }]
        assert_equal 1 [r exists myset]
        assert_equal 3 [r scard myset]
        r debug set-active-expire 1
    } {OK} {needs:debug}

    test "SPOP that pops a live member raises every mutation signal" {
        flush_and_disable_active_expiry
        r sadd myset a b c
        make_members_expired r myset {a b c}
        r sadd myset live1 live2

        set signals [capture_mutation_signals myset {
            assert_match "live*" [r spop myset]
        }]
        assert_equal {spop myset} [lindex $signals 0 0]
        assert_morethan [lindex $signals 1] 0
        assert_equal 1 [lindex $signals 2]

        set signals [capture_mutation_signals myset {
            assert_match "live*" [r spop myset 1]
        }]
        assert_equal {spop myset} [lindex $signals 0 0]
        assert_morethan [lindex $signals 1] 0
        assert_equal 1 [lindex $signals 2]
        r debug set-active-expire 1
    } {OK} {needs:debug}

    foreach command {sinterstore sunionstore sdiffstore} {
        test "$command that writes nothing over a missing destination raises no mutation signal" {
            flush_and_disable_active_expiry
            # Baseline: an empty result from plain sources with no destination.
            r del dst{t}
            assert_equal {{} 0 0} [capture_mutation_signals dst{t} {
                assert_equal 0 [r $command dst{t} nosuch1{t} nosuch2{t}]
            }]
            assert_equal 0 [r exists dst{t}]

            # The same empty result, this time because every source member is
            # expired rather than absent.
            r sadd s1{t} a b
            make_members_expired r s1{t} {a b}
            r del dst{t}
            assert_equal {{} 0 0} [capture_mutation_signals dst{t} {
                assert_equal 0 [r $command dst{t} s1{t} s1{t}]
            }]
            assert_equal 0 [r exists dst{t}]

            # The mutating counterpart: the same empty result, but there is a
            # destination to delete.
            r sadd dst{t} keepme
            set signals [capture_mutation_signals dst{t} {
                assert_equal 0 [r $command dst{t} s1{t} s1{t}]
            }]
            assert_equal {del dst{t}} [lindex $signals 0 0]
            assert_morethan [lindex $signals 1] 0
            assert_equal 1 [lindex $signals 2]
            assert_equal 0 [r exists dst{t}]
            r debug set-active-expire 1
        } {OK} {needs:debug}
    }

    test "SORT STORE that writes nothing over a missing destination raises no mutation signal" {
        flush_and_disable_active_expiry
        r del dst{t}
        assert_equal {{} 0 0} [capture_mutation_signals dst{t} {
            assert_equal 0 [r sort nosuch{t} STORE dst{t}]
        }]
        assert_equal 0 [r exists dst{t}]

        r sadd src{t} 1 2 3
        make_members_expired r src{t} {1 2 3}
        r del dst{t}
        assert_equal {{} 0 0} [capture_mutation_signals dst{t} {
            assert_equal 0 [r sort src{t} STORE dst{t}]
        }]
        assert_equal 0 [r exists dst{t}]

        r rpush dst{t} stale-content
        set signals [capture_mutation_signals dst{t} {
            assert_equal 0 [r sort src{t} STORE dst{t}]
        }]
        assert_equal {del dst{t}} [lindex $signals 0 0]
        assert_morethan [lindex $signals 1] 0
        assert_equal 1 [lindex $signals 2]
        assert_equal 0 [r exists dst{t}]
        r debug set-active-expire 1
    } {OK} {needs:debug}

    r config set notify-keyspace-events ""
}

# Removing the last volatile member flips the object back to the non-volatile
# state, so a command that then deletes the key no longer recognizes it as
# tracked and would leave a dangling pointer in the member-expiry tracking
# table for the next active expire cycle to dereference.
start_server {
    tags {"setexpire"}
    overrides {
        "set-max-intset-entries" 512
        "set-max-listpack-value" 64
    }
} {
    # The key left both the keyspace and the tracking table, and stays gone
    # across the active expire cycles that follow.
    proc assert_key_gone_and_untracked {key} {
        wait_for_condition 50 20 {
            [r ping] eq {PONG} &&
            [r exists $key] == 0 &&
            [r dbsize] == 0 &&
            [get_keys_with_volatile_items r] == 0
        } else {
            fail "$key is still in the keyspace or the tracking table"
        }
    }

    foreach encoding {listpack hashtable} {
        # Every command that empties a set of volatile members must delete the
        # key and untrack it, whether it removes the members or expires them.
        foreach {what members empty} {
            SREM                        {a}   {assert_equal 1 [r srem myset a]}
            SPOP                        {a}   {assert_equal a [r spop myset]}
            {SREM of two members}       {a b} {assert_equal 2 [r srem myset a b]}
            {SPOP with a count}         {a b} {assert_equal {a b} [lsort [r spop myset 2]]}
            {an expiration in the past} {a}   {assert_equal {2} [r spexpireat myset 1 MEMBERS 1 a]}
        } {
            test "$what empties a volatile set, deleting and untracking the key - $encoding" {
                r flushall
                r debug set-active-expire 1
                use_set_encoding $encoding
                assert_equal [llength $members] \
                    [r saddex myset PX 60000 MEMBERS [llength $members] {*}$members]
                assert_encoding $encoding myset
                assert_equal 1 [get_keys_with_volatile_items r]
                eval $empty
                assert_key_gone_and_untracked myset
            } {} {needs:debug}
        }

        test "SPOP below the size pops every live member and leaves the key to active expiry - $encoding" {
            r flushall
            r debug set-active-expire 0
            use_set_encoding $encoding
            assert_equal 3 [r saddex myset PX 60000 MEMBERS 3 live1 live2 gone]
            assert_encoding $encoding myset
            make_members_expired r myset {gone}
            assert_equal {live1 live2} [lsort [r spop myset 2]]
            assert_equal 1 [r exists myset]
            assert_equal {} [r smembers myset]
            r debug set-active-expire 1
            assert_key_gone_and_untracked myset
        } {} {needs:debug}

        test "SMOVE of the only volatile member deletes the source and untracks it - $encoding" {
            r flushall
            r debug set-active-expire 1
            use_set_encoding $encoding
            assert_equal 1 [r saddex src{t} PX 60000 MEMBERS 1 a]
            assert_encoding $encoding src{t}
            assert_equal 1 [get_keys_with_volatile_items r]
            assert_equal 1 [r smove src{t} dst{t} a]
            # Only the destination is left, and only it is tracked.
            wait_for_condition 50 20 {
                [r ping] eq {PONG} &&
                [r exists src{t}] == 0 &&
                [r dbsize] == 1 &&
                [get_keys_with_volatile_items r] == 1
            } else {
                fail "src{t} is still in the keyspace or the tracking table"
            }
            assert_equal 1 [r exists dst{t}]
            r del dst{t}
            assert_equal 0 [get_keys_with_volatile_items r]
        } {} {needs:debug}
    }
}

# SORT reads a set through a vector of pointers it builds itself. An expired
# member must be filtered out of that vector, and building it must tolerate
# SCARD being larger than the number of members SORT actually collects.
start_server {
    tags {"setexpire"}
    overrides {
        "set-max-intset-entries" 512
        "set-max-listpack-entries" 128
        "set-max-listpack-value" 64
    }
} {
    foreach encoding {listpack hashtable} {
        test "SORT and SORT_RO over a $encoding set skip an expired member" {
            flush_and_disable_active_expiry
            use_set_encoding $encoding
            r sadd myset 1 2 3 4 5
            make_members_expired r myset {3}
            assert_encoding $encoding myset
            # The expired member is still counted, so SORT collects fewer
            # members than SCARD announces.
            assert_equal 5 [r scard myset]

            assert_equal {1 2 4 5} [r sort myset]
            assert_equal {PONG} [r ping]
            assert_equal {1 2 4 5} [r sort_ro myset]
            assert_equal {5 4 2 1} [r sort myset DESC]
            assert_equal {1 2 4 5} [r sort myset ALPHA]
            # BY nosort over a set has no defined order unless the result is
            # stored, so only the membership is contractual here.
            assert_equal {1 2 4 5} [lsort [r sort myset BY nosort]]
            assert_equal {1 2} [r sort myset LIMIT 0 2]
            assert_equal {PONG} [r ping]

            # A read does not remove the expired member.
            assert_equal 5 [r scard myset]
            assert_equal {-2} [r sttl myset members 1 3]
            r debug set-active-expire 1
        } {OK} {needs:debug}

        test "SORT over a $encoding set of only expired members returns an empty list" {
            flush_and_disable_active_expiry
            use_set_encoding $encoding
            r sadd myset 1 2 3
            make_members_expired r myset {1 2 3}
            assert_encoding $encoding myset
            assert_equal {} [r sort myset]
            assert_equal {} [r sort_ro myset]
            assert_equal {} [r sort myset ALPHA]
            assert_equal {PONG} [r ping]
            assert_equal 3 [r scard myset]
            r debug set-active-expire 1
        } {OK} {needs:debug}

        test "SORT STORE over a $encoding set writes only the live members" {
            flush_and_disable_active_expiry
            use_set_encoding $encoding
            r sadd src{t} 1 2 3 4 5
            make_members_expired r src{t} {3}
            assert_encoding $encoding src{t}
            r rpush dst{t} stale-content

            assert_equal 4 [r sort src{t} STORE dst{t}]
            assert_equal {PONG} [r ping]
            # The destination is a plain list holding the live members in
            # sorted order, and its previous content is gone.
            assert_equal list [r type dst{t}]
            assert_equal {1 2 4 5} [r lrange dst{t} 0 -1]
            # The volatile source is untouched by the store.
            assert_equal 5 [r scard src{t}]
            assert_equal {-2} [r sttl src{t} members 1 3]
            r debug set-active-expire 1
        } {OK} {needs:debug}
    }
}

# Active defrag over a volatile hashtable set.
#
# Volatile members are indexed by pointer, so moving a member has to re-point
# that index as well as the hashtable bucket. Structure, guards and thresholds
# follow the active-defrag tests in tests/unit/memefficiency.tcl: run_solo,
# the defrag tag, and the jemalloc plus page-size probe, because defrag can
# only move an allocation when the allocator agrees to.
run_solo {defrag} {
    # Members are "m<i>:<padding>". The padding is what makes the set big
    # enough for the fragmentation ratios to be meaningful.
    proc defrag_member {i padding} {
        return "m$i:$padding"
    }

    # Run $prefix (a whole command up to but excluding its member list) over
    # the members with index $from..$to-1 stepping by $step, in batches, so
    # that 100k members cost ~200 commands rather than 100k round trips.
    # 'countarg' is 1 for the commands that take a "MEMBERS <nummembers>"
    # clause.
    proc defrag_batched {prefix padding from to step countarg} {
        set batch {}
        for {set i $from} {$i < $to} {incr i $step} {
            lappend batch [defrag_member $i $padding]
            if {[llength $batch] == 500} {
                defrag_run $prefix $batch $countarg
                set batch {}
            }
        }
        if {[llength $batch] > 0} {
            defrag_run $prefix $batch $countarg
        }
    }

    proc defrag_run {prefix batch countarg} {
        if {$countarg} {
            r {*}$prefix MEMBERS [llength $batch] {*}$batch
        } else {
            r {*}$prefix {*}$batch
        }
    }

    # Every member in $from..$to-1 stepping by $step must still report a
    # positive TTL. Returns the number of members checked.
    proc defrag_assert_all_ttls {key padding from to step} {
        set checked 0
        set batch {}
        for {set i $from} {$i < $to} {incr i $step} {
            lappend batch [defrag_member $i $padding]
            if {[llength $batch] == 500} {
                foreach ttl [r sttl $key MEMBERS [llength $batch] {*}$batch] {
                    assert_morethan $ttl 0
                    incr checked
                }
                set batch {}
            }
        }
        if {[llength $batch] > 0} {
            foreach ttl [r sttl $key MEMBERS [llength $batch] {*}$batch] {
                assert_morethan $ttl 0
                incr checked
            }
        }
        return $checked
    }

    set defrag_tags [list defrag external:skip standalone setexpire]
    # set-max-listpack-entries 0 forces the hashtable encoding, the only one
    # with a pointer index. lazyfree-lazy-user-del no keeps the SREM frees
    # synchronous so that the fragmentation is there when we measure it.
    set defrag_overrides [list \
        appendonly no \
        save {} \
        lazyfree-lazy-user-del no \
        activedefrag no \
        set-max-intset-entries 512 \
        set-max-listpack-entries 0]

    set have_defrag 0
    start_server [list tags $defrag_tags overrides $defrag_overrides] {
        if {[string match {*jemalloc*} [s mem_allocator]] && [r debug mallctl arenas.page] <= 8192} {
            set have_defrag 1
        }
    }

    if {!$have_defrag} {
        puts "Jemalloc not available. Set member TTL defrag test skipped."
    } else {
        start_server [list tags $defrag_tags overrides $defrag_overrides] {
            test "hashtable: active defrag moves the members of a volatile set and keeps their TTLs" {
                r flushall
                r debug set-active-expire 1
                r config set active-defrag-threshold-lower 5
                r config set active-defrag-cycle-min 40
                r config set active-defrag-cycle-max 60
                r config set active-defrag-ignore-bytes 2500kb
                r config set maxmemory 0

                set n 100000
                # Long enough to stay live for the whole test.
                set long_ttl 3600
                set padding [string repeat A 480]

                # Populate: one hashtable set, every member volatile.
                defrag_batched [list sadd myset] $padding 0 $n 1 0
                assert_encoding hashtable myset
                assert_equal $n [r scard myset]
                defrag_batched [list sexpire myset $long_ttl] $padding 0 $n 1 1

                # Fragment: drop every other member. The survivors are the even
                # indexes, so their allocations are interleaved with holes.
                defrag_batched [list srem myset] $padding 1 $n 2 0
                assert_equal [expr {$n / 2}] [r scard myset]
                after 120 ;# serverCron refreshes the memory info every 100ms
                assert_morethan [s allocator_frag_ratio] 1.4

                set digest [debug_digest]

                # Defrag until it stops on its own.
                set old_defrag_time [s total_active_defrag_time]
                r config set activedefrag yes
                wait_for_condition 100 50 {
                    [s total_active_defrag_time] ne $old_defrag_time
                } else {
                    fail "defrag not started"
                }
                wait_for_condition 300 200 {
                    [s active_defrag_running] eq 0
                } else {
                    fail "defrag didn't stop"
                }
                r config set activedefrag no
                after 120

                # It actually moved things, and the fragmentation is gone.
                assert_morethan [s active_defrag_hits] 0
                assert_lessthan [s allocator_frag_ratio] 1.1
                assert_equal $digest [debug_digest]

                # Every surviving member is still there, still volatile, and
                # still reachable through the moved index.
                assert_equal [expr {$n / 2}] [r scard myset]
                assert_equal [expr {$n / 2}] [defrag_assert_all_ttls myset $padding 0 $n 2]

                # And active expiry still walks that index: a 1 second TTL on one
                # member must make it disappear without any read touching it.
                # The wait is generous because the active expire cycle is
                # CPU-budgeted and this test runs alongside the other solo
                # defrag tests.
                set victim [defrag_member 0 $padding]
                assert_equal 1 [r sismember myset $victim]
                assert_equal {1} [r spexpire myset 1000 MEMBERS 1 $victim]
                wait_for_condition 300 100 {
                    [r scard myset] == [expr {$n / 2 - 1}]
                } else {
                    fail "active expiry did not remove the expired member after defrag"
                }
                assert_equal 0 [r sismember myset $victim]
                assert_equal {-2} [r sttl myset MEMBERS 1 $victim]

                r save ;# iterate over every pointer once more
            }

            # A plain hashtable set has no pointer index and no metadata tail,
            # so the first SEXPIRE on it reallocates the hashtable struct.
            # Active defrag is the other writer of that pointer: it moves the
            # struct itself and holds a cursor into the very buckets being
            # scanned. This test drives both at once - the SEXPIRE lands while
            # active_defrag_running is non-zero - with
            # active-defrag-max-scan-fields forced below the set size so the
            # set takes the incremental path and the scan is still in flight.
            test "hashtable: SEXPIRE flips a plain set to volatile while active defrag is running" {
                r flushall
                r debug set-active-expire 1
                r config set active-defrag-threshold-lower 5
                r config set active-defrag-cycle-min 40
                r config set active-defrag-cycle-max 60
                r config set active-defrag-ignore-bytes 2500kb
                r config set maxmemory 0
                # 100 << 20000, so myset is defragged incrementally rather than
                # in one pass inside a single cron call.
                r config set active-defrag-max-scan-fields 100

                set n 40000    ;# 20000 survive the fragmentation step
                set survivors [expr {$n / 2}]
                set padding [string repeat A 980]
                set long_ttl 100000

                # The set: plain members, no TTL anywhere, so the object has no
                # pointer index and no metadata tail yet.
                defrag_batched [list sadd myset] $padding 0 $n 1 0
                assert_encoding hashtable myset
                assert_equal $n [r scard myset]
                assert_equal 0 [get_keys_with_volatile_items r]

                # Fragment: drop every odd-indexed member, so the survivors sit
                # interleaved with holes in the same jemalloc runs.
                defrag_batched [list srem myset] $padding 1 $n 2 0
                assert_equal $survivors [r scard myset]
                after 120 ;# serverCron refreshes the memory info every 100ms
                assert_morethan [s allocator_frag_ratio] 1.4

                # Start defrag slowly enough that the SEXPIRE below is
                # guaranteed to run while a scan is in flight.
                r config set active-defrag-cycle-min 2
                r config set active-defrag-cycle-max 3
                set old_defrag_time [s total_active_defrag_time]
                r config set activedefrag yes
                wait_for_condition 100 50 {
                    [s total_active_defrag_time] ne $old_defrag_time
                } else {
                    fail "defrag not started"
                }
                # active_defrag_running is the CPU percentage of the cycle, so
                # a non-zero value means a defrag cycle is in flight.
                assert_range [s active_defrag_running] 2 3

                # The flip. Every one of these 1000 members is in the set with
                # no TTL, so this is the call that grows the hashtable struct
                # and builds the index, concurrently with the defrag scan.
                set victims {}
                for {set i 0} {$i < 2000} {incr i 2} {
                    lappend victims [defrag_member $i $padding]
                }
                assert_equal 1000 [llength $victims]
                assert_morethan [s active_defrag_running] 0
                foreach reply [r sexpire myset $long_ttl MEMBERS [llength $victims] {*}$victims] {
                    assert_equal 1 $reply
                }
                assert_equal {PONG} [r ping]
                assert_equal 1 [get_keys_with_volatile_items r]
                # Snapshot the value AFTER the flip: defrag must not change it.
                set digest [debug_digest]

                # Let defrag finish the now-volatile set.
                r config set active-defrag-cycle-min 40
                r config set active-defrag-cycle-max 60
                wait_for_condition 300 200 {
                    [s active_defrag_running] eq 0
                } else {
                    fail "defrag didn't stop"
                }
                r config set activedefrag no
                after 120

                # The server is alive, it moved allocations, and nothing was
                # lost or duplicated.
                assert_equal {PONG} [r ping]
                assert_morethan [s active_defrag_hits] 0
                assert_equal $survivors [r scard myset]
                # The 1000 TTLs set during the defrag are all intact.
                assert_equal 1000 [defrag_assert_all_ttls myset $padding 0 2000 2]
                # And every other survivor is still present and still plain.
                set missing 0
                set batch {}
                for {set i 2000} {$i < $n} {incr i 2} {
                    lappend batch [defrag_member $i $padding]
                    if {[llength $batch] == 500} {
                        foreach present [r smismember myset {*}$batch] {
                            if {!$present} {incr missing}
                        }
                        foreach ttl [r sttl myset MEMBERS [llength $batch] {*}$batch] {
                            assert_equal -1 $ttl
                        }
                        set batch {}
                    }
                }
                assert_equal 0 $missing
                assert_equal $digest [debug_digest]

                r config set active-defrag-max-scan-fields 1000
                r save ;# iterate over every pointer once more
            }
        }
    }
} ;# run_solo
