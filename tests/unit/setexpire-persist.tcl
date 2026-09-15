# Set member expiration: propagation, replication, persistence and lifecycle.

# Re-enable active expiry defensively: a test that switches it off and then
# fails an assertion never reaches its own restore line, so every test that
# needs active expiration running asserts that for itself at the top. Wrapped in catch
# so tests that are not tagged needs:debug stay runnable without DEBUG.
proc enable_active_expiry {r} {
    catch {$r DEBUG SET-ACTIVE-EXPIRE 1}
}

# Propagation rewrites, asserted on the replication stream.
#
# An unreclaimed expired member is invisible on the primary but is a plain member
# for a replica and for AOF loading, which both apply under
# POLICY_IGNORE_EXPIRE and never reclaim. So a command whose effect depends on
# which members are live cannot be propagated verbatim: the replica would
# re-evaluate it over a different input. Every rewrite below therefore names
# only what the primary really changed.

start_server {tags {"setexpire external:skip"}} {
    test {SEXPIRE is propagated as SPEXPIREAT with an absolute millisecond time} {
        r FLUSHALL
        set repl [attach_to_replication_stream]

        r SADD myset m1 m2
        r SEXPIRE myset 100 MEMBERS 1 m1

        # The absolute millisecond stamp is computed by the server, so match it
        # with a wildcard; the exactness of the value is checked against the
        # replica in group 2.
        assert_replication_stream $repl {
            {select *}
            {sadd myset m1 m2}
            {spexpireat myset * MEMBERS 1 m1}
        }
        close_replication_stream $repl
    }

    test {SPEXPIRE is propagated as SPEXPIREAT} {
        r FLUSHALL
        set repl [attach_to_replication_stream]

        r SADD myset m1
        r SPEXPIRE myset 100000 MEMBERS 1 m1

        assert_replication_stream $repl {
            {select *}
            {sadd myset m1}
            {spexpireat myset * MEMBERS 1 m1}
        }
        close_replication_stream $repl
    }

    test {SEXPIREAT is propagated as SPEXPIREAT in milliseconds, SPEXPIREAT verbatim} {
        r FLUSHALL
        set at_sec [expr {[clock seconds] + 100}]
        set at_ms [expr {[clock milliseconds] + 100000}]
        set repl [attach_to_replication_stream]

        r SADD myset m1 m2
        r SEXPIREAT myset $at_sec MEMBERS 1 m1
        r SPEXPIREAT myset $at_ms MEMBERS 1 m2

        # Milliseconds are the wire unit, so SEXPIREAT becomes SPEXPIREAT with
        # $at_sec * 1000. SPEXPIREAT is already absolute and goes through
        # verbatim.
        assert_replication_stream $repl [subst {
            {select *}
            {sadd myset m1 m2}
            {spexpireat myset [expr {$at_sec * 1000}] MEMBERS 1 m1}
            {spexpireat myset $at_ms MEMBERS 1 m2}
        }]
        close_replication_stream $repl
    }

    test {SADDEX EX and PX are propagated as PXAT} {
        r FLUSHALL
        set repl [attach_to_replication_stream]

        r SADDEX myset EX 100 MEMBERS 1 m1
        r SADDEX myset PX 100000 MEMBERS 1 m2

        assert_replication_stream $repl {
            {select *}
            {saddex myset PXAT * MEMBERS 1 m1}
            {saddex myset PXAT * MEMBERS 1 m2}
        }
        close_replication_stream $repl
    }

    test {SADDEX is not replicating validation arguments} {
        r FLUSHALL
        set at_ms [expr {[clock milliseconds] + 100000}]
        # XX / MXX only have an effect on a member that already exists.
        r SADD myset m1
        set repl [attach_to_replication_stream]

        r SADDEX myset XX MXX PXAT $at_ms MEMBERS 1 m1
        r SADDEX myset MNX PXAT $at_ms MEMBERS 1 m2

        # The XX/MNX/MXX gates are evaluated against the primary's live view,
        # which a replica does not share (an expired member is still a member
        # there), so the propagated form carries just the effective PXAT and the
        # members. A key that happens to be named like a condition is kept; see
        # the test below.
        assert_replication_stream $repl [subst {
            {select *}
            {saddex myset PXAT $at_ms MEMBERS 1 m1}
            {saddex myset PXAT $at_ms MEMBERS 1 m2}
        }]
        close_replication_stream $repl
    }

    test {SADDEX KEEPTTL is propagated as KEEPTTL} {
        r FLUSHALL
        r SADDEX myset EX 100 MEMBERS 1 m1
        set repl [attach_to_replication_stream]

        # m1 already exists with a TTL, so KEEPTTL alone would be a pure no-op
        # and a no-op is not propagated at all (unlike HSETEX, which always
        # writes a value and therefore always has an effect). Add a new member
        # in the same call so the command is a write and reaches the stream.
        r SADDEX myset KEEPTTL MEMBERS 2 m1 m2

        # KEEPTTL carries no time to rewrite, so it is propagated verbatim,
        # as HSETEX KEEPTTL is.
        assert_replication_stream $repl {
            {select *}
            {saddex myset KEEPTTL MEMBERS 2 m1 m2}
        }
        close_replication_stream $repl

        # KEEPTTL kept m1's TTL and gave m2 none.
        assert_morethan [set_member_ttl r myset m1] 90
        assert_equal -1 [set_member_ttl r myset m2]
    }

    test {SEXPIRE with a time in the past is propagated as SREM} {
        r FLUSHALL
        r DEBUG SET-ACTIVE-EXPIRE 0
        set repl [attach_to_replication_stream]

        r SADD myset m1 m2
        r SEXPIRE myset 0 MEMBERS 1 m1

        # A time in the past deletes the member and propagates as SREM. m2
        # survives, so the key survives and only the SREM propagates - one op,
        # hence no MULTI wrapper.
        assert_replication_stream $repl {
            {select *}
            {sadd myset m1 m2}
            {srem myset m1}
        }
        close_replication_stream $repl
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    test {SEXPIREAT in the past on the last member propagates SREM only} {
        r FLUSHALL
        r DEBUG SET-ACTIVE-EXPIRE 0
        set repl [attach_to_replication_stream]

        r SADD myset m1
        r SEXPIREAT myset 1 MEMBERS 1 m1

        # The command propagates only the member deletion: the replica deletes
        # the emptied key itself when it applies the SREM, as it does for HDEL,
        # so no DEL is on the wire and with a single op there is no MULTI
        # wrapper either. Only active expiration propagates SREM followed
        # by a key deletion.
        assert_replication_stream $repl {
            {select *}
            {sadd myset m1}
            {srem myset m1}
        }
        close_replication_stream $repl

        # The primary drops the emptied key on the spot, without active expiration.
        assert_equal 0 [r EXISTS myset]
        assert_equal 0 [r SCARD myset]
        assert_equal 0 [get_keys_with_volatile_items r]
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    test {SADD over an expired member propagates SREM before SADD} {
        r FLUSHALL
        r DEBUG SET-ACTIVE-EXPIRE 0
        r SADD myset m1 keepme
        make_members_expired r myset {m1}

        set repl [attach_to_replication_stream]
        r SADD myset m1

        # A replica does not consider the member expired, so the primary must
        # propagate SREM for it before the SADD that replaces it with a plain
        # member. A command that propagates more than one op opens the MULTI
        # first and emits the SELECT inside it, so `multi` precedes `select`.
        assert_replication_stream $repl {
            {multi}
            {select *}
            {srem myset m1}
            {sadd myset m1}
            {exec}
        }
        close_replication_stream $repl
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    test {SADDEX KEEPTTL over an expired member propagates SREM first} {
        r FLUSHALL
        r DEBUG SET-ACTIVE-EXPIRE 0
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
        close_replication_stream $repl
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    test {SMOVE onto an expired destination member propagates SREM first} {
        r FLUSHALL
        r DEBUG SET-ACTIVE-EXPIRE 0
        r SADD src{t} m1
        r SADD dst{t} m1 other
        make_members_expired r dst{t} {m1}

        set repl [attach_to_replication_stream]
        r SMOVE src{t} dst{t} m1

        # SMOVE itself is propagated as-is, preceded by the SREM that removes
        # the expired destination copy.
        assert_replication_stream $repl {
            {multi}
            {select *}
            {srem dst{t} m1}
            {smove src{t} dst{t} m1}
            {exec}
        }
        close_replication_stream $repl
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    test {Active expiry propagates one SREM per batch and no DEL while members remain} {
        r FLUSHALL
        enable_active_expiry r
        r SADD myset keepme
        set repl [attach_to_replication_stream]

        r SADDEX myset PX 50 MEMBERS 2 m1 m2
        wait_for_condition 100 100 {
            [r SCARD myset] == 1
        } else {
            fail "active expiry did not reclaim m1/m2"
        }

        # Every member reclaimed in one active-expiration pass is batched into a
        # single SREM. Both TTLs are identical and well under the batch limit,
        # so the two members travel together.
        assert_replication_stream $repl {
            {select *}
            {saddex myset PXAT * MEMBERS 2 m1 m2}
            {srem myset m1 m2}
        }
        close_replication_stream $repl
    }

    test {Active expiry of the last member propagates SREM then UNLINK} {
        r FLUSHALL
        enable_active_expiry r
        set repl [attach_to_replication_stream]

        r SADDEX myset PX 50 MEMBERS 1 m1
        wait_for_condition 100 100 {
            [r EXISTS myset] == 0
        } else {
            fail "active expiry did not delete the emptied key"
        }

        # Active expiration propagates SREM first and the key deletion second, so a
        # replica can report srem notifications before del. The deletion goes
        # through propagateDeletion, so it is an UNLINK while
        # lazyfree-lazy-expire is on (the default, and what expire.tcl asserts
        # for key expiry too), and the two ops are wrapped in MULTI/EXEC.
        assert_replication_stream $repl {
            {select *}
            {saddex myset PXAT * MEMBERS 1 m1}
            {multi}
            {srem myset m1}
            {unlink myset}
            {exec}
        }
        close_replication_stream $repl
    }

    test {Reads never propagate on a set with expired members} {
        r FLUSHALL
        r DEBUG SET-ACTIVE-EXPIRE 0
        r SADD myset m1 keepme
        make_members_expired r myset {m1}

        set repl [attach_to_replication_stream]
        assert_equal 0 [r SISMEMBER myset m1]
        assert_equal {keepme} [r SMEMBERS myset]
        assert_equal 1 [r SISMEMBER myset keepme]
        assert_equal {0} [r SMISMEMBER myset m1]

        # Reads never delete and never propagate. The SELECT preamble is
        # emitted with the first write and there is no write here, so the stream
        # stays empty.
        assert_equal {} [read_from_replication_stream $repl]
        close_replication_stream $repl
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    # A STORE destination is computed from the primary's live view, so the
    # result is propagated instead of the command: UNLINK of the destination
    # followed by the members that were actually written.

    test {SADDEX on a key literally named nx keeps the key and strips only the option} {
        r FLUSHALL
        set repl [attach_to_replication_stream]

        # The key is the string "nx". The rewrite scans only the options region
        # (after the key, before MEMBERS), so the key survives while the NX
        # condition itself is dropped.
        r SADDEX nx NX EX 100 MEMBERS 1 a
        set at_ms [set_member_pexpiretime r nx a]

        assert_replication_stream $repl [subst {
            {select *}
            {saddex nx PXAT $at_ms MEMBERS 1 a}
        }]
        close_replication_stream $repl
        assert_equal 1 [r SISMEMBER nx a]
        assert_range [set_member_ttl r nx a] 1 100
    }

    test {SADD over an expired member inside MULTI/EXEC emits srem then sadd} {
        r FLUSHALL
        r DEBUG SET-ACTIVE-EXPIRE 0
        r SADD s expired keepme
        make_members_expired r s {expired}

        set repl [attach_to_replication_stream]
        r MULTI
        r SADD s expired
        assert_equal {1} [r EXEC]

        # Inside an explicit transaction the SREM that removes the expired
        # member and the SADD that replaces it travel inside the transaction's
        # own MULTI/EXEC. No second, nested MULTI is opened for them.
        assert_replication_stream $repl {
            {multi}
            {select *}
            {srem s expired}
            {sadd s expired}
            {exec}
        }
        close_replication_stream $repl
        assert_equal 1 [r SISMEMBER s expired]
        assert_equal -1 [set_member_ttl r s expired]
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    foreach {cmd expected} {SUNIONSTORE {a b c} SINTERSTORE {a} SDIFFSTORE {c}} {
        test "$cmd with an expired member in a source propagates unlink plus sadd" {
            r FLUSHALL
            r DEBUG SET-ACTIVE-EXPIRE 0
            r SADD s1{t} a expired c
            r SADD s2{t} a b
            r SADD dst{t} stale
            make_members_expired r s1{t} {expired}

            set repl [attach_to_replication_stream]
            r SADD guard{t} warmup ;# carries the SELECT preamble
            r $cmd dst{t} s1{t} s2{t}

            # Live s1 = {a c}, s2 = {a b}. On a replica the expired member is
            # still a member of s1, so re-running the command there would give
            # {a b c expired} for the union and {c expired} for the difference.
            # The result is propagated instead of the command.
            assert_match {select *} [read_from_replication_stream $repl]
            assert_equal [list sadd guard{t} warmup] [read_from_replication_stream $repl]
            assert_equal {multi} [read_from_replication_stream $repl]
            assert_equal [list unlink dst{t}] [read_from_replication_stream $repl]
            # Match the members themselves, not a wildcard: an expired member
            # leaking into the destination has to fail here. The propagated
            # order follows the destination's iteration order, so compare sorted.
            set line [read_from_replication_stream $repl]
            assert_equal sadd [lindex $line 0]
            assert_equal dst{t} [lindex $line 1]
            assert_equal $expected [lsort [lrange $line 2 end]]
            assert_equal {exec} [read_from_replication_stream $repl]
            assert_equal {} [read_from_replication_stream $repl]
            close_replication_stream $repl
            assert_equal $expected [lsort [r SMEMBERS dst{t}]]
            # The destination is plain, whatever the sources carried.
            foreach m $expected {
                assert_equal -1 [set_member_ttl r dst{t} $m]
            }
            r DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}
    }

    test {SDIFFSTORE with an expired member in the subtracted set propagates the result} {
        r FLUSHALL
        r DEBUG SET-ACTIVE-EXPIRE 0
        r SADD s1{t} a b c
        r SADD s2{t} b expired
        r SADD dst{t} stale
        make_members_expired r s2{t} {expired}
        # Make the expired member matter by adding it to s1 as a live member:
        # the primary subtracts only {b}, because s2's expired copy is not an
        # input, while a replica re-running SDIFFSTORE would subtract it too.
        r SADD s1{t} expired
        assert_equal 1 [r SISMEMBER s1{t} expired]

        set repl [attach_to_replication_stream]
        r SADD guard{t} warmup
        assert_equal 3 [r SDIFFSTORE dst{t} s1{t} s2{t}]

        assert_match {select *} [read_from_replication_stream $repl]
        assert_equal [list sadd guard{t} warmup] [read_from_replication_stream $repl]
        assert_equal {multi} [read_from_replication_stream $repl]
        assert_equal [list unlink dst{t}] [read_from_replication_stream $repl]
        # s2's expired copy of the member must not be subtracted on the wire
        # either, so name the three members instead of matching a wildcard.
        set line [read_from_replication_stream $repl]
        assert_equal sadd [lindex $line 0]
        assert_equal dst{t} [lindex $line 1]
        assert_equal {a c expired} [lsort [lrange $line 2 end]]
        assert_equal {exec} [read_from_replication_stream $repl]
        assert_equal {} [read_from_replication_stream $repl]
        close_replication_stream $repl
        assert_equal {a c expired} [lsort [r SMEMBERS dst{t}]]
        assert_equal {-1 -1 -1} [r STTL dst{t} MEMBERS 3 a c expired]
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

        # No member TTL anywhere, so the sources are identical on both sides
        # and the cheap verbatim propagation is kept.
        assert_replication_stream $repl {
            {select *}
            {sunionstore u{t} s1{t} s2{t}}
            {sinterstore i{t} s1{t} s2{t}}
            {sdiffstore d{t} s1{t} s2{t}}
        }
        close_replication_stream $repl
    }

    test {SORT STORE with a volatile source propagates unlink plus rpush} {
        r FLUSHALL
        r DEBUG SET-ACTIVE-EXPIRE 0
        r SADD src{t} 1 2 3 4 5
        r RPUSH dst{t} stale
        make_members_expired r src{t} {3}

        set repl [attach_to_replication_stream]
        r SADD guard{t} warmup
        assert_equal 4 [r SORT src{t} STORE dst{t}]

        # The destination is a LIST, so the result is propagated as RPUSH in
        # the sorted order the primary produced - a replica re-running SORT
        # would include the expired member and produce a different list.
        assert_replication_stream $repl {
            {select *}
            {sadd guard{t} warmup}
            {multi}
            {unlink dst{t}}
            {rpush dst{t} 1 2 4 5}
            {exec}
        }
        close_replication_stream $repl
        assert_equal {1 2 4 5} [r LRANGE dst{t} 0 -1]
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    test {SPOP with a count propagates only the popped members} {
        r FLUSHALL
        r DEBUG SET-ACTIVE-EXPIRE 0
        for {set i 0} {$i < 10} {incr i} {
            r SADD myset m$i
        }
        set expired_members {}
        for {set i 4} {$i < 10} {incr i} {
            lappend expired_members m$i
        }
        make_members_expired r myset $expired_members
        # The expired members are still physically there, keeping the key alive.
        assert_equal 10 [r SCARD myset]
        assert_equal 4 [llength [r SMEMBERS myset]]

        set repl [attach_to_replication_stream]
        # Pop every live member with a count BELOW the physical size (10): that
        # is the pop-one-by-one path, which stops at the first missing live
        # member. SPOP does not reclaim, so the ONLY member deletion on the wire is
        # the pop itself. (count >= SCARD would be the whole-set path,
        # propagated as a single UNLINK.)
        set popped [r SPOP myset 4]
        assert_equal {m0 m1 m2 m3} [lsort $popped]

        set line [read_from_replication_stream $repl]
        assert_match {select *} $line
        set line [read_from_replication_stream $repl]
        assert_equal srem [lindex $line 0]
        assert_equal myset [lindex $line 1]
        # Exactly the four popped members, with no expired member smuggled in.
        assert_equal {m0 m1 m2 m3} [lsort [lrange $line 2 end]]
        # Nothing else: no SREM for the expired members and no DEL, because
        # they keep the key alive.
        assert_equal {} [read_from_replication_stream $repl]
        close_replication_stream $repl

        assert_equal 1 [r EXISTS myset]
        assert_equal 6 [r SCARD myset]
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    test {SRANDMEMBER never propagates anything} {
        r FLUSHALL
        r DEBUG SET-ACTIVE-EXPIRE 0
        for {set i 0} {$i < 10} {incr i} {
            r SADD myset m$i
        }
        make_members_expired r myset {m4 m5 m6 m7 m8 m9}
        assert_equal 4 [llength [r SMEMBERS myset]]

        set repl [attach_to_replication_stream]
        assert_equal {m0 m1 m2 m3} [lsort [r SRANDMEMBER myset 1000]]
        assert_equal 20 [llength [r SRANDMEMBER myset -20]]
        assert_equal 1 [llength [r SRANDMEMBER myset 1]]
        r SRANDMEMBER myset

        # A read never reclaims, so it never writes to the stream either.
        assert_equal {} [read_from_replication_stream $repl]
        close_replication_stream $repl
        assert_equal 10 [r SCARD myset]
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    # At most 1024 members travel per SREM, so replacing more expired members
    # than that in one command splits their removal over several SREMs while the
    # command itself still propagates once.
    foreach nmembers {1024 1025} {
        test "SADD over $nmembers expired members propagates SREM in batches of 1024" {
            r FLUSHALL
            r DEBUG SET-ACTIVE-EXPIRE 0
            set members {}
            for {set i 0} {$i < $nmembers} {incr i} {
                lappend members m$i
            }
            r SADDEX myset PX 10 MEMBERS $nmembers {*}$members
            wait_for_condition 50 100 {
                [r SCARD myset] == $nmembers && [llength [r SMEMBERS myset]] == 0
            } else {
                fail "the $nmembers members are not all unreclaimed expired ones"
            }

            set repl [attach_to_replication_stream]
            assert_equal $nmembers [r SADD myset {*}$members]

            assert_equal {multi} [read_from_replication_stream $repl]
            assert_match {select *} [read_from_replication_stream $repl]
            set batches 0
            set removed 0
            while {$removed < $nmembers} {
                set line [read_from_replication_stream $repl]
                assert_equal srem [lindex $line 0]
                assert_equal myset [lindex $line 1]
                set batch [expr {[llength $line] - 2}]
                assert_lessthan_equal $batch 1024
                incr removed $batch
                incr batches
            }
            assert_equal [expr {($nmembers + 1023) / 1024}] $batches
            # One SADD for all of them, whatever the number of SREM batches.
            set line [read_from_replication_stream $repl]
            assert_equal sadd [lindex $line 0]
            assert_equal myset [lindex $line 1]
            assert_equal $members [lrange $line 2 end]
            assert_equal {exec} [read_from_replication_stream $repl]
            assert_equal {} [read_from_replication_stream $repl]
            close_replication_stream $repl

            # Every member is live again and nothing is volatile any more.
            assert_equal $nmembers [r SCARD myset]
            assert_equal $nmembers [llength [r SMEMBERS myset]]
            assert_equal 0 [get_keys_with_volatile_items r]
            r DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}
    }

    # The SREM and S*EXPIRE propagation filters rewrite the command itself
    # rather than emitting deletions, so they name every member they touched in
    # one command and never split at 1024.
    test {SREM and SEXPIRE NX rewrite more than 1024 members into a single command} {
        set nmembers 1025
        r FLUSHALL
        r DEBUG SET-ACTIVE-EXPIRE 0
        set members {}
        for {set i 0} {$i < $nmembers} {incr i} {
            lappend members m$i
        }
        r SADD myset {*}$members
        r SADDEX myset PX 10 MEMBERS 1 expired
        wait_for_condition 50 100 {
            [r SISMEMBER myset expired] == 0
        } else {
            fail "the member did not expire on the primary"
        }

        # NX passes for every member that has no TTL, and skips the expired one,
        # which is reported missing.
        set repl [attach_to_replication_stream]
        set reply [r SEXPIRE myset 100 NX MEMBERS [expr {$nmembers + 1}] expired {*}$members]
        assert_equal -2 [lindex $reply 0]
        assert_match {select *} [read_from_replication_stream $repl]
        set line [read_from_replication_stream $repl]
        assert_equal spexpireat [lindex $line 0]
        assert_equal myset [lindex $line 1]
        assert_equal MEMBERS [lindex $line 3]
        assert_equal $nmembers [lindex $line 4]
        assert_equal $members [lrange $line 5 end]

        # SREM naming the expired member plus every live one removes only the
        # live ones, again in a single command.
        assert_equal $nmembers [r SREM myset expired {*}$members]
        set line [read_from_replication_stream $repl]
        assert_equal srem [lindex $line 0]
        assert_equal myset [lindex $line 1]
        assert_equal $members [lrange $line 2 end]
        assert_equal {} [read_from_replication_stream $repl]
        close_replication_stream $repl

        # The expired member kept the key alive for active expiration.
        assert_equal 1 [r EXISTS myset]
        assert_equal 1 [r SCARD myset]
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}
}

# Replica consistency: a replica never reclaims, digests agree, failover hands
# expiry to the promoted replica.

start_server {tags {"setexpire external:skip"}} {
    start_server {tags {needs:repl external:skip}} {
        set primary [srv -1 client]
        set primary_host [srv -1 host]
        set primary_port [srv -1 port]
        set replica [srv 0 client]
        set replica_host [srv 0 host]
        set replica_port [srv 0 port]

        test {Full sync carries member TTLs} {
            $primary FLUSHALL
            $replica replicaof no one

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

        test {Replica member TTLs are absolute and identical to the primary} {
            $primary FLUSHALL
            wait_for_ofs_sync $primary $replica

            $primary SADD myset m1 m3
            $primary SPEXPIREAT myset [expr {[clock milliseconds] + 60000}] MEMBERS 1 m1
            $primary SADDEX myset EX 60 MEMBERS 1 m2
            $primary SEXPIRE myset 60 MEMBERS 1 m3
            wait_for_ofs_sync $primary $replica

            # Compare the ABSOLUTE stamps: a relative PTTL read on one side and
            # then on the other is read at two different instants and would only
            # ever hold by luck. Every relative form is rewritten to an
            # absolute PXAT / SPEXPIREAT before propagation, so both sides must
            # report the very same stamp.
            foreach m {m1 m2 m3} {
                assert_equal [set_member_pexpiretime $primary myset $m] [set_member_pexpiretime $replica myset $m]
                assert_equal 1 [$replica SISMEMBER myset $m]
                assert_morethan [set_member_pttl $replica myset $m] 0
            }
        }

        test {A replica never reclaims an expired member on its own} {
            $primary FLUSHALL
            $primary DEBUG SET-ACTIVE-EXPIRE 0
            wait_for_ofs_sync $primary $replica

            $primary SADD myset m1 keepme
            $primary SPEXPIRE myset 10 MEMBERS 1 m1
            wait_for_ofs_sync $primary $replica

            # Once the TTL has passed, the member is invisible on the primary
            # but the primary is not reclaiming (active expire off) so no SREM is
            # propagated.
            wait_for_condition 50 100 {
                [$primary SISMEMBER myset m1] == 0
            } else {
                fail "m1 is not an unreclaimed expired member on the primary"
            }
            after 100
            wait_for_ofs_sync $primary $replica

            # A replica never DELETES, but it does HIDE an expired member from
            # reads, exactly as HEXISTS returns 0 for an expired field on a
            # replica and as an expired key reads as missing there. So the
            # member is invisible on both sides while only the primary may
            # remove it: SCARD still counts it on the replica and the key stays
            # tracked until the SREM lands.
            assert_equal 0 [$replica SISMEMBER myset m1]
            assert_equal {0} [$replica SMISMEMBER myset m1]
            assert_equal 2 [$replica SCARD myset]
            assert_equal {keepme} [$replica SMEMBERS myset]
            assert_equal 1 [get_keys_with_volatile_items $replica]

            # Let the primary reclaim, then the replica converges.
            $primary DEBUG SET-ACTIVE-EXPIRE 1
            wait_for_condition 100 100 {
                [$primary SCARD myset] == 1
            } else {
                fail "primary did not reclaim the expired member after re-enabling active expiry"
            }
            wait_for_ofs_sync $primary $replica
            assert_equal 0 [$replica SISMEMBER myset m1]
            assert_equal 1 [$replica SCARD myset]
        } {} {needs:debug}

        test {A volatile set has the same members and deadlines on primary and replica} {
            $primary FLUSHALL
            wait_for_ofs_sync $primary $replica

            $primary SADD myset m1 m2 m3
            $primary SEXPIRE myset 5000 MEMBERS 2 m1 m2
            wait_for_ofs_sync $primary $replica

            # The digest covers the members only, so the deadlines are compared
            # separately: SPEXPIRETIME is absolute, and a replica that
            # re-evaluated the relative time would answer differently.
            assert_equal [$primary DEBUG DIGEST-VALUE myset] [$replica DEBUG DIGEST-VALUE myset]
            assert_equal [$primary SPEXPIRETIME myset MEMBERS 3 m1 m2 m3] \
                [$replica SPEXPIRETIME myset MEMBERS 3 m1 m2 m3]

            # Encoding is irrelevant: force a hashtable on the primary side by
            # growing the set and re-compare.
            for {set i 0} {$i < 600} {incr i} {
                $primary SADD myset "bulk-$i"
            }
            $primary SEXPIRE myset 5000 MEMBERS 1 bulk-1
            wait_for_ofs_sync $primary $replica
            assert_equal [$primary DEBUG DIGEST-VALUE myset] [$replica DEBUG DIGEST-VALUE myset]
            assert_equal [$primary SPEXPIRETIME myset MEMBERS 3 m1 m2 bulk-1] \
                [$replica SPEXPIRETIME myset MEMBERS 3 m1 m2 bulk-1]
        } {} {needs:debug}

        test {A promoted replica reclaims its expired members itself} {
            $primary FLUSHALL
            $primary DEBUG SET-ACTIVE-EXPIRE 0
            $replica DEBUG SET-ACTIVE-EXPIRE 0
            wait_for_ofs_sync $primary $replica

            $primary SADD myset m1 m2 keepme
            $primary SPEXPIREAT myset [expr {[clock milliseconds] + 100}] MEMBERS 2 m1 m2
            wait_for_ofs_sync $primary $replica
            after 150

            # Still counted on the replica: it is not allowed to reclaim. The
            # members are already invisible to reads, but SCARD keeps counting
            # them because only the primary may remove them.
            assert_equal 3 [$replica SCARD myset]
            assert_equal 0 [$replica SISMEMBER myset m1]
            assert_equal 1 [get_keys_with_volatile_items $replica]

            # Promote it. Now it owns expiry.
            $replica replicaof no one
            wait_for_condition 100 100 {
                [info_field [$replica info replication] role] eq "master"
            } else {
                fail "Replica didn't become master"
            }
            $replica DEBUG SET-ACTIVE-EXPIRE 1

            set initial_expired [get_expired_set_members $replica]
            wait_for_condition 100 100 {
                [$replica SCARD myset] == 1
            } else {
                fail "promoted replica did not reclaim its expired members"
            }
            assert_equal 0 [$replica SISMEMBER myset m1]
            assert_equal 0 [$replica SISMEMBER myset m2]
            assert_equal 1 [$replica SISMEMBER myset keepme]
            assert_morethan [get_expired_set_members $replica] $initial_expired

            # The old primary still holds its expired members: it never reclaimed.
            assert_equal 3 [$primary SCARD myset]

            $primary DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}

        # A replica applies commands under POLICY_IGNORE_EXPIRE and therefore
        # keeps unreclaimed expired members that the primary hides. Any
        # operation that rebuilds a set must carry them across with their
        # expiry or propagate their removal; otherwise SCARD diverges for good,
        # because a later active expiration on the primary finds nothing to
        # remove.
        test {Listpack to hashtable conversion keeps an expired member in step on both sides} {
            $primary FLUSHALL
            $replica replicaof no one
            $primary DEBUG SET-ACTIVE-EXPIRE 0
            $primary CONFIG SET set-max-listpack-entries 4
            $primary SADD myset expired l1 l2
            make_members_expired $primary myset {expired}
            attach_replica $primary $replica $primary_host $primary_port
            $replica CONFIG SET set-max-listpack-entries 4

            # crossing set-max-listpack-entries converts to hashtable on both
            $primary SADD myset l3 l4 l5
            wait_for_ofs_sync $primary $replica
            assert_equal hashtable [$primary OBJECT ENCODING myset]
            assert_equal [$primary SCARD myset] [$replica SCARD myset]
            assert_equal [$primary DEBUG DIGEST-VALUE myset] [$replica DEBUG DIGEST-VALUE myset]
            assert_equal [$primary SPEXPIRETIME myset MEMBERS 1 expired] \
                [$replica SPEXPIRETIME myset MEMBERS 1 expired]
            assert_equal 0 [$primary SISMEMBER myset expired]
            assert_equal 5 [llength [$primary SMEMBERS myset]]

            $primary CONFIG SET set-max-listpack-entries 128
            $replica CONFIG SET set-max-listpack-entries 128
            $primary DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}

        test {COPY of a hashtable set with an expired member keeps both sides in step} {
            $primary FLUSHALL
            $primary DEBUG SET-ACTIVE-EXPIRE 0
            $primary CONFIG SET set-max-listpack-entries 0
            $primary SADD src expired l1 l2
            make_members_expired $primary src {expired}
            assert_equal hashtable [$primary OBJECT ENCODING src]
            $primary COPY src dst
            wait_for_ofs_sync $primary $replica
            assert_equal [$primary SCARD dst] [$replica SCARD dst]
            assert_equal [$primary DEBUG DIGEST-VALUE dst] [$replica DEBUG DIGEST-VALUE dst]
            assert_equal [$primary SPEXPIRETIME dst MEMBERS 1 expired] \
                [$replica SPEXPIRETIME dst MEMBERS 1 expired]
            assert_equal 0 [$primary SISMEMBER dst expired]
            assert_equal {l1 l2} [lsort [$primary SMEMBERS dst]]
            $primary CONFIG SET set-max-listpack-entries 128
            $primary DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}

        test {SRANDMEMBER with a count propagates nothing and SPOP with a count does not reclaim} {
            $primary FLUSHALL
            $primary DEBUG SET-ACTIVE-EXPIRE 0
            $primary SADD myset e1 e2 l1 l2
            make_members_expired $primary myset {e1 e2}
            wait_for_ofs_sync $primary $replica
            set repl [attach_to_replication_stream_on_connection -1]

            # read-only: no reclaim, no stream traffic, expired members still counted
            set got [$primary SRANDMEMBER myset 10]
            assert_equal {l1 l2} [lsort $got]
            assert_equal 4 [$primary SCARD myset]
            assert_equal {} [read_from_replication_stream $repl]

            # write: exactly one SREM, for the popped member only. SPOP <count>
            # does not reclaim, so neither expired member is deleted here and the
            # pop cannot be confused with a reclaim.
            set popped [$primary SPOP myset 1]
            assert_equal 1 [llength $popped]
            # first write on this stream: the SELECT preamble comes first
            set line [read_from_replication_stream $repl]
            assert_match {select *} $line
            set line [read_from_replication_stream $repl]
            assert_equal [list srem myset [lindex $popped 0]] $line
            assert_equal {} [read_from_replication_stream $repl]
            close_replication_stream $repl

            wait_for_ofs_sync $primary $replica
            # 2 expired members + the one live member that was not popped.
            assert_equal 3 [$primary SCARD myset]
            assert_equal [$primary SCARD myset] [$replica SCARD myset]
            $primary DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}

        # End-to-end checks for the UNLINK + SADD / RPUSH rewrites asserted on
        # the stream above: without them the replica recomputes the result over
        # its own view, where the expired members are still members, and the two
        # destinations diverge permanently.

        foreach {cmd expected} {SUNIONSTORE {a b c} SINTERSTORE {a} SDIFFSTORE {c}} {
            test "$cmd destination is identical on primary and replica" {
                $primary FLUSHALL
                $primary DEBUG SET-ACTIVE-EXPIRE 0
                $primary SADD s1{t} a expired c
                $primary SADD s2{t} a b
                $primary SADD dst{t} stale
                make_members_expired $primary s1{t} {expired}
                # The replica still holds the expired member: that is the
                # divergence this test exists to catch.
                wait_for_ofs_sync $primary $replica
                assert_equal 3 [$replica SCARD s1{t}]

                $primary $cmd dst{t} s1{t} s2{t}
                wait_for_ofs_sync $primary $replica

                assert_equal $expected [lsort [$primary SMEMBERS dst{t}]]
                assert_equal $expected [lsort [$replica SMEMBERS dst{t}]]
                assert_equal [$primary SCARD dst{t}] [$replica SCARD dst{t}]
                assert_equal [$primary DEBUG DIGEST-VALUE dst{t}] [$replica DEBUG DIGEST-VALUE dst{t}]
                # No member of the destination is volatile, on either side.
                foreach m $expected {
                    assert_equal -1 [set_member_ttl $primary dst{t} $m]
                    assert_equal -1 [set_member_ttl $replica dst{t} $m]
                }
                # (s1{t} itself is still volatile on both sides - it holds the
                # expired member - so the tracking count is not asserted here.)
                $primary DEBUG SET-ACTIVE-EXPIRE 1
            } {OK} {needs:debug}
        }

        foreach {encoding lp_entries} {listpack 128 hashtable 0} {
            test "SORT STORE from a $encoding set with an expired member is identical on both sides" {
                $primary FLUSHALL
                $primary DEBUG SET-ACTIVE-EXPIRE 0
                $primary CONFIG SET set-max-listpack-entries $lp_entries
                $replica CONFIG SET set-max-listpack-entries $lp_entries
                $primary SADD src{t} 1 2 3 4 5
                $primary RPUSH dst{t} stale
                make_members_expired $primary src{t} {3}
                assert_equal $encoding [$primary OBJECT ENCODING src{t}]

                assert_equal 4 [$primary SORT src{t} STORE dst{t}]
                assert_equal {PONG} [$primary PING]
                wait_for_ofs_sync $primary $replica

                assert_equal {1 2 4 5} [$primary LRANGE dst{t} 0 -1]
                assert_equal {1 2 4 5} [$replica LRANGE dst{t} 0 -1]
                assert_equal [$primary LLEN dst{t}] [$replica LLEN dst{t}]
                assert_equal [$primary DEBUG DIGEST-VALUE dst{t}] [$replica DEBUG DIGEST-VALUE dst{t}]
                # SORT_RO must hide the expired member too, and change nothing.
                assert_equal {1 2 4 5} [$primary SORT_RO src{t}]
                assert_equal {PONG} [$primary PING]

                $primary CONFIG SET set-max-listpack-entries 128
                $replica CONFIG SET set-max-listpack-entries 128
                $primary DEBUG SET-ACTIVE-EXPIRE 1
            } {OK} {needs:debug}
        }

        test {SADD over the last expired member of an all-integer set keeps its encoding on both sides} {
            $primary FLUSHALL
            $primary DEBUG SET-ACTIVE-EXPIRE 0
            $primary SADD myset 1 2 3
            make_members_expired $primary myset {3}
            wait_for_ofs_sync $primary $replica

            $primary SADD myset 3
            wait_for_ofs_sync $primary $replica

            # Losing the last TTL does not convert the set back to an intset,
            # and SREM then SADD is propagated, so both sides keep the encoding
            # the TTL forced them into. A mismatch here diverges the digest on
            # the next reload.
            assert_equal listpack [$primary OBJECT ENCODING myset]
            assert_equal listpack [$replica OBJECT ENCODING myset]
            assert_equal 3 [$replica SCARD myset]
            assert_equal {1 2 3} [lsort [$replica SMEMBERS myset]]
            assert_equal [$primary DEBUG DIGEST-VALUE myset] [$replica DEBUG DIGEST-VALUE myset]
            assert_equal 0 [get_keys_with_volatile_items $replica]
            $primary DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}

        test {SMOVE of an integer over the last expired member keeps the encoding on both sides} {
            $primary FLUSHALL
            $primary DEBUG SET-ACTIVE-EXPIRE 0
            $primary SADD src{t} 3
            $primary SADD dst{t} 1 2 3
            make_members_expired $primary dst{t} {3}
            wait_for_ofs_sync $primary $replica

            assert_equal 1 [$primary SMOVE src{t} dst{t} 3]
            wait_for_ofs_sync $primary $replica

            assert_equal listpack [$primary OBJECT ENCODING dst{t}]
            assert_equal listpack [$replica OBJECT ENCODING dst{t}]
            assert_equal {1 2 3} [lsort [$replica SMEMBERS dst{t}]]
            assert_equal [$primary DEBUG DIGEST-VALUE dst{t}] [$replica DEBUG DIGEST-VALUE dst{t}]
            assert_equal 0 [get_keys_with_volatile_items $replica]
            $primary DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}
    }
}

# RDB reload, DUMP/RESTORE, COPY/MOVE/RENAME, SWAPDB/FLUSHDB.

start_server {tags {"setexpire external:skip"}} {
    foreach encoding {listpack hashtable} {
        test "DEBUG RELOAD keeps member TTLs - $encoding" {
            r FLUSHALL
            r DEBUG SET-ACTIVE-EXPIRE 0
            if {$encoding eq "hashtable"} {
                r config set set-max-listpack-entries 0
            } else {
                r config set set-max-listpack-entries 128
            }

            r SADD myset m1 m2 m3
            set exp [expr {[clock milliseconds] + 100000}]
            r SPEXPIREAT myset $exp MEMBERS 1 m1
            r SEXPIRE myset 200 MEMBERS 1 m2
            # m3 stays persistent.
            assert_encoding $encoding myset
            assert_equal 1 [get_keys_with_volatile_items r]

            r DEBUG RELOAD

            # Encoding names are unchanged by member TTLs: only the RDB type
            # differs (RDB_TYPE_SET_2), never OBJECT ENCODING.
            assert_encoding $encoding myset
            assert_equal 3 [r SCARD myset]
            assert_equal $exp [set_member_pexpiretime r myset m1]
            assert_range [set_member_ttl r myset m2] 100 200
            assert_equal -1 [set_member_ttl r myset m3]
            assert_equal 1 [get_keys_with_volatile_items r]
            assert_equal 1 [get_keyspace_field r keys]

            r config set set-max-listpack-entries 128
            r DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}
    }

    test {DEBUG RELOAD of a set whose members are all expired drops the key} {
        r FLUSHALL
        r DEBUG SET-ACTIVE-EXPIRE 0

        r SADDEX myset PX 1 MEMBERS 3 m1 m2 m3
        wait_for_condition 50 100 {
            [r STTL myset MEMBERS 3 m1 m2 m3] eq {-2 -2 -2}
        } else {
            fail "set members did not expire"
        }

        r SAVE
        r FLUSHALL
        r DEBUG RELOAD NOSAVE

        # An RDB load on a primary skips already-expired members and returns
        # RDB_LOAD_ERR_ALL_ITEMS_EXPIRED when none remain, so the key never
        # materialises.
        assert_equal 0 [r EXISTS myset]
        assert_equal 0 [r SCARD myset]
        assert_equal 0 [get_keys_with_volatile_items r]

        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    test {DUMP/RESTORE keeps member TTLs} {
        r FLUSHALL
        r DEBUG SET-ACTIVE-EXPIRE 0

        r SADD myset{t} m1 m2 m3
        set exp [expr {[clock milliseconds] + 200000}]
        r SPEXPIREAT myset{t} $exp MEMBERS 1 m1
        set serialized [r DUMP myset{t}]

        r RESTORE rstr{t} 0 $serialized

        assert_equal 3 [r SCARD rstr{t}]
        assert_equal $exp [set_member_pexpiretime r rstr{t} m1]
        assert_equal -1 [set_member_ttl r rstr{t} m2]
        assert_equal -1 [set_member_ttl r rstr{t} m3]
        assert_equal 2 [get_keyspace_field r keys]
        assert_equal 2 [get_keys_with_volatile_items r]
        assert_encoding [r OBJECT ENCODING myset{t}] rstr{t}

        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    test {RESTORE of an all-expired payload loads the expired members} {
        r FLUSHALL
        r DEBUG SET-ACTIVE-EXPIRE 0

        r SADDEX myset PX 1 MEMBERS 3 m1 m2 m3
        set serialized [r DUMP myset]
        wait_for_condition 50 100 {
            [r STTL myset MEMBERS 3 m1 m2 m3] eq {-2 -2 -2}
        } else {
            fail "set members did not expire"
        }
        r DEL myset
        r RESTORE myset 0 $serialized

        # RESTORE is not the primary RDB-load path and does NOT skip
        # already-expired items, exactly as it loads expired hash fields. The
        # key therefore exists with SCARD 3, because SCARD counts unreclaimed
        # expired members, while no member is visible; active expiration removes it
        # afterwards.
        assert_equal 1 [r EXISTS myset]
        assert_equal 3 [r SCARD myset]
        assert_equal {} [r SMEMBERS myset]
        assert_equal {0 0 0} [r SMISMEMBER myset m1 m2 m3]

        r DEBUG SET-ACTIVE-EXPIRE 1
        wait_for_condition 100 100 {
            [r EXISTS myset] == 0
        } else {
            fail "active expiry did not remove the restored all-expired key"
        }
    } {} {needs:debug}

    foreach cmd {COPY RENAME} {
        test "$cmd keeps member TTLs" {
            r FLUSHALL
            r DEBUG SET-ACTIVE-EXPIRE 0

            r SADD myset{t} m1 m2 m3
            set exp [expr {[clock milliseconds] + 200000}]
            r SPEXPIREAT myset{t} $exp MEMBERS 2 m1 m2

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
            assert_equal $exp [set_member_pexpiretime r newset{t} m2]
            assert_equal -1 [set_member_ttl r newset{t} m3]
            assert_equal 1 [r SISMEMBER newset{t} m1]

            r DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}
    }

    test {MOVE keeps member TTLs and moves the volatile tracking} {
        r FLUSHALL
        r DEBUG SET-ACTIVE-EXPIRE 0

        r SADD myset m1 m2
        set exp [expr {[clock milliseconds] + 200000}]
        r SPEXPIREAT myset $exp MEMBERS 1 m1
        assert_equal 1 [get_keys_with_volatile_items r]

        assert_equal 1 [r MOVE myset 10]
        assert_equal 0 [r EXISTS myset]
        assert_equal 0 [get_keyspace_field r keys_with_volatile_items 9]

        r select 10
        assert_equal 2 [r SCARD myset]
        assert_equal $exp [set_member_pexpiretime r myset m1]
        assert_equal -1 [set_member_ttl r myset m2]
        assert_equal 1 [get_keyspace_field r keys_with_volatile_items 10]
        r FLUSHDB
        r select 9

        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug singledb:skip}

    test {SWAPDB then FLUSHDB then re-create leaves active expiry working} {
        r FLUSHALL
        enable_active_expiry r
        r SADD myset m1 m2
        r SEXPIRE myset 5000 MEMBERS 1 m1
        assert_equal 1 [get_keys_with_volatile_items r]

        r SWAPDB 9 10

        # The tracking table is per-db and travels with the data.
        assert_equal 0 [get_keyspace_field r keys_with_volatile_items 9]
        assert_equal 0 [r EXISTS myset]
        r select 10
        assert_equal 1 [get_keyspace_field r keys_with_volatile_items 10]
        assert_equal 2 [r SCARD myset]
        assert_morethan [set_member_ttl r myset m1] 4000

        # FLUSHDB must drop the tracking entry with the key, with no crash.
        r FLUSHDB
        assert_equal 0 [get_keyspace_field r keys_with_volatile_items 10]
        assert_equal 0 [get_keyspace_field r keys 10]

        # A freshly created volatile set in the swapped db still expires.
        set initial_expired [get_expired_set_members r]
        r SADD myset survivor
        r SADDEX myset PX 50 MEMBERS 1 doomed
        assert_equal 1 [get_keyspace_field r keys_with_volatile_items 10]
        wait_for_set_active_expiry r myset 1 $initial_expired 1
        assert_equal {survivor} [r SMEMBERS myset]
        assert_equal 0 [get_keyspace_field r keys_with_volatile_items 10]

        r FLUSHDB
        r select 9
    } {OK} {singledb:skip}

    test {FLUSHDB on a db full of volatile sets clears tracking} {
        r FLUSHALL
        enable_active_expiry r
        for {set i 0} {$i < 20} {incr i} {
            r SADD "set-$i" a b c
            r SEXPIRE "set-$i" 5000 MEMBERS 1 a
        }
        assert_equal 20 [get_keys_with_volatile_items r]
        r FLUSHDB
        assert_equal 0 [get_keys_with_volatile_items r]
        assert_equal 0 [get_keyspace_field r keys 9]
    }
}

# The RDB serialization format of a volatile set.
#
# A DUMP payload is the value's RDB body followed by a 10-byte footer: the RDB
# version as two little-endian bytes and then the CRC64 of everything before it.
# The first byte of the body is the RDB type.
proc dump_payload_type {payload} {
    binary scan [string index $payload 0] cu type
    return $type
}

proc dump_payload_rdb_version {payload} {
    binary scan [string range $payload end-9 end-8] su version
    return $version
}

start_server {tags {"setexpire external:skip"}} {
    # RDB type numbers, from enum RdbType. A volatile set needs the TTL-capable
    # type whatever its in-memory encoding is; a plain set keeps the types that
    # existed before member expiration.
    set RDB_TYPE_SET 2
    set RDB_TYPE_SET_INTSET 11
    set RDB_TYPE_SET_LISTPACK 20
    set RDB_TYPE_SET_2 23
    # SET_2 was added in RDB 81 (Valkey 9.1) and is only loaded from that
    # version on, so a payload carrying one is tagged 81.
    set RDB_VERSION_SET_2 81

    foreach {encoding lp_entries} {listpack 128 hashtable 0} {
        test "DUMP of a volatile $encoding set carries RDB type SET_2 and version 81" {
            r FLUSHALL
            r config set set-max-listpack-entries $lp_entries
            r SADD myset m1 m2 m3
            r SEXPIRE myset 100 MEMBERS 1 m1
            assert_encoding $encoding myset

            set payload [r DUMP myset]
            assert_equal $RDB_TYPE_SET_2 [dump_payload_type $payload]
            assert_equal $RDB_VERSION_SET_2 [dump_payload_rdb_version $payload]
            r config set set-max-listpack-entries 128
        }
    }

    test {DUMP of a plain set still uses the set types that predate member expiration} {
        r FLUSHALL
        r config set set-max-listpack-entries 128
        r SADD lp a b c
        r SADD is 1 2 3
        r config set set-max-listpack-entries 0
        r SADD ht a b c
        assert_encoding listpack lp
        assert_encoding intset is
        assert_encoding hashtable ht

        assert_equal $RDB_TYPE_SET_LISTPACK [dump_payload_type [r DUMP lp]]
        assert_equal $RDB_TYPE_SET_INTSET [dump_payload_type [r DUMP is]]
        assert_equal $RDB_TYPE_SET [dump_payload_type [r DUMP ht]]

        # A member TTL on any of them switches the type, and only the type: the
        # reported encoding is untouched.
        r SEXPIRE is 100 MEMBERS 1 1
        assert_equal $RDB_TYPE_SET_2 [dump_payload_type [r DUMP is]]
        r config set set-max-listpack-entries 128
    }

    test {RESTORE of a truncated or corrupted SET_2 payload is refused} {
        r FLUSHALL
        r SADD myset{t} m1 m2 m3
        r SEXPIRE myset{t} 100 MEMBERS 1 m1
        set payload [r DUMP myset{t}]
        r DEBUG SET-SKIP-CHECKSUM-VALIDATION 1

        # Truncated in the middle of a (member, expiry) pair.
        set body [string range $payload 0 end-10]
        set footer [string range $payload end-9 end]
        assert_error {*Bad data format*} {
            r RESTORE dst{t} 0 "[string range $body 0 [expr {[string length $body] / 2}]]$footer"
        }
        assert_equal 0 [r EXISTS dst{t}]

        # A member count larger than the payload actually carries. Byte 1 is the
        # 6-bit length of a 3-member set, so raising it runs the loader off the
        # end of the buffer.
        assert_error {*Bad data format*} {
            r RESTORE dst{t} 0 "[string index $payload 0][binary format c 63][string range $payload 2 end]"
        }
        assert_equal 0 [r EXISTS dst{t}]

        r DEBUG SET-SKIP-CHECKSUM-VALIDATION 0
    } {OK} {needs:debug}

    foreach {load_entries expected_encoding} {0 hashtable 128 listpack} {
        test "A SET_2 payload picks the $expected_encoding encoding at load time and keeps its TTLs" {
            r FLUSHALL
            # Serialize from a listpack set; the encoding is not carried in the
            # RDB, so the loading server's own limits decide.
            r config set set-max-listpack-entries 128
            r SADD src{t} m1 m2 m3
            set exp [expr {[clock milliseconds] + 200000}]
            r SPEXPIREAT src{t} $exp MEMBERS 2 m1 m2
            assert_encoding listpack src{t}
            set payload [r DUMP src{t}]

            r config set set-max-listpack-entries $load_entries
            r RESTORE dst{t} 0 $payload
            assert_encoding $expected_encoding dst{t}
            assert_equal 3 [r SCARD dst{t}]
            assert_equal $exp [set_member_pexpiretime r dst{t} m1]
            assert_equal $exp [set_member_pexpiretime r dst{t} m2]
            assert_equal -1 [set_member_ttl r dst{t} m3]
            assert_equal 2 [get_keys_with_volatile_items r]
            r config set set-max-listpack-entries 128
        }
    }

    # The digest must stay comparable with a plain set, so it folds the members
    # only. The primary/replica comparisons elsewhere cannot see this, since
    # both sides would fold the same TTLs.
    test {DEBUG DIGEST-VALUE ignores the member expiry times} {
        r FLUSHALL
        set exp [expr {[clock milliseconds] + 500000}]
        foreach key {same{t} other_ttl{t} no_ttl{t}} {
            r SADD $key m1 m2 m3
        }
        r SADD reference{t} m1 m2 m3
        r SPEXPIREAT reference{t} $exp MEMBERS 1 m1
        r SPEXPIREAT same{t} $exp MEMBERS 1 m1
        r SPEXPIREAT other_ttl{t} [expr {$exp + 1}] MEMBERS 1 m1

        assert_equal [r DEBUG DIGEST-VALUE reference{t}] [r DEBUG DIGEST-VALUE same{t}]
        assert_equal [r DEBUG DIGEST-VALUE reference{t}] [r DEBUG DIGEST-VALUE other_ttl{t}]
        assert_equal [r DEBUG DIGEST-VALUE reference{t}] [r DEBUG DIGEST-VALUE no_ttl{t}]

        # The deadlines the digest ignores really do differ.
        assert_equal $exp [set_member_pexpiretime r reference{t} m1]
        assert_equal [expr {$exp + 1}] [set_member_pexpiretime r other_ttl{t} m1]
        assert_equal -1 [set_member_pexpiretime r no_ttl{t} m1]
    } {} {needs:debug}
}

# A primary that loads an RDB skips already-expired members and tells its
# replicas so. Only a real startup load does: DEBUG RELOAD skips the members but
# feeds nothing, because it does not pass RDBFLAGS_FEED_REPL.
start_server {tags {"setexpire external:skip"} overrides {save {}}} {
    test {DEBUG RELOAD skips expired members without feeding the replication stream} {
        r DEBUG SET-ACTIVE-EXPIRE 0
        r SADD myset live1 live2
        r SADDEX myset PX 10 MEMBERS 1 expired
        wait_for_condition 50 100 {
            [r SISMEMBER myset expired] == 0
        } else {
            fail "the member is not an unreclaimed expired member"
        }
        assert_equal 3 [r SCARD myset]
        assert_equal 0 [status r master_repl_offset]

        r DEBUG RELOAD
        assert_equal 2 [r SCARD myset]
        assert_equal 0 [status r master_repl_offset]
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    test {A primary loading an RDB at startup feeds SREM for the expired members it skips} {
        r FLUSHALL
        r DEBUG SET-ACTIVE-EXPIRE 0
        r SADD myset live1 live2
        r SADDEX myset PX 10 MEMBERS 1 expired
        wait_for_condition 50 100 {
            [r SISMEMBER myset expired] == 0
        } else {
            fail "the member is not an unreclaimed expired member"
        }
        assert_equal 3 [r SCARD myset]
        r SAVE

        restart_server 0 true false
        wait_done_loading r

        # The expired member was skipped by the load ...
        assert_equal 2 [r SCARD myset]
        assert_equal {live1 live2} [lsort [r SMEMBERS myset]]
        assert_equal 0 [get_keys_with_volatile_items r]
        # ... and the SREM saying so was written to the backlog this load
        # created, so a replica resuming from an earlier offset learns about it.
        set fed_offset [status r master_repl_offset]
        assert_morethan $fed_offset 0

        # The reloaded RDB has nothing left to skip, so the next load feeds
        # nothing: the offset above was not just an artefact of starting up.
        r SAVE
        restart_server 0 true false
        wait_done_loading r
        assert_equal 2 [r SCARD myset]
        assert_equal 0 [status r master_repl_offset]
    } {} {needs:debug}
}

# Member expiration must not cost anything to a set that never had a member TTL,
# so this server keeps its plain sets out of reach of every other test: their
# MEMORY USAGE is measured before any volatile set exists and again after.
start_server {tags {"setexpire external:skip"}} {
    test {MEMORY USAGE of a plain set is unchanged by the feature being used elsewhere} {
        r config set set-max-listpack-entries 128
        r SADD plain_is 1 2 3 4 5
        r SADD plain_lp a b c d e
        assert_encoding intset plain_is
        assert_encoding listpack plain_lp
        set is_before [r MEMORY USAGE plain_is]
        set lp_before [r MEMORY USAGE plain_lp]

        # First volatile set in this server's lifetime.
        r SADD volatile m1 m2
        r SEXPIRE volatile 100000 MEMBERS 1 m1
        assert_equal 1 [get_keys_with_volatile_items r]

        assert_equal $is_before [r MEMORY USAGE plain_is]
        assert_equal $lp_before [r MEMORY USAGE plain_lp]

        # A set built the same way now costs the same as the one built before.
        r SADD later_is 1 2 3 4 5
        r SADD later_lp a b c d e
        assert_equal $is_before [r MEMORY USAGE later_is]
        assert_equal $lp_before [r MEMORY USAGE later_lp]
    }
}

# AOF rewrite and reload. A volatile member is rewritten as
# SADDEX key PXAT <ms> MEMBERS 1 member, a plain member as SADD.

tags {"aof external:skip"} {
    foreach rdb_preamble {"yes" "no"} {
        # start_server_aof reads $defaults from the caller's scope.
        set defaults {appendonly {yes} appendfilename {appendonly.aof} appenddirname {appendonlydir} auto-aof-rewrite-percentage {0}}
        set server_path [tmpdir server.setexpire.aof]
        start_server_aof [list dir $server_path aof-use-rdb-preamble $rdb_preamble] {
            set rdb_preamble [lindex [r config get aof-use-rdb-preamble] 1]
            test "Member TTLs survive AOF rewrite and restart, preamble $rdb_preamble" {
                r FLUSHALL
                r DEBUG SET-ACTIVE-EXPIRE 0
                r config set appendonly yes
                r config set appendfsync always
                assert_equal 0 [get_keys_with_volatile_items r]

                set long_expire [expr {[clock milliseconds] + 1000000000}]

                # 10 volatile members with a long TTL -> 10 PXAT
                for {set i 1} {$i <= 10} {incr i} {
                    r SADDEX myset PXAT $long_expire MEMBERS 1 m$i
                }
                # 10 volatile members with a short TTL -> 10 PXAT
                for {set i 11} {$i <= 20} {incr i} {
                    r SADDEX myset PX 20 MEMBERS 1 m$i
                }
                # 10 members expired on the spot -> 10 SREM
                for {set i 21} {$i <= 30} {incr i} {
                    r SADD myset m$i
                    r SEXPIRE myset 0 MEMBERS 1 m$i
                }
                # 10 plain members -> SADD only
                for {set i 31} {$i <= 40} {incr i} {
                    r SADD myset m$i
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
                validate_aof_content [get_last_incr_aof_path r] 20 10 SREM

                restart_server 0 true false
                r DEBUG SET-ACTIVE-EXPIRE 0
                r debug loadaof

                # The members with a long TTL keep their absolute stamp.
                for {set i 1} {$i <= 10} {incr i} {
                    assert_equal $long_expire [set_member_pexpiretime r myset m$i]
                    assert_equal 1 [r SISMEMBER myset m$i]
                }
                # Every member with a short or already-past expiry is gone or invisible.
                for {set i 11} {$i <= 30} {incr i} {
                    assert_equal -2 [set_member_ttl r myset m$i]
                    assert_equal 0 [r SISMEMBER myset m$i]
                }
                # The plain members are back with no TTL.
                for {set i 31} {$i <= 40} {incr i} {
                    assert_equal -1 [set_member_ttl r myset m$i]
                    assert_equal 1 [r SISMEMBER myset m$i]
                }
                assert_equal 1 [get_keys_with_volatile_items r]

                # A second rewrite drops the SREMs and keeps only live TTLs.
                r BGREWRITEAOF
                waitForBgrewriteaof r
                if {"$rdb_preamble" eq "no"} {
                    validate_aof_content [get_base_aof_path r] 10 0 SREM
                }

                restart_server 0 true false
                r DEBUG SET-ACTIVE-EXPIRE 0
                r debug loadaof

                assert_equal 20 [r SCARD myset]
                for {set i 1} {$i <= 10} {incr i} {
                    assert_equal $long_expire [set_member_pexpiretime r myset m$i]
                }
                for {set i 31} {$i <= 40} {incr i} {
                    assert_equal -1 [set_member_ttl r myset m$i]
                }
                assert_equal 1 [get_keys_with_volatile_items r]

                r DEBUG SET-ACTIVE-EXPIRE 1
            } {OK} {needs:debug}
        }
    }
}

# Active expiry: no access needed, key deletion, INFO counter, keyspace
# notifications, encoding names, memory.

start_server {tags {"setexpire external:skip"}} {
    test {Active expiry removes members with no access at all} {
        r FLUSHALL
        enable_active_expiry r
        set initial_expired [get_expired_set_members r]

        r SADD myset m1 m2 keepme
        r SPEXPIRE myset 50 MEMBERS 2 m1 m2
        assert_equal 1 [get_keys_with_volatile_items r]

        # No read touches the members: the cycle active expiration must do the work.
        wait_for_set_active_expiry r myset 1 $initial_expired 2
        assert_equal {keepme} [r SMEMBERS myset]
        assert_equal 1 [r EXISTS myset]
        assert_equal 0 [get_keys_with_volatile_items r]
    }

    test {Active expiry deletes the key when the last member expires} {
        r FLUSHALL
        enable_active_expiry r
        set initial_expired [get_expired_set_members r]

        r SADDEX myset PX 50 MEMBERS 2 m1 m2
        wait_for_condition 100 100 {
            [r EXISTS myset] == 0
        } else {
            fail "key was not deleted when its last member expired"
        }
        assert_equal 0 [r SCARD myset]
        assert_equal [expr {$initial_expired + 2}] [get_expired_set_members r]
        assert_equal 0 [get_keys_with_volatile_items r]
        assert_equal 0 [get_keyspace_field r keys 9]
    }

    test {expired_set_members counts set members only, not hash fields} {
        r FLUSHALL
        enable_active_expiry r
        set initial_set [get_expired_set_members r]
        set initial_hash [info_field [r info stats] expired_fields]

        r SADDEX myset PX 50 MEMBERS 3 m1 m2 m3
        wait_for_condition 100 100 {
            [r EXISTS myset] == 0
        } else {
            fail "set was not reclaimed"
        }

        assert_equal [expr {$initial_set + 3}] [get_expired_set_members r]
        # Set members have their own counter: expired_fields stays untouched.
        assert_equal $initial_hash [info_field [r info stats] expired_fields]
    }

    test {Active expiry emits sexpired then del} {
        r FLUSHALL
        enable_active_expiry r
        set initial_expired [get_expired_set_members r]
        set rd [setup_single_keyspace_notification r]

        r SADD myset m1
        r SEXPIRE myset 1 MEMBERS 1 m1

        wait_for_condition 100 100 {
            [r EXISTS myset] == 0
        } else {
            fail "key was not deleted by active expiry"
        }

        # Active expiration emits sexpired under NOTIFY_EXPIRED and then del for the
        # emptied key, mirroring hset / hexpire / hexpired / del for hashes.
        assert_keyevent_patterns $rd myset sadd sexpire sexpired del
        assert_morethan [get_expired_set_members r] $initial_expired
        $rd close
    }

    test {Active expiry emits sexpired without del while members remain} {
        r FLUSHALL
        enable_active_expiry r
        set rd [setup_single_keyspace_notification r]

        r SADD myset m1 keepme
        r SEXPIRE myset 1 MEMBERS 1 m1

        wait_for_condition 100 100 {
            [r SCARD myset] == 1
        } else {
            fail "member was not reclaimed"
        }
        assert_keyevent_patterns $rd myset sadd sexpire sexpired
        assert_equal 1 [r EXISTS myset]
        $rd close
    }

    test {OBJECT ENCODING names are unchanged by member TTLs} {
        r FLUSHALL
        r DEBUG SET-ACTIVE-EXPIRE 0
        r config set set-max-listpack-entries 128
        r config set set-max-intset-entries 512

        r SADD intish 1 2 3
        assert_encoding intset intish
        r SADD lpish a b c
        assert_encoding listpack lpish

        # An intset has nowhere to store a deadline, so a TTL converts it; both
        # sets fit the listpack thresholds. OBJECT ENCODING keeps reporting one
        # of the names that existed before member expiration.
        r SEXPIRE intish 5000 MEMBERS 1 1
        assert_encoding listpack intish

        r SEXPIRE lpish 5000 MEMBERS 1 a
        assert_encoding listpack lpish

        r config set set-max-listpack-entries 0
        r SADD htish a b c
        assert_encoding hashtable htish
        r SEXPIRE htish 5000 MEMBERS 1 a
        assert_encoding hashtable htish

        r config set set-max-listpack-entries 128
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    test {MEMORY USAGE of a volatile set exceeds a plain set with the same members} {
        r FLUSHALL
        r DEBUG SET-ACTIVE-EXPIRE 0
        r config set set-max-listpack-entries 0

        set members {}
        for {set i 0} {$i < 64} {incr i} {
            lappend members "member-with-a-long-name-$i"
        }
        r SADD plain {*}$members
        r SADD volat {*}$members
        assert_encoding hashtable plain
        assert_encoding hashtable volat

        set plain_mem [r MEMORY USAGE plain]
        r SEXPIRE volat 100000 MEMBERS 64 {*}$members
        set volat_mem [r MEMORY USAGE volat]

        # The expiry prefix costs extra bytes per volatile member plus a
        # per-key header. Only the direction is contractual.
        assert_morethan $volat_mem $plain_mem

        # SPERSIST on every member must give the memory back: a member that
        # loses its last TTL is rebuilt without the expiry prefix.
        r SPERSIST volat MEMBERS 64 {*}$members
        assert_equal 0 [get_keys_with_volatile_items r]
        assert_morethan $volat_mem [r MEMORY USAGE volat]

        r config set set-max-listpack-entries 128
        r DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}

    test {Active expiry keeps working after the key is deleted and re-created} {
        r FLUSHALL
        enable_active_expiry r
        r SADDEX myset PX 50 MEMBERS 1 m1
        wait_for_condition 100 100 {
            [r EXISTS myset] == 0
        } else {
            fail "first generation of the key was not reclaimed"
        }

        set initial_expired [get_expired_set_members r]
        r SADD myset survivor
        r SADDEX myset PX 50 MEMBERS 1 doomed
        wait_for_set_active_expiry r myset 1 $initial_expired 1
        assert_equal {survivor} [r SMEMBERS myset]

        # Overwrite the whole key with a plain set: the tracking entry must go.
        r SADDEX myset PX 100000 MEMBERS 1 later
        assert_equal 1 [get_keys_with_volatile_items r]
        r DEL myset
        assert_equal 0 [get_keys_with_volatile_items r]
        r SADD myset plainmember
        assert_equal 0 [get_keys_with_volatile_items r]
    }
}

# SREM / SPERSIST / S*EXPIRE take a member LIST and treat an expired member in
# it as missing, so what they propagate must name only the members they really
# changed and must drop the NX|XX|GT|LT condition they evaluated against the
# primary's own live view.

# Seed $key with a live member 'live' carrying a long TTL and an unreclaimed
# expired member 'expired'. The caller must have switched active expiry off.
proc seed_expired_member {r key} {
    $r DEL $key
    $r SADDEX $key PX 100000 MEMBERS 1 live
    $r SADDEX $key PX 20 MEMBERS 1 expired
    wait_for_condition 50 100 {
        [$r SISMEMBER $key expired] == 0 && [$r SCARD $key] == 2
    } else {
        fail "$key was not seeded with an unreclaimed expired member"
    }
}

foreach encoding {listpack hashtable} {
    start_server {tags {"setexpire external:skip"}} {
        if {$encoding eq "hashtable"} {
            r config set set-max-listpack-entries 0
        } else {
            r config set set-max-listpack-entries 128
        }

        test "SREM propagates only the members it removed ($encoding)" {
            r FLUSHALL
            r DEBUG SET-ACTIVE-EXPIRE 0
            seed_expired_member r kr
            assert_encoding $encoding kr

            set repl [attach_to_replication_stream]
            assert_equal 1 [r SREM kr expired live]

            assert_replication_stream $repl {
                {select *}
                {srem kr live}
            }
            close_replication_stream $repl

            # The expired member is left to active expiration, so the key survives
            # with it and SCARD keeps counting it.
            assert_equal 1 [r EXISTS kr]
            assert_equal 1 [r SCARD kr]
            assert_equal {} [r SMEMBERS kr]
            r DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}

        test "SREM of an expired member alone propagates nothing ($encoding)" {
            r FLUSHALL
            r DEBUG SET-ACTIVE-EXPIRE 0
            seed_expired_member r kr

            set repl [attach_to_replication_stream]
            assert_equal 0 [r SREM kr expired]

            assert_equal {} [read_from_replication_stream $repl]
            close_replication_stream $repl
            assert_equal 2 [r SCARD kr]
            r DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}

        test "SPERSIST propagates only the members it changed ($encoding)" {
            r FLUSHALL
            r DEBUG SET-ACTIVE-EXPIRE 0
            seed_expired_member r kp
            assert_encoding $encoding kp

            set repl [attach_to_replication_stream]
            assert_equal {-2 1} [r SPERSIST kp MEMBERS 2 expired live]

            assert_replication_stream $repl {
                {select *}
                {spersist kp MEMBERS 1 live}
            }
            close_replication_stream $repl

            assert_equal -1 [set_member_pttl r kp live]
            assert_equal -2 [set_member_pttl r kp expired]
            r DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}

        test "SPEXPIRE XX drops both the condition and the expired member ($encoding)" {
            r FLUSHALL
            r DEBUG SET-ACTIVE-EXPIRE 0
            seed_expired_member r kx
            assert_encoding $encoding kx

            set repl [attach_to_replication_stream]
            assert_equal {-2 1} [r SPEXPIRE kx 30000 XX MEMBERS 2 expired live]
            set at_ms [set_member_pexpiretime r kx live]

            # XX was already evaluated here against a view the replica does not
            # share, so only the resulting absolute stamp and the one member
            # that got it may travel.
            assert_replication_stream $repl [subst {
                {select *}
                {spexpireat kx $at_ms MEMBERS 1 live}
            }]
            close_replication_stream $repl
            r DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}

        test "SEXPIRE NX propagates only the member it set ($encoding)" {
            r FLUSHALL
            r DEBUG SET-ACTIVE-EXPIRE 0
            r SADD kn hasttl nottl
            r SEXPIRE kn 500 MEMBERS 1 hasttl
            assert_encoding $encoding kn

            set repl [attach_to_replication_stream]
            # NX passes only for the member that has no TTL yet.
            assert_equal {0 1} [r SEXPIRE kn 100 NX MEMBERS 2 hasttl nottl]
            set at_ms [set_member_pexpiretime r kn nottl]

            assert_replication_stream $repl [subst {
                {select *}
                {spexpireat kn $at_ms MEMBERS 1 nottl}
            }]
            close_replication_stream $repl

            assert_morethan [set_member_ttl r kn hasttl] 400
            r DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}

        test "SPEXPIREAT in the past propagates a filtered SREM ($encoding)" {
            r FLUSHALL
            r DEBUG SET-ACTIVE-EXPIRE 0
            seed_expired_member r kd

            set repl [attach_to_replication_stream]
            assert_equal {-2 2} [r SPEXPIREAT kd 1 MEMBERS 2 expired live]

            assert_replication_stream $repl {
                {select *}
                {srem kd live}
            }
            close_replication_stream $repl

            assert_equal 1 [r EXISTS kd]
            assert_equal 1 [r SCARD kd]
            r DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}
    }
}

start_server {tags {"repl external:skip"}} {
    start_server {tags {"repl external:skip"}} {
        set primary [srv -1 client]
        set primary_host [srv -1 host]
        set primary_port [srv -1 port]
        set replica [srv 0 client]

        test {SREM, SPERSIST and SPEXPIRE leave no expired member behind on the replica} {
            $primary FLUSHALL
            $primary DEBUG SET-ACTIVE-EXPIRE 0
            $replica DEBUG SET-ACTIVE-EXPIRE 0
            attach_replica $primary $replica $primary_host $primary_port
            set replica_log_lines [count_log_lines 0]

            foreach key {kr kp kx} {
                $primary SADDEX $key PX 100000 MEMBERS 1 live
                $primary SADDEX $key PX 20 MEMBERS 1 expired
            }
            wait_for_condition 50 100 {
                [$primary SISMEMBER kr expired] == 0 &&
                [$primary SISMEMBER kp expired] == 0 &&
                [$primary SISMEMBER kx expired] == 0
            } else {
                fail "the members did not expire on the primary"
            }

            assert_equal 1 [$primary SREM kr expired live]
            assert_equal {-2 1} [$primary SPERSIST kp MEMBERS 2 expired live]
            assert_equal {-2 1} [$primary SPEXPIRE kx 30000 XX MEMBERS 2 expired live]
            wait_for_ofs_sync $primary $replica

            # Each command touched only the live member, so the expired one
            # must still be expired on both sides: same key, same cardinality
            # (which counts it) and same members.
            foreach key {kr kp kx} {
                assert_equal [$primary EXISTS $key] [$replica EXISTS $key]
                assert_equal [$primary SCARD $key] [$replica SCARD $key]
                assert_equal [$primary DEBUG DIGEST-VALUE $key] [$replica DEBUG DIGEST-VALUE $key]
                # The digest folds the members only, so compare the deadlines
                # too: a replica that re-evaluated a relative time or a
                # condition would differ. -2 for the expired member and -1 for
                # a persisted one must agree the same way.
                assert_equal [$primary SPEXPIRETIME $key MEMBERS 2 expired live] \
                    [$replica SPEXPIRETIME $key MEMBERS 2 expired live] \
                    "SPEXPIRETIME $key"
            }
            assert_equal -1 [set_member_pexpiretime $primary kp live]
            assert_morethan [set_member_pexpiretime $primary kx live] [clock milliseconds]

            # A replica that had dropped kr entirely answers the next RENAME
            # with "-ERR no such key", which the primary logs as a CRITICAL
            # replica error.
            $primary RENAME kr kr2
            wait_for_ofs_sync $primary $replica
            assert_equal 1 [$replica EXISTS kr2]
            verify_no_log_message 0 "*sending an error to its primary*" $replica_log_lines

            # Promote the replica: it owns expiry now, so the expired members
            # must be reclaimable there instead of surviving as live or permanent
            # members.
            $replica replicaof no one
            wait_for_condition 100 100 {
                [info_field [$replica info replication] role] eq "master"
            } else {
                fail "Replica didn't become master"
            }
            $replica DEBUG SET-ACTIVE-EXPIRE 1
            wait_for_condition 100 100 {
                [$replica SCARD kr2] == 0 && [$replica SCARD kp] == 1 && [$replica SCARD kx] == 1
            } else {
                fail "promoted replica did not reclaim: kr2 [$replica SCARD kr2] kp [$replica SCARD kp] kx [$replica SCARD kx]"
            }
            foreach key {kr2 kp kx} {
                assert_equal 0 [$replica SISMEMBER $key expired]
                assert_equal -2 [set_member_pttl $replica $key expired]
            }
            # SPERSIST really did drop the TTL of the live member only.
            assert_equal 1 [$replica SISMEMBER kp live]
            assert_equal -1 [set_member_pttl $replica kp live]
            assert_morethan [set_member_pttl $replica kx live] 0
            assert_equal {PONG} [$replica PING]

            $primary DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}
    }
}
