# Semantics of SEXPIRE, SPEXPIRE, SEXPIREAT, SPEXPIREAT, STTL, SPTTL,
# SEXPIRETIME, SPEXPIRETIME, SPERSIST and SADDEX.

# TTL argument in the unit and frame of reference of the given command or
# SADDEX expiration token.
#
# The EXAT margin is +2s and not +1s because [clock seconds] truncates: at
# X.99s a +1 stamp is only ~10ms in the future, so any scheduling stall makes
# it already past by the time the server runs the command, which takes the
# immediate-expiry path instead.
proc get_long_set_expire_value {command} {
    switch -exact -- $command {
        SEXPIRE - EX { return 100000 }
        SPEXPIRE - PX { return 100000000 }
        SEXPIREAT - EXAT { return [expr {[clock seconds] + 100000}] }
        SPEXPIREAT - PXAT { return [expr {[clock milliseconds] + 100000000}] }
    }
    error "get_long_set_expire_value: unknown command $command"
}

# 0 for the relative forms, a long-past stamp for the absolute forms.
proc get_past_set_expire_value {command} {
    switch -exact -- $command {
        SEXPIRE - EX { return 0 }
        SPEXPIRE - PX { return 0 }
        SEXPIREAT - EXAT { return [expr {[clock seconds] - 200000}] }
        SPEXPIREAT - PXAT { return [expr {[clock milliseconds] - 200000}] }
    }
    error "get_past_set_expire_value: unknown command $command"
}

# The reader that reports back in the same unit and frame as the setter.
proc get_set_ttl_reader {command} {
    switch -exact -- $command {
        SEXPIRE - EX { return STTL }
        SPEXPIRE - PX { return SPTTL }
        SEXPIREAT - EXAT { return SEXPIRETIME }
        SPEXPIREAT - PXAT { return SPEXPIRETIME }
    }
    error "get_set_ttl_reader: unknown command $command"
}

proc create_test_set {key members} {
    r del $key
    r sadd $key {*}$members
}

start_server {tags {"setexpire"}
    overrides {
        "set-max-intset-entries" 512
        "set-max-listpack-entries" 128
        "set-max-listpack-value" 64
    }
} {
    # SEXPIRE reply codes and the option matrix.
    test {SEXPIRE - reply 1 when the TTL is applied} {
        r FLUSHALL
        create_test_set myset {a b c}
        assert_equal {1} [r SEXPIRE myset 100000 MEMBERS 1 a]
        assert_equal 1 [get_keys_with_volatile_items r]
        assert_equal {1 1 1} [r SEXPIRE myset 100000 MEMBERS 3 a b c]
    }

    test {SEXPIRE - reply -2 for a missing key} {
        r FLUSHALL
        assert_equal {-2} [r SEXPIRE nosuchkey 100000 MEMBERS 1 a]
        assert_equal {-2 -2} [r SEXPIRE nosuchkey 100000 MEMBERS 2 a b]
        # A TTL command must not create the key.
        assert_equal 0 [r EXISTS nosuchkey]
    }

    test {SEXPIRE - reply -2 for a missing member of an existing key} {
        r FLUSHALL
        create_test_set myset {a b c}
        assert_equal {-2} [r SEXPIRE myset 100000 MEMBERS 1 nomember]
        assert_equal {1 -2} [r SEXPIRE myset 100000 MEMBERS 2 a nomember]
        # A -2 member must not be added to the set.
        assert_equal 0 [r SISMEMBER myset nomember]
        assert_equal 3 [r SCARD myset]
    }

    test {SEXPIRE - NX applies only to a member without a TTL} {
        r FLUSHALL
        create_test_set myset {a b}
        assert_equal {1} [r SEXPIRE myset 100000 MEMBERS 1 a]
        # a already has a TTL, so NX is not met and replies 0; b has none.
        assert_equal {0 1} [r SEXPIRE myset 100000 NX MEMBERS 2 a b]
    }

    test {SEXPIRE - XX applies only to a member that has a TTL} {
        r FLUSHALL
        create_test_set myset {a b}
        assert_equal {1} [r SEXPIRE myset 100000 MEMBERS 1 a]
        assert_equal {1 0} [r SEXPIRE myset 200000 XX MEMBERS 2 a b]
        # b was rejected, so it must still have no TTL.
        assert_equal {-1} [r STTL myset MEMBERS 1 b]
    }

    test {SEXPIRE - GT applies only a greater expiration} {
        r FLUSHALL
        create_test_set myset {a}
        assert_equal {1} [r SEXPIRE myset 100000 MEMBERS 1 a]
        set original [set_member_pexpiretime r myset a]
        assert_equal {0} [r SEXPIRE myset 1 GT MEMBERS 1 a]
        assert_equal $original [set_member_pexpiretime r myset a]
        assert_equal {1} [r SEXPIRE myset 200000 GT MEMBERS 1 a]
        assert_morethan [set_member_pexpiretime r myset a] $original
    }

    test {SEXPIRE - LT applies only a smaller expiration} {
        r FLUSHALL
        create_test_set myset {a}
        assert_equal {1} [r SEXPIRE myset 100000 MEMBERS 1 a]
        set original [set_member_pexpiretime r myset a]
        assert_equal {0} [r SEXPIRE myset 200000 LT MEMBERS 1 a]
        assert_equal $original [set_member_pexpiretime r myset a]
        assert_equal {1} [r SEXPIRE myset 1 LT MEMBERS 1 a]
        assert_lessthan [set_member_pexpiretime r myset a] $original
    }

    test {SEXPIRE - GT and LT treat a member with no TTL as infinite} {
        r FLUSHALL
        create_test_set myset {a b}
        assert_equal {0} [r SEXPIRE myset 100000 GT MEMBERS 1 a]
        assert_equal {-1} [r STTL myset MEMBERS 1 a]
        assert_equal {1} [r SEXPIRE myset 100000 LT MEMBERS 1 b]
        assert_morethan [set_member_ttl r myset b] 0
    }

    test {SEXPIRE - reply 2 and member removal for a time in the past} {
        r FLUSHALL
        create_test_set myset {a b c}
        assert_equal {2} [r SEXPIRE myset 0 MEMBERS 1 a]
        assert_equal 0 [r SISMEMBER myset a]
        assert_equal {-2} [r STTL myset MEMBERS 1 a]
        assert_equal 1 [r EXISTS myset]
        assert_equal {b c} [lsort [r SMEMBERS myset]]
    }

    test {SEXPIRE - the key is deleted when the last member is removed this way} {
        r FLUSHALL
        create_test_set myset {a}
        assert_equal {2} [r SEXPIRE myset 0 MEMBERS 1 a]
        assert_equal 0 [r EXISTS myset]
        assert_equal 0 [get_keys_with_volatile_items r]

        create_test_set myset {a b c}
        assert_equal {2 2 2} [r SEXPIRE myset 0 MEMBERS 3 a b c]
        assert_equal 0 [r EXISTS myset]
    }

    test {SEXPIRE - a negative relative TTL is rejected} {
        r FLUSHALL
        create_test_set myset {a b}
        # A negative relative TTL is rejected by the shared
        # convertExpireArgumentToUnixTime(), exactly as for HEXPIRE, and the
        # set is left untouched.
        assert_error "*invalid expire time*" {r SEXPIRE myset -10 MEMBERS 1 a}
        assert_equal 1 [r SISMEMBER myset a]
        assert_error "*invalid expire time*" {r SPEXPIRE myset -1 MEMBERS 1 b}
        assert_equal 1 [r EXISTS myset]
    }

    test {SEXPIRE - duplicate members get one reply element each} {
        r FLUSHALL
        create_test_set myset {a}
        assert_equal {1 1} [r SEXPIRE myset 100000 MEMBERS 2 a a]
    }

    test {SEXPIRE - WRONGTYPE against a non-set key} {
        r FLUSHALL
        r SET mystr hello
        r LPUSH mylist a
        r HSET myhash f v
        r ZADD myzset 1 a
        assert_error "WRONGTYPE*" {r SEXPIRE mystr 100000 MEMBERS 1 a}
        assert_error "WRONGTYPE*" {r SEXPIRE mylist 100000 MEMBERS 1 a}
        assert_error "WRONGTYPE*" {r SEXPIRE myhash 100000 MEMBERS 1 f}
        assert_error "WRONGTYPE*" {r SEXPIRE myzset 100000 MEMBERS 1 a}
    }

    test {SEXPIRE - MEMBERS count validation} {
        r FLUSHALL
        create_test_set myset {a b c}
        # The count check and its message mirror HEXPIRE's numfields check.
        assert_error "*greater than 0 and match the provided number*" \
            {r SEXPIRE myset 100000 MEMBERS 2 a}
        assert_error "*greater than 0 and match the provided number*" \
            {r SEXPIRE myset 100000 MEMBERS 1 a b}
        assert_error "*greater than 0 and match the provided number*" \
            {r SEXPIRE myset 100000 MEMBERS 0 a}
        assert_error "*greater than 0 and match the provided number*" \
            {r SEXPIRE myset 100000 MEMBERS -1 a}
        assert_error "*not an integer or out of range*" \
            {r SEXPIRE myset 100000 MEMBERS notanumber a}
    }

    # The three remaining writers: one unit-conversion test each. They share
    # SEXPIRE's option handling, and differ only in the unit and frame of the
    # expiration argument.
    test {SPEXPIRE - the argument is relative milliseconds} {
        r FLUSHALL
        create_test_set myset {a b}
        set now_ms [clock milliseconds]
        assert_equal {1} [r SPEXPIRE myset 100000 MEMBERS 1 a]
        assert_range [set_member_pexpiretime r myset a] \
            [expr {$now_ms + 95000}] [expr {$now_ms + 100001}]
        assert_range [set_member_ttl r myset a] 95 100
        assert_equal {2} [r SPEXPIRE myset 0 MEMBERS 1 b]
        assert_equal 0 [r SISMEMBER myset b]
    }

    test {SEXPIREAT - the argument is an absolute unix time in seconds} {
        r FLUSHALL
        create_test_set myset {a b}
        set at_s [expr {[clock seconds] + 100}]
        assert_equal {1} [r SEXPIREAT myset $at_s MEMBERS 1 a]
        assert_equal $at_s [lindex [r SEXPIRETIME myset MEMBERS 1 a] 0]
        assert_equal [expr {$at_s * 1000}] [set_member_pexpiretime r myset a]
        # A stamp in the past removes the member instead of storing a TTL.
        assert_equal {2} [r SEXPIREAT myset [expr {[clock seconds] - 200000}] MEMBERS 1 b]
        assert_equal 0 [r SISMEMBER myset b]
    }

    test {SPEXPIREAT - the argument is an absolute unix time in milliseconds} {
        r FLUSHALL
        create_test_set myset {a b}
        set at_ms [expr {[clock milliseconds] + 100000}]
        assert_equal {1} [r SPEXPIREAT myset $at_ms MEMBERS 1 a]
        assert_equal $at_ms [set_member_pexpiretime r myset a]
        assert_equal {2} [r SPEXPIREAT myset [expr {[clock milliseconds] - 200000}] MEMBERS 1 b]
        assert_equal 0 [r SISMEMBER myset b]
    }

    # STTL, SPTTL, SEXPIRETIME and SPEXPIRETIME.
    foreach reader {STTL SPTTL SEXPIRETIME SPEXPIRETIME} {
        test "$reader - -2 for a missing key and a missing member" {
            r FLUSHALL
            assert_equal {-2} [r $reader nosuchkey MEMBERS 1 a]
            create_test_set myset {a}
            assert_equal {-2} [r $reader myset MEMBERS 1 nomember]
            assert_equal {-2 -2} [r $reader myset MEMBERS 2 nomember othermember]
        }

        test "$reader - -1 for a member without a TTL" {
            r FLUSHALL
            create_test_set myset {a b}
            assert_equal {-1} [r $reader myset MEMBERS 1 a]
            assert_equal {-1 -1} [r $reader myset MEMBERS 2 a b]
        }

        test "$reader - mixed -2, -1 and value replies keep member order" {
            r FLUSHALL
            create_test_set myset {a b}
            assert_equal {1} [r SEXPIRE myset 100000 MEMBERS 1 a]
            set res [r $reader myset MEMBERS 3 a b nomember]
            assert_equal 3 [llength $res]
            assert_morethan [lindex $res 0] 0
            assert_equal -1 [lindex $res 1]
            assert_equal -2 [lindex $res 2]
        }
    }

    test {STTL and SPTTL - value ranges after a relative expiration} {
        r FLUSHALL
        create_test_set myset {a b}
        assert_equal {1} [r SEXPIRE myset 100 MEMBERS 1 a]
        assert_range [set_member_ttl r myset a] 90 100
        assert_range [set_member_pttl r myset a] 90000 100000
        assert_equal {1} [r SPEXPIRE myset 100000 MEMBERS 1 b]
        assert_range [set_member_ttl r myset b] 90 100
        assert_range [set_member_pttl r myset b] 90000 100000
    }

    test {SEXPIRETIME and SPEXPIRETIME - the second reply is the rounded millisecond stamp} {
        r FLUSHALL
        create_test_set myset {a}
        # A .7s stamp, so the rounded second differs from the truncated one.
        set at_ms [expr {([clock milliseconds] + 100000) / 1000 * 1000 + 700}]
        assert_equal {1} [r SPEXPIREAT myset $at_ms MEMBERS 1 a]
        assert_equal $at_ms [set_member_pexpiretime r myset a]
        # SEXPIRETIME rounds to the nearest second, as HEXPIRETIME does.
        assert_equal [expr {$at_ms / 1000 + 1}] [lindex [r SEXPIRETIME myset MEMBERS 1 a] 0]
    }

    test {SEXPIRETIME - a relative expiration is reported as an absolute stamp} {
        r FLUSHALL
        create_test_set myset {a}
        set now_s [clock seconds]
        assert_equal {1} [r SEXPIRE myset 100 MEMBERS 1 a]
        # The stamp is never earlier than the 100s asked for; the upper bound
        # allows the second to tick over between the two reads.
        assert_range [lindex [r SEXPIRETIME myset MEMBERS 1 a] 0] \
            [expr {$now_s + 100}] [expr {$now_s + 101}]
    }

    test {STTL - a partial second is rounded to the nearest, as HTTL does} {
        r FLUSHALL
        create_test_set myset {a}
        # 1.9s, so the rounded second differs from the truncated one.
        assert_equal {1} [r SPEXPIRE myset 1900 MEMBERS 1 a]
        assert_equal 2 [set_member_ttl r myset a]
        assert_range [set_member_pttl r myset a] 1500 1900
    }

    test {STTL family - WRONGTYPE against a non-set key} {
        r FLUSHALL
        r SET mystr hello
        foreach reader {STTL SPTTL SEXPIRETIME SPEXPIRETIME} {
            assert_error "WRONGTYPE*" {r $reader mystr MEMBERS 1 a}
        }
    }

    test {STTL family - MEMBERS count validation} {
        r FLUSHALL
        create_test_set myset {a b}
        foreach reader {STTL SPTTL SEXPIRETIME SPEXPIRETIME} {
            # The read-only family parses MEMBERS through the generic syntax
            # path, so a count mismatch is a plain syntax error, exactly as
            # HTTL reports one. The count-specific message belongs to the
            # write family, which mirrors HEXPIRE.
            assert_error "*ERR syntax error" {r $reader myset MEMBERS 2 a}
            assert_error "*ERR syntax error" {r $reader myset MEMBERS 1 a b}
            assert_error "*ERR syntax error" {r $reader myset MEMBERS 0 a}
            assert_error "*ERR syntax error" {r $reader myset MEMBERS -2 a}
            assert_error "*ERR syntax error" {r $reader myset FIELDS 1 a}
            assert_error "*not an integer or out of range" {r $reader myset MEMBERS a b}
        }
    }

    # SPERSIST.
    test {SPERSIST - -2 for a missing key and a missing member} {
        r FLUSHALL
        assert_equal {-2} [r SPERSIST nosuchkey MEMBERS 1 a]
        create_test_set myset {a}
        assert_equal {-2} [r SPERSIST myset MEMBERS 1 nomember]
    }

    test {SPERSIST - -1 for a member without a TTL} {
        r FLUSHALL
        create_test_set myset {a}
        assert_equal {-1} [r SPERSIST myset MEMBERS 1 a]
    }

    test {SPERSIST - 1 when a TTL is removed and the readers report -1 afterwards} {
        r FLUSHALL
        create_test_set myset {a b c}
        assert_equal {1 1} [r SEXPIRE myset 100000 MEMBERS 2 a b]
        assert_equal {1} [r SPERSIST myset MEMBERS 1 a]
        assert_equal {-1} [r STTL myset MEMBERS 1 a]
        assert_equal {-1} [r SPTTL myset MEMBERS 1 a]
        assert_equal {-1} [r SEXPIRETIME myset MEMBERS 1 a]
        # The member itself and b's TTL are untouched.
        assert_morethan [set_member_ttl r myset b] 0
        assert_equal 1 [r SISMEMBER myset a]
        assert_equal 3 [r SCARD myset]
        # A second call finds nothing to remove.
        assert_equal {-1} [r SPERSIST myset MEMBERS 1 a]
    }

    test {SPERSIST - mixed replies keep member order} {
        r FLUSHALL
        create_test_set myset {a b}
        assert_equal {1} [r SEXPIRE myset 100000 MEMBERS 1 a]
        assert_equal {1 -1 -2} [r SPERSIST myset MEMBERS 3 a b nomember]
    }

    test {SPERSIST - the key stops being tracked when its last TTL is removed} {
        r FLUSHALL
        create_test_set myset {a b}
        assert_equal {1 1} [r SEXPIRE myset 100000 MEMBERS 2 a b]
        assert_equal 1 [get_keys_with_volatile_items r]
        assert_equal {1} [r SPERSIST myset MEMBERS 1 a]
        assert_equal 1 [get_keys_with_volatile_items r]
        assert_equal {1} [r SPERSIST myset MEMBERS 1 b]
        assert_equal 0 [get_keys_with_volatile_items r]
    }

    test {SPERSIST - WRONGTYPE and argument errors} {
        r FLUSHALL
        r SET mystr hello
        create_test_set myset {a b}
        assert_error "WRONGTYPE*" {r SPERSIST mystr MEMBERS 1 a}
        assert_error "*ERR syntax error" {r SPERSIST myset a 1 b}
        assert_error "*not an integer or out of range" {r SPERSIST myset MEMBERS a b}
        assert_error "*greater than 0 and match the provided number*" \
            {r SPERSIST myset MEMBERS 3 a b}
        assert_error "*greater than 0 and match the provided number*" \
            {r SPERSIST myset MEMBERS 0 a}
    }

    # SADDEX. The full option matrix runs on EX; PX, EXAT and PXAT get one
    # unit-conversion test each.
    test {SADDEX EX - the reply is the added count and the TTL is set} {
        r FLUSHALL
        assert_equal 2 [r SADDEX myset EX 100000 MEMBERS 2 a b]
        assert_equal {a b} [lsort [r SMEMBERS myset]]
        assert_morethan [set_member_ttl r myset a] 0
        assert_morethan [set_member_ttl r myset b] 0
        assert_equal 1 [get_keys_with_volatile_items r]
    }

    test {SADDEX EX - an existing member is not counted but its TTL is updated} {
        r FLUSHALL
        create_test_set myset {a}
        assert_equal 1 [r SADDEX myset EX 100000 MEMBERS 2 a b]
        assert_morethan [set_member_ttl r myset a] 0
        assert_morethan [set_member_ttl r myset b] 0
        assert_equal 0 [r SADDEX myset EX 200000 MEMBERS 2 a b]
        assert_morethan [set_member_ttl r myset a] 100000
    }

    test {SADDEX EX - an expiration in the past adds nothing that survives} {
        r FLUSHALL
        # The member is added and immediately removed, so it is not counted as
        # added and nothing of the key is left behind.
        assert_equal 0 [r SADDEX myset EX 0 MEMBERS 1 a]
        assert_equal 0 [r SISMEMBER myset a]
        assert_equal 0 [r EXISTS myset]
    }

    foreach expiration {PX EXAT PXAT} {
        test "SADDEX $expiration - the expiration is read back in its own unit and frame" {
            r FLUSHALL
            set value [get_long_set_expire_value $expiration]
            set reader [get_set_ttl_reader $expiration]
            assert_equal 1 [r SADDEX myset $expiration $value MEMBERS 1 a]
            if {[string match "*AT" $expiration]} {
                assert_equal $value [lindex [r $reader myset MEMBERS 1 a] 0]
            } else {
                assert_morethan [lindex [r $reader myset MEMBERS 1 a] 0] 0
            }
            # A value already in the past adds nothing that survives, so the
            # added count is 0.
            assert_equal 0 [r SADDEX myset $expiration \
                [get_past_set_expire_value $expiration] MEMBERS 1 b]
            assert_equal 0 [r SISMEMBER myset b]
        }
    }

    test {SADDEX - without an expiration token the members are plain} {
        r FLUSHALL
        # The expiration block is optional, so SADDEX with no expiration token
        # behaves like SADD.
        assert_equal 2 [r SADDEX myset MEMBERS 2 a b]
        assert_equal {-1 -1} [r STTL myset MEMBERS 2 a b]
        assert_equal 0 [get_keys_with_volatile_items r]
    }

    test {SADDEX KEEPTTL - keeps an existing member TTL and adds new members plain} {
        r FLUSHALL
        assert_equal 1 [r SADDEX myset PX 100000 MEMBERS 1 a]
        set original [set_member_pexpiretime r myset a]
        assert_equal 1 [r SADDEX myset KEEPTTL MEMBERS 2 a b]
        assert_equal $original [set_member_pexpiretime r myset a]
        assert_equal {-1} [r STTL myset MEMBERS 1 b]
    }

    foreach expiration {EX PX EXAT PXAT} {
        test "SADDEX MNX $expiration - all or nothing over the whole call" {
            r FLUSHALL
            create_test_set myset {a}
            set value [get_long_set_expire_value $expiration]
            # MNX is all-or-nothing over the whole call, like HSETEX FNX: if
            # any named member already exists, nothing is added, no TTL is
            # applied and the reply is 0.
            assert_equal 0 [r SADDEX myset MNX $expiration $value MEMBERS 2 a b]
            assert_equal {-1} [r STTL myset MEMBERS 1 a]
            assert_equal 0 [r SISMEMBER myset b]
            assert_equal {a} [r SMEMBERS myset]
            # Every named member is new, so all are added with the TTL, and
            # members that were not named stay untouched.
            assert_equal 2 [r SADDEX myset MNX $expiration $value MEMBERS 2 c d]
            assert_equal {a c d} [lsort [r SMEMBERS myset]]
            assert_morethan [set_member_ttl r myset c] 0
            assert_morethan [set_member_ttl r myset d] 0
            assert_equal {-1} [r STTL myset MEMBERS 1 a]
        }

        test "SADDEX MXX $expiration - all or nothing over the whole call" {
            r FLUSHALL
            create_test_set myset {a}
            set value [get_long_set_expire_value $expiration]
            # MXX is all-or-nothing too, like HSETEX FXX: if any named member
            # is missing, nothing is added or set and the reply is 0.
            assert_equal 0 [r SADDEX myset MXX $expiration $value MEMBERS 2 a b]
            assert_equal {-1} [r STTL myset MEMBERS 1 a]
            assert_equal 0 [r SISMEMBER myset b]
            assert_equal {a} [r SMEMBERS myset]
            # Every named member exists, so nothing is added but all of them
            # get the TTL.
            r SADD myset b
            assert_equal 0 [r SADDEX myset MXX $expiration $value MEMBERS 2 a b]
            assert_equal {a b} [lsort [r SMEMBERS myset]]
            assert_morethan [set_member_ttl r myset a] 0
            assert_morethan [set_member_ttl r myset b] 0
        }
    }

    test {SADDEX NX - the key condition is honoured} {
        r FLUSHALL
        assert_equal 1 [r SADDEX myset NX EX 100000 MEMBERS 1 a]
        assert_equal 1 [r EXISTS myset]
        # Key NX on an existing key rejects the whole command, as HSETEX does.
        assert_equal 0 [r SADDEX myset NX EX 100000 MEMBERS 1 b]
        assert_equal 0 [r SISMEMBER myset b]
        assert_equal 1 [r SCARD myset]
    }

    test {SADDEX XX - the key condition is honoured} {
        r FLUSHALL
        # Key XX on a missing key replies 0 and does not create the key.
        assert_equal 0 [r SADDEX myset XX EX 100000 MEMBERS 1 a]
        assert_equal 0 [r EXISTS myset]
        create_test_set myset {a}
        assert_equal 1 [r SADDEX myset XX EX 100000 MEMBERS 1 b]
        assert_morethan [set_member_ttl r myset b] 0
    }

    test {SADDEX - WRONGTYPE and argument errors} {
        r FLUSHALL
        r SET mystr hello
        assert_error "WRONGTYPE*" {r SADDEX mystr EX 100 MEMBERS 1 a}
        assert_error "ERR syntax error" {r SADDEX myset EX 100 PX 1000 MEMBERS 1 a}
        assert_error "ERR syntax error" {r SADDEX myset MNX MXX EX 100 MEMBERS 1 a}
        assert_error "ERR syntax error" {r SADDEX myset NX XX EX 100 MEMBERS 1 a}
        assert_error "*ERR syntax error" {r SADDEX myset EX 100 FIELDS 1 a}
        assert_error "*greater than 0 and match the provided number*" \
            {r SADDEX myset EX 100 MEMBERS 2 a}
        assert_error "*greater than 0 and match the provided number*" \
            {r SADDEX myset EX 100 MEMBERS 0 a}
        assert_error "*not an integer or out of range" {r SADDEX myset EX notanumber MEMBERS 1 a}
        assert_error "*invalid expire time*" {r SADDEX myset EX 9999999999999999 MEMBERS 1 a}
    }

    # Error paths shared by the whole command family.
    test {Set TTL commands - unknown and conflicting tokens} {
        r FLUSHALL
        create_test_set myset {a}
        foreach command {SEXPIRE SPEXPIRE SEXPIREAT SPEXPIREAT} {
            assert_error "ERR Unsupported option BOGUS" {r $command myset 100 BOGUS MEMBERS 1 a}
            assert_error "ERR NX and XX, GT or LT options at the same time are not compatible" \
                {r $command myset 100 NX XX MEMBERS 1 a}
            assert_error "ERR GT and LT options at the same time are not compatible" \
                {r $command myset 100 GT LT MEMBERS 1 a}
        }
    }

    test {Set TTL commands - MEMBERS token missing} {
        r FLUSHALL
        create_test_set myset {a}
        foreach command {SEXPIRE SPEXPIRE SEXPIREAT SPEXPIREAT} {
            assert_error "ERR wrong number of arguments for '[string tolower $command]' command" \
                {r $command myset 100 1 a}
            assert_error "ERR nummembers should be greater than 0 and match the provided number of members" \
                {r $command myset 100 FIELDS 1 a}
        }
        foreach command {STTL SPTTL SEXPIRETIME SPEXPIRETIME SPERSIST} {
            assert_error "ERR wrong number of arguments for '[string tolower $command]' command" \
                {r $command myset 1 a}
        }
        assert_error "ERR syntax error" {r SADDEX myset EX 100 1 a}
    }

    test {Set TTL commands - wrong number of arguments} {
        r FLUSHALL
        create_test_set myset {a}
        foreach command {SEXPIRE SPEXPIRE SEXPIREAT SPEXPIREAT STTL SPTTL
                         SEXPIRETIME SPEXPIRETIME SPERSIST SADDEX} {
            assert_error "*wrong number of arguments*" {r $command}
            assert_error "*wrong number of arguments*" {r $command myset}
        }
    }

    test {Set TTL commands - expiration value out of range} {
        r FLUSHALL
        create_test_set myset {a}
        # The overflow guard and its message mirror HEXPIRE's. A value that
        # does not fit int64 never reaches the guard, in hashes either: it
        # fails integer parsing first.
        assert_error "*invalid expire time*" {r SEXPIRE myset 9223372036854775 MEMBERS 1 a}
        assert_error "*invalid expire time*" {r SPEXPIRE myset 9223372036854775807 MEMBERS 1 a}
        assert_error "*invalid expire time*" {r SEXPIREAT myset 9223372036854775807 MEMBERS 1 a}
        # SPEXPIREAT takes an absolute millisecond stamp, so no int64 value
        # overflows its guard: int64 max is accepted, as HPEXPIREAT accepts it.
        assert_equal {1} [r SPEXPIREAT myset 9223372036854775807 MEMBERS 1 a]
        assert_error "*not an integer or out of range*" {r SEXPIRE myset 9999999999999999999 MEMBERS 1 a}
        assert_error "*not an integer or out of range*" {r SPEXPIREAT myset 9999999999999999999 MEMBERS 1 a}
        assert_error "*not an integer or out of range*" {r SEXPIRE myset notanumber MEMBERS 1 a}
        assert_error "*not an integer or out of range*" {r SEXPIRE myset 1.5 MEMBERS 1 a}
    }
}

# The encoding of a set that holds a TTL: applying, updating and dropping a
# member TTL must never change it. Conversion out of intset, which a TTL does
# force, is covered in tests/unit/setexpire-setops.tcl.
start_server {tags {"setexpire"}
    overrides {
        "set-max-intset-entries" 512
        "set-max-listpack-entries" 128
        "set-max-listpack-value" 64
    }
} {
    foreach encoding {listpack hashtable} {
        test "A member TTL does not change a $encoding set's encoding" {
            r FLUSHALL
            use_set_encoding $encoding
            create_test_set myset {a b c}
            assert_encoding $encoding myset
            assert_equal {1} [r SEXPIRE myset 100000 MEMBERS 1 a]
            assert_encoding $encoding myset
            assert_equal {1} [r SEXPIRE myset 200000 MEMBERS 1 a]
            assert_encoding $encoding myset
            assert_equal {1} [r SPERSIST myset MEMBERS 1 a]
            assert_encoding $encoding myset
            assert_equal 1 [r SADDEX myset EX 100000 MEMBERS 1 d]
            assert_encoding $encoding myset
            assert_equal {a b c d} [lsort [r SMEMBERS myset]]
        }
    }
}

# An expired member is one whose TTL has passed while active expiry has not
# removed it yet. Active expiry is disabled so that state is reachable, and
# these tests assert what must hold while it lasts.
start_server {tags {"setexpire"}
    overrides {
        "set-max-intset-entries" 512
        "set-max-listpack-entries" 128
        "set-max-listpack-value" 64
    }
} {
    foreach encoding {listpack hashtable} {
        test "SADD over an expired member returns 1 and clears the TTL - $encoding" {
            r FLUSHALL
            r DEBUG SET-ACTIVE-EXPIRE 0
            use_set_encoding $encoding
            create_test_set myset {gone live1}
            make_members_expired r myset {gone}
            assert_equal 1 [r SADD myset gone]
            assert_equal {-1} [r STTL myset MEMBERS 1 gone]
            assert_equal 1 [r SISMEMBER myset gone]
            assert_equal {gone live1} [lsort [r SMEMBERS myset]]
            # The expired member was replaced, not duplicated.
            assert_equal 2 [r SCARD myset]
            assert_encoding $encoding myset
            r DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}

        test "SADD over a live volatile member returns 0 and keeps the TTL - $encoding" {
            r FLUSHALL
            r DEBUG SET-ACTIVE-EXPIRE 0
            use_set_encoding $encoding
            create_test_set myset {a}
            assert_equal {1} [r SEXPIRE myset 100000 MEMBERS 1 a]
            set original [set_member_pexpiretime r myset a]
            assert_equal 0 [r SADD myset a]
            assert_equal $original [set_member_pexpiretime r myset a]
            # A multi-member SADD must not disturb the TTL either.
            assert_equal 1 [r SADD myset a b]
            assert_equal $original [set_member_pexpiretime r myset a]
            assert_equal {-1} [r STTL myset MEMBERS 1 b]
            r DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}

        test "SADDEX KEEPTTL over an expired member gives a plain member - $encoding" {
            r FLUSHALL
            r DEBUG SET-ACTIVE-EXPIRE 0
            use_set_encoding $encoding
            create_test_set myset {gone}
            make_members_expired r myset {gone}
            # The expired member is dropped first, so KEEPTTL has no TTL to
            # keep and the member ends up plain.
            assert_equal 1 [r SADDEX myset KEEPTTL MEMBERS 1 gone]
            assert_equal {-1} [r STTL myset MEMBERS 1 gone]
            assert_equal 1 [r SISMEMBER myset gone]
            r DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}

        test "SADDEX MNX over an expired member treats it as absent - $encoding" {
            r FLUSHALL
            r DEBUG SET-ACTIVE-EXPIRE 0
            use_set_encoding $encoding
            create_test_set myset {gone live1}
            make_members_expired r myset {gone}
            # An expired member is not a member, so MNX is met and the call
            # adds it back with the new TTL.
            assert_equal 1 [r SADDEX myset MNX EX 100000 MEMBERS 1 gone]
            assert_equal 1 [r SISMEMBER myset gone]
            assert_morethan [set_member_ttl r myset gone] 0
            assert_equal 2 [r SCARD myset]
            # live1 exists, so MNX now rejects the whole call.
            assert_equal 0 [r SADDEX myset MNX EX 100000 MEMBERS 2 gone live1]
            r DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}

        test "SADDEX MXX over an expired member treats it as absent - $encoding" {
            r FLUSHALL
            r DEBUG SET-ACTIVE-EXPIRE 0
            use_set_encoding $encoding
            create_test_set myset {gone live1}
            make_members_expired r myset {gone}
            # MXX needs every named member to exist, and the expired one does
            # not, so the whole call is rejected and live1 keeps no TTL.
            assert_equal 0 [r SADDEX myset MXX EX 100000 MEMBERS 2 gone live1]
            assert_equal 0 [r SISMEMBER myset gone]
            assert_equal {-1} [r STTL myset MEMBERS 1 live1]
            # Only live members named: the TTL is applied.
            assert_equal 0 [r SADDEX myset MXX EX 100000 MEMBERS 1 live1]
            assert_morethan [set_member_ttl r myset live1] 0
            r DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}

        test "SEXPIRE on an expired member replies -2 and does not revive it - $encoding" {
            r FLUSHALL
            r DEBUG SET-ACTIVE-EXPIRE 0
            use_set_encoding $encoding
            create_test_set myset {gone live1}
            make_members_expired r myset {gone}
            assert_equal {-2} [r SEXPIRE myset 100000 MEMBERS 1 gone]
            assert_equal 0 [r SISMEMBER myset gone]
            r DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}

        test "A set of nothing but expired members is still reported by EXISTS - $encoding" {
            r FLUSHALL
            r DEBUG SET-ACTIVE-EXPIRE 0
            use_set_encoding $encoding
            create_test_set myset {g1 g2}
            make_members_expired r myset {g1 g2}
            assert_equal 1 [r EXISTS myset]
            assert_equal {} [r SMEMBERS myset]
            assert_equal {0 0} [r SMISMEMBER myset g1 g2]
            assert_equal {} [r SRANDMEMBER myset 5]
            # A negative count must not loop forever looking for a live member.
            assert_equal {} [r SRANDMEMBER myset -5]
            assert_equal {} [r SPOP myset 5]
            r DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}

        test "SADD over an expired member emits sexpired then sadd and counts the expiry - $encoding" {
            r FLUSHALL
            r config resetstat
            r DEBUG SET-ACTIVE-EXPIRE 0
            use_set_encoding $encoding
            create_test_set myset {gone live1}
            make_members_expired r myset {gone}
            set rd [setup_single_keyspace_notification r]
            assert_equal 1 [r SADD myset gone]
            # The expiry is observed and propagated before the add.
            assert_keyevent_patterns $rd myset sexpired sadd
            assert_equal 1 [get_expired_set_members r]
            $rd close
            r DEBUG SET-ACTIVE-EXPIRE 1
        } {OK} {needs:debug}
    }
}

# Keyspace notifications and INFO stats. Neither depends on the encoding.
start_server {tags {"setexpire"}
    overrides {
        "set-max-intset-entries" 512
        "set-max-listpack-entries" 128
        "set-max-listpack-value" 64
    }
} {
    test {SEXPIRE emits a single sexpire notification} {
        r FLUSHALL
        create_test_set myset {a b c}
        set rd [setup_single_keyspace_notification r]
        r SEXPIRE myset 100000 MEMBERS 3 a b c
        assert_keyevent_patterns $rd myset sexpire
        # Nothing else may be pending: the next event is the marker.
        r SADD dummy dummy
        assert_keyevent_patterns $rd dummy sadd
        $rd close
    }

    test {SEXPIRE on a missing member emits nothing} {
        r FLUSHALL
        create_test_set myset {a}
        set rd [setup_single_keyspace_notification r]
        r SEXPIRE myset 100000 MEMBERS 1 nomember
        r SADD dummy dummy
        assert_keyevent_patterns $rd dummy sadd
        $rd close
    }

    test {A past expiration emits sexpired and del for every writer} {
        foreach command {SEXPIRE SPEXPIRE SEXPIREAT SPEXPIREAT} {
            r FLUSHALL
            r config resetstat
            create_test_set myset {a}
            set rd [setup_single_keyspace_notification r]
            assert_equal {2} [r $command myset [get_past_set_expire_value $command] MEMBERS 1 a]
            assert_keyevent_patterns $rd myset sexpired del
            assert_equal 0 [r EXISTS myset]
            assert_equal 1 [get_expired_set_members r]
            $rd close
        }
    }

    test {SPERSIST emits an spersist notification} {
        r FLUSHALL
        create_test_set myset {a}
        r SEXPIRE myset 100000 MEMBERS 1 a
        set rd [setup_single_keyspace_notification r]
        r SPERSIST myset MEMBERS 1 a
        assert_keyevent_patterns $rd myset spersist
        $rd close
    }

    test {SPERSIST on a member without a TTL emits nothing} {
        r FLUSHALL
        create_test_set myset {a}
        set rd [setup_single_keyspace_notification r]
        r SPERSIST myset MEMBERS 1 a
        r SADD dummy dummy
        assert_keyevent_patterns $rd dummy sadd
        $rd close
    }

    test {SADDEX emits sadd and sexpire} {
        r FLUSHALL
        set rd [setup_single_keyspace_notification r]
        r SADDEX myset EX 100000 MEMBERS 1 a
        # The member is added before its TTL is applied, mirroring HSETEX.
        assert_keyevent_patterns $rd myset sadd sexpire
        $rd close
    }

    test {Active expiry emits sexpired and increments expired_set_members} {
        r FLUSHALL
        r config resetstat
        create_test_set myset {a live1}
        set rd [setup_single_keyspace_notification r]
        r SPEXPIRE myset 50 MEMBERS 1 a
        assert_keyevent_patterns $rd myset sexpire
        wait_for_set_active_expiry r myset 1 0 1
        assert_keyevent_patterns $rd myset sexpired
        assert_equal 0 [r SISMEMBER myset a]
        $rd close
    }

    test {Active expiry of the last member deletes the key} {
        r FLUSHALL
        r config resetstat
        create_test_set myset {a}
        set rd [setup_single_keyspace_notification r]
        r SPEXPIRE myset 50 MEMBERS 1 a
        assert_keyevent_patterns $rd myset sexpire
        assert_keyevent_patterns $rd myset sexpired del
        wait_for_condition 100 100 {
            [r EXISTS myset] == 0
        } else {
            fail "active expiry did not delete the key"
        }
        assert_equal 1 [get_expired_set_members r]
        assert_equal 0 [get_keys_with_volatile_items r]
        $rd close
    }

    test {expired_set_members counts every member, not every key} {
        r FLUSHALL
        r config resetstat
        create_test_set myset {a b c live1}
        assert_equal 0 [get_expired_set_members r]
        r SPEXPIRE myset 50 MEMBERS 3 a b c
        wait_for_set_active_expiry r myset 1 0 3
        # The hash counter must not be touched by set expiry.
        assert_equal 0 [info_field [r info stats] expired_fields]
        assert_equal {live1} [r SMEMBERS myset]
    }

    r config set notify-keyspace-events ""
}
