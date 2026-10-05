# D+ Phase 1 deterministic correctness battery (S5).
# Requires an instrumented build (-DIO_LOOKUP_OFFLOAD_STATS).
#
# DECOMPOSITION NOTE: a live command-driven mutation can NEVER land inside a
# held reader's window -- the parked worker freezes main at the
# waitForClientIO rendezvous (verified: zero serverCron heartbeats during a
# 2s hold), so every mutation serializes after the reader wakes. The paired
# protocol is therefore proven in two independent pillars:
#
#   Pillar 1 (writer side): every mutation path bumps its key's shard version
#   by exactly one even bracket (+2) -- probed directly with
#   DEBUG dplus-shard-version, no timing involved.
#
#   Pillar 2 (reader side): a version change between a reader's copy and its
#   validation forces a punt -- an odd/even bracket is injected at the exact
#   preemption point via DEBUG dplus-prevalidate-hold <ms> bump.
#
#   Together: mutation => bracket (pillar 1); bracket during window => punt
#   (pillar 2). The end-to-end composition under real concurrency is covered
#   by the T-RACE-* stress tests below (nondeterministic interleavings,
#   coherence asserted on every reply) and the ASan/TSan legs.

proc dplus_field {r field} {
    set payload [$r info dplus]
    if {![regexp "${field}:(\\d+)" $payload -> value]} {
        fail "missing $field in INFO dplus"
    }
    return $value
}

proc shard_version {r key} {
    return [lindex [$r debug dplus-shard-version $key] 1]
}

# A deferring client that sends nothing before its first command: a SELECT would move
# it off the fast path, and only fast-path clients execute reads on their IO thread.
proc dplus_client {} {
    return [valkey_deferring_client_by_addr [srv 0 host] [srv 0 port]]
}

# Pump pipelined GETs until the speculative path demonstrably engages for this client.
proc ensure_engaged {r rd key} {
    if {!$::dplus_instrumented} {
        # No hit counter to verify against -- pump enough for ownership
        # handoff and proceed; the race tests remain valid coherence checks.
        for {set i 0} {$i < 30} {incr i} { $rd get $key }
        for {set i 0} {$i < 30} {incr i} { $rd read }
        return
    }
    set before [dplus_field $r dplus_speculative_hits]
    for {set round 0} {$round < 20} {incr round} {
        for {set i 0} {$i < 10} {incr i} { $rd get $key }
        for {set i 0} {$i < 10} {incr i} { $rd read }
        if {[dplus_field $r dplus_speculative_hits] > $before} return
    }
    fail "speculative path never engaged for test client"
}

start_server {tags {"dplus-correctness"} overrides {io-threads 4 save {}}} {
    r select 0 ;# the same db as the fast-path clients

    # Instrumented-build probe: the prevalidate hold exists only with
    # -DIO_LOOKUP_OFFLOAD_STATS. P2 hold tests are guarded on it; P1 probes
    # and the race/parity/rehash tests run everywhere.
    set ::dplus_instrumented 1
    if {[catch {r debug dplus-prevalidate-hold 1} e]} {
        if {[string match "*instrumented*" $e]} { set ::dplus_instrumented 0 }
    }
    after 20 ;# a 1ms probe arm expires harmlessly if set

    # The P1 checks assert exact version deltas, and a resize of the keyspace table brackets every
    # shard. The cron resizes the table once the P1 keys fill it, so resizing stays off while they run.
    r debug dict-resizing 0

    test {P1-BRACKET-SET-INSERT: fresh SET brackets its shard (+even, +2 min)} {
        r set p1:a v1
        set v [shard_version r p1:a]
        assert {$v % 2 == 0 && $v >= 2}
    }

    test {P1-BRACKET-SET-OVERWRITE: overwrite SET brackets (+2, stays even)} {
        r set p1:b v1
        set v0 [shard_version r p1:b]
        r set p1:b v2
        assert_equal [expr {$v0 + 2}] [shard_version r p1:b]
    }

    test {P1-BRACKET-SET-INPLACE-SWAP: non-embstr refcount-1 overwrite (dbSetValue swap branch)} {
        r set p1:c [string repeat A 64]
        set v0 [shard_version r p1:c]
        r set p1:c [string repeat B 64]
        assert_equal [expr {$v0 + 2}] [shard_version r p1:c]
    }

    test {P1-BRACKET-FIRST-EXPIRE: first-time EXPIRE (shell realloc) brackets} {
        r set p1:d vd
        set v0 [shard_version r p1:d]
        r expire p1:d 1000
        assert_equal [expr {$v0 + 2}] [shard_version r p1:d]
        assert_equal 1000 [r ttl p1:d]
    }

    test {P1-BRACKET-TTL-UPDATE: existing-TTL update (in-place 8-byte) brackets} {
        r set p1:e ve ex 500
        set v0 [shard_version r p1:e]
        r expire p1:e 900
        assert_equal [expr {$v0 + 2}] [shard_version r p1:e]
    }

    test {P1-BRACKET-PERSIST: removeExpire in-place clear brackets} {
        r set p1:f vf ex 500
        set v0 [shard_version r p1:f]
        r persist p1:f
        # PERSIST = expires-table pop + in-place expiry clear; both bracket
        # the same shard: pop is main-private, the value bracket is +2.
        set v1 [shard_version r p1:f]
        assert {$v1 % 2 == 0 && $v1 > $v0}
    }

    test {P1-BRACKET-INCR: tagged-int in-place INCR brackets} {
        r set p1:g 41
        set v0 [shard_version r p1:g]
        r incr p1:g
        assert_equal [expr {$v0 + 2}] [shard_version r p1:g]
        assert_equal 42 [r get p1:g]
    }

    test {P1-BRACKET-APPEND: in-place and relocating APPEND both bracket} {
        r set p1:h [string repeat x 8]
        set v0 [shard_version r p1:h]
        r append p1:h yy                     ;# within capacity: in-place
        set v1 [shard_version r p1:h]
        assert {$v1 % 2 == 0 && $v1 > $v0}
        r append p1:h [string repeat z 4096] ;# forces replacement publication
        set v2 [shard_version r p1:h]
        assert {$v2 % 2 == 0 && $v2 > $v1}
        assert_equal [expr {8 + 2 + 4096}] [r strlen p1:h]
    }

    test {P1-BRACKET-SETRANGE: grow + memcpy brackets; content correct} {
        r set p1:i hello
        set v0 [shard_version r p1:i]
        r setrange p1:i 2 XYZ
        set v1 [shard_version r p1:i]
        assert {$v1 % 2 == 0 && $v1 > $v0}
        assert_equal "heXYZ" [r get p1:i]
        r setrange p1:i 4000 TAIL            ;# forces growth via replacement
        set v2 [shard_version r p1:i]
        assert {$v2 % 2 == 0 && $v2 > $v1}
        assert_equal 4004 [r strlen p1:i]
    }

    test {P1-BRACKET-DEL: delete brackets the shard} {
        r set p1:j vj
        set v0 [shard_version r p1:j]
        r del p1:j
        r set p1:j vj2                       ;# re-insert to probe the shard
        set v1 [shard_version r p1:j]
        assert {$v1 % 2 == 0 && $v1 >= $v0 + 4} ;# del bracket + insert bracket
    }

    r debug dict-resizing 1

    if {$::dplus_instrumented} {
    test {P2-PUNT-INJECTED-BRACKET: version change inside the copy-validate window forces punt} {
        r set p2:a stable
        set rd [dplus_client]
        ensure_engaged r $rd p2:a
        set before_miss [dplus_field r dplus_validation_misses]
        # Arm hold+bump: reader copies 'stable', parks 50ms, an odd/even
        # bracket fires on its shard at the preemption point, validation
        # must fail, the whole burst punts to main.
        r debug dplus-prevalidate-hold 50 bump
        for {set i 0} {$i < 4} {incr i} { $rd get p2:a }
        for {set i 0} {$i < 4} {incr i} { assert_equal "stable" [$rd read] }
        wait_for_condition 100 50 {
            [dplus_field r dplus_validation_misses] > $before_miss
        } else {
            fail "injected bracket did not force a validation punt"
        }
        $rd close
    }

    test {P2-CLEAN-HOLD-CONTROL: hold without bump validates clean (no false punts)} {
        r set p2:b steady
        set rd [dplus_client]
        ensure_engaged r $rd p2:b
        set before_miss [dplus_field r dplus_validation_misses]
        set before_hits [dplus_field r dplus_speculative_hits]
        r debug dplus-prevalidate-hold 50
        for {set i 0} {$i < 4} {incr i} { $rd get p2:b }
        for {set i 0} {$i < 4} {incr i} { assert_equal "steady" [$rd read] }
        assert_equal $before_miss [dplus_field r dplus_validation_misses]
        wait_for_condition 100 50 {
            [dplus_field r dplus_speculative_hits] > $before_hits
        } else {
            fail "clean held read did not complete speculatively"
        }
        $rd close
    }
    } ;# end dplus_instrumented guard

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

    test {T-PARITY-REST: versions even at rest -- speculation still engages after mixed load} {
        for {set i 0} {$i < 500} {incr i} {
            r set p:$i v$i
            r expire p:$i 1000
            r incr ctr
            r append astr xx
            r del p:$i
        }
        r set final fval
        set before_hits 0
        if {$::dplus_instrumented} { set before_hits [dplus_field r dplus_speculative_hits] }
        set rd [dplus_client]
        for {set i 0} {$i < 50} {incr i} { $rd get final }
        for {set i 0} {$i < 50} {incr i} { assert_equal "fval" [$rd read] }
        $rd close
        if {$::dplus_instrumented} {
            wait_for_condition 100 50 {
                [dplus_field r dplus_speculative_hits] > $before_hits
            } else {
                fail "speculation dead after mixed load -- parity residue (missed bracket?)"
            }
        } else {
            # Uninstrumented: parity residue is still observable indirectly --
            # an odd shard makes every read of 'final' punt, which the
            # even-at-rest probe catches:
            set v [lindex [r debug dplus-shard-version final] 1]
            assert {$v % 2 == 0}
        }
    }

    test {T-REHASH-WINDOW: reads racing active rehash punt or serve coherently, never crash} {
        r flushall
        for {set i 0} {$i < 20000} {incr i} { r set rh:$i v$i }
        set rd [dplus_client]
        for {set round 0} {$round < 20} {incr round} {
            for {set i 0} {$i < 200} {incr i} { $rd get rh:[expr {$round * 200 + $i}] }
            for {set i 0} {$i < 200} {incr i} { $rd read }
            $rd get rh:19999
            for {set i 0} {$i < 500} {incr i} { r del rh:[expr {$round * 500 + $i}] }
            $rd read
        }
        $rd close
        assert_equal "PONG" [r ping]
    }
}
