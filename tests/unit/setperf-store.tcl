# Propagation contracts for the STORE forms of the set operations when a source
# set carries member TTLs: SUNIONSTORE / SINTERSTORE / SDIFFSTORE and
# SORT ... STORE.
#
# Requires a server built with `make WORK_COUNTERS=yes`; counters are read
# through DEBUG WORKCTR (helpers in tests/support/workctr.tcl). Budgets come
# from the parent algorithm (the `none` fixture runs the parent paths unchanged)
# and from source inspection, never from the measured commit.
#
# Source of the contracts (src/t_set.c sinterGenericCommand /
# sunionDiffGenericCommand, src/sort.c sortCommandGeneric, src/db.c
# propagateStoreAsEffects):
#   volatile_source = dstkey && setTypeHasVolatileMembers(src) -- true while ANY
#   source member carries a TTL, expired or not. When set, the head replaces the
#   verbatim command with DEL/UNLINK dst plus the WHOLE result re-emitted as
#   SADD (RPUSH for SORT STORE) in batches of 1024 arguments, each member copied
#   into a fresh robj.
#
# Contract S1 (future TTLs only): effects are needed only when a source holds an
#   expired-but-unreclaimed member, because only then would the replica recompute
#   over a member the primary did not see. With every TTL in the future the
#   replica recomputes the identical result, so the propagation of a STORE whose
#   sources hold only future TTLs must stay the verbatim command: no result-sized
#   argv, no result-sized member copies, no extra pass over the result.
#   The narrowest safe condition is "this command hid an expired member", not
#   "a source has a TTL": the residual risk in the second case is a TTL elapsing
#   between primary execution and replica application, which is the same
#   replication-lag window key-level expiry already lives with and which a
#   24h-future TTL cannot enter. Either refinement leaves the cost measured here
#   unjustified, so the contract does not depend on which one is chosen.
# Contract S2 (no consumer): with no replica and AOF off, alsoPropagate drops
#   every command (shouldPropagate false, counter prop_cmds_dropped). Building
#   effects nobody consumes is pure waste, so the number of dropped
#   alsoPropagate calls must not grow with the result size either.
# Contract S3 (expired members present): effects ARE legitimate. The bound is
#   then one copy of the result (plus the legitimate SREM propagation of members
#   actually reclaimed), and the replica's dst must equal the primary's dst --
#   the correctness requirement any cheaper propagation must also satisfy.
#
# Instrumentation note: DEBUG WORKCTR ARM snapshots after afterCommand() (see
# src/server.c call()), so the command's own alsoPropagate, the pending
# propagation flush and the AOF/replication feed ARE in the ARM window. The
# replica and AOF blocks below still use wc_window (whole client round trip,
# coarser but equivalent for these contracts).

proc sto_inter {a b} {
    array set sto_h {}
    foreach x $b { set sto_h($x) 1 }
    set out {}
    foreach x $a { if {[info exists sto_h($x)]} { lappend out $x } }
    return $out
}

proc sto_diff {a b} {
    array set sto_h {}
    foreach x $b { set sto_h($x) 1 }
    set out {}
    foreach x $a { if {![info exists sto_h($x)]} { lappend out $x } }
    return $out
}

# Expected result of one operation over the LIVE members of A and the plain B.
proc sto_expected {kind a b} {
    switch -- $kind {
        union { return [lsort -unique [concat $a $b]] }
        inter { return [sto_inter $a $b] }
        diff { return [sto_diff $a $b] }
        source { return $a }
    }
    error "unknown result kind $kind"
}

# Members traversed by the command. propagateStoreAsEffects iterates the result
# once more, which shows up here for a set destination. A list destination
# (SORT ... STORE) is iterated with listTypeNext, which has no counter: for
# those commands the extra pass is observed through the member copies instead.
proc sto_traversed {d} {
    wc_sum $d set_iter_next ht_iter_visits
}

proc sto_payload_bytes {members} {
    set s 0
    foreach m $members { incr s [string length $m] }
    return $s
}

proc sto_dst_size {dsttype key {client ""}} {
    if {$client eq ""} { set client [srv 0 client] }
    if {$dsttype eq "set"} { return [$client scard $key] }
    return [$client llen $key]
}

proc sto_dst_members {dsttype key {client ""}} {
    if {$client eq ""} { set client [srv 0 client] }
    if {$dsttype eq "set"} { return [$client smembers $key] }
    return [$client lrange $key 0 -1]
}

# label / command template / result kind / destination type
set ::sto_store_cmds {
    {"SUNIONSTORE dst A B" {sunionstore sto:dst $sto_ka $sto_kb} union set}
    {"SINTERSTORE dst A B" {sinterstore sto:dst $sto_ka $sto_kb} inter set}
    {"SDIFFSTORE dst A B" {sdiffstore sto:dst $sto_ka $sto_kb} diff set}
    {"SORT A ALPHA STORE dst" {sort $sto_ka ALPHA STORE sto:dst} source list}
    {"SORT A BY nosort STORE dst" {sort $sto_ka BY nosort STORE sto:dst} source list}
}

# ---------------------------------------------------------------------------
# No consumer: no replica attached, AOF off. Every propagated command is
# dropped, so any effects construction here is entirely wasted work.
# ---------------------------------------------------------------------------
start_server {tags {"setperf set external:skip needs:debug"}} {
    if {![wc_available]} {
        test "setperf-store: skipped, server lacks WORK_COUNTERS" {
            skip "build with: make WORK_COUNTERS=yes"
        }
    } else {
    set sto_saved [wc_quiesce]

    foreach sto_spec $::sto_store_cmds {
        lassign $sto_spec sto_label sto_tpl sto_kind sto_dsttype
        test "setperf-store: $sto_label, a future TTL must not rebuild the result as propagation effects" {
            set sto_ka sto:a
            set sto_kb sto:b
            foreach n {2000 20000} {
                # B is a plain set overlapping A by half.
                set sto_bmem [wc_members $n short [expr {$n / 2}]]
                r del $sto_kb
                wc_batched [srv 0 client] sadd $sto_kb $sto_bmem
                foreach ttl {none one} {
                    set sto_live [wc_fixture $sto_ka $n $ttl]
                    assert_equal [dict get [wc_setinfo $sto_ka] encoding] hashtable
                    set sto_cmd($ttl) [subst -nocommands $sto_tpl]
                    set sto_d($ttl) [wc_measure "r $sto_cmd($ttl)"]
                    set sto_exp [sto_expected $sto_kind $sto_live $sto_bmem]
                    # A fast result is only a result if it is the right one.
                    assert_equal $::wc_last_reply [llength $sto_exp]
                    assert_equal [sto_dst_size $sto_dsttype sto:dst] [llength $sto_exp]
                    if {$n == 2000} {
                        assert_equal [lsort [sto_dst_members $sto_dsttype sto:dst]] [lsort $sto_exp]
                    }
                    wc_record $::cur_test $sto_cmd($ttl) \
                        [dict create n $n ttl $ttl result [llength $sto_exp] consumer none] $sto_d($ttl)
                }
                set sto_c $sto_cmd(one)
                set sto_extra "result=[llength $sto_exp] consumer=none"
                if {$::verbose} {
                    foreach ttl {none one} {
                        puts "no-consumer $sto_label n=$n ttl=$ttl result=[llength $sto_exp]: dropped=[wc_get $sto_d($ttl) prop_cmds_dropped] traversed=[sto_traversed $sto_d($ttl)] str_objs=[wc_get $sto_d($ttl) str_objs_created] str_obj_bytes=[wc_get $sto_d($ttl) str_obj_bytes] sds_copies=[wc_get $sto_d($ttl) sds_copies] prop_args=[wc_get $sto_d($ttl) prop_args] prop_arg_bytes=[wc_get $sto_d($ttl) prop_arg_bytes] mem_max_alloc=[wc_get $sto_d($ttl) mem_max_alloc] mem_peak_live_delta=[wc_get $sto_d($ttl) mem_peak_live_delta]"
                    }
                }
                # Contract S2: with no consumer the effects are dropped, so
                # constructing them cannot be justified by anything.
                wc_assert_ratio "alsoPropagate calls dropped (no consumer)" \
                    [wc_get $sto_d(one) prop_cmds_dropped] [wc_get $sto_d(none) prop_cmds_dropped] 1 1 \
                    "no consumer: a future TTL must not queue result-sized batches that are then dropped (n=$n)" \
                    $sto_c sto:a $sto_extra
                if {$sto_dsttype eq "set"} {
                    # `none` and `one` hold the same live members, so the parent
                    # traversal is identical; the only difference a future TTL
                    # can add is the extra pass over the result. Factor 1 (not
                    # 2) because a whole extra pass is exactly the regression:
                    # for SUNIONSTORE the parent already walks 2n and the extra
                    # pass is 1.5n, which a factor-2 bound would hide.
                    wc_assert_ratio "members traversed (set_iter_next+ht_iter_visits)" \
                        [sto_traversed $sto_d(one)] [sto_traversed $sto_d(none)] 1 256 \
                        "a future TTL must not add a pass over the result (n=$n)" $sto_c sto:a $sto_extra
                }
                wc_assert_ratio "string objects created" \
                    [wc_get $sto_d(one) str_objs_created] [wc_get $sto_d(none) str_objs_created] 2 16 \
                    "a future TTL must not copy every result member into a new robj (n=$n)" \
                    $sto_c sto:a $sto_extra
                # Contract S1: the queued argv is the command itself, so a
                # future TTL may add at most the DEL that a rewrite would need.
                # With no consumer alsoPropagate returns before counting argv,
                # so these two only bite in the replica / AOF blocks.
                wc_assert_ratio "payload bytes of queued argv" \
                    [wc_get $sto_d(one) prop_arg_bytes] [wc_get $sto_d(none) prop_arg_bytes] 2 256 \
                    "a future TTL must not queue a result-sized argv (n=$n)" $sto_c sto:a $sto_extra
                wc_assert_ratio "argv entries queued by alsoPropagate" \
                    [wc_get $sto_d(one) prop_args] [wc_get $sto_d(none) prop_args] 1 4 \
                    "a STORE whose sources hold only future TTLs propagates the command verbatim (n=$n)" \
                    $sto_c sto:a $sto_extra
            }
        } {} {slow}
    }

    test "setperf-store: SUNIONSTORE propagation work must not grow with the result size" {
        set sto_ka sto:a
        set sto_kb sto:b
        set sto_sizes [wc_ht_sizes]
        foreach ttl {none one} {
            set sto_dropped($ttl) {}
            set sto_objs($ttl) {}
        }
        foreach n $sto_sizes {
            set sto_bmem [wc_members $n short [expr {$n / 2}]]
            r del $sto_kb
            wc_batched [srv 0 client] sadd $sto_kb $sto_bmem
            foreach ttl {none one} {
                set sto_live [wc_fixture $sto_ka $n $ttl]
                set sto_d($ttl) [wc_measure {r sunionstore sto:dst $sto_ka $sto_kb}]
                set sto_exp [sto_expected union $sto_live $sto_bmem]
                assert_equal $::wc_last_reply [llength $sto_exp]
                lappend sto_dropped($ttl) [wc_get $sto_d($ttl) prop_cmds_dropped]
                lappend sto_objs($ttl) [wc_get $sto_d($ttl) str_objs_created]
                wc_record $::cur_test "sunionstore dst A B" \
                    [dict create n $n ttl $ttl result [llength $sto_exp] consumer none] $sto_d($ttl)
            }
        }
        if {$::verbose} {
            puts "no-consumer SUNIONSTORE sizes {$sto_sizes}: dropped none={$sto_dropped(none)} one={$sto_dropped(one)}; str_objs none={$sto_objs(none)} one={$sto_objs(one)}"
        }
        # Baseline sanity: the parent propagates one command whatever the size.
        wc_assert_flat "alsoPropagate calls dropped (plain sources)" $sto_sizes $sto_dropped(none) 1 1 \
            "the parent propagates one command per STORE" "sunionstore dst A B" sto:a
        wc_assert_flat "alsoPropagate calls dropped (one future TTL)" $sto_sizes $sto_dropped(one) 1 2 \
            "a future TTL must keep propagation request-sized, not result-sized" "sunionstore dst A B" sto:a
        wc_assert_flat "string objects created (one future TTL)" $sto_sizes $sto_objs(one) 2 64 \
            "a future TTL must not copy the whole result on every STORE" "sunionstore dst A B" sto:a
    } {} {slow}

    # Non-STORE forms take no dstkey, so volatile_source is never set and no
    # effects path exists. This pins that: the TTL must cost nothing here.
    foreach sto_spec {
        {"SUNION A B" {sunion $sto_ka $sto_kb} union list}
        {"SINTER A B" {sinter $sto_ka $sto_kb} inter list}
        {"SDIFF A B" {sdiff $sto_ka $sto_kb} diff list}
        {"SINTERCARD 2 A B" {sintercard 2 $sto_ka $sto_kb} inter count}
        {"SORT A ALPHA" {sort $sto_ka ALPHA} source list}
    } {
        lassign $sto_spec sto_label sto_tpl sto_kind sto_replytype
        test "setperf-store: $sto_label without a destination propagates nothing and costs no extra pass" {
            set n 20000
            set sto_ka sto:a
            set sto_kb sto:b
            set sto_bmem [wc_members $n short [expr {$n / 2}]]
            r del $sto_kb
            wc_batched [srv 0 client] sadd $sto_kb $sto_bmem
            foreach ttl {none one all} {
                set sto_live [wc_fixture $sto_ka $n $ttl]
                set sto_cmd($ttl) [subst -nocommands $sto_tpl]
                set sto_d($ttl) [wc_measure "r $sto_cmd($ttl)"]
                set sto_exp [sto_expected $sto_kind $sto_live $sto_bmem]
                if {$sto_replytype eq "count"} {
                    assert_equal $::wc_last_reply [llength $sto_exp]
                } else {
                    assert_equal [llength $::wc_last_reply] [llength $sto_exp]
                }
                # A read-only operation queues no propagation at all, dropped
                # or otherwise: no effects are built behind our back.
                assert_equal [wc_get $sto_d($ttl) prop_cmds] 0
                assert_equal [wc_get $sto_d($ttl) prop_cmds_dropped] 0
                wc_record $::cur_test $sto_cmd($ttl) \
                    [dict create n $n ttl $ttl result [llength $sto_exp] consumer none] $sto_d($ttl)
            }
            foreach ttl {one all} {
                wc_assert_ratio "members traversed (set_iter_next+ht_iter_visits)" \
                    [sto_traversed $sto_d($ttl)] [sto_traversed $sto_d(none)] 2 16 \
                    "TTLs on the sources must not add a traversal to a destination-less operation ($ttl)" \
                    $sto_cmd($ttl) sto:a
                wc_assert_ratio "string objects created" \
                    [wc_get $sto_d($ttl) str_objs_created] [wc_get $sto_d(none) str_objs_created] 2 16 \
                    "TTLs on the sources must not multiply member copies ($ttl)" $sto_cmd($ttl) sto:a
            }
        } {} {slow}
    }

    wc_restore $sto_saved
    }
}

# ---------------------------------------------------------------------------
# A real replica attached: propagation is consumed, so repl_bytes and
# prop_now_cmds become observable and consistency is checkable.
# ---------------------------------------------------------------------------
start_server {tags {"setperf set repl external:skip needs:debug"}} {
    if {![wc_available]} {
        test "setperf-store (replica): skipped, server lacks WORK_COUNTERS" {
            skip "build with: make WORK_COUNTERS=yes"
        }
    } else {
    start_server {} {
        set sto_primary [srv -1 client]
        set sto_replica [srv 0 client]
        $sto_replica replicaof [srv -1 host] [srv -1 port]
        wait_for_sync $sto_replica
        set sto_saved [wc_quiesce $sto_primary]
        $sto_replica debug set-active-expire 0

        foreach sto_spec $::sto_store_cmds {
            lassign $sto_spec sto_label sto_tpl sto_kind sto_dsttype
            test "setperf-store: $sto_label with a replica, a future TTL must not amplify the replication stream" {
                set n 20000
                set sto_ka sto:a
                set sto_kb sto:b
                set sto_bmem [wc_members $n short [expr {$n / 2}]]
                $sto_primary del $sto_kb
                wc_batched $sto_primary sadd $sto_kb $sto_bmem
                foreach ttl {none one} {
                    set sto_live [wc_fixture $sto_ka $n $ttl short 3 $sto_primary]
                    set sto_cmd($ttl) [subst -nocommands $sto_tpl]
                    # wc_window, not wc_measure: repl_bytes and prop_now_cmds
                    # are produced after the ARM snapshot point.
                    set sto_d($ttl) [wc_window "\$sto_primary $sto_cmd($ttl)" $sto_primary]
                    set sto_exp [sto_expected $sto_kind $sto_live $sto_bmem]
                    assert_equal [sto_dst_size $sto_dsttype sto:dst $sto_primary] [llength $sto_exp]
                    # Consistency: the replica must end up with the same dst,
                    # whichever propagation shape was chosen.
                    wait_for_ofs_sync $sto_primary $sto_replica
                    assert_equal [lsort [sto_dst_members $sto_dsttype sto:dst $sto_replica]] [lsort $sto_exp]
                    wc_record $::cur_test $sto_cmd($ttl) \
                        [dict create n $n ttl $ttl result [llength $sto_exp] consumer replica] $sto_d($ttl)
                }
                if {$::verbose} {
                    puts "$sto_label repl_bytes: none=[wc_get $sto_d(none) repl_bytes] one=[wc_get $sto_d(one) repl_bytes]; prop_now_cmds none=[wc_get $sto_d(none) prop_now_cmds] one=[wc_get $sto_d(one) prop_now_cmds]"
                }
                set sto_extra "result=[llength $sto_exp] consumer=replica"
                wc_assert_ratio "bytes fed to the replication stream" \
                    [wc_get $sto_d(one) repl_bytes] [wc_get $sto_d(none) repl_bytes] 2 512 \
                    "a STORE whose sources hold only future TTLs replicates verbatim; the replica recomputes the identical result" \
                    $sto_cmd(one) sto:a $sto_extra
                wc_assert_ratio "propagateNow calls" \
                    [wc_get $sto_d(one) prop_now_cmds] [wc_get $sto_d(none) prop_now_cmds] 1 2 \
                    "a future TTL must not split one STORE into result-sized batches" \
                    $sto_cmd(one) sto:a $sto_extra
            } {} {slow}
        }

        test "setperf-store: SUNIONSTORE replication volume must not grow with the result size" {
            set sto_ka sto:a
            set sto_kb sto:b
            set sto_sizes [wc_ht_sizes]
            foreach ttl {none one} { set sto_repl($ttl) {} }
            foreach n $sto_sizes {
                set sto_bmem [wc_members $n short [expr {$n / 2}]]
                $sto_primary del $sto_kb
                wc_batched $sto_primary sadd $sto_kb $sto_bmem
                foreach ttl {none one} {
                    set sto_live [wc_fixture $sto_ka $n $ttl short 3 $sto_primary]
                    set sto_d($ttl) [wc_window {$sto_primary sunionstore sto:dst $sto_ka $sto_kb} $sto_primary]
                    set sto_exp [sto_expected union $sto_live $sto_bmem]
                    assert_equal [$sto_primary scard sto:dst] [llength $sto_exp]
                    lappend sto_repl($ttl) [wc_get $sto_d($ttl) repl_bytes]
                    wc_record $::cur_test "sunionstore dst A B" \
                        [dict create n $n ttl $ttl result [llength $sto_exp] consumer replica] $sto_d($ttl)
                }
            }
            if {$::verbose} {
                puts "SUNIONSTORE repl_bytes over sizes {$sto_sizes}: none={$sto_repl(none)} one={$sto_repl(one)}"
            }
            wc_assert_flat "bytes fed to the replication stream (plain sources)" $sto_sizes $sto_repl(none) 2 256 \
                "the parent replicates one fixed-size command per STORE" "sunionstore dst A B" sto:a
            wc_assert_flat "bytes fed to the replication stream (one future TTL)" $sto_sizes $sto_repl(one) 2 512 \
                "a future TTL must keep replication request-sized, not result-sized" "sunionstore dst A B" sto:a
        } {} {slow}

        # Expired-but-unreclaimed members: effects are the CORRECT propagation
        # here (the replica must not recompute over members the primary treated
        # as gone). The contract is the price: one copy of the result.
        foreach sto_spec $::sto_store_cmds {
            lassign $sto_spec sto_label sto_tpl sto_kind sto_dsttype
            test "setperf-store: $sto_label over a mostly-expired source costs one copy of the result and stays consistent" {
                set n 20000
                set sto_live_keep 3
                set sto_ka sto:a
                set sto_kb sto:b
                set sto_bmem [wc_members $n short [expr {$n / 2}]]
                $sto_primary del $sto_kb
                wc_batched $sto_primary sadd $sto_kb $sto_bmem
                set sto_live [wc_fixture $sto_ka $n mostly_expired short $sto_live_keep $sto_primary]
                set sto_info [wc_setinfo $sto_ka $sto_primary]
                assert_equal [dict get $sto_info live] $sto_live_keep
                assert_equal [dict get $sto_info physical] $n
                set sto_mcmd [subst -nocommands $sto_tpl]
                set sto_md [wc_window "\$sto_primary $sto_mcmd" $sto_primary]
                set sto_exp [sto_expected $sto_kind $sto_live $sto_bmem]
                assert_equal [sto_dst_size $sto_dsttype sto:dst $sto_primary] [llength $sto_exp]
                wait_for_ofs_sync $sto_primary $sto_replica
                # The replica still holds every physical member of the source,
                # so recomputing there would give a different answer: dst must
                # be shipped, and must match.
                assert_equal [dict get [wc_setinfo $sto_ka $sto_replica] physical] $n
                assert_equal [lsort [sto_dst_members $sto_dsttype sto:dst $sto_replica]] [lsort $sto_exp]
                wc_record $::cur_test $sto_mcmd \
                    [dict create n $n ttl mostly_expired result [llength $sto_exp] consumer replica] $sto_md
                set sto_card [llength $sto_exp]
                set sto_reclaimed [wc_get $sto_md set_members_reclaimed]
                # One copy of the result, plus the legitimate SREM propagation
                # of members actually reclaimed during the command (brief rule 4).
                # One RESP copy of the result: payload + per-arg framing (<= 12
                # bytes for short members) + command headers, x1.3 for the
                # MULTI/EXEC wrapper and the batch headers.
                set sto_bound [expr {([sto_payload_bytes $sto_exp] + 12 * $sto_card) * 13 / 10 \
                                     + $sto_reclaimed * 24 + 512}]
                if {$::verbose} {
                    puts "$sto_label mostly_expired: repl_bytes=[wc_get $sto_md repl_bytes] result=$sto_card reclaimed=$sto_reclaimed bound=$sto_bound"
                }
                wc_assert_le "bytes fed to the replication stream" [wc_get $sto_md repl_bytes] $sto_bound \
                    "shipping the result is legitimate here, but it must be shipped once" \
                    $sto_mcmd $sto_ka "result=$sto_card reclaimed=$sto_reclaimed"
            } {} {slow}
        }

        test "setperf-store: SUNIONSTORE peak retained propagation memory must not scale with the result" {
            set n 200000
            set sto_ka sto:a
            set sto_kb sto:b
            set sto_bmem [wc_members $n short [expr {$n / 2}]]
            $sto_primary del $sto_kb
            wc_batched $sto_primary sadd $sto_kb $sto_bmem
            foreach ttl {none one} {
                set sto_live [wc_fixture $sto_ka $n $ttl short 3 $sto_primary]
                # The peak is reached inside the command proc, so ARM sees it.
                set sto_d($ttl) [wc_measure {$sto_primary sunionstore sto:dst $sto_ka $sto_kb} $sto_primary]
                set sto_exp_card [expr {$n + $n / 2}]
                assert_equal $::wc_last_reply $sto_exp_card
                assert_equal [$sto_primary scard sto:dst] $sto_exp_card
                wc_record $::cur_test "sunionstore dst A B" \
                    [dict create n $n ttl $ttl result $sto_exp_card consumer replica] $sto_d($ttl)
            }
            if {$::verbose} {
                puts "SUNIONSTORE n=$n peak retained propagation bytes: none=[wc_get $sto_d(none) prop_peak_retained_bytes] one=[wc_get $sto_d(one) prop_peak_retained_bytes]; largest single allocation none=[wc_get $sto_d(none) mem_max_alloc] one=[wc_get $sto_d(one) mem_max_alloc]"
            }
            # The 1024-argument batch bounds the stack argv, not the memory:
            # every batch stays queued in also_propagate until the execution
            # unit ends, so the retained payload is the whole result. The
            # baseline is the verbatim argv, which the ARM window does not see
            # (it is queued after the snapshot point), hence the 1024-byte
            # absolute allowance: a STORE argv is a handful of key names.
            wc_assert_ratio "peak payload bytes retained in also_propagate" \
                [wc_get $sto_d(one) prop_peak_retained_bytes] [wc_get $sto_d(none) prop_peak_retained_bytes] 2 1024 \
                "a future TTL must not retain a result-sized propagation payload" \
                "sunionstore dst A B" sto:a "result=[expr {$n + $n / 2}] consumer=replica"
        } {} {slow}

        wc_restore $sto_saved $sto_primary
    }
    }
}

# ---------------------------------------------------------------------------
# AOF as the consumer: the same amplification lands in the AOF, and the reload
# must reproduce the destination.
# ---------------------------------------------------------------------------
start_server {tags {"setperf set aof external:skip needs:debug"} overrides {appendonly yes appendfsync always}} {
    if {![wc_available]} {
        test "setperf-store (aof): skipped, server lacks WORK_COUNTERS" {
            skip "build with: make WORK_COUNTERS=yes"
        }
    } else {
    set sto_saved [wc_quiesce]

    foreach sto_spec $::sto_store_cmds {
        lassign $sto_spec sto_label sto_tpl sto_kind sto_dsttype
        test "setperf-store: $sto_label with AOF on, a future TTL must not amplify the AOF" {
            set n 20000
            set sto_ka sto:a
            set sto_kb sto:b
            set sto_bmem [wc_members $n short [expr {$n / 2}]]
            r del $sto_kb
            wc_batched [srv 0 client] sadd $sto_kb $sto_bmem
            foreach ttl {none one} {
                set sto_live [wc_fixture $sto_ka $n $ttl]
                set sto_cmd($ttl) [subst -nocommands $sto_tpl]
                set sto_d($ttl) [wc_window "r $sto_cmd($ttl)" [srv 0 client]]
                set sto_exp [sto_expected $sto_kind $sto_live $sto_bmem]
                assert_equal [sto_dst_size $sto_dsttype sto:dst] [llength $sto_exp]
                # The AOF must reproduce the destination it recorded.
                set sto_before [lsort [sto_dst_members $sto_dsttype sto:dst]]
                r debug loadaof
                assert_equal [lsort [sto_dst_members $sto_dsttype sto:dst]] $sto_before
                assert_equal $sto_before [lsort $sto_exp]
                wc_record $::cur_test $sto_cmd($ttl) \
                    [dict create n $n ttl $ttl result [llength $sto_exp] consumer aof] $sto_d($ttl)
            }
            if {$::verbose} {
                puts "$sto_label aof_bytes: none=[wc_get $sto_d(none) aof_bytes] one=[wc_get $sto_d(one) aof_bytes]"
            }
            wc_assert_ratio "bytes fed to the AOF buffer" \
                [wc_get $sto_d(one) aof_bytes] [wc_get $sto_d(none) aof_bytes] 2 512 \
                "a STORE whose sources hold only future TTLs is replayed correctly from the verbatim command" \
                $sto_cmd(one) sto:a "result=[llength $sto_exp] consumer=aof"
        } {} {slow}
    }

    test "setperf-store: SUNIONSTORE over a mostly-expired source reloads from the AOF exactly" {
        set n 20000
        set sto_live_keep 3
        set sto_ka sto:a
        set sto_kb sto:b
        set sto_bmem [wc_members $n short [expr {$n / 2}]]
        r del $sto_kb
        wc_batched [srv 0 client] sadd $sto_kb $sto_bmem
        set sto_live [wc_fixture $sto_ka $n mostly_expired short $sto_live_keep]
        set sto_md [wc_window {r sunionstore sto:dst $sto_ka $sto_kb} [srv 0 client]]
        set sto_exp [sto_expected union $sto_live $sto_bmem]
        assert_equal [r scard sto:dst] [llength $sto_exp]
        r debug loadaof
        assert_equal [lsort [r smembers sto:dst]] [lsort $sto_exp]
        wc_record $::cur_test "sunionstore dst A B" \
            [dict create n $n ttl mostly_expired result [llength $sto_exp] consumer aof] $sto_md
        set sto_reclaimed [wc_get $sto_md set_members_reclaimed]
        set sto_bound [expr {([sto_payload_bytes $sto_exp] + 12 * [llength $sto_exp]) * 13 / 10 \
                             + $sto_reclaimed * 24 + 512}]
        if {$::verbose} {
            puts "SUNIONSTORE mostly_expired aof_bytes=[wc_get $sto_md aof_bytes] result=[llength $sto_exp] reclaimed=$sto_reclaimed bound=$sto_bound"
        }
        wc_assert_le "bytes fed to the AOF buffer" [wc_get $sto_md aof_bytes] $sto_bound \
            "recording the result is legitimate here, but it must be recorded once" \
            "sunionstore dst A B" $sto_ka "result=[llength $sto_exp] reclaimed=$sto_reclaimed"
    } {} {slow}

    wc_restore $sto_saved
    }
}
