# Performance contracts for the lifecycle paths of a set with member TTLs:
# MEMORY USAGE sampling, the first-TTL representation conversion, SPERSIST,
# COPY, RDB save/load, AOF rewrite, active expiration, SMEMBERS/SSCAN/SCARD.
#
# Requires a server built with `make WORK_COUNTERS=yes`; the counters are read
# through DEBUG WORKCTR (helpers in tests/support/workctr.tcl). Budgets are
# derived from the parent algorithms (the no-TTL fixture runs the parent code
# paths unchanged), from source inspection and from the sibling hash
# implementation of the same lifecycle path, never from the measured commit.
#
# Contract E: an estimator must stay sample-sized. MEMORY USAGE samples
# SAMPLES members; a member TTL must not turn that into a walk of the
# population, and memory that is physically allocated must stay accounted for
# whatever the deadlines say.
# Contract F: a whole-object rebuild (intset conversion, COPY, RDB save/load,
# AOF rewrite, reclaim) is ONE pass over the members plus work proportional to
# the volatile count through the expiry index -- not one full pass per volatile
# member, and not one pass per member.

# actual >= floor, for accounting contracts.
proc lif_assert_ge {what actual floor contract cmd key {extra ""}} {
    if {$actual < $floor} {
        fail "CONTRACT VIOLATED: $contract\n  $what = $actual, required floor = $floor\n  [wc_ctx $cmd $key $extra]\n  [wc_repro $cmd]"
    }
}

proc lif_note {msg} {
    if {$::verbose} { puts "  \[lifecycle\] $msg" }
}

# ---- forked-child counter dumps (AOF rewrite / RDB save) ------------------

proc lif_child_path {fname} {
    return [file join [lindex [r config get dir] 1] $fname]
}

proc lif_forget_child {fname} {
    catch {file delete [lif_child_path $fname]}
}

proc lif_child_counters {fname} {
    set fp [open [lif_child_path $fname] r]
    set data [read $fp]
    close $fp
    set d [dict create]
    foreach line [split $data "\n"] {
        if {[llength $line] == 2} { dict set d [lindex $line 0] [lindex $line 1] }
    }
    return $d
}

proc lif_wait_child {fname infofield} {
    wait_for_condition 200 50 {
        [status [srv 0 client] $infofield] == 0 && [file exists [lif_child_path $fname]]
    } else {
        fail "the forked child did not produce $fname ($infofield still set)"
    }
}

# ---- fixtures -------------------------------------------------------------

proc lif_mu_fixtures {n} {
    foreach fx {none one all mostly_expired all_expired} {
        wc_fixture lif:mu:$fx $n $fx short 5
    }
}

# A hash with the same shape, to tell an inherited cost from an introduced one:
# hash field TTLs exist in the parent commit and use the same vset index.
proc lif_hash_fixture {key n ms} {
    r del $key
    set pairs {}
    set fields {}
    for {set i 0} {$i < $n} {incr i} {
        lappend pairs "h$i" v
        lappend fields "h$i"
    }
    for {set i 0} {$i < [llength $pairs]} {incr i 4000} {
        r hset $key {*}[lrange $pairs $i [expr {$i + 3999}]]
    }
    if {$ms > 0} {
        for {set i 0} {$i < $n} {incr i 4000} {
            set chunk [lrange $fields $i [expr {$i + 3999}]]
            r hpexpire $key $ms fields [llength $chunk] {*}$chunk
        }
    }
    return $fields
}

proc lif_int_members {n} {
    set l {}
    for {set i 0} {$i < $n} {incr i} { lappend l $i }
    return $l
}

start_server {tags {"setperf set external:skip needs:debug"}} {
    if {![wc_available]} {
        test "setperf-lifecycle: skipped, server lacks WORK_COUNTERS" {
            skip "build with: make WORK_COUNTERS=yes"
        }
    } else {
    set lif_saved [wc_quiesce]

    # ======================================================================
    # E. MEMORY USAGE sampling
    # ======================================================================

    test "setperf-lifecycle: MEMORY USAGE samples a bounded number of members of a live volatile set" {
        set n 20000
        lif_mu_fixtures $n
        foreach fx {none one all} {
            set key lif:mu:$fx
            assert_equal hashtable [dict get [wc_setinfo $key] encoding]
            foreach spec {{1 {memory usage $key samples 1}} {5 {memory usage $key}} {0 {memory usage $key samples 0}}} {
                lassign $spec samples cmdtpl
                set cmd [subst -nocommands $cmdtpl]
                set d [wc_measure "r $cmd"]
                set lif_usage($fx,$samples) $::wc_last_reply
                wc_record $::cur_test $cmd [dict create n $n ttl $fx enc hashtable samples $samples] $d
                lif_note "MEMORY USAGE $fx samples=$samples -> $::wc_last_reply bytes, ht_iter_visits=[wc_get $d ht_iter_visits] validate=[wc_get $d ht_validate_calls] vset_buckets=[wc_get $d vset_bucket_visits]"
                if {$samples == 0} {
                    # SAMPLES 0 asks for an exact walk: one pass, no more.
                    wc_assert_le "hashtable positions examined" [wc_get $d ht_iter_visits] [expr {$n + 8}] \
                        "SAMPLES 0 is exactly one pass over the members (n=$n)" $cmd $key
                } else {
                    # The estimator stops at SAMPLES members; the iterator is
                    # advanced once more before the budget is re-checked.
                    wc_assert_le "hashtable positions examined" [wc_get $d ht_iter_visits] [expr {$samples + 8}] \
                        "the estimator examines SAMPLES members, not the population (n=$n)" $cmd $key
                }
                # The expiry index is measured per deadline group (the fixtures
                # create at most a handful), never per member.
                wc_assert_le "vset buckets visited" [wc_get $d vset_bucket_visits] 64 \
                    "sizing the expiry index is bounded by its deadline groups (n=$n)" $cmd $key
            }
        }
        # Identical live members: the TTL machinery may add bytes, never remove them.
        foreach samples {1 5 0} {
            # SAMPLES 0 is an exact walk: no sampling noise, so the floor is exact.
            lif_assert_ge "MEMORY USAGE (SAMPLES $samples) of the all-TTL set" $lif_usage(all,$samples) \
                [expr {$samples == 0 ? $lif_usage(none,$samples) : $lif_usage(none,$samples) * 8 / 10}] \
                "a volatile set holding the same members must not report less memory than a plain one" \
                "memory usage key samples $samples" lif:mu:all "plain=$lif_usage(none,$samples)"
        }
    }

    test "setperf-lifecycle: MEMORY USAGE must not walk the expired population to collect samples" {
        set n 20000
        lif_mu_fixtures $n
        # Measure every fixture before gating any of them, so one violation
        # still reports the whole picture.
        set lif_e2 {}
        foreach fx {mostly_expired all_expired} {
            foreach spec {{1 {memory usage $key samples 1}} {5 {memory usage $key}}} {
                lassign $spec samples cmdtpl
                set key lif:mu:$fx
                set cmd [subst -nocommands $cmdtpl]
                set d [wc_measure "r $cmd"]
                wc_record $::cur_test $cmd [dict create n $n ttl $fx enc hashtable samples $samples] $d
                lif_note "MEMORY USAGE $fx samples=$samples -> $::wc_last_reply bytes, ht_iter_visits=[wc_get $d ht_iter_visits] rejected=[wc_get $d ht_iter_rejected]"
                lappend lif_e2 [list $fx $samples $cmd $key [wc_get $d ht_iter_visits] [wc_get $d ht_iter_rejected]]
            }
        }
        # The estimator iterates with a validating iterator, so every expired
        # member it steps over is population work spent to find one sample.
        # Sizing an object must not depend on how many members are expired.
        foreach row $lif_e2 {
            lassign $row fx samples cmd key visits rejected
            wc_assert_le "hashtable positions examined ($fx)" $visits [expr {$samples + 8}] \
                "collecting $samples samples must not traverse the expired population (n=$n)" $cmd $key \
                "hidden by validation: $rejected; all fixtures: [list $lif_e2]"
        }
    }

    test "setperf-lifecycle: MEMORY USAGE must account for physically allocated expired members" {
        set n 20000
        lif_mu_fixtures $n
        # lif:mu:all and lif:mu:all_expired hold the same n physical members,
        # the same smember allocations and the same expiry index; only the
        # deadlines differ. Nothing has been freed, so nothing may vanish from
        # the report -- an estimator that samples zero members adds zero member
        # memory and under-reports the object.
        set lif_e3 {}
        foreach spec {{1 {samples 1}} {5 {}} {0 {samples 0}}} {
            lassign $spec samples tail
            set base [wc_measure "r memory usage lif:mu:all $tail"]
            set base_usage $::wc_last_reply
            set d [wc_measure "r memory usage lif:mu:all_expired $tail"]
            set exp_usage $::wc_last_reply
            wc_record $::cur_test "memory usage key $tail" \
                [dict create n $n ttl all_expired enc hashtable samples $samples] $d
            lif_note "MEMORY USAGE samples=$samples: all=$base_usage all_expired=$exp_usage (physical members identical)"
            lappend lif_e3 [list $samples $tail $base_usage $exp_usage]
        }
        foreach row $lif_e3 {
            lassign $row samples tail base_usage exp_usage
            lif_assert_ge "MEMORY USAGE (SAMPLES $samples) of the all-expired set" $exp_usage \
                [expr {$samples == 0 ? $base_usage : $base_usage * 8 / 10}] \
                "expired-but-unreclaimed members are physically allocated and must stay accounted for" \
                "memory usage key $tail" lif:mu:all_expired \
                "same-members baseline=$base_usage; all fixtures: [list $lif_e3]"
        }
    }

    test "setperf-lifecycle: MEMORY USAGE on a volatile hash, same estimator (classification probe)" {
        set n 20000
        lif_hash_fixture lif:h:live $n 86400000
        lif_hash_fixture lif:h:exp $n 1
        after 10
        assert_equal hashtable [r object encoding lif:h:exp]
        assert_equal hashtable [r object encoding lif:h:live]
        foreach k {lif:h:live lif:h:exp} {
            set d [wc_measure "r memory usage $k"]
            set lif_hu($k) $::wc_last_reply
            set lif_hv($k) [wc_get $d ht_iter_visits]
            wc_record $::cur_test "memory usage key" [dict create n $n type hash ttl $k] $d
        }
        # Recorded, not gated: this is the pre-existing hash path measured with
        # the same counters, so the reader can tell whether the set numbers
        # above are a new defect or the same defect in a new place.
        lif_note "hash probe: live usage=$lif_hu(lif:h:live) visits=$lif_hv(lif:h:live); expired usage=$lif_hu(lif:h:exp) visits=$lif_hv(lif:h:exp)"
        # HLEN counts expired-but-unreclaimed fields, as SCARD counts members.
        assert_equal $n [r hlen lif:h:exp]
    }

    # ======================================================================
    # F. Lifecycle paths
    # ======================================================================

    test "setperf-lifecycle: the first member TTL converts an intset in one pass" {
        set lif_maxint [lindex [r config get set-max-intset-entries] 1]
        r config set set-max-intset-entries 100000
        foreach n {64 512 5000} {
            set key lif:is:$n
            r del $key
            wc_batched [srv 0 client] sadd $key [lif_int_members $n]
            assert_equal intset [dict get [wc_setinfo $key] encoding]
            set intset_usage [r memory usage $key samples 0]

            set cmd "sexpire $key 1000 members 1 7"
            set d [wc_measure "r $cmd"]
            assert_equal {1} $::wc_last_reply
            set enc [dict get [wc_setinfo $key] encoding]
            # An intset holds no metadata, so the first TTL must convert; the
            # listpack budget decides which representation it lands in.
            assert_equal [expr {$n <= 128 ? "listpack" : "hashtable"}] $enc
            assert_equal $n [r scard $key]
            assert_equal 1 [r sismember $key 7]
            set ttl_usage [r memory usage $key samples 0]
            wc_record $::cur_test $cmd [dict create n $n ttl first enc $enc] $d
            lif_note "intset n=$n -> $enc: set_iter_next=[wc_get $d set_iter_next] smember_created=[wc_get $d smember_created] lp_inserts=[wc_get $d lp_inserts] mem_max_alloc=[wc_get $d mem_max_alloc] usage $intset_usage -> $ttl_usage"

            wc_assert_le "members read from the intset" [wc_get $d set_iter_next] [expr {$n + 8}] \
                "the conversion reads each member once (n=$n)" $cmd $key
            wc_assert_le "largest single allocation (bytes)" [wc_get $d mem_max_alloc] [expr {32 * $n + 8192}] \
                "the conversion allocates one destination sized by the set (n=$n)" $cmd $key
            if {$enc eq "hashtable"} {
                wc_assert_le "smembers created" [wc_get $d smember_created] [expr {$n + 8}] \
                    "the conversion copies each member once (n=$n)" $cmd $key
                wc_assert_le "hashtable positions iterated" [wc_get $d ht_iter_visits] 8 \
                    "the destination table is filled by insertion, not iterated (n=$n)" $cmd $key
            } else {
                wc_assert_le "listpack insertions" [wc_get $d lp_inserts] [expr {$n + 8}] \
                    "the conversion appends each member once (n=$n)" $cmd $key
                wc_assert_le "listpack entries examined" [wc_lp_examined $d] [expr {3 * $n + 16}] \
                    "the conversion is a bounded number of listpack passes (n=$n)" $cmd $key
            }

            # ... and back: dropping the last TTL is a lookup, not a rebuild.
            set cmd2 "spersist $key members 1 7"
            set d2 [wc_measure "r $cmd2"]
            assert_equal {1} $::wc_last_reply
            assert_equal {-1} [r spttl $key members 1 7]
            assert_equal 0 [dict get [wc_setinfo $key] volatile]
            set persist_usage [r memory usage $key samples 0]
            wc_record $::cur_test $cmd2 [dict create n $n ttl last-removed enc $enc] $d2
            lif_note "spersist n=$n ($enc): ht_iter_visits=[wc_get $d2 ht_iter_visits] lp_find_calls=[wc_get $d2 lp_find_calls] vset_removes=[wc_get $d2 vset_removes] usage $ttl_usage -> $persist_usage"
            if {$enc eq "hashtable"} {
                wc_assert_le "hashtable positions iterated" [wc_get $d2 ht_iter_visits] 0 \
                    "removing the last TTL is a lookup, not a population pass (n=$n)" $cmd2 $key
            } else {
                wc_assert_le "listpack find calls" [wc_get $d2 lp_find_calls] 2 \
                    "removing the last TTL locates the member once (n=$n)" $cmd2 $key
            }
            wc_assert_le "MEMORY USAGE after the last TTL was removed" $persist_usage $ttl_usage \
                "dropping the last TTL must not retain expiry storage (n=$n)" $cmd2 $key
        }
        r config set set-max-intset-entries $lif_maxint
    }

    test "setperf-lifecycle: COPY of a volatile hashtable set is one pass and one copy per member" {
        set n 20000
        foreach fx {none one all} {
            set src lif:cp:$fx
            wc_fixture $src $n $fx
            r del lif:cp:dst
            set info [wc_setinfo $src]
            set volatile [dict get $info volatile]
            set cmd "copy $src lif:cp:dst"
            set d [wc_measure "r $cmd"]
            assert_equal 1 $::wc_last_reply
            wc_record $::cur_test $cmd [dict create n $n ttl $fx enc hashtable] $d
            lif_note "COPY $fx: set_iter_next=[wc_get $d set_iter_next] ht_iter_visits=[wc_get $d ht_iter_visits] smember_created=[wc_get $d smember_created] vset_adds=[wc_get $d vset_adds] ht_hash_calls=[wc_get $d ht_hash_calls] mem_max_alloc=[wc_get $d mem_max_alloc]"

            wc_assert_le "members read from the source" [wc_get $d set_iter_next] [expr {$n + 8}] \
                "COPY reads the source once (n=$n, ttl=$fx)" $cmd $src
            wc_assert_le "hashtable positions examined by iteration" [wc_get $d ht_iter_visits] [expr {2 * $n + 16}] \
                "COPY is one traversal of the source (n=$n, ttl=$fx)" $cmd $src
            assert_equal $n [wc_get $d smember_created]
            assert_lessthan_equal [wc_get $d vset_adds] $volatile
            if {$fx eq "none"} {
                set lif_cp_hash [wc_get $d ht_hash_calls]
                set lif_cp_alloc [wc_get $d mem_max_alloc]
            } else {
                # Building the destination's expiry index hashes each volatile
                # member a constant number of times; it must not scale worse.
                wc_assert_ratio "hash function invocations" [wc_get $d ht_hash_calls] $lif_cp_hash 5 500 \
                    "indexing the copied TTLs is a constant factor of the plain copy (n=$n)" $cmd $src
                wc_assert_ratio "largest single allocation (bytes)" [wc_get $d mem_max_alloc] $lif_cp_alloc 2 4096 \
                    "COPY needs no population-sized temporary beyond the destination (n=$n)" $cmd $src
            }

            # Correctness: same live members, same absolute deadlines.
            assert_equal [lsort [r smembers $src]] [lsort [r smembers lif:cp:dst]]
            assert_equal [r scard $src] [r scard lif:cp:dst]
            foreach m [list m0 m1 m[expr {$n - 1}]] {
                assert_equal [r spexpiretime $src members 1 $m] [r spexpiretime lif:cp:dst members 1 $m]
            }
            assert_equal $volatile [dict get [wc_setinfo lif:cp:dst] volatile]
        }
        # An expired-but-unreclaimed member is physical state: the copy keeps it
        # (neither resurrected nor silently dropped).
        wc_fixture lif:cp:mexp $n mostly_expired short 5
        r del lif:cp:dst
        set d [wc_measure {r copy lif:cp:mexp lif:cp:dst}]
        wc_record $::cur_test "copy src dst" [dict create n $n ttl mostly_expired enc hashtable] $d
        set si [wc_setinfo lif:cp:mexp]
        set di [wc_setinfo lif:cp:dst]
        assert_equal [dict get $si physical] [dict get $di physical]
        assert_equal [dict get $si live] [dict get $di live]
        assert_equal [dict get $si volatile] [dict get $di volatile]
        wc_assert_le "members read from the source" [wc_get $d set_iter_next] [expr {$n + 8}] \
            "COPY of a mostly-expired set is still one pass (n=$n)" "copy src dst" lif:cp:mexp
    }

    test "setperf-lifecycle: COPY of a volatile listpack set stays a blob copy" {
        set n 64
        foreach fx {none one all} {
            set src lif:cplp:$fx
            wc_fixture $src $n $fx
            assert_equal listpack [dict get [wc_setinfo $src] encoding]
            set bytes [dict get [wc_setinfo $src] bytes]
            r del lif:cplp:dst
            set cmd "copy $src lif:cplp:dst"
            set d [wc_measure "r $cmd"]
            assert_equal 1 $::wc_last_reply
            wc_record $::cur_test $cmd [dict create n $n ttl $fx enc listpack] $d
            lif_note "COPY listpack $fx: lp_examined=[wc_lp_examined $d] lp_inserts=[wc_get $d lp_inserts] mem_max_alloc=[wc_get $d mem_max_alloc] bytes=$bytes"
            # The metadata entries travel inside the same buffer, so a TTL must
            # not turn the single memcpy into a member-by-member rebuild.
            wc_assert_le "listpack entries examined" [wc_lp_examined $d] 0 \
                "copying a listpack set is a blob copy (n=$n, ttl=$fx)" $cmd $src
            wc_assert_le "listpack insertions" [wc_get $d lp_inserts] 0 \
                "copying a listpack set inserts nothing member by member (n=$n, ttl=$fx)" $cmd $src
            wc_assert_le "largest single allocation (bytes)" [wc_get $d mem_max_alloc] [expr {2 * $bytes + 512}] \
                "the copy allocates one buffer of the source's size (n=$n, ttl=$fx)" $cmd $src
            assert_equal [lsort [r smembers $src]] [lsort [r smembers lif:cplp:dst]]
            foreach m {m0 m1 m63} {
                assert_equal [r spexpiretime $src members 1 $m] [r spexpiretime lif:cplp:dst members 1 $m]
            }
        }
    }

    test "setperf-lifecycle: DEBUG RELOAD saves and loads each member once" {
        set sizes {2000 20000}
        foreach fx {none one all} {
            set work {}
            foreach n $sizes {
                r flushall
                set key lif:rl:$fx
                wc_fixture $key $n $fx
                set volatile [dict get [wc_setinfo $key] volatile]
                set before {}
                foreach m [list m0 m1 m[expr {$n - 1}]] { lappend before [r spexpiretime $key members 1 $m] }
                set d [wc_measure {r debug reload}]
                wc_record $::cur_test "debug reload" [dict create n $n ttl $fx enc hashtable] $d
                lif_note "DEBUG RELOAD $fx n=$n: saved=[wc_get $d rdb_members_saved] loaded=[wc_get $d rdb_members_loaded] smember_created=[wc_get $d smember_created] vset_adds=[wc_get $d vset_adds] examined=[wc_ht_examined $d]"

                assert_equal $n [wc_get $d rdb_members_saved]
                assert_equal $n [wc_get $d rdb_members_loaded]
                wc_assert_le "smembers created" [wc_get $d smember_created] [expr {$n + 16}] \
                    "the load copies each member once (n=$n, ttl=$fx)" "debug reload" $key
                assert_lessthan_equal [wc_get $d vset_adds] $volatile
                # Correctness: same members, same absolute deadlines.
                assert_equal $n [r scard $key]
                set after {}
                foreach m [list m0 m1 m[expr {$n - 1}]] { lappend after [r spexpiretime $key members 1 $m] }
                assert_equal $before $after
                lappend work [wc_sum $d ht_iter_visits ht_bucket_probes lp_find_steps lp_next_steps]
            }
            # 10x the members must cost about 10x, not 100x.
            wc_assert_ratio "entries examined by save+load" [lindex $work 1] [lindex $work 0] 12 2000 \
                "RDB save+load is linear in members (sizes $sizes, ttl=$fx, values {$work})" \
                "debug reload" lif:rl:$fx
        }
    }

    test "setperf-lifecycle: the BGSAVE child writes each member once" {
        set n 20000
        foreach fx {none all} {
            r flushall
            set key lif:bg:$fx
            wc_fixture $key $n $fx
            lif_forget_child workctr-rdb-child.txt
            r debug workctr reset
            r bgsave
            lif_wait_child workctr-rdb-child.txt rdb_bgsave_in_progress
            set d [lif_child_counters workctr-rdb-child.txt]
            wc_record $::cur_test "bgsave (child)" [dict create n $n ttl $fx enc hashtable] $d
            lif_note "BGSAVE child $fx: saved=[wc_get $d rdb_members_saved] ht_iter_visits=[wc_get $d ht_iter_visits]"
            assert_equal $n [wc_get $d rdb_members_saved]
            wc_assert_le "hashtable positions examined in the child" [wc_get $d ht_iter_visits] [expr {$n + 64}] \
                "the child dumps the set in one pass (n=$n, ttl=$fx)" "bgsave" $key
        }
    }

    test "setperf-lifecycle: loading a volatile listpack set checks each member once" {
        set n 64
        foreach fx {none all} {
            r flushall
            set key lif:rllp:$fx
            wc_fixture $key $n $fx
            assert_equal listpack [dict get [wc_setinfo $key] encoding]
            set before {}
            foreach m {m0 m1 m63} { lappend before [r spexpiretime $key members 1 $m] }
            set d [wc_measure {r debug reload}]
            wc_record $::cur_test "debug reload" [dict create n $n ttl $fx enc listpack] $d
            lif_note "DEBUG RELOAD listpack $fx n=$n: lp_find_calls=[wc_get $d lp_find_calls] lp_find_steps=[wc_get $d lp_find_steps] saved=[wc_get $d rdb_members_saved] loaded=[wc_get $d rdb_members_loaded]"
            assert_equal listpack [dict get [wc_setinfo $key] encoding]
            assert_equal $n [r scard $key]
            set after {}
            foreach m {m0 m1 m63} { lappend after [r spexpiretime $key members 1 $m] }
            assert_equal $before $after
            # A volatile listpack set is saved and reloaded member by member
            # (each with its deadline) instead of as one blob, so each member is
            # checked for duplication once. The per-member scan of the prefix
            # that check performs is quadratic in the listpack length, bounded
            # by set-max-listpack-entries: measured and recorded, not gated.
            wc_assert_le "listpack duplicate checks" [wc_get $d lp_find_calls] [expr {$n + 8}] \
                "the load checks each loaded member once (n=$n, ttl=$fx)" "debug reload" $key
        }
    }

    test "setperf-lifecycle: SMEMBERS is one pass over the physical members" {
        set n 20000
        foreach fx {none one mostly_expired} {
            set key lif:sm:$fx
            wc_fixture $key $n $fx short 5
            set expected [expr {$fx eq "mostly_expired" ? 5 : $n}]
            set cmd "smembers $key"
            set d [wc_measure "r $cmd"]
            assert_equal $expected [llength $::wc_last_reply]
            set lif_sm($fx) [wc_ht_examined $d]
            wc_record $::cur_test $cmd [dict create n $n ttl $fx enc hashtable] $d
            lif_note "SMEMBERS $fx: examined=$lif_sm($fx) returned=[llength $::wc_last_reply] rejected=[wc_get $d ht_iter_rejected]"
            wc_assert_le "hashtable entries examined" $lif_sm($fx) [expr {2 * $n + 16}] \
                "returning the live members is one pass over the physical members (n=$n, ttl=$fx)" $cmd $key
        }
        # Hiding expired members costs the same single pass as returning them.
        wc_assert_ratio "hashtable entries examined" $lif_sm(mostly_expired) $lif_sm(none) 2 500 \
            "expired members are skipped inside the same traversal" "smembers key" lif:sm:mostly_expired
        wc_assert_ratio "hashtable entries examined" $lif_sm(one) $lif_sm(none) 2 500 \
            "one TTL adds no traversal" "smembers key" lif:sm:one
    }

    test "setperf-lifecycle: a full SSCAN loop is one pass and returns only live members" {
        set n 20000
        foreach fx {none one mostly_expired} {
            set key lif:ss:$fx
            wc_fixture $key $n $fx short 5
            set expected [expr {$fx eq "mostly_expired" ? 5 : $n}]
            set calls 0
            set found [dict create]
            set d [wc_window {
                set cursor 0
                while 1 {
                    set res [r sscan $key $cursor count 100]
                    incr calls
                    set cursor [lindex $res 0]
                    foreach m [lindex $res 1] { dict set found $m 1 }
                    if {$cursor == 0} break
                }
            }]
            set examined [wc_sum $d ht_scan_visits ht_scan_buckets]
            wc_record $::cur_test "sscan key cursor count 100 (full loop)" \
                [dict create n $n ttl $fx enc hashtable calls $calls] $d
            lif_note "SSCAN loop $fx: calls=$calls examined=$examined returned=[dict size $found] rejected=[wc_get $d ht_scan_rejected]"
            assert_equal $expected [dict size $found]
            # The guarantee is one visit per position over the whole loop, plus
            # the per-call cursor overhead; not one pass per call.
            wc_assert_le "hashtable positions visited by the scan loop" $examined [expr {3 * $n + 100 * $calls}] \
                "a full SSCAN loop visits each position about once (n=$n, ttl=$fx, calls=$calls)" \
                "sscan key cursor count 100" $key
        }
    }

    test "setperf-lifecycle: SCARD stays O(1) on a volatile set" {
        set n 20000
        foreach fx {none one mostly_expired} {
            set key lif:sc:$fx
            wc_fixture $key $n $fx short 5
            set d [wc_measure "r scard $key"]
            wc_record $::cur_test "scard key" [dict create n $n ttl $fx enc hashtable] $d
            lif_note "SCARD $fx -> $::wc_last_reply (physical), examined=[wc_ht_examined $d]"
            wc_assert_le "hashtable entries examined" [wc_ht_examined $d] 8 \
                "SCARD reads a counter (n=$n, ttl=$fx)" "scard key" $key
            # SCARD reports the physical cardinality, expired-but-unreclaimed
            # members included, exactly as HLEN does for hash fields.
            assert_equal $n $::wc_last_reply
        }
        lif_hash_fixture lif:sc:h $n 1
        after 10
        assert_equal $n [r hlen lif:sc:h]
    }

    # ---- active expiration ---------------------------------------------
    # These enable active expiration, so the database must hold only the key
    # under test: another expired-heavy key would be reclaimed in the same
    # window and its work would be counted here.

    test "setperf-lifecycle: active expiration of a listpack set is no worse than the hash path" {
        set n 128
        r flushall
        wc_fixture lif:ae:lp $n all_expired
        assert_equal listpack [dict get [wc_setinfo lif:ae:lp] encoding]
        set set_bytes [dict get [wc_setinfo lif:ae:lp] bytes]
        set ds [wc_window {
            r debug set-active-expire 1
            wait_for_condition 100 50 {
                [r exists lif:ae:lp] == 0
            } else {
                fail "the all-expired listpack set was not reclaimed"
            }
        }]
        r debug set-active-expire 0

        r flushall
        lif_hash_fixture lif:ae:hlp $n 1
        assert_equal listpack [r object encoding lif:ae:hlp]
        set hash_bytes [r memory usage lif:ae:hlp samples 0]
        set dh [wc_window {
            r debug set-active-expire 1
            wait_for_condition 100 50 {
                [r exists lif:ae:hlp] == 0
            } else {
                fail "the all-expired listpack hash was not reclaimed"
            }
        }]
        r debug set-active-expire 0

        wc_record $::cur_test "active expire (set listpack)" [dict create n $n ttl all_expired enc listpack] $ds
        wc_record $::cur_test "active expire (hash listpack)" [dict create n $n ttl all_expired enc listpack type hash] $dh
        set set_moved [wc_get $ds lp_tail_bytes_moved]
        set hash_moved [wc_get $dh lp_tail_bytes_moved]
        # x100 integer ratios: plain division truncates small movements to 0.
        set set_ratio [expr {$set_bytes > 0 ? ($set_moved * 100) / $set_bytes : 0}]
        set hash_ratio [expr {$hash_bytes > 0 ? ($hash_moved * 100) / $hash_bytes : 0}]
        lif_note "active expire listpack: set moved=$set_moved bytes over $set_bytes (x$set_ratio/100), deletes=[wc_get $ds lp_deletes], reclaimed=[wc_get $ds set_members_reclaimed]; hash moved=$hash_moved over $hash_bytes (x$hash_ratio/100), deletes=[wc_get $dh lp_deletes]"

        assert_equal $n [wc_get $ds set_members_reclaimed]
        wc_assert_le "listpack deletions" [wc_get $ds lp_deletes] [expr {$n + 8}] \
            "each expired member is deleted once (n=$n)" "active expire" lif:ae:lp
        # Each deletion memmoves the tail, so the bytes moved to drain a whole
        # listpack are quadratic in its length. hashTypeDeleteExpiredFields
        # deletes the same way, so the cost is the parent's; what is gated here
        # is that the set path is not WORSE than the path it copies.
        wc_assert_ratio "listpack bytes moved per byte of listpack (x100)" $set_ratio $hash_ratio 2 800 \
            "reclaiming a listpack set must not move more bytes than the equivalent hash path (n=$n)" \
            "active expire" lif:ae:lp "set=$set_moved hash=$hash_moved"
    }

    test "setperf-lifecycle: active expiration of a hashtable set reclaims each member once" {
        set lif_ae_work {}
        foreach n {2000 20000} {
            r flushall
            wc_fixture lif:ae:ht $n all_expired
            assert_equal hashtable [dict get [wc_setinfo lif:ae:ht] encoding]
            set d [wc_window {
                r debug set-active-expire 1
                wait_for_condition 200 50 {
                    [r exists lif:ae:ht] == 0
                } else {
                    fail "the all-expired hashtable set was not reclaimed (n=$n)"
                }
            }]
            r debug set-active-expire 0
            wc_record $::cur_test "active expire (set hashtable)" [dict create n $n ttl all_expired enc hashtable] $d
            lif_note "active expire hashtable n=$n: reclaimed=[wc_get $d set_members_reclaimed] vset_entry_visits=[wc_get $d vset_entry_visits] vset_expire_calls=[wc_get $d vset_expire_calls] ht_iter_buckets=[wc_sum $d ht_iter_buckets idx_ht_iter_buckets] ht_iter_visits=[wc_sum $d ht_iter_visits idx_ht_iter_visits]"
            assert_equal $n [wc_get $d set_members_reclaimed]
            wc_assert_le "expiry-index entries visited" [wc_get $d vset_entry_visits] [expr {2 * $n + 16}] \
                "the index yields each expired member once (n=$n)" "active expire" lif:ae:ht
            wc_assert_le "hashtable positions examined" [wc_sum $d ht_iter_visits idx_ht_iter_visits] [expr {2 * $n + 16}] \
                "each expired member is examined once (n=$n)" "active expire" lif:ae:ht
            lappend lif_ae_work [wc_sum $d ht_iter_buckets idx_ht_iter_buckets]
        }
        # Bucket stepping across the batches: each reclaim batch restarts its
        # iteration, so the buckets stepped grow with batches*buckets. Measured
        # and recorded; the hash path below shows whose cost this is.
        r flushall
        lif_hash_fixture lif:ae:h20 20000 1
        set dh [wc_window {
            r debug set-active-expire 1
            wait_for_condition 200 50 {
                [r exists lif:ae:h20] == 0
            } else {
                fail "the all-expired hashtable hash was not reclaimed"
            }
        }]
        r debug set-active-expire 0
        wc_record $::cur_test "active expire (hash hashtable)" [dict create n 20000 ttl all_expired enc hashtable type hash] $dh
        lif_note "active expire bucket steps: set {2000 20000} = {$lif_ae_work}; hash n=20000 = [wc_get $dh ht_iter_buckets]"
        wc_assert_ratio "hashtable bucket steps during reclaim" [lindex $lif_ae_work 1] [wc_get $dh ht_iter_buckets] 2 1000 \
            "reclaiming a set must not step more buckets than the equivalent hash path (n=20000)" \
            "active expire" lif:ae:ht "set sizes {2000 20000} = {$lif_ae_work}"
    }

    wc_restore $lif_saved
    }
}

# The AOF rewrite path. aof-use-rdb-preamble must be off: with the default
# preamble the rewrite dumps an RDB and rewriteSetObject is never reached.
# auto-aof-rewrite-percentage 0 keeps an automatic rewrite from overwriting the
# child's counter dump.
start_server {tags {"setperf set aof external:skip needs:debug"} overrides {appendonly yes aof-use-rdb-preamble no auto-aof-rewrite-percentage 0}} {
    if {![wc_available]} {
        test "setperf-lifecycle (aof): skipped, server lacks WORK_COUNTERS" {
            skip "build with: make WORK_COUNTERS=yes"
        }
    } else {
    set lif_saved [wc_quiesce]

    proc lif_rewrite {} {
        waitForBgrewriteaof r
        lif_forget_child workctr-aofrw-child.txt
        r debug workctr reset
        r bgrewriteaof
        lif_wait_child workctr-aofrw-child.txt aof_rewrite_in_progress
        return [lif_child_counters workctr-aofrw-child.txt]
    }

    test "setperf-lifecycle: the AOF rewrite makes at most two passes over a set" {
        set n 20000
        foreach fx {none one all mostly_expired} {
            r flushall
            set key lif:ao:$fx
            wc_fixture $key $n $fx short 5
            set volatile [dict get [wc_setinfo $key] volatile]
            set d [lif_rewrite]
            wc_record $::cur_test "bgrewriteaof (child)" [dict create n $n ttl $fx enc hashtable] $d
            lif_note "AOF rewrite $fx: ht_iter_visits=[wc_get $d ht_iter_visits] set_iter_next=[wc_get $d set_iter_next] vset_entry_visits=[wc_get $d vset_entry_visits] cmds=[wc_get $d aof_rewrite_cmds] bytes=[wc_get $d aof_rewrite_bytes] volatile=$volatile"
            # Volatile members are written first, TTL-free members after: two
            # passes at most, never one pass per member.
            wc_assert_le "hashtable positions examined in the child" [wc_get $d ht_iter_visits] [expr {2 * $n + 64}] \
                "the rewrite is at most two passes over the set (n=$n, ttl=$fx)" "bgrewriteaof" $key
            wc_assert_le "commands written" [wc_get $d aof_rewrite_cmds] [expr {$n + 8}] \
                "the rewrite writes at most one command per member (n=$n, ttl=$fx)" "bgrewriteaof" $key
        }
    }

    test "setperf-lifecycle: the AOF rewrite must reach the volatile members through the index" {
        set n 20000
        # A set with ONE volatile member out of n, and the hash with the same
        # shape. rewriteHashObject walks the expiry index for its volatile pass
        # (hashTypeInitVolatileIterator -> vsetInitIterator), so the volatile
        # pass costs the volatile count; the same index is available to a set.
        r flushall
        wc_fixture lif:aoi:set $n one
        set ds [lif_rewrite]
        r flushall
        lif_hash_fixture lif:aoi:hash $n 0
        r hpexpireat lif:aoi:hash 99999999999999 fields 1 h0
        set dh [lif_rewrite]
        wc_record $::cur_test "bgrewriteaof (child, set)" [dict create n $n ttl one enc hashtable] $ds
        wc_record $::cur_test "bgrewriteaof (child, hash)" [dict create n $n ttl one enc hashtable type hash] $dh
        lif_note "AOF rewrite one-volatile: set ht_iter_visits=[wc_get $ds ht_iter_visits] vset_entry_visits=[wc_get $ds vset_entry_visits]; hash ht_iter_visits=[wc_get $dh ht_iter_visits] vset_entry_visits=[wc_get $dh vset_entry_visits]"
        wc_assert_le "hashtable positions examined in the child" [wc_get $ds ht_iter_visits] [expr {$n + 1 + 64}] \
            "the volatile pass is proportional to the volatile members, not to the population (n=$n, volatile=1)" \
            "bgrewriteaof" lif:aoi:set "hash with the same shape: ht_iter_visits=[wc_get $dh ht_iter_visits] vset_entry_visits=[wc_get $dh vset_entry_visits]"
    }

    test "setperf-lifecycle: the AOF rewrite of an all-volatile set stays linear" {
        set sizes {2000 20000}
        set per_member {}
        set lif_ao_cmds {}
        foreach n $sizes {
            r flushall
            wc_fixture lif:aoa:none $n none
            set dn [lif_rewrite]
            r flushall
            wc_fixture lif:aoa:all $n all
            set da [lif_rewrite]
            wc_record $::cur_test "bgrewriteaof (child)" [dict create n $n ttl all enc hashtable] $da
            lappend per_member [expr {[wc_get $da aof_rewrite_bytes] / $n}]
            lappend lif_ao_cmds [wc_get $da aof_rewrite_cmds]
            # One SADDEX per volatile member repeats the key and the deadline;
            # TTL-free members are batched 64 per SADD. Equal deadlines could be
            # batched the same way (SADDEX takes MEMBERS <n> ...), and the hash
            # rewriter has the same one-command-per-field shape, so this is
            # recorded rather than gated.
            lif_note "AOF rewrite n=$n: none cmds=[wc_get $dn aof_rewrite_cmds] bytes=[wc_get $dn aof_rewrite_bytes] ([expr {[wc_get $dn aof_rewrite_bytes] / $n}] B/member); all cmds=[wc_get $da aof_rewrite_cmds] bytes=[wc_get $da aof_rewrite_bytes] ([expr {[wc_get $da aof_rewrite_bytes] / $n}] B/member)"
            wc_assert_le "commands written" [wc_get $da aof_rewrite_cmds] [expr {$n + 8}] \
                "at most one command per volatile member (n=$n)" "bgrewriteaof" lif:aoa:all
        }
        # Bytes per member must not grow with the set: the encoding of one
        # member is fixed, only the member name grows a digit.
        wc_assert_flat "AOF bytes written per volatile member" $sizes $per_member 2 8 \
            "the rewrite is linear in members (cmds {$lif_ao_cmds})" "bgrewriteaof" lif:aoa:all
    }

    test "setperf-lifecycle: the rewritten AOF restores members and TTLs" {
        set n 2000
        r flushall
        wc_fixture lif:aol:all $n all
        wc_fixture lif:aol:mix $n one
        set members [lsort [r smembers lif:aol:all]]
        set mixed [lsort [r smembers lif:aol:mix]]
        set deadlines {}
        foreach m [list m0 m1 m[expr {$n - 1}]] {
            lappend deadlines [r spexpiretime lif:aol:all members 1 $m] [r spexpiretime lif:aol:mix members 1 $m]
        }
        lif_rewrite
        r debug loadaof
        assert_equal $members [lsort [r smembers lif:aol:all]]
        assert_equal $mixed [lsort [r smembers lif:aol:mix]]
        set after {}
        foreach m [list m0 m1 m[expr {$n - 1}]] {
            lappend after [r spexpiretime lif:aol:all members 1 $m] [r spexpiretime lif:aol:mix members 1 $m]
        }
        assert_equal $deadlines $after
        assert_equal $n [dict get [wc_setinfo lif:aol:all] volatile]
        assert_equal 1 [dict get [wc_setinfo lif:aol:mix] volatile]
    }

    wc_restore $lif_saved
    }
}
