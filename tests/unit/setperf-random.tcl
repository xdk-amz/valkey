# Performance contracts for SPOP / SRANDMEMBER on sets with member TTLs.
#
# Requires a server built with `make WORK_COUNTERS=yes`; the counters are read
# through DEBUG WORKCTR (helpers in tests/support/workctr.tcl). Budgets are
# derived from the parent algorithms (the no-TTL fixture runs the parent code
# paths unchanged) and from source inspection, never from the measured commit.
#
# Contract A: a member TTL must not switch a request-sized random selection
# into a population-sized traversal or a population-sized temporary copy.
# Contract A-reclaim: expired members met during selection are reclaimed, so
# repeated commands do not rescan the same expired population.

start_server {tags {"setperf set external:skip needs:debug"}} {
    if {![wc_available]} {
        test "setperf-random: skipped, server lacks WORK_COUNTERS" {
            skip "build with: make WORK_COUNTERS=yes"
        }
    } else {
    set saved [wc_quiesce]

    # ---- hashtable: fixed small request, growing population -------------
    foreach cmdspec {
        {"SPOP key"            {spop $key}}
        {"SPOP key 1"          {spop $key 1}}
        {"SPOP key 2"          {spop $key 2}}
        {"SRANDMEMBER key"     {srandmember $key}}
        {"SRANDMEMBER key 2"   {srandmember $key 2}}
        {"SRANDMEMBER key -5"  {srandmember $key -5}}
    } {
        lassign $cmdspec label cmdtpl
        test "setperf-random: $label hashtable, one future TTL adds no population scan" {
            set sizes [wc_ht_sizes]
            set none_vals {}
            set one_vals {}
            foreach n $sizes {
                foreach ttl {none one all} {
                    set key "sp:$ttl"
                    wc_fixture $key $n $ttl
                    set cmda($ttl) [subst -nocommands $cmdtpl]
                    set d($ttl) [wc_measure "r $cmda($ttl)"]
                    wc_record $::cur_test $cmda($ttl) [dict create n $n ttl $ttl enc hashtable] $d($ttl)
                    assert_equal [dict get [wc_setinfo $key] encoding] hashtable
                }
                set ex_none [wc_ht_examined $d(none)]
                set ex_one [wc_ht_examined $d(one)]
                set ex_all [wc_ht_examined $d(all)]
                lappend none_vals $ex_none
                lappend one_vals $ex_one
                # The parent sampler examines FAIR_RANDOM_SAMPLE_SIZE (70) entries
                # per pick; the largest request here is 5 picks (SRANDMEMBER -5), so
                # <= ~700 entries plus the key lookup: 1024 is request-sized.
                wc_assert_le "hashtable entries examined (plain)" $ex_none 1024 \
                    "the no-TTL path must stay request-sized (n=$n)" $cmda(none) sp:none
                # Same live contents, one member carries a TTL: work must stay
                # within a small factor of the parent sampler (sampler noise).
                wc_assert_ratio "hashtable entries examined (iter+scan+probes)" $ex_one $ex_none 4 500 \
                    "one future TTL must not add population-sized traversal (n=$n)" $cmda(one) sp:one
                wc_assert_ratio "hashtable entries examined (iter+scan+probes)" $ex_all $ex_none 4 500 \
                    "all-future TTLs must not add population-sized traversal (n=$n)" $cmda(all) sp:all
                wc_assert_ratio "full-iteration reservoir passes" [wc_get $d(one) set_reservoir_passes] 0 1 0 \
                    "a live set must not be selected by a full reservoir pass (n=$n)" $cmda(one) sp:one
                # Temporary storage and member copies must stay request-sized.
                wc_assert_ratio "largest single allocation (bytes)" [wc_get $d(one) mem_max_alloc] [wc_get $d(none) mem_max_alloc] 2 1024 \
                    "one future TTL must not add a population-sized temporary array (n=$n)" $cmda(one) sp:one
                wc_assert_ratio "string objects created" [wc_get $d(one) str_objs_created] [wc_get $d(none) str_objs_created] 3 8 \
                    "one future TTL must not multiply member copies (n=$n)" $cmda(one) sp:one
            }
            # Fixed request across geometrically growing populations: flat.
            wc_assert_flat "hashtable entries examined (plain, sanity)" $sizes $none_vals 4 500 \
                "parent sampler is request-sized" $cmda(none) sp:none
            wc_assert_flat "hashtable entries examined (one TTL)" $sizes $one_vals 4 500 \
                "request-sized work must not grow with the population" $cmda(one) sp:one
        } {} {slow}
    }

    test "setperf-random: SRANDMEMBER key 2 hashtable, long payloads do not change the contract" {
        set n 20000
        foreach ttl {none one} {
            wc_fixture sp:$ttl $n $ttl long
            set d($ttl) [wc_measure {r srandmember sp:$ttl 2}]
            wc_record $::cur_test "srandmember key 2" [dict create n $n ttl $ttl enc hashtable payload long] $d($ttl)
        }
        wc_assert_ratio "hashtable entries examined" [wc_ht_examined $d(one)] [wc_ht_examined $d(none)] 4 500 \
            "one future TTL must not add population-sized traversal (long payload)" "srandmember key 2" sp:one
        wc_assert_ratio "bytes hashed" [wc_get $d(one) ht_hash_bytes] [wc_get $d(none) ht_hash_bytes] 4 512 \
            "one future TTL must not multiply hashing work" "srandmember key 2" sp:one
    }

    # ---- hashtable: counts at / above cardinality ------------------------
    test "setperf-random: SRANDMEMBER count >= cardinality streams without a population copy" {
        set n 20000
        foreach ttl {none one} {
            wc_fixture sp:$ttl $n $ttl
            set d($ttl) [wc_measure {r srandmember sp:$ttl [expr {$n + 10}]}]
            assert_equal [llength $::wc_last_reply] $n
            wc_record $::cur_test "srandmember key n+10" [dict create n $n ttl $ttl enc hashtable] $d($ttl)
        }
        # One pass over the table is inherent; a second copy of every member is not.
        wc_assert_ratio "hashtable entries examined" [wc_ht_examined $d(one)] [wc_ht_examined $d(none)] 2 500 \
            "returning every member is one traversal" "srandmember key n+10" sp:one
        wc_assert_ratio "largest single allocation (bytes)" [wc_get $d(one) mem_max_alloc] [wc_get $d(none) mem_max_alloc] 2 4096 \
            "streaming all members needs no population-sized selection array" "srandmember key n+10" sp:one
        wc_assert_ratio "peak live auxiliary bytes" [wc_get $d(one) mem_peak_live_delta] [wc_get $d(none) mem_peak_live_delta] 2 4096 \
            "streaming all members needs no population-sized auxiliary memory" "srandmember key n+10" sp:one
    }

    test "setperf-random: SRANDMEMBER large negative count on hashtable is per-result sampling" {
        set n 20000
        foreach ttl {none one} {
            wc_fixture sp:$ttl $n $ttl
            set d($ttl) [wc_measure {r srandmember sp:$ttl -3000}]
            assert_equal [llength $::wc_last_reply] 3000
            wc_record $::cur_test "srandmember key -3000" [dict create n $n ttl $ttl enc hashtable] $d($ttl)
        }
        wc_assert_ratio "hashtable entries examined" [wc_ht_examined $d(one)] [wc_ht_examined $d(none)] 4 500 \
            "negative count keeps per-result sampling" "srandmember key -3000" sp:one
    }

    # ---- listpack ---------------------------------------------------------
    foreach cmdspec {
        {"SPOP key"            {spop $key}}
        {"SPOP key 3"          {spop $key 3}}
        {"SRANDMEMBER key"     {srandmember $key}}
        {"SRANDMEMBER key 3"   {srandmember $key 3}}
        {"SRANDMEMBER key -100" {srandmember $key -100}}
    } {
        lassign $cmdspec label cmdtpl
        test "setperf-random: $label listpack, one future TTL keeps single-traversal selection" {
            foreach n {16 64 128} {
                foreach ttl {none one} {
                    set key "lp:$ttl"
                    wc_fixture $key $n $ttl
                    assert_equal [dict get [wc_setinfo $key] encoding] listpack
                    set cmda($ttl) [subst -nocommands $cmdtpl]
                    set d($ttl) [wc_measure "r $cmda($ttl)"]
                    wc_record $::cur_test $cmda($ttl) [dict create n $n ttl $ttl enc listpack] $d($ttl)
                }
                set cmdone $cmda(one)
                # Listpack selection is inherently O(n) per traversal; the parent
                # does a bounded number of traversals per command (one per
                # <=1000 results for negative counts). A TTL may add one
                # validation pass, not one traversal per result.
                wc_assert_ratio "listpack entries examined (find+next+random steps)" [wc_lp_examined $d(one)] [wc_lp_examined $d(none)] 3 [expr {2 * $n}] \
                    "one future TTL must not add per-result traversals (n=$n)" $cmdone lp:one
                wc_assert_ratio "string objects created" [wc_get $d(one) str_objs_created] [wc_get $d(none) str_objs_created] 3 8 \
                    "one future TTL must not multiply member copies (n=$n)" $cmdone lp:one
            }
        }
    }

    # ---- expired members: reclaim instead of repeated rejection ------------
    foreach cmdspec {
        {"SPOP key"          {spop $key}}
        {"SPOP key 1"        {spop $key 1}}
        {"SRANDMEMBER key"   {srandmember $key}}
        {"SRANDMEMBER key 2" {srandmember $key 2}}
    } {
        lassign $cmdspec label cmdtpl
        test "setperf-random: $label mostly-expired hashtable is reclaimed, not rescanned" {
            set n 20000
            set rounds 10
            set live_keep 40
            # Budget per command from the plain set holding just the live members.
            wc_fixture sp:plain $live_keep none
            wc_force_hashtable sp:plain
            set key sp:plain
            set cmd [subst -nocommands $cmdtpl]
            set base [wc_measure "r $cmd"]
            set per_cmd [expr {[wc_ht_examined $base] * 4 + 500}]

            set key sp:mexp
            wc_fixture $key $n mostly_expired short $live_keep
            set info [wc_setinfo $key]
            assert_equal [dict get $info live] $live_keep
            assert_equal [dict get $info physical] $n
            set total 0
            set cmd [subst -nocommands $cmdtpl]
            for {set i 0} {$i < $rounds} {incr i} {
                set m [wc_measure "r $cmd"]
                incr total [wc_ht_examined $m]
                wc_record $::cur_test $cmd [dict create n $n ttl mostly_expired enc hashtable round $i] $m
            }
            set after [wc_setinfo $key]
            # Either the expired members met during selection are reclaimed (one
            # cleanup pass over the population, legitimate for a write command)
            # or selection stays request-sized; rescanning the same unreclaimed
            # population on every command is neither. A read command may not
            # mutate, so for SRANDMEMBER only the second option applies.
            wc_assert_le "hashtable entries examined over $rounds commands" $total \
                [expr {3 * $n + $rounds * $per_cmd}] \
                "a mostly-expired set must be reclaimed once or sampled request-sized, not rescanned every command" \
                $cmd $key "after: [list $after]"
        } {} {slow}
    }

    test "setperf-random: all-expired hashtable terminates and is reclaimed by the first pass" {
        set n 20000
        set key sp:aexp
        wc_fixture $key $n all_expired
        set total 0
        foreach cmd {{spop sp:aexp} {spop sp:aexp 3} {srandmember sp:aexp} {srandmember sp:aexp 3} {spop sp:aexp}} {
            set m [wc_measure "r $cmd"]
            incr total [wc_ht_examined $m]
            wc_record $::cur_test $cmd [dict create n $n ttl all_expired enc hashtable] $m
            # Correct replies: nothing is live.
            switch -- [llength $cmd] {
                2 { assert_equal $::wc_last_reply {} }
                3 { assert_equal $::wc_last_reply {} }
            }
        }
        set after [wc_setinfo $key]
        # 5 commands; one population pass plus bounded sampler retries each.
        wc_assert_le "hashtable entries examined over 5 commands" $total [expr {3 * $n + 5 * 8000}] \
            "an all-expired set is reclaimed by the first pass or detected request-sized, not rescanned by every command" \
            "spop/srandmember sp:aexp" $key "after: [list $after]"
    }

    test "setperf-random: listpack SPOP on expired members deletes each once" {
        set n 128
        set key lp:mexp
        wc_fixture $key $n mostly_expired short 8
        set m [wc_measure {r spop lp:mexp 2}]
        wc_record $::cur_test "spop key 2" [dict create n $n ttl mostly_expired enc listpack] $m
        set after [wc_setinfo $key]
        assert_equal [llength $::wc_last_reply] 2
        wc_assert_le "listpack deletions" [wc_get $m lp_deletes] [expr {2 + [dict get $after live] + 120}] \
            "popping from a listpack deletes each selected or reclaimed member once" "spop key 2" $key
        # Reclaiming 120 expired members is one pass; not one traversal per member.
        wc_assert_le "listpack entries examined" [wc_lp_examined $m] [expr {6 * $n}] \
            "listpack SPOP on an expired-heavy set is a bounded number of traversals" "spop key 2" $key "after: [list $after]"
    }

    wc_restore $saved
    }
}

# Replicas apply IGNORE_EXPIRE: every physical member is visible and nothing
# may be reclaimed there, but selection must still be request-sized.
start_server {tags {"setperf set repl external:skip needs:debug"}} {
    if {![wc_available]} {
        test "setperf-random (replica): skipped, server lacks WORK_COUNTERS" {
            skip "build with: make WORK_COUNTERS=yes"
        }
    } else {
    start_server {} {
        set primary [srv -1 client]
        set replica [srv 0 client]
        $replica replicaof [srv -1 host] [srv -1 port]
        wait_for_sync $replica
        set saved [wc_quiesce $primary]
        $replica debug set-active-expire 0

        test "setperf-random: replica SRANDMEMBER key 2 on a one-TTL set is request-sized" {
            set n 20000
            foreach ttl {none one} {
                wc_fixture sp:$ttl $n $ttl short 3 $primary
            }
            wait_for_ofs_sync $primary $replica
            foreach ttl {none one} {
                set d($ttl) [wc_measure "\$replica srandmember sp:$ttl 2" $replica]
                assert_equal [llength $::wc_last_reply] 2
                wc_record $::cur_test "srandmember key 2 (replica)" [dict create n $n ttl $ttl enc hashtable role replica] $d($ttl)
            }
            wc_assert_ratio "hashtable entries examined (replica)" [wc_ht_examined $d(one)] [wc_ht_examined $d(none)] 4 500 \
                "replica reads of a one-TTL set stay request-sized" "srandmember key 2" sp:one
        }

        test "setperf-random: replica SRANDMEMBER on a mostly-expired set hides expired members without reclaiming" {
            set n 20000
            set live_keep 40
            wc_fixture sp:mexp $n mostly_expired short $live_keep $primary
            wait_for_ofs_sync $primary $replica
            set info [wc_setinfo sp:mexp $replica]
            # A replica reports expired members as gone but must not delete them.
            assert_equal [dict get $info live] $live_keep
            assert_equal [dict get $info physical] $n
            set total 0
            for {set i 0} {$i < 5} {incr i} {
                set m [wc_measure {$replica srandmember sp:mexp 2} $replica]
                incr total [wc_ht_examined $m]
                assert_equal [llength $::wc_last_reply] 2
                foreach mem $::wc_last_reply { assert_morethan_equal [$replica sttl sp:mexp members 1 $mem] -1 }
                wc_record $::cur_test "srandmember key 2 (replica)" [dict create n $n ttl mostly_expired enc hashtable role replica round $i] $m
            }
            assert_equal [dict get [wc_setinfo sp:mexp $replica] physical] $n
            # No reclaim is allowed here, so the work is reported, not gated:
            # the primary-side reclaim contract is tested in the primary block.
            if {$::verbose} { puts "replica mostly-expired: $total hashtable entries examined over 5 commands (no reclaim permitted)" }
        }
        wc_restore $saved $primary
    }
    }
}
