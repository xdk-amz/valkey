# Performance contracts for SPOP / SRANDMEMBER on sets with member TTLs.
#
# Requires a server built with `make WORK_COUNTERS=yes`; the counters are read
# through DEBUG WORKCTR (helpers in tests/support/workctr.tcl). Budgets are
# derived from the parent algorithms (the no-TTL fixture runs the parent code
# paths unchanged) and from source inspection, never from the measured commit.
#
# Contract A: a member TTL must not switch a request-sized random selection
# into a population-sized traversal or a population-sized temporary copy.
# Contract A-hidden: an expired member is hidden, not deleted -- no selection
# path reclaims it, propagates it or writes because of it, so it survives every
# read and pop until active expiration runs, and the cost of stepping over it
# must stay request-sized (bounded probes, at most one fallback pass).

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

    # ---- dense hidden population: bounded probes, one fallback pass ---------
    #
    # Expired members are hidden, not deleted on the selection path, so a set
    # whose population is mostly hidden must still terminate, must return live
    # members only, and must pay at most ONE population pass per command -- the
    # fallback the sampler reaches for once repeated probes keep landing on
    # hidden members -- never one pass per pick. Reclamation belongs to active
    # expiration, so no number of reads or pops changes the physical population
    # beyond the members a pop returned.
    foreach cmdspec {
        {"SPOP key"          {spop $key}}
        {"SPOP key 1"        {spop $key 1}}
        {"SRANDMEMBER key"   {srandmember $key}}
        {"SRANDMEMBER key 2" {srandmember $key 2}}
    } {
        lassign $cmdspec label cmdtpl
        test "setperf-random: $label on a mostly-hidden hashtable is bounded probes plus at most one fallback pass" {
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
            set live [wc_fixture $key $n mostly_expired short $live_keep]
            set info [wc_setinfo $key]
            assert_equal [dict get $info live] $live_keep
            assert_equal [dict get $info physical] $n
            set cmd [subst -nocommands $cmdtpl]
            set is_pop [string match spop* $cmd]
            set popped 0
            set reclaimed 0
            for {set i 0} {$i < $rounds} {incr i} {
                set m [wc_measure "r $cmd"]
                foreach mem $::wc_last_reply {
                    if {[lsearch -exact $live $mem] == -1} {
                        fail "CONTRACT VIOLATED: a hidden member must never be returned\n  $cmd returned '$mem'; live members: $live\n  [wc_ctx $cmd $key] \n  [wc_repro $cmd]"
                    }
                }
                if {$is_pop} {
                    incr popped [llength $::wc_last_reply]
                    foreach mem $::wc_last_reply { set live [lsearch -all -inline -not -exact $live $mem] }
                }
                incr reclaimed [wc_get $m set_members_reclaimed]
                wc_assert_le "full-iteration passes in one command" [wc_get $m set_reservoir_passes] 1 \
                    "a dense hidden population costs at most one fallback pass per command, not one per pick" $cmd $key
                wc_assert_le "hashtable entries examined in one command" [wc_ht_examined $m] [expr {2 * $n + $per_cmd}] \
                    "one fallback pass plus a request-sized probe sequence bounds the command" $cmd $key
                wc_record $::cur_test $cmd [dict create n $n ttl mostly_expired enc hashtable round $i] $m
            }
            set after [wc_setinfo $key]
            wc_assert_le "members reclaimed over $rounds commands" $reclaimed 0 \
                "hidden members are reclaimed by active expiration, never by a selection path" \
                $cmd $key "after: [list $after]"
            wc_assert_eq "physical members after $rounds commands" [dict get $after physical] [expr {$n - $popped}] \
                "the hidden population survives every read and every pop" $cmd $key "popped=$popped"
        } {} {slow}
    }

    test "setperf-random: an all-hidden hashtable terminates, survives, and is left to active expiration" {
        set n 20000
        set key sp:aexp
        wc_fixture $key $n all_expired
        set reclaimed 0
        foreach cmd {{spop sp:aexp} {spop sp:aexp 3} {srandmember sp:aexp} {srandmember sp:aexp 3} {spop sp:aexp}} {
            set m [wc_measure "r $cmd"]
            # Nothing is live, so every form replies empty and terminates.
            assert_equal $::wc_last_reply {}
            incr reclaimed [wc_get $m set_members_reclaimed]
            wc_record $::cur_test $cmd [dict create n $n ttl all_expired enc hashtable] $m
            wc_assert_le "full-iteration passes in one command" [wc_get $m set_reservoir_passes] 1 \
                "an all-hidden set is detected in at most one pass, not one per pick" $cmd $key
            wc_assert_le "hashtable entries examined in one command" [wc_ht_examined $m] [expr {2 * $n + 8000}] \
                "one fallback pass plus bounded probes bounds the command" $cmd $key
            # Checked before reading the fixture: a command that emptied the set
            # has removed the key, and DEBUG WORKCTR SETINFO would error instead
            # of reporting the violation.
            wc_assert_eq "key still exists" [r exists $key] 1 \
                "a command that returns nothing must not delete the hidden population or its key" \
                $cmd $key "reclaimed so far: $reclaimed"
            wc_assert_eq "physical members" [dict get [wc_setinfo $key] physical] $n \
                "every member is still allocated" $cmd $key
        }
        wc_assert_le "members reclaimed over 5 commands" $reclaimed 0 \
            "an empty reply is not a licence to delete: active expiration reclaims, selection does not" \
            "spop/srandmember sp:aexp" $key
        assert_equal $n [r scard $key]
        # And active expiration is what does remove them.
        r debug set-active-expire 1
        wait_for_condition 100 50 {
            [r exists sp:aexp] == 0
        } else {
            fail "active expiration did not reclaim the all-hidden set"
        }
        r debug set-active-expire 0
    } {OK} {slow}

    test "setperf-random: listpack SPOP leaves the hidden members in place and deletes only what it returns" {
        set n 128
        set key lp:mexp
        set live [wc_fixture $key $n mostly_expired short 8]
        assert_equal [dict get [wc_setinfo $key] physical] $n
        set m [wc_measure {r spop lp:mexp 2}]
        wc_record $::cur_test "spop key 2" [dict create n $n ttl mostly_expired enc listpack] $m
        assert_equal [llength $::wc_last_reply] 2
        foreach mem $::wc_last_reply {
            if {[lsearch -exact $live $mem] == -1} {
                fail "CONTRACT VIOLATED: a hidden member must never be popped\n  spop lp:mexp 2 returned '$mem'; live members: $live\n  [wc_ctx "spop key 2" $key]"
            }
        }
        set after [wc_setinfo $key]
        wc_assert_le "members reclaimed" [wc_get $m set_members_reclaimed] 0 \
            "a listpack pop must not reclaim the hidden members it steps past" "spop key 2" $key
        wc_assert_eq "physical members after the pop" [dict get $after physical] [expr {$n - 2}] \
            "only the two returned members are deleted" "spop key 2" $key
        wc_assert_le "listpack deletions" [wc_get $m lp_deletes] 2 \
            "popping two members deletes two entries, not the hidden population" "spop key 2" $key
        wc_assert_le "listpack entries examined" [wc_lp_examined $m] [expr {6 * $n}] \
            "listpack SPOP on a mostly-hidden set is a bounded number of traversals" "spop key 2" $key "after: [list $after]"
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
            set reclaimed 0
            for {set i 0} {$i < 5} {incr i} {
                set m [wc_measure {$replica srandmember sp:mexp 2} $replica]
                incr total [wc_ht_examined $m]
                incr reclaimed [wc_get $m set_members_reclaimed]
                assert_equal [llength $::wc_last_reply] 2
                foreach mem $::wc_last_reply { assert_morethan_equal [$replica sttl sp:mexp members 1 $mem] -1 }
                wc_assert_le "full-iteration passes in one command" [wc_get $m set_reservoir_passes] 1 \
                    "a dense hidden population costs at most one fallback pass per replica read" "srandmember key 2" sp:mexp
                wc_record $::cur_test "srandmember key 2 (replica)" [dict create n $n ttl mostly_expired enc hashtable role replica round $i] $m
            }
            set after [wc_setinfo sp:mexp $replica]
            wc_assert_le "members reclaimed on the replica" $reclaimed 0 \
                "a replica reclaims nothing on its own" "srandmember key 2" sp:mexp
            wc_assert_eq "physical members on the replica" [dict get $after physical] $n \
                "the replica's copy of the hidden population is untouched" "srandmember key 2" sp:mexp
            if {$::verbose} { puts "replica mostly-expired: $total hashtable entries examined over 5 commands" }
        }

        # One hidden member on a replica is the steady state the primary-side
        # gates cannot see: no policy there ever reclaims it, so a rule of the
        # form "this set holds an expired member, take the full-traversal path"
        # makes every SRANDMEMBER on the replica O(n), for the lifetime of the
        # member, at every population size.
        foreach cmdspec {
            {"SRANDMEMBER key"    {srandmember $key}}
            {"SRANDMEMBER key 2"  {srandmember $key 2}}
            {"SRANDMEMBER key -5" {srandmember $key -5}}
        } {
            lassign $cmdspec label cmdtpl
            test "setperf-random: replica $label with one hidden member stays request-sized" {
                set sizes [wc_ht_sizes]
                set exp_vals {}
                foreach n $sizes {
                    foreach ttl {none one_expired} {
                        wc_fixture spr:$ttl $n $ttl short 3 $primary
                    }
                    wait_for_ofs_sync $primary $replica
                    foreach ttl {none one_expired} {
                        set key "spr:$ttl"
                        set cmda($ttl) [subst -nocommands $cmdtpl]
                        set d($ttl) [wc_measure "\$replica $cmda($ttl)" $replica]
                        set reply($ttl) $::wc_last_reply
                        wc_record $::cur_test $cmda($ttl) [dict create n $n ttl $ttl enc hashtable role replica] $d($ttl)
                        assert_equal [dict get [wc_setinfo $key $replica] encoding] hashtable
                    }
                    # The hidden member is invisible to the read, and the reply
                    # has the same shape as on the plain set.
                    assert_equal [lsearch -exact $reply(one_expired) m0] -1
                    assert_equal [llength $reply(one_expired)] [llength $reply(none)]
                    set info [wc_setinfo spr:one_expired $replica]
                    wc_assert_eq "physical members on the replica" [dict get $info physical] $n \
                        "a replica read changes nothing: the hidden member stays allocated (n=$n)" \
                        $cmda(one_expired) spr:one_expired
                    wc_assert_le "members reclaimed on the replica" [wc_get $d(one_expired) set_members_reclaimed] 0 \
                        "a replica must not reclaim a hidden member (n=$n)" $cmda(one_expired) spr:one_expired
                    wc_assert_le "commands propagated by a replica read" [wc_propagated $d(one_expired)] 0 \
                        "a replica read propagates nothing (n=$n)" $cmda(one_expired) spr:one_expired
                    wc_assert_le "full-iteration reservoir passes" [wc_get $d(one_expired) set_reservoir_passes] 0 \
                        "one hidden member must not send a replica read through a full reservoir pass (n=$n)" \
                        $cmda(one_expired) spr:one_expired
                    set ex_exp [wc_ht_examined $d(one_expired)]
                    lappend exp_vals $ex_exp
                    wc_assert_ratio "hashtable entries examined (replica)" $ex_exp [wc_ht_examined $d(none)] 4 500 \
                        "one hidden member must not add population-sized traversal to a replica read (n=$n)" \
                        $cmda(one_expired) spr:one_expired
                }
                wc_assert_flat "hashtable entries examined (replica, one hidden member)" $sizes $exp_vals 4 500 \
                    "the request is the same at every size, so the work must be too" \
                    "$label (replica)" spr:one_expired
            } {} {slow}
        }
        wc_restore $saved $primary
    }
    }
}

# One expired, unreclaimed member in an otherwise live population. Expired
# members are hidden (like hash fields), so nothing on the selection path may
# remove it and it stays in the set until active expiration runs. A small
# request must still be request-sized: the sampler rejects the one expired
# pick it may land on (probability 1/n) and re-samples. Any rule of the form
# "the set holds an expired member, so take the full-traversal path" turns
# every SPOP/SRANDMEMBER on a million-member set into O(n), and because the
# member is never reclaimed, every subsequent command repeats that work.
start_server {tags {"setperf set external:skip needs:debug"}} {
    if {![wc_available]} {
        test "setperf-random (one expired): skipped, server lacks WORK_COUNTERS" {
            skip "build with: make WORK_COUNTERS=yes"
        }
    } else {
    set saved [wc_quiesce]

    foreach cmdspec {
        {"SPOP key"            {spop $key}}
        {"SPOP key 1"          {spop $key 1}}
        {"SPOP key 2"          {spop $key 2}}
        {"SRANDMEMBER key"     {srandmember $key}}
        {"SRANDMEMBER key 2"   {srandmember $key 2}}
        {"SRANDMEMBER key -5"  {srandmember $key -5}}
    } {
        lassign $cmdspec label cmdtpl
        test "setperf-random: $label hashtable, one expired member adds no population scan" {
            set sizes [wc_ht_sizes]
            set none_vals {}
            set exp_vals {}
            foreach n $sizes {
                foreach ttl {none one_expired} {
                    set key "sp:$ttl"
                    wc_fixture $key $n $ttl
                    set cmda($ttl) [subst -nocommands $cmdtpl]
                    set dirty0 [wc_dirty]
                    set d($ttl) [wc_measure "r $cmda($ttl)"]
                    set reply($ttl) $::wc_last_reply
                    set dirty($ttl) [expr {[wc_dirty] - $dirty0}]
                    wc_record $::cur_test $cmda($ttl) [dict create n $n ttl $ttl enc hashtable] $d($ttl)
                    assert_equal [dict get [wc_setinfo $key] encoding] hashtable
                }
                # Correctness first: the hidden member is never returned and the
                # reply has the requested shape.
                assert_equal [lsearch -exact $reply(one_expired) m0] -1
                assert_equal [llength $reply(one_expired)] [llength $reply(none)]
                set is_pop [string match spop* $cmda(one_expired)]
                set popped [expr {$is_pop ? [llength $reply(one_expired)] : 0}]
                set info [wc_setinfo sp:one_expired]
                assert_equal [dict get $info live] [expr {$n - 1 - $popped}]
                # Hide-only semantics: nothing on the selection path reclaims the
                # hidden member, so the only members that leave are the ones SPOP
                # was asked to pop and the physical count says so exactly.
                wc_assert_le "members reclaimed during selection" [wc_get $d(one_expired) set_members_reclaimed] 0 \
                    "an expired member stays hidden until active expiration reclaims it; selection must not reclaim (n=$n)" \
                    $cmda(one_expired) sp:one_expired
                wc_assert_eq "physical members after the command" [dict get $info physical] [expr {$n - $popped}] \
                    "only the members returned by SPOP leave the set; the hidden member survives (n=$n)" \
                    $cmda(one_expired) sp:one_expired "returned=[llength $reply(one_expired)] popped=$popped"
                wc_assert_eq "volatile members after the command" [dict get $info volatile] 1 \
                    "the hidden member keeps its index entry until active expiration runs (n=$n)" \
                    $cmda(one_expired) sp:one_expired
                if {$is_pop} {
                    # A pop writes exactly what it returned, so the propagated
                    # volume must not exceed the no-TTL command's by a member.
                    wc_assert_ratio "propagated arguments" [wc_sum $d(one_expired) prop_args prop_dropped_args] \
                        [wc_sum $d(none) prop_args prop_dropped_args] 1 2 \
                        "SPOP propagates the members it returned, not the hidden one (n=$n)" \
                        $cmda(one_expired) sp:one_expired
                } else {
                    # SRANDMEMBER is a read: no keyspace change, no propagation.
                    wc_assert_le "keyspace changes booked by a read" $dirty(one_expired) 0 \
                        "SRANDMEMBER must not write (n=$n)" $cmda(one_expired) sp:one_expired
                    wc_assert_le "commands propagated by a read" [wc_propagated $d(one_expired)] 0 \
                        "SRANDMEMBER must not propagate (n=$n)" $cmda(one_expired) sp:one_expired
                }

                set ex_none [wc_ht_examined $d(none)]
                set ex_exp [wc_ht_examined $d(one_expired)]
                lappend none_vals $ex_none
                lappend exp_vals $ex_exp
                # Same budget as the one-future-TTL contract: the only extra work
                # a single hidden member can add is one rejected pick.
                wc_assert_ratio "hashtable entries examined (iter+scan+probes)" $ex_exp $ex_none 4 500 \
                    "one expired member must not add population-sized traversal (n=$n)" $cmda(one_expired) sp:one_expired
                wc_assert_ratio "full-iteration reservoir passes" [wc_get $d(one_expired) set_reservoir_passes] 0 1 0 \
                    "a set with one hidden member must not be selected by a full reservoir pass (n=$n)" $cmda(one_expired) sp:one_expired
                wc_assert_ratio "largest single allocation (bytes)" [wc_get $d(one_expired) mem_max_alloc] [wc_get $d(none) mem_max_alloc] 2 1024 \
                    "one expired member must not add a population-sized temporary array (n=$n)" $cmda(one_expired) sp:one_expired
                wc_assert_ratio "string objects created" [wc_get $d(one_expired) str_objs_created] [wc_get $d(none) str_objs_created] 3 8 \
                    "one expired member must not multiply member copies (n=$n)" $cmda(one_expired) sp:one_expired
            }
            wc_assert_flat "hashtable entries examined (one expired member)" $sizes $exp_vals 4 500 \
                "request-sized work must not grow with the population" $cmda(one_expired) sp:one_expired
        } {} {slow}
    }

    # The hidden member persists across commands (no reclaim on the selection
    # path), so the cost must be request-sized on every command, not just the
    # first: no per-command population pass, and no cumulative pass either.
    foreach cmdspec {
        {"SPOP key 1"        {spop $key 1}}
        {"SRANDMEMBER key 2" {srandmember $key 2}}
    } {
        lassign $cmdspec label cmdtpl
        test "setperf-random: repeated $label on a hashtable with one expired member stays request-sized" {
            set n 200000
            set rounds 10
            set key sp:none
            wc_fixture $key $n none
            set cmd [subst -nocommands $cmdtpl]
            set base [wc_measure "r $cmd"]
            set per_cmd [expr {[wc_ht_examined $base] * 4 + 500}]

            set key sp:one_expired
            wc_fixture $key $n one_expired
            set cmd [subst -nocommands $cmdtpl]
            set total 0
            set passes 0
            set reclaimed 0
            set popped 0
            set is_pop [string match spop* $cmd]
            for {set i 0} {$i < $rounds} {incr i} {
                set m [wc_measure "r $cmd"]
                incr total [wc_ht_examined $m]
                incr passes [wc_get $m set_reservoir_passes]
                incr reclaimed [wc_get $m set_members_reclaimed]
                if {$is_pop} { incr popped [llength $::wc_last_reply] }
                assert_equal [lsearch -exact $::wc_last_reply m0] -1
                wc_record $::cur_test $cmd [dict create n $n ttl one_expired enc hashtable round $i] $m
            }
            set after [wc_setinfo $key]
            wc_assert_le "hashtable entries examined over $rounds commands" $total [expr {$rounds * $per_cmd}] \
                "a hidden member must not cost a population pass per command" $cmd $key "after: [list $after]"
            wc_assert_le "full-iteration reservoir passes over $rounds commands" $passes 0 \
                "a hidden member must not trigger reservoir passes" $cmd $key "after: [list $after]"
            # The member stays hidden for all $rounds commands: that is what makes
            # a per-command population pass a steady-state cost rather than a
            # one-off, so its survival is part of the same contract.
            wc_assert_le "members reclaimed over $rounds commands" $reclaimed 0 \
                "no command on the selection path may reclaim the hidden member" $cmd $key "after: [list $after]"
            wc_assert_eq "physical members after $rounds commands" [dict get $after physical] [expr {$n - $popped}] \
                "the hidden member survives every command; only popped members leave" $cmd $key "popped=$popped"
        } {} {slow}
    }

    # Listpack: selection is O(n) per traversal by construction; one hidden
    # member may add a validation step per entry, not a traversal per result.
    foreach cmdspec {
        {"SPOP key 3"           {spop $key 3}}
        {"SRANDMEMBER key 3"    {srandmember $key 3}}
        {"SRANDMEMBER key -100" {srandmember $key -100}}
    } {
        lassign $cmdspec label cmdtpl
        test "setperf-random: $label listpack, one expired member keeps single-traversal selection" {
            foreach n {16 64 128} {
                foreach ttl {none one_expired} {
                    set key "lp:$ttl"
                    wc_fixture $key $n $ttl
                    assert_equal [dict get [wc_setinfo $key] encoding] listpack
                    set cmda($ttl) [subst -nocommands $cmdtpl]
                    set d($ttl) [wc_measure "r $cmda($ttl)"]
                    set reply($ttl) $::wc_last_reply
                    wc_record $::cur_test $cmda($ttl) [dict create n $n ttl $ttl enc listpack] $d($ttl)
                }
                assert_equal [lsearch -exact $reply(one_expired) m0] -1
                assert_equal [llength $reply(one_expired)] [llength $reply(none)]
                set popped [expr {[string match spop* $cmda(one_expired)] ? [llength $reply(one_expired)] : 0}]
                set info [wc_setinfo lp:one_expired]
                wc_assert_le "members reclaimed during selection" [wc_get $d(one_expired) set_members_reclaimed] 0 \
                    "a listpack selection path must not reclaim the hidden member either (n=$n)" \
                    $cmda(one_expired) lp:one_expired
                wc_assert_eq "physical members after the command" [dict get $info physical] [expr {$n - $popped}] \
                    "only the members returned leave the listpack; the hidden member survives (n=$n)" \
                    $cmda(one_expired) lp:one_expired "returned=[llength $reply(one_expired)]"
                wc_assert_ratio "listpack entries examined (find+next+random steps)" [wc_lp_examined $d(one_expired)] [wc_lp_examined $d(none)] 3 [expr {2 * $n}] \
                    "one expired member must not add per-result traversals (n=$n)" $cmda(one_expired) lp:one_expired
                wc_assert_ratio "string objects created" [wc_get $d(one_expired) str_objs_created] [wc_get $d(none) str_objs_created] 3 8 \
                    "one expired member must not multiply member copies (n=$n)" $cmda(one_expired) lp:one_expired
            }
        }
    }

    wc_restore $saved
    }
}

start_server {tags {"setperf set external:skip needs:debug"}} {
    if {![wc_available]} {
        test "setperf-random (review regressions): skipped, server lacks WORK_COUNTERS" {
            skip "build with: make WORK_COUNTERS=yes"
        }
    } else {
        set saved [wc_quiesce]
        unset -nocomplain d

        test "setperf-random: scalar listpack reads with one future TTL avoid a population pass" {
            foreach cmd {{srandmember lp:one} {srandmember lp:one 1} {srandmember lp:one -1}} {
                foreach n {16 64 128} {
                    wc_fixture lp:one $n one
                    set d [wc_measure "r $cmd"]
                    assert_equal 1 [llength $::wc_last_reply]
                    wc_assert_eq "full-iteration reservoir passes" [wc_get $d set_reservoir_passes] 0 \
                        "one future TTL must not send a scalar read through the population" $cmd lp:one
                    wc_assert_eq "members returned by full iteration" [wc_get $d set_iter_next] 0 \
                        "a scalar read must use the random-position path" $cmd lp:one
                    wc_record $::cur_test $cmd [dict create n $n ttl one enc listpack] $d
                }
            }
        }

        test "setperf-random: SRANDMEMBER -1 fallback does not allocate by physical cardinality" {
            set n 20000
            foreach ttl {mostly_expired all_expired} {
                set key sp:$ttl
                wc_fixture $key $n $ttl short 40
                set d [wc_measure "r srandmember $key -1"]
                set expected [expr {$ttl eq "all_expired" ? 0 : 1}]
                assert_equal $expected [llength $::wc_last_reply]
                wc_assert_le "largest single allocation" [wc_get $d mem_max_alloc] 4096 \
                    "one requested member must not allocate by the physical population" \
                    "srandmember key -1" $key
                wc_assert_le "full-iteration reservoir passes" [wc_get $d set_reservoir_passes] 1 \
                    "dense expiry may require only one fallback pass" "srandmember key -1" $key
                wc_assert_eq "physical members after the read" [dict get [wc_setinfo $key] physical] $n \
                    "SRANDMEMBER is read-only" "srandmember key -1" $key
                wc_record $::cur_test "srandmember key -1" \
                    [dict create n $n ttl $ttl enc hashtable] $d
            }
        }

        test "setperf-random: all-hidden counted SPOP does not rebuild the set" {
            set sizes [wc_ht_sizes]
            set objects {}
            set allocations {}
            foreach n $sizes {
                set key sp:all_expired
                wc_fixture $key $n all_expired
                set d [wc_measure "r spop $key $n"]
                assert_equal {} $::wc_last_reply
                lappend objects [wc_get $d str_objs_created]
                lappend allocations [wc_get $d mem_max_alloc]
                wc_assert_le "string objects created" [wc_get $d str_objs_created] 8 \
                    "an empty reply may create only fixed command bookkeeping" "spop key n" $key
                wc_assert_eq "set entries inserted" [wc_get $d ht_inserts] 0 \
                    "an all-hidden set must not be rebuilt" "spop key n" $key
                wc_assert_le "largest single allocation" [wc_get $d mem_max_alloc] 16384 \
                    "temporary storage must not scale with the hidden population" "spop key n" $key
                wc_assert_eq "physical members after the pop" [dict get [wc_setinfo $key] physical] $n \
                    "an empty pop leaves hidden members to active expiration" "spop key n" $key
                wc_record $::cur_test "spop key n" \
                    [dict create n $n ttl all_expired enc hashtable] $d
            }
            wc_assert_flat "string objects created" $sizes $objects 1 4 \
                "fixed command bookkeeping must not grow with the hidden population" \
                "spop key n" sp:all_expired
            wc_assert_flat "largest single allocation" $sizes $allocations 1 1024 \
                "an empty pop must not allocate by the hidden population" \
                "spop key n" sp:all_expired
        }

        test "setperf-random: persistent CASE 3 retains members without temporary SDS conversions" {
            set n 128
            set count 107
            wc_fixture sp:listpack $n none
            set d [wc_measure "r spop sp:listpack $count"]
            wc_assert_le "SDS copies" [wc_get $d sds_copies] [expr {$count + 8}] \
                "retained listpack members use the raw insertion path" "spop key count" sp:listpack
            wc_record $::cur_test "spop key count" \
                [dict create n $n count $count ttl none enc listpack] $d

            set n 512
            set count 427
            r del sp:intset
            for {set i 0} {$i < $n} {incr i 128} {
                set members {}
                for {set j $i} {$j < $i + 128} {incr j} { lappend members $j }
                r sadd sp:intset {*}$members
            }
            assert_equal intset [dict get [wc_setinfo sp:intset] encoding]
            set d [wc_measure "r spop sp:intset $count"]
            wc_assert_le "application allocations" [wc_get $d mem_allocs] 16 \
                "retained integers must not be formatted into temporary SDS values" \
                "spop key count" sp:intset
            wc_record $::cur_test "spop key count" \
                [dict create n $n count $count ttl none enc intset] $d
        }

        wc_restore $saved
    }
}

# ---------------------------------------------------------------------------
# Census / rank-select contracts.
#
# A hidden-dense set is answered from the volatile index, not from a pass over
# the members: one census reports the exact live/hidden partition from the
# bucket keys, and a uniform rank is carried to a member by the index's
# rank/select. So SRANDMEMBER and SPOP must visit no members at all on such a
# set, must allocate by the request rather than by the population, and must cost
# the same at 10,000 members as at 1,000,000.
#
# This holds only where the index can see the live members, i.e. where they
# carry a TTL (wc_fixture_hidden_ttl). The `mostly_expired` fixture, whose live
# members carry no TTL, is the documented needle case: the index cannot name
# them, so a walk is the only way to find one and no sublinear bound is claimed
# for it. Both are measured below, with different contracts.
start_server {tags {"setperf set external:skip needs:debug"}} {
    if {![wc_available]} {
        test "setperf-random (census): skipped, server lacks WORK_COUNTERS" {
            skip "build with: make WORK_COUNTERS=yes"
        }
    } else {
    set saved [wc_quiesce]

    # SETPERF_EXTENDED=1 adds the 1,000,000-member size; the flat contracts need
    # at least two sizes to say anything, and both are present either way.
    proc wc_census_sizes {} {
        if {$::wc_extended} { return {10000 1000000} }
        return {10000 100000}
    }

    foreach cmdspec {
        {"SRANDMEMBER key 1"  {srandmember $key 1}  read}
        {"SRANDMEMBER key 2"  {srandmember $key 2}  read}
        {"SRANDMEMBER key -2" {srandmember $key -2} read}
        {"SPOP key 1"         {spop $key 1}         pop}
        {"SPOP key 2"         {spop $key 2}         pop}
    } {
        lassign $cmdspec label cmdtpl kind

        test "setperf-random: repeated $label on a future-TTL hidden-dense hashtable is answered by the index" {
            set sizes [wc_census_sizes]
            set rounds 10
            set live_keep 40
            set visit_vals {}
            set alloc_vals {}
            foreach n $sizes {
                set key sp:httl
                set live [wc_fixture_hidden_ttl $key $n $live_keep]
                set info [wc_setinfo $key]
                assert_equal [dict get $info encoding] hashtable
                assert_equal [dict get $info physical] $n
                assert_equal [dict get $info live] $live_keep
                set cmd [subst -nocommands $cmdtpl]
                set popped 0
                set reclaimed 0
                unset -nocomplain dirty_total
                set dirty_total 0
                set visits 0
                set peak 0
                for {set i 0} {$i < $rounds} {incr i} {
                    set dirty0 [wc_dirty]
                    set d [wc_measure "r $cmd"]
                    incr dirty_total [expr {[wc_dirty] - $dirty0}]
                    wc_record $::cur_test $cmd \
                        [dict create n $n ttl hidden_ttl live $live_keep enc hashtable round $i] $d

                    # Correctness: only live members, and the requested shape.
                    foreach mem $::wc_last_reply {
                        if {[lsearch -exact $live $mem] == -1} {
                            fail "CONTRACT VIOLATED: a hidden member must never be returned\n  $cmd returned '$mem'\n  [wc_ctx $cmd $key "n=$n round=$i"]\n  [wc_repro $cmd]"
                        }
                    }
                    if {$kind eq "pop"} {
                        incr popped [llength $::wc_last_reply]
                        foreach mem $::wc_last_reply { set live [lsearch -all -inline -not -exact $live $mem] }
                    }
                    incr reclaimed [wc_get $d set_members_reclaimed]

                    # The index suffices, so no member is read: no reservoir
                    # pass, no needle walk, no member-iterator visit.
                    wc_assert_eq "full-iteration reservoir passes" [wc_get $d set_reservoir_passes] 0 \
                        "an indexed live population must not be selected by a reservoir pass (n=$n)" $cmd $key "round=$i"
                    wc_assert_eq "persistent-member walks" [wc_get $d set_persistent_scans] 0 \
                        "every live member carries a TTL, so no walk for an untimed member is needed (n=$n)" $cmd $key "round=$i"
                    wc_assert_eq "member-iterator visits" [wc_get $d set_iter_next] 0 \
                        "a census plus rank/select reads no member (n=$n)" $cmd $key "round=$i"
                    wc_assert_le "census member examinations" [wc_get $d set_census_entries] 0 \
                        "a hashtable census reads the index, never the members (n=$n)" $cmd $key "round=$i"
                    wc_assert_ge "census taken" [wc_get $d set_census_calls] 1 \
                        "the exact partition must come from a census, not from sampling (n=$n)" $cmd $key "round=$i"

                    # Auxiliary storage is the request, not the population.
                    wc_assert_le "largest single allocation (bytes)" [wc_get $d mem_max_alloc] 16384 \
                        "temporary storage must not scale with the population (n=$n)" $cmd $key "round=$i"
                    wc_assert_le "peak live allocation (bytes)" [wc_get $d mem_peak_live_delta] 65536 \
                        "the peak auxiliary allocation must be request-sized (n=$n)" $cmd $key "round=$i"

                    incr visits [wc_ht_examined $d]
                    if {[wc_get $d mem_peak_live_delta] > $peak} { set peak [wc_get $d mem_peak_live_delta] }
                }

                # Reclamation belongs to active expiration. A read changes
                # nothing at all; a pop removes exactly what it returned.
                wc_assert_eq "members reclaimed over $rounds commands" $reclaimed 0 \
                    "no selection path may reclaim a hidden member (n=$n)" $cmd $key
                set after [wc_setinfo $key]
                wc_assert_eq "physical members after $rounds commands" [dict get $after physical] [expr {$n - $popped}] \
                    "the hidden population survives every command (n=$n)" $cmd $key "popped=$popped"
                wc_assert_eq "key still exists" [r exists $key] 1 \
                    "a set still holding hidden members keeps its key (n=$n)" $cmd $key
                if {$kind eq "read"} {
                    wc_assert_eq "keyspace changes booked by a read" $dirty_total 0 \
                        "SRANDMEMBER must not write (n=$n)" $cmd $key
                    wc_assert_eq "commands propagated by a read" [wc_propagated $d] 0 \
                        "SRANDMEMBER must not propagate (n=$n)" $cmd $key
                }
                lappend visit_vals $visits
                lappend alloc_vals $peak
            }
            # The request is the same at every size, so the work must be too.
            wc_assert_flat "hashtable entries examined over $rounds commands" $sizes $visit_vals 4 2000 \
                "index-answered selection must not grow with the population" $label sp:httl
            wc_assert_flat "peak auxiliary allocation (bytes)" $sizes $alloc_vals 2 4096 \
                "auxiliary allocation must not grow with the population" $label sp:httl
        } {} {slow}

        test "setperf-random: repeated $label on an all-hidden hashtable is answered by the index" {
            set sizes [wc_census_sizes]
            set rounds 10
            set visit_vals {}
            foreach n $sizes {
                set key sp:allh
                wc_fixture $key $n all_expired
                assert_equal [dict get [wc_setinfo $key] physical] $n
                set cmd [subst -nocommands $cmdtpl]
                set reclaimed 0
                set visits 0
                for {set i 0} {$i < $rounds} {incr i} {
                    set d [wc_measure "r $cmd"]
                    wc_record $::cur_test $cmd \
                        [dict create n $n ttl all_expired enc hashtable round $i] $d
                    # Nothing is live, so the census answers the whole command.
                    assert_equal $::wc_last_reply {}
                    incr reclaimed [wc_get $d set_members_reclaimed]
                    incr visits [wc_ht_examined $d]
                    wc_assert_eq "full-iteration reservoir passes" [wc_get $d set_reservoir_passes] 0 \
                        "an all-hidden set is recognised from the index, not by a pass (n=$n)" $cmd $key "round=$i"
                    wc_assert_eq "member-iterator visits" [wc_get $d set_iter_next] 0 \
                        "an empty reply must read no member (n=$n)" $cmd $key "round=$i"
                    wc_assert_eq "persistent-member walks" [wc_get $d set_persistent_scans] 0 \
                        "an all-hidden set has no untimed member to walk for (n=$n)" $cmd $key "round=$i"
                    wc_assert_eq "set entries inserted" [wc_get $d ht_inserts] 0 \
                        "an all-hidden set must not be rebuilt (n=$n)" $cmd $key "round=$i"
                    wc_assert_le "largest single allocation (bytes)" [wc_get $d mem_max_alloc] 16384 \
                        "an empty reply must not allocate by the hidden population (n=$n)" $cmd $key "round=$i"
                    wc_assert_le "peak live allocation (bytes)" [wc_get $d mem_peak_live_delta] 65536 \
                        "an empty reply must not allocate by the hidden population (n=$n)" $cmd $key "round=$i"
                    # Checked every round: a command that emptied the set would
                    # have removed the key and SETINFO would error instead.
                    wc_assert_eq "key still exists" [r exists $key] 1 \
                        "an empty reply is not a licence to delete the key (n=$n)" $cmd $key "reclaimed so far: $reclaimed"
                    wc_assert_eq "physical members" [dict get [wc_setinfo $key] physical] $n \
                        "every hidden member is still allocated (n=$n)" $cmd $key "round=$i"
                }
                wc_assert_eq "members reclaimed over $rounds commands" $reclaimed 0 \
                    "active expiration reclaims, selection does not (n=$n)" $cmd $key
                lappend visit_vals $visits
            }
            wc_assert_flat "hashtable entries examined over $rounds commands" $sizes $visit_vals 4 2000 \
                "recognising an all-hidden set must not grow with the population" $label sp:allh
        } {} {slow}
    }

    # The needle: the live members carry no TTL, so the index cannot name them
    # and a walk is the only way to reach one. Linear work is correct here, and
    # deliberately not gated. What IS gated is everything else: correctness, the
    # absence of reclamation, and allocation that stays request-sized even though
    # the search is a pass.
    foreach cmdspec {
        {"SRANDMEMBER key 1"  {srandmember $key 1}  read}
        {"SRANDMEMBER key -2" {srandmember $key -2} read}
        {"SPOP key 1"         {spop $key 1}         pop}
    } {
        lassign $cmdspec label cmdtpl kind
        test "setperf-random: $label on a persistent-live needle set stays correct and allocation-bounded" {
            set n 10000
            set rounds 5
            set key sp:needle
            set live [wc_fixture $key $n mostly_expired short 1]
            assert_equal [llength $live] 1
            set info [wc_setinfo $key]
            assert_equal [dict get $info encoding] hashtable
            assert_equal [dict get $info live] 1
            set cmd [subst -nocommands $cmdtpl]
            set popped 0
            for {set i 0} {$i < $rounds} {incr i} {
                set d [wc_measure "r $cmd"]
                wc_record $::cur_test $cmd \
                    [dict create n $n ttl mostly_expired live 1 enc hashtable round $i] $d
                if {$popped == 0} {
                    # The one live member is the only possible answer.
                    foreach mem $::wc_last_reply {
                        wc_assert_eq "member returned" $mem [lindex $live 0] \
                            "only the live member may be returned" $cmd $key "round=$i"
                    }
                    if {$kind eq "read"} {
                        wc_assert_ge "members returned" [llength $::wc_last_reply] 1 \
                            "a live member exists, so the reply may not be empty" $cmd $key "round=$i"
                    }
                }
                if {$kind eq "pop"} { incr popped [llength $::wc_last_reply] }
                wc_assert_eq "members reclaimed" [wc_get $d set_members_reclaimed] 0 \
                    "the needle case must not reclaim the hidden members it walks past" $cmd $key "round=$i"
                wc_assert_le "largest single allocation (bytes)" [wc_get $d mem_max_alloc] 16384 \
                    "a walk must not also buffer the population" $cmd $key "round=$i"
                wc_assert_le "peak live allocation (bytes)" [wc_get $d mem_peak_live_delta] 65536 \
                    "a walk must not also buffer the population" $cmd $key "round=$i"
                wc_assert_eq "full-iteration reservoir passes" [wc_get $d set_reservoir_passes] 0 \
                    "the needle is found by a rank walk, not by reservoir sampling" $cmd $key "round=$i"
            }
            set after [wc_setinfo $key]
            wc_assert_eq "physical members after $rounds commands" [dict get $after physical] [expr {$n - $popped}] \
                "only the members returned leave the set" $cmd $key "popped=$popped"
            wc_assert_eq "key still exists" [r exists $key] 1 \
                "the hidden population keeps the key alive" $cmd $key
        } {} {slow}
    }

    # Negative counts are draws WITH replacement, so they must be independent and
    # uniform over the live members -- not a shuffled snapshot, and not biased
    # toward whichever member a rank walk reaches first. Fixed seed, and bounds
    # wide enough that the check does not depend on the RNG stream: with 4,000
    # draws over 40 members the expected count is 100 and the chance of any
    # member falling outside [40,190] is far below one in a million.
    test "setperf-random: SRANDMEMBER -k on a hidden-dense hashtable draws independently and uniformly" {
        set n 10000
        set live_keep 40
        set draws 4000
        foreach {tag fixture} {future_ttl hidden_ttl persistent mostly_expired} {
            set key sp:dist
            if {$fixture eq "hidden_ttl"} {
                set live [wc_fixture_hidden_ttl $key $n $live_keep]
            } else {
                set live [wc_fixture $key $n mostly_expired short $live_keep]
            }
            assert_equal [llength $live] $live_keep
            assert_equal [dict get [wc_setinfo $key] encoding] hashtable
            set d [wc_measure "r srandmember $key -$draws"]
            wc_record $::cur_test "srandmember key -$draws" \
                [dict create n $n ttl $tag live $live_keep enc hashtable] $d
            unset -nocomplain drawn
            set drawn $::wc_last_reply

            # With replacement: exactly `draws` members, all live.
            wc_assert_eq "members returned ($tag)" [llength $drawn] $draws \
                "a negative count replies with exactly that many members" "srandmember key -$draws" $key
            unset -nocomplain seen
            set seen [dict create]
            foreach mem $drawn {
                if {[lsearch -exact $live $mem] == -1} {
                    fail "CONTRACT VIOLATED: a hidden member must never be returned\n  srandmember $key -$draws returned '$mem'\n  [wc_ctx "srandmember key -$draws" $key $tag]"
                }
                dict incr seen $mem
            }
            # Independence: every live member is reachable, and none dominates.
            wc_assert_eq "distinct members drawn ($tag)" [dict size $seen] $live_keep \
                "independent draws must be able to reach every live member" "srandmember key -$draws" $key
            set expected [expr {$draws / $live_keep}]
            dict for {mem cnt} $seen {
                wc_assert_ge "draws of $mem ($tag)" $cnt [expr {$expected * 4 / 10}] \
                    "no live member may be starved by the draw" "srandmember key -$draws" $key
                wc_assert_le "draws of $mem ($tag)" $cnt [expr {$expected * 19 / 10}] \
                    "no live member may dominate the draw" "srandmember key -$draws" $key
            }
            # Not a snapshot replayed in order: a permutation of the live set
            # repeated `draws/live_keep` times would make every window distinct.
            set repeats 0
            for {set i 1} {$i < $draws} {incr i} {
                if {[lindex $drawn $i] eq [lindex $drawn [expr {$i - 1}]]} { incr repeats }
            }
            wc_assert_ge "adjacent repeats ($tag)" $repeats 1 \
                "draws with replacement over $live_keep members must sometimes repeat" \
                "srandmember key -$draws" $key
            # One reply is one population read at most, whatever the encoding.
            wc_assert_le "full-iteration reservoir passes ($tag)" [wc_get $d set_reservoir_passes] 0 \
                "a negative count must not reservoir-sample" "srandmember key -$draws" $key
            wc_assert_eq "members reclaimed ($tag)" [wc_get $d set_members_reclaimed] 0 \
                "SRANDMEMBER must not reclaim" "srandmember key -$draws" $key
            wc_assert_eq "physical members after the read ($tag)" [dict get [wc_setinfo $key] physical] $n \
                "SRANDMEMBER is read-only" "srandmember key -$draws" $key
        }
    } {} {slow}

    test "setperf-random: large indexed populations switch to one bulk pass" {
        set n 20000
        set live_keep 5000
        set count 500
        foreach cmdspec {
            {"SRANDMEMBER key 500"  {srandmember $key $count}}
            {"SRANDMEMBER key -500" {srandmember $key -$count}}
            {"SPOP key 500"         {spop $key $count}}
        } {
            lassign $cmdspec label cmdtpl
            set key sp:bulk
            set live [wc_fixture_hidden_ttl $key $n $live_keep]
            set cmd [subst -nocommands $cmdtpl]
            set d [wc_measure "r $cmd"]
            wc_record $::cur_test $cmd \
                [dict create n $n ttl hidden_ttl live $live_keep count $count enc hashtable] $d
            wc_assert_eq "members returned" [llength $::wc_last_reply] $count \
                "the bulk strategy must preserve the requested reply size" $cmd $key
            foreach mem $::wc_last_reply {
                if {[lsearch -exact $live $mem] == -1} {
                    fail "CONTRACT VIOLATED: $label returned hidden member '$mem'"
                }
            }
            wc_assert_eq "per-member rank selections" [wc_get $d set_rank_selects] 0 \
                "a large request must not restart an indexed bucket walk for every result" $cmd $key
            wc_assert_le "member iterator visits" [wc_get $d set_iter_next] [expr {$n + 16}] \
                "the fallback is at most one pass over the physical set" $cmd $key
            wc_assert_le "expiry-index entry visits" [wc_get $d vset_entry_visits] [expr {$live_keep + 16}] \
                "the census may inspect the boundary bucket once, not once per result" $cmd $key
            wc_assert_eq "members reclaimed" [wc_get $d set_members_reclaimed] 0 \
                "selection must not reclaim hidden members" $cmd $key
        }
    } {} {slow}

    test "setperf-random: a large negative count allocates one bounded batch" {
        set key sp:negative-batch
        set count 100000
        set live [wc_fixture $key 5 mostly_expired short 1]
        wc_force_hashtable $key
        assert_equal 1 [llength $live]
        assert_equal hashtable [dict get [wc_setinfo $key] encoding]

        set d [wc_measure "r srandmember $key -$count"]
        wc_record $::cur_test "srandmember key -$count" \
            [dict create n 5 ttl mostly_expired live 1 count $count enc hashtable] $d
        wc_assert_eq "members returned" [llength $::wc_last_reply] $count \
            "a bounded sampler must preserve negative-count reply semantics" \
            "srandmember key -$count" $key
        wc_assert_le "largest single allocation (bytes)" [wc_get $d mem_max_alloc] 262144 \
            "scratch allocation must be bounded independently of the requested count" \
            "srandmember key -$count" $key
        wc_assert_eq "members reclaimed" [wc_get $d set_members_reclaimed] 0 \
            "SRANDMEMBER must remain read-only" "srandmember key -$count" $key
    } {} {slow}

    test "setperf-random: SPOP crossing the live-density threshold uses one bounded pass" {
        set n 20000
        set live_keep 10000
        set count 10000
        set key sp:cross-threshold
        set live [wc_fixture_hidden_ttl $key $n $live_keep]
        set d [wc_measure "r spop $key $count"]
        wc_record $::cur_test "spop key $count" \
            [dict create n $n ttl hidden_ttl live $live_keep count $count enc hashtable] $d
        wc_assert_eq "members returned" [llength $::wc_last_reply] $count \
            "crossing the density threshold must preserve SPOP results" \
            "spop key $count" $key
        foreach mem $::wc_last_reply {
            if {[lsearch -exact $live $mem] == -1} {
                fail "CONTRACT VIOLATED: SPOP returned hidden member '$mem'"
            }
        }
        wc_assert_le "per-member rank selections" [wc_get $d set_rank_selects] 2 \
            "SPOP must reconsider the bulk strategy after the live ratio changes" \
            "spop key $count" $key
        wc_assert_le "expiry-index entry visits" [wc_get $d vset_entry_visits] [expr {3 * $n + 16}] \
            "threshold crossing must not restart a large bucket walk per result" \
            "spop key $count" $key
        wc_assert_le "member iterator visits" [wc_get $d set_iter_next] [expr {$n + 16}] \
            "the bulk fallback may make at most one member pass" \
            "spop key $count" $key
        wc_assert_eq "members reclaimed" [wc_get $d set_members_reclaimed] 0 \
            "SPOP removes only returned live members" "spop key $count" $key
    } {} {slow}

    wc_restore $saved
    }
}
