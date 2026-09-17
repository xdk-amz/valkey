# Performance contracts for the SET insertion paths (SADD / SADDEX / SMOVE) and
# for the request-sized membership / TTL reads, on sets with member TTLs.
#
# Requires a server built with `make WORK_COUNTERS=yes`; counters are read
# through DEBUG WORKCTR (helpers in tests/support/workctr.tcl). Budgets come
# from the parent algorithm and from source inspection, never from the numbers
# this commit produces.
#
# Parent insertion (t_set.c setTypeAddAux) is ONE hashtableFindPositionForInsert
# per member: one findBucket (ht_lookups) plus one insert-position request, then
# the insert. The listpack parent is one lpFind per member.
#
# Contract I-1: adding a member to a set that has volatile members must cost at
#   most ONE probe to locate or replace the physical slot plus ONE
#   insert-position request. A member that never existed must never be looked
#   up a third time. The hashtable budget allows two probes per member (a
#   validating existence check and a non-validating slot probe) even though the
#   parent needs one; the listpack budget is the parent's single lpFind, because
#   setTypeSetExpiryInternal already reads the expiry from the position lpFind
#   returned.
# Contract I-2: legitimate expiry work is one index operation per member whose
#   TTL changed, plus one physical removal for an expired member being replaced.
#   It is not a second and third full lookup of the same member.
# Contract I-3: membership and TTL reads are request-sized: work proportional to
#   the members named, never to the population, on every TTL distribution.
#
# Note on ht_lookups: the expiry index (vset) is bracketed by WC_INDEX_BEGIN/END
# in the set code, so its internal hashtable probes land in the idx_ account and
# never in ht_lookups. The budgets below therefore count probes of the set's own
# hashtable only.
#
# ht_hash_calls and ht_insert_positions are NOT used as budgets here: incremental
# rehashing rehashes and re-inserts unrelated entries on the same write, and
# those counters cannot tell that work apart from the command's own.

# Lookups the expiry index may legitimately spend for this command.
proc ins_vset_ops {d} {
    wc_sum $d vset_adds vset_removes vset_updates
}

# ht_lookups budget: `per_member` probes of the set's own hashtable for each of
# `k` members, plus `slack` for the keyspace lookup and key-tracking bookkeeping.
proc ins_budget {per_member k {slack 4}} {
    expr {$per_member * $k + $slack}
}

proc ins_no_traversal {d cmd key} {
    wc_assert_le "hashtable entries visited by iteration" [wc_get $d ht_iter_visits] 0 \
        "a keyed insertion or read must not iterate the set" $cmd $key
    wc_assert_le "hashtable entries visited by scan" [wc_get $d ht_scan_visits] 0 \
        "a keyed insertion or read must not scan the set" $cmd $key
}

start_server {tags {"setperf set external:skip needs:debug"}} {
    if {![wc_available]} {
        test "setperf-insert: skipped, server lacks WORK_COUNTERS" {
            skip "build with: make WORK_COUNTERS=yes"
        }
    } else {
    set saved [wc_quiesce]
    set ins_n 20000
    set ins_k 100
    # Fresh member names: absent from every fixture of size $ins_n.
    set ins_absent [wc_members $ins_k short $ins_n]

    # ---- absent members: one probe, not three ----------------------------
    test "setperf-insert: SADD of absent members costs one probe per member on a volatile set" {
        foreach ttl {none one all} {
            set key "ins:add:$ttl"
            wc_fixture $key $ins_n $ttl
            assert_equal [dict get [wc_setinfo $key] encoding] hashtable
            set insda($ttl) [wc_measure "r sadd $key $ins_absent"]
            assert_equal $::wc_last_reply $ins_k
            assert_equal [r scard $key] [expr {$ins_n + $ins_k}]
            assert_equal [r sismember $key [lindex $ins_absent 0]] 1
            assert_equal [lindex [r sttl $key members 1 [lindex $ins_absent 0]] 0] -1
            wc_record $::cur_test "sadd key <$ins_k absent>" \
                [dict create n $ins_n ttl $ttl enc hashtable k $ins_k] $insda($ttl)
        }
        # SADD adds no TTL, so nothing here touches the expiry index at all.
        foreach ttl {one all} {
            set ins_disp "sadd ins:add:$ttl <$ins_k absent members>"
            assert_equal [ins_vset_ops $insda($ttl)] 0
            wc_assert_le "hashtable lookups" [wc_get $insda($ttl) ht_lookups] \
                [ins_budget 2 $ins_k] \
                "adding an absent member probes the slot once, not once per stage (I-1)" \
                $ins_disp "ins:add:$ttl"
            wc_assert_ratio "hashtable lookups" [wc_get $insda($ttl) ht_lookups] \
                [wc_get $insda(none) ht_lookups] 2 8 \
                "a member TTL must not multiply the lookups of the parent insertion (I-1)" \
                $ins_disp "ins:add:$ttl"
            ins_no_traversal $insda($ttl) $ins_disp "ins:add:$ttl"
        }
        # Sanity: the parent path spends one lookup per member.
        wc_assert_le "hashtable lookups (plain set, sanity)" [wc_get $insda(none) ht_lookups] \
            [expr {$ins_k + 4}] "the parent insertion is one lookup per member" \
            "sadd ins:add:none <$ins_k absent members>" ins:add:none
    }

    # SADDEX reaches the same insertion through setTypeSetExpiryInternal (one
    # hashtableFindRef) instead of setTypeIsMember; the slot probe and the
    # insert-position request are the same two operations. EX 1000 keeps the new
    # members out of the fixture's far-future expiry bucket, so the expiry index
    # stays a sparse vector and adds no lookup of its own.
    foreach ins_spec {
        {"SADDEX key EX 1000 MEMBERS k"      {saddex $key EX 1000 MEMBERS $ins_k $ins_absent}     2 ttl}
        {"SADDEX key PX 600000 MEMBERS k"    {saddex $key PX 600000 MEMBERS $ins_k $ins_absent}   2 ttl}
        {"SADDEX key KEEPTTL MEMBERS k"      {saddex $key KEEPTTL MEMBERS $ins_k $ins_absent}     2 none}
        {"SADDEX key XX EX 1000 MEMBERS k"   {saddex $key XX EX 1000 MEMBERS $ins_k $ins_absent}  2 ttl}
        {"SADDEX key MNX EX 1000 MEMBERS k"  {saddex $key MNX EX 1000 MEMBERS $ins_k $ins_absent} 3 ttl}
    } {
        lassign $ins_spec ins_label ins_tpl ins_per_member ins_expect
        test "setperf-insert: $ins_label of absent members costs one probe per member on a volatile set" {
            foreach ttl {none one all} {
                set key "ins:addex:$ttl"
                wc_fixture $key $ins_n $ttl
                set inscmda($ttl) [subst -nocommands $ins_tpl]
                set insda($ttl) [wc_measure "r $inscmda($ttl)"]
                assert_equal $::wc_last_reply $ins_k
                assert_equal [r scard $key] [expr {$ins_n + $ins_k}]
                set ins_first [lindex $ins_absent 0]
                assert_equal [r sismember $key $ins_first] 1
                set ins_sttl [lindex [r sttl $key members 1 $ins_first] 0]
                if {$ins_expect eq "ttl"} {
                    assert_morethan $ins_sttl 0
                } else {
                    assert_equal $ins_sttl -1
                }
                wc_record $::cur_test $ins_label \
                    [dict create n $ins_n ttl $ttl enc hashtable k $ins_k] $insda($ttl)
            }
            foreach ttl {one all} {
                set ins_disp "$ins_label (ins:addex:$ttl)"
                # MNX/MXX add one required all-or-nothing membership check per
                # member before any mutation; that check is in per_member.
                wc_assert_le "hashtable lookups" [wc_get $insda($ttl) ht_lookups] \
                    [ins_budget $ins_per_member $ins_k] \
                    "adding an absent member probes the slot once, not once per stage (I-1)" \
                    $ins_disp "ins:addex:$ttl"
                wc_assert_ratio "hashtable lookups" [wc_get $insda($ttl) ht_lookups] \
                    [wc_get $insda(none) ht_lookups] 2 8 \
                    "a member TTL must not multiply the lookups of the insertion (I-1)" \
                    $ins_disp "ins:addex:$ttl"
                ins_no_traversal $insda($ttl) $ins_disp "ins:addex:$ttl"
            }
        }
    }

    test "setperf-insert: SADDEX MXX on existing members is one lookup and one index operation each" {
        foreach ttl {none one all} {
            set key "ins:mxx:$ttl"
            set ins_live [wc_fixture $key $ins_n $ttl]
            set ins_targets [lrange $ins_live 300 [expr {300 + $ins_k - 1}]]
            set insda($ttl) [wc_measure "r saddex $key MXX EX 1000 MEMBERS $ins_k $ins_targets"]
            assert_equal $::wc_last_reply 0
            assert_equal [r scard $key] $ins_n
            foreach ins_t [lrange $ins_targets 0 4] {
                assert_morethan [lindex [r sttl $key members 1 $ins_t] 0] 0
            }
            set ins_disp "saddex ins:mxx:$ttl MXX EX 1000 MEMBERS $ins_k <existing>"
            wc_record $::cur_test $ins_disp \
                [dict create n $ins_n ttl $ttl enc hashtable k $ins_k] $insda($ttl)
            # The required MXX check plus the TTL update: two probes.
            wc_assert_le "hashtable lookups" [wc_get $insda($ttl) ht_lookups] \
                [ins_budget 2 $ins_k] \
                "an existing member is checked once and updated once (I-2)" $ins_disp "ins:mxx:$ttl"
            # Nothing is added to the set; any insert here belongs to the
            # expiry index, whose dense buckets are hashtables of their own.
            wc_assert_le "entries inserted" [wc_get $insda($ttl) ht_inserts] \
                [ins_vset_ops $insda($ttl)] \
                "MXX on existing members inserts nothing into the set" $ins_disp "ins:mxx:$ttl"
            ins_no_traversal $insda($ttl) $ins_disp "ins:mxx:$ttl"
        }
    }

    test "setperf-insert: SEXPIRE of existing members is one lookup and one index operation each" {
        foreach ttl {none one all} {
            set key "ins:sexp:$ttl"
            set ins_live [wc_fixture $key $ins_n $ttl]
            set ins_targets [lrange $ins_live 1000 [expr {1000 + $ins_k - 1}]]
            set insda($ttl) [wc_measure "r sexpire $key 1000 members $ins_k $ins_targets"]
            assert_equal [llength $::wc_last_reply] $ins_k
            foreach ins_r $::wc_last_reply { assert_equal $ins_r 1 }
            foreach ins_t [lrange $ins_targets 0 4] {
                assert_morethan [lindex [r sttl $key members 1 $ins_t] 0] 0
            }
            set ins_disp "sexpire ins:sexp:$ttl 1000 members $ins_k <existing>"
            wc_record $::cur_test $ins_disp \
                [dict create n $ins_n ttl $ttl enc hashtable k $ins_k] $insda($ttl)
            # One lookup to reach the member, plus at most one inside the index.
            wc_assert_le "hashtable lookups" [wc_get $insda($ttl) ht_lookups] \
                [ins_budget 2 $ins_k] \
                "setting a TTL on an existing member is one lookup plus one index operation (I-2)" \
                $ins_disp "ins:sexp:$ttl"
            assert_equal [expr {[wc_get $insda($ttl) vset_adds] + [wc_get $insda($ttl) vset_updates]}] $ins_k
            ins_no_traversal $insda($ttl) $ins_disp "ins:sexp:$ttl"
        }
    }

    test "setperf-insert: SADD of members that are already live is one lookup each" {
        foreach ttl {none one all} {
            set key "ins:noop:$ttl"
            set ins_live [wc_fixture $key $ins_n $ttl]
            set ins_targets [lrange $ins_live 500 [expr {500 + $ins_k - 1}]]
            set insda($ttl) [wc_measure "r sadd $key $ins_targets"]
            assert_equal $::wc_last_reply 0
            assert_equal [r scard $key] $ins_n
            set ins_disp "sadd ins:noop:$ttl <$ins_k existing live members>"
            wc_record $::cur_test $ins_disp \
                [dict create n $ins_n ttl $ttl enc hashtable k $ins_k] $insda($ttl)
            # A present, live member is found by the first probe; nothing is
            # inserted, removed or indexed.
            wc_assert_le "hashtable lookups" [wc_get $insda($ttl) ht_lookups] \
                [expr {$ins_k + 4}] \
                "a no-op SADD is one lookup per member (I-1)" $ins_disp "ins:noop:$ttl"
            wc_assert_le "entries inserted" [wc_get $insda($ttl) ht_inserts] 0 \
                "a no-op SADD inserts nothing" $ins_disp "ins:noop:$ttl"
            wc_assert_le "expiry index operations" [ins_vset_ops $insda($ttl)] 0 \
                "a no-op SADD touches no expiry index" $ins_disp "ins:noop:$ttl"
        }
    }

    # ---- replacing an expired member -------------------------------------
    #
    # Legitimate work: locate the physical entry, drop it from the expiry index,
    # and put the new member in its place. The member string is unchanged, so
    # the entry can be replaced where it was found. The expired population all
    # shares one expiry, so its index bucket is hashtable-encoded and each
    # removal is one findBucket of its own: three lookups per member, not four.
    foreach ins_spec {
        {"SADD"           {sadd $key $ins_targets}                          0 none}
        {"SADDEX EX"      {saddex $key EX 1000 MEMBERS $ins_k $ins_targets} 1 ttl}
        {"SADDEX KEEPTTL" {saddex $key KEEPTTL MEMBERS $ins_k $ins_targets} 0 none}
    } {
        lassign $ins_spec ins_label ins_tpl ins_new_index ins_expect
        test "setperf-insert: $ins_label over expired members replaces them without re-lookups" {
            set key ins:mexp
            set ins_live_keep 200
            wc_fixture $key $ins_n mostly_expired short $ins_live_keep
            set ins_info [wc_setinfo $key]
            assert_equal [dict get $ins_info live] $ins_live_keep
            assert_equal [dict get $ins_info physical] $ins_n
            # Expired names, taken after the live prefix the fixture kept.
            set ins_targets [wc_members $ins_k short $ins_live_keep]
            set inscmd1 [subst -nocommands $ins_tpl]
            set insd1 [wc_measure "r $inscmd1"]
            assert_equal $::wc_last_reply $ins_k
            wc_record $::cur_test $ins_label \
                [dict create n $ins_n ttl mostly_expired enc hashtable k $ins_k] $insd1
            # Correctness: the members are live again with the requested TTL.
            foreach ins_t [lrange $ins_targets 0 4] {
                assert_equal [r sismember $key $ins_t] 1
                set ins_sttl [lindex [r sttl $key members 1 $ins_t] 0]
                if {$ins_expect eq "ttl"} {
                    assert_morethan $ins_sttl 0
                } else {
                    assert_equal $ins_sttl -1
                }
            }
            assert_equal [r sismember $key [lindex $ins_targets end]] 1
            # Each replaced member is reclaimed once and de-indexed once.
            assert_equal [wc_get $insd1 set_members_reclaimed] $ins_k
            assert_lessthan_equal [wc_get $insd1 vset_removes] $ins_k
            assert_lessthan_equal [wc_get $insd1 vset_adds] [expr {$ins_new_index * $ins_k}]
            wc_assert_le "hashtable lookups" [wc_get $insd1 ht_lookups] \
                [ins_budget 2 $ins_k] \
                "replacing an expired member is one slot probe plus its index removal, not a third lookup (I-2)" \
                $ins_label $key
            wc_assert_le "entries popped" [wc_get $insd1 ht_pops] [expr {$ins_k}] \
                "each replaced member is removed from the set once (its index removal is idx_ work) (I-2)" \
                $ins_label $key
            ins_no_traversal $insd1 $ins_label $key
        }
    }

    # ---- SMOVE -----------------------------------------------------------
    test "setperf-insert: SMOVE into a volatile set is bounded per move" {
        set ins_moves 100
        set ins_dst ins:mv:dst
        wc_fixture $ins_dst $ins_n one
        assert_equal [dict get [wc_setinfo $ins_dst] encoding] hashtable
        # A listpack source spends no hashtable lookups, so the budget is about
        # the destination: two keyspace lookups plus one destination slot probe,
        # with one more allowed for the probe/insert split and one per index op.
        r del ins:mv:src
        set ins_src_members [wc_members 128 short 900000]
        r sadd ins:mv:src {*}$ins_src_members
        assert_equal [dict get [wc_setinfo ins:mv:src] encoding] listpack
        set ins_total 0
        set ins_idx 0
        for {set i 0} {$i < $ins_moves} {incr i} {
            set ins_m [lindex $ins_src_members $i]
            set insd1 [wc_measure "r smove ins:mv:src $ins_dst $ins_m"]
            assert_equal $::wc_last_reply 1
            incr ins_total [wc_get $insd1 ht_lookups]
            incr ins_idx [ins_vset_ops $insd1]
            if {$i == 0} {
                wc_record $::cur_test "smove src dst member" \
                    [dict create n $ins_n ttl one enc hashtable] $insd1
                ins_no_traversal $insd1 "smove ins:mv:src $ins_dst $ins_m" $ins_dst
            }
        }
        assert_equal [r scard $ins_dst] [expr {$ins_n + $ins_moves}]
        assert_equal [r scard ins:mv:src] [expr {128 - $ins_moves}]
        assert_equal [r sismember $ins_dst [lindex $ins_src_members 0]] 1
        assert_equal [r sismember ins:mv:src [lindex $ins_src_members 0]] 0
        wc_assert_le "hashtable lookups over $ins_moves moves" $ins_total \
            [expr {3 * $ins_moves + $ins_idx + 4}] \
            "one move is two keyspace lookups plus one destination slot probe (I-1)" \
            "smove ins:mv:src ins:mv:dst member (x$ins_moves)" $ins_dst
    }

    test "setperf-insert: SMOVE carries the member TTL and stays bounded per move" {
        set ins_moves 50
        set ins_dst ins:mvt:dst
        wc_fixture $ins_dst $ins_n one
        r del ins:mvt:src
        set ins_src_members [wc_members 128 short 800000]
        r sadd ins:mvt:src {*}$ins_src_members
        set ins_far [expr {[clock milliseconds] + 3600 * 1000 * 24}]
        r spexpireat ins:mvt:src $ins_far members $ins_moves \
            {*}[lrange $ins_src_members 0 [expr {$ins_moves - 1}]]
        set ins_total 0
        set ins_idx 0
        for {set i 0} {$i < $ins_moves} {incr i} {
            set ins_m [lindex $ins_src_members $i]
            set insd1 [wc_measure "r smove ins:mvt:src $ins_dst $ins_m"]
            assert_equal $::wc_last_reply 1
            incr ins_total [wc_get $insd1 ht_lookups]
            incr ins_idx [ins_vset_ops $insd1]
            # The TTL moves with the member.
            assert_morethan [lindex [r spttl $ins_dst members 1 $ins_m] 0] 0
            if {$i == 0} {
                wc_record $::cur_test "smove src dst member (with TTL)" \
                    [dict create n $ins_n ttl one enc hashtable] $insd1
            }
        }
        assert_equal [r scard $ins_dst] [expr {$ins_n + $ins_moves}]
        wc_assert_le "hashtable lookups over $ins_moves moves" $ins_total \
            [expr {3 * $ins_moves + $ins_idx + 4}] \
            "one move is two keyspace lookups plus one destination slot probe (I-1)" \
            "smove ins:mvt:src ins:mvt:dst member (x$ins_moves)" $ins_dst
    }

    # ---- listpack --------------------------------------------------------
    test "setperf-insert: listpack insertion does not add a probe per member" {
        set ins_lp_k 4
        foreach n {16 64 128} {
            set ins_lp_add [wc_members $ins_lp_k short 700000]
            foreach ttl {none one} {
                set key "ins:lp:$ttl"
                wc_fixture $key $n $ttl
                assert_equal [dict get [wc_setinfo $key] encoding] listpack
                set insda($ttl) [wc_measure "r sadd $key $ins_lp_add"]
                assert_equal $::wc_last_reply $ins_lp_k
                assert_equal [r scard $key] [expr {$n + $ins_lp_k}]
                foreach ins_m $ins_lp_add { assert_equal [r sismember $key $ins_m] 1 }
                wc_record $::cur_test "sadd key <$ins_lp_k absent> (listpack)" \
                    [dict create n $n ttl $ttl enc listpack k $ins_lp_k] $insda($ttl)
            }
            set ins_disp "sadd ins:lp:one <$ins_lp_k absent members> (n=$n)"
            # One lpFind locates the member or its absence and exposes its expiry
            # at the same position, so a TTL adds no second probe.
            wc_assert_le "lpFind calls" [wc_get $insda(one) lp_find_calls] \
                [expr {$ins_lp_k + 2}] \
                "an absent member costs one listpack probe (I-1, n=$n)" $ins_disp ins:lp:one
            wc_assert_ratio "listpack entries examined" [wc_lp_examined $insda(one)] \
                [wc_lp_examined $insda(none)] 2 [expr {2 * $n}] \
                "a member TTL must not add a traversal per member (n=$n)" $ins_disp ins:lp:one
            wc_assert_le "listpack insertions" [wc_get $insda(one) lp_inserts] $ins_lp_k \
                "each added TTL-free member is one listpack insertion (n=$n)" $ins_disp ins:lp:one
            wc_assert_ratio "listpack reallocations" [wc_get $insda(one) lp_reallocs] \
                [wc_get $insda(none) lp_reallocs] 1 $ins_lp_k \
                "a member TTL adds at most one reallocation per member (n=$n)" $ins_disp ins:lp:one
        }
    }

    test "setperf-insert: listpack SADDEX adds the member, its expiry and the volatile count" {
        set ins_lp_k 4
        foreach n {16 64 128} {
            set ins_lp_add [wc_members $ins_lp_k short 710000]
            foreach ttl {none one} {
                set key "ins:lpx:$ttl"
                wc_fixture $key $n $ttl
                set insda($ttl) [wc_measure "r saddex $key EX 1000 MEMBERS $ins_lp_k $ins_lp_add"]
                assert_equal $::wc_last_reply $ins_lp_k
                foreach ins_m $ins_lp_add {
                    assert_equal [r sismember $key $ins_m] 1
                    assert_morethan [lindex [r sttl $key members 1 $ins_m] 0] 0
                }
                assert_equal [llength [r smembers $key]] [expr {$n + $ins_lp_k}]
                wc_record $::cur_test "saddex key EX 1000 MEMBERS k <absent> (listpack)" \
                    [dict create n $n ttl $ttl enc listpack k $ins_lp_k] $insda($ttl)
            }
            set ins_disp "saddex ins:lpx:one EX 1000 MEMBERS $ins_lp_k <absent> (n=$n)"
            # Member entry, its expiry metadata entry, and the set's volatile
            # count header, which is rewritten once per member added. Batching
            # the count per command would make this two; three is what the
            # per-member design costs and it does not grow with the population.
            wc_assert_le "listpack insertions" [wc_get $insda(one) lp_inserts] \
                [expr {3 * $ins_lp_k}] \
                "a member with an expiry is its entry, its metadata and the volatile count (I-2, n=$n)" \
                $ins_disp ins:lpx:one
            wc_assert_le "lpFind calls" [wc_get $insda(one) lp_find_calls] \
                [expr {$ins_lp_k + 2}] \
                "an absent member costs one listpack probe (I-1, n=$n)" $ins_disp ins:lpx:one
            wc_assert_ratio "listpack entries examined" [wc_lp_examined $insda(one)] \
                [wc_lp_examined $insda(none)] 2 [expr {2 * $n}] \
                "a member TTL must not add a traversal per member (n=$n)" $ins_disp ins:lpx:one
        }
    }

    # ---- request-sized reads ---------------------------------------------
    #
    # ht_lookups counts findBucket calls. SMISMEMBER on a hashtable goes through
    # hashtableFindBatch, whose incremental find never calls findBucket, so its
    # per-member probing is invisible to ht_lookups and ht_bucket_probes;
    # ht_key_compares and ht_validate_calls do observe it and are asserted too.
    test "setperf-insert: membership and TTL reads are request-sized on every TTL distribution" {
        set ins_sizes [wc_ht_sizes]
        set ins_rk 50
        set ins_rmem [wc_members $ins_rk short]
        set ins_specs {
            {"SISMEMBER"    {sismember $key m1}                      1  1}
            {"SMISMEMBER"   {smismember $key $ins_rmem}              50 50}
            {"STTL"         {sttl $key MEMBERS 50 $ins_rmem}         50 50}
            {"SPTTL"        {spttl $key MEMBERS 50 $ins_rmem}        50 50}
            {"SEXPIRETIME"  {sexpiretime $key MEMBERS 50 $ins_rmem}  50 50}
            {"SPEXPIRETIME" {spexpiretime $key MEMBERS 50 $ins_rmem} 50 50}
            {"SPERSIST"     {spersist $key MEMBERS 50 $ins_rmem}     50 50}
        }
        foreach ttl {none one all mostly_expired} {
            foreach ins_spec $ins_specs {
                set ins_flat([lindex $ins_spec 0],$ttl) {}
            }
        }
        foreach n $ins_sizes {
            foreach ttl {none one all mostly_expired} {
                set key "ins:rd:$ttl"
                wc_fixture $key $n $ttl short 100
                assert_equal [dict get [wc_setinfo $key] encoding] hashtable
                foreach ins_spec $ins_specs {
                    lassign $ins_spec ins_label ins_tpl ins_nmem ins_replylen
                    set inscmd1 [subst -nocommands $ins_tpl]
                    set insd1 [wc_measure "r $inscmd1"]
                    assert_equal [llength $::wc_last_reply] $ins_replylen
                    wc_record $::cur_test $ins_label \
                        [dict create n $n ttl $ttl enc hashtable members $ins_nmem] $insd1
                    set ins_disp "$ins_label ins:rd:$ttl (n=$n)"
                    # One lookup per named member; SPERSIST also removes each
                    # changed member from the expiry index, which may itself be
                    # one lookup when that index bucket is dense.
                    wc_assert_le "hashtable lookups" [wc_get $insd1 ht_lookups] \
                        [ins_budget 1 $ins_nmem [expr {[ins_vset_ops $insd1] + 2}]] \
                        "a read names its members: one lookup each (I-3)" $ins_disp $key
                    # A member compared or validated once each; the few extra
                    # come from de-indexing the members a read changed and from
                    # dropping the key's volatile-items tracking when the last
                    # TTL goes away.
                    wc_assert_le "key comparisons" [wc_get $insd1 ht_key_compares] \
                        [expr {$ins_nmem + [ins_vset_ops $insd1] + 4}] \
                        "a read compares each named member once (I-3)" $ins_disp $key
                    wc_assert_le "validateEntry calls" [wc_get $insd1 ht_validate_calls] \
                        [expr {$ins_nmem + 4}] \
                        "a read validates each named member at most once (I-3)" $ins_disp $key
                    ins_no_traversal $insd1 $ins_disp $key
                    lappend ins_flat($ins_label,$ttl) [wc_get $insd1 ht_key_compares]
                }
                # SCARD reads the size: no member work at all.
                set insd1 [wc_measure "r scard $key"]
                assert_equal $::wc_last_reply $n
                wc_assert_le "hashtable lookups" [wc_get $insd1 ht_lookups] 1 \
                    "SCARD is the key lookup and nothing else (I-3)" "scard ins:rd:$ttl (n=$n)" $key
                # An expired member is reported absent for the price of one lookup.
                if {$ttl eq "mostly_expired"} {
                    set insd1 [wc_measure "r sismember $key m500"]
                    assert_equal $::wc_last_reply 0
                    wc_assert_le "hashtable lookups" [wc_get $insd1 ht_lookups] 2 \
                        "an expired member is hidden by the same single lookup (I-3)" \
                        "sismember ins:rd:mostly_expired m500 (n=$n)" $key
                    ins_no_traversal $insd1 "sismember expired member (n=$n)" $key
                }
            }
        }
        foreach ttl {none one all mostly_expired} {
            foreach ins_spec $ins_specs {
                set ins_label [lindex $ins_spec 0]
                wc_assert_flat "key comparisons ($ins_label, $ttl)" $ins_sizes \
                    $ins_flat($ins_label,$ttl) 1 4 \
                    "read work must not grow with the population (I-3)" \
                    "$ins_label ins:rd:$ttl" "ins:rd:$ttl"
            }
        }
    } {} {slow}

    wc_restore $saved
    }
}
