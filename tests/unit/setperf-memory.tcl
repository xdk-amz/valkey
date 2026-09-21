# Auxiliary-memory and deletion-work contracts for SPOP / SRANDMEMBER.
#
# Requires a server built with `make WORK_COUNTERS=yes`; counters are read
# through DEBUG WORKCTR (helpers in tests/support/workctr.tcl). Budgets come
# from the parent algorithms (the no-TTL fixture runs the parent code paths
# unchanged) and from source inspection, never from the measured commit.
#
# Contract B1 (working memory): the strategy chosen for a request must size its
# auxiliary storage by what it has to remember, not by the request. The parent
# near-total SPOP (CASE 3) remembers only the `remaining` members; a member TTL
# must not turn that into a `count`-sized array of copies.
# Contract B2 (TTL independence of the whole-set paths): SPOP count >= size and
# SRANDMEMBER count >= size do not consult TTLs, so their memory and traversal
# must not depend on the TTL distribution.
# Contract B3 (listpack deletion): removing k selected members from a listpack
# is one batch delete of k positions, not k search-and-delete passes.
#
# Propagation is measured, not gated: every queued SREM argv is retained until
# the end of the execution unit in BOTH the parent (CASE 3 streams `count`
# members) and here, so prop_peak_retained_bytes is inherited O(count). It is
# recorded and printed so it is not mistaken for algorithmic working memory,
# which is what mem_max_alloc / mem_peak_live_delta / str_objs_created measure.

# Reply must be a duplicate-free subset of the fixture members.
proc spm_assert_members {reply members ctx} {
    catch {unset ::spm_fxmap}
    foreach m $members { set ::spm_fxmap($m) 1 }
    catch {unset ::spm_seenmap}
    foreach m $reply {
        if {![info exists ::spm_fxmap($m)]} {
            fail "$ctx: reply member '$m' is not a member of the fixture"
        }
        if {[info exists ::spm_seenmap($m)]} {
            fail "$ctx: reply member '$m' was returned twice"
        }
        set ::spm_seenmap($m) 1
    }
    catch {unset ::spm_fxmap}
    catch {unset ::spm_seenmap}
}

proc spm_ratio_x100 {target baseline} {
    if {$baseline <= 0} { set baseline 1 }
    return [expr {($target * 100) / $baseline}]
}

start_server {tags {"setperf set external:skip needs:debug"}} {
    if {![wc_available]} {
        test "setperf-memory: skipped, server lacks WORK_COUNTERS" {
            skip "build with: make WORK_COUNTERS=yes"
        }
    } else {
    set saved [wc_quiesce]

    # ---- near-total SPOP: auxiliary memory follows `remaining` -------------
    test "setperf-memory: near-total SPOP keeps working memory proportional to the remainder" {
        set sizes [wc_ht_sizes]
        set spm_one_max {}
        set spm_none_max {}
        set spm_one_str {}
        set spm_none_str {}
        foreach n $sizes {
            set count [expr {$n - 5}]
            foreach ttl {none one} {
                set key "spm:$ttl"
                set fx [wc_fixture $key $n $ttl]
                assert_equal [dict get [wc_setinfo $key] encoding] hashtable
                set spm_d($ttl) [wc_measure "r spop $key $count"]
                # A near-total pop returns `count` distinct live members and
                # leaves exactly the remainder behind.
                assert_equal [llength $::wc_last_reply] $count
                spm_assert_members $::wc_last_reply $fx "spop $key $count (ttl=$ttl n=$n)"
                assert_equal [r scard $key] 5
                wc_record $::cur_test "spop key n-5" [dict create n $n ttl $ttl enc hashtable count $count] $spm_d($ttl)
            }
            lappend spm_none_max [wc_get $spm_d(none) mem_max_alloc]
            lappend spm_one_max [wc_get $spm_d(one) mem_max_alloc]
            lappend spm_none_str [wc_get $spm_d(none) str_objs_created]
            lappend spm_one_str [wc_get $spm_d(one) str_objs_created]
            if {$::verbose} {
                puts "near-total SPOP n=$n count=$count:"
                puts "  none: mem_max_alloc=[wc_get $spm_d(none) mem_max_alloc] mem_peak_live_delta=[wc_get $spm_d(none) mem_peak_live_delta] str_objs=[wc_get $spm_d(none) str_objs_created] sds_copies=[wc_get $spm_d(none) sds_copies] prop_peak_retained=[wc_get $spm_d(none) prop_peak_retained_bytes] prop_dropped=[wc_get $spm_d(none) prop_cmds_dropped]"
                puts "  one : mem_max_alloc=[wc_get $spm_d(one) mem_max_alloc] mem_peak_live_delta=[wc_get $spm_d(one) mem_peak_live_delta] str_objs=[wc_get $spm_d(one) str_objs_created] sds_copies=[wc_get $spm_d(one) sds_copies] prop_peak_retained=[wc_get $spm_d(one) prop_peak_retained_bytes] reservoir_passes=[wc_get $spm_d(one) set_reservoir_passes]"
            }
        }
        foreach n $sizes vnone $spm_none_max vone $spm_one_max snone $spm_none_str sone $spm_one_str {
            # `fixture=` below reports the post-pop remainder; the measured
            # population is named here.
            set extra "measured: n=$n count=[expr {$n - 5}] remainder=5"
            # The parent remembers `remaining` (5) members; its largest single
            # allocation is the fixed propagation argv. One TTL must not add a
            # `count`-sized array.
            wc_assert_ratio "largest single allocation (bytes)" $vone $vnone 2 8192 \
                "near-total SPOP working memory is sized by the remainder, not by count (n=$n)" \
                "spop key [expr {$n - 5}]" spm:one $extra
            wc_assert_ratio "string objects created" $sone $snone 2 16 \
                "near-total SPOP must not copy more members than it returns (n=$n)" \
                "spop key [expr {$n - 5}]" spm:one $extra
        }
        # Same request shape (pop all but 5) at growing populations: the
        # remainder is constant, so the working set must be too.
        wc_assert_flat "largest single allocation (bytes, plain, sanity)" $sizes $spm_none_max 2 8192 \
            "the parent near-total strategy allocates by the remainder" "spop key n-5" spm:none
        wc_assert_flat "largest single allocation (bytes, one TTL)" $sizes $spm_one_max 2 8192 \
            "a member TTL must not make near-total SPOP allocate a population-sized array" "spop key n-5" spm:one
    } {} {slow}

    test "setperf-memory: near-total SPOP count = n-1 keeps working memory proportional to the remainder" {
        set n 20000
        set count [expr {$n - 1}]
        foreach ttl {none one all} {
            set key "spm:$ttl"
            set fx [wc_fixture $key $n $ttl]
            assert_equal [dict get [wc_setinfo $key] encoding] hashtable
            set spm_e($ttl) [wc_measure "r spop $key $count"]
            assert_equal [llength $::wc_last_reply] $count
            spm_assert_members $::wc_last_reply $fx "spop $key $count (ttl=$ttl)"
            assert_equal [r scard $key] 1
            wc_record $::cur_test "spop key n-1" [dict create n $n ttl $ttl enc hashtable count $count] $spm_e($ttl)
        }
        if {$::verbose} {
            foreach ttl {none one all} {
                puts "SPOP n-1 ttl=$ttl: mem_max_alloc=[wc_get $spm_e($ttl) mem_max_alloc] mem_peak_live_delta=[wc_get $spm_e($ttl) mem_peak_live_delta] str_objs=[wc_get $spm_e($ttl) str_objs_created] prop_peak_retained=[wc_get $spm_e($ttl) prop_peak_retained_bytes]"
            }
        }
        foreach ttl {one all} {
            wc_assert_ratio "largest single allocation (bytes)" [wc_get $spm_e($ttl) mem_max_alloc] [wc_get $spm_e(none) mem_max_alloc] 2 8192 \
                "popping all but one member needs no count-sized auxiliary array (ttl=$ttl)" "spop key $count" spm:$ttl
            wc_assert_ratio "string objects created" [wc_get $spm_e($ttl) str_objs_created] [wc_get $spm_e(none) str_objs_created] 2 16 \
                "popping all but one member copies each returned member once (ttl=$ttl)" "spop key $count" spm:$ttl
        }
    } {} {slow}

    # ---- near-total SPOP with a hidden member ------------------------------
    #
    # The remainder a near-total pop has to remember is defined by the LIVE
    # members it returns; a hidden member is part of neither the reply nor the
    # reclamation work, so it must not change the strategy, the working memory
    # or the propagated volume, and it must still be in the set afterwards.
    test "setperf-memory: near-total SPOP with one hidden member sizes memory by the remainder and reclaims nothing" {
        set n 20000
        set live_n [expr {$n - 1}]
        foreach count [list [expr {$n - 5}] $live_n] {
            foreach ttl {none one_expired} {
                set key "spm:$ttl"
                set fx [wc_fixture $key $n $ttl]
                assert_equal [dict get [wc_setinfo $key] encoding] hashtable
                set spm_h($ttl) [wc_measure "r spop $key $count"]
                set spm_reply($ttl) $::wc_last_reply
                wc_record $::cur_test "spop key $count" \
                    [dict create n $n ttl $ttl enc hashtable count $count] $spm_h($ttl)
            }
            # The reply is drawn from the live members only: never the hidden
            # one, never a duplicate, and as many as the live population allows.
            set expected [expr {$count < $live_n ? $count : $live_n}]
            assert_equal [llength $spm_reply(one_expired)] $expected
            assert_equal [lsearch -exact $spm_reply(one_expired) m0] -1
            spm_assert_members $spm_reply(one_expired) $fx "spop spm:one_expired $count"
            set extra "measured: n=$n count=$count live=$live_n returned=$expected"
            # Checked before reading the fixture: a pop that also removed the
            # hidden member can leave the set empty and the key gone, and DEBUG
            # WORKCTR SETINFO would error instead of reporting the violation.
            wc_assert_eq "key still exists" [r exists spm:one_expired] 1 \
                "the hidden member is not the pop's to remove, so the key survives a near-total pop" \
                "spop key $count" spm:one_expired $extra
            set info [wc_setinfo spm:one_expired]
            wc_assert_le "members reclaimed" [wc_get $spm_h(one_expired) set_members_reclaimed] 0 \
                "a pop reclaims nothing it was not asked to pop" "spop key $count" spm:one_expired $extra
            wc_assert_eq "physical members after the pop" [dict get $info physical] [expr {$n - $expected}] \
                "the hidden member is still allocated after a near-total pop" \
                "spop key $count" spm:one_expired $extra
            wc_assert_eq "volatile members after the pop" [dict get $info volatile] 1 \
                "the hidden member keeps its index entry" "spop key $count" spm:one_expired $extra
            # Same strategy as the no-TTL pop: the remainder, not the count.
            wc_assert_ratio "largest single allocation (bytes)" \
                [wc_get $spm_h(one_expired) mem_max_alloc] [wc_get $spm_h(none) mem_max_alloc] 2 8192 \
                "one hidden member must not make the pop allocate a count-sized array" \
                "spop key $count" spm:one_expired $extra
            wc_assert_ratio "string objects created" \
                [wc_get $spm_h(one_expired) str_objs_created] [wc_get $spm_h(none) str_objs_created] 2 16 \
                "the pop copies the members it returns, not the hidden one" \
                "spop key $count" spm:one_expired $extra
            wc_assert_ratio "propagated arguments" \
                [wc_sum $spm_h(one_expired) prop_args prop_dropped_args] \
                [wc_sum $spm_h(none) prop_args prop_dropped_args] 1 8 \
                "the pop propagates the members it returned and nothing else" \
                "spop key $count" spm:one_expired $extra
        }
    } {} {slow}

    # ---- SPOP count >= cardinality: TTL-independent whole-set return -------
    test "setperf-memory: SPOP count >= cardinality is TTL-independent" {
        set sizes [wc_ht_sizes]
        set spm_ratios {}
        foreach n $sizes {
            set count [expr {$n + 10}]
            foreach ttl {none one} {
                set key "spm:$ttl"
                set fx [wc_fixture $key $n $ttl]
                assert_equal [dict get [wc_setinfo $key] encoding] hashtable
                set spm_t($ttl) [wc_measure "r spop $key $count"]
                assert_equal [llength $::wc_last_reply] $n
                spm_assert_members $::wc_last_reply $fx "spop $key $count (ttl=$ttl n=$n)"
                # The whole set was popped, so the key is gone.
                assert_equal [r exists $key] 0
                assert_equal [r scard $key] 0
                wc_record $::cur_test "spop key n+10" [dict create n $n ttl $ttl enc hashtable count $count] $spm_t($ttl)
            }
            set spm_ex_none [wc_ht_examined $spm_t(none)]
            set spm_ex_one [wc_ht_examined $spm_t(one)]
            if {$::verbose} {
                puts "total-pop n=$n: examined none=$spm_ex_none one=$spm_ex_one; mem_max_alloc none=[wc_get $spm_t(none) mem_max_alloc] one=[wc_get $spm_t(one) mem_max_alloc]; mem_peak_live_delta none=[wc_get $spm_t(none) mem_peak_live_delta] one=[wc_get $spm_t(one) mem_peak_live_delta]"
            }
            # This path never consults member TTLs: same code, same budget.
            wc_assert_ratio "hashtable entries examined" $spm_ex_one $spm_ex_none 2 500 \
                "returning the whole set does not depend on TTLs (n=$n)" "spop key $count" spm:one
            # Three inherent passes: iterate the source, insert into the
            # temporary union set, iterate it to reply.
            wc_assert_le "hashtable entries examined (one TTL)" $spm_ex_one [expr {5 * $n + 1000}] \
                "returning the whole set is a bounded number of passes, not one per member" "spop key $count" spm:one
            wc_assert_ratio "largest single allocation (bytes)" [wc_get $spm_t(one) mem_max_alloc] [wc_get $spm_t(none) mem_max_alloc] 2 8192 \
                "TTLs add no auxiliary storage to the whole-set return (n=$n)" "spop key $count" spm:one
            wc_assert_ratio "peak live auxiliary bytes" [wc_get $spm_t(one) mem_peak_live_delta] [wc_get $spm_t(none) mem_peak_live_delta] 2 8192 \
                "TTLs add no auxiliary storage to the whole-set return (n=$n)" "spop key $count" spm:one
            lappend spm_ratios [spm_ratio_x100 [wc_get $spm_t(one) mem_max_alloc] [wc_get $spm_t(none) mem_max_alloc]]
        }
        # The absolute allocation of this path grows with n in BOTH fixtures:
        # the reply is built by copying the whole set into a temporary union set
        # (sunionDiffGenericCommand), which is inherited from the parent and out
        # of scope here. What must not grow is the TTL fixture's share of it.
        wc_assert_flat "one-TTL/plain largest-allocation ratio (x100)" $sizes $spm_ratios 2 100 \
            "the TTL fixture's share of the whole-set copy must not grow with the population" "spop key n+10" spm:one
    } {} {slow}

    # ---- SRANDMEMBER: aux table vs reservoir --------------------------------
    test "setperf-memory: SRANDMEMBER count = n/2 auxiliary memory stays within the parent aux table" {
        set n 20000
        set count [expr {$n / 2}]
        foreach ttl {none one all} {
            set key "spm:$ttl"
            set fx [wc_fixture $key $n $ttl]
            assert_equal [dict get [wc_setinfo $key] encoding] hashtable
            set spm_r($ttl) [wc_measure "r srandmember $key $count"]
            assert_equal [llength $::wc_last_reply] $count
            spm_assert_members $::wc_last_reply $fx "srandmember $key $count (ttl=$ttl)"
            # SRANDMEMBER is read-only.
            assert_equal [r scard $key] $n
            wc_record $::cur_test "srandmember key n/2" [dict create n $n ttl $ttl enc hashtable count $count] $spm_r($ttl)
        }
        if {$::verbose} {
            foreach ttl {none one all} {
                puts "srandmember n/2 ttl=$ttl: examined=[wc_ht_examined $spm_r($ttl)] mem_max_alloc=[wc_get $spm_r($ttl) mem_max_alloc] mem_peak_live_delta=[wc_get $spm_r($ttl) mem_peak_live_delta] str_objs=[wc_get $spm_r($ttl) str_objs_created] sds_copies=[wc_get $spm_r($ttl) sds_copies]"
            }
        }
        # At count*3 > size the parent legitimately builds an auxiliary table of
        # ALL members and subtracts randoms, so the contract is "no worse than
        # the parent by more than a factor", not "no auxiliary memory".
        foreach ttl {one all} {
            wc_assert_ratio "hashtable entries examined" [wc_ht_examined $spm_r($ttl)] [wc_ht_examined $spm_r(none)] 2 500 \
                "half-set selection is one pass over the population (ttl=$ttl)" "srandmember key $count" spm:$ttl
            wc_assert_ratio "largest single allocation (bytes)" [wc_get $spm_r($ttl) mem_max_alloc] [wc_get $spm_r(none) mem_max_alloc] 2 4096 \
                "half-set selection must not need more working memory than the parent aux table (ttl=$ttl)" "srandmember key $count" spm:$ttl
            wc_assert_ratio "peak live auxiliary bytes" [wc_get $spm_r($ttl) mem_peak_live_delta] [wc_get $spm_r(none) mem_peak_live_delta] 2 4096 \
                "half-set selection must not need more working memory than the parent aux table (ttl=$ttl)" "srandmember key $count" spm:$ttl
            wc_assert_ratio "member copies (string objects + sds copies)" \
                [wc_sum $spm_r($ttl) str_objs_created sds_copies] [wc_sum $spm_r(none) str_objs_created sds_copies] 2 16 \
                "half-set selection must not copy each member more than the parent does (ttl=$ttl)" "srandmember key $count" spm:$ttl
        }
    } {} {slow}

    test "setperf-memory: SRANDMEMBER count = 5 stays request-sized" {
        set n 20000
        set count 5
        foreach ttl {none one all} {
            set key "spm:$ttl"
            set fx [wc_fixture $key $n $ttl]
            assert_equal [dict get [wc_setinfo $key] encoding] hashtable
            set spm_s($ttl) [wc_measure "r srandmember $key $count"]
            assert_equal [llength $::wc_last_reply] $count
            spm_assert_members $::wc_last_reply $fx "srandmember $key $count (ttl=$ttl)"
            assert_equal [r scard $key] $n
            wc_record $::cur_test "srandmember key 5" [dict create n $n ttl $ttl enc hashtable count $count] $spm_s($ttl)
        }
        if {$::verbose} {
            foreach ttl {none one all} {
                puts "srandmember 5 ttl=$ttl: examined=[wc_ht_examined $spm_s($ttl)] mem_max_alloc=[wc_get $spm_s($ttl) mem_max_alloc] mem_peak_live_delta=[wc_get $spm_s($ttl) mem_peak_live_delta] str_objs=[wc_get $spm_s($ttl) str_objs_created] sds_copies=[wc_get $spm_s($ttl) sds_copies] reservoir_passes=[wc_get $spm_s($ttl) set_reservoir_passes]"
            }
        }
        # 5 of 20000: the parent samples 5 random entries into a 5-entry table.
        foreach ttl {one all} {
            wc_assert_ratio "hashtable entries examined" [wc_ht_examined $spm_s($ttl)] [wc_ht_examined $spm_s(none)] 4 500 \
                "a 5-member request must not traverse the population (ttl=$ttl)" "srandmember key 5" spm:$ttl
            wc_assert_ratio "largest single allocation (bytes)" [wc_get $spm_s($ttl) mem_max_alloc] [wc_get $spm_s(none) mem_max_alloc] 2 1024 \
                "a 5-member request keeps request-sized working memory (ttl=$ttl)" "srandmember key 5" spm:$ttl
            wc_assert_ratio "peak live auxiliary bytes" [wc_get $spm_s($ttl) mem_peak_live_delta] [wc_get $spm_s(none) mem_peak_live_delta] 2 1024 \
                "a 5-member request keeps request-sized working memory (ttl=$ttl)" "srandmember key 5" spm:$ttl
            wc_assert_ratio "member copies (string objects + sds copies)" \
                [wc_sum $spm_s($ttl) str_objs_created sds_copies] [wc_sum $spm_s(none) str_objs_created sds_copies] 3 8 \
                "a 5-member request copies at most the members it returns (ttl=$ttl)" "srandmember key 5" spm:$ttl
        }
    } {} {slow}

    # ---- listpack: batched removal vs search-and-delete per member ---------
    test "setperf-memory: listpack SPOP removes the selected members in one batch" {
        set spm_lprows {}
        foreach n {32 128} {
            set counts [lsort -integer -unique [list 4 16 [expr {$n / 2}]]]
            foreach count $counts {
                foreach ttl {none one} {
                    set key "spmlp:$ttl"
                    set fx [wc_fixture $key $n $ttl]
                    set info [wc_setinfo $key]
                    assert_equal [dict get $info encoding] listpack
                    set spm_lpbytes($ttl) [dict get $info bytes]
                    set spm_l($ttl) [wc_measure "r spop $key $count"]
                    assert_equal [llength $::wc_last_reply] $count
                    spm_assert_members $::wc_last_reply $fx "spop $key $count (ttl=$ttl n=$n)"
                    assert_equal [r scard $key] [expr {$n - $count}]
                    wc_record $::cur_test "spop key $count" [dict create n $n ttl $ttl enc listpack count $count] $spm_l($ttl)
                }
                lappend spm_lprows [list $n $count $spm_lpbytes(one) $spm_l(none) $spm_l(one)]
                if {$::verbose} {
                    puts "listpack SPOP n=$n count=$count: none lp_deletes=[wc_get $spm_l(none) lp_deletes] lp_batch=[wc_get $spm_l(none) lp_batch_deletes] lp_finds=[wc_get $spm_l(none) lp_find_calls] tail_moved=[wc_get $spm_l(none) lp_tail_bytes_moved] | one lp_deletes=[wc_get $spm_l(one) lp_deletes] lp_batch=[wc_get $spm_l(one) lp_batch_deletes] lp_finds=[wc_get $spm_l(one) lp_find_calls] tail_moved=[wc_get $spm_l(one) lp_tail_bytes_moved]"
                }
            }
        }
        foreach row $spm_lprows {
            foreach {n count bytes dnone done} $row break
            set cmd "spop key $count"
            # `fixture=` below reports the key's post-test state; the measured
            # fixture is named here.
            set extra "measured: n=$n count=$count listpack bytes=$bytes"
            # The positions are already known from the selection pass, so the
            # removal is one batch delete: no re-search per member.
            wc_assert_le "listpack find calls" [wc_get $done lp_find_calls] 1 \
                "selected listpack members are deleted by position, not searched again (n=$n count=$count)" $cmd spmlp:one $extra
            wc_assert_le "listpack delete operations" [wc_get $done lp_deletes] [expr {1 + [wc_get $dnone lp_deletes]}] \
                "removing $count selected members is one batch delete (n=$n)" $cmd spmlp:one $extra
            # A batch delete compacts the listpack once; per-member deletion
            # memmoves the tail once per member.
            wc_assert_le "listpack bytes memmoved" [wc_get $done lp_tail_bytes_moved] \
                [expr {2 * $bytes + [wc_get $dnone lp_tail_bytes_moved]}] \
                "batched removal moves the tail a bounded number of times (n=$n count=$count)" $cmd spmlp:one $extra
        }
    }

    test "setperf-memory: listpack SREM is one find per named member" {
        set n 128
        set k 8
        foreach ttl {none one} {
            set key "spmlp:$ttl"
            set fx [wc_fixture $key $n $ttl]
            assert_equal [dict get [wc_setinfo $key] encoding] listpack
            set spm_rem [lrange $fx 0 [expr {$k - 1}]]
            set spm_m($ttl) [wc_measure "r srem $key $spm_rem"]
            assert_equal $::wc_last_reply $k
            assert_equal [r scard $key] [expr {$n - $k}]
            foreach mem $spm_rem { assert_equal [r sismember $key $mem] 0 }
            wc_record $::cur_test "srem key $k members" [dict create n $n ttl $ttl enc listpack count $k] $spm_m($ttl)
        }
        if {$::verbose} {
            foreach ttl {none one} {
                puts "listpack SREM k=$k ttl=$ttl: lp_finds=[wc_get $spm_m($ttl) lp_find_calls] lp_deletes=[wc_get $spm_m($ttl) lp_deletes] tail_moved=[wc_get $spm_m($ttl) lp_tail_bytes_moved] find_steps=[wc_get $spm_m($ttl) lp_find_steps]"
            }
        }
        # Baseline of legitimate per-member deletion: SREM names its members, so
        # one find and one delete each is the whole cost. This is what a batched
        # SPOP must NOT degrade to; only the find count is asserted here.
        foreach ttl {none one} {
            wc_assert_le "listpack find calls" [wc_get $spm_m($ttl) lp_find_calls] $k \
                "SREM finds each named member once (ttl=$ttl)" "srem key $k members" spmlp:$ttl
        }
    }

    # ---- repeated small SPOP: algorithm selection, no expiry work ----------
    test "setperf-memory: repeated small SPOP on an all-TTL hashtable does not rescan the population" {
        set n 20000
        set rounds 20
        set count 3
        wc_fixture spm:none $n none
        assert_equal [dict get [wc_setinfo spm:none] encoding] hashtable
        set spm_base [wc_measure "r spop spm:none $count"]
        assert_equal [llength $::wc_last_reply] $count
        set spm_base_ex [wc_ht_examined $spm_base]
        wc_record $::cur_test "spop key 3 (baseline)" [dict create n $n ttl none enc hashtable count $count] $spm_base

        # Every member has a far-future TTL: nothing is expired, so no reclaim
        # work is due and the only difference from the baseline is which
        # strategy the command picks.
        set fx [wc_fixture spm:all $n all]
        assert_equal [dict get [wc_setinfo spm:all] encoding] hashtable
        assert_equal [dict get [wc_setinfo spm:all] live] $n
        set spm_total 0
        set spm_passes 0
        for {set i 0} {$i < $rounds} {incr i} {
            set spm_round [wc_measure "r spop spm:all $count"]
            assert_equal [llength $::wc_last_reply] $count
            spm_assert_members $::wc_last_reply $fx "spop spm:all $count (round $i)"
            assert_equal [r scard spm:all] [expr {$n - $count * ($i + 1)}]
            incr spm_total [wc_ht_examined $spm_round]
            incr spm_passes [wc_get $spm_round set_reservoir_passes]
            wc_record $::cur_test "spop key 3" [dict create n $n ttl all enc hashtable count $count round $i] $spm_round
        }
        assert_equal [dict get [wc_setinfo spm:all] physical] [expr {$n - $count * $rounds}]
        if {$::verbose} {
            puts "repeated SPOP 3 x $rounds on all-TTL n=$n: examined total=$spm_total (baseline none=$spm_base_ex per command), reservoir passes=$spm_passes"
        }
        wc_assert_le "hashtable entries examined over $rounds commands" $spm_total \
            [expr {$rounds * ($spm_base_ex * 4 + 500)}] \
            "popping 3 members from a live set is request-sized whether or not the members carry TTLs" \
            "spop spm:all $count" spm:all "baseline (plain set, same size): $spm_base_ex examined per command"
    } {} {slow}

    wc_restore $saved
    }
}

# Propagation retention is inherited: it needs a consumer to be observable, so
# it is measured against a fake replica instead of the standalone block above
# (where alsoPropagate drops every command and retains nothing).
start_server {tags {"setperf set repl external:skip needs:debug"}} {
    if {![wc_available]} {
        test "setperf-memory (repl): skipped, server lacks WORK_COUNTERS" {
            skip "build with: make WORK_COUNTERS=yes"
        }
    } else {
    set saved [wc_quiesce]

    test "setperf-memory: near-total SPOP propagates one SREM argv per popped member" {
        set n 3000
        set count [expr {$n - 5}]
        # 1024 members per propagated SREM (batchsize in spopWithCountCommand).
        set batches [expr {($count + 1023) / 1024}]
        foreach ttl {none one} {
            set key "spmp:$ttl"
            set fx [wc_fixture $key $n $ttl]
            assert_equal [dict get [wc_setinfo $key] encoding] hashtable
            set repl [attach_to_replication_stream]
            set spm_p($ttl) [wc_measure "r spop $key $count"]
            assert_equal [llength $::wc_last_reply] $count
            spm_assert_members $::wc_last_reply $fx "spop $key $count (ttl=$ttl)"
            set patterns [list {multi} {select *}]
            for {set i 0} {$i < $batches} {incr i} { lappend patterns "srem $key *" }
            lappend patterns {exec}
            assert_replication_stream $repl $patterns
            close_replication_stream $repl
            assert_equal [r scard $key] 5
            wc_record $::cur_test "spop key n-5 (repl attached)" [dict create n $n ttl $ttl enc hashtable count $count] $spm_p($ttl)
        }
        if {$::verbose} {
            foreach ttl {none one} {
                puts "SPOP n-5 with replica, ttl=$ttl: prop_cmds=[wc_get $spm_p($ttl) prop_cmds] prop_args=[wc_get $spm_p($ttl) prop_args] prop_arg_bytes=[wc_get $spm_p($ttl) prop_arg_bytes] prop_peak_retained_bytes=[wc_get $spm_p($ttl) prop_peak_retained_bytes] mem_max_alloc=[wc_get $spm_p($ttl) mem_max_alloc]"
            }
        }
        # Both paths queue one SREM per 1024 popped members and hold every argv
        # until the execution unit ends, so peak retained bytes are O(count) in
        # the parent too: an inherited design question, asserted only as
        # equivalence between the fixtures, never as a bound on `count`.
        wc_assert_ratio "propagated commands" [wc_get $spm_p(one) prop_cmds] [wc_get $spm_p(none) prop_cmds] 1 1 \
            "a member TTL must not change how the pop is propagated" "spop key $count" spmp:one
        wc_assert_ratio "propagated argv entries" [wc_get $spm_p(one) prop_args] [wc_get $spm_p(none) prop_args] 1 2 \
            "a member TTL must not propagate extra members" "spop key $count" spmp:one
        wc_assert_ratio "peak propagation bytes retained" [wc_get $spm_p(one) prop_peak_retained_bytes] [wc_get $spm_p(none) prop_peak_retained_bytes] 1 512 \
            "propagation retention is inherited and identical for both fixtures" "spop key $count" spmp:one
    } {} {slow}

    wc_restore $saved
    }
}
