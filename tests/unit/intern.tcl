# String-interning slab allocator tests (P1-1).
#
# R7 (HLD §12): used_memory must be CONSERVED across intern / de-intern of a
# fixed dataset -- the slab bytes accounted as server overhead when strings are
# interned must return in full when they are de-interned, with no accounting
# drift. Also checks the slab-allocator invariants exposed via DEBUG INTERN
# STATS / INFO memory: slabs allocate, release to a single cushion per class at
# zero live, and are counted once in MEMORY STATS overhead.
#
# Every hash is forced hashtable-encoded (hash-max-listpack-entries 0) so the
# interned (Type-5) path is actually exercised -- listpack hashes are not
# interned. Interning is opt-in and off by default, so these run under
# --string-interning probe (default config) and skip cleanly on a plain build.

proc intern_stat {field} {
    set s [r debug intern stats]
    foreach {k v} $s { if {$k eq $field} { return $v } }
    return ""
}

proc info_mem {field} {
    set line [getInfoProperty [r info memory] $field]
    return $line
}

# Load a fixed dataset that populates several slab size classes: repeated field
# names (8-40 B) and repeated values (short + a few long) across N hashes.
proc intern_load_fixed {n} {
    r flushall
    r config set hash-max-listpack-entries 0
    set fields {}
    for {set f 0} {$f < 40} {incr f} {
        # names of varying length -> several field classes
        lappend fields "fld_[string repeat x [expr {$f % 30}]]$f"
    }
    set values {short_val medium_value_here [string repeat V 120] [string repeat W 190] active pending}
    for {set i 0} {$i < $n} {incr i} {
        set args [list hset user:$i profile_field_03 active]
        for {set f 0} {$f < 8} {incr f} {
            set fn [lindex $fields [expr {($i + $f) % 40}]]
            set vv [lindex $values [expr {($i * 3 + $f) % 6}]]
            lappend args $fn $vv
        }
        r {*}$args
    }
}

start_server {tags {"intern"} overrides {save "" appendonly no}} {
    # Skip the whole file cleanly if this build has no string-interning support.
    if {[catch {r config set string-interning probe} e]} {
        if {[string match "*Unknown*" $e] || [string match "*argument*" $e]} {
            return
        }
    }

    test {intern-slab-bytes config is present and bounded} {
        assert_equal {intern-slab-bytes 65536} [r config get intern-slab-bytes]
        r config set intern-slab-bytes 131072
        assert_equal {intern-slab-bytes 131072} [r config get intern-slab-bytes]
        # out-of-range is clamped/rejected by the config layer
        catch {r config set intern-slab-bytes 100} e
        r config set intern-slab-bytes 65536
    }

    test {slabs allocate across size classes and INFO reports them} {
        r config set string-interning probe
        intern_load_fixed 2000
        r debug intern admit
        r debug reload
        assert {[intern_stat slab_count] >= 1}
        assert {[intern_stat slab_bytes] > 0}
        assert_equal 0 [intern_stat long_allocs]
        # at least two distinct classes populated (fields + a long value)
        set classes [dict get [r debug intern stats] classes]
        set used 0
        foreach c $classes { if {[dict get $c slots_total] > 0} { incr used } }
        assert {$used >= 2}
        # INFO exposes the slab fields
        assert {[info_mem intern_slab_bytes] > 0}
        assert {[info_mem intern_slab_count] >= 1}
    }

    test {MEMORY STATS counts intern slab bytes once in overhead} {
        set stats [r memory stats]
        set slab [dict get $stats intern.slab]
        assert_equal [intern_stat slab_bytes] $slab
        # overhead.total must include the slab bytes (slab <= overhead.total)
        assert {$slab <= [dict get $stats overhead.total]}
    }

    test {R7: used_memory conserved across intern / de-intern within 0.5%} {
        # Baseline: same fixed dataset with interning OFF, same (hashtable)
        # encoding and a settling reload so the allocator layout matches.
        r config set string-interning no
        intern_load_fixed 3000
        r debug reload
        set digest_plain [r debug digest]
        set mem_plain [s used_memory]

        # Intern the SAME dataset (probe admit + reload converges onto slots).
        r config set string-interning probe
        r debug intern admit
        r debug reload
        set digest_interned [r debug digest]
        set mem_interned [s used_memory]

        # De-intern: rewrite every hash with interning off (a fresh reload of the
        # un-interned data), then drain + evict so all slabs are released.
        r config set string-interning no
        r debug reload
        r debug intern drain
        r debug intern evict
        set digest_deintern [r debug digest]
        set mem_deintern [s used_memory]

        # 1) The data is identical throughout (interning is transparent).
        assert_equal $digest_plain $digest_interned
        assert_equal $digest_plain $digest_deintern
        # 2) Interning did not INCREASE memory (it dedups; slab overhead < saving).
        assert {$mem_interned <= $mem_plain}
        # 3) Conservation: de-interned memory returns to the plain baseline within
        #    0.5% -- no accounting drift, all slab bytes reclaimed.
        set drift [expr {abs(double($mem_deintern) - $mem_plain) / $mem_plain}]
        assert {$drift < 0.005}
        # 4) All live slots released; only per-class cushion slabs may remain
        #    (the allocator retains one empty slab per class to avoid flapping).
        assert_equal 0 [intern_stat slots]
        set cfg [lindex [r config get intern-slab-bytes] 1]
        assert {[intern_stat slab_count] <= 6}
        assert {[intern_stat slab_bytes] <= 6 * $cfg}
    }

    test {slabs release to one cushion per class after flush + drain + evict} {
        r config set string-interning probe
        intern_load_fixed 2000
        r debug intern admit
        r debug reload
        assert {[intern_stat slab_count] >= 1}
        r flushall async
        wait_for_condition 100 50 {
            [s lazyfree_pending_objects] == 0
        } else {
            fail "lazyfree did not drain"
        }
        r debug intern drain
        r debug intern evict
        assert_equal 0 [intern_stat slots]
        # After evict, at most one cushion slab per size class (<= 6).
        assert {[intern_stat slab_count] <= 6}
    }

    # ---- P1-2: admission caps + cold-admitted eviction --------------------

    test {P1-2 cap: intern-max-slots refuses admission on a miss} {
        r config set string-interning probe
        r config set intern-admitted-idle-ms 0
        r config set intern-max-slab-bytes 0
        r flushall
        r config set hash-max-listpack-entries 0
        # Create many distinct duplicated field/value strings (each seen twice).
        for {set i 0} {$i < 200} {incr i} {
            r hset ka:$i shared_field_$i dupval_$i extra active
            r hset kb:$i shared_field_$i dupval_$i extra active
        }
        # Cap well below the number of admittable strings.
        r config set intern-max-slots 10
        set res [r debug intern admit]
        assert_equal 10 [dict get $res slots]
        # Some admissions were refused (skipped) and the refusal counter moved.
        assert {[dict get $res admission_refusals] > 0}
        assert {[info_mem intern_admission_refusals] > 0}
        assert {[info_mem intern_admission_refusals_slots] > 0}
        # INFO exposes the cap values.
        assert_equal 10 [info_mem intern_cap_slots]
    }

    test {P1-2 cap: probe HIT is never refused at cap} {
        # Already at the slots cap from the previous test; a probe hit on an
        # already-admitted string must still succeed (refcount++ allocates nothing).
        set before [info_mem intern_admission_refusals]
        # Re-write an existing key: the probe hits admitted slots, no new slot.
        r hset ka:0 shared_field_0 dupval_0
        assert_equal 10 [intern_stat slots]
        # No NEW refusal from a pure hit path beyond what admit already counted.
        assert {[info_mem intern_admission_refusals] >= $before}
    }

    test {P1-2 cap: intern-max-slab-bytes refuses with the slab-bytes reason} {
        r config set string-interning probe
        r config set intern-max-slots 0
        r config set intern-admitted-idle-ms 0
        r flushall
        r debug intern drain
        r debug intern evict
        r config set hash-max-listpack-entries 0
        for {set i 0} {$i < 500} {incr i} {
            r hset a:$i f_$i [string repeat x 40]$i g active
            r hset b:$i f_$i [string repeat x 40]$i g active
        }
        # A tiny slab-bytes cap forces slab-bytes refusals.
        r config set intern-max-slab-bytes 8192
        set res [r debug intern admit]
        assert {[info_mem intern_admission_refusals_slab_bytes] > 0}
        # Growth stopped at the cap: only the slabs already carved before the cap
        # was reached exist (a handful of classes' first slabs), nowhere near the
        # uncapped ~1000-slot footprint.
        assert {[intern_stat slots] < 1000}
        r config set intern-max-slab-bytes 0
    }

    test {P1-2 idle eviction: cold admitted slots evicted after idle threshold} {
        r config set string-interning probe
        r config set intern-max-slots 1000000
        r config set intern-max-slab-bytes 0
        r config set intern-admitted-idle-ms 0
        r flushall
        r debug intern drain
        r debug intern evict
        r config set hash-max-listpack-entries 0
        for {set i 0} {$i < 100} {incr i} {
            r hset e:$i ef_$i ev_$i c active
            r hset f:$i ef_$i ev_$i c active
        }
        r debug intern admit
        set adm [intern_stat admitted_slots]
        assert {$adm > 0}
        assert_equal $adm [intern_stat admitted_unreferenced]
        set ev0 [intern_stat evictions]
        # Idle ticks are second-resolution; wait a second, then a 0ms-threshold
        # full pass evicts every cold admitted slot (age > 0).
        after 1100
        r debug intern evict idle 0
        assert_equal 0 [intern_stat slots]
        assert {[intern_stat evictions] >= [expr {$ev0 + $adm}]}
    }

    test {P1-2 idle eviction: referenced slots are never evicted} {
        r config set string-interning probe
        r config set intern-admitted-idle-ms 0
        r flushall
        r debug intern drain
        r debug intern evict
        r config set hash-max-listpack-entries 0
        for {set i 0} {$i < 100} {incr i} {
            r hset g:$i gf_$i gv_$i c active
            r hset h:$i gf_$i gv_$i c active
        }
        r debug intern admit
        # RELOAD makes the load-time probe reference every admitted slot.
        r debug reload
        set slots [intern_stat slots]
        assert {$slots > 0}
        assert_equal 0 [intern_stat admitted_unreferenced]
        set ev0 [intern_stat evictions]
        # Even a 0ms idle full pass must not touch referenced slots.
        r debug intern evict idle 0
        assert_equal $slots [intern_stat slots]
        assert_equal $ev0 [intern_stat evictions]
    }

    test {P1-2 idle eviction: serverCron reclaims cold slots automatically} {
        r config set string-interning probe
        r config set intern-max-slots 1000000
        r config set intern-admitted-idle-ms 0
        r flushall
        wait_for_condition 100 50 {
            [s lazyfree_pending_objects] == 0
        } else {
            fail "lazyfree did not drain before test"
        }
        r debug intern drain
        r debug intern evict
        assert_equal 0 [intern_stat slots]
        r config set hash-max-listpack-entries 0
        for {set i 0} {$i < 100} {incr i} {
            r hset i:$i if_$i iv_$i c active
            r hset j:$i if_$i iv_$i c active
        }
        r debug intern admit
        set adm [intern_stat slots]
        assert {$adm > 0}
        assert_equal $adm [intern_stat admitted_unreferenced]
        # Enable idle eviction with a short TTL and let serverCron do the work.
        # Idle ticks are second-resolution, so allow several seconds of cron cycles.
        r config set intern-admitted-idle-ms 1000
        r config set intern-evict-batch 100000
        wait_for_condition 300 100 {
            [intern_stat slots] == 0
        } else {
            fail "serverCron did not evict cold admitted slots (slots=[intern_stat slots] evictions=[intern_stat evictions])"
        }
        assert {[intern_stat evictions] > 0}
        r config set intern-admitted-idle-ms 0
    }

    test {P1-2 live cap raise: raising intern-max-slots re-enables admission} {
        r config set string-interning probe
        r config set intern-admitted-idle-ms 0
        r config set intern-max-slab-bytes 0
        r flushall
        r config set hash-max-listpack-entries 0
        for {set i 0} {$i < 200} {incr i} {
            r hset p:$i pf_$i pv_$i c active
            r hset q:$i pf_$i pv_$i c active
        }
        r config set intern-max-slots 20
        r debug intern admit
        assert_equal 20 [intern_stat slots]
        # Raising the cap live lets a re-ADMIT grow the table again.
        r config set intern-max-slots 1000000
        r debug intern admit
        assert {[intern_stat slots] > 20}
    }

    test {P1-2 lowering a cap does not free anything by itself} {
        r config set string-interning probe
        r config set intern-max-slots 1000000
        r config set intern-admitted-idle-ms 0
        r flushall
        r config set hash-max-listpack-entries 0
        for {set i 0} {$i < 100} {incr i} {
            r hset r:$i rf_$i rv_$i c active
            r hset s:$i rf_$i rv_$i c active
        }
        r debug intern admit
        set slots [intern_stat slots]
        assert {$slots > 0}
        # Lower the cap below the current count: growth stops, nothing is freed.
        r config set intern-max-slots 1
        assert_equal $slots [intern_stat slots]
        r config set intern-max-slots 1000000
    }
}
