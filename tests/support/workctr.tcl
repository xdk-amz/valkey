# Helpers for the set performance-contract suite (tests/unit/setperf-*.tcl).
#
# They drive the test-only work counters exposed by DEBUG WORKCTR, which exist
# only in a server built with `make WORK_COUNTERS=yes` (see src/workctr.h).
# Every measurement is one top-level command: `wc_measure {r spop k 2}` arms the
# counters, runs the command and returns a dict of counter name -> value.

set ::wc_seed [expr {[info exists ::env(SETPERF_SEED)] ? $::env(SETPERF_SEED) : 12345}]
set ::wc_extended [expr {[info exists ::env(SETPERF_EXTENDED)] && $::env(SETPERF_EXTENDED) ne "0"}]

# Hashtable fixture sizes: geometric, CI-sized; SETPERF_EXTENDED=1 adds larger ones.
proc wc_ht_sizes {} {
    if {$::wc_extended} { return {2000 20000 200000 1000000} }
    return {2000 20000 200000}
}

proc wc_available {} {
    if {[catch {r debug workctr get} e]} { return 0 }
    return 1
}

proc wc_seed {} {
    r debug workctr seed $::wc_seed
}

# Run exactly one command under the counters. `script` must issue one command
# through `r` (or the given client). Returns the counter dict and stores the
# reply in ::wc_last_reply.
proc wc_measure {script {client ""}} {
    if {$client eq ""} { set client [srv 0 client] }
    $client debug workctr seed $::wc_seed
    set expected_generation [$client debug workctr arm]
    set ::wc_last_reply [uplevel 1 $script]
    set d [dict create {*}[$client debug workctr get]]
    # A command rejected before call() (arity, type, ACL) leaves the arm set and
    # the GET itself would be measured next: refuse to hand back such a snapshot.
    if {[dict get $d generation] != $expected_generation} {
        error "wc_measure: the measured command did not reach call() (generation [dict get $d generation], expected $expected_generation); script: $script"
    }
    return $d
}

# Counters accumulated across a whole script (several commands, active expire,
# etc). Coarser than wc_measure: argument parsing and replies are included.
proc wc_window {script {client ""}} {
    if {$client eq ""} { set client [srv 0 client] }
    $client debug workctr seed $::wc_seed
    $client debug workctr reset
    uplevel 1 $script
    return [dict create {*}[$client debug workctr current]]
}

proc wc_get {d name} {
    # The vset_* family is index work by definition: report it whether it was
    # counted inside an index bracket (idx_ account) or outside (hash paths).
    if {[string match vset_* $name] && [dict exists $d idx_$name]} {
        return [expr {[dict get $d $name] + [dict get $d idx_$name]}]
    }
    dict get $d $name
}

# Sum of several counters.
proc wc_sum {d args} {
    set s 0
    foreach n $args { incr s [wc_get $d $n] }
    return $s
}

# encoding / physical / volatile / live / bytes of a set key as a dict.
proc wc_setinfo {key {client ""}} {
    if {$client eq ""} { set client [srv 0 client] }
    dict create {*}[$client debug workctr setinfo $key]
}

# Total entries examined by a hashtable command, whatever the access path.
proc wc_ht_examined {d} {
    wc_sum $d ht_iter_visits ht_scan_visits ht_bucket_probes
}

# Total listpack entries examined.
proc wc_lp_examined {d} {
    wc_sum $d lp_find_steps lp_next_steps lp_random_steps
}

# ---- fixtures -------------------------------------------------------------
#
# TTL distributions (all fixtures of one size and payload hold the SAME live
# members unless expired):
#   none          no member TTL
#   one           exactly one far-future TTL (isolates algorithm selection)
#   all           every member has a far-future TTL
#   mostly_expired all but `live_keep` members expired and unreclaimed
#   all_expired   every member expired and unreclaimed
#
# `payload` short = "m<i>", long = "m<i>" + 60 bytes (forces hashtable when it
# exceeds set-max-listpack-value).

proc wc_member {i payload} {
    if {$payload eq "long"} {
        return "m${i}-[string repeat x 60]"
    }
    return "m$i"
}

proc wc_members {n payload {start 0}} {
    set l {}
    for {set i $start} {$i < $start + $n} {incr i} { lappend l [wc_member $i $payload] }
    return $l
}

# Issue `cmd key ... members` in batches so no single argv is enormous.
proc wc_batched {client cmd key members {batch 4000}} {
    set n [llength $members]
    for {set i 0} {$i < $n} {incr i $batch} {
        set chunk [lrange $members $i [expr {$i + $batch - 1}]]
        $client $cmd $key {*}$chunk
    }
}

proc wc_spexpire_batched {client key ms members {batch 4000}} {
    set n [llength $members]
    for {set i 0} {$i < $n} {incr i $batch} {
        set chunk [lrange $members $i [expr {$i + $batch - 1}]]
        $client spexpire $key $ms members [llength $chunk] {*}$chunk
    }
}

# Build a set fixture. Returns the list of live members (in insertion order).
# Active expiration must already be disabled (wc_quiesce) for the expired
# distributions; expiry happens outside any measured command. `one_expired`
# expires m0 only: a large live population with a single hidden member.
proc wc_fixture {key n ttl {payload short} {live_keep 3} {client ""}} {
    if {$client eq ""} { set client [srv 0 client] }
    $client del $key
    set members [wc_members $n $payload]
    wc_batched $client sadd $key $members
    set far [expr {[clock milliseconds] + 3600 * 1000 * 24}]
    switch -- $ttl {
        none {}
        one { $client spexpireat $key $far members 1 [lindex $members 0] }
        all { wc_spexpire_batched $client $key 86400000 $members }
        one_expired {
            $client spexpire $key 1 members 1 [lindex $members 0]
            after 5
            set members [lrange $members 1 end]
        }
        mostly_expired {
            set expiring [lrange $members $live_keep end]
            wc_spexpire_batched $client $key 1 $expiring
            after 5
            set members [lrange $members 0 [expr {$live_keep - 1}]]
        }
        all_expired {
            wc_spexpire_batched $client $key 1 $members
            after 5
            set members {}
        }
        default { error "unknown ttl distribution $ttl" }
    }
    return $members
}

# n live members whose deadlines fall into `buckets` distinct expiry buckets.
# The index files an entry under its deadline rounded up to an 8192 ms window
# (vset.c get_max_bucket_ts), so the deadlines are spaced a minute apart to make
# one bucket each; all of them are far in the future, so nothing is hidden and
# the only variable is how many deadline groups the index holds.
proc wc_fixture_buckets {key n buckets {client ""}} {
    if {$client eq ""} { set client [srv 0 client] }
    $client del $key
    set members [wc_members $n short]
    wc_batched $client sadd $key $members
    set base [expr {[clock milliseconds] + 3600 * 1000}]
    for {set b 0} {$b < $buckets} {incr b} {
        set chunk {}
        for {set i $b} {$i < $n} {incr i $buckets} { lappend chunk [lindex $members $i] }
        if {[llength $chunk] == 0} continue
        set at [expr {$base + $b * 60000}]
        for {set j 0} {$j < [llength $chunk]} {incr j 4000} {
            set part [lrange $chunk $j [expr {$j + 3999}]]
            $client spexpireat $key $at members [llength $part] {*}$part
        }
    }
    return $members
}

# `live_keep` live members that all carry a FAR-FUTURE TTL, with the rest of the
# n members expired and unreclaimed. Distinct from `mostly_expired`, whose live
# members carry no TTL at all: here every live member is indexed by the vset, so
# a rank/select over the index can name one without reading a member, while in
# `mostly_expired` the live members are invisible to the index and finding one
# is the documented needle walk. Returns the live members.
proc wc_fixture_hidden_ttl {key n live_keep {client ""}} {
    if {$client eq ""} { set client [srv 0 client] }
    $client del $key
    set members [wc_members $n short]
    wc_batched $client sadd $key $members
    set live [lrange $members 0 [expr {$live_keep - 1}]]
    set far [expr {[clock milliseconds] + 3600 * 1000 * 24}]
    for {set i 0} {$i < [llength $live]} {incr i 4000} {
        set part [lrange $live $i [expr {$i + 3999}]]
        $client spexpireat $key $far members [llength $part] {*}$part
    }
    wc_spexpire_batched $client $key 1 [lrange $members $live_keep end]
    after 5
    return $live
}

# Force a hashtable encoding for small cardinalities by adding+removing a
# member longer than set-max-listpack-value (a hashtable never converts back).
proc wc_force_hashtable {key {client ""}} {
    if {$client eq ""} { set client [srv 0 client] }
    set big [string repeat y 200]
    $client sadd $key $big
    $client srem $key $big
}

# Disable everything that could touch the dataset behind our back. Returns the
# previous config so wc_restore can put it back.
proc wc_quiesce {{client ""}} {
    if {$client eq ""} { set client [srv 0 client] }
    set saved [dict create activedefrag [lindex [$client config get activedefrag] 1]]
    foreach opt {lazyfree-lazy-server-del lazyfree-lazy-user-del lazyfree-lazy-expire lazyfree-lazy-user-flush} {
        dict set saved $opt [lindex [$client config get $opt] 1]
    }
    $client debug set-active-expire 0
    catch {$client config set activedefrag no}
    # Frees on the bio threads are invisible to the (main-thread) counters.
    foreach opt {lazyfree-lazy-server-del lazyfree-lazy-user-del lazyfree-lazy-expire lazyfree-lazy-user-flush} {
        $client config set $opt no
    }
    return $saved
}

proc wc_restore {saved {client ""}} {
    if {$client eq ""} { set client [srv 0 client] }
    $client debug set-active-expire 1
    catch {$client config set activedefrag [dict get $saved activedefrag]}
    foreach opt {lazyfree-lazy-server-del lazyfree-lazy-user-del lazyfree-lazy-expire lazyfree-lazy-user-flush} {
        $client config set $opt [dict get $saved $opt]
    }
}

# ---- contract assertions -------------------------------------------------
#
# Every failure prints the command, fixture, seed, the compared counters and
# the violated contract, plus a reproduction line.

proc wc_ctx {cmd key {extra ""}} {
    set info ""
    catch {set info [wc_setinfo $key]}
    return "cmd={$cmd} key=$key fixture=[list $info] seed=$::wc_seed $extra"
}

proc wc_repro {cmd} {
    # ::cur_test is "<name> in <file>"; --only wants the bare name.
    set name $::cur_test
    regsub { in tests/.*$} $name {} name
    set unit [file rootname [string map {tests/ {}} $::curfile]]
    return "repro: SETPERF_SEED=$::wc_seed ./runtest --single $unit --only \"$name\"; server: DEBUG WORKCTR SEED $::wc_seed; DEBUG WORKCTR ARM; $cmd; DEBUG WORKCTR GET"
}

# actual <= bound
proc wc_assert_le {what actual bound contract cmd key {extra ""}} {
    if {$actual > $bound} {
        fail "CONTRACT VIOLATED: $contract\n  $what = $actual, bound = $bound\n  [wc_ctx $cmd $key $extra]\n  [wc_repro $cmd]"
    }
}

# actual == expected, for the survival and reply-shape contracts.
proc wc_assert_eq {what actual expected contract cmd key {extra ""}} {
    if {$actual != $expected} {
        fail "CONTRACT VIOLATED: $contract\n  $what = $actual, required = $expected\n  [wc_ctx $cmd $key $extra]\n  [wc_repro $cmd]"
    }
}

# actual >= floor, for accounting contracts.
proc wc_assert_ge {what actual floor contract cmd key {extra ""}} {
    if {$actual < $floor} {
        fail "CONTRACT VIOLATED: $contract\n  $what = $actual, required floor = $floor\n  [wc_ctx $cmd $key $extra]\n  [wc_repro $cmd]"
    }
}

# Keyspace changes the server booked, to gate a read command against writing.
proc wc_dirty {{client ""}} {
    if {$client eq ""} { set client [srv 0 client] }
    return [getInfoProperty [$client info persistence] rdb_changes_since_last_save]
}

# Commands a measured window handed to replication/AOF, whether or not a
# consumer was attached (with none they are counted as dropped).
proc wc_propagated {d} {
    wc_sum $d prop_cmds prop_cmds_dropped
}

# target <= baseline * factor + slack, for plain-vs-volatile comparisons.
proc wc_assert_ratio {what target baseline factor slack contract cmd key {extra ""}} {
    set bound [expr {$baseline * $factor + $slack}]
    if {$target > $bound} {
        fail "CONTRACT VIOLATED: $contract\n  $what: target = $target, baseline = $baseline, allowed = baseline*$factor+$slack = $bound\n  [wc_ctx $cmd $key $extra]\n  [wc_repro $cmd]"
    }
}

# Scaling check: values measured at geometrically increasing sizes must not
# grow with the size (fixed/request-sized work), allowing `factor` of noise.
proc wc_assert_flat {what sizes values factor slack contract cmd key} {
    set first [lindex $values 0]
    foreach n $sizes v $values {
        set bound [expr {$first * $factor + $slack}]
        if {$v > $bound} {
            fail "CONTRACT VIOLATED: $contract\n  $what at sizes {$sizes} = {$values}; at n=$n value $v exceeds first*$factor+$slack = $bound (population-sized work)\n  [wc_ctx $cmd $key] \n  [wc_repro $cmd]"
        }
    }
}

# Pretty one-line summary of the counters that are non-zero, for reports.
proc wc_nonzero {d} {
    set out {}
    dict for {k v} $d { if {$v != 0} { lappend out "$k=$v" } }
    return [join $out " "]
}

# Append a machine-readable record to the results file when SETPERF_RESULTS is set.
proc wc_record {test cmd fixture d} {
    if {![info exists ::env(SETPERF_RESULTS)]} return
    set fp [open $::env(SETPERF_RESULTS) a]
    set kv {}
    dict for {k v} $d { lappend kv "\"$k\":$v" }
    set fx {}
    dict for {k v} $fixture { lappend fx "\"$k\":\"$v\"" }
    puts $fp "{\"test\":\"$test\",\"cmd\":\"$cmd\",\"seed\":$::wc_seed,\"fixture\":{[join $fx ,]},\"counters\":{[join $kv ,]}}"
    close $fp
}
