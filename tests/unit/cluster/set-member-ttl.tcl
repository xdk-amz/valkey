# Set member expiration across a slot migration.

# Index of the node that owns $key's slot, resolved from the MOVED redirect a
# node answers with when the slot is not its own.
proc key_owner_index {key nodes} {
    if {![catch {R 0 EXISTS $key} err]} {
        return 0
    }
    assert_match "MOVED*" $err
    set target [lindex [split $err] 2]
    for {set i 0} {$i < $nodes} {incr i} {
        if {$target eq "127.0.0.1:[lindex [R $i CONFIG GET port] 1]"} {
            return $i
        }
    }
    fail "MOVED pointed at a node that is not in the cluster: $err"
}

# Hand $key's whole slot from node $from to node $to, moving the key with it.
proc migrate_key_slot {from to key} {
    set slot [R $from CLUSTER KEYSLOT $key]
    set from_id [R $from CLUSTER MYID]
    set to_id [R $to CLUSTER MYID]
    set to_port [lindex [R $to CONFIG GET port] 1]

    assert_equal {OK} [R $from CLUSTER SETSLOT $slot MIGRATING $to_id]
    assert_equal {OK} [R $to CLUSTER SETSLOT $slot IMPORTING $from_id]
    assert_equal {OK} [R $from MIGRATE 127.0.0.1 $to_port $key 0 5000]
    assert_equal {OK} [R $to CLUSTER SETSLOT $slot NODE $to_id]
    assert_equal {OK} [R $from CLUSTER SETSLOT $slot NODE $to_id]

    # The exporting node kept nothing: COUNTKEYSINSLOT answers for a slot it no
    # longer owns, where a plain EXISTS would only earn a redirect.
    assert_equal 0 [R $from CLUSTER COUNTKEYSINSLOT $slot]
    assert_equal 1 [R $to CLUSTER COUNTKEYSINSLOT $slot]
}

start_cluster 2 0 {tags {external:skip cluster}} {
    set key "{smemberttl}myset"

    test "MIGRATE carries member TTLs to the importing node" {
        set src [key_owner_index $key 2]
        set dst [expr {1 - $src}]
        R $src FLUSHALL
        R $dst FLUSHALL

        set exp [expr {[clock milliseconds] + 300000}]
        R $src SADD $key volatile1 volatile2 persistent
        R $src SPEXPIREAT $key $exp MEMBERS 2 volatile1 volatile2
        assert_equal 1 [get_keys_with_volatile_items [Rn $src]]

        migrate_key_slot $src $dst $key

        # The members arrived with the same absolute expiry stamps, not with the
        # time left at the moment of the migration, and the importing node now
        # tracks the key for active expiry.
        assert_equal 3 [R $dst SCARD $key]
        assert_equal [list $exp $exp -1] \
            [R $dst SPEXPIRETIME $key MEMBERS 3 volatile1 volatile2 persistent]
        assert_equal {persistent volatile1 volatile2} [lsort [R $dst SMEMBERS $key]]
        assert_equal 1 [get_keys_with_volatile_items [Rn $dst]]
        assert_equal 0 [get_keys_with_volatile_items [Rn $src]]
    }

    test "MIGRATE carries an unreclaimed expired member for the importing node to remove" {
        set src [key_owner_index $key 2]
        set dst [expr {1 - $src}]
        R $src FLUSHALL
        R $dst FLUSHALL
        R $src DEBUG SET-ACTIVE-EXPIRE 0
        R $dst DEBUG SET-ACTIVE-EXPIRE 0

        R $src SADD $key live
        R $src SADDEX $key PX 20 MEMBERS 1 expired
        wait_for_condition 50 100 {
            [R $src SISMEMBER $key expired] == 0
        } else {
            fail "the member did not expire on the exporting node"
        }
        assert_equal 2 [R $src SCARD $key]

        migrate_key_slot $src $dst $key

        # The serialized payload carries every member with its expiry, expired
        # ones included, and RESTORE does not skip them. Dropping them here would
        # give the importing node a different cardinality and digest for a set
        # the exporting node had not removed it yet.
        assert_equal 2 [R $dst SCARD $key]
        assert_equal {live} [R $dst SMEMBERS $key]
        assert_equal 0 [R $dst SISMEMBER $key expired]
        assert_equal -2 [lindex [R $dst SPTTL $key MEMBERS 1 expired] 0]

        # The importing node owns expiry from now on, so its own active expiration removes
        # the member and stops tracking the key.
        R $dst DEBUG SET-ACTIVE-EXPIRE 1
        wait_for_condition 100 100 {
            [R $dst SCARD $key] == 1
        } else {
            fail "the importing node did not remove the expired member"
        }
        assert_equal {live} [R $dst SMEMBERS $key]
        assert_equal 0 [get_keys_with_volatile_items [Rn $dst]]

        R $src DEBUG SET-ACTIVE-EXPIRE 1
    } {OK} {needs:debug}
}
