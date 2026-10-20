proc intern_node_stat {idx field} {
    if {[regexp "\r\n$field:(\[0-9\]+)" [R $idx info memory] -> v]} {return $v}
    return 0
}

start_cluster 2 0 {tags {external:skip cluster intern}} {
    test "Atomic slot migration moves shared values and the target shares them" {
        set slot [R 0 cluster keyslot "{intern}"]
        if {[catch {R 0 set "{intern}probe" x}]} {
            set src 1
            set dst 0
        } else {
            R 0 del "{intern}probe"
            set src 0
            set dst 1
        }
        set v [string repeat c 300]
        for {set i 0} {$i < 1000} {incr i} {
            R $src set "{intern}k$i" $v
            R $src hset "{intern}h$i" f $v
        }
        assert_morethan [intern_node_stat $src intern_refs] 1900

        assert_match OK [R $src CLUSTER MIGRATESLOTS SLOTSRANGE $slot $slot NODE [R $dst CLUSTER MYID]]
        wait_for_condition 100 100 {
            [R $dst CLUSTER COUNTKEYSINSLOT $slot] == 2000 && [R $src CLUSTER COUNTKEYSINSLOT $slot] == 0
        } else {
            fail "slot $slot did not migrate"
        }
        wait_for_cluster_propagation

        assert_morethan [intern_node_stat $dst intern_refs] 1900
        assert_equal $v [R $dst get "{intern}k0"]
        assert_equal $v [R $dst hget "{intern}h999" f]
        wait_for_condition 50 100 {
            [intern_node_stat $src intern_strings] == 0 && [intern_node_stat $src intern_refs] == 0 &&
            [intern_node_stat $src intern_singletons] == 0
        } else {
            fail "source did not release its references: [R $src info memory]"
        }
    }
}
