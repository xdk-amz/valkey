# Fast-path clients under AOF appendfsync always: the reply-publication barrier holds a write's
# reply until the beforeSleep fsync, so this asserts that fast-path DB0 connections stay correct
# and keep write/read ordering under that policy. The sub-millisecond timing of the barrier is
# proven deterministically by the C++ unit tests (test_fastpath_aof_barrier.cpp); here we assert
# functional correctness end to end.

# A deferring client that sends nothing before its first command, so it stays on the fast path
# (a SELECT would move it to the main path).
proc fp_client {} {
    return [valkey [srv 0 host] [srv 0 port] 1 $::tls]
}

proc fp_wait_fastpath_clients {n} {
    wait_for_condition 100 20 {
        [getInfoProperty [r info fastpath] fastpath_clients] == $n
    } else {
        fail "expected $n fast-path clients: [r info fastpath]"
    }
}

start_server {tags {"fastpath aof external:skip"} overrides {appendonly yes appendfsync always io-threads 2 enable-debug-command yes}} {
    assert_equal {io-threads 2} [r config get io-threads]
    assert_equal {io-threads-fast-path yes} [r config get io-threads-fast-path]
    assert_equal {appendfsync always} [r config get appendfsync]
    # A named, main-path control connection to observe INFO and the shared dataset.
    r client setname fp-control
    r select 0

    test {Fast path appendfsync always: interleaved writes and reads stay correct and ordered} {
        set rd [fp_client]
        fp_wait_fastpath_clients 1

        # Pipeline writes and reads that depend on those writes; replies must come back in order.
        $rd set k1 v1
        $rd get k1
        $rd incr counter
        $rd incr counter
        $rd get counter
        $rd append k1 z
        $rd get k1

        assert_equal OK [$rd read]
        assert_equal v1 [$rd read]
        assert_equal 1 [$rd read]
        assert_equal 2 [$rd read]
        assert_equal 2 [$rd read]
        assert_equal 3 [$rd read]
        assert_equal v1z [$rd read]

        # Once every reply is read the writes were released past the fsync, so nothing stays held.
        assert_equal 0 [getInfoProperty [r info fastpath] fastpath_pending_durable_batches]
        $rd close
    }

    test {Fast path appendfsync always: a fast-path write is visible to the main-path connection} {
        set rd [fp_client]
        fp_wait_fastpath_clients 1
        $rd set persisted yes
        assert_equal OK [$rd read]
        assert_equal yes [r get persisted]
        $rd close
    }

    test {Fast path appendfsync always: many pipelined writes all acknowledge and persist in order} {
        set rd [fp_client]
        fp_wait_fastpath_clients 1
        for {set i 0} {$i < 200} {incr i} { $rd rpush mylist item:$i }
        for {set i 0} {$i < 200} {incr i} { assert_equal [expr {$i + 1}] [$rd read] }
        $rd lrange mylist 0 -1
        set got [$rd read]
        assert_equal 200 [llength $got]
        assert_equal item:0 [lindex $got 0]
        assert_equal item:199 [lindex $got 199]
        assert_equal 0 [getInfoProperty [r info fastpath] fastpath_pending_durable_batches]
        $rd close
    }
}

start_server {tags {"fastpath aof external:skip tls:skip"} overrides {appendonly yes appendfsync everysec io-threads 4 io-threads-always-active yes save ""}} {
    r select 0
    waitForBgrewriteaof r
    proc fpa_speculated {} { getInfoProperty [r info fastpath] fastpath_speculated }

    test {Fast path appendfsync always: IO-thread reads stop while every reply must wait for the fsync} {
        r set k v
        set rd [fp_client]
        fp_wait_fastpath_clients 1
        set before [fpa_speculated]
        for {set round 0} {$round < 50 && [fpa_speculated] == $before} {incr round} {
            for {set i 0} {$i < 10} {incr i} { $rd get k }
            for {set i 0} {$i < 10} {incr i} { assert_equal v [$rd read] }
        }
        assert_morethan [fpa_speculated] $before

        r config set appendfsync always
        set before [fpa_speculated]
        for {set i 0} {$i < 200} {incr i} { $rd get k }
        for {set i 0} {$i < 200} {incr i} { assert_equal v [$rd read] }
        assert_equal $before [fpa_speculated]
        $rd close
        r config set appendfsync everysec
    }
}
