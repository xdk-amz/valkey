set testmodule [file normalize tests/modules/eventloop.so]

start_server {tags {"modules"}} {
    r module load $testmodule

    test "Module eventloop sendbytes" {
        assert_match "OK" [r test.sendbytes 5000000]
        assert_match "OK" [r test.sendbytes 2000000]
    }

    test "Module eventloop iteration" {
        set iteration [r test.iteration]
        set next_iteration [r test.iteration]
        assert {$next_iteration > $iteration}
    }

    test "Module eventloop sanity" {
        r test.sanity
    }

    test "Module eventloop oneshot" {
        r test.oneshot
    }

    test "Unload the module - eventloop" {
        assert_equal {OK} [r module unload eventloop]
    }
}

start_server {tags {"modules external:skip tls:skip"} overrides {io-threads 2 io-batch-hold-us 10000}} {
    r module load $testmodule
    r client setname control ;# named clients stay on the main path

    test "Module eventloop iteration for a client read by IO threads" {
        set rr [valkey [srv 0 host] [srv 0 port] 0 $::tls]
        wait_for_condition 100 20 {
            [getInfoProperty [r info fastpath] fastpath_clients] == 1
        } else {
            fail "a fresh client did not join the fast path"
        }
        # Each call asserts that no command runs between the loop's before-sleep and after-sleep events.
        for {set i 0} {$i < 200} {incr i} {
            $rr test.iteration
        }
        $rr close
    }
}
