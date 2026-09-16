# Tests for reply copy-avoidance (zero-copy bulk/raw string reply offload).
#
# The server ships a scatter-gather reply path that, instead of memcpy-ing a
# stored RAW string value into the client output buffer, stores a reference to
# the value object (bulkStrRef) and lets the writer emit the object bytes
# directly via writev. It is gated by isCopyAvoidPreferred() and three hidden
# configs. These tests force the ref path on and assert wire correctness,
# ordering, refcount safety on teardown, partial-write resume, and that the COB
# limiter accounts the true on-wire size (io_tracked_reply_len) rather than the
# 16-byte reference proxy.

# Force the copy-avoidance ref path on for any RAW value size. With
# min-io-threads-avoid-copy-reply <= io-threads, isCopyAvoidPreferred() returns
# true for every RAW, non-shared string regardless of size.
proc reply_offload_on {} {
    if {$::ca_mode eq "adaptive"} {
        r config set avoid-copy-reply-mode adaptive
        r debug copy-avoid engage 1
    } else {
        r config set avoid-copy-reply-mode static
        r config set io-threads-always-active yes
        r config set min-io-threads-avoid-copy-reply 2
        r config set min-string-size-avoid-copy-reply 1
        r config set min-string-size-avoid-copy-reply-threaded 1
    }
}

# Force the ref path off: no thread-count shortcut and an unreachable size gate.
proc reply_offload_off {} {
    if {$::ca_mode eq "adaptive"} {
        r config set avoid-copy-reply-mode adaptive
        r debug copy-avoid release 2147483647
    } else {
        r config set avoid-copy-reply-mode static
        r config set min-io-threads-avoid-copy-reply 0
        r config set min-string-size-avoid-copy-reply 0
        r config set min-string-size-avoid-copy-reply-threaded 2147483647
    }
}

# A 64KiB and a 1MiB RAW value with a non-uniform, position-sensitive pattern so
# any truncation, reordering, or byte corruption is detected on comparison.
proc mkbig {nbytes} {
    set unit "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!@#$"
    set s [string repeat $unit [expr {$nbytes / [string length $unit] + 1}]]
    return [string range $s 0 [expr {$nbytes - 1}]]
}

start_server {tags {"reply-offload"} overrides {io-threads 3 enable-debug-command yes save ""}} {
    assert_equal {io-threads 3} [r config get io-threads]

    # Run the whole suite once per gate mode: static (fixed size gates) and
    # adaptive (pressure-driven floor, pinned via DEBUG COPY-AVOID). Wire
    # correctness and offload engagement must hold identically in both.
    foreach ::ca_mode {static adaptive} {
    # -------- M1(a): wire correctness of large GET via the ref path --------
    test {Reply offload: large GET is byte-exact via ref path (64KB, 1MB)} {
        reply_offload_on
        foreach nbytes {65536 1048576} {
            set v [mkbig $nbytes]
            r set k $v
            assert_equal raw [r object encoding k]
            set before [s reply_copy_avoided]
            set got [r get k]
            assert_equal $nbytes [string length $got]
            assert_equal $v $got
            # The value reply rode the copy-avoidance path.
            assert_morethan [s reply_copy_avoided] $before
        }
    }

    test {Reply offload: identical result with ref path off (control)} {
        reply_offload_off
        set v [mkbig 65536]
        r set k $v
        set before [s reply_copy_avoided]
        assert_equal $v [r get k]
        # Nothing offloaded when the gate is closed.
        assert_equal $before [s reply_copy_avoided]
        reply_offload_on
    }

    test {Reply offload: large GET byte-exact when forced onto reply list} {
        reply_offload_on
        r debug client-enforce-reply-list 1
        set v [mkbig 262144]
        r set k $v
        assert_equal $v [r get k]
        r debug client-enforce-reply-list 0
    }

    # -------- M1(b): pipelined mixed replies keep exact order --------
    test {Reply offload: pipelined mix of big/small/set/error keeps order} {
        reply_offload_on
        set big [mkbig 131072]
        set small "hello-small"
        r set bk $big
        r set sk $small
        r set nk "not-an-int"
        r del ctr
        set rd [valkey_deferring_client]
        $rd get bk
        $rd get sk
        $rd set wk 123
        $rd incr ctr
        $rd incr nk
        $rd get missing
        $rd get bk
        $rd flush
        assert_equal $big     [$rd read]
        assert_equal $small   [$rd read]
        assert_equal OK       [$rd read]
        assert_equal 1        [$rd read]
        assert_error "*not an integer*" {$rd read}
        assert_equal {}       [$rd read]
        assert_equal $big     [$rd read]
        $rd close
    }

    # -------- M1(c): disconnect and CLIENT KILL with large write pending --------
    test {Reply offload: abrupt disconnect with large reply pending is clean} {
        reply_offload_on
        r set k [mkbig 1048576]
        for {set i 0} {$i < 20} {incr i} {
            set rd [valkey_deferring_client]
            $rd get k
            $rd flush
            # Close without reading: reply refs are outstanding at teardown.
            $rd close
        }
        # Server survived and keeps serving (refs released, no crash/leak).
        assert_equal PONG [r ping]
        assert_equal 1048576 [r strlen k]
    }

    test {Reply offload: CLIENT KILL while large reply is in flight is clean} {
        reply_offload_on
        r set k [mkbig 1048576]
        set rd [valkey_deferring_client]
        $rd client id
        set cid [$rd read]
        $rd get k
        $rd flush
        # Kill it from the main connection; async free must not free the value
        # object out from under an in-flight writev.
        r client kill id $cid
        catch {$rd read} e
        $rd close
        assert_equal PONG [r ping]
    }

    # -------- M1(d): partial-write fence resume across many writev calls --------
    test {Reply offload: multi-megabyte GET resumes correctly across partial writes} {
        reply_offload_on
        # 24MB dwarfs the socket buffer and NET_MAX_WRITES_PER_EVENT, forcing
        # writevToClient to return partial and resume via io_last_written.
        set v [mkbig 25165824]
        r set big $v
        set got [r get big]
        assert_equal [string length $v] [string length $got]
        assert_equal $v $got
    }

    # -------- M1(e): COB limit accounts true wire size, not the 16-byte ref --------
    test {Reply offload: output-buffer-limit trips on ref accounting (io_tracked_reply_len)} {
        reply_offload_on
        set orig [lindex [r config get client-output-buffer-limit] 1]
        # 1MB hard limit for normal clients.
        r config set client-output-buffer-limit {normal 1048576 0 0}
        r set k [mkbig 8388608] ;# 8MB value, ref proxy is only 16 bytes
        set rd [valkey_deferring_client]
        $rd client id
        set cid [$rd read]
        assert_match "*id=$cid *" [r client list]
        $rd get k
        $rd flush
        # If the limiter counted only the 16-byte ref it would never trip;
        # io_tracked_reply_len exposes the real 8MB backlog, so the client is closed.
        wait_for_condition 100 100 {
            ![string match "*id=$cid *" [r client list]]
        } else {
            fail "client with oversized offloaded reply was not closed by COB hard limit"
        }
        catch {$rd close}
        r config set client-output-buffer-limit $orig
        assert_equal PONG [r ping]
    }

    # -------- M2: addReply() raw arm routes through RAW_STR_REF (no framing) ----
    test {Reply offload: DEBUG REPLY-RAW value is byte-exact via raw ref arm} {
        reply_offload_on
        # Small embstr value: not RAW, so it stays inline (correctness only).
        r set k [mkbig 100]
        assert_equal [mkbig 100] [r debug reply-raw k]
        # Large RAW values ride the raw-ref arm (no bulk framing) and offload.
        foreach nbytes {65536 1048576} {
            set v [mkbig $nbytes]
            r set k $v
            assert_equal raw [r object encoding k]
            set before [s reply_copy_avoided]
            set got [r debug reply-raw k]
            assert_equal $nbytes [string length $got]
            assert_equal $v $got
            assert_morethan [s reply_copy_avoided] $before
        }
    }

    test {Reply offload: raw ref arm matches inline path when gate closed} {
        reply_offload_off
        set v [mkbig 65536]
        r set k $v
        assert_equal $v [r debug reply-raw k]
        reply_offload_on
    }

    # -------- M3: aggregate elements ride the ref path with inline framing ------
    test {Reply offload: MGET large elements byte-exact and ordered} {
        reply_offload_on
        set a [mkbig 100000]
        set b "tiny"
        set c [mkbig 300000]
        r mset ma $a mb $b mc $c
        set before [s reply_copy_avoided]
        set res [r mget ma mb mc missing]
        assert_equal 4 [llength $res]
        assert_equal $a [lindex $res 0]
        assert_equal $b [lindex $res 1]
        assert_equal $c [lindex $res 2]
        assert_equal {} [lindex $res 3]
        # Large elements offloaded; the framing/length prefixes stay inline.
        assert_morethan [s reply_copy_avoided] $before
    }

    test {Reply offload: LRANGE large elements byte-exact and ordered} {
        reply_offload_on
        r del mylist
        set e0 [mkbig 80000]
        set e1 "mid"
        set e2 [mkbig 200000]
        r rpush mylist $e0 $e1 $e2
        set res [r lrange mylist 0 -1]
        assert_equal 3 [llength $res]
        assert_equal $e0 [lindex $res 0]
        assert_equal $e1 [lindex $res 1]
        assert_equal $e2 [lindex $res 2]
    }

    test {Reply offload: HGETALL large values byte-exact} {
        reply_offload_on
        r del myhash
        set v1 [mkbig 90000]
        set v2 [mkbig 150000]
        r hset myhash f1 $v1 f2 $v2
        set res [r hgetall myhash]
        assert_equal $v1 [dict get $res f1]
        assert_equal $v2 [dict get $res f2]
    }
    } ;# foreach ::ca_mode
}
