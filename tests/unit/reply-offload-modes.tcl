# Tests for the adaptive reply copy-avoidance gate (avoid-copy-reply-mode).
#
# The gate has three modes:
#   static   - fixed size gates (legacy behavior, default).
#   adaptive - a main-thread busy-pct EMA drives a hysteretic size floor:
#              engaged (main is the constraint) -> floor drops toward 1024 so
#              large values offload; released (main idle) -> high floor so
#              copying stays inline and loopback rps is preserved.
#   off      - never copy-avoid; always serialize inline.
#
# Wire correctness must hold in every mode and across runtime mode flips. The
# adaptive hysteresis is pinned deterministically for tests via
#   DEBUG COPY-AVOID <ENGAGE|RELEASE> [floor]
# so no load generation or benchmarking is needed here.

proc mkbig {nbytes} {
    set unit "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!@#$"
    set s [string repeat $unit [expr {$nbytes / [string length $unit] + 1}]]
    return [string range $s 0 [expr {$nbytes - 1}]]
}

start_server {tags {"reply-offload"} overrides {io-threads 6 enable-debug-command yes save ""}} {
    assert_equal {io-threads 6} [r config get io-threads]

    # -------- off mode: correctness holds, nothing is ever offloaded --------
    test {avoid-copy-reply-mode off: large GET byte-exact and zero offload} {
        r config set avoid-copy-reply-mode off
        foreach nbytes {65536 1048576} {
            set v [mkbig $nbytes]
            r set k $v
            set before [s reply_copy_avoided]
            set got [r get k]
            assert_equal $v $got
            # off mode must never take the ref path, even at io-threads 6.
            assert_equal $before [s reply_copy_avoided]
        }
    }

    # -------- adaptive: engaged floor offloads, released floor does not ------
    test {avoid-copy-reply-mode adaptive: engaged low floor offloads large GET} {
        r config set avoid-copy-reply-mode adaptive
        r debug copy-avoid engage 1024
        set v [mkbig 65536]
        r set k $v
        set before [s reply_copy_avoided]
        assert_equal $v [r get k]
        assert_morethan [s reply_copy_avoided] $before
    }

    test {avoid-copy-reply-mode adaptive: released high floor keeps copy inline} {
        r config set avoid-copy-reply-mode adaptive
        r debug copy-avoid release 2147483647
        set v [mkbig 65536]
        r set k $v
        set before [s reply_copy_avoided]
        assert_equal $v [r get k]
        # Floor above the value size: value is copied inline, not offloaded.
        assert_equal $before [s reply_copy_avoided]
    }

    # -------- adaptive floor boundary: >= floor offloads, < floor inline -----
    test {avoid-copy-reply-mode adaptive: floor is the exact offload boundary} {
        r config set avoid-copy-reply-mode adaptive
        r debug copy-avoid engage 32768
        # Below floor: byte-exact but inline.
        set small [mkbig 16384]
        r set sk $small
        set before [s reply_copy_avoided]
        assert_equal $small [r get sk]
        assert_equal $before [s reply_copy_avoided]
        # At/above floor: byte-exact and offloaded.
        set big [mkbig 65536]
        r set bk $big
        set before [s reply_copy_avoided]
        assert_equal $big [r get bk]
        assert_morethan [s reply_copy_avoided] $before
    }

    # -------- INFO surface for the final bench to verify --------------------
    test {INFO exposes copy_avoid_mode, current_floor, main_thread_busy_pct} {
        r config set avoid-copy-reply-mode adaptive
        r debug copy-avoid engage 4096
        assert_equal "adaptive" [s copy_avoid_mode]
        assert_equal 4096 [s copy_avoid_current_floor]
        # busy pct is present and in range.
        set pct [s main_thread_busy_pct]
        assert {$pct >= 0 && $pct <= 100}
        r config set avoid-copy-reply-mode off
        assert_equal "off" [s copy_avoid_mode]
        r config set avoid-copy-reply-mode static
        assert_equal "static" [s copy_avoid_mode]
    }

    # -------- runtime mode flip under concurrent large-GET load -------------
    test {Mode flips at runtime under load stay byte-exact on the wire} {
        set v [mkbig 200000]
        r set bk $v
        r config set avoid-copy-reply-mode static

        # Several deferring clients hammering large GETs while we flip the mode
        # (and, in adaptive, flip engagement) underneath the in-flight traffic.
        set clients {}
        for {set i 0} {$i < 4} {incr i} {
            lappend clients [valkey_deferring_client]
        }

        set modes {static adaptive off adaptive static off}
        for {set round 0} {$round < 24} {incr round} {
            set m [lindex $modes [expr {$round % [llength $modes]}]]
            r config set avoid-copy-reply-mode $m
            if {$m eq "adaptive"} {
                if {$round % 2 == 0} {
                    r debug copy-avoid engage 1024
                } else {
                    r debug copy-avoid release 2147483647
                }
            }
            # Queue a GET on every client, then drain and verify byte-exact.
            foreach rd $clients { $rd get bk }
            foreach rd $clients { $rd flush }
            foreach rd $clients { assert_equal $v [$rd read] }
        }

        foreach rd $clients { $rd close }
        r config set avoid-copy-reply-mode static
        assert_equal PONG [r ping]
    }
}
