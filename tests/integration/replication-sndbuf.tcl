# repl-sndbuf-bytes sets SO_SNDBUF on each replica link when it is established.
# The kernel clamps the request to net.core.wmem_max and stops autotuning the
# socket, so the server skips a request that the clamp would leave below the
# autotune ceiling (the last field of net.ipv4.tcp_wmem). Which branch a
# request takes depends on the host, so the tests read the same two limits
# the server reads and assert the branch they imply. Both branches run in CI
# on a stock Linux host (wmem_max 212992 < tcp_wmem max 4194304: skip) and on a
# tuned host (wmem_max >= tcp_wmem max / 2: apply). The default (0) never
# touches the socket.

proc sndbuf_host_limits {} {
    if {[catch {
        set f [open /proc/sys/net/core/wmem_max]; set wmem_max [string trim [read $f]]; close $f
        set f [open /proc/sys/net/ipv4/tcp_wmem]; set autotune_max [lindex [read $f] 2]; close $f
    }]} {
        return {}
    }
    return [list $wmem_max $autotune_max]
}

# What the server does with a request on this host: skip, clamp or apply.
proc sndbuf_outcome {requested} {
    set limits [sndbuf_host_limits]
    if {$limits eq {}} { return apply }
    lassign $limits wmem_max autotune_max
    set clamped [expr {min($requested, $wmem_max)}]
    if {2 * $clamped < $autotune_max} { return skip }
    if {$clamped < $requested} { return clamp }
    return apply
}

# Lines after 'from_line' of the server's stdout that contain 'needle' (plain substring).
proc sndbuf_log_count_since {srv_idx needle from_line} {
    incr from_line
    set n 0
    foreach ln [split [exec tail -n +$from_line < [srv $srv_idx stdout]] "\n"] {
        if {[string first $needle $ln] >= 0} { incr n }
    }
    return $n
}

start_server {tags {"repl network external:skip cluster:skip"} overrides {save ""}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    start_server {overrides {save ""}} {
        set replica [srv 0 client]

        test {repl-sndbuf-bytes defaults to 0 and does not touch the replica link} {
            assert_equal 0 [lindex [$primary config get repl-sndbuf-bytes] 1]
            set loglines [count_log_lines -1]
            $replica replicaof $primary_host $primary_port
            wait_for_sync $replica
            # Give the server a moment to log anything it would have.
            $primary set after-attach 1
            wait_for_ofs_sync $primary $replica
            assert_equal 0 [sndbuf_log_count_since -1 "SO_SNDBUF on replica" $loglines]
        }

        test {An explicit request is applied, clamped or skipped according to the host limits} {
            $primary config set repl-sndbuf-bytes 8mb
            set loglines [count_log_lines -1]
            $replica replicaof no one
            $replica replicaof $primary_host $primary_port
            wait_for_sync $replica
            switch [sndbuf_outcome 8388608] {
                skip {
                    # A small wmem_max would leave the socket below autotuning: the
                    # link keeps the kernel default and the log names the limit.
                    wait_for_log_messages -1 {"*Not setting SO_SNDBUF on replica * link: net.core.wmem_max * keeping the kernel default*"} $loglines 100 50
                    assert_equal 0 [sndbuf_log_count_since -1 " link set to " $loglines]
                }
                clamp {
                    wait_for_log_messages -1 {"*SO_SNDBUF on replica * link set to * (requested 8388608, clamped by net.core.wmem_max*"} $loglines 100 50
                }
                apply {
                    wait_for_log_messages -1 {"*SO_SNDBUF on replica * link set to 8388608 (kernel reports 16777216)*"} $loglines 100 50
                }
            }
        }

        test {Setting repl-sndbuf-bytes back to 0 leaves the next replica link untouched} {
            $primary config set repl-sndbuf-bytes 0
            set loglines [count_log_lines -1]
            $replica replicaof no one
            $replica replicaof $primary_host $primary_port
            wait_for_sync $replica
            # Give the server a moment to log anything it would have.
            $primary set after-reattach 1
            wait_for_ofs_sync $primary $replica
            assert_equal 0 [sndbuf_log_count_since -1 "SO_SNDBUF on replica" $loglines]
        }

        test {repl-sndbuf-bytes applies to the next replica link after CONFIG SET, not the current one} {
            set loglines [count_log_lines -1]
            $primary config set repl-sndbuf-bytes 4mb
            assert_equal 4194304 [lindex [$primary config get repl-sndbuf-bytes] 1]

            # Already-attached links are left alone; only a new link gets the setting.
            $primary set before-reattach 1
            wait_for_ofs_sync $primary $replica
            assert_equal 0 [sndbuf_log_count_since -1 "SO_SNDBUF on replica" $loglines]

            $replica replicaof no one
            $replica replicaof $primary_host $primary_port
            wait_for_sync $replica
            switch [sndbuf_outcome 4194304] {
                skip { wait_for_log_messages -1 {"*Not setting SO_SNDBUF on replica * link: * clamp the 4194304 request*"} $loglines 100 50 }
                clamp { wait_for_log_messages -1 {"*SO_SNDBUF on replica * link set to * (requested 4194304, clamped*"} $loglines 100 50 }
                apply { wait_for_log_messages -1 {"*SO_SNDBUF on replica * link set to 4194304 (kernel reports 8388608)*"} $loglines 100 50 }
            }
        }

        test {A request the host can honor is applied to the socket} {
            # Below every default ceiling. The guard compares against autotuning, so
            # on a host where even this is below tcp_wmem max it is skipped, and the
            # test only proves the skip is logged; on other hosts it proves the set.
            set loglines [count_log_lines -1]
            $primary config set repl-sndbuf-bytes 64kb
            $replica replicaof no one
            $replica replicaof $primary_host $primary_port
            wait_for_sync $replica
            if {[sndbuf_outcome 65536] eq "skip"} {
                wait_for_log_messages -1 {"*Not setting SO_SNDBUF on replica * link: * clamp the 65536 request*"} $loglines 100 50
            } else {
                wait_for_log_messages -1 {"*SO_SNDBUF on replica * link set to 65536 (kernel reports 131072)*"} $loglines 100 50
            }
        }

        test {Replication works with a per-socket send buffer} {
            set value [string repeat x 4096]
            set rd [valkey_deferring_client -1]
            for {set i 0} {$i < 2000} {incr i} {
                $rd set sndbuf:$i $value
            }
            for {set i 0} {$i < 2000} {incr i} {
                assert_equal OK [$rd read]
            }
            $rd close
            wait_for_ofs_sync $primary $replica
            assert_equal $value [$replica get sndbuf:1999]
            assert_equal [$primary dbsize] [$replica dbsize]
        }

        test {repl-sndbuf-bytes rejects out-of-range values} {
            catch {$primary config set repl-sndbuf-bytes -1} err
            assert_match {*ERR*} $err
            catch {$primary config set repl-sndbuf-bytes 3gb} err
            assert_match {*ERR*} $err
            assert_equal 65536 [lindex [$primary config get repl-sndbuf-bytes] 1]
        }
    }
}
