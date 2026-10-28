proc intern_stat {field {client r}} {
    set info [$client info memory]
    if {[regexp "\r\n$field:(\[0-9\]+)" $info -> v]} {return $v}
    return 0
}

proc wait_intern_released {{client r}} {
    wait_for_condition 50 100 {
        [intern_stat intern_strings $client] == 0 && [intern_stat intern_refs $client] == 0 &&
        [intern_stat intern_singletons $client] == 0
    } else {
        fail "intern references not released: [$client info memory]"
    }
}

# Writes n values of `size` bytes as string values, hash values or set members.
# With dup set, value i equals value i+1 for even i < n/2; the rest are unique.
proc intern_fill_pairs {client holder n size {dup 1}} {
    set pad [string repeat p $size]
    for {set i 0} {$i < $n} {incr i} {
        if {$dup && $i < $n / 2} {set id "d[expr {$i / 2}]"} else {set id "u$i"}
        set v [string range "$id$pad" 0 [expr {$size - 1}]]
        switch $holder {
            string {$client set k$i $v}
            hash {$client hset h[expr {$i % 64}] f$i $v}
            set {$client sadd s[expr {$i % 64}] $v}
        }
    }
}

start_server {tags {"intern"}} {

    test {Identical string values are stored once} {
        r flushall
        set v [string repeat x 300]
        for {set i 0} {$i < 2000} {incr i} {r set k$i $v}
        assert_equal $v [r get k1999]
        assert_equal raw [r object encoding k0]
        assert_lessthan [intern_stat intern_strings] 3
        assert_morethan [intern_stat intern_refs] 1000
    }

    test {Modifying a shared string value does not change other keys} {
        r flushall
        set v [string repeat y 300]
        for {set i 0} {$i < 500} {incr i} {r set k$i $v}
        r append k1 Z
        r setrange k2 0 AB
        r setbit k3 0 1
    }

    test {Shared values survive other keys being changed} {
        set v [string repeat y 300]
        assert_equal "${v}Z" [r get k1]
        assert_equal "AB[string range $v 2 end]" [r get k2]
        assert_equal $v [r get k5]
        assert_equal $v [r get k499]
    }

    test {ZUNIONSTORE over sets with shared members} {
        r flushall
        r config set set-max-listpack-entries 0
        set m [string repeat m 40]
        for {set i 0} {$i < 200} {incr i} {r sadd s$i "$m$i" "$m common" "$m-[expr {$i % 3}]"}
        assert_morethan [intern_stat intern_refs] 100
        r zunionstore z1 3 s1 s2 s3
        r zinterstore z2 2 s1 s2
        assert_equal 1 [r zcard z2]
        assert_equal 3 [r zscore z1 "$m common"]
        r zadd zz 1 "$m common"
        assert_equal 1 [r zscore zz "$m common"]
        assert_equal {1} [r zmscore zz "$m common"]
        r zrem zz "$m common"
        assert_equal 0 [r zcard zz]
        r config set set-max-listpack-entries 128
    }

    test {Shared references are released on delete and flush} {
        r flushall
        set v [string repeat q 200]
        for {set i 0} {$i < 1000} {incr i} {r set k$i $v; r hset h$i f $v; r sadd s$i "$v$i" "$v"}
        r config set lazyfree-lazy-user-del yes
        for {set i 0} {$i < 1000} {incr i} {r unlink k$i h$i}
        r flushall async
        wait_for_condition 50 100 {
            [intern_stat intern_strings] == 0 && [intern_stat intern_refs] == 0
        } else {
            fail "intern references not released"
        }
    }

    test {DEBUG RELOAD keeps shared values intact} {
        r flushall
        set v [string repeat r 500]
        for {set i 0} {$i < 1000} {incr i} {r set k$i $v; r hset h$i a $v b x$i}
        set d [debug_digest]
        r debug reload
        assert_equal $d [debug_digest]
        assert_morethan [intern_stat intern_refs] 1000
    } {} {needs:debug}

    foreach holder {string hash set} {
        test "DEBUG RELOAD shares every repeated $holder value" {
            set n 8000
            r flushall sync
            intern_fill_pairs r $holder $n 300 0
            r debug reload
            set distinct [s used_memory]
            r flushall sync
            # Half the keys hold a value that exactly one other key holds.
            intern_fill_pairs r $holder $n 300
            r debug reload
            wait_for_condition 50 100 {
                [intern_stat intern_strings] == $n / 4 && [intern_stat intern_refs] == $n / 2
            } else {
                fail "reload did not share every pair: [r info memory]"
            }
            assert_lessthan [s used_memory] [expr {$distinct - $n / 4 * 300}]
        } {} {needs:debug}
    }

    test {DEBUG RELOAD shares values bigger as a shared copy from the second holder} {
        r flushall sync
        # A shared copy of 250 bytes needs the next allocation size class.
        for {set g 0} {$g < 1000} {incr g} {
            set v [string range "[string repeat w 250]$g" end-249 end]
            for {set j 0} {$j < 3} {incr j} {r set k$g:$j $v}
        }
        # A synchronous flush leaves no old copy for the load to share.
        r config set replica-lazy-flush no
        r debug reload
        r config set replica-lazy-flush yes
        assert_equal 1000 [intern_stat intern_strings]
        assert_range [intern_stat intern_refs] 2000 2020
    } {} {needs:debug}

    test {intern_saved_bytes matches the memory saved} {
        set n 20000
        r flushall sync
        intern_fill_pairs r string $n 1024 0
        r debug reload
        set distinct [s used_memory]
        r flushall sync
        intern_fill_pairs r string $n 1024
        r debug reload
        wait_for_condition 50 100 {
            [intern_stat intern_refs] == $n / 2 && [s lazyfree_pending_objects] == 0
        } else {
            fail "references did not settle: [r info memory]"
        }
        set saved [intern_stat intern_saved_bytes]
        assert_range [expr {$distinct - [s used_memory]}] [expr {$saved * 0.97}] [expr {$saved * 1.03}]
    } {} {needs:debug}

    test {intern_saved_bytes counts whole allocations} {
        set n 4000
        set pad [string repeat q 1024]
        r flushall sync
        for {set i 0} {$i < $n} {incr i} {r set k$i [string range "$i$pad" 0 1023]}
        set distinct [s used_memory]
        r flushall sync
        for {set i 0} {$i < $n} {incr i} {r set k$i $pad}
        set saved [intern_stat intern_saved_bytes]
        assert_morethan [intern_stat intern_refs] [expr {$n * 9 / 10}]
        assert_range [expr {$distinct - [s used_memory]}] [expr {$saved * 0.95}] [expr {$saved * 1.05}]
    }

    test {MEMORY USAGE charges a share of a shared value} {
        r flushall
        set v [string repeat u 1000]
        for {set i 0} {$i < 1000} {incr i} {r set k$i $v}
        assert_lessthan [r memory usage k999] 600
    }

    test {valkey-check-rdb reads an RDB written from shared values} {
        r flushall
        # One value compresses on save and one does not, so both string load paths run.
        set z [string repeat c 300]
        set p [randstring 300 300 alpha]
        for {set i 0} {$i < 500} {incr i} {
            r set z$i $z
            r set p$i $p
            r hset h$i a $p
            r sadd s$i $p "$p$i"
        }
        assert_morethan [intern_stat intern_refs] 1000
        r save
        set dump [file join [lindex [r config get dir] 1] dump.rdb]
        catch {exec $::VALKEY_CHECK_RDB_BIN $dump} result
        assert_match {*RDB looks OK*} $result
    } {} {external:skip}

    test {Shared references are released when keys and hash fields expire} {
        r flushall
        set v [string repeat e 300]
        for {set i 0} {$i < 1000} {incr i} {
            r set k$i $v px 100
            r hsetex h$i px 100 fields 1 f $v
        }
        assert_morethan [intern_stat intern_refs] 1500
        wait_for_condition 100 100 {
            [r dbsize] == 0
        } else {
            fail "keys did not expire"
        }
        wait_intern_released
    }

    test {Shared references are released on lazy expiry} {
        r flushall
        r debug set-active-expire 0
        set v [string repeat l 300]
        for {set i 0} {$i < 1000} {incr i} {r set k$i $v px 1}
        assert_morethan [intern_stat intern_refs] 900
        after 20
        for {set i 0} {$i < 1000} {incr i} {assert_equal {} [r get k$i]}
        r debug set-active-expire 1
        wait_intern_released
    } {} {needs:debug}

    test {Shared references are released when keys are evicted} {
        r flushall
        set v [string repeat v 400]
        for {set i 0} {$i < 2000} {incr i} {r set k$i $v}
        assert_morethan [intern_stat intern_refs] 1900
        set evicted [s evicted_keys]
        r config set maxmemory-policy allkeys-random
        r config set maxmemory [expr {[s used_memory] + 200000}]
        for {set i 0} {$i < 2000} {incr i} {r set p$i "$i[string repeat p 400]"}
        r config set maxmemory 0
        r config set maxmemory-policy noeviction
        assert_morethan [s evicted_keys] $evicted
        set kept 0
        for {set i 0} {$i < 2000} {incr i} {
            set got [r get k$i]
            if {$got ne {}} {
                assert_equal $v $got
                incr kept
            }
        }
        assert_lessthan $kept 2000
        r flushall
        wait_intern_released
    }

    test {Shared values survive an AOF reload} {
        r flushall
        set v [string repeat a 300]
        for {set i 0} {$i < 500} {incr i} {
            r set k$i $v
            r hset h$i f $v
            r sadd s$i "$v$i" $v
        }
        r config set appendonly yes
        waitForBgrewriteaof r
        r config set aof-use-rdb-preamble no
        r bgrewriteaof
        waitForBgrewriteaof r
        # Commands appended after the rewrite are replayed too.
        for {set i 500} {$i < 1000} {incr i} {r set k$i $v}
        set d [debug_digest]
        r debug loadaof
        assert_equal $d [debug_digest]
        assert_morethan [intern_stat intern_refs] 1500
        assert_equal $v [r hget h7 f]
        r config set aof-use-rdb-preamble yes
        r bgrewriteaof
        waitForBgrewriteaof r
        r debug loadaof
        assert_equal $d [debug_digest]
        assert_morethan [intern_stat intern_refs] 1500
        r config set appendonly no
        r flushall
        wait_intern_released
    } {} {needs:debug external:skip}
}

start_server {tags {"intern external:skip"}} {
    start_server {} {
        set src [srv -1 client]
        set dst [srv 0 client]
        set dst_host [srv 0 host]
        set dst_port [srv 0 port]

        test {MIGRATE moves shared values and the target shares them} {
            set v [string repeat g 300]
            set keys {}
            for {set i 0} {$i < 1000} {incr i} {
                $src set k$i $v
                lappend keys k$i
            }
            assert_morethan [intern_stat intern_refs $src] 900
            assert_equal OK [$src migrate $dst_host $dst_port "" 9 5000 keys {*}$keys]
            assert_equal 0 [$src dbsize]
            wait_intern_released $src
            assert_equal 1000 [$dst dbsize]
            assert_morethan [intern_stat intern_refs $dst] 900
            assert_equal $v [$dst get k0]
            assert_equal $v [$dst get k999]
        }
    }
}

foreach load {disabled swapdb} {
    start_server {tags {"intern repl external:skip"}} {
        set replica [srv 0 client]
        start_server {} {
            set master [srv 0 client]
            set master_host [srv 0 host]
            set master_port [srv 0 port]

            test "Replica shares values from a full and a partial sync (repl-diskless-load $load)" {
                set v [string repeat s 300]
                for {set i 0} {$i < 1000} {incr i} {
                    $master set k$i $v
                    $master hset h$i f $v
                    $master sadd s$i "$v$i" $v
                }
                $replica config set repl-diskless-load $load
                $replica replicaof $master_host $master_port
                wait_for_condition 100 100 {
                    [s -1 master_link_status] eq "up"
                } else {
                    fail "Replica did not sync"
                }
                wait_for_ofs_sync $master $replica
                assert_equal [$master debug digest] [$replica debug digest]
                assert_morethan [intern_stat intern_refs $replica] 2500

                # Writes from the replication stream are shared too.
                set w [string repeat t 300]
                for {set i 0} {$i < 1000} {incr i} {$master set k$i $w}
                wait_for_ofs_sync $master $replica
                assert_equal $w [$replica get k999]
                assert_equal [$master debug digest] [$replica debug digest]
                assert_morethan [intern_stat intern_refs $replica] 2500

                set partial [s 0 sync_partial_ok]
                $master client kill type replica
                for {set i 0} {$i < 1000} {incr i} {$master set n$i $w}
                wait_for_condition 100 100 {
                    [s 0 sync_partial_ok] > $partial && [s -1 master_link_status] eq "up"
                } else {
                    fail "Replica did not partially resync"
                }
                wait_for_ofs_sync $master $replica
                assert_equal [$master debug digest] [$replica debug digest]
                assert_morethan [intern_stat intern_refs $replica] 3500

                $master flushall
                wait_for_ofs_sync $master $replica
                wait_intern_released $replica
                wait_intern_released $master
            } {} {needs:debug}
        }
    }
}

start_server {tags {"intern repl external:skip"}} {
    set replica [srv 0 client]
    start_server {} {
        set master [srv 0 client]
        set master_host [srv 0 host]
        set master_port [srv 0 port]

        test {RESTORE served during an async load does not disturb the load} {
            $master config set repl-diskless-sync yes
            $master config set repl-diskless-sync-delay 0
            $master config set save ""
            $master config set rdbcompression no
            $replica config set repl-diskless-load swapdb
            $replica config set dual-channel-replication-enabled no
            $replica config set replica-read-only no
            $replica config set save ""
            $replica replicaof $master_host $master_port
            wait_for_condition 100 100 {
                [s -1 master_link_status] eq "up"
            } else {
                fail "Replica did not sync"
            }

            # Each value read spans several event-processing chunks on the replica.
            set v [randstring 3000 3000 alpha]
            for {set i 0} {$i < 4000} {incr i} {$master set k$i $v}
            $master set probe [randstring 2500 2500 alpha]
            set payload [$master dump probe]
            $master del probe
            wait_for_ofs_sync $master $replica

            $replica config set loading-process-events-interval-bytes 1024
            $replica config set key-load-delay 100
            set sync_full [s 0 sync_full]
            # Overflow the backlog so the reconnect is a full sync with the same replid.
            $master multi
            $master client kill type replica
            $master config set repl-backlog-size 16384
            for {set i 0} {$i < 10} {incr i} {$master set fill$i [string repeat A 16384]}
            $master exec
            wait_for_condition 100 100 {
                [s 0 sync_full] > $sync_full
            } else {
                fail "Replica did not start a full sync"
            }
            wait_for_condition 100 50 {
                [s -1 async_loading] eq 1
            } else {
                fail "Replica did not start async loading"
            }

            set restored 0
            while {[s -1 async_loading] eq 1} {
                $replica restore r$restored 0 $payload
                incr restored
            }
            assert_morethan $restored 10

            $replica config set key-load-delay 0
            wait_for_condition 100 100 {
                [s -1 master_link_status] eq "up"
            } else {
                fail "Replica did not finish the sync"
            }
            wait_for_ofs_sync $master $replica
            # A RESTORE that lands after the swap stays on the replica.
            foreach k [$replica keys r*] {$replica del $k}
            assert_equal [$master debug digest] [$replica debug digest]
            assert_equal $v [$replica get k3999]
        } {} {needs:debug}
    }
}

start_server {tags {"intern external:skip"} overrides {io-threads 4 io-threads-always-active yes}} {
    test {Values parsed by IO threads are shared} {
        r flushall
        set v [string repeat w 300]
        for {set i 0} {$i < 3000} {incr i} {r set k$i $v}
        assert_morethan [s io_threaded_reads_processed] 1000
        assert_morethan [intern_stat intern_refs] 2500
        for {set i 0} {$i < 3000} {incr i 97} {assert_equal $v [r get k$i]}
    }

    test {Values parsed by IO threads are shared when they differ only in the middle} {
        r flushall
        # Same length and same first and last bytes, so they map to one hot slot.
        set vals {}
        for {set j 0} {$j < 50} {incr j} {
            lappend vals "[string repeat a 100][format %03d $j][string repeat z 100]"
        }
        for {set pass 0} {$pass < 2} {incr pass} {
            for {set i 0} {$i < 3000} {incr i} {r set k$i [lindex $vals [expr {($i * 7 + $pass) % 50}]]}
        }
        assert_morethan [s io_threaded_reads_processed] 1000
        assert_morethan [intern_stat intern_refs] 2900
        for {set i 0} {$i < 3000} {incr i 97} {assert_equal [lindex $vals [expr {($i * 7 + 1) % 50}]] [r get k$i]}
    }

    test {Values shared by IO threads are released on overwrite and flush} {
        set w [string repeat z 300]
        for {set i 0} {$i < 3000} {incr i} {r set k$i $w}
        for {set i 0} {$i < 3000} {incr i 89} {assert_equal $w [r get k$i]}
        r flushall
        wait_for_condition 50 100 {
            [intern_stat intern_strings] == 0 && [intern_stat intern_refs] == 0
        } else {
            fail "intern references not released"
        }
    }
}

# The setup relies on every write being offered, which holds only for commands
# parsed on the main thread.
start_server {tags {"intern"} overrides {io-threads 1}} {
    test {An async flush of many distinct shared strings releases all of them} {
        # More distinct shared strings than one drain slice applies.
        for {set i 0} {$i < 3000} {incr i} {
            set v "[string repeat p 150]$i"
            r mset a$i $v b$i $v c$i $v d$i $v
        }
        assert_morethan [intern_stat intern_strings] 1024
        r flushall async
        assert_equal PONG [r ping]
        wait_for_condition 50 100 {
            [intern_stat intern_strings] == 0 && [intern_stat intern_refs] == 0
        } else {
            fail "intern references not released"
        }
    }
}

start_server {tags {"intern repl external:skip"}} {
    set replica [srv 0 client]
    start_server {} {
        set master [srv 0 client]
        set master_host [srv 0 host]
        set master_port [srv 0 port]

        foreach load {disabled swapdb} {
            test "Full sync shares every repeated value (repl-diskless-load $load)" {
                $master flushall
                $master debug change-repl-id
                intern_fill_pairs $master string 8000 300
                set full [s 0 sync_full]
                $replica config set repl-diskless-load $load
                $replica replicaof $master_host $master_port
                wait_for_condition 100 100 {
                    [s 0 sync_full] > $full && [s -1 master_link_status] eq "up"
                } else {
                    fail "Replica did not full sync"
                }
                wait_for_ofs_sync $master $replica
                assert_equal [$master debug digest] [$replica debug digest]
                wait_for_condition 50 100 {
                    [intern_stat intern_strings $replica] == 2000 && [intern_stat intern_refs $replica] == 4000
                } else {
                    fail "replica did not share every pair: [$replica info memory]"
                }
                assert_equal OK [$replica replicaof no one]
            } {} {needs:debug}
        }
    }
}
