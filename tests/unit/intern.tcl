start_server {tags {"intern"}} {
    proc intern_stat {field} {
        set info [r info memory]
        if {[regexp "\r\n$field:(\[0-9\]+)" $info -> v]} {return $v}
        return 0
    }

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
    proc intern_stat {field} {
        set info [r info memory]
        if {[regexp "\r\n$field:(\[0-9\]+)" $info -> v]} {return $v}
        return 0
    }

    test {Values parsed by IO threads are shared} {
        r flushall
        set v [string repeat w 300]
        for {set i 0} {$i < 3000} {incr i} {r set k$i $v}
        assert_morethan [s io_threaded_reads_processed] 1000
        assert_morethan [intern_stat intern_refs] 2500
        for {set i 0} {$i < 3000} {incr i 97} {assert_equal $v [r get k$i]}
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
