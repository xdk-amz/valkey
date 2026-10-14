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
        for {set i 0} {$i < 100} {incr i} {r set k$i $v}
        assert_lessthan [r memory usage k50] 600
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
