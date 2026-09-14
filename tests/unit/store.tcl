start_server {tags {"store"}} {
    test {STORE + LPOP persists and replays without re-executing} {
        r del jobs
        r rpush jobs job-17 job-18
        assert_equal "OK" [r store req:1 px 60000]
        assert_equal "job-17" [r lpop jobs]
        assert_equal "REPLAY" [r store req:1 px 60000]
        assert_equal "job-17" [r lpop jobs]
        assert_equal 1 [r llen jobs]
    }

    test {STORE mismatch returns STOREMISMATCH and executes nothing} {
        assert_equal "REPLAY" [r store req:1 px 60000]
        assert_error "*STOREMISMATCH*" {r lpop other}
    }

    test {STORE + INCRBY is at-most-once (no double debit)} {
        r set balance 1700
        assert_equal "OK" [r store req:2 px 60000]
        assert_equal 1200 [r incrby balance -500]
        assert_equal "REPLAY" [r store req:2 px 60000]
        assert_equal 1200 [r incrby balance -500]
        assert_equal 1200 [r get balance]
    }

    test {STORE + BLPOP served by LPUSH replays the served reply} {
        r del jobs
        set rd [valkey_deferring_client]
        $rd store req:b px 60000
        assert_equal "OK" [$rd read]
        $rd blpop jobs 5
        wait_for_blocked_client
        r lpush jobs job-99
        assert_equal {jobs job-99} [$rd read]
        $rd close
        assert_equal "REPLAY" [r store req:b px 60000]
        assert_equal {jobs job-99} [r blpop jobs 5]
        assert_equal 0 [r llen jobs]
    }

    test {STORE + BLPOP disconnect while blocked stores nothing} {
        r del jobs2 req:d
        set rd [valkey_deferring_client]
        $rd store req:d px 60000
        assert_equal "OK" [$rd read]
        $rd blpop jobs2 10
        wait_for_blocked_client
        $rd close
        wait_for_condition 50 100 {
            [s blocked_clients] == 0
        } else {
            fail "client still blocked after disconnect"
        }
        assert_equal 0 [r exists req:d]
    }

    test {STORE + BLPOP timeout stores and replays the nil} {
        r del empty
        assert_equal "OK" [r store req:t px 60000]
        assert_equal {} [r blpop empty 1]
        assert_equal "REPLAY" [r store req:t px 60000]
        assert_equal {} [r blpop empty 1]
    }

    test {STORE + rejected command stores nothing} {
        r del req:r
        assert_equal "OK" [r store req:r px 60000]
        assert_error "*unknown command*" {r nosuchcmd x}
        assert_equal 0 [r exists req:r]
    }

    test {STORE inside MULTI stores and replays the EXEC array} {
        r del jobs
        r rpush jobs job-A
        r set balance 1200
        r multi
        r lpop jobs
        r get balance
        r store req:m px 60000
        assert_equal {job-A 1200} [r exec]
        r rpush jobs job-B
        r multi
        r lpop jobs
        r get balance
        r store req:m px 60000
        assert_equal {job-A 1200} [r exec]
        assert_equal 1 [r llen jobs]
    }

    test {STORE of a not-storable command errors, executes nothing} {
        r del req:s
        assert_equal "OK" [r store req:s px 60000]
        assert_error "*cannot be stored*" {r subscribe ch}
        r reset
        assert_equal 0 [r exists req:s]
    }

    test {stored blob carries VKSR header and a TTL} {
        r del req:h jobs
        r rpush jobs one
        assert_equal "OK" [r store req:h px 60000]
        r lpop jobs
        set blob [r get req:h]
        assert_equal "VKSR" [string range $blob 0 3]
        assert {[r pttl req:h] > 0}
    }

    test {STORE second-while-pending is an error and leaves target intact} {
        r del req:p1 req:p2 jobs
        r rpush jobs z
        assert_equal "OK" [r store req:p1 px 60000]
        assert_error "*already pending*" {r store req:p2 px 60000}
        assert_equal "z" [r lpop jobs]
        assert_equal 0 [r exists req:p2]
        assert_equal 1 [r exists req:p1]
    }

    test {STOREINVALID when the key holds a non-STORE string} {
        r del jobs req:bad
        r rpush jobs a
        r set req:bad "not a valid blob"
        assert_equal "REPLAY" [r store req:bad px 60000]
        assert_error "*STOREINVALID*" {r lpop jobs}
        assert_equal 1 [r llen jobs]
    }

    test {STOREPROTO when the stored reply used the other RESP version} {
        r del jobs req:proto
        r rpush jobs p1 p2
        # Store the reply under RESP3.
        r hello 3
        assert_equal "OK" [r store req:proto px 60000]
        r lpop jobs
        # Retry the identical command from a RESP2 connection: same argv, so the
        # fingerprint matches, but the stored RESP version does not.
        r hello 2
        assert_equal "REPLAY" [r store req:proto px 60000]
        assert_error "*STOREPROTO*" {r lpop jobs}
        assert_equal 1 [r llen jobs]
    }

    test {oversized reply stores a tombstone; retry gets STOREOMITTED} {
        r del jobs req:big
        set prev [lindex [r config get store-max-reply-bytes] 1]
        r config set store-max-reply-bytes 8
        # A 64-byte element exceeds the 8-byte cap, so only a tombstone is stored.
        r rpush jobs [string repeat x 64]
        assert_equal "OK" [r store req:big px 60000]
        assert_equal [string repeat x 64] [r lpop jobs]
        assert_equal 1 [r exists req:big]
        assert_equal "REPLAY" [r store req:big px 60000]
        assert_error "*STOREOMITTED*" {r lpop jobs}
        # The transaction DID run the first time: the element is gone and not replayed.
        assert_equal 0 [r llen jobs]
        r config set store-max-reply-bytes $prev
    }

    test {marker TTL expiry reopens the window; retry re-executes} {
        r del jobs req:ttl
        r rpush jobs t1 t2
        assert_equal "OK" [r store req:ttl px 40]
        assert_equal "t1" [r lpop jobs]
        assert_equal 1 [r exists req:ttl]
        after 80
        assert_equal 0 [r exists req:ttl]
        # Marker gone: the retry is a fresh execution and pops the next element.
        assert_equal "OK" [r store req:ttl px 40]
        assert_equal "t2" [r lpop jobs]
        assert_equal 0 [r llen jobs]
    }

    test {volatile-ttl eviction drops the TTL-bearing marker (guarantee caveat)} {
        r flushall
        set prevmem [lindex [r config get maxmemory] 1]
        set prevpol [lindex [r config get maxmemory-policy] 1]
        r config set maxmemory-policy volatile-ttl
        r rpush jobs e1
        assert_equal "OK" [r store req:ev px 100000]
        assert_equal "e1" [r lpop jobs]
        assert_equal 1 [r exists req:ev]
        # Force eviction: a tiny maxmemory makes the server evict a volatile key,
        # and the marker carries a TTL so it is a candidate. This demonstrates the
        # documented caveat that only noeviction is strict for markers.
        r config set maxmemory 1
        # A write triggers eviction; the marker (volatile) is dropped.
        catch {r set filler [string repeat y 1000]}
        assert_equal 0 [r exists req:ev]
        r config set maxmemory $prevmem
        r config set maxmemory-policy $prevpol
    }
}

start_server {tags {"store" "repl"}} {
    start_server {} {
        set master [srv -1 client]
        set master_host [srv -1 host]
        set master_port [srv -1 port]
        set replica [srv 0 client]

        test {STORE effects and marker replicate; digests match after LPOP} {
            $master del jobs
            $master rpush jobs job-r1 job-r2
            $replica replicaof $master_host $master_port
            wait_for_sync $replica

            assert_equal "OK" [$master store rrep:1 px 60000]
            assert_equal "job-r1" [$master lpop jobs]

            wait_for_ofs_sync $master $replica
            assert_equal "VKSR" [string range [$replica get rrep:1] 0 3]
            assert {[$replica pttl rrep:1] > 0}
            assert_equal 1 [$replica llen jobs]
            assert_equal [$master debug digest] [$replica debug digest]
        }

        test {served BLPOP effect and marker replicate; digests match} {
            $master del jobs
            set rd [valkey_deferring_client -1]
            $rd store rrep:b px 60000
            assert_equal "OK" [$rd read]
            $rd blpop jobs 5
            wait_for_blocked_client -1
            $master lpush jobs job-r9
            assert_equal {jobs job-r9} [$rd read]
            $rd close

            wait_for_ofs_sync $master $replica
            assert_equal "VKSR" [string range [$replica get rrep:b] 0 3]
            assert_equal 0 [$replica llen jobs]
            assert_equal [$master debug digest] [$replica debug digest]
        }
    }
}
