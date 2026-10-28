# keyspace-shard-bits resolves to an effective value at startup: auto (the default) gives 8 on every
# standalone server whatever io-threads is, cluster mode gives 0, and an explicit value is used as given.
# CONFIG GET reports the configured value and CONFIG REWRITE keeps auto as auto; INFO reports the
# effective value.

proc effective_shard_bits {} {
    return [getInfoProperty [r info fastpath] keyspace_shard_bits]
}

proc configured_shard_bits {} {
    return [lindex [r config get keyspace-shard-bits] 1]
}

start_server {tags {"keyspace-shard-bits" "external:skip"} overrides {io-threads 1}} {
    test {auto with one io-thread resolves to 8} {
        assert_equal {-1} [configured_shard_bits]
        assert_equal {8} [effective_shard_bits]
    }
}

start_server {tags {"keyspace-shard-bits" "external:skip" "tls:skip"} overrides {io-threads 1 io-threads-parallel-writes-min 1}} {
    test {a server started with one io-thread runs writes on the lanes after io-threads is raised} {
        r config set notify-keyspace-events ""
        r config set io-threads 4
        set fd [socket [srv 0 host] [srv 0 port]]
        fconfigure $fd -translation {crlf binary} -blocking 1 -buffering full
        puts -nonewline $fd "*1\r\n\$4\r\nping\r\n"
        flush $fd
        assert_equal {+PONG} [gets $fd]
        wait_for_condition 200 20 {
            [getInfoProperty [r info fastpath] fastpath_clients] >= 1
        } else {
            fail "socket never joined the fast path: [r info fastpath]"
        }
        set before [getInfoProperty [r info fastpath] fastpath_write_lane_entries]
        set buf ""
        for {set i 0} {$i < 400} {incr i} {
            append buf "*3\r\n\$3\r\nset\r\n\$[string length ksb:$i]\r\nksb:$i\r\n\$1\r\nv\r\n"
        }
        puts -nonewline $fd $buf
        flush $fd
        for {set i 0} {$i < 400} {incr i} { assert_equal {+OK} [gets $fd] }
        close $fd
        assert_morethan [getInfoProperty [r info fastpath] fastpath_write_lane_entries] $before
        r select 0
        assert_equal 400 [r dbsize]
    }
}

start_server {tags {"keyspace-shard-bits" "external:skip"} overrides {io-threads 4}} {
    test {auto with several io-threads resolves to 8} {
        assert_equal {-1} [configured_shard_bits]
        assert_equal {8} [effective_shard_bits]
    }

    test {CONFIG REWRITE keeps auto as auto} {
        r config rewrite
        restart_server 0 true false
        assert_equal {-1} [configured_shard_bits]
        assert_equal {8} [effective_shard_bits]
    }
}

start_server {tags {"keyspace-shard-bits" "external:skip"} overrides {io-threads 4 keyspace-shard-bits 0}} {
    test {explicit 0 is honored with several io-threads} {
        assert_equal {0} [configured_shard_bits]
        assert_equal {0} [effective_shard_bits]
    }
}

start_server {tags {"keyspace-shard-bits" "external:skip"} overrides {io-threads 4 keyspace-shard-bits 6}} {
    test {explicit 6 is honored with several io-threads} {
        assert_equal {6} [configured_shard_bits]
        assert_equal {6} [effective_shard_bits]
    }
}
