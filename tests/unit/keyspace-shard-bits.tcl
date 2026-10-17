# keyspace-shard-bits resolves to an effective value at startup: auto (the default) gives 8 with more
# than one io-thread and 0 with one, cluster mode gives 0, and an explicit value is used as given.
# CONFIG GET reports the configured value and CONFIG REWRITE keeps auto as auto; INFO reports the
# effective value.

proc effective_shard_bits {} {
    return [getInfoProperty [r info fastpath] keyspace_shard_bits]
}

proc configured_shard_bits {} {
    return [lindex [r config get keyspace-shard-bits] 1]
}

start_server {tags {"keyspace-shard-bits" "external:skip"} overrides {io-threads 1}} {
    test {auto with one io-thread resolves to 0} {
        assert_equal {-1} [configured_shard_bits]
        assert_equal {0} [effective_shard_bits]
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
