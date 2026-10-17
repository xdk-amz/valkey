# In cluster mode the keyspace is already split by slot, so auto keyspace-shard-bits resolves to the
# standalone effective value 0 even with several io-threads.

start_cluster 1 0 {tags {"external:skip" "cluster"} overrides {io-threads 4}} {
    test {cluster mode with auto resolves keyspace-shard-bits to 0} {
        assert_equal {-1} [lindex [R 0 config get keyspace-shard-bits] 1]
        assert_equal {0} [getInfoProperty [R 0 info fastpath] keyspace_shard_bits]
    }
}
