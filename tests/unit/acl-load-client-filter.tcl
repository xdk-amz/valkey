# CLIENT LIST USER, CLIENT KILL USER and CLIENT KILL NOT-USER must match a client by its
# user after ACL LOAD. A fast-path client keeps the retired user object until it hands
# back to main, so the filters must resolve it to the live user (audit finding F4).

set server_path [tmpdir "server.acl-load-filter"]
exec cp -f tests/assets/user.acl $server_path
start_server [list overrides [list "dir" $server_path "aclfile" "user.acl" \
        "io-threads" 4 "io-threads-always-active" "yes" "save" ""] \
        tags [list "acl fastget external:skip tls:skip"]] {
    test {CLIENT LIST/KILL USER and NOT-USER match a fast-path client after ACL LOAD} {
        r set fl:k v
        set rd [valkey_deferring_client]
        $rd auth alice alice
        assert_equal OK [$rd read]
        for {set i 0} {$i < 20} {incr i} { $rd get fl:k }
        for {set i 0} {$i < 20} {incr i} { assert_equal v [$rd read] }
        # Engagement: the alice connection is on the fast path.
        assert_morethan [getInfoProperty [r info fastpath] fastpath_clients] 0
        assert_morethan [getInfoProperty [r info fastpath] fastpath_speculated] 0
        assert_equal 1 [llength [split [string trim [r client list user alice]] "\n"]]

        # File unchanged: alice is replaced by a new user object with the same rules.
        assert_equal OK [r acl load]
        for {set i 0} {$i < 5} {incr i} { $rd get fl:k }
        for {set i 0} {$i < 5} {incr i} { assert_equal v [$rd read] }
        assert_morethan [getInfoProperty [r info fastpath] fastpath_clients] 0

        assert_equal 1 [llength [split [string trim [r client list user alice]] "\n"]]
        assert_equal 0 [r client kill not-user alice skipme yes]
        assert_equal 1 [r client kill user alice]
        wait_for_condition 50 100 {
            [string trim [r client list user alice]] eq {}
        } else {
            fail "alice was not killed"
        }
        $rd close
    }
}
