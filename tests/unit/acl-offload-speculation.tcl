# acl-offload + IO-thread reads: a revoked permission stops IO-thread GETs for a client that is
# still IO-owned when the rules change. ACL LOAD retires the client's principal, so the read gate
# must be recomputed against the live successor, not the retired rules.

proc als_write_aclfile {path lines} { set fd [open $path w]; foreach l $lines { puts $fd $l }; close $fd }
set server_path [tmpdir "acl-offload-speculation"]
als_write_aclfile [file join $server_path "users.acl"] {
    {user default on nopass ~* &* +@all}
    {user alice on >pw ~* +get +@connection}
}
start_server [list overrides [list "dir" $server_path "aclfile" "users.acl" "io-threads" 4 \
        "io-threads-always-active" "yes" "acl-offload" "yes" "save" ""] \
        tags [list "acl acl-offload external:skip tls:skip"]] {

    set aclfile [file join $server_path "users.acl"]
    r select 0 ;# the same db as the fast-path client
    proc als_speculated {} { getInfoProperty [r info fastpath] fastpath_speculated }
    proc als_fpc {} { getInfoProperty [r info fastpath] fastpath_clients }

    # Raw db-0 client: a SELECT would move it off the fast path.
    proc als_engaged_client {} {
        set c [valkey [srv 0 host] [srv 0 port] 1 $::tls]
        $c auth alice pw; assert_equal OK [$c read]
        wait_for_condition 200 20 { [als_fpc] >= 1 } else { fail "alice did not attach: [r info fastpath]" }
        set before [als_speculated]
        for {set round 0} {$round < 50} {incr round} {
            for {set i 0} {$i < 10} {incr i} { $c get k }
            for {set i 0} {$i < 10} {incr i} { assert_equal v [$c read] }
            if {[als_speculated] > $before} { return $c }
        }
        fail "IO-thread reads never engaged for alice"
    }

    test {acl-offload: ACL LOAD revoking GET stops IO-thread reads of a retired principal} {
        r set k v
        set c [als_engaged_client]
        als_write_aclfile $aclfile {
            {user default on nopass ~* &* +@all}
            {user alice on >pw ~* +set +@connection}
        }
        r acl load
        $c get k
        assert_error {*NOPERM*} {$c read}
        $c close
    }

    test {acl-offload: ACL SETUSER revoking GET stops IO-thread reads} {
        als_write_aclfile $aclfile {
            {user default on nopass ~* &* +@all}
            {user alice on >pw ~* +get +@connection}
        }
        r acl load
        r set k v
        set c [als_engaged_client]
        r acl setuser alice -get
        $c get k
        assert_error {*NOPERM*} {$c read}
        $c close
    }
}
