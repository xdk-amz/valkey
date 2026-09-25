# acl-offload: fast-path attachment boundary for module-authenticated principals.
#
# The ACL LOAD survivor keep-alive (aclReloadRetiresPrincipal gating original->fp_refs++) only
# matters for a fast-path client bound to a user that survives the reload un-retired. Config users
# are retired by the reload (their keep-alive is balanced on hand-back); the default user is guarded;
# the only other survivor is a module user. These tests pin why that keep-alive is defensive: a
# fast-path client is re-admitted after authentication only via the password path
# (checkPasswordBasedAuth sets fp_readmit), while ValkeyModule_AuthenticateClientWithUser does not,
# so a module-authenticated client never re-attaches to the fast path. No fast-path client is ever
# bound to a surviving module user during ACL LOAD. If a future change re-admits module-authed
# clients, the second test fails, flagging that the survivor keep-alive is now load-bearing.

set modpath [file normalize tests/modules/aclcheck.so]
proc fpa_write_aclfile {path lines} { set fd [open $path w]; foreach l $lines { puts $fd $l }; close $fd }
set server_path [tmpdir "acl-offload-fpref-attach"]
fpa_write_aclfile [file join $server_path "users.acl"] {
    {user default on nopass ~* &* +@all}
    {role hr ~h:* +@read +set}
}
start_server [list overrides [list "dir" $server_path "aclfile" "users.acl" "io-threads" 4 \
        "io-threads-always-active" "yes" "io-batch-hold-us" 10000 "acl-offload" "yes" \
        "enable-module-command" "yes" "loadmodule" $modpath] tags [list "acl acl-offload modules external:skip tls:skip"]] {

    proc fpa_fpc {} { getInfoProperty [r info fastpath] fastpath_clients }

    test {password-authed restricted client attaches to the fast path (control)} {
        r acl setuser fpu on >pw ~h:* +@read +@write +@connection
        set a [valkey [srv 0 host] [srv 0 port] 1 $::tls]
        $a auth fpu pw; assert_equal OK [$a read]
        wait_for_condition 200 20 { [fpa_fpc] >= 1 } else { fail "password-authed client did not attach: [r info fastpath]" }
        catch {$a close}
        wait_for_condition 200 20 { [fpa_fpc] == 0 } else { fail "fast-path client did not detach after close" }
    }

    test {module-authed client never attaches (survivor keep-alive is defensive)} {
        set c [valkey [srv 0 host] [srv 0 port] 1 $::tls]
        $c aclcheck.auth.module.user.with.role modu hr
        assert_equal OK [$c read]
        for {set i 0} {$i < 200} {incr i} { $c get h:$i }
        after 300
        for {set i 0} {$i < 200} {incr i} { catch {$c read} }
        assert_equal 0 [fpa_fpc]
        catch {$c close}
    }
}
