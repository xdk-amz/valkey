# acl-offload: fast-path attachment boundary for module-authenticated principals.
#
# The ACL LOAD survivor keep-alive (aclReloadRetiresPrincipal gating original->fp_refs++) matters for
# a fast-path client bound to a user that survives the reload un-retired. Config users are retired by
# the reload (their keep-alive is balanced on hand-back); the default user is guarded; the only other
# survivor is a module user. A client handed to main for a main-only command rejoins the fast path
# once main has nothing further to do for it (fastpathHandoffDone sets fp_readmit), so a module-authed
# client that runs its module AUTH on main and is otherwise eligible DOES attach. That makes the
# survivor keep-alive load-bearing: these tests pin that a module user can hold a live fast-path
# client across ACL LOAD and the reload stays safe (no crash, no UAF).

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

    test {module-authed client attaches to the fast path after its module AUTH ran on main} {
        set c [valkey [srv 0 host] [srv 0 port] 1 $::tls]
        $c aclcheck.auth.module.user.with.role modu hr
        assert_equal OK [$c read]
        for {set i 0} {$i < 200} {incr i} { $c get h:$i }
        for {set i 0} {$i < 200} {incr i} { catch {$c read} }
        # The module AUTH ran on main; with nothing further owed to it the client rejoins the fast
        # path, so a module-authed principal now holds a live fast-path client.
        wait_for_condition 200 20 { [fpa_fpc] == 1 } else { fail "module-authed client did not attach: [r info fastpath]" }
        catch {$c close}
        wait_for_condition 200 20 { [fpa_fpc] == 0 } else { fail "fast-path client did not detach after close" }
    }

    test {ACL LOAD with a surviving module user holding a live fast-path client is safe} {
        # modu is module-created, so ACL LOAD leaves it surviving and exercises the keep-alive that
        # is now load-bearing: the reload must handle the live fast-path ref without crash or UAF.
        set c [valkey [srv 0 host] [srv 0 port] 1 $::tls]
        $c aclcheck.auth.module.user.with.role modu2 hr
        assert_equal OK [$c read]
        $c get h:0
        catch {$c read}
        wait_for_condition 200 20 { [fpa_fpc] == 1 } else { fail "module-authed client did not attach: [r info fastpath]" }
        fpa_write_aclfile [file join $server_path "users.acl"] {
            {user default on nopass ~* &* +@all}
            {role hr ~h:* ~extra:* +@read +set}
        }
        assert_equal OK [r acl load]
        assert_equal PONG [r ping]
        wait_for_condition 200 20 { [fpa_fpc] == 0 } else { fail "fast-path ref not released after reload: [r info fastpath]" }
        catch {$c close}
        assert_equal PONG [r ping]
    }
}
