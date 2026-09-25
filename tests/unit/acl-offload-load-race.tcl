# acl-offload: ACL LOAD role-remap race regression (third review).
#
# ACLRemapSurvivingRoleMembers runs during runtime ACL LOAD and remaps the roles held by SURVIVING
# users -- users the LOAD file does not redefine, i.e. MODULE-created users. A surviving user is
# reader-reachable (a bound fast-path client names it), and admission walks its roles list, so the
# remap must publish a detached replacement list by release store + epoch bump + quiesce, never edit
# the published list's nodes in place. These tests drive a module user that HOLDS a role and stays
# authenticated on a live connection, while runtime ACL LOAD (a) drops the role, (b) replaces/remaps
# it, and (c) for a multi-role survivor drops one role while retaining another -- under acl-offload.
#
# The aclcheck test module provides `aclcheck.auth.module.user.with.role <user> <role>` which creates
# a persistent module user, assigns it the (already-existing) role, and authenticates the calling
# connection to it. The module user is NOT in the aclfile, so ACL LOAD leaves it surviving and hits
# the remap path.
#
# Run under ThreadSanitizer to prove the survivor remap is data-race-free (build both the server and
# the module instrumented, and extend the timeout to absorb TSan's slowdown):
#   make -j SANITIZER=thread && make -C tests/modules SANITIZER=thread
#   ./runtest --single unit/acl-offload-load-race --timeout 2400
# The role-holder's admission still reads u->roles (the offload-eligibility check) while ACL LOAD
# swaps it, so a regression to in-place mutation surfaces here as a TSan report on u->roles.

set modpath [file normalize tests/modules/aclcheck.so]

proc alr_write_aclfile {path lines} {
    set fd [open $path w]
    foreach l $lines { puts $fd $l }
    close $fd
}
proc alr_offload_hits {} { getInfoProperty [r info stats] acl_offload_hits }

set server_path [tmpdir "acl-offload-load-race"]
# The server aborts startup if aclfile is missing; seed it before start_server.
alr_write_aclfile [file join $server_path "users.acl"] {
    {user default on nopass ~* &* +@all}
    {role hr ~h:* +@read +set}
}
start_server [list overrides [list "dir" $server_path "aclfile" "users.acl" "io-threads" 4 \
        "io-threads-always-active" "yes" "acl-offload" "yes" "enable-module-command" "yes" \
        "loadmodule" $modpath] tags [list "acl acl-offload modules external:skip tls:skip"]] {

    set aclfile [file join $server_path "users.acl"]

    test {acl-offload LOAD: drop the role held by a surviving module user} {
        # base aclfile defines default + the roles, then a fresh conn becomes the module role-holder
        alr_write_aclfile $aclfile {
            {user default on nopass ~* &* +@all}
            {role hr ~h:* +@read +set}
        }
        r acl load
        set rd [valkey [srv 0 host] [srv 0 port] 0 $::tls] ;# non-deferring control conn
        $rd aclcheck.auth.module.user.with.role modu1 hr
        # the role-holder pipelines commands from a raw fast-path-ish conn
        set c [valkey [srv 0 host] [srv 0 port] 1 $::tls]
        $c aclcheck.auth.module.user.with.role modu1 hr
        for {set i 0} {$i < 20} {incr i} { $c get h:$i }
        # now DROP the role via ACL LOAD (file no longer defines role hr) while commands are in flight
        alr_write_aclfile $aclfile { {user default on nopass ~* &* +@all} }
        r acl load
        for {set i 0} {$i < 20} {incr i} { catch {$c read} }
        assert_equal PONG [r ping] ;# survivor roles-list remap (drop) did not crash/UAF
        catch {$c close}; catch {$rd close}
    }

    test {acl-offload LOAD: replace/remap the role of a surviving module user} {
        alr_write_aclfile $aclfile {
            {user default on nopass ~* &* +@all}
            {role hr ~h:* +@read +set}
        }
        r acl load
        set c [valkey [srv 0 host] [srv 0 port] 1 $::tls]
        $c aclcheck.auth.module.user.with.role modu2 hr
        for {set i 0} {$i < 20} {incr i} { $c get h:$i }
        # redefine role hr with different rules -> LOAD creates a NEW hr object; survivor must remap to it
        alr_write_aclfile $aclfile {
            {user default on nopass ~* &* +@all}
            {role hr ~h:* ~extra:* +@read +set}
        }
        r acl load
        for {set i 0} {$i < 20} {incr i} { catch {$c read} }
        assert_equal PONG [r ping]
        # the surviving module user still holds the (remapped) role -- read its ACL string via the module
        assert_match "*role=*hr*" [r aclcheck.get.module.user.acl]
        catch {$c close}
    }

    test {acl-offload LOAD: multi-role survivor drops one role, retains another} {
        alr_write_aclfile $aclfile {
            {user default on nopass ~* &* +@all}
            {role hr ~h:* +@read +set}
            {role hr2 ~h2:* +@read +set}
        }
        r acl load
        set c [valkey [srv 0 host] [srv 0 port] 1 $::tls]
        # module user holds BOTH roles in one assignment (role= replaces the set with the named list)
        $c aclcheck.auth.module.user.with.role modu3 hr,hr2
        for {set i 0} {$i < 20} {incr i} { $c get h:$i }
        # drop hr but keep hr2 -> survivor's multi-role list must lose exactly one node, atomically
        alr_write_aclfile $aclfile {
            {user default on nopass ~* &* +@all}
            {role hr2 ~h2:* +@read +set}
        }
        r acl load
        for {set i 0} {$i < 20} {incr i} { catch {$c read} }
        assert_equal PONG [r ping]
        set acl [r aclcheck.get.module.user.acl]
        assert_match "*hr2*" $acl   ;# retained
        catch {$c close}
    }

    test {acl-offload LOAD: role holders are punted, never fast-path hits} {
        # A role-holding principal is not offload-eligible: its allowed commands must be main-checked.
        alr_write_aclfile $aclfile {
            {user default on nopass ~* &* +@all}
            {role hr ~h:* +@read +set}
        }
        r acl load
        set c [valkey [srv 0 host] [srv 0 port] 1 $::tls]
        $c aclcheck.auth.module.user.with.role modu4 hr
        set h0 [alr_offload_hits]
        for {set i 0} {$i < 40} {incr i} { $c get h:$i }
        for {set i 0} {$i < 40} {incr i} { catch {$c read} }
        assert_equal $h0 [alr_offload_hits] ;# role holder produced no fast-path hits (INTENDED path)
        catch {$c close}
    }
}
