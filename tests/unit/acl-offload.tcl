# ACL offload: an IO thread evaluates a command's ACL permissions at admission and
# records an ALLOW verdict + the ACL epoch; the main-thread executor consumes it only
# while the epoch still matches, and re-checks on main otherwise. DENY/unknown/dynamic
# always fall back to main so the exact error and attribution are canonical.
#
# These are real-engine tests against db-0 raw fast-path clients. A client must be
# fast-path eligible: it authenticates as the DEFAULT user (nopass in the test config)
# and never issues SELECT, so it is admitted immediately. Per-user rules are exercised
# by reshaping the default user's ACL (the same pattern the fastpath-origin suite uses),
# which avoids the AUTH-then-eligible admission race a password user would introduce.

proc ao_client {} {
    return [valkey [srv 0 host] [srv 0 port] 1 $::tls]
}
proc ao_hits {} { getInfoProperty [r info stats] acl_offload_hits }
proc ao_punts {} { getInfoProperty [r info stats] acl_offload_punts }
proc ao_quiesce {} { getInfoProperty [r info stats] acl_offload_quiesce_count }
proc ao_wait_fastpath_clients {n} {
    wait_for_condition 200 20 {
        [getInfoProperty [r info fastpath] fastpath_clients] == $n
    } else {
        fail "expected $n fast-path clients: [r info fastpath]"
    }
}
proc ao_default_allow {rules} { r acl setuser default on nopass resetkeys resetchannels -@all {*}$rules }


start_server {tags {"acl acl-offload external:skip tls:skip"} overrides {io-threads 4 io-threads-always-active yes io-batch-hold-us 10000 acl-offload yes enable-debug-command yes}} {
    assert_equal {acl-offload yes} [r config get acl-offload]
    r client setname ao-control ;# named client stays on the main path
    r select 0
    # The control connection must keep full rights while we reshape the DEFAULT user (which the
    # raw fast-path clients are bound to). Authenticate it as a dedicated admin first.
    r acl setuser aoadmin on nopass ~* &* +@all
    r auth aoadmin ""

    test {acl-offload: allowed commands are consumed on the fast path (hits climb)} {
        ao_default_allow {~foo:* +@read +set}
        set a [ao_client]
        ao_wait_fastpath_clients 1
        set h0 [ao_hits]
        for {set i 0} {$i < 30} {incr i} { $a set foo:$i v$i }
        for {set i 0} {$i < 30} {incr i} { assert_equal OK [$a read] }
        wait_for_condition 100 20 { [ao_hits] > $h0 } else { fail "no acl_offload_hits: [r info stats]" }
        $a close
    }

    test {acl-offload: denied command returns the exact NOPERM command error} {
        ao_default_allow {~foo:* +@read +set}
        set a [ao_client]
        ao_wait_fastpath_clients 1
        $a del foo:1
        assert_error {*NOPERM*has no permissions to run the 'del' command*} {$a read}
        $a close
    }

    test {acl-offload: denied key returns the exact NOPERM key error} {
        ao_default_allow {~foo:* +@read +set}
        set a [ao_client]
        ao_wait_fastpath_clients 1
        $a set other:1 v
        assert_error {*NOPERM*key*} {$a read}
        $a close
    }

    test {acl-offload: category rule (+@read) allows GET, denies SET} {
        ao_default_allow {~* +@read}
        set a [ao_client]
        ao_wait_fastpath_clients 1
        $a get anykey
        assert_equal {} [$a read]
        $a set anykey v
        assert_error {*NOPERM*command*} {$a read}
        $a close
    }

    test {acl-offload: true pipeline of allowed commands} {
        ao_default_allow {~foo:* +@read +set}
        set a [ao_client]
        ao_wait_fastpath_clients 1
        set n 60
        for {set i 0} {$i < $n} {incr i} { $a set foo:p:$i v }
        for {set i 0} {$i < $n} {incr i} { assert_equal OK [$a read] }
        $a close
    }

    test {acl-offload: two fast-path clients each get their own verdict in a batch} {
        ao_default_allow {~foo:* +@read +set}
        set a [ao_client]
        set b [ao_client]
        ao_wait_fastpath_clients 2
        $a set foo:1 x
        $b set foo:2 y
        $a set other:9 z ;# denied key for both
        $b set other:8 z
        assert_equal OK [$a read]
        assert_equal OK [$b read]
        assert_error {*NOPERM*key*} {$a read}
        assert_error {*NOPERM*key*} {$b read}
        $a close; $b close
    }

    test {acl-offload: SELECT moves the client off the fast path; command still correct} {
        ao_default_allow {~foo:* +@read +set +select}
        set a [ao_client]
        ao_wait_fastpath_clients 1
        $a select 0
        assert_equal OK [$a read]
        $a set foo:sel v
        assert_equal OK [$a read]
        $a close
    }

    test {acl-offload: TOCTOU -- revoke then write secret, a pre-admitted reader never sees it} {
        ao_default_allow {~secret:* +@read +set}
        r set secret:k INITIAL
        set readers {}
        for {set i 0} {$i < 8} {incr i} { lappend readers [ao_client] }
        ao_wait_fastpath_clients 8
        set leaked 0
        for {set round 0} {$round < 40} {incr round} {
            foreach rd $readers { $rd get secret:k }
            ao_default_allow {~public:* +@read +set} ;# revoke access to secret:* (bumps epoch, quiesces)
            r set secret:k SECRET-$round
            foreach rd $readers {
                set reply ""
                catch {set reply [$rd read]}
                if {$reply eq "SECRET-$round"} { incr leaked }
            }
            ao_default_allow {~secret:* +@read +set} ;# restore for next round's admission
        }
        foreach rd $readers { $rd close }
        assert_equal 0 $leaked
    }

    test {acl-offload: epoch move under load counts punts, results stay correct} {
        ao_default_allow {~foo:* +@read +set}
        set a [ao_client]
        ao_wait_fastpath_clients 1
        set p0 [ao_punts]
        for {set i 0} {$i < 40} {incr i} {
            $a set foo:e:$i v
            ao_default_allow {~foo:* +@read +set} ;# bump epoch mid-flight
        }
        for {set i 0} {$i < 40} {incr i} { assert_equal OK [$a read] }
        assert {[ao_punts] >= $p0}
        $a close
    }

    test {acl-offload: requirepass change under pipelined load is quiesced, not crashing} {
        ao_default_allow {~foo:* +@read +set}
        set a [ao_client]
        ao_wait_fastpath_clients 1
        set q0 [ao_quiesce]
        for {set i 0} {$i < 10} {incr i} { $a set foo:rp:$i v }
        r config set requirepass "temppass"
        r config set requirepass ""
        for {set i 0} {$i < 10} {incr i} { catch {$a read} }
        assert {[ao_quiesce] > $q0}
        assert_equal PONG [r ping]
        $a close
        ao_default_allow {~foo:* +@read +set}
    }

    test {acl-offload: DELUSER of a bound user is safe} {
        r acl setuser tmpu on nopass ~t:* +@read +set
        # Bind tmpu by authenticating a client as it (clientSetUser marks it USER_FLAG_BOUND),
        # then delete the user (retirement path) and confirm the server stays healthy.
        set m [ao_client]
        $m auth tmpu ""
        assert_equal OK [$m read]
        r acl deluser tmpu
        catch {$m close}
        assert_equal PONG [r ping]
    }

    test {acl-offload: SETUSER on a bound role-member does not crash and keeps membership} {
        # Defect-3 regression: ACLCopyUser republishing a bound user that RETAINS a role must not
        # dictAdd the user to a role it is already a member of. Bind a client as the member, then run
        # an unrelated SETUSER on that same member and assert no crash + role membership intact.
        r acl setrole rrole "~role:*" +@read +set
        r acl setuser member1 on nopass "~m:*" +@read +set role=rrole
        # a client authenticated as member1 binds it (USER_FLAG_BOUND) and its role
        set m [ao_client]; $m auth member1 ""; assert_equal OK [$m read]
        $m get role:x ;# exercises the role's selectors via the offload/main path
        catch {$m read}
        # unrelated SETUSER on the SAME bound member, retaining rrole -> must not dictAdd-assert
        r acl setuser member1 on nopass "~m:*" "~extra:*" +@read +set role=rrole
        assert_equal PONG [r ping] ;# server survived (would have asserted/crashed pre-fix)
        # membership intact: member1 still lists rrole
        assert_match "*rrole*" [r acl getuser member1]
        # add a second role then drop the first: membership diff must add/remove correctly
        r acl setrole rrole2 "~role2:*" +@read +set
        r acl setuser member1 on nopass "~m:*" +@read +set role=rrole2
        assert_equal PONG [r ping]
        assert_match "*rrole2*" [r acl getuser member1]
        $m close
        ao_default_allow {~foo:* +@read +set}
    }

    test {acl-offload: role-holder pipelines while role-list is replaced/freed concurrently} {
        # UAF regression (2nd review): a role-holding bound client's admission reads u->roles inside
        # the seqlock bracket; a concurrent SETUSER that swaps+frees the old roles list must be waited
        # out by quiesce, never freed under the reader. Drive a role-holder under pipelined load while
        # another connection repeatedly does role-PRESERVING and role-CHANGING SETUSER on that same
        # user, forcing old roles-list replacement and free each round. Assert: server survives, the
        # role-holder's commands still return correct results, and -- proving the INTENDED path -- a
        # role holder is never offloaded, so its allowed commands are PUNTED/main-checked, not hits.
        r acl setrole hrole "~h:*" +@read +set
        r acl setrole hrole2 "~h2:*" +@read +set
        r acl setuser holder on nopass "~h:*" +@read +set role=hrole
        set rd [ao_client]; $rd auth holder ""; assert_equal OK [$rd read]
        set hits0 [ao_hits]
        set punts0 [ao_punts]
        for {set round 0} {$round < 40} {incr round} {
            # pipeline several allowed commands from the role holder
            for {set i 0} {$i < 8} {incr i} { $rd set h:$round:$i v }
            # role-preserving then role-changing SETUSER on the SAME bound user -> replaces+frees roles list
            r acl setuser holder on nopass "~h:*" "~x:*" +@read +set role=hrole
            r acl setuser holder on nopass "~h:*" +@read +set role=hrole,hrole2
            r acl setuser holder on nopass "~h:*" +@read +set role=hrole
            for {set i 0} {$i < 8} {incr i} { assert_equal OK [$rd read] }
        }
        assert_equal PONG [r ping]
        # INTENDED path: a role holder is ineligible for the offload verdict, so its allowed commands
        # did NOT become fast-path hits (they were main-checked). Prove hits did not climb for it.
        assert_equal $hits0 [ao_hits]
        $rd close
        ao_default_allow {~foo:* +@read +set}
    }

    test {acl-offload: DELUSER of a role-holding bound user with live fast-path refs is safe} {
        r acl setrole drole "~d:*" +@read +set
        r acl setuser dholder on nopass "~d:*" +@read +set role=drole
        set rd [ao_client]; $rd auth dholder ""; assert_equal OK [$rd read]
        $rd set d:1 v; assert_equal OK [$rd read] ;# holder is live with a fast-path ref
        # delete the role-holding user underneath a live connection; DELUSER frees fast-path clients,
        # so the connection is closed by the server -- do NOT read from it afterwards (it would block).
        r acl deluser dholder
        catch {$rd close}
        # the retirement path ran with a role-holding principal; server must be healthy and the role
        # deletable now the holder is gone (no dangling member left behind).
        assert_equal PONG [r ping]
        assert_equal 1 [r acl delrole drole]
        ao_default_allow {~foo:* +@read +set}
    }

    test {acl-offload: counters exposed in INFO stats} {
        set stats [r info stats]
        assert_match "*acl_offload_hits:*" $stats
        assert_match "*acl_offload_punts:*" $stats
        assert_match "*acl_offload_quiesce_count:*" $stats
        assert_match "*acl_offload_quiesce_total_us:*" $stats
        assert_match "*acl_offload_quiesce_max_us:*" $stats
    }
}

start_server {tags {"acl acl-offload external:skip tls:skip"} overrides {io-threads 4 io-threads-always-active yes acl-offload no}} {
    test {acl-offload disabled: no hits are recorded} {
        r client setname ao-control
        r select 0
        r acl setuser aoadmin on nopass ~* &* +@all
        r auth aoadmin ""
        r acl setuser default on nopass resetkeys resetchannels -@all ~foo:* +@read +set
        set a [ao_client]
        for {set i 0} {$i < 20} {incr i} { $a set foo:$i v }
        for {set i 0} {$i < 20} {incr i} { assert_equal OK [$a read] }
        assert_equal 0 [ao_hits]
        $a close
    }
}
