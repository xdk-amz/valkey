# Fast-path commands carry the originating client's id and peer by value, so
# ACL LOG and other observability name the real client rather than the
# main-thread executor that ran the command.

# A deferring client that sends nothing before its first command, so it stays
# on the fast path (SELECT would move it to the main path).
proc fp_client {} {
    return [valkey [srv 0 host] [srv 0 port] 1 $::tls]
}

proc fp_peer_of {rd} {
    set sock [fconfigure [$rd channel] -sockname]
    return "[lindex $sock 0]:[lindex $sock 2]"
}

# CLIENT LIST line of the connection whose peer address is $peer.
proc fp_client_list_entry {peer} {
    foreach line [split [string trim [r client list]] "\n"] {
        if {[string match "*addr=$peer *" $line]} { return $line }
    }
    return ""
}

proc fp_client_id_of {peer} {
    set line [fp_client_list_entry $peer]
    assert {$line ne ""}
    regexp {id=(\d+)} $line -> id
    return $id
}

proc fp_wait_fastpath_clients {n} {
    wait_for_condition 100 20 {
        [getInfoProperty [r info fastpath] fastpath_clients] == $n
    } else {
        fail "expected $n fast-path clients: [r info fastpath]"
    }
}

proc fp_acl_log_entries_for_object {object} {
    set res {}
    foreach entry [r acl log] {
        if {[dict get $entry object] eq $object} { lappend res $entry }
    }
    return $res
}

proc fp_deny_keys {} {
    r acl setuser default resetkeys ~allowed:*
}

proc fp_allow_keys {} {
    r acl setuser default resetkeys allkeys
}

# A deferring control connection that stays on the main path (named clients are not admitted).
proc fp_ctl_client {} {
    set c [valkey_deferring_client]
    $c client setname fp-ctl
    assert_equal OK [$c read]
    return $c
}

proc fp_resp {args} {
    set cmd "*[llength $args]\r\n"
    foreach a $args { append cmd "$[string length $a]\r\n$a\r\n" }
    return $cmd
}

# Runs $cmds on main right behind a DEBUG SLEEP, all from one read, so no event-loop
# pass (hence no batch drain) happens in between: a batch published during the sleep
# is still unexecuted when $cmds run.
proc fp_run_behind_sleep {ctl cmds} {
    set payload [fp_resp debug sleep 0.4]
    foreach cmd $cmds { append payload [fp_resp {*}$cmd] }
    $ctl write $payload
    $ctl flush
}

# The monitor also sees the control connection's commands; skip to the one wanted.
proc fp_monitor_line_matching {m pattern} {
    for {set i 0} {$i < 50} {incr i} {
        set line [$m read]
        if {[string match $pattern $line]} { return $line }
    }
    fail "no MONITOR line matching $pattern"
}

start_server {tags {"fastpath origin external:skip tls:skip"} overrides {io-threads 2 io-batch-hold-us 10000 enable-debug-command yes}} {
    assert_equal {io-threads 2} [r config get io-threads]
    assert_equal {io-threads-fast-path yes} [r config get io-threads-fast-path]
    # Named clients stay on the main path, so the control connection is never counted.
    r client setname fp-control
    r select 0 ;# the same db as the raw fast-path clients

    test {Fast path: two clients in one batch keep distinct ids and peers} {
        r acl log reset
        fp_deny_keys
        set a [fp_client]
        set b [fp_client]
        fp_wait_fastpath_clients 2
        set peer_a [fp_peer_of $a]
        set peer_b [fp_peer_of $b]
        set id_a [fp_client_id_of $peer_a]
        set id_b [fp_client_id_of $peer_b]
        assert {$id_a != $id_b}
        assert {$peer_a ne $peer_b}

        set batches [getInfoProperty [r info fastpath] fastpath_batches]
        $a get denied:a
        $b get denied:b
        assert_error {*NOPERM*} {$a read}
        assert_error {*NOPERM*} {$b read}
        # Both commands were held into the same batch.
        assert_equal [expr {$batches + 1}] [getInfoProperty [r info fastpath] fastpath_batches]

        set ea [lindex [fp_acl_log_entries_for_object denied:a] 0]
        set eb [lindex [fp_acl_log_entries_for_object denied:b] 0]
        assert_match "id=$id_a addr=$peer_a *" [dict get $ea client-info]
        assert_match "id=$id_b addr=$peer_b *" [dict get $eb client-info]
        $a close
        $b close
        fp_allow_keys
    }

    test {Fast path: several commands from one client keep the same origin} {
        r acl log reset
        fp_deny_keys
        set a [fp_client]
        fp_wait_fastpath_clients 1
        set peer_a [fp_peer_of $a]
        set id_a [fp_client_id_of $peer_a]
        for {set i 0} {$i < 5} {incr i} { $a get denied:same:$i }
        for {set i 0} {$i < 5} {incr i} { assert_error {*NOPERM*} {$a read} }
        for {set i 0} {$i < 5} {incr i} {
            set e [lindex [fp_acl_log_entries_for_object denied:same:$i] 0]
            assert_match "id=$id_a addr=$peer_a *" [dict get $e client-info]
        }
        $a close
        fp_allow_keys
    }

    test {Fast path: ACL denial is attributed to the origin, not the executor} {
        r acl log reset
        fp_deny_keys
        set a [fp_client]
        fp_wait_fastpath_clients 1
        set peer_a [fp_peer_of $a]
        set id_a [fp_client_id_of $peer_a]
        $a get denied:attr
        assert_error {*NOPERM*} {$a read}
        set e [lindex [fp_acl_log_entries_for_object denied:attr] 0]
        set info [dict get $e client-info]
        assert_match "id=$id_a addr=$peer_a *" $info
        # The executor itself has no connection, so its peer text is empty.
        assert_no_match "* addr= *" $info
        assert_match "*user=default*" $info
        $a close
        fp_allow_keys
    }

    test {Fast path: recycled batches do not leak origin between clients} {
        r acl log reset
        fp_deny_keys
        set a [fp_client]
        fp_wait_fastpath_clients 1
        set peer_a [fp_peer_of $a]
        set id_a [fp_client_id_of $peer_a]
        $a get denied:first
        assert_error {*NOPERM*} {$a read}
        $a close
        fp_wait_fastpath_clients 0

        # The next batch on this IO thread comes from the freelist.
        set b [fp_client]
        fp_wait_fastpath_clients 1
        set peer_b [fp_peer_of $b]
        set id_b [fp_client_id_of $peer_b]
        assert {$id_a != $id_b}
        $b get denied:second
        assert_error {*NOPERM*} {$b read}
        set e [lindex [fp_acl_log_entries_for_object denied:second] 0]
        assert_match "id=$id_b addr=$peer_b *" [dict get $e client-info]
        assert_no_match "*id=$id_a *" [dict get $e client-info]
        assert_no_match "*addr=$peer_a *" [dict get $e client-info]
        $b close
        fp_allow_keys
    }

    test {Fast path: origin survives a client disconnecting while its batch is in flight} {
        r acl log reset
        fp_deny_keys
        set a [fp_client]
        fp_wait_fastpath_clients 1
        set peer_a [fp_peer_of $a]
        set id_a [fp_client_id_of $peer_a]

        # Park main so the batch cannot execute before the client is gone.
        set ctl [fp_client]
        $ctl client id
        $ctl read
        $ctl debug sleep 0.5
        after 50
        $a get denied:inflight
        $a flush
        after 50
        $a close
        assert_equal {OK} [$ctl read]
        $ctl close

        wait_for_condition 100 20 {
            [llength [fp_acl_log_entries_for_object denied:inflight]] == 1
        } else {
            fail "denied command was not logged"
        }
        set e [lindex [fp_acl_log_entries_for_object denied:inflight] 0]
        assert_match "id=$id_a addr=$peer_a *" [dict get $e client-info]
        fp_wait_fastpath_clients 0
        assert_equal PONG [r ping]
        fp_allow_keys
    }

    test {Fast path: SLOWLOG reports the origin peer} {
        r slowlog reset
        r config set slowlog-log-slower-than 0
        set a [fp_client]
        fp_wait_fastpath_clients 1
        set peer_a [fp_peer_of $a]
        $a set slow:key v
        assert_equal OK [$a read]
        r config set slowlog-log-slower-than 1000000
        set found 0
        foreach e [r slowlog get 128] {
            if {[lindex $e 3] eq {set slow:key v}} {
                assert_equal $peer_a [lindex $e 4]
                set found 1
            }
        }
        assert_equal 1 $found
        $a close
    }

    test {Fast path: a client returns to the fast path after HELLO ran on main} {
        set a [fp_client]
        fp_wait_fastpath_clients 1
        $a hello 2
        set id [dict get [$a read] id]
        fp_wait_fastpath_clients 1
        set batches [getInfoProperty [r info fastpath] fastpath_batches]
        $a set readmit:k v
        assert_equal OK [$a read]
        assert_equal [expr {$batches + 1}] [getInfoProperty [r info fastpath] fastpath_batches]
        assert_equal $id [fp_client_id_of [fp_peer_of $a]]
        $a close
    }

    test {Fast path: other unsupported commands keep the client on the main path} {
        set a [fp_client]
        fp_wait_fastpath_clients 1
        $a client id
        $a read
        fp_wait_fastpath_clients 0
        $a set stay:k v
        assert_equal OK [$a read]
        assert_equal 0 [getInfoProperty [r info fastpath] fastpath_clients]
        $a close
    }

    test {Fast path: MONITOR names the origin peer} {
        set m [valkey_deferring_client]
        $m monitor
        assert_match {*OK*} [$m read]
        set a [fp_client]
        fp_wait_fastpath_clients 1
        set peer [fp_peer_of $a]
        $a set mon:k v
        assert_equal OK [$a read]
        set line [fp_monitor_line_matching $m {*"set" "mon:k" "v"*}]
        assert_match "*\\\[0 $peer\\\] \"set\" \"mon:k\" \"v\"*" $line
        $m close
        $a close
    }

    test {Fast path: AUTH as a named user runs on the fast path under that user's ACL} {
        r acl setuser bob on >pw ~allowed:* +@all
        r acl log reset
        set a [fp_client]
        fp_wait_fastpath_clients 1
        set peer [fp_peer_of $a]
        set id [fp_client_id_of $peer]
        $a auth bob pw
        assert_equal OK [$a read]
        fp_wait_fastpath_clients 1
        set batches [getInfoProperty [r info fastpath] fastpath_batches]
        $a set allowed:k v
        assert_equal OK [$a read]
        $a get denied:k
        assert_error {*NOPERM*} {$a read}
        assert {[getInfoProperty [r info fastpath] fastpath_batches] >= $batches + 1}
        set e [lindex [fp_acl_log_entries_for_object denied:k] 0]
        assert_equal bob [dict get $e username]
        set info [dict get $e client-info]
        assert_match "id=$id addr=$peer laddr=*:[srv 0 port] *" $info
        assert_match "*user=bob*" $info
        assert_match "*user=bob*" [fp_client_list_entry $peer]
        $a close
        fp_wait_fastpath_clients 0
        r acl deluser bob
    }

    test {Fast path: DELUSER drops the deleted user's queued commands and closes its client} {
        r acl setuser bob on >pw ~* +@all
        set a [fp_client]
        fp_wait_fastpath_clients 1
        $a auth bob pw
        assert_equal OK [$a read]
        fp_wait_fastpath_clients 1
        set ctl [fp_ctl_client]
        set batches [getInfoProperty [r info fastpath] fastpath_batches]
        fp_run_behind_sleep $ctl {{acl deluser bob}}
        after 50
        $a set deluser:k v
        $a flush
        wait_for_condition 100 20 {
            [getInfoProperty [r info fastpath] fastpath_batches] == $batches + 1
        } else {
            fail "command was not published while main slept"
        }
        assert_equal OK [$ctl read]
        assert_equal 1 [$ctl read]
        fp_wait_fastpath_clients 0
        assert_equal 0 [r exists deluser:k]
        catch {$a read} e
        assert_match {*I/O error*} $e
        $a close
        $ctl close
    }

    test {Fast path: CLIENT KILL drops the killed client's queued commands} {
        set a [fp_client]
        fp_wait_fastpath_clients 1
        set peer [fp_peer_of $a]
        set ctl [fp_ctl_client]
        set batches [getInfoProperty [r info fastpath] fastpath_batches]
        fp_run_behind_sleep $ctl [list [list client kill addr $peer]]
        after 50
        $a set kill:k v
        $a flush
        wait_for_condition 100 20 {
            [getInfoProperty [r info fastpath] fastpath_batches] == $batches + 1
        } else {
            fail "command was not published while main slept"
        }
        assert_equal OK [$ctl read]
        assert_equal 1 [$ctl read]
        fp_wait_fastpath_clients 0
        assert_equal 0 [r exists kill:k]
        catch {$a read} e
        assert_match {*I/O error*} $e
        $a close
        $ctl close
    }

    test {Fast path: CLIENT PAUSE postpones fast-path clients on the main path} {
        set a [fp_client]
        fp_wait_fastpath_clients 1
        set b [fp_client]
        fp_wait_fastpath_clients 2
        r client pause 5000 write
        $a set paused:k v
        $b get paused:k
        $a flush
        $b flush
        # Reads run at once; the write is postponed and visible as a blocked client,
        # so both clients are on the main path now.
        assert_equal {} [$b read]
        wait_for_blocked_clients_count 1
        fp_wait_fastpath_clients 0
        assert_equal 0 [r exists paused:k]
        r client unpause
        assert_equal OK [$a read]
        assert_equal 1 [r exists paused:k]
        assert_equal v [$b get paused:k ; $b read]
        $a close
        $b close
    }
}

# ACL LOAD replaces user structs; a fast-path client's queued commands must see the reloaded rules.
set fp_acl_dir [tmpdir "server.fastpath-acl"]
proc fp_write_acl {dir bob_rules} {
    set fd [open [file join $dir users.acl] w]
    puts $fd "user default on nopass ~* &* +@all"
    puts $fd "user bob on >pw $bob_rules"
    close $fd
}
fp_write_acl $fp_acl_dir {~* &* +@all}

start_server [list tags {"fastpath origin external:skip tls:skip"} overrides [list dir $fp_acl_dir aclfile users.acl io-threads 2 io-batch-hold-us 10000]] {
    r client setname fp-control
    r select 0 ;# the same db as the raw fast-path clients

    test {Fast path: ACL LOAD applies the reloaded rules to a queued command without disconnecting} {
        r acl log reset
        set a [fp_client]
        fp_wait_fastpath_clients 1
        set peer [fp_peer_of $a]
        set id [fp_client_id_of $peer]
        $a auth bob pw
        assert_equal OK [$a read]
        fp_wait_fastpath_clients 1
        set ctl [fp_ctl_client]
        set batches [getInfoProperty [r info fastpath] fastpath_batches]
        fp_write_acl [lindex [r config get dir] 1] {~* &* +@all -set}
        fp_run_behind_sleep $ctl {{acl load}}
        after 50
        $a set load:k v
        $a flush
        wait_for_condition 100 20 {
            [getInfoProperty [r info fastpath] fastpath_batches] == $batches + 1
        } else {
            fail "command was not published while main slept"
        }
        assert_equal OK [$ctl read]
        assert_equal OK [$ctl read]
        assert_error {*NOPERM*} {$a read}
        assert_equal 0 [r exists load:k]
        set e [lindex [fp_acl_log_entries_for_object set] 0]
        assert_equal bob [dict get $e username]
        assert_match "id=$id addr=$peer *user=bob*" [dict get $e client-info]

        # Still connected, still on the fast path, under the new rules.
        assert_equal 1 [getInfoProperty [r info fastpath] fastpath_clients]
        $a get load:k
        assert_equal {} [$a read]
        assert_match "*user=bob*" [fp_client_list_entry $peer]
        $a close
        $ctl close
        fp_wait_fastpath_clients 0
    }

    test {Fast path: ACL LOAD disconnects a fast-path client whose user is gone} {
        fp_write_acl [lindex [r config get dir] 1] {~* &* +@all}
        assert_equal OK [r acl load]
        set a [fp_client]
        fp_wait_fastpath_clients 1
        $a auth bob pw
        assert_equal OK [$a read]
        fp_wait_fastpath_clients 1
        set ctl [fp_ctl_client]
        set batches [getInfoProperty [r info fastpath] fastpath_batches]
        set fd [open [file join [lindex [r config get dir] 1] users.acl] w]
        puts $fd "user default on nopass ~* &* +@all"
        close $fd
        fp_run_behind_sleep $ctl {{acl load}}
        after 50
        $a set gone:k v
        $a flush
        wait_for_condition 100 20 {
            [getInfoProperty [r info fastpath] fastpath_batches] == $batches + 1
        } else {
            fail "command was not published while main slept"
        }
        assert_equal OK [$ctl read]
        assert_equal OK [$ctl read]
        fp_wait_fastpath_clients 0
        assert_equal 0 [r exists gone:k]
        catch {$a read} e
        assert_match {*I/O error*} $e
        $a close
        $ctl close
    }
}

# With the default user password-protected no client is admitted at accept; it joins the fast path after AUTH.
start_server {tags {"fastpath origin external:skip tls:skip"} overrides {requirepass fppw io-threads 2 io-batch-hold-us 10000}} {
    r auth fppw
    r client setname fp-control
    r select 0
    r acl setuser bob on >pw ~* &* +@all

    test {Fast path: a client that authenticates with commands pipelined behind AUTH joins the fast path} {
        set a [fp_client]
        $a write [fp_resp auth bob pw]
        $a write [fp_resp set pipelined:k v]
        $a write [fp_resp get pipelined:k]
        $a flush
        assert_equal OK [$a read]
        assert_equal OK [$a read]
        assert_equal v [$a read]
        fp_wait_fastpath_clients 1
        set batches [getInfoProperty [r info fastpath] fastpath_batches]
        $a set pipelined:k2 v2
        assert_equal OK [$a read]
        assert_equal [expr {$batches + 1}] [getInfoProperty [r info fastpath] fastpath_batches]
        assert_match "*user=bob*" [fp_client_list_entry [fp_peer_of $a]]
        $a close
        fp_wait_fastpath_clients 0
    }

    test {Fast path: a client that authenticates as the password-protected default user joins the fast path} {
        set a [fp_client]
        $a auth fppw
        assert_equal OK [$a read]
        fp_wait_fastpath_clients 1
        set batches [getInfoProperty [r info fastpath] fastpath_batches]
        $a set default:k v
        assert_equal OK [$a read]
        assert_equal [expr {$batches + 1}] [getInfoProperty [r info fastpath] fastpath_batches]
        $a close
        fp_wait_fastpath_clients 0
    }

    test {Fast path: a failed AUTH keeps the client on the main path} {
        set a [fp_client]
        $a auth bob wrong
        assert_error {*WRONGPASS*} {$a read}
        $a ping
        assert_error {*NOAUTH*} {$a read}
        assert_equal 0 [getInfoProperty [r info fastpath] fastpath_clients]
        $a auth fppw
        assert_equal OK [$a read]
        fp_wait_fastpath_clients 1
        $a close
        fp_wait_fastpath_clients 0
    }
}
