# Staged event loop: every path that closes a client reaches a fast-path client. Each client
# here is a raw db-0 socket: the test framework's client sends SELECT first, which moves a
# connection off the fast path.

proc slc_fastpath_clients {} {
    return [getInfoProperty [r info fastpath] fastpath_clients]
}

proc slc_client_count {} {
    return [llength [split [string trim [r client list]] "\n"]]
}

proc slc_stat {name} {
    return [getInfoProperty [r info stats] $name]
}

proc slc_resp {args} {
    set cmd "*[llength $args]\r\n"
    foreach a $args { append cmd "$[string length $a]\r\n$a\r\n" }
    return $cmd
}

# A raw socket that ran one command and sits on the fast path.
proc slc_fastpath_socket {{auth {}}} {
    set before [slc_fastpath_clients]
    set fd [socket [srv 0 host] [srv 0 port]]
    fconfigure $fd -translation binary -blocking 1 -buffering full
    if {$auth ne {}} {
        puts -nonewline $fd [slc_resp auth {*}$auth]
        flush $fd
        assert_equal "+OK" [string trim [gets $fd]]
    }
    puts -nonewline $fd [slc_resp set slc:probe 1]
    flush $fd
    assert_equal "+OK" [string trim [gets $fd]]
    wait_for_condition 200 20 {
        [slc_fastpath_clients] == $before + 1
    } else {
        fail "the socket did not join the fast path: [r info fastpath]"
    }
    return $fd
}

# The id of the one connection that is neither the test client nor named.
proc slc_other_client_id {} {
    set self [r client id]
    foreach line [split [string trim [r client list]] "\n"] {
        regexp {id=(\d+)} $line -> id
        if {$id != $self} { return $id }
    }
    fail "no other client: [r client list]"
}

proc slc_wait_closed {fd count} {
    wait_for_condition 200 20 {
        [slc_client_count] == $count && [slc_fastpath_clients] == 0
    } else {
        fail "client not closed: [r client list] [r info fastpath]"
    }
    fconfigure $fd -blocking 0
    wait_for_condition 200 20 {
        [catch {read $fd}] || [eof $fd]
    } else {
        fail "socket still open"
    }
    catch {close $fd} ;# unsent bytes of a reset connection fail the close
}

start_server {tags {"staged-loop external:skip tls:skip"} overrides {io-threads 3}} {
    r select 0

    test {Staged loop close: output buffer limit disconnects a fast-path client doing big GETs} {
        r config set client-output-buffer-limit "normal 256kb 0 0"
        r set slc:big [string repeat x 65536]
        set count [slc_client_count]
        set closed [slc_stat client_output_buffer_limit_disconnections]
        set fd [slc_fastpath_socket]
        assert_equal [expr {$count + 1}] [slc_client_count]
        # Never read the replies: the kernel buffers fill and the rest stays unwritten.
        set payload ""
        for {set i 0} {$i < 400} {incr i} { append payload [slc_resp get slc:big] }
        fconfigure $fd -blocking 0
        catch {
            puts -nonewline $fd $payload
            flush $fd
        }
        slc_wait_closed $fd $count
        assert_equal [expr {$closed + 1}] [slc_stat client_output_buffer_limit_disconnections]
        r config set client-output-buffer-limit "normal 0 0 0"
        assert_equal PONG [r ping]
    }

    test {Staged loop close: query buffer limit disconnects a fast-path client} {
        r config set client-query-buffer-limit 1mb
        set count [slc_client_count]
        set closed [slc_stat client_query_buffer_limit_disconnections]
        set fd [slc_fastpath_socket]
        # A bulk argument announced larger than the limit and streamed in part.
        fconfigure $fd -blocking 0
        catch {
            puts -nonewline $fd "*3\r\n\$3\r\nSET\r\n\$5\r\nslc:q\r\n\$4000000\r\n"
            puts -nonewline $fd [string repeat y 1500000]
            flush $fd
        }
        slc_wait_closed $fd $count
        assert_equal [expr {$closed + 1}] [slc_stat client_query_buffer_limit_disconnections]
        r config set client-query-buffer-limit 1gb
        assert_equal PONG [r ping]
    }

    test {Staged loop close: CLIENT KILL ID disconnects a fast-path client} {
        set count [slc_client_count]
        set fd [slc_fastpath_socket]
        assert_equal 1 [r client kill id [slc_other_client_id]]
        slc_wait_closed $fd $count
    }

    test {Staged loop close: CLIENT KILL USER disconnects a fast-path client} {
        r acl setuser slc-kill on >pw ~* +@all
        set count [slc_client_count]
        set fd [slc_fastpath_socket {slc-kill pw}]
        assert_equal 1 [r client kill user slc-kill]
        slc_wait_closed $fd $count
        r acl deluser slc-kill
    }

    test {Staged loop close: ACL DELUSER disconnects a fast-path client of that user} {
        r acl setuser slc-del on >pw ~* +@all
        set count [slc_client_count]
        set fd [slc_fastpath_socket {slc-del pw}]
        assert_equal 1 [r acl deluser slc-del]
        slc_wait_closed $fd $count
        assert_equal PONG [r ping]
    }

    test {Staged loop close: a fast-path client keeps working after other clients are closed} {
        set fd [slc_fastpath_socket]
        puts -nonewline $fd [slc_resp incr slc:n]
        flush $fd
        assert_equal ":1" [string trim [gets $fd]]
        assert_equal 1 [slc_fastpath_clients]
        close $fd
        wait_for_condition 200 20 {
            [slc_fastpath_clients] == 0
        } else {
            fail "fast-path client not released"
        }
    }
}
