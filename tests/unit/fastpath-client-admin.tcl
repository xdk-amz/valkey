# Client administration against fast-path (IO-owned) clients.
#
# Each test first puts a raw client on the fast path and checks INFO fastpath
# (fastpath_clients, fastpath_speculated, fastpath_net_output_bytes), so a pass
# proves the fast path was engaged. The control connection `r` is named (CLIENT
# SETNAME), which keeps it on the main path; on this branch INFO fastpath_clients
# counts the asking client too (issue 77), so an unnamed `r` would count as one.

proc fpca_info {field} { getInfoProperty [r info fastpath] $field }

proc fpca_wait_fastpath_clients {n} {
    wait_for_condition 200 20 {
        [fpca_info fastpath_clients] == $n
    } else {
        fail "expected $n fast-path clients: [r info fastpath]"
    }
}

# Deferring client on db 0 (no SELECT, so it stays fast-path eligible).
proc fpca_client {} {
    return [valkey [srv 0 host] [srv 0 port] 1 $::tls]
}

# Send GETs until the client is on the fast path and a GET was served there.
proc fpca_engage {c key} {
    set s0 [fpca_info fastpath_speculated]
    for {set i 0} {$i < 200} {incr i} {
        for {set j 0} {$j < 10} {incr j} { $c get $key }
        for {set j 0} {$j < 10} {incr j} { $c read }
        if {[fpca_info fastpath_clients] >= 1 && [fpca_info fastpath_speculated] > $s0} return
    }
    fail "client never engaged the fast path: [r info fastpath]"
}

proc fpca_alive {c} {
    if {[catch {$c ping; $c read} e]} { return 0 }
    return [expr {$e eq "PONG"}]
}

# ---------------------------------------------------------------------------
# Plan item 10 (CLI-13): a slow reader of a large reply is not idle-closed.
# The reader pauses between 64 KB reads; the pause shapes the load, it does
# not synchronise anything. Stock Valkey counts every write as interaction.
# ---------------------------------------------------------------------------
start_server {tags {"fastpath client-admin idle-timeout external:skip tls:skip"} overrides {io-threads 4 io-threads-always-active yes}} {
    r select 9
    # A named client stays off the fast path, so it is not counted in fastpath_clients.
    r client setname fpca-control

    test {CLI-13: a fast-path client slowly reading a 32 MB reply is not closed by timeout 1} {
        set size 33554432
        r select 0
        r setrange big [expr {$size - 1}] x
        r select 9
        set s [socket [srv 0 host] [srv 0 port]]
        fconfigure $s -translation binary -blocking 1 -buffering none -buffersize 65536
        # Put the raw connection on the fast path first.
        for {set i 0} {$i < 50} {incr i} {
            puts -nonewline $s "*1\r\n\$4\r\nPING\r\n"
            assert_equal "+PONG\r\n" [read $s 7]
            if {[fpca_info fastpath_clients] == 1} break
        }
        fpca_wait_fastpath_clients 1
        set out0 [fpca_info fastpath_net_output_bytes]
        r config set timeout 1
        puts -nonewline $s "*2\r\n\$3\r\nGET\r\n\$3\r\nbig\r\n"
        set header "\$$size\r\n"
        set want [expr {[string length $header] + $size + 2}]
        set got 0
        set t0 [clock milliseconds]
        while {$got < $want} {
            set chunk [read $s [expr {min(65536, $want - $got)}]]
            if {[string length $chunk] == 0} break
            incr got [string length $chunk]
            # Keep the main-path control connection itself from going idle.
            if {($got / 65536) % 32 == 0} { r ping }
            after 10
        }
        set elapsed [expr {[clock milliseconds] - $t0}]
        r config set timeout 0
        close $s
        # The read must have run long enough for a timeout to matter (validity check).
        assert {$elapsed > 2500 || $got < $want}
        assert_equal $want $got "reply cut after $got of $want bytes, $elapsed ms"
        # Engagement: the fast path wrote the reply.
        assert {[fpca_info fastpath_net_output_bytes] - $out0 >= $size}
        r select 0
        r del big
        r select 9
    }
}
