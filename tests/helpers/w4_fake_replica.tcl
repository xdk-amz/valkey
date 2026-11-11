# A raw-socket replica for shutdown tests. It completes a disk-backed full sync (REPLCONF,
# PSYNC ? -1, reads the RDB bulk) and then reads nothing until a trigger file appears, so the
# primary's replication stream for it piles up in the backlog. After the trigger it drains the
# socket SLOWLY (a small read, then a short sleep) until the primary closes the link, so it models
# a slow live replica: the backlog stays large while writes make slow forward progress.
# It writes the number of stream bytes read after the trigger to outfile, and touches readyfile
# once it is online.
#
# usage: tclsh w4_fake_replica.tcl host port triggerfile outfile readyfile

proc w4_main {host port trigger outfile readyfile} {
    set fd [socket $host $port]
    fconfigure $fd -translation binary -buffering none -blocking 1
    puts -nonewline $fd "REPLCONF capa psync2\r\n"
    flush $fd
    set line [string trim [gets $fd]]
    if {$line ne "+OK"} { error "REPLCONF: $line" }
    puts -nonewline $fd "PSYNC ? -1\r\n"
    flush $fd
    set line [string trim [gets $fd]]
    if {![string match "+FULLRESYNC *" $line]} { error "PSYNC: $line" }
    # Skip the newline keepalives the primary sends while the RDB child runs.
    while {1} {
        set n [gets $fd header]
        if {$n < 0} { error "EOF before the RDB bulk" }
        set header [string trim $header]
        if {$header ne ""} break
    }
    if {[string index $header 0] ne {$}} { error "bulk header: $header" }
    set remaining [string range $header 1 end]
    while {$remaining > 0} {
        set chunk [read $fd $remaining]
        if {$chunk eq ""} { error "EOF inside the RDB bulk" }
        incr remaining -[string length $chunk]
    }
    # Online now. Report readiness, then do not read until the trigger appears.
    set rf [open $readyfile w]
    puts $rf ready
    close $rf
    while {![file exists $trigger]} { after 20 }
    # Drain SLOWLY: read a small chunk, then sleep. A slow live replica is the point of the N3
    # shutdown test - the primary's output to it piles up and is drained a little at a time. Reading
    # ~32 KB every 20 ms (about 1.5 MB/s) keeps the socket mostly full, so the primary always has an
    # offloaded write in flight and a large backlog pending behind it, for as long as the test needs.
    fconfigure $fd -blocking 1
    set total 0
    while {1} {
        if {[catch {set chunk [read $fd 32768]}]} break
        if {$chunk eq ""} break
        incr total [string length $chunk]
        after 20
    }
    catch {close $fd}
    set of [open $outfile w]
    puts $of $total
    close $of
}

if {[llength $argv] != 5} {
    puts stderr "usage: tclsh w4_fake_replica.tcl host port triggerfile outfile readyfile"
    exit 2
}
w4_main {*}$argv
