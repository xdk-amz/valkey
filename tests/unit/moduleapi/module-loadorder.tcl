set testmodule_infotest [file normalize tests/modules/infotest.so]
set testmodule_timer [file normalize tests/modules/timer.so]
set testmodule_fork [file normalize tests/modules/fork.so]

test {modules config rewrite preserves load order} {
    start_server {tags {"modules"}} {
        # Load modules in a specific order: timer, fork, infotest
        r module load $testmodule_timer
        r module load $testmodule_fork
        r module load $testmodule_infotest

        # Verify all are loaded
        set modules [lmap x [r module list] {dict get $x name}]
        assert_not_equal [lsearch $modules timer] -1
        assert_not_equal [lsearch $modules fork] -1
        assert_not_equal [lsearch $modules infotest] -1

        # Rewrite config
        r config rewrite

        # Read the config file and extract loadmodule lines in order
        set cfg [srv 0 config_file]
        set fd [open $cfg r]
        set content [read $fd]
        close $fd

        set loadmodule_order {}
        foreach line [split $content "\n"] {
            if {[string match "loadmodule *" $line]} {
                if {[string match "*timer*" $line]} {
                    lappend loadmodule_order "timer"
                } elseif {[string match "*fork*" $line]} {
                    lappend loadmodule_order "fork"
                } elseif {[string match "*infotest*" $line]} {
                    lappend loadmodule_order "infotest"
                }
            }
        }

        # All three modules should appear
        assert_equal [llength $loadmodule_order] 3

        # The order in the config file must match the load order
        assert_equal [lindex $loadmodule_order 0] "timer"
        assert_equal [lindex $loadmodule_order 1] "fork"
        assert_equal [lindex $loadmodule_order 2] "infotest"
    }
}
