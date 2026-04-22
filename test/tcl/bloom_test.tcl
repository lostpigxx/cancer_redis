#!/usr/bin/env tclsh
#
# TCL integration tests for the redis_bloom module.
# Self-contained: starts a redis-server, loads the module, runs tests, shuts down.
#
# Usage: tclsh bloom_test.tcl [path/to/redis_bloom.so]

package require Tcl 8.5

# ============================================================
# Minimal Redis client over TCP (no external dependencies)
# ============================================================

proc redis_connect {host port} {
  set fd [socket $host $port]
  fconfigure $fd -translation binary -buffering full
  return $fd
}

proc redis_command {fd args} {
  set cmd "*[llength $args]\r\n"
  foreach arg $args {
    append cmd "\$[string length $arg]\r\n$arg\r\n"
  }
  puts -nonewline $fd $cmd
  flush $fd
  return [redis_read_reply $fd]
}

proc redis_read_reply {fd} {
  gets $fd line
  set type [string index $line 0]
  set data [string range $line 1 end]
  # Strip trailing \r
  set data [string trimright $data "\r"]

  switch $type {
    "+" { return $data }
    "-" { error $data }
    ":" { return [expr {int($data)}] }
    "$" {
      set len $data
      if {$len == -1} { return "(nil)" }
      set payload [read $fd [expr {$len + 2}]]
      return [string range $payload 0 end-2]
    }
    "*" {
      set count $data
      if {$count == -1} { return "(nil)" }
      set result {}
      for {set i 0} {$i < $count} {incr i} {
        lappend result [redis_read_reply $fd]
      }
      return $result
    }
    default {
      error "Unknown reply type: $type ($line)"
    }
  }
}

proc r {args} {
  global redis_fd
  return [redis_command $redis_fd {*}$args]
}

# ============================================================
# Test framework
# ============================================================

set test_passed 0
set test_failed 0
set test_errors {}

proc test {name body expected} {
  global test_passed test_failed test_errors
  set result ""
  set err ""
  if {[catch {set result [uplevel 1 $body]} err]} {
    set result "ERROR: $err"
  }
  if {$result eq $expected} {
    incr test_passed
    puts "  PASS: $name"
  } else {
    incr test_failed
    set msg "$name\n    expected: $expected\n    got:      $result"
    lappend test_errors $msg
    puts "  FAIL: $name"
    puts "    expected: $expected"
    puts "    got:      $result"
  }
}

proc test_assert {name body} {
  global test_passed test_failed test_errors
  if {[catch {uplevel 1 $body} err]} {
    incr test_failed
    lappend test_errors "$name\n    $err"
    puts "  FAIL: $name"
    puts "    $err"
  } else {
    incr test_passed
    puts "  PASS: $name"
  }
}

proc test_error {name body pattern} {
  global test_passed test_failed test_errors
  set caught 0
  if {[catch {uplevel 1 $body} err]} {
    set caught 1
    if {[string match $pattern $err]} {
      incr test_passed
      puts "  PASS: $name"
      return
    } else {
      incr test_failed
      set msg "$name\n    expected error matching: $pattern\n    got error: $err"
      lappend test_errors $msg
      puts "  FAIL: $name"
      puts "    expected error matching: $pattern"
      puts "    got error: $err"
      return
    }
  }
  if {!$caught} {
    incr test_failed
    lappend test_errors "$name\n    expected error but command succeeded"
    puts "  FAIL: $name (expected error but succeeded)"
  }
}

# ============================================================
# Server lifecycle
# ============================================================

proc find_free_port {} {
  set sock [socket -server {} 0]
  set port [lindex [fconfigure $sock -sockname] 2]
  close $sock
  return $port
}

proc start_redis {module_path port} {
  catch {
    exec redis-server \
      --port $port \
      --daemonize yes \
      --loglevel warning \
      --logfile /tmp/bloom_tcl_test.log \
      --dbfilename bloom_tcl_test.rdb \
      --dir /tmp \
      --loadmodule $module_path
  }
  for {set i 0} {$i < 50} {incr i} {
    if {![catch {socket localhost $port} fd]} {
      close $fd
      return
    }
    after 100
  }
  # Print log for debugging
  catch {
    set f [open /tmp/bloom_tcl_test.log r]
    puts "Redis log:\n[read $f]"
    close $f
  }
  error "redis-server failed to start on port $port"
}

proc stop_redis {fd} {
  catch {redis_command $fd SHUTDOWN NOSAVE}
  catch {close $fd}
  after 200
  file delete -force /tmp/bloom_tcl_test.rdb
  file delete -force /tmp/bloom_tcl_test.log
}

# ============================================================
# Resolve module path
# ============================================================

if {$argc > 0} {
  set module_path [file normalize [lindex $argv 0]]
} else {
  set script_dir [file dirname [file normalize [info script]]]
  set module_path [file normalize "$script_dir/../../build/redis_bloom.so"]
}

if {![file exists $module_path]} {
  puts "ERROR: Module not found at $module_path"
  puts "Usage: tclsh bloom_test.tcl \[path/to/redis_bloom.so\]"
  exit 1
}

puts "Module: $module_path"

# ============================================================
# Start server
# ============================================================

set port [find_free_port]
puts "Starting redis-server on port $port..."
start_redis $module_path $port
set redis_fd [redis_connect localhost $port]
puts "Connected.\n"

# ============================================================
# Test cases
# ============================================================

puts "=== BF.RESERVE ==="

test "BF.RESERVE creates a new filter" {
  r BF.RESERVE reserve_basic 0.01 1000
} {OK}

test_error "BF.RESERVE on existing key returns error" {
  r BF.RESERVE reserve_basic 0.01 1000
} {ERR*key already exists*}

test_error "BF.RESERVE with invalid error rate (0)" {
  r BF.RESERVE reserve_err0 0 1000
} {ERR*rate*}

test_error "BF.RESERVE with invalid error rate (1)" {
  r BF.RESERVE reserve_err1 1.0 1000
} {ERR*rate*}

test_error "BF.RESERVE with invalid error rate (negative)" {
  r BF.RESERVE reserve_errneg -0.5 1000
} {ERR*rate*}

test_error "BF.RESERVE with invalid capacity (0)" {
  r BF.RESERVE reserve_cap0 0.01 0
} {ERR*capacity*}

test_error "BF.RESERVE with invalid capacity (negative)" {
  r BF.RESERVE reserve_capneg 0.01 -1
} {ERR*capacity*}

test "BF.RESERVE with EXPANSION" {
  r BF.RESERVE reserve_exp 0.01 100 EXPANSION 4
} {OK}

test "BF.RESERVE with NONSCALING" {
  r BF.RESERVE reserve_ns 0.01 100 NONSCALING
} {OK}

test_error "BF.RESERVE wrong arity" {
  r BF.RESERVE onlytwo 0.01
} {ERR*wrong*}

puts "\n=== BF.ADD ==="

test "BF.ADD new item returns 1" {
  r BF.ADD add_basic hello
} {1}

test "BF.ADD duplicate item returns 0" {
  r BF.ADD add_basic hello
} {0}

test "BF.ADD second unique item returns 1" {
  r BF.ADD add_basic world
} {1}

test "BF.ADD auto-creates filter on missing key" {
  r BF.ADD add_auto testitem
} {1}

test_error "BF.ADD wrong arity" {
  r BF.ADD
} {ERR*wrong*}

puts "\n=== BF.EXISTS ==="

test "BF.EXISTS finds existing item" {
  r BF.EXISTS add_basic hello
} {1}

test "BF.EXISTS returns 0 for missing item" {
  r BF.EXISTS add_basic nonexistent
} {0}

test "BF.EXISTS returns 0 for missing key" {
  r BF.EXISTS no_such_key anything
} {0}

puts "\n=== BF.MADD ==="

test "BF.MADD adds multiple items" {
  r BF.MADD madd_test a b c
} {1 1 1}

test "BF.MADD with duplicates returns mixed results" {
  r BF.MADD madd_test a d
} {0 1}

puts "\n=== BF.MEXISTS ==="

test "BF.MEXISTS checks multiple items" {
  r BF.MEXISTS madd_test a b c d missing
} {1 1 1 1 0}

test "BF.MEXISTS on missing key returns all zeros" {
  r BF.MEXISTS no_such_key x y z
} {0 0 0}

puts "\n=== BF.INSERT ==="

test "BF.INSERT basic with ITEMS" {
  r BF.INSERT insert_basic ITEMS x y z
} {1 1 1}

test "BF.INSERT with custom ERROR and CAPACITY" {
  r BF.INSERT insert_custom ERROR 0.001 CAPACITY 5000 ITEMS a b
} {1 1}

test "BF.INSERT with EXPANSION" {
  r BF.INSERT insert_exp EXPANSION 4 ITEMS m n
} {1 1}

test "BF.INSERT duplicate returns 0" {
  r BF.INSERT insert_basic ITEMS x
} {0}

test_error "BF.INSERT with NOCREATE on missing key" {
  r BF.INSERT insert_nocreate NOCREATE ITEMS a
} {ERR*key does not exist*}

test "BF.INSERT with NOCREATE on existing key works" {
  r BF.INSERT insert_basic NOCREATE ITEMS new_item
} {1}

test_error "BF.INSERT without ITEMS keyword" {
  r BF.INSERT insert_noitems a b c
} {ERR*}

puts "\n=== BF.INFO ==="

test "BF.INFO returns full info" {
  set info [r BF.INFO reserve_basic]
  # Should be a list of 10 elements (5 key-value pairs)
  llength $info
} {10}

test_assert "BF.INFO Capacity field is correct" {
  set info [r BF.INFO reserve_basic]
  set idx [lsearch $info "Capacity"]
  set val [lindex $info [expr {$idx + 1}]]
  if {$val != 1000} { error "Capacity=$val, expected 1000" }
}

test_assert "BF.INFO Number of filters >= 1" {
  set info [r BF.INFO reserve_basic]
  set idx [lsearch $info "Number of filters"]
  set val [lindex $info [expr {$idx + 1}]]
  if {$val < 1} { error "Filters=$val, expected >= 1" }
}

test_assert "BF.INFO Expansion rate is 2" {
  set info [r BF.INFO reserve_basic]
  set idx [lsearch $info "Expansion rate"]
  set val [lindex $info [expr {$idx + 1}]]
  if {$val != 2} { error "Expansion=$val, expected 2" }
}

test "BF.INFO single field: Capacity" {
  r BF.INFO reserve_basic Capacity
} {1000}

test "BF.INFO single field: Items" {
  r BF.INFO reserve_basic Items
} {0}

test_error "BF.INFO on missing key" {
  r BF.INFO no_such_key
} {ERR*key does not exist*}

test_error "BF.INFO unrecognized field" {
  r BF.INFO reserve_basic BadField
} {ERR*unknown*}

test_assert "BF.INFO NONSCALING filter shows null expansion" {
  set info [r BF.INFO reserve_ns]
  set idx [lsearch $info "Expansion rate"]
  set val [lindex $info [expr {$idx + 1}]]
  if {$val ne "(nil)"} { error "Expected nil but got: $val" }
}

puts "\n=== BF.CARD ==="

test "BF.CARD returns inserted count" {
  r DEL card_test
  r BF.ADD card_test a
  r BF.ADD card_test b
  r BF.ADD card_test c
  r BF.ADD card_test a
  r BF.CARD card_test
} {3}

test "BF.CARD on missing key returns 0" {
  r BF.CARD no_such_key
} {0}

puts "\n=== NONSCALING behavior ==="

test_assert "NONSCALING filter rejects overflow" {
  r BF.RESERVE ns_overflow 0.01 10 NONSCALING
  set rejected 0
  for {set i 0} {$i < 100} {incr i} {
    if {[catch {r BF.ADD ns_overflow "item_$i"} err]} {
      set rejected 1
      break
    }
  }
  if {!$rejected} { error "Expected overflow rejection but all items accepted" }
}

test_assert "NONSCALING filter stays at 1 layer" {
  set info [r BF.INFO ns_overflow]
  set idx [lsearch $info "Number of filters"]
  set val [lindex $info [expr {$idx + 1}]]
  if {$val != 1} { error "Expected 1 filter but got $val" }
}

puts "\n=== Auto-scaling behavior ==="

test_assert "Filter auto-scales when full" {
  r BF.RESERVE scale_test 0.01 10
  for {set i 0} {$i < 50} {incr i} {
    r BF.ADD scale_test "scale_$i"
  }
  set info [r BF.INFO scale_test]
  set idx [lsearch $info "Number of filters"]
  set val [lindex $info [expr {$idx + 1}]]
  if {$val <= 1} { error "Expected multiple filters but got $val" }
}

test_assert "All items survive scaling (no false negatives)" {
  for {set i 0} {$i < 50} {incr i} {
    set exists [r BF.EXISTS scale_test "scale_$i"]
    if {$exists != 1} { error "False negative for scale_$i" }
  }
}

puts "\n=== BF.SCANDUMP / BF.LOADCHUNK round-trip ==="

test_assert "SCANDUMP/LOADCHUNK preserves data" {
  r DEL dump_src dump_dst
  r BF.RESERVE dump_src 0.01 500
  for {set i 0} {$i < 200} {incr i} {
    r BF.ADD dump_src "dump_item_$i"
  }

  # Dump all chunks: each call returns [next_cursor, data]
  # cursor=0 returns header, cursor=1..N returns bit arrays per layer
  set load_iter 1
  set scan_iter 0
  while {1} {
    set reply [r BF.SCANDUMP dump_src $scan_iter]
    set next_cursor [lindex $reply 0]
    set chunk_data [lindex $reply 1]

    r BF.LOADCHUNK dump_dst $load_iter $chunk_data
    incr load_iter

    if {$next_cursor == 0} break
    set scan_iter $next_cursor
  }

  # Verify cardinality
  set src_card [r BF.CARD dump_src]
  set dst_card [r BF.CARD dump_dst]
  if {$src_card != $dst_card} {
    error "Cardinality mismatch: src=$src_card dst=$dst_card"
  }

  # Verify all items
  for {set i 0} {$i < 200} {incr i} {
    set exists [r BF.EXISTS dump_dst "dump_item_$i"]
    if {$exists != 1} { error "False negative after LOADCHUNK for dump_item_$i" }
  }
}

puts "\n=== Wrong type errors ==="

test_error "BF.ADD on string key" {
  r SET string_key hello
  r BF.ADD string_key item
} {WRONGTYPE*}

test_error "BF.EXISTS on string key" {
  r BF.EXISTS string_key item
} {WRONGTYPE*}

test_error "BF.INFO on string key" {
  r BF.INFO string_key
} {WRONGTYPE*}

puts "\n=== Edge cases ==="

test "BF.ADD empty string as item" {
  r BF.ADD edge_empty ""
} {1}

test "BF.EXISTS empty string as item" {
  r BF.EXISTS edge_empty ""
} {1}

test "BF.ADD very long item" {
  set long_item [string repeat "x" 10000]
  r BF.ADD edge_long $long_item
} {1}

test "BF.EXISTS very long item" {
  set long_item [string repeat "x" 10000]
  r BF.EXISTS edge_long $long_item
} {1}

puts "\n=== RDB persistence ==="

test_assert "Data survives BGSAVE + restart" {
  r DEL persist_test
  r BF.RESERVE persist_test 0.01 1000
  r BF.ADD persist_test alpha
  r BF.ADD persist_test beta
  r BF.ADD persist_test gamma

  r BGSAVE
  after 2000

  # Reconnect on same port after restart
  global redis_fd port module_path
  catch {redis_command $redis_fd SHUTDOWN SAVE}
  catch {close $redis_fd}
  after 1000

  start_redis $module_path $port
  set redis_fd [redis_connect localhost $port]

  set e1 [r BF.EXISTS persist_test alpha]
  set e2 [r BF.EXISTS persist_test beta]
  set e3 [r BF.EXISTS persist_test gamma]
  set card [r BF.CARD persist_test]

  if {$e1 != 1 || $e2 != 1 || $e3 != 1} {
    error "Items missing after restart: alpha=$e1 beta=$e2 gamma=$e3"
  }
  if {$card != 3} { error "Card=$card after restart, expected 3" }
}

# ============================================================
# Cleanup & Summary
# ============================================================

puts "\n=========================================="
puts "Results: $test_passed passed, $test_failed failed"
puts "==========================================\n"

if {$test_failed > 0} {
  puts "Failed tests:"
  foreach err $test_errors {
    puts "  - $err"
  }
  puts ""
}

stop_redis $redis_fd
file delete -force /tmp/bloom_tcl_test.rdb

exit $test_failed
