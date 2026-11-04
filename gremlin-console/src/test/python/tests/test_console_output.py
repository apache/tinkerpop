'''
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
'''

"""How results are printed: iteration, the result limit, empty and null results, explain() width and colors."""

import re
import time

from .console_helpers import (MODERN_GRAPH, PROMPT, TERMINAL_MODES, clean_lines, exit_console, load_modern, results, resync, send,
                              writes_preferences)


def test_traversal_prints_every_result_in_order(console):
    child, _ = console.start("-C")
    load_modern(child)
    assert results(child, "g.V()") == ["==>v[%d]" % i for i in range(1, 7)]
    assert results(child, "g.V().values('name')") == \
        ["==>marko", "==>vadas", "==>lop", "==>josh", "==>ripple", "==>peter"]
    assert exit_console(child) == 0


def test_default_limit_prints_100_results_then_ellipsis(console):
    child, _ = console.start("-C")
    lines = send(child, "(1..101).toList()")
    assert [line for line in lines if line.startswith("==>")] == ["==>%d" % i for i in range(1, 101)]
    assert lines[-1] == "..."
    assert exit_console(child) == 0


def test_exactly_100_results_has_no_ellipsis(console):
    child, _ = console.start("-C")
    lines = send(child, "(1..100).toList()")
    assert len([line for line in lines if line.startswith("==>")]) == 100
    assert "..." not in lines
    assert exit_console(child) == 0


def test_empty_results_print_nothing(console):
    child, _ = console.start("-C")
    load_modern(child)
    assert send(child, "[]") == []
    assert send(child, "[:]") == []
    assert send(child, "g.V().has('name', 'nobody')") == []
    assert exit_console(child) == 0


def test_appending_to_a_list_that_was_printed_empty(console):
    # Console.groovy has special handling so that printing an empty list does not keep its iterator around, which
    # would otherwise make the next line fail with a ConcurrentModificationException
    child, _ = console.start("-C")
    assert send(child, "x = []") == ["==>null"]
    assert send(child, "x") == []
    assert results(child, "x << 'test'") == ["==>test"]
    assert results(child, "x << 'more'") == ["==>test", "==>more"]
    assert exit_console(child) == 0


def test_top_level_map_prints_one_entry_per_line(console):
    child, _ = console.start("-C")
    assert results(child, "[a: 1, b: 2]") == ["==>a=1", "==>b=2"]
    # a map nested in a list is printed whole
    assert results(child, "[[a: 1]]") == ["==>[a:1]"]
    assert exit_console(child) == 0


def test_println_output_then_null_result(console):
    child, _ = console.start("-C")
    assert send(child, "println 'hi'") == ["hi", "==>null"]
    assert results(child, "null") == ["==>null"]
    assert exit_console(child) == 0


def test_assignment_prints_null_and_sets_the_variable(console):
    child, _ = console.start("-C")
    assert results(child, "x = 5") == ["==>null"]
    assert results(child, "s = 'text'") == ["==>null"]
    assert results(child, "x") == ["==>5"]
    assert results(child, "s") == ["==>text"]
    # an assigned traversal is not iterated by the assignment
    load_modern(child)
    assert results(child, "t = g.V().values('name')") == ["==>null"]
    assert results(child, "t.count()") == ["==>6"]
    assert exit_console(child) == 0


def test_unicode_round_trip(console):
    child, _ = console.start("-C")
    assert results(child, "'h\u00e9llo \u65e5\u672c'") == ["==>h\u00e9llo \u65e5\u672c"]
    assert results(child, "'\u65e5\u672c'.length()") == ["==>2"]
    assert exit_console(child) == 0


def _explain_line_lengths(console, columns):
    """Runs a long explain() in a terminal of the given width and returns the lengths of the printed lines."""
    child, _ = console.start("-C", dimensions=(50, columns))
    load_modern(child)
    # build the long traversal inside a list so that it is printed rather than iterated, and keep every typed line
    # shorter than the terminal so the echo does not wrap
    send(child, "x = [g.V().out().in().out().in().out().in().out().in().out().in()]")
    lines = send(child, "x[0].has('name', 'marko').values('name').explain()")
    assert lines[0] == "==>Traversal Explanation", lines
    assert exit_console(child) == 0
    return [len(line.rstrip()) for line in lines]


def test_explain_fits_a_narrow_terminal(console):
    assert max(_explain_line_lengths(console, 100)) <= 100


def test_explain_uses_the_full_width_of_a_wide_terminal(console):
    # the same explanation is not wrapped at 100 columns when the terminal is wider
    assert max(_explain_line_lengths(console, 400)) > 100


@writes_preferences
def test_quoted_result_prompt_with_trailing_space(console):
    child, _ = console.start("-C")
    # the null result of :set itself may already use the new prompt, so its output is not checked
    send(child, ':set result.prompt "--> "')
    time.sleep(0.5)
    assert send(child, "1") == ["--> 1"]
    assert exit_console(child) == 0


@writes_preferences
def test_colors_are_rendered_when_enabled(console):
    color_prompt = r"gremlin>(?:\x1b\[[0-9;]*m)* "
    child = console.spawn()
    child.expect(color_prompt)
    child.sendline(":set vertex.color red")
    child.expect(color_prompt)
    time.sleep(0.5)
    child.sendline(MODERN_GRAPH)
    child.expect(color_prompt)
    child.sendline("g.V(1)")
    child.expect(color_prompt)
    # the vertex is wrapped in the ANSI code for red (31)
    assert re.search(r"\x1b\[(?:[0-9]+;)*31m(?:\x1b\[[0-9;]*m)*v\[1\]", child.before), repr(child.before)
    assert exit_console(child) == 0


def test_colors_are_off_with_dash_c(console):
    child, _ = console.start("-C")
    load_modern(child)
    child.sendline("g.V(1)")
    child.expect_exact(PROMPT)
    # look at the raw output, with only the line reader's terminal mode switches removed
    result_lines = [line for line in TERMINAL_MODES.sub("", child.before).split("\r\n")
                    if "v[1]" in line and "g.V(1)" not in line]
    assert result_lines == ["==>v[1]"], repr(child.before)
    assert exit_console(child) == 0


def test_banner_has_no_color_codes_with_dash_c(console):
    child, _ = console.start("-C")
    raw = child.before
    assert "\x1b[0m" not in raw and "\x1b[3" not in raw, repr(raw)
    assert "plugin activated: tinkerpop.remote" in clean_lines(raw)
    assert exit_console(child) == 0


@writes_preferences
def test_max_iteration_minus_one_removes_the_limit(console):
    child, _ = console.start("-C")
    assert send(child, ":set max-iteration -1") == ["==>null"]
    time.sleep(0.5)
    lines = send(child, "(1..500).toList()")
    assert len([line for line in lines if line.startswith("==>")]) == 500
    assert "..." not in lines
    assert exit_console(child) == 0


@writes_preferences
def test_invalid_max_iteration_falls_back_to_100(console):
    child, _ = console.start("-C")
    child.sendline(":set max-iteration abc")
    # preference changes are applied on a background thread, so the warning may come before or after the next prompt
    child.expect_exact("Unable to convert 'abc' to integer. Using default 100")
    resync(child)
    lines = send(child, "(1..101).toList()")
    assert len([line for line in lines if line.startswith("==>")]) == 100
    assert exit_console(child) == 0


def test_an_iterator_is_only_printed_once(console):
    child, _ = console.start("-C")
    assert results(child, "it = [1, 2].iterator()") == ["==>null"]
    assert results(child, "it") == ["==>1", "==>2"]
    # printing it used it up
    assert results(child, "it") == []
    assert results(child, "it.hasNext()") == ["==>false"]
    assert exit_console(child) == 0


@writes_preferences
def test_null_result_indicator_can_be_changed_or_hidden(console):
    child, _ = console.start("-C")
    # the null result of :set itself may already use the new indicator, so its output is not checked
    send(child, ":set result.indicator.null nothing")
    time.sleep(0.5)
    assert send(child, "null") == ["==>nothing"]
    send(child, ':set result.indicator.null ""')
    time.sleep(0.5)
    # an empty indicator prints no line at all
    assert send(child, "null") == []
    assert exit_console(child) == 0


def test_tokens_and_enums(console):
    child, _ = console.start("-C")
    assert results(child, "T.id") == ["==>id"]
    assert results(child, "T.label") == ["==>label"]
    assert exit_console(child) == 0


def test_profile_prints_metrics(console):
    child, _ = console.start("-C")
    load_modern(child)
    text = "\n".join(send(child, "g.V().out().profile()"))
    for expected in ("Step", "Count", "Traversers", "Time (ms)", "TinkerGraphStep", ">TOTAL"):
        assert expected in text, text
    assert exit_console(child) == 0


def test_result_prompt_from_environment_variable(console):
    child, _ = console.start("-C", env=console.with_env(GREMLIN_PROMPT="env> "))
    assert send(child, "1") == ["env> 1"]
    assert exit_console(child) == 0


def test_result_prompt_from_system_property(console):
    console.add_java_tool_option("-Dgremlin.prompt=prop>")
    child, _ = console.start("-C")
    assert send(child, "1") == ["prop>1"]
    assert exit_console(child) == 0


@writes_preferences
def test_input_prompt_can_be_changed(console):
    child, _ = console.start("-C")
    # the change is applied on a background thread, so the prompt right after :set can be the old or the new one
    child.sendline(":set input.prompt g>")
    child.expect_exact(["g> ", PROMPT])
    time.sleep(0.5)
    assert results(child, "1+1", prompt="g> ") == ["==>2"]
    # the continuation prompt is padded to the width of the new prompt
    send(child, "[1,", prompt="1> ")
    assert results(child, "2]", prompt="g> ") == ["==>1", "==>2"]
    assert exit_console(child) == 0


def test_long_string_result_is_not_cut(console):
    child, _ = console.start("-C", dimensions=(50, 400))
    lines = send(child, "'x' * 5000")
    assert "".join(line.strip() for line in lines) == "==>" + "x" * 5000
    assert exit_console(child) == 0


@writes_preferences
def test_colors_preference_false_turns_colors_off(console):
    color_prompt = r"gremlin>(?:\x1b\[[0-9;]*m)* "
    child = console.spawn()
    child.expect(color_prompt)
    child.sendline(":set vertex.color red")
    child.expect(color_prompt)
    child.sendline(":set colors false")
    child.expect(color_prompt)
    time.sleep(0.5)
    child.sendline(MODERN_GRAPH)
    child.expect(color_prompt)
    child.sendline("g.V(1)")
    child.expect(color_prompt)
    assert "\x1b[31m" not in child.before, repr(child.before)
    assert "v[1]" in child.before
    assert exit_console(child) == 0


@writes_preferences
def test_invalid_color_is_reported_and_falls_back_to_default(console):
    child, _ = console.start("-C")
    child.sendline(":set vertex.color notacolor")
    # printed by the background preference listener, before or after the next prompt
    child.expect_exact("Invalid color option for vertex.color: notacolor")
    resync(child)
    load_modern(child)
    assert results(child, "g.V(1)") == ["==>v[1]"]
    assert exit_console(child) == 0
