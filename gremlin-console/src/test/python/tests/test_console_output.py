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

"""
How results are printed where it needs a real terminal or the console's own process: colors and the GREMLIN_PROMPT
environment variable. The rest is covered by ConsoleOutputSessionTest.
"""

import re
import time

from .console_helpers import (MODERN_GRAPH, PROMPT, TERMINAL_MODES, clean_lines, exit_console, load_modern, send,
                              writes_preferences)


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


def test_result_prompt_from_environment_variable(console):
    child, _ = console.start("-C", env=console.with_env(GREMLIN_PROMPT="env> "))
    assert send(child, "1") == ["env> 1"]
    assert exit_console(child) == 0
