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
How typed Groovy is evaluated where it needs a real terminal or the console's own process: multi-line input as it is
typed, Ctrl-C and scripts run with -e. The rest is covered by ConsoleEvalSessionTest.
"""

import time

from .console_helpers import (PROMPT, STACK_TRACE_PROMPT, exit_console, results, resync, send, writes_preferences)

CONTINUATION_1 = "......1> "
CONTINUATION_2 = "......2> "


# a real terminal shows the continuation prompt and closes brackets as they are typed, unlike piped input


def test_continuation_prompt_numbers_each_extra_line(console):
    child, _ = console.start("-C")
    # the continuation prompt is the line number padded with dots to the width of "gremlin>"
    send(child, "[1,2,", prompt=CONTINUATION_1)
    send(child, "3,", prompt=CONTINUATION_2)
    assert results(child, "4]") == ["==>1", "==>2", "==>3", "==>4"]
    assert exit_console(child) == 0


def test_multiline_closure_and_map(console):
    child, _ = console.start("-C")
    send(child, "add3 = { a ->", prompt=CONTINUATION_1)
    send(child, "  a + 3", prompt=CONTINUATION_2)
    send(child, "}")
    assert results(child, "add3(1)") == ["==>4"]

    send(child, "m = [a: 1,", prompt=CONTINUATION_1)
    assert results(child, "b: 2]") == ["==>null"]
    assert results(child, "m") == ["==>a=1", "==>b=2"]
    assert exit_console(child) == 0


def test_multiline_statement_in_execute_script(console):
    # script files are fed to the console one line at a time, so statements spanning lines must still work
    # (the closure is not called "sum": that name is taken by the statically imported Operator.sum)
    script = console.write_file("multi.groovy",
                                "adder = { a, b ->\n"
                                "    a + b\n"
                                "}\n"
                                "total = [1,\n"
                                "         2,\n"
                                "         3].sum()\n"
                                "println adder(2, 3)\n"
                                "println total\n")
    completed = console.run("-e", script)
    assert completed.returncode == 0, completed.stderr
    stdout_lines = [line.strip() for line in completed.stdout.splitlines()]
    assert stdout_lines[-2:] == ["5", "6"], completed.stdout


def test_execute_script_syntax_error_reports_the_line_and_exits_one(console):
    # a mistake more lines cannot fix is reported where it is, instead of swallowing the rest of the script
    script = console.write_file("syntax.groovy", "println 'a'\nx = = 1\nprintln 'b'\n")
    completed = console.run("-e", script)
    assert completed.returncode == 1, completed.stdout
    assert "Error in %s at [2: x = = 1]" % script in completed.stderr, completed.stderr
    assert "b" not in completed.stdout.splitlines(), completed.stdout


def test_execute_script_ending_in_an_unfinished_statement_is_reported(console):
    script = console.write_file("unfinished.groovy", "println 'a'\ng.V().\n")
    completed = console.run("-e", script)
    assert completed.returncode == 1, completed.stdout
    assert "Error in %s at [2: g.V().] - the statement is not finished at the end of the file" % script in \
        completed.stderr, completed.stderr


# --- Ctrl-C ---------------------------------------------------------------------------------------------------------


def _interrupt_and_recover(child):
    """Presses Ctrl-C and waits until the console is back at its prompt."""
    child.sendintr()
    child.expect_exact("Execution interrupted by ctrl+c", timeout=30)
    # an interrupted evaluation surfaces as an InterruptedException, which goes through the normal error handling
    # and its stack trace question; an interrupted result listing returns straight to the prompt
    if child.expect_exact([STACK_TRACE_PROMPT, PROMPT], timeout=30) == 0:
        child.sendline("n")
        child.expect_exact(PROMPT, timeout=30)
    resync(child)


def test_ctrl_c_discards_an_unfinished_statement(console):
    child, _ = console.start("-C")
    send(child, "[1,2,", prompt=CONTINUATION_1)
    child.sendintr()
    child.expect_exact(PROMPT)
    resync(child)
    # the unfinished list is gone, so this is evaluated on its own
    assert results(child, "1+1") == ["==>2"]
    assert exit_console(child) == 0


def test_ctrl_c_interrupts_an_endless_loop(console):
    child, _ = console.start("-C")
    child.sendline("while (true) {}")
    time.sleep(3)
    _interrupt_and_recover(child)
    assert child.isalive()
    assert exit_console(child) == 0


def test_console_is_usable_after_ctrl_c_interrupts_a_loop(console):
    child, _ = console.start("-C")
    child.sendline("while (true) {}")
    time.sleep(3)
    _interrupt_and_recover(child)
    child.sendline("1+1")
    assert child.expect_exact(["==>2", STACK_TRACE_PROMPT], timeout=30) == 0
    child.expect_exact(PROMPT)
    assert exit_console(child) == 0


def test_ctrl_c_interrupts_a_sleep(console):
    child, _ = console.start("-C")
    child.sendline("Thread.sleep(60000)")
    time.sleep(3)
    _interrupt_and_recover(child)
    assert results(child, "1+1") == ["==>2"]
    assert exit_console(child) == 0


@writes_preferences
def test_ctrl_c_interrupts_printing_of_an_endless_result(console):
    child, _ = console.start("-C")
    # remove the result limit so the listing would never end on its own
    assert send(child, ":set max-iteration -1") == ["==>null"]
    time.sleep(0.5)
    child.sendline("[hasNext: { true }, next: { 1 }] as Iterator")
    for _ in range(50):
        child.expect_exact("==>1")
    _interrupt_and_recover(child)
    # where ctrl+c lands depends on timing, so only check that the console survives
    assert child.isalive()
    assert exit_console(child) == 0


def test_ctrl_c_discards_a_half_typed_line(console):
    child, _ = console.start("-C")
    child.send("1+")
    time.sleep(1)
    child.sendintr()
    # ctrl+c drops the typed text and shows a fresh prompt
    child.expect_exact(PROMPT)
    resync(child)
    assert results(child, "1") == ["==>1"]
    assert exit_console(child) == 0


def test_ctrl_c_at_an_idle_prompt_does_not_exit(console):
    child, _ = console.start("-C")
    child.sendintr()
    # ctrl+c shows a fresh prompt
    child.expect_exact(PROMPT)
    resync(child)
    assert child.isalive()
    assert results(child, "1+1") == ["==>2"]
    assert exit_console(child) == 0
