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
The console driven through pipes instead of a terminal.

This is how the documentation build (docs/tinkeradoc-extension, GremlinConsole.java) and bin/validate-distribution.sh
run the console: they wait for "gremlin>" on stdout, answer "Display stack trace?" on stderr, and parse "==>" lines.
Lines are sent one at a time after each prompt, as those tools do, because the stack trace question reads its answer
from the same input.
"""

from .console_helpers import MODERN_GRAPH, MODERN_GRAPH_RESULT, PROMPT, STACK_TRACE_PROMPT


def test_piped_session_prints_prompts_and_results_on_stdout(console):
    piped = console.piped()
    banner = piped.out.read_until(PROMPT)
    assert "plugin activated: tinkerpop.tinkergraph" in banner

    piped.send(MODERN_GRAPH)
    assert "==>null" in piped.out.read_until(PROMPT)
    piped.send("g")
    assert MODERN_GRAPH_RESULT in piped.out.read_until(PROMPT)
    piped.send("g.V().count()")
    assert "==>6" in piped.out.read_until(PROMPT)

    # closing stdin ends the session normally
    piped.close_input()
    assert piped.wait() == 0
    assert "==>" not in piped.err.text()


def test_piped_error_goes_to_stderr_and_the_stack_trace_question_can_be_answered(console):
    piped = console.piped()
    piped.out.read_until(PROMPT)

    piped.send("1/0")
    error = piped.err.read_until(STACK_TRACE_PROMPT)
    assert "Division by zero" in error
    assert "Type ':help' or ':h' for help." in error

    piped.send("y")
    assert "java.lang.ArithmeticException" in piped.err.read_until("java.lang.ArithmeticException")
    piped.out.read_until(PROMPT)

    # nothing about the error reached stdout, and the console keeps working
    assert "Division by zero" not in piped.out.text()
    assert STACK_TRACE_PROMPT not in piped.out.text()
    piped.send("1+1")
    assert "==>2" in piped.out.read_until(PROMPT)

    piped.close_input()
    assert piped.wait() == 0


def test_piped_multiline_statement(console):
    piped = console.piped()
    piped.out.read_until(PROMPT)
    piped.send("[1,2,")
    piped.out.read_until("......1> ")
    piped.send("3]")
    output = piped.out.read_until(PROMPT)
    for expected in ("==>1", "==>2", "==>3"):
        assert expected in output
    piped.close_input()
    assert piped.wait() == 0


def test_piped_nested_multiline_statement(console):
    # piped input brings its own closing brackets, so the console must not add any while reading it
    piped = console.piped("-C")
    piped.out.read_until(PROMPT)
    piped.send("m = [a: 1,")
    piped.send("  b: [x: 2,")
    piped.send("    y: 3]]")
    assert "==>null" in piped.out.read_until(PROMPT)
    piped.send("m.b.y")
    assert "==>3" in piped.out.read_until(PROMPT)
    piped.close_input()
    assert piped.wait() == 0
    assert "Unexpected" not in piped.err.text()


def test_piped_output_has_no_color_codes(console):
    # no -C here: colors are switched off on their own when stdout is not a terminal
    piped = console.piped()
    piped.out.read_until(PROMPT)
    piped.send("g = traversal().with(TinkerFactory.createModern()); g.V(1)")
    piped.out.read_until(PROMPT)
    piped.close_input()
    assert piped.wait() == 0
    assert "\x1b[" not in piped.out.text()


def test_execute_with_piped_stdout_has_no_color_codes(console):
    script = console.write_file("plain.groovy", "println 'plain-output'\n")
    completed = console.run("-e", script)
    assert completed.returncode == 0, completed.stderr
    assert "plain-output" in completed.stdout
    assert "\x1b[" not in completed.stdout


def test_piped_input_is_echoed_after_the_prompt(console):
    # the docs build and validate-distribution.sh read stdout, so the exact layout matters: the prompt, the echoed
    # input line, then its results, each on its own line
    # the line reader ends the echoed line with \r\n and the results with \n
    piped = console.piped("-C")
    piped.out.read_until(PROMPT)
    piped.send("1+1")
    assert piped.out.read_until(PROMPT) == "1+1\r\n==>2\n" + PROMPT
    piped.send("[1,")
    # the continuation prompt is indented by the number of open brackets
    assert piped.out.read_until("......1> ") == "[1,\r\n......1> "
    piped.send("2]")
    assert piped.out.read_until(PROMPT) == "  2]\r\n==>1\n==>2\n" + PROMPT
    piped.close_input()
    assert piped.wait() == 0


def test_validate_distribution_style_pipe(console):
    # bin/validate-distribution.sh feeds a whole script on stdin at once and greps the ==> lines
    completed = console.run_with_input(MODERN_GRAPH + "\ng.V().count()\n")
    assert completed.returncode == 0, completed.stderr
    assert [line for line in completed.stdout.splitlines() if line.startswith("==>")][-1] == "==>6"
