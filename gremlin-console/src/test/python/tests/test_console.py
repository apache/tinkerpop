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

import pexpect
import os
import tempfile


class TestConsole(object):

    # the base command must pass -C because if colors are enabled pexpect gets garbled input and tests won't pass
    gremlinsh = "bash gremlin-console/bin/gremlin.sh -C "

    def test_basic_console_operations(self):
        child = pexpect.spawn(TestConsole.gremlinsh)
        TestConsole._expect_gremlin_header(child)
        TestConsole._send(child, "1-1")
        child.expect("==>0\r\n")
        TestConsole._expect_prompt(child)
        TestConsole._close(child)

    def test_just_dash_i(self):
        child = pexpect.spawn(TestConsole.gremlinsh + "-i x.script")
        TestConsole._expect_gremlin_header(child)
        TestConsole._send(child, "x")
        child.expect("==>2\r\n")
        TestConsole._expect_prompt(child)
        TestConsole._close(child)

    def test_just_dash_dash_interactive(self):
        child = pexpect.spawn(TestConsole.gremlinsh + "--interactive x.script")
        TestConsole._expect_gremlin_header(child)
        TestConsole._send(child, "x")
        child.expect("==>2\r\n")
        TestConsole._expect_prompt(child)
        TestConsole._close(child)

    def test_dash_i_with_args(self):
        child = pexpect.spawn(TestConsole.gremlinsh + "-i y.script 1 2 3")
        TestConsole._expect_gremlin_header(child)
        TestConsole._send(child, "y")
        child.expect("==>6\r\n")
        TestConsole._expect_prompt(child)
        TestConsole._close(child)

    def test_dash_dash_interactive_with_args(self):
        child = pexpect.spawn(TestConsole.gremlinsh + "--interactive y.script 1 2 3")
        TestConsole._expect_gremlin_header(child)
        TestConsole._send(child, "y")
        child.expect("==>6\r\n")
        TestConsole._expect_prompt(child)
        TestConsole._close(child)

    def test_dash_dash_interactive_with_args_and_equals(self):
        child = pexpect.spawn(TestConsole.gremlinsh + "--interactive=\"y.script 1 2 3\"")
        TestConsole._expect_gremlin_header(child)
        TestConsole._send(child, "y")
        child.expect("==>6\r\n")
        TestConsole._expect_prompt(child)
        TestConsole._close(child)

    def test_dash_i_multiple_scripts(self):
        child = pexpect.spawn(TestConsole.gremlinsh + "-i y.script 1 2 3 -i x.script -i \"z.script x -i = --color -D\"")
        TestConsole._expect_gremlin_header(child)
        TestConsole._send(child, "y")
        child.expect("==>6\r\n")
        TestConsole._expect_prompt(child)
        TestConsole._send(child, "x")
        child.expect("==>2\r\n")
        TestConsole._expect_prompt(child)
        TestConsole._send(child, "z")
        child.expect("==>argument=\\[x, -i, =, --color, -D\\]\r\n")
        TestConsole._expect_prompt(child)
        TestConsole._close(child)

    def test_dash_dash_interactive_multiple_scripts(self):
        child = pexpect.spawn(TestConsole.gremlinsh + "--interactive y.script 1 2 3 --interactive x.script -i \"z.script x -i = --color -D\"")
        TestConsole._expect_gremlin_header(child)
        TestConsole._send(child, "y")
        child.expect("==>6\r\n")
        TestConsole._expect_prompt(child)
        TestConsole._send(child, "x")
        child.expect("==>2\r\n")
        TestConsole._expect_prompt(child)
        TestConsole._send(child, "z")
        child.expect("==>argument=\\[x, -i, =, --color, -D\\]\r\n")
        TestConsole._expect_prompt(child)
        TestConsole._close(child)

    def test_mixed_interactive_long_short_opts_with_multiple_scripts(self):
        child = pexpect.spawn(TestConsole.gremlinsh + "--interactive y.script 1 2 3 --interactive x.script -i \"z.script x -i = --color -D\"")
        TestConsole._expect_gremlin_header(child)
        TestConsole._send(child, "y")
        child.expect("==>6\r\n")
        TestConsole._expect_prompt(child)
        TestConsole._send(child, "x")
        child.expect("==>2\r\n")
        TestConsole._expect_prompt(child)
        TestConsole._send(child, "z")
        child.expect("==>argument=\\[x, -i, =, --color, -D\\]\r\n")
        TestConsole._expect_prompt(child)
        TestConsole._close(child)

    def test_just_dash_e(self):
        child = pexpect.spawn(TestConsole.gremlinsh + "-e x-printed.script")
        child.expect("2\r\n")
        TestConsole._close(child)

    def test_just_dash_e_file_not_found(self):
        child = pexpect.spawn(TestConsole.gremlinsh + "-e=x-printed.script")
        child.expect("Gremlin file not found at \\[=x-printed.script\\]\\.\r\n")
        child.expect(pexpect.EOF)

    def test_just_dash_dash_execute(self):
        child = pexpect.spawn(TestConsole.gremlinsh + "--execute x-printed.script")
        child.expect("2\r\n")
        TestConsole._close(child)

    def test_dash_e_with_args(self):
        child = pexpect.spawn(TestConsole.gremlinsh + "-e y-printed.script 1 2 3")
        child.expect("6\r\n")
        TestConsole._close(child)

    def test_dash_dash_execute_with_args(self):
        child = pexpect.spawn(TestConsole.gremlinsh + "--execute y-printed.script 1 2 3")
        child.expect("6\r\n")
        TestConsole._close(child)

    def test_dash_e_multiple_scripts(self):
        child = pexpect.spawn(TestConsole.gremlinsh + "-e y-printed.script 1 2 3 -e x-printed.script -e \"z-printed.script x -e = --color -D\"")
        child.expect("6\r\n")
        child.expect("2\r\n")
        child.expect("argument=\\[x, -e, =, --color, -D\\]\r\n")
        TestConsole._close(child)

    def test_dash_dash_execute_multiple_scripts(self):
        child = pexpect.spawn(TestConsole.gremlinsh + "--execute y-printed.script 1 2 3 --execute x-printed.script --execute \"z-printed.script x -e = --color -D\"")
        child.expect("6\r\n")
        child.expect("2\r\n")
        child.expect("argument=\\[x, -e, =, --color, -D\\]\r\n")
        TestConsole._close(child)

    def test_mixed_execute_long_short_opts_with_multiple_scripts(self):
        child = pexpect.spawn(TestConsole.gremlinsh + "--execute y-printed.script 1 2 3 -e x-printed.script --execute \"z-printed.script x -e = --color -D\"")
        child.expect("6\r\n")
        child.expect("2\r\n")
        child.expect("argument=\\[x, -e, =, --color, -D\\]\r\n")
        TestConsole._close(child)

    def test_no_mix_dash_i_and_dash_e(self):
        child = pexpect.spawn(TestConsole.gremlinsh + "-i y.script 1 2 3 -i x.script -e \"z.script x -i --color -D\"")
        child.expect("-i and -e options are mutually exclusive - provide one or the other")
        child.expect(pexpect.EOF)

    def test_debug_logging(self):
        child = pexpect.spawn(TestConsole.gremlinsh + "-l DEBUG --execute y-printed.script 1 2 3")
        child.expect("6\r\n")
        TestConsole._close(child)

    # dimensions=(50, 400): the wide terminal stops the pty from wrapping long
    # input lines, which would otherwise garble pexpect echo/output matching.
    def test_result_rendering_of_various_types(self):
        child = pexpect.spawn(TestConsole.gremlinsh, dimensions=(50, 400), timeout=120)
        TestConsole._expect_gremlin_header(child)
        TestConsole._setup_modern_graph(child)

        # number result
        child.sendline("g.V().count()")
        child.expect_exact("==>6")
        TestConsole._expect_prompt(child)

        # list of strings - iterated one result per line, assert on the first
        child.sendline("g.V().values('name')")
        child.expect_exact("==>marko")
        TestConsole._expect_prompt(child)

        # map result iterated one entry per line
        child.sendline("g.V().has('name','marko').valueMap().next()")
        child.expect_exact("==>name=[marko]")
        TestConsole._expect_prompt(child)

        # vertex result
        child.sendline("g.V(1).next()")
        child.expect_exact("==>v[1]")
        TestConsole._expect_prompt(child)

        # null indicator for a null result
        child.sendline("null")
        child.expect_exact("==>null")
        TestConsole._expect_prompt(child)

        TestConsole._close(child)

    def test_error_handling_runtime_exception(self):
        child = pexpect.spawn(TestConsole.gremlinsh, dimensions=(50, 400), timeout=120)
        TestConsole._expect_gremlin_header(child)

        # a runtime exception should be reported and then the interactive
        # console prompts to display the stack trace
        child.sendline("1/0")
        child.expect_exact("Division by zero")
        child.expect_exact("Display stack trace? [yN]")
        child.sendline("n")
        TestConsole._expect_prompt(child)

        TestConsole._close(child)

    def test_error_handling_missing_property(self):
        child = pexpect.spawn(TestConsole.gremlinsh, dimensions=(50, 400), timeout=120)
        TestConsole._expect_gremlin_header(child)

        child.sendline("undefinedVariable123")
        child.expect_exact("No such property")
        child.expect_exact("Display stack trace? [yN]")
        child.sendline("n")
        TestConsole._expect_prompt(child)

        TestConsole._close(child)

    def test_command_help(self):
        child = pexpect.spawn(TestConsole.gremlinsh, dimensions=(50, 400), timeout=120)
        TestConsole._expect_gremlin_header(child)

        child.sendline(":help")
        # the help listing enumerates the available commands
        child.expect_exact(":help")
        child.expect_exact(":quit")
        TestConsole._expect_prompt(child)

        TestConsole._close(child)

    def test_command_clear(self):
        child = pexpect.spawn(TestConsole.gremlinsh, dimensions=(50, 400), timeout=120)
        TestConsole._expect_gremlin_header(child)

        # :clear resets the buffer and returns to the prompt without error
        child.sendline(":clear")
        TestConsole._expect_prompt(child)
        # confirm the console is still usable afterwards
        child.sendline("1+1")
        child.expect_exact("==>2")
        TestConsole._expect_prompt(child)

        TestConsole._close(child)

    def test_set_max_iteration_limits_results(self):
        child = pexpect.spawn(TestConsole.gremlinsh, dimensions=(50, 400), timeout=120)
        TestConsole._expect_gremlin_header(child)

        child.sendline(":set max-iteration 5")
        TestConsole._expect_prompt(child)

        # a result larger than the limit should be truncated with an ellipsis
        child.sendline("(1..100).toList()")
        child.expect_exact("==>5")
        child.expect_exact("...")
        TestConsole._expect_prompt(child)

        TestConsole._close(child)

    def test_set_with_too_many_arguments_errors(self):
        child = pexpect.spawn(TestConsole.gremlinsh, dimensions=(50, 400), timeout=120)
        TestConsole._expect_gremlin_header(child)

        # :set accepts at most <name> [<value>]; three args is too many and is rejected
        # with the arg-form error message
        child.sendline(":set foo bar baz")
        child.expect_exact("requires arguments: <name> [<value>]")
        TestConsole._expect_prompt(child)

        TestConsole._close(child)

    def test_plugin_list(self):
        child = pexpect.spawn(TestConsole.gremlinsh, dimensions=(50, 400), timeout=120)
        TestConsole._expect_gremlin_header(child)

        child.sendline(":plugin list")
        child.expect_exact("tinkerpop.tinkergraph")
        TestConsole._expect_prompt(child)

        TestConsole._close(child)

    def test_multiline_input(self):
        child = pexpect.spawn(TestConsole.gremlinsh, dimensions=(50, 400), timeout=120)
        TestConsole._expect_gremlin_header(child)

        # an incomplete expression continues on the next line; completing it
        # yields the full result
        child.sendline("[1,2,")
        child.sendline("3]")
        child.expect_exact("==>1")
        child.expect_exact("==>2")
        child.expect_exact("==>3")
        TestConsole._expect_prompt(child)

        TestConsole._close(child)

    def test_error_handling_display_stack_trace_when_answering_yes(self):
        child = pexpect.spawn(TestConsole.gremlinsh, dimensions=(50, 400), timeout=120)
        TestConsole._expect_gremlin_header(child)

        # answering 'y' to the stack trace prompt drives the branch that prints the full trace
        child.sendline("1/0")
        child.expect_exact("Division by zero")
        child.expect_exact("Display stack trace? [yN]")
        child.sendline("y")
        # the printed trace begins with the fully qualified exception class name
        child.expect_exact("java.lang.ArithmeticException")
        TestConsole._expect_prompt(child)

        TestConsole._close(child)

    def test_error_handling_failure_step(self):
        child = pexpect.spawn(TestConsole.gremlinsh, dimensions=(50, 400), timeout=120)
        TestConsole._expect_gremlin_header(child)
        TestConsole._setup_modern_graph(child)

        # the fail() step raises a Failure which is rendered with the dedicated Failure formatter
        # (note: unlike other errors, this path does not prompt for a stack trace)
        child.sendline("g.inject(1).fail('boomfailure123')")
        child.expect_exact("fail() Step Triggered")
        child.expect_exact("boomfailure123")
        TestConsole._expect_prompt(child)

        TestConsole._close(child)

    def test_result_rendering_path(self):
        child = pexpect.spawn(TestConsole.gremlinsh, dimensions=(50, 400), timeout=120)
        TestConsole._expect_gremlin_header(child)
        TestConsole._setup_modern_graph(child)

        # a path result is an Iterable that is colorized element by element
        child.sendline("g.V(1).out().path()")
        child.expect_exact("[v[1]")
        TestConsole._expect_prompt(child)

        TestConsole._close(child)

    def test_result_rendering_edge(self):
        child = pexpect.spawn(TestConsole.gremlinsh, dimensions=(50, 400), timeout=120)
        TestConsole._expect_gremlin_header(child)
        TestConsole._setup_modern_graph(child)

        # edges are rendered through the dedicated Edge branch of the colorizer
        child.sendline("g.E().hasLabel('knows')")
        child.expect_exact("-knows->")
        TestConsole._expect_prompt(child)

        TestConsole._close(child)

    def test_result_rendering_map_with_tokens(self):
        child = pexpect.spawn(TestConsole.gremlinsh, dimensions=(50, 400), timeout=120)
        TestConsole._expect_gremlin_header(child)
        TestConsole._setup_modern_graph(child)

        # folding the element map yields a list containing a Map, so the Map is colorized as a whole
        # (exercising the Map branch and the T-token key rendering of the colorizer)
        child.sendline("g.V(1).elementMap().fold()")
        child.expect_exact("label:person")
        TestConsole._expect_prompt(child)

        TestConsole._close(child)

    def test_result_rendering_explain(self):
        child = pexpect.spawn(TestConsole.gremlinsh, dimensions=(50, 400), timeout=120)
        TestConsole._expect_gremlin_header(child)
        TestConsole._setup_modern_graph(child)

        # explain() returns a TraversalExplanation that is pretty printed via its own rendering branch
        child.sendline("g.V().out().explain()")
        child.expect_exact("Traversal Explanation")
        TestConsole._expect_prompt(child)

        TestConsole._close(child)

    def test_result_rendering_object_array(self):
        child = pexpect.spawn(TestConsole.gremlinsh, dimensions=(50, 400), timeout=120)
        TestConsole._expect_gremlin_header(child)

        # an Object[] is iterated one element per line through the array branch of the result handler
        child.sendline("[1, 2, 3].toArray()")
        child.expect_exact("==>1")
        child.expect_exact("==>2")
        child.expect_exact("==>3")
        TestConsole._expect_prompt(child)

        TestConsole._close(child)

    def test_result_rendering_nested_lists(self):
        child = pexpect.spawn(TestConsole.gremlinsh, dimensions=(50, 400), timeout=120)
        TestConsole._expect_gremlin_header(child)

        # each element of the outer list is itself an Iterable, exercising the recursive colorizer
        child.sendline("[[1, 2], [3, 4]]")
        child.expect_exact("==>[1,2]")
        child.expect_exact("==>[3,4]")
        TestConsole._expect_prompt(child)

        TestConsole._close(child)

    def test_result_rendering_large_number_and_boolean(self):
        child = pexpect.spawn(TestConsole.gremlinsh, dimensions=(50, 400), timeout=120)
        TestConsole._expect_gremlin_header(child)

        # large numbers are rendered through the Number branch
        child.sendline("123456789012345")
        child.expect_exact("==>123456789012345")
        TestConsole._expect_prompt(child)

        # a boolean falls through to the default toString branch
        child.sendline("1 < 2")
        child.expect_exact("==>true")
        TestConsole._expect_prompt(child)

        TestConsole._close(child)

    def test_plugin_use_activates_available_plugin(self):
        child = pexpect.spawn(TestConsole.gremlinsh, dimensions=(50, 400), timeout=120)
        TestConsole._expect_gremlin_header(child)

        # activating an already available plugin exercises the plugin activation closures without any network
        child.sendline(":plugin use tinkerpop.tinkergraph")
        child.expect_exact("tinkerpop.tinkergraph activated")
        TestConsole._expect_prompt(child)

        TestConsole._close(child)

    def test_plugin_use_unknown_plugin(self):
        child = pexpect.spawn(TestConsole.gremlinsh, dimensions=(50, 400), timeout=120)
        TestConsole._expect_gremlin_header(child)

        child.sendline(":plugin use does.not.exist")
        child.expect_exact("could not be found")
        TestConsole._expect_prompt(child)

        TestConsole._close(child)

    def test_uninstall_absent_module(self):
        child = pexpect.spawn(TestConsole.gremlinsh, dimensions=(50, 400), timeout=120)
        TestConsole._expect_gremlin_header(child)

        # uninstalling a module that is not present reports a not-found message (no network access)
        child.sendline(":uninstall com.example.nonexistent")
        child.expect_exact("There is no module with the name")
        TestConsole._expect_prompt(child)

        TestConsole._close(child)

    def test_uninstall_without_argument(self):
        child = pexpect.spawn(TestConsole.gremlinsh, dimensions=(50, 400), timeout=120)
        TestConsole._expect_gremlin_header(child)

        child.sendline(":uninstall")
        child.expect_exact("Specify the name of the module")
        TestConsole._expect_prompt(child)

        TestConsole._close(child)

    def test_interactive_script_with_error_keeps_console_open(self):
        # -i executes the script and, when a line errors, reports it and leaves the console open
        script = TestConsole._write_error_script()
        try:
            child = pexpect.spawn(TestConsole.gremlinsh + "-i " + script, dimensions=(50, 400), timeout=120)
            child.expect_exact("Error in " + script)
            TestConsole._expect_prompt(child)
            # the console remains usable after the script error
            child.sendline("1+1")
            child.expect_exact("==>2")
            TestConsole._expect_prompt(child)
            TestConsole._close(child)
        finally:
            os.remove(script)

    def test_execute_script_with_error_closes_console(self):
        # -e executes the script and, on error, reports it and closes the console
        script = TestConsole._write_error_script()
        try:
            child = pexpect.spawn(TestConsole.gremlinsh + "-e " + script, dimensions=(50, 400), timeout=120)
            child.expect_exact("Error in " + script)
            child.expect(pexpect.EOF)
        finally:
            os.remove(script)

    @staticmethod
    def _write_error_script():
        # generate a throwaway Gremlin script that errors, rather than committing a fixture file
        fd, path = tempfile.mkstemp(suffix=".script")
        with os.fdopen(fd, "w") as f:
            f.write("throw new RuntimeException(\'scripterror123\')\n")
        return path

    @staticmethod
    def _setup_modern_graph(child):
        # builds an in-memory TinkerGraph and an embedded traversal source, no server required
        child.sendline("graph = org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory.createModern()")
        child.expect_exact("==>tinkergraph[vertices:6 edges:6]")
        TestConsole._expect_prompt(child)
        child.sendline("g = traversal().withEmbedded(graph)")
        child.expect_exact("==>graphtraversalsource[tinkergraph[vertices:6 edges:6], standard]")
        TestConsole._expect_prompt(child)

    @staticmethod
    def _expect_gremlin_header(child):
        # skip/read the Gremlin graphics
        child.expect("\r\n")
        child.expect(["plugin activated: tinkerpop.remote", "plugin activated: tinkerpop.utilities", "plugin activated: tinkerpop.tinkergraph"])
        child.expect(["plugin activated: tinkerpop.remote", "plugin activated: tinkerpop.utilities", "plugin activated: tinkerpop.tinkergraph"])
        child.expect(["plugin activated: tinkerpop.remote", "plugin activated: tinkerpop.utilities", "plugin activated: tinkerpop.tinkergraph"])
        TestConsole._expect_prompt(child)

    @staticmethod
    def _send(child, line):
        child.sendline(line)
        child.expect(line + "\r\n")

    @staticmethod
    def _expect_prompt(child):
        child.expect("gremlin> ")

    @staticmethod
    def _close(child):
        child.sendline(":x")
