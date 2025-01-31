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
