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
import pytest
import tempfile

MULTIPLE_INTERACTIVE = [("y", "6"), ("x", "2"), ("z", "argument=\\[x, -i, =, --color, -D\\]")]
MULTIPLE_EXECUTE = ["6", "2", "argument=\\[x, -e, =, --color, -D\\]"]



class TestConsole(object):

    # the base command must pass -C because if colors are enabled pexpect gets garbled input and tests won't pass
    gremlinsh = "bash gremlin-console/bin/gremlin.sh -C "

    # How long any expect() waits for output before failing. An output mismatch costs exactly this
    # much, so it stays bounded: at 120s a handful of stale assertions was enough to push the
    # gremlin-console CI job past its timeout. 60s still leaves ample headroom for a cold JVM start.
    EXPECT_TIMEOUT = 60

    # Parent of the per-console user.home directories (see _spawn); removed when the test process exits.
    _homes = tempfile.TemporaryDirectory(prefix="gremlin-console-test-")

    @pytest.mark.parametrize("args,results", [
        ("-i x.script", [("x", "2")]),
        ("--interactive x.script", [("x", "2")]),
        ("-i y.script 1 2 3", [("y", "6")]),
        ("--interactive y.script 1 2 3", [("y", "6")]),
        ("--interactive=\"y.script 1 2 3\"", [("y", "6")]),
        ("-i y.script 1 2 3 -i x.script -i \"z.script x -i = --color -D\"", MULTIPLE_INTERACTIVE),
        ("--interactive y.script 1 2 3 --interactive x.script --interactive \"z.script x -i = --color -D\"",
         MULTIPLE_INTERACTIVE),
        ("--interactive y.script 1 2 3 -i x.script --interactive \"z.script x -i = --color -D\"", MULTIPLE_INTERACTIVE),
        ("--interactive y.script 1 2 3 --interactive x.script -i \"z.script x -i = --color -D\"", MULTIPLE_INTERACTIVE),
    ])
    def test_interactive_options(self, args, results):
        child = TestConsole._spawn(args)
        TestConsole._expect_gremlin_header(child)
        for variable, value in results:
            TestConsole._send(child, variable)
            child.expect("==>" + value + "\r\n")
            TestConsole._expect_prompt(child)
        TestConsole._close(child)

    @pytest.mark.parametrize("args,printed", [
        ("-e x-printed.script", ["2"]),
        ("--execute x-printed.script", ["2"]),
        ("-e y-printed.script 1 2 3", ["6"]),
        ("--execute y-printed.script 1 2 3", ["6"]),
        ("-e y-printed.script 1 2 3 -e x-printed.script -e \"z-printed.script x -e = --color -D\"", MULTIPLE_EXECUTE),
        ("--execute y-printed.script 1 2 3 --execute x-printed.script --execute \"z-printed.script x -e = --color -D\"",
         MULTIPLE_EXECUTE),
        ("--execute y-printed.script 1 2 3 -e x-printed.script --execute \"z-printed.script x -e = --color -D\"",
         MULTIPLE_EXECUTE),
        ("-l DEBUG --execute y-printed.script 1 2 3", ["6"]),
        # the short option takes everything after it, so "=" becomes part of the file name
        ("-e=x-printed.script", ["Gremlin file not found at \\[=x-printed.script\\]\\."]),
    ])
    def test_execute_options(self, args, printed):
        child = TestConsole._spawn(args)
        for line in printed:
            child.expect(line + "\r\n")
        TestConsole._close(child)

    def test_error_handling_failure_step(self):
        child = TestConsole._spawn()
        TestConsole._expect_gremlin_header(child)
        TestConsole._setup_modern_graph(child)

        # the fail() step raises a Failure which is rendered with the dedicated Failure formatter
        # (note: unlike other errors, this path does not prompt for a stack trace)
        child.sendline("g.inject(1).fail('boomfailure123').toList()")
        child.expect_exact("fail() Step Triggered")
        child.expect_exact("boomfailure123")
        TestConsole._expect_prompt(child)

        # a traversal that is not iterated on the line fails while the shell checks its result, which reports only
        # the exception
        child.sendline("g.inject(1).fail('boomfailure456')")
        child.expect_exact("FailException: boomfailure456")
        TestConsole._expect_prompt(child)

        TestConsole._close(child)

    @staticmethod
    def _spawn(args=""):
        # Every console gets its own user.home so the state it persists there (java.util.prefs, which
        # backs :set, and the history file) can't leak into later consoles - e.g. a :set max-iteration
        # would otherwise truncate the results of every test that runs after it. The JVM ignores the
        # HOME environment variable for user.home, so it is passed as a system property instead.
        # dimensions=(50, 400): the wide terminal stops the pty from wrapping long input lines, which
        # would otherwise garble pexpect echo/output matching.
        home = tempfile.mkdtemp(dir=TestConsole._homes.name)
        java_options = (os.environ.get("JAVA_OPTIONS", "") + " -Duser.home=" + home).strip()
        return pexpect.spawn(TestConsole.gremlinsh + args, dimensions=(50, 400), timeout=TestConsole.EXPECT_TIMEOUT,
                             env=dict(os.environ, JAVA_OPTIONS=java_options))

    @staticmethod
    def _setup_modern_graph(child):
        # builds an in-memory TinkerGraph and an embedded traversal source, no server required
        # an assignment reports a null result, so check each variable on its own line
        child.sendline("graph = org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory.createModern()")
        TestConsole._expect_prompt(child)
        child.sendline("graph")
        child.expect_exact("==>tinkermemorygraph[vertices:6 edges:6]")
        TestConsole._expect_prompt(child)
        child.sendline("g = traversal().withEmbedded(graph)")
        TestConsole._expect_prompt(child)
        child.sendline("g")
        child.expect_exact("==>graphtraversalsource[tinkermemorygraph[vertices:6 edges:6], standard]")
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
        # the line reader ends the echoed line with \r\n, which the terminal shows as \r\r\n
        child.expect(line + "\r+\n")

    @staticmethod
    def _expect_prompt(child):
        child.expect("gremlin> ")

    @staticmethod
    def _close(child):
        # an -e script ends the console by itself, so it may already be gone. Either way wait for the exit so a
        # console that ignores :x fails the test, and force the pty closed so no console process outlives its test.
        try:
            if child.isalive():
                try:
                    child.sendline(":x")
                except OSError:
                    pass
            child.expect(pexpect.EOF, timeout=10)
        finally:
            child.close(force=True)
