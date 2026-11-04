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
Shared helpers for the black-box Gremlin Console tests.

These tests start bin/gremlin.sh as a separate process and only look at what it prints and how it exits, so they
do not depend on how the console is implemented (groovysh today, something else after the Groovy upgrade).

Each test gets its own throwaway home directory through JAVA_TOOL_OPTIONS=-Duser.home=..., which isolates the
history file (~/.gremlin_groovy_history), Grape and, on Linux, the java.util.prefs store used by :set. The JVM
prints a "Picked up JAVA_TOOL_OPTIONS" line on stderr at startup because of this; tests ignore it.
"""

import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time

import pexpect
import pytest

# the tests run from gremlin-console/src/test/python with the console distribution mounted at ./gremlin-console
# (see the console-integration-tests profile in gremlin-console/pom.xml)
CONSOLE_HOME = os.path.abspath("gremlin-console")
GREMLIN_SH = os.path.join(CONSOLE_HOME, "bin", "gremlin.sh")
PLUGINS_TXT = os.path.join(CONSOLE_HOME, "ext", "plugins.txt")

PROMPT = "gremlin> "
STACK_TRACE_PROMPT = "Display stack trace? [yN]"
DEFAULT_TIMEOUT = 120

REMOTE_PLUGIN = "org.apache.tinkerpop.gremlin.console.jsr223.RemoteGremlinPlugin"
UTILITIES_PLUGIN = "org.apache.tinkerpop.gremlin.console.jsr223.UtilitiesGremlinPlugin"
TINKERGRAPH_PLUGIN = "org.apache.tinkerpop.gremlin.tinkergraph.jsr223.TinkerGraphGremlinPlugin"
DEFAULT_PLUGINS = [REMOTE_PLUGIN, UTILITIES_PLUGIN, TINKERGRAPH_PLUGIN]

MODERN_GRAPH = "g = traversal().with(TinkerFactory.createModern())"
MODERN_GRAPH_RESULT = "==>graphtraversalsource[tinkermemorygraph[vertices:6 edges:6], standard]"

ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]|\x1b[()][A-Za-z0-9]|\x1b[=>]")

# the line reader switches terminal modes (keypad, mouse, bracketed paste) around each statement it runs
TERMINAL_MODES = re.compile(r"\x1b\[\?[0-9;]*[hl]|\x1b[=>]")

EXT_DIR = os.path.join(CONSOLE_HOME, "ext")

# java.util.prefs only follows user.home on Linux, where it is file based. On other platforms (macOS keeps preferences
# in a plist under the real home and has no working file based store) the console is pointed at the module's
# in-memory store used by the JUnit tests, when it has been compiled, so preference changes stay inside the process.
IS_LINUX = sys.platform.startswith("linux")
_IN_MEMORY_PREFS_DIR = os.path.abspath(os.path.join("..", "..", "..", "target", "test-classes", "org", "apache",
                                                    "tinkerpop", "gremlin", "console"))
_IN_MEMORY_PREFS_CLASS = "org.apache.tinkerpop.gremlin.console.InMemoryPreferencesFactory"
IN_MEMORY_PREFS_AVAILABLE = os.path.isfile(os.path.join(_IN_MEMORY_PREFS_DIR, "InMemoryPreferencesFactory.class"))

# changes preferences during a single console session
writes_preferences = pytest.mark.skipif(
    not IS_LINUX and not IN_MEMORY_PREFS_AVAILABLE,
    reason="preferences can only be isolated per test on Linux, or with the module's compiled test classes")

# needs preferences to survive a console restart, which the in-memory store cannot do
persists_preferences = pytest.mark.skipif(
    not IS_LINUX,
    reason="preferences are only file based and isolated per test on Linux")


class ConsoleEnv(object):
    """An isolated environment for starting the console: a fresh home directory and a known ext/plugins.txt."""

    def __init__(self):
        # tempfile rather than pytest's tmp_path: gremlin.sh greps "java -version" output for the word "version", and
        # the JVM echoes JAVA_TOOL_OPTIONS (which contains this path) into that output, so the path must not contain it
        # the fixed suffix keeps the JVM's "Picked up JAVA_TOOL_OPTIONS: ...-Duser.home=<path>" line from ending in a
        # digit, which loose patterns such as "2\r\n" in test_console.py could otherwise match
        self.home = tempfile.mkdtemp(prefix="gremlin-console-", suffix="-home")
        # pre-create the java.util.prefs directory so the JVM does not log "Created user preferences directory."
        os.makedirs(os.path.join(self.home, ".java", ".userPrefs"))

        self.env = dict(os.environ)
        # these change console behavior and must come from the test, not the developer's shell
        for name in ("JAVA_OPTIONS", "GREMLIN_PROMPT", "GREMLIN_LOG_LEVEL", "SCRIPT_DEBUG", "CLASSPATH", "EDITOR",
                     "BROWSER", "GROOVYSH_BROWSER"):
            self.env.pop(name, None)
        self.java_tool_options = ["-Duser.home=" + self.home]
        if not IS_LINUX and IN_MEMORY_PREFS_AVAILABLE:
            # copy only the factory classes: the rest of test-classes (e.g. logback-test.xml) would change the console
            classes = os.path.join(self.home, "test-classes")
            target = os.path.join(classes, "org", "apache", "tinkerpop", "gremlin", "console")
            os.makedirs(target)
            for name in os.listdir(_IN_MEMORY_PREFS_DIR):
                if name.startswith("InMemoryPreferencesFactory"):
                    shutil.copy(os.path.join(_IN_MEMORY_PREFS_DIR, name), target)
            self.env["CLASSPATH"] = classes
            self.java_tool_options.append("-Djava.util.prefs.PreferencesFactory=" + _IN_MEMORY_PREFS_CLASS)
        self.env["JAVA_TOOL_OPTIONS"] = " ".join(self.java_tool_options)
        self.env["TERM"] = "xterm"
        self.env.setdefault("LANG", "C.UTF-8")

        with open(PLUGINS_TXT) as f:
            self._original_plugins = f.read()
        self._original_ext = set(os.listdir(EXT_DIR))
        self.write_plugins(DEFAULT_PLUGINS)

        self._children = []

    @property
    def history_file(self):
        return os.path.join(self.home, ".gremlin_groovy_history")

    @property
    def prefs_file(self):
        return os.path.join(self.home, ".java", ".userPrefs", "org", "apache", "tinkerpop", "gremlin", "console",
                            "prefs.xml")

    def write_file(self, name, content):
        """Writes a file in the test's home directory and returns its absolute path."""
        path = os.path.join(self.home, name)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        return path

    def write_plugins(self, class_names):
        """Writes ext/plugins.txt, one class name per line (an empty string gives a blank line)."""
        with open(PLUGINS_TXT, "w") as f:
            f.write("".join(name + "\n" for name in class_names))

    def with_env(self, **overrides):
        """Returns a copy of the environment with the given variables set (or removed when the value is None)."""
        env = dict(self.env)
        for name, value in overrides.items():
            if value is None:
                env.pop(name, None)
            else:
                env[name] = value
        return env

    def add_java_tool_option(self, option):
        self.java_tool_options.append(option)
        self.env["JAVA_TOOL_OPTIONS"] = " ".join(self.java_tool_options)

    def read_plugins(self):
        with open(PLUGINS_TXT) as f:
            return [line.strip() for line in f if line.strip()]

    def spawn(self, *args, dimensions=(50, 400), timeout=DEFAULT_TIMEOUT, env=None):
        """Starts the console in a pseudo-terminal, as a user typing into a real terminal would."""
        child = pexpect.spawn("bash", [GREMLIN_SH] + list(args), env=env or self.env, dimensions=dimensions,
                              timeout=timeout, encoding="utf-8", codec_errors="replace",
                              preexec_fn=_receive_ctrl_c)
        self._children.append(child)
        return child

    def start(self, *args, **kwargs):
        """Starts the console in a pseudo-terminal and waits for the first prompt. Returns (child, banner_lines)."""
        child = self.spawn(*args, **kwargs)
        child.expect_exact(PROMPT)
        return child, clean_lines(child.before)

    def run(self, *args, cwd=None, timeout=DEFAULT_TIMEOUT, env=None):
        """Runs the console to completion with plain pipes (no terminal) and returns the CompletedProcess."""
        env = env or self.env
        return subprocess.run(["bash", GREMLIN_SH] + list(args), stdin=subprocess.DEVNULL, capture_output=True,
                              env=env, cwd=cwd, timeout=timeout, encoding="utf-8", errors="replace")

    def run_with_input(self, text, *args, timeout=DEFAULT_TIMEOUT):
        """Runs the console with the given text on stdin, all at once, and returns the CompletedProcess."""
        return subprocess.run(["bash", GREMLIN_SH] + list(args), input=text, capture_output=True, env=self.env,
                              timeout=timeout, encoding="utf-8", errors="replace")

    def piped(self, *args):
        """Starts the console with stdin, stdout and stderr each on its own pipe, as the docs build does."""
        console = PipedConsole(self, list(args))
        self._children.append(console)
        return console

    def close(self):
        for child in self._children:
            try:
                if child.isalive():
                    child.terminate(force=True)
            except Exception:
                pass
        # a test may have made the file read-only, so unlock it before restoring it
        os.chmod(PLUGINS_TXT, 0o644)
        with open(PLUGINS_TXT, "w") as f:
            f.write(self._original_plugins)
        # remove anything a test added to ext/ (e.g. a module created for :uninstall)
        for name in set(os.listdir(EXT_DIR)) - self._original_ext:
            path = os.path.join(EXT_DIR, name)
            if os.path.isdir(path):
                shutil.rmtree(path, ignore_errors=True)
            else:
                os.remove(path)
        shutil.rmtree(self.home, ignore_errors=True)


def _receive_ctrl_c():
    # a process started in the background (e.g. "mvn ... &" or nohup) ignores SIGINT and passes that on to its
    # children, so the console would never see the ctrl+c the tests send
    signal.signal(signal.SIGINT, signal.SIG_DFL)


class _StreamCollector(object):
    """Reads one output stream of a process on a background thread so it can be searched without blocking."""

    def __init__(self, stream):
        self._stream = stream
        self._buffer = b""
        self._position = 0
        self._eof = False
        self._condition = threading.Condition()
        thread = threading.Thread(target=self._pump, daemon=True)
        thread.start()

    def _pump(self):
        while True:
            chunk = self._stream.read(4096)
            with self._condition:
                if not chunk:
                    self._eof = True
                    self._condition.notify_all()
                    return
                self._buffer += chunk
                self._condition.notify_all()

    def read_until(self, text, timeout=DEFAULT_TIMEOUT):
        """Waits for text to appear after the last match and returns everything up to and including it."""
        needle = text.encode("utf-8")
        deadline = time.monotonic() + timeout
        with self._condition:
            while True:
                index = self._buffer.find(needle, self._position)
                if index >= 0:
                    end = index + len(needle)
                    chunk = self._buffer[self._position:end]
                    self._position = end
                    return chunk.decode("utf-8", "replace")
                remaining = deadline - time.monotonic()
                if self._eof or remaining <= 0:
                    raise AssertionError("did not see %r; stream so far ends with: %r" % (text, self.text()[-2000:]))
                self._condition.wait(remaining)

    def wait_for_eof(self, timeout=DEFAULT_TIMEOUT):
        deadline = time.monotonic() + timeout
        with self._condition:
            while not self._eof and time.monotonic() < deadline:
                self._condition.wait(deadline - time.monotonic())

    def text(self):
        with self._condition:
            return self._buffer.decode("utf-8", "replace")


class PipedConsole(object):
    """A console whose stdin, stdout and stderr are separate pipes, so the two output streams can be checked apart."""

    def __init__(self, console_env, args):
        self.process = subprocess.Popen(["bash", GREMLIN_SH] + args, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE, env=console_env.env, bufsize=0)
        self.out = _StreamCollector(self.process.stdout)
        self.err = _StreamCollector(self.process.stderr)

    def send(self, line):
        self.process.stdin.write((line + "\n").encode("utf-8"))
        self.process.stdin.flush()

    def close_input(self):
        self.process.stdin.close()

    def wait(self, timeout=DEFAULT_TIMEOUT):
        code = self.process.wait(timeout=timeout)
        self.out.wait_for_eof(timeout=10)
        self.err.wait_for_eof(timeout=10)
        return code

    def isalive(self):
        return self.process.poll() is None

    def terminate(self, force=True):
        self.process.kill()


def clean_lines(text):
    """
    Strips ANSI escape codes and backspaces, normalizes line endings and splits into lines. The line reader echoes
    typed text with cursor moves and backspaces, e.g. when it closes a bracket for the user.
    """
    text = ANSI_ESCAPE.sub("", text).replace("\x08", "").replace("\r\n", "\n").replace("\r", "\n")
    return text.split("\n")


def send(child, line, prompt=PROMPT):
    """
    Types one line, waits for the next prompt and returns the non-empty output lines printed in between, without the
    terminal's echo of the typed line.
    """
    child.sendline(line)
    child.expect_exact(prompt)
    lines = clean_lines(child.before)
    for index, output_line in enumerate(lines):
        if output_line.rstrip().endswith(line.strip()):
            lines = lines[index + 1:]
            break
    return [output_line for output_line in lines if output_line.strip() != ""]


def results(child, line, prompt=PROMPT):
    """Like send() but only returns the result lines, i.e. those starting with the default result prompt ==>."""
    return [output_line for output_line in send(child, line, prompt) if output_line.startswith("==>")]


def answer_stack_trace_prompt(child, answer="n"):
    child.expect_exact(STACK_TRACE_PROMPT)
    child.sendline(answer)
    child.expect_exact(PROMPT)


def resync(child):
    """
    Waits until the console has handled everything typed so far. Use it after output that may be printed before or
    after a prompt, such as the warnings of the background preference listener, and after ctrl+c at the prompt, which
    now and then prints a second prompt.
    """
    child.sendline("'resync'")
    child.expect_exact("==>resync")
    child.expect_exact(PROMPT)


def load_modern(child):
    # an assignment reports a null result, so check the variable on its own line
    send(child, MODERN_GRAPH)
    assert results(child, "g") == [MODERN_GRAPH_RESULT]


def exit_console(child, command=":x"):
    """Sends an exit command, waits for the process to end and returns its exit code."""
    child.sendline(command)
    child.expect(pexpect.EOF)
    child.close()
    return child.exitstatus


def wait_for_exit(child):
    child.expect(pexpect.EOF)
    child.close()
    return child.exitstatus


def plugin_activated_lines(lines):
    return [line.strip() for line in lines if line.strip().startswith("plugin activated: ")]
