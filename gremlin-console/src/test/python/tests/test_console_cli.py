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

"""Command line options, the startup banner and exit codes."""

import re

import pytest  # noqa: F401 (also used by the parametrize markers below)

from .console_helpers import PROMPT, UTILITIES_PLUGIN, clean_lines, exit_console, plugin_activated_lines, results, \
    wait_for_exit


def test_help_prints_usage_and_exits_zero(console):
    for option in ("-h", "--help"):
        completed = console.run(option)
        assert completed.returncode == 0, completed.stderr
        for expected in ("gremlin.sh", "--help", "--version", "--quiet", "--verbose", "--debug", "--interactive",
                         "--execute", "--color", "Display this help message"):
            assert expected in completed.stdout, "missing %r in usage for %s:\n%s" % (expected, option,
                                                                                       completed.stdout)
        # the console itself never starts
        assert "oOOo" not in completed.stdout


def test_version_prints_version_and_exits_zero(console):
    for option in ("-v", "--version"):
        completed = console.run(option)
        assert completed.returncode == 0, completed.stderr
        assert re.search(r"^gremlin \d+\.\d+\.\d+\S*$", completed.stdout, re.MULTILINE), completed.stdout
        assert "oOOo" not in completed.stdout


def test_unknown_option_exits_22(console):
    completed = console.run("--bogus")
    assert completed.returncode == 22, completed.stdout + completed.stderr
    assert "--bogus" in completed.stderr
    assert "oOOo" not in completed.stdout


def test_default_banner(console):
    child, banner = console.start("-C")
    lines = [line for line in banner if not line.startswith("Picked up JAVA_TOOL_OPTIONS")]

    art = lines.index("         \\,,,/")
    assert lines[art + 1] == "         (o o)"
    assert re.match(r"^-----oOOo-\(\d+\)-oOOo-----$", lines[art + 2]), lines[art + 2]

    # one line per plugin listed in ext/plugins.txt, in file order
    assert plugin_activated_lines(lines) == ["plugin activated: tinkerpop.remote",
                                             "plugin activated: tinkerpop.utilities",
                                             "plugin activated: tinkerpop.tinkergraph"]
    assert exit_console(child) == 0


def test_quiet_hides_banner_but_keeps_prompt_and_results(console):
    for option in ("-Q", "--quiet"):
        child, banner = console.start("-C", option)
        text = "\n".join(banner)
        assert "oOOo" not in text
        assert "plugin activated" not in text
        assert results(child, "1+1") == ["==>2"]
        assert exit_console(child) == 0


def test_execute_success_exits_zero_and_is_quiet(console):
    script = console.write_file("ok.groovy", "x = 1 + 1\nprintln x\n")
    for option in ("-e", "--execute"):
        completed = console.run(option, script)
        assert completed.returncode == 0, completed.stderr
        stdout_lines = [line.strip() for line in completed.stdout.splitlines()]
        assert "2" in stdout_lines, completed.stdout
        # -e implies quiet: no banner, no plugin messages, no prompt, no result lines
        assert "oOOo" not in completed.stdout
        assert "plugin activated" not in completed.stdout
        assert PROMPT not in completed.stdout
        assert "==>" not in completed.stdout


def test_execute_script_error_exits_one(console):
    script = console.write_file("boom.groovy", "x = 1\nthrow new RuntimeException('boom123')\nprintln 'unreachable'\n")
    completed = console.run("-e", script)
    assert completed.returncode == 1, completed.stdout + completed.stderr
    # the failing line is reported with its line number and the exception message, followed by the stack trace
    assert "Error in %s at [2: throw new RuntimeException('boom123')] - boom123" % script in completed.stderr
    assert "java.lang.RuntimeException: boom123" in completed.stderr
    # errors go to stderr only, and the script stops at the failing line
    assert "boom123" not in completed.stdout
    assert "unreachable" not in completed.stdout


def test_execute_missing_file_exits_one(console):
    missing = console.home + "/does-not-exist.groovy"
    completed = console.run("-e", missing)
    assert completed.returncode == 1, completed.stdout + completed.stderr
    assert "Gremlin file not found at [%s]." % missing in completed.stderr


def test_execute_relative_script_from_another_working_directory(console):
    # the console process runs from its own home directory, so a relative path has to be resolved against the
    # directory gremlin.sh was started from (bin/validate-distribution.sh checks this too)
    console.write_file("work/relative.groovy", "println 'relative-ok'\n")
    completed = console.run("-e", "relative.groovy", cwd=console.home + "/work")
    assert completed.returncode == 0, completed.stderr
    assert "relative-ok" in [line.strip() for line in completed.stdout.splitlines()], completed.stdout


def test_execute_absolute_script_path(console):
    console.write_file("abs.groovy", "println 'absolute-ok'\n")
    completed = console.run("-e", console.home + "/abs.groovy", cwd="/")
    assert completed.returncode == 0, completed.stderr
    assert "absolute-ok" in completed.stdout


def test_interactive_script_with_exit_command_exits_zero(console):
    script = console.write_file("leave.groovy", "x = 1\n:x\n")
    child = console.spawn("-C", "-i", script)
    assert wait_for_exit(child) == 0
    assert PROMPT not in "\n".join(clean_lines(child.before or ""))


@pytest.mark.parametrize("option", ["-V", "--verbose", "-D", "--debug"])
def test_verbose_and_debug_options_start_the_console(console, option):
    child, banner = console.start("-C", option)
    assert "oOOo" in "\n".join(banner)
    assert results(child, "1+1") == ["==>2"]
    assert exit_console(child) == 0


def test_interactive_and_execute_together_is_rejected(console):
    script = console.write_file("x.groovy", "x = 1\n")
    completed = console.run("-i", script, "-e", script)
    assert "-i and -e options are mutually exclusive - provide one or the other" in completed.stdout
    # surprising but current behavior: the rejected command line still exits with 0
    assert completed.returncode == 0
    assert "oOOo" not in completed.stdout


def test_execute_long_option_with_equals(console):
    script = console.write_file("eq.groovy", "println 'equals-ok'\n")
    completed = console.run("--execute=" + script)
    assert completed.returncode == 0, completed.stderr
    assert "equals-ok" in completed.stdout


def test_execute_without_arguments_binds_empty_args(console):
    script = console.write_file("noargs.groovy", "println 'args=' + args + ' size=' + args.size()\n")
    completed = console.run("-e", script)
    assert completed.returncode == 0, completed.stderr
    assert "args=[] size=0" in completed.stdout


def test_execute_with_arguments_binds_them_as_strings(console):
    script = console.write_file("args.groovy", "println args.collect { it.getClass().simpleName + ':' + it }\n")
    completed = console.run("-e", script, "1", "two")
    assert completed.returncode == 0, completed.stderr
    assert "[String:1, String:two]" in completed.stdout


def test_execute_script_runs_console_commands(console):
    script = console.write_file("commands.groovy", "\n".join([
        ':set result.prompt "a b> "',
        ":set result.indicator.null 'no thing'",
        ':set   max-iteration   7',
        'x = [1,',
        '  2]',
        '  :pin unuse tinkerpop.utilities',
        ':set input.prompt "say \\"hi\\""',
        "prefs = java.util.prefs.Preferences.userRoot().node('/org/apache/tinkerpop/gremlin/console')",
        "println(['result.prompt', 'result.indicator.null', 'max-iteration', 'input.prompt'].collect { prefs.get(it, null) })",
        'println x',
    ]) + "\n")
    completed = console.run("-e", script)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert '[a b> , no thing, 7, say "hi"]' in completed.stdout
    assert "[1, 2]" in completed.stdout
    assert UTILITIES_PLUGIN not in console.read_plugins()


def test_execute_script_with_a_bad_console_command_exits_one(console):
    script = console.write_file("bad-command.groovy", 'x = 1\n:set result.prompt "oops\nprintln "unreachable"\n')
    completed = console.run("-e", script)
    assert completed.returncode == 1, completed.stdout + completed.stderr
    assert 'Error in %s at [2: :set result.prompt "oops] - Missing closing "' % script in completed.stderr
    assert "unreachable" not in completed.stdout


def test_only_the_last_scripts_args_remain(console):
    first = console.write_file("first.groovy", "firstArgs = args\n")
    second = console.write_file("second.groovy", "println 'first=' + firstArgs + ' last=' + args\n")
    completed = console.run("-e", first, "a", "b", "-e", second, "c")
    assert completed.returncode == 0, completed.stderr
    assert "first=[a, b] last=[c]" in completed.stdout


def test_system_exit_in_a_script_sets_the_exit_code(console):
    script = console.write_file("exit3.groovy", "println 'before'\nSystem.exit(3)\nprintln 'after'\n")
    completed = console.run("-e", script)
    assert completed.returncode == 3
    assert "before" in completed.stdout
    assert "after" not in completed.stdout


def test_relative_script_from_another_directory_with_java_options_set(console):
    # gremlin.sh adds its own -D options after JAVA_OPTIONS; they must survive when JAVA_OPTIONS is set
    console.write_file("work/rel-opts.groovy", "println 'relative-with-options-ok'\n")
    completed = console.run("-e", "rel-opts.groovy", cwd=console.home + "/work",
                            env=console.with_env(JAVA_OPTIONS="-Xmx512m"))
    assert completed.returncode == 0, completed.stderr
    assert "relative-with-options-ok" in completed.stdout


def test_java_options_are_passed_to_the_jvm(console):
    script = console.write_file("prop.groovy", "println 'prop=' + System.getProperty('gremlin.test.prop')\n")
    completed = console.run("-e", script, env=console.with_env(JAVA_OPTIONS="-Dgremlin.test.prop=hello"))
    assert completed.returncode == 0, completed.stderr
    assert "prop=hello" in completed.stdout
