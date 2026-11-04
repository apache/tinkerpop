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

"""Console commands (:help, :plugin, :set, :load, groovysh commands and aliases), saved preferences and history."""

import os
import re
import time

import pexpect
import pytest

from .console_helpers import (ANSI_ESCAPE, DEFAULT_PLUGINS, EXT_DIR, PLUGINS_TXT, PROMPT, REMOTE_PLUGIN,
                              TINKERGRAPH_PLUGIN, UTILITIES_PLUGIN, answer_stack_trace_prompt, exit_console,
                              load_modern, persists_preferences, plugin_activated_lines, results, send,
                              writes_preferences)

# up arrow as the terminal sends it while the line reader has switched it to application cursor mode
UP_ARROW = "\x1bOA"
LEFT_ARROW = "\x1bOD"


def command(child, line):
    """Like send() for a console command, without the null result that every command reports when it finishes."""
    return [output_line for output_line in send(child, line) if output_line != "==>null"]


def _listed_commands(text):
    return set(re.findall(r"(?:^|\s)(/\S+)", text))


GREMLIN_COMMANDS = {"/install", "/uninstall", "/plugin", "/set"}
GROOVYSH_COMMANDS = {"/help", "/exit", "/load", "/save", "/history", "/alias", "/cls", "/clear", "/imports", "/show",
                     "/vars", "/del", "/grab", "/doc", "/inspect"}


def test_help_lists_gremlin_and_groovy_commands(console):
    child, _ = console.start("-C")
    listed = _listed_commands("\n".join(send(child, ":help")))
    assert GREMLIN_COMMANDS <= listed, listed
    assert GROOVYSH_COMMANDS <= listed, listed
    assert exit_console(child) == 0


@pytest.mark.parametrize("help_command", [":h", "/help"])
def test_help_aliases(console, help_command):
    child, _ = console.start("-C")
    assert GREMLIN_COMMANDS <= _listed_commands("\n".join(send(child, help_command)))
    assert exit_console(child) == 0


@pytest.mark.parametrize("removed", [":?", ":+", ":-", ":=", ":c", ":i java.util.concurrent.TimeUnit", ":p",
                                     ":purge preferences", ":S", ":display", ":record", ":edit", ":register x"])
def test_removed_commands_are_reported_as_syntax_errors(console, removed):
    child, _ = console.start("-C")
    child.sendline(removed)
    # a line that is not a known command is evaluated as Groovy, where a leading ":" does not compile
    child.expect_exact("Unexpected input: ':'")
    answer_stack_trace_prompt(child)
    assert results(child, "1+1") == ["==>2"]
    assert exit_console(child) == 0


def test_cls_prints_the_clear_screen_sequence(console):
    for command in (":cls", ":C", ":clear"):
        child, _ = console.start("-C")
        child.sendline(command)
        child.expect_exact("\x1b[H\x1b[2J")
        child.expect_exact("gremlin> ")
        assert exit_console(child) == 0


def test_uninstall_without_module(console):
    child, _ = console.start("-C")
    assert results(child, ":uninstall") == ["==>Specify the name of the module containing plugins to uninstall"]
    assert exit_console(child) == 0


# --- plugins --------------------------------------------------------------------------------------------------------


def test_plugin_list_marks_default_plugins_active(console):
    for command in (":plugin list", ":pin list"):
        child, _ = console.start("-C")
        listed = results(child, command)
        for name in ("tinkerpop.remote", "tinkerpop.utilities", "tinkerpop.tinkergraph"):
            assert "==>%s[active]" % name in listed, listed
        assert exit_console(child) == 0


def test_plugin_unuse_and_use_are_saved_for_the_next_start(console):
    # deactivate
    child, _ = console.start("-C")
    assert results(child, ":plugin unuse tinkerpop.utilities") == \
        ["==>tinkerpop.utilities deactivated - restart your console for this to take effect"]
    listed = results(child, ":plugin list")
    assert "==>tinkerpop.utilities" in listed, listed
    assert "==>tinkerpop.utilities[active]" not in listed, listed
    assert exit_console(child) == 0

    saved = console.read_plugins()
    assert UTILITIES_PLUGIN not in saved
    assert REMOTE_PLUGIN in saved and TINKERGRAPH_PLUGIN in saved

    # the next start no longer announces it
    child, banner = console.start("-C")
    activated = plugin_activated_lines(banner)
    assert "plugin activated: tinkerpop.utilities" not in activated, activated
    assert "plugin activated: tinkerpop.remote" in activated
    assert "plugin activated: tinkerpop.tinkergraph" in activated

    # re-activate
    assert results(child, ":plugin use tinkerpop.utilities") == ["==>tinkerpop.utilities activated"]
    assert exit_console(child) == 0
    assert UTILITIES_PLUGIN in console.read_plugins()

    child, banner = console.start("-C")
    assert "plugin activated: tinkerpop.utilities" in plugin_activated_lines(banner)
    assert exit_console(child) == 0


def test_invalid_plugin_in_plugins_txt_is_reported_and_removed(console):
    console.write_plugins(DEFAULT_PLUGINS + ["com.example.NoSuchPlugin"])
    child, banner = console.start("-C")
    assert "invalid plugin: com.example.NoSuchPlugin -- removing reference to this plugin." in \
        [line.strip() for line in banner]
    # the valid plugins still load
    assert len(plugin_activated_lines(banner)) == 3
    assert exit_console(child) == 0
    assert "com.example.NoSuchPlugin" not in console.read_plugins()


# --- preferences ----------------------------------------------------------------------------------------------------


@persists_preferences
def test_preferences_are_kept_for_the_next_start(console):
    child, _ = console.start("-C")
    # the null result of :set may already use the new prompt, so the output of these is not checked
    send(child, ':set result.prompt "R> "')
    send(child, ":set max-iteration 2")
    time.sleep(0.5)
    assert exit_console(child) == 0

    child, _ = console.start("-C")
    assert send(child, "[1, 2, 3]") == ["R> 1", "R> 2", "..."]
    assert exit_console(child) == 0


@persists_preferences
def test_preferences_saved_by_the_current_console_are_read(console):
    # a preferences file in the format java.util.prefs writes today (under the node the console uses), so a new
    # console implementation keeps reading settings that users already have
    os.makedirs(os.path.dirname(console.prefs_file))
    with open(console.prefs_file, "w") as f:
        f.write('<?xml version="1.0" encoding="UTF-8" standalone="no"?>\n'
                '<!DOCTYPE map SYSTEM "http://java.sun.com/dtd/preferences.dtd">\n'
                '<map MAP_XML_VERSION="1.0">\n'
                '  <entry key="max-iteration" value="3"/>\n'
                '  <entry key="result.prompt" value="saved&gt; "/>\n'
                '  <entry key="result.indicator.null" value="nothing"/>\n'
                '</map>\n')
    child, _ = console.start("-C")
    assert send(child, "[1, 2, 3, 4, 5]") == ["saved> 1", "saved> 2", "saved> 3", "..."]
    assert send(child, "null") == ["saved> nothing"]
    assert exit_console(child) == 0


# --- :load ----------------------------------------------------------------------------------------------------------


def test_load_runs_a_file_and_keeps_its_variables(console):
    script = console.write_file("loaded.groovy", "loadedA = 40\nloadedB = loadedA + 2\n")
    for load_command in (":load", ":l"):
        child, _ = console.start("-C")
        # the file runs as a whole and only reports that it was loaded
        assert command(child, "%s %s" % (load_command, script)) == ["Loaded: " + script]
        assert results(child, "loadedB") == ["==>42"]
        assert exit_console(child) == 0


# --- history --------------------------------------------------------------------------------------------------------


def test_history_is_saved_and_recalled_with_the_up_arrow(console):
    child, _ = console.start("-C")
    send(child, "historyValue = 41 + 1")
    assert exit_console(child) == 0

    # each entry is saved as "<timestamp>:<line>"
    with open(console.history_file, encoding="utf-8") as f:
        assert any(line.rstrip("\n").endswith(":historyValue = 41 + 1") for line in f)

    child, _ = console.start("-C")
    # the last entry is ":x", so the one before it is the assignment
    child.send(UP_ARROW + UP_ARROW)
    time.sleep(1)
    child.send("\r")
    child.expect_exact(PROMPT)
    assert results(child, "historyValue") == ["==>42"]
    assert exit_console(child) == 0


def test_history_file_from_an_earlier_version_is_replaced(console):
    # earlier versions (JLine 2) wrote one entry per line, which the new line reader does not read
    with open(console.history_file, "w", encoding="utf-8") as f:
        f.write("seededFromHistory = 99\n")
    child, _ = console.start("-C")
    child.send(UP_ARROW)
    time.sleep(1)
    child.send("\r")
    child.expect_exact(PROMPT)
    assert "==>99" not in child.before
    send(child, "41 + 1")
    assert exit_console(child) == 0

    with open(console.history_file, encoding="utf-8") as f:
        entries = [line.rstrip("\n") for line in f]
    assert not any("seededFromHistory" in entry for entry in entries), entries
    assert any(entry.endswith("41 + 1") for entry in entries), entries


# --- more Gremlin commands ------------------------------------------------------------------------------------------


def test_help_for_one_command_uses_its_properties_file(console):
    child, _ = console.start("-C")
    for help_command in (":help /plugin", ":plugin --help"):
        output = [line.strip() for line in command(child, help_command)]
        assert "Usage: plugin [use <plugin-name>|list|unuse <plugin-name>]" in output, output
        assert "Manage plugins for the Console." in output, output
    assert exit_console(child) == 0


def test_plugin_command_argument_errors(console):
    child, _ = console.start("-C")
    # "use" is the default sub-command
    assert results(child, ":plugin") == ["==>Specify the name of the plugin to use"]
    assert results(child, ":plugin use") == ["==>Specify the name of the plugin to use"]
    assert results(child, ":plugin unuse") == ["==>Specify the name of the plugin to deactivate"]
    assert send(child, ":plugin bogus") == ["Unknown function name: 'bogus'. Valid arguments: [use, list, unuse]"]
    assert exit_console(child) == 0


@pytest.mark.skipif(hasattr(os, "geteuid") and os.geteuid() == 0, reason="root can write read-only files")
def test_plugin_use_with_read_only_plugins_txt(console):
    os.chmod(PLUGINS_TXT, 0o444)
    child, _ = console.start("-C")
    output = results(child, ":plugin use tinkerpop.utilities")
    assert len(output) == 1 and output[0].startswith("==>tinkerpop.utilities activated but "), output
    assert output[0].endswith("plugins.txt is readonly - the plugin list will remain unchanged on console restart")
    assert exit_console(child) == 0


@pytest.mark.parametrize("arguments, message", [
    ("", "Group cannot be null or empty"),
    (" org.example", "Module cannot be null or empty"),
    (" org.example some-artifact", "Version cannot be null or empty")])
def test_install_with_missing_coordinates(console, arguments, message):
    # the argument check runs outside the command's own error handling, so it is reported like a script error
    child, _ = console.start("-C")
    child.sendline(":install" + arguments)
    child.expect_exact(message)
    answer_stack_trace_prompt(child)
    assert exit_console(child) == 0


def test_install_of_an_artifact_that_does_not_exist(console):
    child, _ = console.start("-C", timeout=300)
    output = results(child, ":install com.tinkerpop.nonexistent.fake no-such-artifact-xyz 9.9.9")
    assert len(output) == 1, output
    assert "unresolved dependency" in output[0] and "no-such-artifact-xyz" in output[0], output
    assert exit_console(child) == 0
    assert not os.path.exists(os.path.join(EXT_DIR, "no-such-artifact-xyz"))


def test_uninstall_removes_an_installed_module(console):
    module = os.path.join(EXT_DIR, "test-uninstall-module")
    os.makedirs(os.path.join(module, "plugin"))
    child, _ = console.start("-C")
    assert results(child, ":uninstall test-uninstall-module") == \
        ["==>Uninstalled test-uninstall-module - restart the console for removal to take effect"]
    assert exit_console(child) == 0
    assert not os.path.exists(module)


# --- more preferences -----------------------------------------------------------------------------------------------


@writes_preferences
def test_set_without_arguments_lists_preferences(console):
    child, _ = console.start("-C")
    assert command(child, ":set") == ["No preferences are set"]
    assert send(child, ":set max-iteration 7") == ["==>null"]
    # a name without a value is stored as true
    assert send(child, ":set warnings") == ["==>null"]
    time.sleep(0.5)
    listing = command(child, ":set")
    assert listing[0] == "Preferences:"
    assert sorted(line.strip() for line in listing[1:]) == ["max-iteration=7", "warnings=true"]
    assert exit_console(child) == 0


def test_set_with_unbalanced_quotes(console):
    child, _ = console.start("-C")
    child.sendline(':set result.prompt "oops')
    child.expect_exact("Missing closing \"")
    answer_stack_trace_prompt(child)
    assert results(child, "1") == ["==>1"]
    assert exit_console(child) == 0


# --- Groovy shell commands ------------------------------------------------------------------------------------------


def test_unknown_colon_command_is_a_syntax_error(console):
    child, _ = console.start("-C")
    child.sendline(":nope")
    child.expect_exact("Unexpected input: ':'")
    answer_stack_trace_prompt(child)
    assert results(child, "1+1") == ["==>2"]
    assert exit_console(child) == 0


def test_show_variables_and_imports(console):
    child, _ = console.start("-C")
    send(child, "myVar = 5")
    output = command(child, "/show")
    assert any(re.match(r"^myVar\s+5$", line) for line in output), output
    assert command(child, "/imports") == []
    send(child, "import java.util.concurrent.TimeUnit")
    assert [line.strip() for line in command(child, "/imports")] == ["import java.util.concurrent.TimeUnit"]
    assert exit_console(child) == 0


def test_del_removes_variables(console):
    child, _ = console.start("-C")
    send(child, "myVar = 5")
    send(child, "otherVar = 6")
    assert send(child, "/del myVar") == ["==>null"]
    child.sendline("myVar")
    child.expect_exact("No such property: myVar")
    answer_stack_trace_prompt(child)
    assert results(child, "otherVar") == ["==>6"]
    assert send(child, "/del *") == ["==>null"]
    child.sendline("otherVar")
    child.expect_exact("No such property: otherVar")
    answer_stack_trace_prompt(child)
    assert exit_console(child) == 0


def _history_entries(output):
    """Returns the numbered entries of a /history listing as (number, text) pairs, oldest first."""
    matches = [re.match(r"^\s*(\d+)\s+(.*)$", line) for line in output]
    return [(int(match.group(1)), match.group(2)) for match in matches if match]


def test_history_show_lists_numbered_entries(console):
    child, _ = console.start("-C")
    send(child, "firstEntry = 1")
    send(child, "secondEntry = 2")
    for history_command in (":history", ":H"):
        entries = [text for _, text in _history_entries(command(child, history_command))]
        # oldest first, ending with the command itself
        assert entries.index("firstEntry = 1") < entries.index("secondEntry = 2"), entries
        assert entries[-1] == history_command, entries
    assert exit_console(child) == 0


def test_history_recall_runs_an_earlier_entry(console):
    child, _ = console.start("-C")
    assert results(child, "40 + 2") == ["==>42"]
    number = [number for number, text in _history_entries(command(child, ":history")) if text == "40 + 2"][0]
    # the command reports its own null result as well as that of the entry it runs
    assert "==>42" in results(child, "/history -s %d" % number)
    assert exit_console(child) == 0


def test_alias_creates_a_new_command(console):
    child, _ = console.start("-C")
    assert send(child, ":alias helpme /help") == ["==>null"]
    assert GREMLIN_COMMANDS <= _listed_commands("\n".join(send(child, "helpme")))
    assert send(child, ":alias two 1+1") == ["==>null"]
    assert results(child, "two") == ["==>2"]
    assert exit_console(child) == 0


def test_grab_inspect_and_doc_commands(console):
    child, _ = console.start("-C")
    child.sendline(":grab nonsense")
    child.expect_exact("Invalid command parameter: nonsense")
    answer_stack_trace_prompt(child)
    send(child, "inspectMe = 1")
    # a variable is passed to a command with a leading $
    output = [line.strip() for line in command(child, ":inspect $inspectMe")]
    assert "public final class Integer" in output, output
    assert send(child, ":doc") == ["==>null"]
    assert exit_console(child) == 0


def test_register_with_an_unknown_class(console):
    child, _ = console.start("-C")
    child.sendline(":register com.example.NoSuchCommand")
    child.expect_exact("com.example.NoSuchCommand")
    answer_stack_trace_prompt(child)
    assert exit_console(child) == 0


# --- line editing and completion ------------------------------------------------------------------------------------


def _tab(child, text):
    """Types text, presses Tab and returns what the terminal shows afterwards, then clears the line."""
    child.send(text + "\t")
    time.sleep(2)
    shown = ""
    try:
        child.expect(r"(?s).+", timeout=3)
        shown = child.before + child.after
    except pexpect.TIMEOUT:
        pass
    child.send("\x15\r")
    child.expect_exact(PROMPT)
    return shown


def test_tab_completes_command_names(console):
    child, _ = console.start("-C")
    # a command typed with ":" completes to its "/" name
    assert ANSI_ESCAPE.sub("", _tab(child, ":plu")).endswith("/plugin ")
    assert ANSI_ESCAPE.sub("", _tab(child, ":unin")).endswith("/uninstall ")
    # with more than one match the candidates are listed
    shown = ANSI_ESCAPE.sub("", _tab(child, ":se"))
    assert "/set" in shown and "/setopt" in shown, repr(shown)
    assert exit_console(child) == 0


def test_tab_completes_preference_names(console):
    child, _ = console.start("-C")
    assert _tab(child, ":set max").endswith("max-iteration ")
    assert exit_console(child) == 0


def test_tab_completes_variables_classes_and_keywords(console):
    child, _ = console.start("-C")
    send(child, "myVariable = 1")
    assert _tab(child, "myVar").endswith("myVariable")
    shown = ANSI_ESCAPE.sub("", _tab(child, "Strin"))
    assert "Classes" in shown and "StringWriter" in shown, repr(shown)
    assert "import" in _tab(child, "impo")
    assert exit_console(child) == 0


def test_tab_completes_method_names(console):
    child, _ = console.start("-C")
    load_modern(child)
    shown = ANSI_ESCAPE.sub("", _tab(child, "g.V().ou"))
    assert "outE" in shown and "outV" in shown, repr(shown)
    assert exit_console(child) == 0


def test_line_editing_keys(console):
    child, _ = console.start("-C")
    # Ctrl-A moves to the start of the line
    child.send("+2")
    child.send("\x01")
    child.send("1\r")
    child.expect_exact("==>3")
    child.expect_exact(PROMPT)
    # the left arrow moves back inside the line (sent in application cursor mode, see UP_ARROW)
    child.send("1+2")
    child.send(LEFT_ARROW + LEFT_ARROW)
    child.send("0\r")
    child.expect_exact("==>12")
    child.expect_exact(PROMPT)
    # Ctrl-E moves to the end again
    child.send("1+")
    child.send("\x01\x05")
    child.send("5\r")
    child.expect_exact("==>6")
    child.expect_exact(PROMPT)
    assert exit_console(child) == 0
