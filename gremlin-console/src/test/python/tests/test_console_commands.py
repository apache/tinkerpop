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
Console commands where they need a real terminal or the console's own process: what is saved for the next start
(plugins, preferences, history), the ext/plugins.txt of the distribution, line editing and completion. The rest is
covered by ConsoleCommandsSessionTest.
"""

import os
import time

import pexpect
import pytest

from .console_helpers import (ANSI_ESCAPE, DEFAULT_PLUGINS, PLUGINS_TXT, PROMPT, REMOTE_PLUGIN, TINKERGRAPH_PLUGIN,
                              UTILITIES_PLUGIN, exit_console, load_modern, persists_preferences,
                              plugin_activated_lines, results, send)

# up arrow as the terminal sends it while the line reader has switched it to application cursor mode
UP_ARROW = "\x1bOA"
LEFT_ARROW = "\x1bOD"


def test_clear_prints_the_clear_screen_sequence(console):
    # groovysh's own :clear asks the terminal how to clear the screen, which a real terminal can answer
    child, _ = console.start("-C")
    child.sendline(":clear")
    child.expect_exact("\x1b[H\x1b[2J")
    child.expect_exact("gremlin> ")
    assert exit_console(child) == 0


# --- plugins --------------------------------------------------------------------------------------------------------


def _plugin_states(child):
    """Returns the :plugin list as a map from plugin name to its state."""
    states = {}
    for line in results(child, ":plugin list"):
        name, state = line[len("==>"):].rstrip("]").split("[", 1)
        states[name] = state
    return states


def test_only_the_plugins_in_plugins_txt_are_activated(console):
    # sugar and credentials are on the classpath but not in the plugins.txt written by the fixture
    child, banner = console.start("-C")
    assert len(plugin_activated_lines(banner)) == 3, banner
    states = _plugin_states(child)
    assert states["tinkerpop.sugar"] == "inactive", states
    assert states["tinkerpop.credentials"] == "inactive", states
    assert states["tinkerpop.remote"] == "active", states
    assert exit_console(child) == 0
    # a start does not rewrite the list
    assert console.read_plugins() == DEFAULT_PLUGINS


def test_plugin_unuse_and_use_are_saved_for_the_next_start(console):
    # deactivate
    child, _ = console.start("-C")
    assert results(child, ":plugin unuse tinkerpop.utilities") == \
        ["==>tinkerpop.utilities deactivated - restart your console for this to take effect"]
    assert _plugin_states(child)["tinkerpop.utilities"] == "active until restart"
    assert exit_console(child) == 0

    saved = console.read_plugins()
    assert UTILITIES_PLUGIN not in saved
    assert REMOTE_PLUGIN in saved and TINKERGRAPH_PLUGIN in saved

    # the next start no longer activates it
    child, banner = console.start("-C")
    activated = plugin_activated_lines(banner)
    assert "plugin activated: tinkerpop.utilities" not in activated, activated
    assert "plugin activated: tinkerpop.remote" in activated
    assert "plugin activated: tinkerpop.tinkergraph" in activated
    assert _plugin_states(child)["tinkerpop.utilities"] == "inactive"

    # re-activate
    assert results(child, ":plugin use tinkerpop.utilities") == ["==>tinkerpop.utilities activated"]
    assert _plugin_states(child)["tinkerpop.utilities"] == "active"
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


@pytest.mark.skipif(hasattr(os, "geteuid") and os.geteuid() == 0, reason="root can write read-only files")
def test_plugin_use_with_read_only_plugins_txt(console):
    os.chmod(PLUGINS_TXT, 0o444)
    child, _ = console.start("-C")
    output = results(child, ":plugin use tinkerpop.utilities")
    assert len(output) == 1 and output[0].startswith("==>tinkerpop.utilities activated but "), output
    assert output[0].endswith("plugins.txt is readonly - the plugin list will remain unchanged on console restart")
    assert exit_console(child) == 0


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
