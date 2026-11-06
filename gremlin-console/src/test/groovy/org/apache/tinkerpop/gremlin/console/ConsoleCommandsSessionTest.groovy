/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.console

import org.junit.Test

import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertFalse
import static org.junit.Assert.assertTrue

/**
 * The console commands: :help, :plugin, :set, :load, :install, the groovysh commands and aliases, and ending the
 * session.
 */
class ConsoleCommandsSessionTest extends AbstractConsoleSessionTest {

    private static final Set<String> GREMLIN_COMMANDS = ["/install", "/uninstall", "/plugin", "/set"] as Set
    private static final Set<String> GROOVYSH_COMMANDS = ["/help", "/exit", "/load", "/save", "/history", "/alias",
                                                          "/cls", "/clear", "/imports", "/show", "/vars", "/del",
                                                          "/grab", "/doc", "/inspect"] as Set

    private static Set<String> listedCommands(final List<String> lines) {
        return (lines.join("\n") =~ /(?:^|\s)(\/\S+)/).collect { it[1] } as Set
    }

    @Test
    void shouldListTheGremlinAndGroovyCommandsInHelp() {
        final ConsoleSession console = start()
        for (String help : [":help", ":h", "/help"]) {
            final Set<String> listed = listedCommands(console.command(help))
            assertTrue("$help: $listed".toString(), listed.containsAll(GREMLIN_COMMANDS))
            assertTrue("$help: $listed".toString(), listed.containsAll(GROOVYSH_COMMANDS))
        }
    }

    @Test
    void shouldReportRemovedCommandsAsSyntaxErrors() {
        final ConsoleSession console = start()
        // a line that is not a known command is evaluated as Groovy, where a leading ":" does not compile
        for (String removed : [":?", ":+", ":-", ":=", ":c", ":i java.util.concurrent.TimeUnit", ":p",
                               ":purge preferences", ":S", ":display", ":record", ":edit", ":register x", ":nope"]) {
            console.fails(removed, "Unexpected input: ':'")
            assertEquals(removed, ["==>2"], console.results("1+1"))
        }
    }

    @Test
    void shouldReportRegisterWithAnUnknownClass() {
        final ConsoleSession console = start()
        console.fails(":register com.example.NoSuchCommand", "com.example.NoSuchCommand")
    }

    @Test
    void shouldPrintTheClearScreenSequenceForCls() {
        final ConsoleSession console = start()
        for (String cls : [":cls", ":C"]) {
            final int mark = console.out.length()
            console.send(cls)
            assertTrue(cls, console.out.text().substring(mark).contains("\u001b[H\u001b[2J"))
        }
    }

    @Test
    void shouldEndTheSessionWithAnExitCommand() {
        for (String exit : [":x", ":exit", ":quit", ":q"]) {
            final ConsoleSession console = start()
            console.type(exit)
            assertEquals(exit, 0, console.waitForExit())
        }
    }

    @Test
    void shouldEndTheSessionAtTheEndOfInput() {
        assertEquals(0, start().close())
    }

    // --- plugins ----------------------------------------------------------------------------------------------------

    @Test
    void shouldMarkTheDefaultPluginsActiveInTheList() {
        final ConsoleSession console = start()
        for (String list : [":plugin list", ":pin list"]) {
            final List<String> listed = console.results(list)
            for (String name : ["tinkerpop.remote", "tinkerpop.utilities", "tinkerpop.tinkergraph"])
                assertTrue("$list: $listed".toString(), listed.contains("==>$name[active]".toString()))
        }
    }

    @Test
    void shouldShowTheHelpOfOneCommand() {
        final ConsoleSession console = start()
        for (String help : [":help /plugin", ":plugin --help"]) {
            final List<String> output = console.command(help).collect { it.trim() }
            assertTrue(output.toString(), output.contains("Usage: plugin [use <plugin-name>|list|unuse <plugin-name>]"))
            assertTrue(output.toString(), output.contains("Manage plugins for the Console."))
        }
    }

    @Test
    void shouldReportPluginCommandArgumentErrors() {
        final ConsoleSession console = start()
        // "use" is the default sub-command
        assertEquals(["==>Specify the name of the plugin to use"], console.results(":plugin"))
        assertEquals(["==>Specify the name of the plugin to use"], console.results(":plugin use"))
        assertEquals(["==>Specify the name of the plugin to deactivate"], console.results(":plugin unuse"))
        assertEquals(["Unknown function name: 'bogus'. Valid arguments: [use, list, unuse]"], console.command(":plugin bogus"))
    }

    @Test
    void shouldReportMissingInstallCoordinates() {
        final ConsoleSession console = start()
        // the argument check runs outside the command's own error handling, so it is reported like a script error
        console.fails(":install", "Group cannot be null or empty")
        console.fails(":install org.example", "Module cannot be null or empty")
        console.fails(":install org.example some-artifact", "Version cannot be null or empty")
    }

    @Test
    void shouldReportAnArtifactThatDoesNotExist() {
        final ConsoleSession console = start()
        final List<String> output = console.results(":install com.tinkerpop.nonexistent.fake no-such-artifact-xyz 9.9.9")
        assertEquals(output.toString(), 1, output.size())
        assertTrue(output[0], output[0].contains("unresolved dependency") && output[0].contains("no-such-artifact-xyz"))
        assertFalse(new File(ConsoleFs.CONSOLE_HOME_DIR, "no-such-artifact-xyz").exists())
    }

    @Test
    void shouldReportUninstallWithoutAModule() {
        assertEquals(["==>Specify the name of the module containing plugins to uninstall"], start().results(":uninstall"))
    }

    @Test
    void shouldUninstallAnInstalledModule() {
        final File module = new File(ConsoleFs.CONSOLE_HOME_DIR, "test-uninstall-module")
        new File(module, "plugin").mkdirs()
        try {
            assertEquals(["==>Uninstalled test-uninstall-module - restart the console for removal to take effect"],
                    start().results(":uninstall test-uninstall-module"))
            assertFalse(module.exists())
        } finally {
            module.deleteDir()
        }
    }

    // --- preferences ------------------------------------------------------------------------------------------------

    @Test
    void shouldListPreferencesWithSetAndNoArguments() {
        final ConsoleSession console = start()
        assertEquals(["No preferences are set"], console.command(":set"))
        assertEquals(["==>null"], console.send(":set max-iteration 7"))
        // a name without a value is stored as true
        assertEquals(["==>null"], console.send(":set warnings"))
        awaitPreference { Preferences.maxIteration == 7 }
        final List<String> listing = console.command(":set")
        assertEquals("Preferences:", listing[0])
        assertEquals(["max-iteration=7", "warnings=true"], listing.drop(1).collect { it.trim() }.sort())
    }

    @Test
    void shouldReportSetWithUnbalancedQuotes() {
        final ConsoleSession console = start()
        console.fails(':set result.prompt "oops', 'Missing closing "')
        assertEquals(["==>1"], console.results("1"))
    }

    // --- :load and the groovysh commands ----------------------------------------------------------------------------

    @Test
    void shouldLoadAFileAndKeepItsVariables() {
        final File script = writeFile("loaded.groovy", "loadedA = 40\nloadedB = loadedA + 2\n")
        for (String load : [":load", ":l"]) {
            final ConsoleSession console = start()
            // the file runs as a whole and only reports that it was loaded
            assertEquals(load, ["Loaded: " + script.absolutePath], console.command("$load ${script.absolutePath}"))
            assertEquals(load, ["==>42"], console.results("loadedB"))
        }
    }

    @Test
    void shouldShowVariablesAndImports() {
        final ConsoleSession console = start()
        console.send("myVar = 5")
        final List<String> output = console.command("/show")
        assertTrue(output.toString(), output.any { it ==~ /^myVar\s+5$/ })
        assertEquals([], console.command("/imports"))
        console.send("import java.util.concurrent.TimeUnit")
        assertEquals(["import java.util.concurrent.TimeUnit"], console.command("/imports").collect { it.trim() })
    }

    @Test
    void shouldDeleteVariables() {
        final ConsoleSession console = start()
        console.send("myVar = 5")
        console.send("otherVar = 6")
        assertEquals(["==>null"], console.send("/del myVar"))
        console.fails("myVar", "No such property: myVar")
        assertEquals(["==>6"], console.results("otherVar"))
        assertEquals(["==>null"], console.send("/del *"))
        console.fails("otherVar", "No such property: otherVar")
    }

    /**
     * The numbered entries of a /history listing as [number, text] pairs, oldest first.
     */
    private static List<List> historyEntries(final List<String> output) {
        return output.collect { it =~ /^\s*(\d+)\s+(.*)$/ }.findAll { it.matches() }
                .collect { [Integer.parseInt(it.group(1)), it.group(2)] }
    }

    @Test
    void shouldListNumberedHistoryEntries() {
        final ConsoleSession console = start()
        console.send("firstEntry = 1")
        console.send("secondEntry = 2")
        for (String history : [":history", ":H"]) {
            final List<String> entries = historyEntries(console.command(history)).collect { it[1] as String }
            // oldest first, ending with the command itself
            assertTrue(entries.toString(), entries.indexOf("firstEntry = 1") < entries.indexOf("secondEntry = 2"))
            assertEquals(entries.toString(), history, entries[-1])
        }
    }

    @Test
    void shouldRunAnEarlierHistoryEntryAgain() {
        final ConsoleSession console = start()
        assertEquals(["==>42"], console.results("40 + 2"))
        final int number = historyEntries(console.command(":history")).find { it[1] == "40 + 2" }[0] as int
        // the command reports its own null result as well as that of the entry it runs
        assertTrue(console.results("/history -s $number").contains("==>42"))
    }

    @Test
    void shouldRunTheGrabInspectAndDocCommands() {
        final ConsoleSession console = start()
        console.fails(":grab nonsense", "Invalid command parameter: nonsense")
        console.send("inspectMe = 1")
        // a variable is passed to a command with a leading $
        final List<String> output = console.command(':inspect $inspectMe').collect { it.trim() }
        assertTrue(output.toString(), output.contains("public final class Integer"))
        assertEquals(["==>null"], console.send(":doc"))
    }

    @Test
    void shouldCreateANewCommandWithAlias() {
        final ConsoleSession console = start()
        assertEquals(["==>null"], console.send(":alias helpme /help"))
        assertTrue(listedCommands(console.command("helpme")).containsAll(GREMLIN_COMMANDS))
        assertEquals(["==>null"], console.send(":alias two 1+1"))
        assertEquals(["==>2"], console.results("two"))
    }
}
