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
 * How typed Groovy is evaluated: state kept between lines, imports, multi-line input, errors and scripts run with
 * {@code -i}.
 */
class ConsoleEvalSessionTest extends AbstractConsoleSessionTest {

    private static final String CONTINUATION_1 = "......1> "
    private static final String CONTINUATION_2 = "......2> "

    @Test
    void shouldKeepVariablesBetweenLines() {
        final ConsoleSession console = start()
        // an assignment reports a null result, but the variable is set
        assertEquals(["==>null"], console.results("x = 1"))
        assertEquals(["==>2"], console.results("x + 1"))
        assertEquals(["==>null"], console.results("x = x * 10"))
        assertEquals(["==>10"], console.results("x"))
    }

    @Test
    void shouldNotKeepDefVariablesBetweenLines() {
        // without groovysh's interpreterMode a "def" variable is local to the line that declared it
        final ConsoleSession console = start()
        console.send("def y = 1")
        console.fails("y", "No such property: y")
    }

    @Test
    void shouldCallAMethodDefinedOnAnEarlierLine() {
        final ConsoleSession console = start()
        // a declaration on its own evaluates to null
        assertEquals(["==>null"], console.results("int add(int a, int b) { a + b }"))
        assertEquals(["==>3"], console.results("add(1, 2)"))
    }

    @Test
    void shouldUseAClassDefinedOnAnEarlierLine() {
        final ConsoleSession console = start()
        assertEquals(["==>null"], console.results("class Foo { int a }"))
        assertEquals(["==>1"], console.results("new Foo(a: 1).a"))
    }

    @Test
    void shouldKeepAnImportBetweenLines() {
        final ConsoleSession console = start()
        // an import is a plain Groovy statement with a null result
        assertEquals(["==>null"], console.results("import java.util.concurrent.TimeUnit"))
        assertEquals(["==>SECONDS"], console.results("TimeUnit.SECONDS"))
        assertEquals(["==>120"], console.results("TimeUnit.MINUTES.toSeconds(2)"))
    }

    @Test
    void shouldKeepAStaticImportBetweenLines() {
        final ConsoleSession console = start()
        assertEquals(["==>null"], console.results("import static java.lang.Math.max"))
        assertEquals(["==>7"], console.results("max(3, 7)"))
    }

    @Test
    void shouldNotNeedImportsForTheDefaultGremlinClasses() {
        final ConsoleSession console = start()
        // TinkerFactory comes from the tinkergraph plugin, traversal() from the core imports
        loadModern(console)
        // static enum import (Order.desc)
        assertEquals(["==>peter", "==>josh", "==>marko", "==>vadas"],
                console.results("g.V().hasLabel('person').order().by('age', desc).values('name')"))
        // static method import (P.gt)
        assertEquals(["==>josh", "==>peter"], console.results("g.V().has('age', gt(30)).values('name').order()"))
        // anonymous traversal class (__)
        assertEquals(["==>marko"], console.results("g.V(1).where(__.out('created')).values('name')"))
        // class imports (T, Direction) and static enum import (Scope.local)
        assertEquals(["==>2"], console.results("g.V().has(T.label, 'software').count()"))
        assertEquals(["==>OUT"], console.results("Direction.OUT"))
        assertEquals(["==>4"], console.results("g.V().hasLabel('person').values('age').fold().count(local)"))
    }

    @Test
    void shouldMakeThePluginImportsAvailable() {
        final ConsoleSession console = start()
        // tinkerpop.remote plugin
        assertEquals(["==>org.apache.tinkerpop.gremlin.driver.Cluster"], console.results("Cluster.class.name"))
        assertEquals(["==>org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection"],
                console.results("DriverRemoteConnection.class.name"))
        // tinkerpop.tinkergraph plugin
        assertEquals(["==>tinkermemorygraph[vertices:0 edges:0]"], console.results("TinkerGraph.open()"))
    }

    @Test
    void shouldDefineDescribeGraphFromTheUtilitiesPluginScript() {
        final ConsoleSession console = start()
        final String output = console.send("describeGraph(TinkerGraph)").join("\n")
        assertTrue(output, output.contains("IMPLEMENTATION - org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph"))
        assertTrue(output, output.contains("TINKERPOP TEST SUITE"))
    }

    @Test
    void shouldNumberEachContinuationLine() {
        final ConsoleSession console = start()
        // the continuation prompt is the line number padded with dots to the width of "gremlin>"
        console.send("[1,2,", CONTINUATION_1)
        console.send("3,", CONTINUATION_2)
        assertEquals(["==>1", "==>2", "==>3", "==>4"], console.results("4]"))
    }

    @Test
    void shouldReadAMultiLineClosureAndMap() {
        final ConsoleSession console = start()
        console.send("add3 = { a ->", CONTINUATION_1)
        console.send("  a + 3", CONTINUATION_2)
        console.send("}")
        assertEquals(["==>4"], console.results("add3(1)"))

        console.send("m = [a: 1,", CONTINUATION_1)
        assertEquals(["==>null"], console.results("b: 2]"))
        assertEquals(["==>a=1", "==>b=2"], console.results("m"))
    }

    @Test
    void shouldReportASyntaxErrorWithoutTheCompilerPrefix() {
        final ConsoleSession console = start()
        final String error = console.fails("x = )", "Type ':help' or ':h' for help.")
        // the "startup failed:" prefix Groovy puts on compilation errors is stripped
        assertFalse(error, error.contains("startup failed:"))
        // the broken line was discarded rather than kept as an unfinished statement
        assertEquals(["==>2"], console.results("1+1"))
    }

    @Test
    void shouldPrintAHintForAStackOverflow() {
        final ConsoleSession console = start()
        assertEquals(["==>null"], console.results("def deep(n) { deep(n + 1) }"))
        console.fails("deep(0)", "A StackOverflowError can indicate that the Gremlin traversal being executed is too long.")
        assertEquals(["==>2"], console.results("1+1"))
    }

    @Test
    void shouldReachTheConsoleClassesThroughTheClassLoader() {
        final ConsoleSession console = start()
        assertEquals(["==>org.apache.tinkerpop.gremlin.driver.Cluster"],
                console.results("this.class.classLoader.loadClass('org.apache.tinkerpop.gremlin.driver.Cluster').name"))
        assertEquals(["==>TinkerGraph"],
                console.results("Class.forName('org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph').simpleName"))
    }

    @Test
    void shouldKeepTheLastResultAsUnderscore() {
        final ConsoleSession console = start()
        assertEquals(["==>42"], console.results("21 * 2"))
        assertEquals(["==>43"], console.results("_ + 1"))
    }

    // --- scripts run with -i ------------------------------------------------------------------------------------------

    @Test
    void shouldRunAMultiLineStatementInAnInteractiveScript() {
        final File script = writeFile("multi-i.groovy", "total = [1,\n  2,\n  3].sum()\n")
        final ConsoleSession console = start([[script.absolutePath]])
        assertEquals(["==>6"], console.results("total"))
    }

    @Test
    void shouldRunConsoleCommandsInsideAScriptFile() {
        writePluginsFile(DEFAULT_PLUGINS)
        final File script = writeFile("commands.groovy", ":plugin unuse tinkerpop.utilities\nafterCommand = 'ran'\n")
        final ConsoleSession console = start([[script.absolutePath]])
        assertEquals(["==>ran"], console.results("afterCommand"))
        final List<String> listed = console.results(":plugin list")
        assertTrue(listed.toString(), listed.contains("==>tinkerpop.utilities[active until restart]"))
    }

    @Test
    void shouldReadTheArgumentsOfCommandsInAScriptLikeTypedOnes() {
        final List<String> commands = [':set result.prompt "a b> "', ':set result.indicator.null "say \\"hi\\""',
                                       ':set   max-iteration   7']
        final List<String> keys = [Preferences.PREF_RESULT_PROMPT, Preferences.PREF_RESULT_IND_NULL,
                                   Preferences.PREFERENCE_ITERATION_MAX]
        final ConsoleSession typed = start()
        commands.each { typed.send(it) }
        final List<String> typedValues = keys.collect { STORE.get(it, null) }
        typed.close()
        assertEquals(["a b> ", "7"], [typedValues[0], typedValues[2]])

        STORE.clear()
        start([[writeFile("commands.groovy", commands.join("\n") + "\n").absolutePath]])
        assertEquals(typedValues, keys.collect { STORE.get(it, null) })
    }

    @Test
    void shouldRunScriptCommandsByAliasWithLeadingSpacesAndSingleQuotes() {
        writePluginsFile(DEFAULT_PLUGINS)
        final File script = writeFile("aliases.groovy",
                "  :pin unuse tinkerpop.utilities\nx = [1,\n  2]\n:set result.prompt 'c d> '\n")
        final ConsoleSession console = start([[script.absolutePath]])
        assertEquals("c d> ", STORE.get(Preferences.PREF_RESULT_PROMPT, null))
        awaitPreference { Preferences.resultPrompt == "c d> " }
        assertTrue(console.send(":plugin list").contains("c d> tinkerpop.utilities[active until restart]"))
        assertEquals(["c d> 1", "c d> 2"], console.send("x"))
    }

    @Test
    void shouldReportAnUnclosedQuoteInAScriptCommandWithItsLine() {
        final File script = writeFile("unclosed.groovy", 'a = 1\n:set result.prompt "oops\nb = 2\n')
        final ConsoleSession console = start([[script.absolutePath]])
        final String error = console.err.text()
        assertTrue(error, error.contains("Error in ${script.absolutePath} at [2: :set result.prompt \"oops] - Missing closing \"".toString()))
        assertEquals(["==>1"], console.results("a"))
        console.fails("b", "No such property: b")
    }

    @Test
    void shouldReportAShellCommandInsideAScriptFile() {
        // the groovysh commands only exist once the shell has started, which is after the scripts ran
        final File script = writeFile("shell-command.groovy", "before = 1\n:load other.groovy\nafter = 2\n")
        final ConsoleSession console = start([[script.absolutePath]])
        final String error = console.err.text()
        assertTrue(error, error.contains("Error in ${script.absolutePath} at [2: :load other.groovy] - :load cannot be used in a script".toString()))
        assertEquals(["==>1"], console.results("before"))
    }

    @Test
    void shouldReportTheLineOfAnErrorInAnInteractiveScriptAndStop() {
        final File script = writeFile("stops.groovy", "a = 1\nb = 1/0\nc = 3\n")
        final ConsoleSession console = start([[script.absolutePath]])
        final String error = console.err.text()
        assertTrue(error, error.contains("Error in ${script.absolutePath} at [2: b = 1/0] - Division by zero".toString()))
        assertEquals(["==>1"], console.results("a"))
        // the line after the error was not run
        console.fails("c", "No such property: c")
    }
}
