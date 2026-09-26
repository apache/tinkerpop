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


import org.apache.tinkerpop.gremlin.console.jsr223.AbstractGremlinServerIntegrationTest
import org.apache.tinkerpop.gremlin.console.jsr223.RemoteGremlinPlugin
import org.apache.tinkerpop.gremlin.console.commands.GremlinSetCommand
import org.apache.tinkerpop.gremlin.structure.io.Storage
import org.apache.tinkerpop.gremlin.util.TestSupport
import org.apache.groovy.groovysh.Command
import org.apache.groovy.groovysh.commands.SetCommand
import org.codehaus.groovy.tools.shell.IO
import org.junit.Test

import java.nio.file.Files

import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertFalse
import static org.junit.Assert.assertNotNull
import static org.junit.Assert.assertNull
import static org.junit.Assert.assertSame
import static org.junit.Assert.assertTrue

class GremlinGroovyshTest extends AbstractGremlinServerIntegrationTest {
    private IO testio
    private ByteArrayOutputStream out
    private ByteArrayOutputStream err
    private Mediator mediator
    private GremlinGroovysh shell
    private File propertiesFile

    @Override
    void setUp() {
        super.setUp()
        out = new ByteArrayOutputStream()
        err = new ByteArrayOutputStream()
        testio = new IO(new ByteArrayInputStream(), out, err)
        mediator = new Mediator(null)
        shell = new GremlinGroovysh(mediator, testio)

        prepareConfigFiles()
    }

    @Test
    void shouldGetResultFromRemote() {
        shell.execute("import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal")
        shell.execute("g = traversal().with('" + Storage.toPath(propertiesFile) + "')")
        out.reset()
        shell.execute("g.V().count().next()")

        // 6 vertices in modern graph
        assertTrue(out.toString().endsWith("6" + System.lineSeparator()))
    }

    @Test
    void shouldUseRemotePlugin() {
        final RemoteGremlinPlugin plugin = new RemoteGremlinPlugin();
        final PluggedIn pluggedIn = new PluggedIn(plugin, shell, testio, false)
        pluggedIn.activate()

        shell.execute("g = traversal().with('" + Storage.toPath(propertiesFile) + "')")
        out.reset()
        shell.execute("g.V().count().next()")

        // 6 vertices in modern graph
        assertTrue(out.toString().endsWith("6" + System.lineSeparator()))
    }

    @Test
    void shouldParseLineIntoWhitespaceDelimitedTokens() {
        assertEquals(["g.V().count()"], shell.parseLine("g.V().count()"))
        assertEquals([":set", "a", "b"], shell.parseLine("  :set a b  "))
        assertTrue(shell.parseLine("   ").isEmpty())
    }

    @Test
    void shouldReturnNullCommandForEmptyOrBlankLine() {
        assertNull(shell.findCommand(""))
        assertNull(shell.findCommand("   "))
    }

    @Test
    void shouldReturnNullCommandForNonCommandLine() {
        assertNull(shell.findCommand("g.V().count()", []))
    }

    @Test
    void shouldFindCommandWithoutPopulatingArgsForSingleToken() {
        final List<String> parsed = []
        final Command cmd = shell.findCommand(":help", parsed)
        assertNotNull(cmd)
        assertTrue(parsed.isEmpty())
    }

    @Test
    void shouldPopulateParsedArgsForNonSetCommand() {
        // the standard :set command (still registered on a bare shell) drives the non-GremlinSetCommand
        // branch which simply appends the remaining tokens
        final List<String> parsed = []
        final Command cmd = shell.findCommand(":set foo bar", parsed)
        assertNotNull(cmd)
        assertEquals(["foo", "bar"], parsed)
    }

    @Test
    void shouldParseQuotedArgsForGremlinSetCommand() {
        // replace the standard SetCommand with the Gremlin variant, as the real Console does, so that the
        // GremlinSetCommand branch (which honours quoted arguments) is exercised
        shell.getRegistry().commands().findAll { it instanceof SetCommand }.each { shell.getRegistry().remove(it) }
        shell.register(new GremlinSetCommand(shell))

        final List<String> parsed = []
        final Command cmd = shell.findCommand(":set result.prompt \"==> \"", parsed)
        assertTrue(cmd instanceof GremlinSetCommand)
        // the quoted value is preserved as a single argument rather than being split on whitespace
        assertEquals("result.prompt", parsed[0])
        assertEquals("==> ", parsed[1])
    }

    @Test
    void shouldEvaluateLocalExpressionAndResetEvaluatingFlag() {
        out.reset()
        final def result = shell.execute("1 + 1")
        assertEquals(2, result)
        // the evaluating flag is toggled during execution and reset in the finally block
        assertFalse(mediator.evaluating.get())
    }

    @Test
    void shouldResetEvaluatingFlagAfterFailedExecution() {
        try {
            shell.execute("this is not valid groovy ^^^")
        } catch (Throwable ignored) {
            // a compilation/evaluation failure is expected here
        }
        assertFalse(mediator.evaluating.get())
    }

    @Override
    void tearDown() {
        super.tearDown()
        shell.execute(":purge preferences") // for test cases where persistent preferences (interpreterMode) are set.

        Files.deleteIfExists(propertiesFile.toPath())
    }

    void prepareConfigFiles() {
        final File configFile = TestSupport.generateTempFileFromResource(AbstractGremlinServerIntegrationTest.class, "remote.yaml", "")
        propertiesFile = File.createTempFile("remote-graph", ".properties")

        Files.deleteIfExists(propertiesFile.toPath())
        Files.createFile(propertiesFile.toPath())
        try (PrintStream out = new PrintStream(new FileOutputStream(propertiesFile.toPath().toString()))) {
            out.print("gremlin.remote.remoteConnectionClass=org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection\n")
            out.print("gremlin.remote.driver.clusterFile=" + Storage.toPath(configFile))
            out.print("\ngremlin.remote.driver.sourceName=g\n")
        }
    }
}
