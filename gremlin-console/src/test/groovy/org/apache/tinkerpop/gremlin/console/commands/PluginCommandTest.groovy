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
package org.apache.tinkerpop.gremlin.console.commands

import org.apache.groovy.groovysh.jline.GroovyEngine
import org.apache.tinkerpop.gremlin.console.ConsoleFs
import org.apache.tinkerpop.gremlin.console.Mediator
import org.apache.tinkerpop.gremlin.console.PluggedIn
import org.apache.tinkerpop.gremlin.jsr223.AbstractGremlinPlugin
import org.apache.tinkerpop.gremlin.jsr223.DefaultScriptCustomizer
import org.apache.tinkerpop.gremlin.jsr223.GremlinPlugin
import org.jline.builtins.Options.HelpException
import org.jline.shell.CommandSession
import org.junit.Before
import org.junit.Test

import static org.apache.tinkerpop.gremlin.console.commands.CommandCompletion.complete
import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertTrue
import static org.junit.Assert.fail
import static org.junit.Assume.assumeFalse

class PluginCommandTest {
    private PluginCommand cmd

    @Before
    void setUp() {
        cmd = new PluginCommand(new Mediator(null))
    }

    @Test
    void shouldRequirePluginNameForUse() {
        assertTrue(((String) cmd.execute(["use"])).contains("Specify the name"))
    }

    @Test
    void shouldReportUnknownPluginForUse() {
        assertTrue(((String) cmd.execute(["use", "no.such.plugin"])).contains("could not be found"))
    }

    @Test
    void shouldRequirePluginNameForUnuse() {
        assertTrue(((String) cmd.execute(["unuse"])).contains("Specify the name"))
    }

    @Test
    void shouldReportUnknownPluginForUnuse() {
        assertTrue(((String) cmd.execute(["unuse", "no.such.plugin"])).contains("could not be found"))
    }

    @Test
    void shouldDeactivateAnAvailablePlugin() {
        def engine = new GroovyEngine()
        def mediator = new Mediator(null)
        // populate a real plugin from the classpath so the lookup and deactivate path run
        def plugin = ServiceLoader.load(GremlinPlugin, engine.getClassLoader()).iterator().next()
        mediator.availablePlugins.put(plugin.class.name, new PluggedIn((GremlinPlugin) plugin, engine, false))
        def cmd = new PluginCommand(mediator)
        def result = (String) cmd.execute(["unuse", plugin.name])
        assertTrue(result.contains("deactivated"))
    }

    // the function handling groovysh 4's ComplexCommandSupport used to provide

    @Test
    void shouldUseByDefault() {
        assertTrue(((String) cmd.execute([])).contains("Specify the name of the plugin to use"))
    }

    @Test(expected = UsageException)
    void shouldRejectUnknownFunction() {
        cmd.execute(["bogus"])
    }

    @Test
    void shouldListPlugins() {
        def engine = new GroovyEngine()
        def mediator = new Mediator(null)
        def plugin = ServiceLoader.load(GremlinPlugin, engine.getClassLoader()).iterator().next()
        mediator.availablePlugins.put(plugin.class.name, new PluggedIn((GremlinPlugin) plugin, engine, true))
        assertEquals([plugin.name + "[active]"], new PluginCommand(mediator).execute(["list"]))
    }

    @Test
    void shouldCompleteFunctions() {
        assertEquals(["list", "unuse", "use"], complete(cmd, "/plugin "))
        assertEquals(["unuse", "use"], complete(cmd, "/pin u"))
    }

    // added with Groovy 6

    @Test
    void shouldShowUsageInHelp() {
        def session = new CommandSession(null, new ByteArrayInputStream(new byte[0]),
                new PrintStream(new ByteArrayOutputStream(), true), new PrintStream(new ByteArrayOutputStream(), true))
        try {
            cmd.execute(session, ["--help"] as String[])
            fail("should have thrown HelpException")
        } catch (HelpException e) {
            assertTrue(e.message, e.message.contains("Usage: plugin [use <plugin-name>|list|unuse <plugin-name>]"))
        }
    }

    private static GremlinPlugin plugin(final String name, final boolean requireRestart = false) {
        // a set of scripts, as a list would choose the constructor that takes script files
        final Collection<List<String>> scripts = [["activations << '$name'".toString()]] as Set<List<String>>
        return new AbstractGremlinPlugin(name, new DefaultScriptCustomizer(scripts)) {
            @Override
            boolean requireRestart() { requireRestart }
        }
    }

    /**
     * Runs the code with a writable plugins.txt, which writePluginState only writes to when it already exists.
     */
    private static void withPluginsFile(final Closure code) {
        final File file = new File(ConsoleFs.PLUGIN_CONFIG_FILE)
        final boolean createdDir = file.parentFile.mkdirs()
        try {
            file.text = ""
            code.call(file)
        } finally {
            file.delete()
            if (createdDir) file.parentFile.delete()
        }
    }

    @Test
    void shouldListWhetherEachPluginIsActiveNowAndAfterARestart() {
        def engine = new GroovyEngine()
        def mediator = new Mediator(null)
        def unused = new PluggedIn(plugin("test.c"), engine, true)
        unused.deactivate()
        def pending = new PluggedIn(plugin("test.d", true), engine, false)
        pending.restartRequired = true
        pending.activate()
        mediator.availablePlugins.put("a", new PluggedIn(plugin("test.a"), engine, true))
        mediator.availablePlugins.put("b", new PluggedIn(plugin("test.b"), engine, false))
        mediator.availablePlugins.put("c", unused)
        mediator.availablePlugins.put("d", pending)
        assertEquals(["test.a[active]",
                      "test.b[inactive]",
                      "test.c[active until restart]",
                      "test.d[inactive until restart]"], new PluginCommand(mediator).execute(["list"]))
    }

    @Test
    void shouldApplyAPluginOnlyOnce() {
        def engine = new GroovyEngine()
        engine.put("activations", [])
        def pluggedIn = new PluggedIn(plugin("test.a"), engine, false)
        pluggedIn.activate()
        pluggedIn.deactivate()
        pluggedIn.activate()
        assertEquals(["test.a"], engine.get("activations"))
        assertTrue(pluggedIn.loaded && pluggedIn.activated)
    }

    @Test
    void shouldOnlyApplyAPluginThatNeedsARestartAfterTheRestart() {
        def engine = new GroovyEngine()
        engine.put("activations", [])
        def mediator = new Mediator(null)
        def pluggedIn = new PluggedIn(plugin("test.d", true), engine, false)
        pluggedIn.restartRequired = true
        mediator.availablePlugins.put("d", pluggedIn)
        withPluginsFile { File file ->
            assertEquals("test.d will be active after the console restarts",
                    new PluginCommand(mediator).execute(["use", "test.d"]).toString())
            assertEquals([], engine.get("activations"))
            assertEquals(["d"], file.readLines())
        }
    }

    @Test
    void shouldOnlyMentionTheRestartWhenUnusingAPluginThatIsActiveNow() {
        def engine = new GroovyEngine()
        def mediator = new Mediator(null)
        mediator.availablePlugins.put("a", new PluggedIn(plugin("test.a"), engine, true))
        mediator.availablePlugins.put("b", new PluggedIn(plugin("test.b"), engine, false))
        withPluginsFile {
            assertEquals("test.a deactivated - restart your console for this to take effect",
                    new PluginCommand(mediator).execute(["unuse", "test.a"]).toString())
            assertEquals("test.b deactivated", new PluginCommand(mediator).execute(["unuse", "test.b"]).toString())
        }
    }

    @Test
    void shouldKeepTheStateAfterARestartWhenThePluginListIsReadOnly() {
        def engine = new GroovyEngine()
        engine.put("activations", [])
        def mediator = new Mediator(null)
        def pending = new PluggedIn(plugin("test.d", true), engine, false)
        pending.restartRequired = true
        mediator.availablePlugins.put("a", new PluggedIn(plugin("test.a"), engine, true))
        mediator.availablePlugins.put("b", new PluggedIn(plugin("test.b"), engine, false))
        mediator.availablePlugins.put("d", pending)
        final PluginCommand cmd = new PluginCommand(mediator)
        withPluginsFile { File file ->
            file.setWritable(false)
            // root can still write to it
            assumeFalse(file.canWrite())
            final String readonly = " but " + ConsoleFs.PLUGIN_CONFIG_FILE +
                    " is readonly - the plugin list will remain unchanged on console restart"
            assertEquals("test.a deactivated" + readonly, cmd.execute(["unuse", "test.a"]).toString())
            assertEquals("test.b activated" + readonly, cmd.execute(["use", "test.b"]).toString())
            assertEquals("test.d will not be active after the console restarts as " + ConsoleFs.PLUGIN_CONFIG_FILE +
                    " is readonly", cmd.execute(["use", "test.d"]).toString())
            assertEquals(["test.a[active]", "test.b[active until restart]", "test.d[inactive]"], cmd.execute(["list"]))
            assertEquals(["test.b"], engine.get("activations"))
        }
    }

    @Test
    void shouldCompletePluginNames() {
        def engine = new GroovyEngine()
        def mediator = new Mediator(null)
        def plugin = ServiceLoader.load(GremlinPlugin, engine.getClassLoader()).iterator().next()
        mediator.availablePlugins.put(plugin.class.name, new PluggedIn((GremlinPlugin) plugin, engine, false))
        assertEquals([plugin.name], complete(new PluginCommand(mediator), "/plugin use "))
        assertEquals([], complete(new PluginCommand(mediator), "/plugin nope "))
    }
}
