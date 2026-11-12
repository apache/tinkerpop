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
import org.apache.tinkerpop.gremlin.console.Mediator
import org.apache.tinkerpop.gremlin.console.PluggedIn
import org.apache.tinkerpop.gremlin.jsr223.GremlinPlugin
import org.jline.builtins.Options.HelpException
import org.jline.shell.CommandSession
import org.junit.Before
import org.junit.Test

import static org.apache.tinkerpop.gremlin.console.commands.CommandCompletion.complete
import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertTrue
import static org.junit.Assert.fail

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
