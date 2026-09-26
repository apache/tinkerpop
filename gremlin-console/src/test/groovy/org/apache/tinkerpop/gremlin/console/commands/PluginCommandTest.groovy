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

import org.apache.tinkerpop.gremlin.console.GremlinGroovysh
import org.apache.tinkerpop.gremlin.console.Mediator
import org.apache.tinkerpop.gremlin.console.PluggedIn
import org.apache.tinkerpop.gremlin.jsr223.GremlinPlugin
import org.codehaus.groovy.tools.shell.IO
import org.junit.Before
import org.junit.Test

import static org.junit.Assert.assertTrue

class PluginCommandTest {
    private PluginCommand cmd

    @Before
    void setUp() {
        def io = new IO(new ByteArrayInputStream(), new ByteArrayOutputStream(), new ByteArrayOutputStream())
        def shell = new GremlinGroovysh(new Mediator(null), io)
        cmd = new PluginCommand(shell, new Mediator(null))
    }

    @Test
    void shouldRequirePluginNameForUse() {
        assertTrue(((String) cmd.do_use.call([])).contains("Specify the name"))
    }

    @Test
    void shouldReportUnknownPluginForUse() {
        assertTrue(((String) cmd.do_use.call(["no.such.plugin"])).contains("could not be found"))
    }

    @Test
    void shouldRequirePluginNameForUnuse() {
        assertTrue(((String) cmd.do_unuse.call([])).contains("Specify the name"))
    }

    @Test
    void shouldReportUnknownPluginForUnuse() {
        assertTrue(((String) cmd.do_unuse.call(["no.such.plugin"])).contains("could not be found"))
    }

    @Test
    void shouldDeactivateAnAvailablePlugin() {
        def io = new IO(new ByteArrayInputStream(), new ByteArrayOutputStream(), new ByteArrayOutputStream())
        def mediator = new Mediator(null)
        def shell = new GremlinGroovysh(mediator, io)
        // populate a real plugin from the classpath so do_unuse's any/find predicates + deactivate path run
        def plugin = ServiceLoader.load(GremlinPlugin, shell.getInterp().getClassLoader()).iterator().next()
        mediator.availablePlugins.put(plugin.class.name, new PluggedIn((GremlinPlugin) plugin, shell, io, false))
        def cmd = new PluginCommand(shell, mediator)
        def result = (String) cmd.do_unuse.call([plugin.name])
        assertTrue(result.contains("deactivated"))
    }
}
