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

import org.apache.groovy.groovysh.jline.GroovyEngine
import org.apache.tinkerpop.gremlin.console.jsr223.AbstractGremlinServerIntegrationTest
import org.apache.tinkerpop.gremlin.console.jsr223.RemoteGremlinPlugin
import org.apache.tinkerpop.gremlin.structure.io.Storage
import org.apache.tinkerpop.gremlin.util.TestSupport
import org.junit.Test

import java.nio.file.Files
import java.util.concurrent.atomic.AtomicReference

import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertFalse
import static org.junit.Assert.assertTrue

/**
 * The engine the console runs statements in.
 */
class ConsoleEngineTest extends AbstractGremlinServerIntegrationTest {
    private GroovyEngine engine
    private File propertiesFile

    @Override
    void setUp() {
        super.setUp()
        engine = Console.createEngine()
        prepareConfigFiles()
    }

    @Test
    void shouldGetResultFromRemote() {
        engine.execute("g = traversal().with('" + Storage.toPath(propertiesFile) + "')")

        // 6 vertices in modern graph
        assertEquals(6L, engine.execute("g.V().count().next()"))
    }

    @Test
    void shouldUseRemotePlugin() {
        final RemoteGremlinPlugin plugin = new RemoteGremlinPlugin()
        final PluggedIn pluggedIn = new PluggedIn(plugin, engine, false)
        pluggedIn.activate()

        // Cluster comes from the remote plugin's imports
        assertEquals("org.apache.tinkerpop.gremlin.driver.Cluster", engine.execute("Cluster.class.name"))

        engine.execute("g = traversal().with('" + Storage.toPath(propertiesFile) + "')")
        assertEquals(6L, engine.execute("g.V().count().next()"))
    }

    @Test
    void shouldEvaluateWithCoreGremlinImports() {
        assertEquals(2, engine.execute("1 + 1"))
        // traversal() and T are core imports; TinkerFactory would come from the tinkergraph plugin
        assertEquals(6L, engine.execute("traversal().with(org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory.createModern()).V().count().next()"))
        assertEquals(org.apache.tinkerpop.gremlin.structure.T.label, engine.execute("T.label"))
    }

    @Test
    void shouldStopAStatementWhenItsThreadIsInterrupted() {
        final AtomicReference<Throwable> failure = new AtomicReference<>()
        final Thread t = new Thread({
            try {
                engine.execute("while (true) {}")
            } catch (Throwable e) {
                failure.set(e)
            }
        })
        t.start()
        Thread.sleep(500)
        t.interrupt()
        t.join(10000)

        assertFalse(t.isAlive())
        assertTrue(String.valueOf(failure.get()), failure.get() instanceof InterruptedException)
    }

    @Override
    void tearDown() {
        super.tearDown()
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
