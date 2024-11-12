/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.tinkerpop.gremlin.structure.io.Storage
import org.apache.tinkerpop.gremlin.util.TestSupport
import org.codehaus.groovy.tools.shell.IO
import org.junit.Test

import java.nio.file.Files

import static org.junit.Assert.assertTrue

class GremlinGroovyshTest extends AbstractGremlinServerIntegrationTest {
    private IO testio
    private ByteArrayOutputStream out
    private ByteArrayOutputStream err
    private GremlinGroovysh shell
    private File propertiesFile

    @Override
    void setUp() {
        super.setUp()
        out = new ByteArrayOutputStream()
        err = new ByteArrayOutputStream()
        testio = new IO(new ByteArrayInputStream(), out, err)
        shell = new GremlinGroovysh(new Mediator(null), testio)

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
