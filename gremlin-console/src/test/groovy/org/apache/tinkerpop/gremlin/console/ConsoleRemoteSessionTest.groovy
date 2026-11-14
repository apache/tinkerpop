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
import org.apache.tinkerpop.gremlin.structure.io.Storage
import org.apache.tinkerpop.gremlin.util.TestSupport
import org.junit.After
import org.junit.Before
import org.junit.Test

import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertTrue

/**
 * Sending traversals from the console to a Gremlin Server serving the modern graph.
 */
class ConsoleRemoteSessionTest extends AbstractConsoleSessionTest {

    private static final int PORT = 45940

    private AbstractGremlinServerIntegrationTest server
    private File remoteProperties

    @Before
    void startServer() {
        // serves the modern graph as g (see gremlin-server-integration.yaml)
        server = new AbstractGremlinServerIntegrationTest() {}
        server.setUp()
        final File remoteYaml = TestSupport.generateTempFileFromResource(AbstractGremlinServerIntegrationTest, "remote.yaml", "")
        remoteProperties = writeFile("remote-graph.properties",
                "gremlin.remote.remoteConnectionClass=org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection\n" +
                "gremlin.remote.driver.clusterFile=${Storage.toPath(remoteYaml)}\n" +
                "gremlin.remote.driver.sourceName=g\n")
    }

    @After
    void stopServer() {
        server.tearDown()
    }

    @Test
    void shouldTraverseARemoteTraversalSource() {
        final ConsoleSession console = start()
        assertEquals(["==>null"], console.results("g = traversal().with('${Storage.toPath(remoteProperties)}')"))
        final List<String> output = console.results("g")
        assertTrue(output.toString(), output.size() == 1 && output[0].startsWith("==>graphtraversalsource["))
        assertEquals(["==>6"], console.results("g.V().count()"))
        assertEquals(["==>josh", "==>vadas"], console.results("g.V().has('name', 'marko').out('knows').values('name').order()"))
        assertClosesWithoutAnError(console)
    }

    @Test
    void shouldTraverseADriverRemoteConnection() {
        final ConsoleSession console = start()
        console.results("g = traversal().with(DriverRemoteConnection.using('localhost', $PORT, 'g'))")
        assertEquals(["==>2"], console.results("g.V().hasLabel('software').count()"))
        assertClosesWithoutAnError(console)
    }

    private static void assertClosesWithoutAnError(final ConsoleSession console) {
        console.send("g.close()")
        assertEquals("", console.lastErr)
    }

    @Test
    void shouldSubmitAScriptWithAClient() {
        final ConsoleSession console = start()
        console.results("cluster = Cluster.build('localhost').port($PORT).create()")
        console.results("client = cluster.connect()")
        assertEquals(["==>6"], console.results("client.submit('g.V().count()').one().getLong()"))
        console.results("client.close()")
        console.results("cluster.close()")
    }
}
