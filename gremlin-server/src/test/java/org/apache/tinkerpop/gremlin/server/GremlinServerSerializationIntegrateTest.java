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
package org.apache.tinkerpop.gremlin.server;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.tinkerpop.gremlin.util.ser.AbstractMessageSerializer;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV2d0;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV3d0;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class GremlinServerSerializationIntegrateTest extends AbstractGremlinServerIntegrationTest {

    private AbstractMessageSerializer serializer;
    private Cluster cluster = null;
    private Client client = null;
    private GraphTraversalSource g = null;

    public GremlinServerSerializationIntegrateTest(AbstractMessageSerializer serializer) {
        this.serializer = serializer;
    }

    @Parameterized.Parameters
    public static Collection serializers() {
        return Arrays.asList(new Object[][] {
                { new GraphBinaryMessageSerializerV1() },
                { new GraphSONMessageSerializerV3d0() },
                { new GraphSONMessageSerializerV2d0() }
        });
    }

    @Before
    public void openConnection() {
        cluster = TestClientFactory.build().serializer(serializer).create();
        client = cluster.connect();
        g = AnonymousTraversalSource.traversal().withRemote(
                DriverRemoteConnection.using(cluster, "gmodern"));
    }

    @After
    public void closeConnection() {
        if (cluster != null) cluster.close();
    }

    @Test
    public void shouldDeserializeVertexPropertiesForGremlin() {
        final Vertex vertex = client.submit("gmodern.V(1)").one().getVertex();

        assertVertexWithProperties(vertex);
    }

    @Test
    public void shouldSkipVertexPropertiesForGremlin() {
        final Vertex vertex = client.submit("gmodern.with('materializeProperties', 'tokens').V(1)").one().getVertex();

        assertEquals(Collections.emptyIterator(), vertex.properties());
    }

    @Test
    public void shouldDeserializeVertexPropertiesForBytecode() {
        final Vertex vertex = g.V(1).next();

        assertVertexWithProperties(vertex);
    }

    @Test
    public void shouldSkipVertexPropertiesForBytecode() {
        final Vertex vertex = g.with("materializeProperties", "tokens").V(1).next();

        assertEquals(Collections.emptyIterator(), vertex.properties());
    }

    @Test
    public void shouldDeserializeEdgePropertiesForGremlin() {
        final Edge edge = client.submit("gmodern.E(7)").one().getEdge();

        assertEdgeWithProperties(edge);
    }

    @Test
    public void shouldSkipEdgePropertiesForGremlin() {
        final Edge edge = client.submit("gmodern.with('materializeProperties', 'tokens').E(7)").one().getEdge();

        assertEquals(Collections.emptyIterator(), edge.properties());
    }

    @Test
    public void shouldDeserializeEdgePropertiesForBytecode() {
        final Edge edge = g.E(7).next();

        assertEdgeWithProperties(edge);
    }

    @Test
    public void shouldSkipEdgePropertiesForBytecode() {
        final Edge edge = g.with("materializeProperties", "tokens").E(7).next();

        assertEquals(Collections.emptyIterator(), edge.properties());
    }

    private void assertVertexWithProperties(final Vertex vertex) {
        assertEquals(1,  vertex.id());

        assertEquals(2,  IteratorUtils.count(vertex.properties()));
        assertEquals("marko", vertex.property("name").value());
        assertEquals(29, vertex.property("age").value());
    }

    private void assertEdgeWithProperties(final Edge edge) {
        assertEquals(7,  edge.id());

        assertEquals(1,  IteratorUtils.count(edge.properties()));
        assertEquals(0.5, edge.property("weight").value());
    }
}
