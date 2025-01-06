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
import org.apache.tinkerpop.gremlin.driver.RequestOptions;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.tinkerpop.gremlin.util.ser.AbstractMessageSerializer;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV4;
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

    private final RequestOptions gmodern = RequestOptions.build().addG("gmodern").create();
    private final RequestOptions gcrew = RequestOptions.build().addG("gcrew").create();


    public GremlinServerSerializationIntegrateTest(AbstractMessageSerializer serializer) {
        this.serializer = serializer;
    }

    @Parameterized.Parameters
    public static Collection serializers() {
        return Arrays.asList(new Object[][]{
                {new GraphBinaryMessageSerializerV4()}
        });
    }

    @Before
    public void openConnection() {
        cluster = TestClientFactory.build().serializer(serializer).create();
        client = cluster.connect();

        // VertexProperty related test tun on crew graph
        final String remoteTraversalSourceName = name.getMethodName().contains("VertexProperty") ? "gcrew" : "gmodern";

        g = AnonymousTraversalSource.traversal().withRemote(
                DriverRemoteConnection.using(cluster, remoteTraversalSourceName));
    }

    @After
    public void closeConnection() {
        if (cluster != null) cluster.close();
    }

    @Test
    public void shouldDeserializeVertexPropertiesForScripts() {
        final Vertex vertex = client.submit("g.V(1)", gmodern).one().getVertex();

        assertVertexWithProperties(vertex);
    }

    @Test
    public void shouldSkipVertexPropertiesForScripts() {
        final Vertex vertex = client.submit("g.with('materializeProperties', 'tokens').V(1)", gmodern).one().getVertex();

        assertVertexWithoutProperties(vertex);
    }

    @Test
    public void shouldDeserializeVertexPropertiesForBytecode() {
        final Vertex vertex = g.V(1).next();

        assertVertexWithProperties(vertex);
    }

    @Test
    public void shouldSkipVertexPropertiesForBytecode() {
        final Vertex vertex = g.with("materializeProperties", "tokens").V(1).next();

        assertVertexWithoutProperties(vertex);
    }

    @Test
    public void shouldDeserializeEdgePropertiesForScripts() {
        final Edge edge = client.submit("g.E(7)", gmodern).one().getEdge();

        assertEdgeWithProperties(edge);
    }

    @Test
    public void shouldSkipEdgePropertiesForScripts() {
        final Edge edge = client.submit("g.with('materializeProperties', 'tokens').E(7)", gmodern).one().getEdge();

        assertEdgeWithoutProperties(edge);
    }

    @Test
    public void shouldDeserializeEdgePropertiesForBytecode() {
        final Edge edge = g.E(7).next();

        assertEdgeWithProperties(edge);
    }

    @Test
    public void shouldSkipEdgePropertiesForBytecode() {
        final Edge edge = g.with("materializeProperties", "tokens").E(7).next();

        assertEdgeWithoutProperties(edge);
    }

    @Test
    public void shouldDeserializeVertexPropertyPropertiesForScripts() {
        final Vertex vertex = client.submit("g.V(7)", gcrew).one().getVertex();

        assertVertexWithVertexProperties(vertex);
    }

    @Test
    public void shouldSkipVertexPropertyPropertiesForScripts() {
        final Vertex vertex = client.submit("g.with('materializeProperties', 'tokens').V(7)", gcrew).one().getVertex();

        assertVertexWithoutVertexProperties(vertex);
    }

    @Test
    public void shouldDeserializeVertexPropertyPropertiesForBytecode() {
        final Vertex vertex = g.V(7).next();

        assertVertexWithVertexProperties(vertex);
    }

    @Test
    public void shouldSkipVertexPropertyPropertiesForBytecode() {
        final Vertex vertex = g.with("materializeProperties", "tokens").V(7).next();

        assertVertexWithoutVertexProperties(vertex);
    }

    // asserted vertex 7 from crew graph
    private void assertVertexWithVertexProperties(final Vertex vertex) {
        assertEquals(7, vertex.id());
        assertEquals("person", vertex.label());

        assertEquals(4, IteratorUtils.count(vertex.properties()));
        assertEquals(3, IteratorUtils.count(vertex.properties("location")));
        final VertexProperty vertexProperty = vertex.properties("location").next();
        assertEquals("centreville", vertexProperty.value());
        assertEquals(2, IteratorUtils.count(vertexProperty.properties()));
        final Property vertexPropertyPropertyStartTime = vertexProperty.property("startTime");
        assertEquals(1990, vertexPropertyPropertyStartTime.value());
        final Property vertexPropertyPropertyEndTime = vertexProperty.property("endTime");
        assertEquals(2000, vertexPropertyPropertyEndTime.value());
    }

    // asserted vertex 7 from crew graph
    private void assertVertexWithoutVertexProperties(final Vertex vertex) {
        assertEquals(7, vertex.id());
        assertEquals("person", vertex.label());

        assertEquals(0, IteratorUtils.count(vertex.properties()));
    }

    // asserted vertex 1 from modern graph
    private void assertVertexWithoutProperties(final Vertex vertex) {
        assertEquals(1, vertex.id());
        assertEquals("person", vertex.label());

        assertEquals(Collections.emptyIterator(), vertex.properties());
    }

    // asserted vertex 1 from modern graph
    private void assertVertexWithProperties(final Vertex vertex) {
        assertEquals(1, vertex.id());
        assertEquals("person", vertex.label());

        assertEquals(2, IteratorUtils.count(vertex.properties()));
        assertEquals("marko", vertex.property("name").value());
        assertEquals(29, vertex.property("age").value());
    }

    // asserted edge 7 from modern graph
    private void assertEdgeWithoutProperties(final Edge edge) {
        assertEquals(7, edge.id());
        assertEquals("knows", edge.label());

        assertEquals(Collections.emptyIterator(), edge.properties());
    }

    // asserted edge 7 from modern graph
    private void assertEdgeWithProperties(final Edge edge) {
        assertEquals(7, edge.id());
        assertEquals("knows", edge.label());

        assertEquals(1, IteratorUtils.count(edge.properties()));
        assertEquals(0.5, edge.property("weight").value());
    }
}
