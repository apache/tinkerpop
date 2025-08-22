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
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.tinkerpop.gremlin.util.ser.AbstractMessageSerializer;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV2;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV3;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class GremlinServerSerializationIntegrateTest extends AbstractGremlinServerIntegrationTest {

    private AbstractMessageSerializer serializer;
    private Cluster cluster = null;
    private Client client = null;
    private GraphTraversalSource g = null;

    public GremlinServerSerializationIntegrateTest(AbstractMessageSerializer serializer) {
        this.serializer = serializer;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection serializers() {
        return Arrays.asList(new Object[][]{
                {new GraphBinaryMessageSerializerV1()},
                {new GraphSONMessageSerializerV3()},
                {new GraphSONMessageSerializerV2()}
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
        final Vertex vertex = client.submit("gmodern.V(1)").one().getVertex();

        assertVertexWithProperties(vertex);
    }

    @Test
    public void shouldSkipVertexPropertiesForScripts() {
        final Vertex vertex = client.submit("gmodern.with('materializeProperties', 'tokens').V(1)").one().getVertex();

        assertVertexWithoutProperties(vertex);
    }

    @Test
    public void shouldDeserializeVertexPropertiesForBytecode() {
        final Vertex vertex = g.V(1).next();

        assertVertexWithProperties(vertex);
    }

    @Test
    public void shouldSkipVertexPropertiesForBytecode() {
        final Vertex vertex = g.with(Tokens.ARGS_MATERIALIZE_PROPERTIES, Tokens.MATERIALIZE_PROPERTIES_TOKENS).V(1).next();

        assertVertexWithoutProperties(vertex);
    }

    @Test
    public void shouldDeserializeEdgePropertiesForScripts() {
        final Edge edge = client.submit("gmodern.E(7)").one().getEdge();

        assertEdgeWithProperties(edge);
    }

    @Test
    public void shouldSkipEdgePropertiesForScripts() {
        final Edge edge = client.submit("gmodern.with('materializeProperties', 'tokens').E(7)").one().getEdge();

        assertEdgeWithoutProperties(edge);
    }

    @Test
    public void shouldDeserializeEdgePropertiesForBytecode() {
        final Edge edge = g.E(7).next();

        assertEdgeWithProperties(edge);
    }

    @Test
    public void shouldSkipEdgePropertiesForBytecode() {
        final Edge edge = g.with(Tokens.ARGS_MATERIALIZE_PROPERTIES, Tokens.MATERIALIZE_PROPERTIES_TOKENS).E(7).next();

        assertEdgeWithoutProperties(edge);
    }

    @Test
    public void shouldDeserializeVertexPropertyPropertiesForScripts() {
        final Vertex vertex = client.submit("gcrew.V(7)").one().getVertex();

        assertVertexWithVertexProperties(vertex);
    }

    @Test
    public void shouldSkipVertexPropertyPropertiesForScripts() {
        final Vertex vertex = client.submit("gcrew.with('materializeProperties', 'tokens').V(7)").one().getVertex();

        assertVertexWithoutVertexProperties(vertex);
    }

    @Test
    public void shouldDeserializeVertexPropertyPropertiesForBytecode() {
        final Vertex vertex = g.V(7).next();

        assertVertexWithVertexProperties(vertex);
    }

    @Test
    public void shouldSkipVertexPropertyPropertiesForBytecode() {
        final Vertex vertex = g.with(Tokens.ARGS_MATERIALIZE_PROPERTIES, Tokens.MATERIALIZE_PROPERTIES_TOKENS).V(7).next();

        assertVertexWithoutVertexProperties(vertex);
    }

    @Test
    public void shouldDeserializePathPropertiesForScripts() {
        final Path p = client.submit("gmodern.V().has('name','marko').outE().inV().hasLabel('software').path()").one().getPath();
        assertPathElementsWithProperties(p);
    }

    @Test
    public void shouldDeserializePathPropertiesForScriptsWithTokens() {
        final Path p = client.submit("gmodern.with('materializeProperties','tokens').V().has('name','marko').outE().inV().hasLabel('software').path()").one().getPath();
        assertPathElementsWithoutProperties(p);
    }

    @Test
    public void shouldDeserializePathPropertiesForScriptsForBytecode() {
        final Path p = g.V().has("name", "marko").outE().inV().hasLabel("software").path().next();
        assertPathElementsWithProperties(p);
    }

    @Test
    public void shouldDeserializePathPropertiesForScriptsWithTokensForBytecode() {
        final Path p = g.with(Tokens.ARGS_MATERIALIZE_PROPERTIES, Tokens.MATERIALIZE_PROPERTIES_TOKENS).
                V().has("name", "marko").outE().inV().hasLabel("software").path().next();
        assertPathElementsWithoutProperties(p);
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

    private void assertPathElementsWithProperties(final Path p) {
        // expect a V-E-V path
        assertEquals(3, p.size());

        final Object a = p.get(0);
        final Object b = p.get(1);
        final Object c = p.get(2);

        // basic type assertions (prefer Hamcrest)
        assertThat(a, instanceOf(Vertex.class));
        assertThat(b, instanceOf(Edge.class));
        assertThat(c, instanceOf(Vertex.class));

        // properties should be present on each element in the path with expected key/values
        final Vertex vOut = (Vertex) a;
        final Edge e = (Edge) b;
        final Vertex vIn = (Vertex) c;

        // vertex 'a' should be marko with age 29
        assertThat(IteratorUtils.count(vOut.properties()) > 0, is(true));
        assertEquals("marko", vOut.value("name"));
        assertEquals(Integer.valueOf(29), vOut.value("age"));

        // edge 'b' should have weight 0.4
        assertThat(IteratorUtils.count(e.properties()) > 0, is(true));
        assertEquals(0.4, (Double) e.value("weight"), 0.0000001d);

        // vertex 'c' should be lop with lang java
        assertThat(IteratorUtils.count(vIn.properties()) > 0, is(true));
        assertEquals("lop", vIn.value("name"));
        assertEquals("java", vIn.value("lang"));
    }

    private void assertPathElementsWithoutProperties(final Path p) {
        // expect a V-E-V path
        assertEquals(3, p.size());

        final Object a = p.get(0);
        final Object b = p.get(1);
        final Object c = p.get(2);

        // basic type assertions (prefer Hamcrest)
        assertThat(a, instanceOf(Vertex.class));
        assertThat(b, instanceOf(Edge.class));
        assertThat(c, instanceOf(Vertex.class));

        // properties should NOT be present on each element in the path when materializeProperties is 'tokens'
        final Vertex vOut = (Vertex) a;
        final Edge e = (Edge) b;
        final Vertex vIn = (Vertex) c;

        assertEquals(0, IteratorUtils.count(vOut.properties()));
        assertEquals(0, IteratorUtils.count(e.properties()));
        assertEquals(0, IteratorUtils.count(vIn.properties()));
    }
}
