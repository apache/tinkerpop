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
package org.apache.tinkerpop.gremlin.server;

import org.apache.http.Consts;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.RequestOptions;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import org.apache.tinkerpop.gremlin.structure.io.pdt.ProviderDefinedType;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.tinkerpop.gremlin.util.ser.AbstractMessageSerializer;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV4;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

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

    private final RequestOptions gmodern = RequestOptions.build().addG("gmodern").create();
    private final RequestOptions gcrew = RequestOptions.build().addG("gcrew").create();


    public GremlinServerSerializationIntegrateTest(AbstractMessageSerializer serializer) {
        this.serializer = serializer;
    }

    @Parameterized.Parameters(name = "{0}")
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
        final Vertex vertex = g.with(Tokens.ARGS_MATERIALIZE_PROPERTIES, Tokens.MATERIALIZE_PROPERTIES_TOKENS).V(1).next();

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
        final Edge edge = g.with(Tokens.ARGS_MATERIALIZE_PROPERTIES, Tokens.MATERIALIZE_PROPERTIES_TOKENS).E(7).next();

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
        final Vertex vertex = g.with(Tokens.ARGS_MATERIALIZE_PROPERTIES, Tokens.MATERIALIZE_PROPERTIES_TOKENS).V(7).next();

        assertVertexWithoutVertexProperties(vertex);
    }

    @Test
    public void shouldDeserializePathPropertiesForScripts() {
        final Path p = client.submit("g.V().has('name','marko').outE().inV().hasLabel('software').path()", gmodern).one().getPath();
        assertPathElementsWithProperties(p);
    }

    @Test
    public void shouldDeserializePathPropertiesForScriptsWithTokens() {
        final Path p = client.submit("g.with('materializeProperties','tokens').V().has('name','marko').outE().inV().hasLabel('software').path()", gmodern).one().getPath();
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

    @Test
    public void shouldRoundTripSimplePointPdt() throws Exception {
        final List<Result> results = client.submit(
                "g.inject(PDT(\"Point\", [\"x\":1, \"y\":2]))").all().get();

        assertEquals(1, results.size());
        final ProviderDefinedType pdt = (ProviderDefinedType) results.get(0).getObject();
        assertEquals("Point", pdt.getName());
        assertEquals(1, pdt.getProperties().get("x"));
        assertEquals(2, pdt.getProperties().get("y"));
    }

    @Test
    public void shouldRoundTripNestedPdt() throws Exception {
        final List<Result> results = client.submit(
                "g.inject(PDT(\"Person\", [\"name\":\"Alice\", \"age\":30, " +
                "\"address\":PDT(\"Address\", [\"street\":\"123 Main St\", \"city\":\"Springfield\", \"zip\":\"12345\"])]))").all().get();

        assertEquals(1, results.size());
        final ProviderDefinedType person = (ProviderDefinedType) results.get(0).getObject();
        assertEquals("Person", person.getName());
        assertEquals("Alice", person.getProperties().get("name"));
        assertEquals(30, person.getProperties().get("age"));

        final ProviderDefinedType address = (ProviderDefinedType) person.getProperties().get("address");
        assertEquals("Address", address.getName());
        assertEquals("123 Main St", address.getProperties().get("street"));
        assertEquals("Springfield", address.getProperties().get("city"));
    }

    @Test
    public void shouldRoundTripPdtInCollection() throws Exception {
        final List<Result> results = client.submit(
                "g.inject([PDT(\"Point\", [\"x\":1, \"y\":2]), PDT(\"Point\", [\"x\":3, \"y\":4])])").all().get();

        assertEquals(1, results.size());
        final List<?> list = (List<?>) results.get(0).getObject();
        assertEquals(2, list.size());

        final ProviderDefinedType p1 = (ProviderDefinedType) list.get(0);
        assertEquals("Point", p1.getName());
        assertEquals(1, p1.getProperties().get("x"));
        assertEquals(2, p1.getProperties().get("y"));

        final ProviderDefinedType p2 = (ProviderDefinedType) list.get(1);
        assertEquals("Point", p2.getName());
        assertEquals(3, p2.getProperties().get("x"));
        assertEquals(4, p2.getProperties().get("y"));
    }

    @Test
    public void shouldReturnPdtAsGraphSONCompositePdtInHttpResponse() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader("Content-Type", "application/json");
        httppost.addHeader("Accept", "application/json");
        httppost.setEntity(new StringEntity(
                "{\"gremlin\":\"g.inject(org.apache.tinkerpop.gremlin.structure.io.pdt.ProviderDefinedType.from(new org.apache.tinkerpop.gremlin.server.pdt.Point(1, 2)))\",\"language\":\"gremlin-groovy\"}",
                Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            final JsonNode root = new ObjectMapper().readTree(EntityUtils.toString(response.getEntity()));
            final JsonNode pdtNode = root.get("result").get("data").get(GraphSONTokens.VALUEPROP).get(0);

            assertEquals("g:CompositePdt", pdtNode.get("@type").asText());
            final JsonNode value = pdtNode.get(GraphSONTokens.VALUEPROP);
            assertEquals("Point", value.get("type").asText());
            assertEquals(1, value.get("fields").get("x").get(GraphSONTokens.VALUEPROP).intValue());
            assertEquals(2, value.get("fields").get("y").get(GraphSONTokens.VALUEPROP).intValue());
        }
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
