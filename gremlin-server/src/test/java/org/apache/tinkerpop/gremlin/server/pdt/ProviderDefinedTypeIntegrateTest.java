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
package org.apache.tinkerpop.gremlin.server.pdt;

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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.server.AbstractGremlinServerIntegrationTest;
import org.apache.tinkerpop.gremlin.server.TestClientFactory;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import org.apache.tinkerpop.gremlin.structure.io.pdt.ProviderDefinedType;
import org.apache.tinkerpop.gremlin.structure.io.pdt.ProviderDefinedTypeAdapter;
import org.apache.tinkerpop.gremlin.structure.io.pdt.ProviderDefinedTypeRegistry;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.junit.After;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests verifying that Provider Defined Types (PDT) flow end-to-end through gremlin-server.
 * <p>
 * The PDT serialization converts {@code @ProviderDefined}-annotated objects to {@link ProviderDefinedType}
 * during GraphBinary serialization. On the client side, values are always deserialized as
 * {@link ProviderDefinedType} (unless a {@code ProviderDefinedTypeAdapter} is registered).
 */
public class ProviderDefinedTypeIntegrateTest extends AbstractGremlinServerIntegrationTest {

    private static final RequestOptions GROOVY = RequestOptions.build().language("gremlin-groovy").create();

    private final Supplier<Graph> graphGetter =
            () -> server.getServerGremlinExecutor().getGraphManager().getGraph("graph");

    @After
    public void cleanup() {
        final Graph graph = graphGetter.get();
        graph.traversal().V().hasLabel("location").drop().iterate();
    }

    @Test
    public void shouldStoreAndRetrievePdtViaGremlinLang() throws Exception {
        final Cluster cluster = TestClientFactory.build().create();
        final Client client = cluster.connect();

        try {
            // Store a Point as a vertex property using gremlin-lang PDT literal
            client.submit(
                    "g.addV('location').property('point', PDT(\"Point\", [\"x\":1, \"y\":2])).iterate()").all().get();

            // Retrieve the property value - it should come back as ProviderDefinedType
            final List<Result> results = client.submit(
                    "g.V().hasLabel('location').values('point')").all().get();

            assertEquals(1, results.size());
            final Object obj = results.get(0).getObject();
            assertTrue("Expected ProviderDefinedType but got: " + obj.getClass().getName(),
                    obj instanceof ProviderDefinedType);

            final ProviderDefinedType pdt = (ProviderDefinedType) obj;
            assertEquals("Point", pdt.getName());
            assertEquals(2, pdt.getProperties().size());
            assertEquals(1, pdt.getProperties().get("x"));
            assertEquals(2, pdt.getProperties().get("y"));
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldRetrievePdtViaBytecodeTraversal() throws Exception {
        final Cluster cluster = TestClientFactory.build().create();
        final Client client = cluster.connect();

        try {
            // Store via gremlin-lang PDT literal
            client.submit(
                    "g.addV('location').property('point', PDT(\"Point\", [\"x\":10, \"y\":20])).iterate()").all().get();

            // Retrieve via bytecode traversal (gremlin-lang)
            final GraphTraversalSource g = traversal().with(DriverRemoteConnection.using(cluster));
            final Object result = g.V().hasLabel("location").values("point").next();

            assertTrue("Expected ProviderDefinedType but got: " + result.getClass().getName(),
                    result instanceof ProviderDefinedType);

            final ProviderDefinedType pdt = (ProviderDefinedType) result;
            assertEquals("Point", pdt.getName());
            assertEquals(10, pdt.getProperties().get("x"));
            assertEquals(20, pdt.getProperties().get("y"));
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldStoreAndRetrievePdtWithAdditionalProperties() throws Exception {
        final Cluster cluster = TestClientFactory.build().create();
        final Client client = cluster.connect();

        try {
            // Store via gremlin-lang PDT literal with additional vertex properties
            client.submit(
                    "g.addV('location').property('point', PDT(\"Point\", [\"x\":5, \"y\":6])).property('name', 'office')").all().get();

            // Retrieve via bytecode traversal and verify PDT alongside normal properties
            final GraphTraversalSource g = traversal().with(DriverRemoteConnection.using(cluster));
            final Object pointValue = g.V().hasLabel("location").values("point").next();

            assertTrue("Expected ProviderDefinedType but got: " + pointValue.getClass().getName(),
                    pointValue instanceof ProviderDefinedType);

            final ProviderDefinedType pdt = (ProviderDefinedType) pointValue;
            assertEquals("Point", pdt.getName());
            assertEquals(5, pdt.getProperties().get("x"));
            assertEquals(6, pdt.getProperties().get("y"));
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldStorePdtAsOriginalObjectInTinkerGraph() throws Exception {
        final Cluster cluster = TestClientFactory.build().create();
        final Client client = cluster.connect();

        try {
            // Store a Point via Groovy script (needed to create the actual Point object in TinkerGraph)
            client.submit(
                    "g.addV('location').property('point', new org.apache.tinkerpop.gremlin.server.pdt.Point(3, 4)).iterate()",
                    GROOVY).all().get();

            // Verify TinkerGraph stores the original Point object (not a ProviderDefinedType)
            final Graph graph = graphGetter.get();
            final Vertex v = graph.traversal().V().hasLabel("location").next();
            final Object storedValue = v.property("point").value();

            assertTrue("TinkerGraph should store the original Point object but got: " + storedValue.getClass().getName(),
                    storedValue instanceof Point);

            final Point storedPoint = (Point) storedValue;
            assertEquals(3, storedPoint.x);
            assertEquals(4, storedPoint.y);
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldRoundTripPdtViaInjectScript() throws Exception {
        final Cluster cluster = TestClientFactory.build().create();
        final Client client = cluster.connect();

        try {
            // Inject a Point directly via gremlin-lang PDT literal
            final List<Result> results = client.submit(
                    "g.inject(PDT(\"Point\", [\"x\":7, \"y\":8]))").all().get();

            assertEquals(1, results.size());
            final Object obj = results.get(0).getObject();
            assertTrue("Expected ProviderDefinedType but got: " + obj.getClass().getName(),
                    obj instanceof ProviderDefinedType);

            final ProviderDefinedType pdt = (ProviderDefinedType) obj;
            assertEquals("Point", pdt.getName());
            assertEquals(7, pdt.getProperties().get("x"));
            assertEquals(8, pdt.getProperties().get("y"));
        } finally {
            cluster.close();
        }
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
            assertEquals("application/json", response.getEntity().getContentType().getValue());

            final String json = EntityUtils.toString(response.getEntity());
            final ObjectMapper objectMapper = new ObjectMapper();
            final JsonNode root = objectMapper.readTree(json);

            // Navigate: result.data.@value[0] should be the PDT
            final JsonNode pdtNode = root.get("result").get("data")
                    .get(GraphSONTokens.VALUEPROP).get(0);

            assertEquals("g:CompositePdt", pdtNode.get("@type").asText());

            final JsonNode value = pdtNode.get(GraphSONTokens.VALUEPROP);
            assertEquals("Point", value.get("type").asText());

            final JsonNode fields = value.get("fields");
            assertEquals("g:Int32", fields.get("x").get("@type").asText());
            assertEquals(1, fields.get("x").get(GraphSONTokens.VALUEPROP).intValue());
            assertEquals("g:Int32", fields.get("y").get("@type").asText());
            assertEquals(2, fields.get("y").get(GraphSONTokens.VALUEPROP).intValue());
        }
    }

    @Test
    public void shouldHydratePdtViaRegistryFromDriverResult() throws Exception {
        final Cluster cluster = TestClientFactory.build().create();
        final Client client = cluster.connect();

        try {
            // Set up a client-side registry with a Point adapter
            final ProviderDefinedTypeRegistry registry = ProviderDefinedTypeRegistry.empty();
            registry.register(new ProviderDefinedTypeAdapter<Point>() {
                @Override public String typeName() { return "Point"; }
                @Override public Class<Point> targetClass() { return Point.class; }
                @Override public Map<String, Object> toProperties(final Point obj) {
                    final Map<String, Object> m = new HashMap<>();
                    m.put("x", obj.x);
                    m.put("y", obj.y);
                    return m;
                }
                @Override public Point fromProperties(final Map<String, Object> properties) {
                    return new Point((int) properties.get("x"), (int) properties.get("y"));
                }
            });

            // Retrieve a PDT from the server via the driver
            final List<Result> results = client.submit(
                    "g.inject(new org.apache.tinkerpop.gremlin.server.pdt.Point(9, 11))",
                    GROOVY).all().get();

            assertEquals(1, results.size());
            final Object raw = results.get(0).getObject();
            assertTrue(raw instanceof ProviderDefinedType);

            // Hydrate the PDT using the client-side registry
            final Object hydrated = registry.hydrate((ProviderDefinedType) raw);
            assertTrue("Expected Point but got: " + hydrated.getClass().getName(),
                    hydrated instanceof Point);

            final Point point = (Point) hydrated;
            assertEquals(9, point.x);
            assertEquals(11, point.y);
        } finally {
            cluster.close();
        }
    }
}
