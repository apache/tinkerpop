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
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.IoTest;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryMapper;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferencePath;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV4;
import org.apache.tinkerpop.gremlin.util.ser.Serializers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Parameterized.class)
public class GremlinResultSetIntegrateTest extends AbstractGremlinServerIntegrationTest {

    private Cluster cluster;
    private Client client;

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> data() {
        final MessageSerializer<GraphBinaryMapper> graphBinaryMessageSerializerV4 = new GraphBinaryMessageSerializerV4();

        return Arrays.asList(new Object[][]{
                {Serializers.GRAPHBINARY_V4, graphBinaryMessageSerializerV4}
        });
    }

    @Parameterized.Parameter(value = 0)
    public Serializers name;

    @Parameterized.Parameter(value = 1)
    public MessageSerializer<?> messageSerializer;

    @Before
    public void beforeTest() {
        cluster = TestClientFactory.build().serializer(messageSerializer).create();
        client = cluster.connect();
    }

    @After
    public void afterTest() {
        cluster.close();
    }

    @Test
    public void shouldHandleNullResult() throws Exception {
        final ResultSet results = client.submit("g.inject(null)");
        assertNull(results.all().get().get(0).getObject());
    }

    @Test
    public void shouldHandleVoidResult() throws Exception {
        final ResultSet results = client.submit("g.V().drop().iterate()");
        assertEquals(0, results.all().get().size());
    }

    @Test
    public void shouldHandleEmptyResult() throws Exception {
        RequestOptions options = RequestOptions.build().addG("gmodern").create();
        final ResultSet results = client.submit("g.V(100,1000,1000)", options);
        assertEquals(0, results.all().get().size());
    }

    @Test
    public void shouldHandleVertexResult() throws Exception {
        RequestOptions options = RequestOptions.build().addG("gmodern").create();
        final ResultSet results = client.submit("g.V(1).next()", options);
        final Vertex v = results.all().get().get(0).getVertex();
        assertThat(v, instanceOf(DetachedVertex.class));
    }

    @Test
    public void shouldHandleVertexPropertyResult() throws Exception {
        RequestOptions options = RequestOptions.build().addG("gmodern").create();
        final ResultSet results = client.submit("g.V().properties('name').next()", options);
        final VertexProperty<String> v = results.all().get().get(0).getVertexProperty();
        assertThat(v, instanceOf(DetachedVertexProperty.class));
    }

    @Test
    public void shouldHandleEdgeResult() throws Exception {
        RequestOptions options = RequestOptions.build().addG("gmodern").create();
        final ResultSet results = client.submit("g.E().next()", options);
        final Edge e = results.all().get().get(0).getEdge();
        assertThat(e, instanceOf(DetachedEdge.class));
    }

    @Test
    public void shouldHandlePropertyResult() throws Exception {
        RequestOptions options = RequestOptions.build().addG("gmodern").create();
        final ResultSet results = client.submit("g.E().properties('weight').next()", options);
        final Property<Double> p = results.all().get().get(0).getProperty();
        assertThat(p, instanceOf(ReferenceProperty.class));
    }

    @Test
    public void shouldHandlePathResult() throws Exception {
        RequestOptions options = RequestOptions.build().addG("gmodern").create();
        final ResultSet results = client.submit("g.V().out().path()", options);
        final Path p = results.all().get().get(0).getPath();
        assertThat(p, instanceOf(ReferencePath.class));
    }

    @Test
    public void shouldHandleTinkerGraphResult() throws Exception {
        RequestOptions options = RequestOptions.build().language("gremlin-groovy").create();
        final ResultSet results = client.submit("modern", options);
        final Graph graph = results.all().get().get(0).get(TinkerGraph.class);

        // test is "lossy for id" because TinkerGraph is configured by default to use the ANY id manager
        // and doesn't coerce to specific types - which is how it is on the server as well so we can expect
        // some id shiftiness
        IoTest.assertModernGraph(graph, true, true);
    }

    @Test
    public void shouldHandleMapIteratedResult() throws Exception {
        RequestOptions options = RequestOptions.build().addG("gmodern").create();
        final ResultSet results = client.submit("g.V().groupCount().by(bothE().count())", options);
        final List<Result> resultList = results.all().get();
        final Map m = resultList.get(0).get(HashMap.class);
        assertEquals(2, m.size());
        assertEquals(3L, m.get(1L));
        assertEquals(3L, m.get(3L));
    }
}
