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
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
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
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
import org.apache.tinkerpop.gremlin.util.ser.Serializers;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
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
        final MessageSerializer<GraphBinaryMapper> graphBinaryMessageSerializerV1 = new GraphBinaryMessageSerializerV1();

        return Arrays.asList(new Object[][]{
                {Serializers.GRAPHBINARY_V1, graphBinaryMessageSerializerV1}
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
    public void shouldReturnResponseAttributesViaNoContent() throws Exception {
        final ResultSet results = client.submit("[]");
        final Map<String,Object> attr = results.statusAttributes().get(20000, TimeUnit.MILLISECONDS);
        assertThat(attr.containsKey(Tokens.ARGS_HOST), is(true));
    }

    @Test
    public void shouldReturnResponseAttributesViaSuccess() throws Exception {
        final ResultSet results = client.submit("gmodern.V()");
        final Map<String,Object> attr = results.statusAttributes().get(20000, TimeUnit.MILLISECONDS);
        assertThat(attr.containsKey(Tokens.ARGS_HOST), is(true));
    }

    @Test
    public void shouldHandleVertexResultFromTraversalBulked() throws Exception {
        final Graph graph = TinkerGraph.open();
        final GraphTraversalSource g = graph.traversal();
        final Client aliased = client.alias("gmodern");
        final ResultSet resultSetUnrolled = aliased.submit(g.V().both().barrier().both().barrier());
        final List<Result> results = resultSetUnrolled.all().get();

        assertThat(results.get(0).getObject(), CoreMatchers.instanceOf(Traverser.class));
        assertEquals(6, results.size());
    }

    @Test
    public void shouldHandleNullResult() throws Exception {
        final ResultSet results = client.submit("gmodern.V().drop().iterate();null");
        assertNull(results.all().get().get(0).getObject());
    }

    @Test
    public void shouldHandleVoidResult() throws Exception {
        final ResultSet results = client.submit("gmodern.V().drop().iterate()");
        assertEquals(0, results.all().get().size());
    }

    @Test
    public void shouldHandleEmptyResult() throws Exception {
        final ResultSet results = client.submit("gmodern.V(100,1000,1000)");
        assertEquals(0, results.all().get().size());
    }

    @Test
    public void shouldHandleVertexResult() throws Exception {
        final ResultSet results = client.submit("gmodern.V(1).next()");
        final Vertex v = results.all().get().get(0).getVertex();
        assertThat(v, instanceOf(DetachedVertex.class));
    }

    @Test
    public void shouldHandleVertexPropertyResult() throws Exception {
        final ResultSet results = client.submit("gmodern.V().properties('name').next()");
        final VertexProperty<String> v = results.all().get().get(0).getVertexProperty();
        assertThat(v, instanceOf(DetachedVertexProperty.class));
    }

    @Test
    public void shouldHandleEdgeResult() throws Exception {
        final ResultSet results = client.submit("gmodern.E().next()");
        final Edge e = results.all().get().get(0).getEdge();
        assertThat(e, instanceOf(DetachedEdge.class));
    }

    @Test
    public void shouldHandlePropertyResult() throws Exception {
        final ResultSet results = client.submit("gmodern.E().properties('weight').next()");
        final Property<Double> p = results.all().get().get(0).getProperty();
        assertThat(p, instanceOf(ReferenceProperty.class));
    }

    @Test
    public void shouldHandlePathResult() throws Exception {
        final ResultSet results = client.submit("gmodern.V().out().path()");
        final Path p = results.all().get().get(0).getPath();
        assertThat(p, instanceOf(ReferencePath.class));
    }

    @Test
    public void shouldHandleTinkerGraphResult() throws Exception {
        final ResultSet results = client.submit("modern");
        final Graph graph = results.all().get().get(0).get(TinkerGraph.class);

        // test is "lossy for id" because TinkerGraph is configured by default to use the ANY id manager
        // and doesn't coerce to specific types - which is how it is on the server as well so we can expect
        // some id shiftiness
        IoTest.assertModernGraph(graph, true, true);
    }

    @Test
    public void shouldHandleMapIteratedResult() throws Exception {
        final ResultSet results = client.submit("gmodern.V().groupCount().by(bothE().count())");
        final List<Result> resultList = results.all().get();
        final Map m = resultList.get(0).get(HashMap.class);
        assertEquals(2, m.size());
        assertEquals(3L, m.get(1L));
        assertEquals(3L, m.get(3L));
    }
}
