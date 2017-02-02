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
package org.apache.tinkerpop.gremlin.structure.io;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.FeatureRequirement;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.graphson.TypeInfo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.VertexByteArrayInputStream;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.apache.tinkerpop.gremlin.structure.Graph.Features.DataTypeFeatures.FEATURE_STRING_VALUES;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;
import static org.mockito.Mockito.mock;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Parameterized.class)
public class IoVertexTest extends AbstractGremlinTest {

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"graphson-v1", false, false,
                        (Function<Graph, GraphReader>) g -> g.io(IoCore.graphson()).reader().create(),
                        (Function<Graph, GraphWriter>) g -> g.io(IoCore.graphson()).writer().create()},
                {"graphson-v1-embedded", true, false,
                        (Function<Graph, GraphReader>) g -> g.io(IoCore.graphson()).reader().mapper(g.io(IoCore.graphson()).mapper().embedTypes(true).create()).create(),
                        (Function<Graph, GraphWriter>) g -> g.io(IoCore.graphson()).writer().mapper(g.io(IoCore.graphson()).mapper().embedTypes(true).create()).create()},
                {"graphson-v2", false, false,
                        (Function<Graph, GraphReader>) g -> g.io(IoCore.graphson()).reader().mapper(g.io(GraphSONIo.build(GraphSONVersion.V2_0)).mapper().typeInfo(TypeInfo.NO_TYPES).create()).create(),
                        (Function<Graph, GraphWriter>) g -> g.io(IoCore.graphson()).writer().mapper(g.io(GraphSONIo.build(GraphSONVersion.V2_0)).mapper().typeInfo(TypeInfo.NO_TYPES).create()).create()},
                {"graphson-v2-embedded", true, false,
                        (Function<Graph, GraphReader>) g -> g.io(IoCore.graphson()).reader().mapper(g.io(GraphSONIo.build(GraphSONVersion.V2_0)).mapper().typeInfo(TypeInfo.PARTIAL_TYPES).create()).create(),
                        (Function<Graph, GraphWriter>) g -> g.io(IoCore.graphson()).writer().mapper(g.io(GraphSONIo.build(GraphSONVersion.V2_0)).mapper().typeInfo(TypeInfo.PARTIAL_TYPES).create()).create()},
                {"graphson-v3", true, false,
                        (Function<Graph, GraphReader>) g -> g.io(IoCore.graphson()).reader().mapper(g.io(GraphSONIo.build(GraphSONVersion.V3_0)).mapper().create()).create(),
                        (Function<Graph, GraphWriter>) g -> g.io(IoCore.graphson()).writer().mapper(g.io(GraphSONIo.build(GraphSONVersion.V3_0)).mapper().create()).create()},
                {"gryo", true, true,
                        (Function<Graph, GraphReader>) g -> g.io(IoCore.gryo()).reader().create(),
                        (Function<Graph, GraphWriter>) g -> g.io(IoCore.gryo()).writer().create()}
        });
    }

    @Parameterized.Parameter(value = 0)
    public String ioType;

    @Parameterized.Parameter(value = 1)
    public boolean assertViaDirectEquality;

    @Parameterized.Parameter(value = 2)
    public boolean assertEdgesAtSameTimeAsVertex;

    @Parameterized.Parameter(value = 3)
    public Function<Graph, GraphReader> readerMaker;

    @Parameterized.Parameter(value = 4)
    public Function<Graph, GraphWriter> writerMaker;

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteVertexWithBOTHEdges() throws Exception {
        final Vertex v1 = graph.addVertex("name", "marko", T.label, "person");

        final Vertex v2 = graph.addVertex(T.label, "person");
        final Edge e1 = v2.addEdge("friends", v1, "weight", 0.5d);
        final Edge e2 = v1.addEdge("friends", v2, "weight", 1.0d);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphWriter writer = writerMaker.apply(graph);
            writer.writeVertex(os, v1, Direction.BOTH);

            final AtomicBoolean calledVertex = new AtomicBoolean(false);
            final AtomicBoolean calledEdge1 = new AtomicBoolean(false);
            final AtomicBoolean calledEdge2 = new AtomicBoolean(false);

            final GraphReader reader = readerMaker.apply(graph);
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais, attachable -> {
                    final Vertex detachedVertex = attachable.get();
                    if (assertViaDirectEquality) {
                        TestHelper.validateVertexEquality(v1, detachedVertex, assertEdgesAtSameTimeAsVertex);
                    } else {
                        assertEquals(v1.id(), graph.vertices(detachedVertex.id().toString()).next().id());
                        assertEquals(v1.label(), detachedVertex.label());
                        assertEquals(1, IteratorUtils.count(detachedVertex.properties()));
                        assertEquals("marko", detachedVertex.value("name"));
                    }
                    calledVertex.set(true);
                    return detachedVertex;
                }, attachable -> {
                    final Edge detachedEdge = attachable.get();
                    final Predicate<Edge> matcher = assertViaDirectEquality ? e -> detachedEdge.id().equals(e.id()) :
                            e -> graph.edges(detachedEdge.id().toString()).next().id().equals(e.id());
                    if (matcher.test(e1)) {
                        if (assertViaDirectEquality) {
                            TestHelper.validateEdgeEquality(e1, detachedEdge);
                        } else {
                            assertEquals(e1.id(), graph.edges(detachedEdge.id().toString()).next().id());
                            assertEquals(v1.id(), graph.vertices(detachedEdge.inVertex().id().toString()).next().id());
                            assertEquals(v2.id(), graph.vertices(detachedEdge.outVertex().id().toString()).next().id());
                            assertEquals(v2.label(), detachedEdge.inVertex().label());
                            assertEquals(e1.label(), detachedEdge.label());
                            assertEquals(1, IteratorUtils.count(detachedEdge.properties()));
                            assertEquals(0.5d, detachedEdge.value("weight"), 0.000001d);
                        }
                        calledEdge1.set(true);
                    } else if (matcher.test(e2)) {
                        if (assertViaDirectEquality) {
                            TestHelper.validateEdgeEquality(e2, detachedEdge);
                        } else {
                            assertEquals(e2.id(), graph.edges(detachedEdge.id().toString()).next().id());
                            assertEquals(v2.id(), graph.vertices(detachedEdge.inVertex().id().toString()).next().id());
                            assertEquals(v1.id(), graph.vertices(detachedEdge.outVertex().id().toString()).next().id());
                            assertEquals(v1.label(), detachedEdge.outVertex().label());
                            assertEquals(e2.label(), detachedEdge.label());
                            assertEquals(1, IteratorUtils.count(detachedEdge.properties()));
                            assertEquals(1.0d, detachedEdge.value("weight"), 0.000001d);
                        }
                        calledEdge2.set(true);
                    } else {
                        fail("An edge id generated that does not exist");
                    }

                    return null;
                }, Direction.BOTH);
            }

            assertTrue(calledVertex.get());
            assertTrue(calledEdge1.get());
            assertTrue(calledEdge2.get());
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteVertexWithINEdges() throws Exception {
        final Vertex v1 = graph.addVertex("name", "marko", T.label, "person");

        final Vertex v2 = graph.addVertex(T.label, "person");
        final Edge e = v2.addEdge("friends", v1, "weight", 0.5d);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphWriter writer = writerMaker.apply(graph);
            writer.writeVertex(os, v1, Direction.IN);

            final AtomicBoolean calledVertex = new AtomicBoolean(false);
            final AtomicBoolean calledEdge = new AtomicBoolean(false);

            final GraphReader reader = readerMaker.apply(graph);
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais, attachable -> {
                    final Vertex detachedVertex = attachable.get();
                    if (assertViaDirectEquality) {
                        TestHelper.validateVertexEquality(v1, detachedVertex, assertEdgesAtSameTimeAsVertex);
                    } else {
                        assertEquals(v1.id(), graph.vertices(detachedVertex.id().toString()).next().id());
                        assertEquals(v1.label(), detachedVertex.label());
                        assertEquals(1, IteratorUtils.count(detachedVertex.properties()));
                        assertEquals("marko", detachedVertex.value("name"));
                    }
                    calledVertex.set(true);
                    return detachedVertex;
                }, attachable -> {
                    final Edge detachedEdge = attachable.get();
                    if (assertViaDirectEquality)
                        TestHelper.validateEdgeEquality(e, detachedEdge);
                    else {
                        assertEquals(e.id(), graph.edges(detachedEdge.id().toString()).next().id());
                        assertEquals(v1.id(), graph.vertices(detachedEdge.inVertex().id().toString()).next().id());
                        assertEquals(v2.id(), graph.vertices(detachedEdge.outVertex().id().toString()).next().id());
                        assertEquals(v2.label(), detachedEdge.inVertex().label());
                        assertEquals(e.label(), detachedEdge.label());
                        assertEquals(1, IteratorUtils.count(detachedEdge.properties()));
                        assertEquals(0.5d, detachedEdge.value("weight"), 0.000001d);
                    }
                    calledEdge.set(true);

                    return detachedEdge;
                }, Direction.IN);
            }

            assertTrue(calledVertex.get());
            assertTrue(calledEdge.get());
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteVertexWithOUTEdges() throws Exception {
        final Vertex v1 = graph.addVertex("name", "marko", T.label, "person");

        final Vertex v2 = graph.addVertex(T.label, "person");
        final Edge e = v1.addEdge("friends", v2, "weight", 0.5d);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphWriter writer = writerMaker.apply(graph);
            writer.writeVertex(os, v1, Direction.OUT);

            final AtomicBoolean calledVertex = new AtomicBoolean(false);
            final AtomicBoolean calledEdge = new AtomicBoolean(false);
            final GraphReader reader = readerMaker.apply(graph);

            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais, attachable -> {
                            final Vertex detachedVertex = attachable.get();
                            if (assertViaDirectEquality) {
                                TestHelper.validateVertexEquality(v1, detachedVertex, assertEdgesAtSameTimeAsVertex);
                            } else {
                                assertEquals(v1.id(), graph.vertices(detachedVertex.id().toString()).next().id());
                                assertEquals(v1.label(), detachedVertex.label());
                                assertEquals(1, IteratorUtils.count(detachedVertex.properties()));
                                assertEquals("marko", detachedVertex.value("name"));
                            }
                            calledVertex.set(true);
                            return detachedVertex;
                        },
                        attachable -> {
                            final Edge detachedEdge = attachable.get();
                            if (assertViaDirectEquality)
                                TestHelper.validateEdgeEquality(e, detachedEdge);
                            else {
                                assertEquals(e.id(), graph.edges(detachedEdge.id().toString()).next().id());
                                assertEquals(v1.id(), graph.vertices(detachedEdge.outVertex().id().toString()).next().id());
                                assertEquals(v2.id(), graph.vertices(detachedEdge.inVertex().id().toString()).next().id());
                                assertEquals(v1.label(), detachedEdge.outVertex().label());
                                assertEquals(e.label(), detachedEdge.label());
                                assertEquals(1, IteratorUtils.count(detachedEdge.properties()));
                                assertEquals(0.5d, detachedEdge.value("weight"), 0.000001d);
                            }
                            calledEdge.set(true);

                            return detachedEdge;
                        }, Direction.OUT);
            }

            assertTrue(calledVertex.get());
            assertTrue(calledEdge.get());
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteVertexNoEdges() throws Exception {
        final Vertex v1 = graph.addVertex("name", "marko", "acl", "rw");
        final Vertex v2 = graph.addVertex();
        v1.addEdge("friends", v2, "weight", 0.5d);
        assertVertexToSerialize(v1, true);
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteDetachedVertexNoEdges() throws Exception {
        final Vertex v1 = graph.addVertex("name", "marko", "acl", "rw");
        final Vertex v2 = graph.addVertex();
        v1.addEdge("friends", v2, "weight", 0.5d);
        assertVertexToSerialize(DetachedFactory.detach(v1, true), true);
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteDetachedVertexAsReferenceNoEdges() throws Exception {
        final Vertex v1 = graph.addVertex("name", "marko", "acl", "rw");
        final Vertex v2 = graph.addVertex();
        v1.addEdge("friends", v2, "weight", 0.5d);
        assertVertexToSerialize(DetachedFactory.detach(v1, false), false);
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_MULTI_PROPERTIES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_META_PROPERTIES)
    public void shouldReadWriteVertexMultiPropsNoEdges() throws Exception {
        final Vertex v1 = graph.addVertex("name", "marko", "name", "mark", "acl", "rw");
        v1.property(VertexProperty.Cardinality.single, "propsSquared", 123, "x", "a", "y", "b");
        final Vertex v2 = graph.addVertex();
        v1.addEdge("friends", v2, "weight", 0.5d);

        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphWriter writer = writerMaker.apply(graph);
            writer.writeVertex(os, v1);

            final AtomicBoolean called = new AtomicBoolean(false);
            final GraphReader reader = readerMaker.apply(graph);
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais, attachable -> {
                    final Vertex detachedVertex = attachable.get();
                    assertEquals(v1.id(), assertViaDirectEquality ? detachedVertex.id() : graph.vertices(detachedVertex.id().toString()).next().id());
                    assertEquals(v1.label(), detachedVertex.label());
                    assertEquals(4, IteratorUtils.count(detachedVertex.properties()));
                    assertEquals("a", detachedVertex.property("propsSquared").value("x"));
                    assertEquals("b", detachedVertex.property("propsSquared").value("y"));
                    assertEquals(2, IteratorUtils.count(detachedVertex.properties("name")));
                    assertTrue(IteratorUtils.stream(detachedVertex.properties("name")).allMatch(p -> p.key().equals("name") && (p.value().equals("marko") || p.value().equals("mark"))));
                    assertEquals(v1.value("acl"), detachedVertex.value("acl").toString());
                    called.set(true);
                    return mock(Vertex.class);
                });
            }
            assertTrue(called.get());
        }
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldReadWriteVerticesNoEdges() throws Exception {
        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphWriter writer = writerMaker.apply(graph);
            writer.writeVertices(os, g.V().has("age", P.gt(30)));

            final AtomicInteger called = new AtomicInteger(0);
            final GraphReader reader = readerMaker.apply(graph);

            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                final Iterator<Vertex> itty = reader.readVertices(bais,
                        attachable -> {
                            final Vertex detachedVertex = attachable.get();
                            called.incrementAndGet();
                            return detachedVertex;
                        }, null, null);

                assertNotNull(itty.next());
                assertNotNull(itty.next());
                assertFalse(itty.hasNext());
            }

            assertEquals(2, called.get());
        }
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC)
    public void shouldReadWriteVerticesNoEdgesToGraphSONManual() throws Exception {
        assumeThat(ioType, startsWith("graphson"));
        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphWriter writer = writerMaker.apply(graph);
            writer.writeVertices(os, g.V().has("age", P.gt(30)));

            final AtomicInteger called = new AtomicInteger(0);
            final GraphReader reader = readerMaker.apply(graph);
            try (final BufferedReader br = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(os.toByteArray())))) {
                String line = br.readLine();
                reader.readVertex(new ByteArrayInputStream(line.getBytes()),
                        attachable -> {
                            called.incrementAndGet();
                            return mock(Vertex.class);
                        });

                line = br.readLine();
                reader.readVertex(new ByteArrayInputStream(line.getBytes()),
                        detachedVertex -> {
                            called.incrementAndGet();
                            return mock(Vertex.class);
                        });
            }

            assertEquals(2, called.get());
        }
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC)
    public void shouldReadWriteVerticesNoEdgesToGryoManual() throws Exception {
        assumeThat(ioType, is("gryo"));
        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphWriter writer = writerMaker.apply(graph);
            writer.writeVertices(os, g.V().has("age", P.gt(30)));

            final AtomicInteger called = new AtomicInteger(0);
            final GraphReader reader = readerMaker.apply(graph);
            try (final VertexByteArrayInputStream vbais = new VertexByteArrayInputStream(new ByteArrayInputStream(os.toByteArray()))) {
                final byte[] y = vbais.readVertexBytes().toByteArray();
                reader.readVertex(new ByteArrayInputStream(y),
                        attachable -> {
                            final Vertex detachedVertex = attachable.get();
                            called.incrementAndGet();
                            return detachedVertex;
                        });

                final byte[] z = vbais.readVertexBytes().toByteArray();
                reader.readVertex(new ByteArrayInputStream(z),
                        attachable -> {
                            final Vertex detachedVertex = attachable.get();
                            called.incrementAndGet();
                            return detachedVertex;
                        });
            }

            assertEquals(2, called.get());
        }
    }

    private void assertVertexToSerialize(final Vertex toSerialize, final boolean assertProperties) throws IOException {
        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphWriter writer = writerMaker.apply(graph);
            writer.writeVertex(os, toSerialize);

            final AtomicBoolean called = new AtomicBoolean(false);
            final GraphReader reader = readerMaker.apply(graph);
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertex(bais, attachable -> {
                    final Vertex detachedVertex = attachable.get();
                    if (assertViaDirectEquality) {
                        TestHelper.validateVertexEquality(toSerialize, detachedVertex, false); // don't assert edges - there are none
                    } else {
                        assertEquals(toSerialize.id(), graph.vertices(detachedVertex.id().toString()).next().id());
                        assertEquals(toSerialize.label(), detachedVertex.label());
                        if (assertProperties) {
                            assertEquals(2, IteratorUtils.count(detachedVertex.properties()));
                            assertEquals(toSerialize.value("name"), detachedVertex.value("name").toString());
                            assertEquals(toSerialize.value("acl"), detachedVertex.value("acl").toString());
                        } else {
                            assertEquals(0, IteratorUtils.count(detachedVertex.properties()));
                        }
                    }

                    assertEquals(0, IteratorUtils.count(detachedVertex.edges(Direction.BOTH)));

                    called.set(true);
                    return mock(Vertex.class);
                });
            }
            assertTrue(called.get());
        }
    }
}
