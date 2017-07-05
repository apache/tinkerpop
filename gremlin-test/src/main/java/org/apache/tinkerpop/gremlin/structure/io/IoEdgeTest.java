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
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.graphson.TypeInfo;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for all IO implementations that are specific to reading and writing of a {@link Edge}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Parameterized.class)
public class IoEdgeTest extends AbstractGremlinTest {

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"graphson-v1", false, false,
                        (Function<Graph,GraphReader>) g -> g.io(IoCore.graphson()).reader().create(),
                        (Function<Graph, GraphWriter>) g -> g.io(IoCore.graphson()).writer().create()},
                {"graphson-v1-embedded", true, true,
                        (Function<Graph,GraphReader>) g -> g.io(IoCore.graphson()).reader().mapper(g.io(IoCore.graphson()).mapper().embedTypes(true).create()).create(),
                        (Function<Graph, GraphWriter>) g -> g.io(IoCore.graphson()).writer().mapper(g.io(IoCore.graphson()).mapper().embedTypes(true).create()).create()},
                {"graphson-v2", false, false,
                        (Function<Graph, GraphReader>) g -> g.io(IoCore.graphson()).reader().mapper(g.io(GraphSONIo.build(GraphSONVersion.V2_0)).mapper().typeInfo(TypeInfo.NO_TYPES).create()).create(),
                        (Function<Graph, GraphWriter>) g -> g.io(IoCore.graphson()).writer().mapper(g.io(GraphSONIo.build(GraphSONVersion.V2_0)).mapper().typeInfo(TypeInfo.NO_TYPES).create()).create()},
                {"graphson-v2-embedded", true, true,
                        (Function<Graph, GraphReader>) g -> g.io(IoCore.graphson()).reader().mapper(g.io(GraphSONIo.build(GraphSONVersion.V2_0)).mapper().typeInfo(TypeInfo.PARTIAL_TYPES).create()).create(),
                        (Function<Graph, GraphWriter>) g -> g.io(IoCore.graphson()).writer().mapper(g.io(GraphSONIo.build(GraphSONVersion.V2_0)).mapper().typeInfo(TypeInfo.PARTIAL_TYPES).create()).create()},
                {"graphson-v3", true, true,
                        (Function<Graph, GraphReader>) g -> g.io(IoCore.graphson()).reader().mapper(g.io(GraphSONIo.build(GraphSONVersion.V3_0)).mapper().create()).create(),
                        (Function<Graph, GraphWriter>) g -> g.io(IoCore.graphson()).writer().mapper(g.io(GraphSONIo.build(GraphSONVersion.V3_0)).mapper().create()).create()},
                {"gryo", true, true,
                        (Function<Graph,GraphReader>) g -> g.io(IoCore.gryo()).reader().create(),
                        (Function<Graph, GraphWriter>) g -> g.io(IoCore.gryo()).writer().create()}
        });
    }

    @Parameterized.Parameter(value = 0)
    public String ioType;

    @Parameterized.Parameter(value = 1)
    public boolean assertIdDirectly;

    @Parameterized.Parameter(value = 2)
    public boolean assertDouble;

    @Parameterized.Parameter(value = 3)
    public Function<Graph, GraphReader> readerMaker;

    @Parameterized.Parameter(value = 4)
    public Function<Graph, GraphWriter> writerMaker;

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteEdge() throws Exception {
        final Vertex v1 = graph.addVertex(T.label, "person");
        final Vertex v2 = graph.addVertex(T.label, "person");
        final Edge e = v1.addEdge("friend", v2, "weight", 0.5d, "acl", "rw");

        assertEdge(v1, v2, e, true);
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteDetachedEdge() throws Exception {
        final Vertex v1 = graph.addVertex(T.label, "person");
        final Vertex v2 = graph.addVertex(T.label, "person");
        final Edge e = DetachedFactory.detach(v1.addEdge("friend", v2, "weight", 0.5d, "acl", "rw"), true);

        assertEdge(v1, v2, e, true);
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldReadWriteDetachedEdgeAsReference() throws Exception {
        final Vertex v1 = graph.addVertex(T.label, "person");
        final Vertex v2 = graph.addVertex(T.label, "person");
        final Edge e = DetachedFactory.detach(v1.addEdge("friend", v2, "weight", 0.5d, "acl", "rw"), false);

        assertEdge(v1, v2, e, false);
    }

    private void assertEdge(final Vertex v1, final Vertex v2, final Edge e, final boolean assertProperties) throws IOException {
        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphWriter writer = writerMaker.apply(graph);
            writer.writeEdge(os, e);

            final AtomicBoolean called = new AtomicBoolean(false);
            final GraphReader reader = readerMaker.apply(graph);
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readEdge(bais, edge -> {
                    final Edge detachedEdge = (Edge) edge;
                    assertEquals(e.id(), assertIdDirectly ? detachedEdge.id() : graph.edges(detachedEdge.id().toString()).next().id());
                    assertEquals(v1.id(), assertIdDirectly ? detachedEdge.outVertex().id() : graph.vertices(detachedEdge.outVertex().id().toString()).next().id());
                    assertEquals(v2.id(), assertIdDirectly ? detachedEdge.inVertex().id() : graph.vertices(detachedEdge.inVertex().id().toString()).next().id());
                    assertEquals(v1.label(), detachedEdge.outVertex().label());
                    assertEquals(v2.label(), detachedEdge.inVertex().label());
                    assertEquals(e.label(), detachedEdge.label());

                    if (assertProperties) {
                        assertEquals(assertDouble ? 0.5d : 0.5f, e.properties("weight").next().value());
                        assertEquals("rw", e.properties("acl").next().value());
                    } else {
                        assertEquals(e.keys().size(), IteratorUtils.count(detachedEdge.properties()));
                    }

                    called.set(true);
                    return null;
                });
            }

            assertTrue(called.get());
        }
    }
}
