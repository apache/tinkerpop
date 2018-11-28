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

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.FeatureRequirement;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.graphson.TypeInfo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoVersion;
import org.apache.tinkerpop.gremlin.structure.io.util.CustomId;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.tinkerpop.shaded.jackson.databind.module.SimpleModule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static org.apache.tinkerpop.gremlin.structure.Graph.Features.ElementFeatures.FEATURE_ANY_IDS;
import static org.apache.tinkerpop.gremlin.structure.Graph.Features.ElementFeatures.FEATURE_USER_SUPPLIED_IDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Parameterized.class)
public class IoCustomTest extends AbstractGremlinTest {

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> data() {
        final SimpleModule moduleV1d0 = new SimpleModule();
        moduleV1d0.addSerializer(CustomId.class, new CustomId.CustomIdJacksonSerializerV1d0());

        final SimpleModule moduleV2d0 = new CustomId.CustomIdTinkerPopJacksonModuleV2d0();
        final SimpleModule modulev3d0 = new CustomId.CustomIdTinkerPopJacksonModuleV3d0();

        return Arrays.asList(new Object[][]{
                {"graphson-v1-embedded", true,
                        (Function<Graph, GraphReader>) g -> g.io(GraphSONIo.build(GraphSONVersion.V1_0)).reader().mapper(g.io(GraphSONIo.build(GraphSONVersion.V1_0)).mapper().addCustomModule(moduleV1d0).typeInfo(TypeInfo.PARTIAL_TYPES).create()).create(),
                        (Function<Graph, GraphWriter>) g -> g.io(GraphSONIo.build(GraphSONVersion.V1_0)).writer().mapper(g.io(GraphSONIo.build(GraphSONVersion.V1_0)).mapper().addCustomModule(moduleV1d0).typeInfo(TypeInfo.PARTIAL_TYPES).create()).create()},
                {"graphson-v2-embedded", true,
                        (Function<Graph, GraphReader>) g -> g.io(GraphSONIo.build(GraphSONVersion.V2_0)).reader().mapper(g.io(GraphSONIo.build(GraphSONVersion.V2_0)).mapper().addCustomModule(moduleV2d0).typeInfo(TypeInfo.PARTIAL_TYPES).create()).create(),
                        (Function<Graph, GraphWriter>) g -> g.io(GraphSONIo.build(GraphSONVersion.V2_0)).writer().mapper(g.io(GraphSONIo.build(GraphSONVersion.V2_0)).mapper().addCustomModule(moduleV2d0).typeInfo(TypeInfo.PARTIAL_TYPES).create()).create()},
                {"graphson-v3", true,
                        (Function<Graph, GraphReader>) g -> g.io(GraphSONIo.build(GraphSONVersion.V3_0)).reader().mapper(g.io(GraphSONIo.build(GraphSONVersion.V3_0)).mapper().addCustomModule(modulev3d0).create()).create(),
                        (Function<Graph, GraphWriter>) g -> g.io(GraphSONIo.build(GraphSONVersion.V3_0)).writer().mapper(g.io(GraphSONIo.build(GraphSONVersion.V3_0)).mapper().addCustomModule(modulev3d0).create()).create()},
                {"gryo-v1", true,
                        (Function<Graph, GraphReader>) g -> g.io(GryoIo.build(GryoVersion.V1_0)).reader().mapper(g.io(GryoIo.build(GryoVersion.V1_0)).mapper().version(GryoVersion.V1_0).addCustom(CustomId.class).create()).create(),
                        (Function<Graph, GraphWriter>) g -> g.io(GryoIo.build(GryoVersion.V1_0)).writer().mapper(g.io(GryoIo.build(GryoVersion.V1_0)).mapper().version(GryoVersion.V1_0).addCustom(CustomId.class).create()).create()},
                {"gryo-v3", true,
                        (Function<Graph, GraphReader>) g -> g.io(GryoIo.build(GryoVersion.V3_0)).reader().mapper(g.io(GryoIo.build(GryoVersion.V3_0)).mapper().version(GryoVersion.V3_0).addCustom(CustomId.class).create()).create(),
                        (Function<Graph, GraphWriter>) g -> g.io(GryoIo.build(GryoVersion.V3_0)).writer().mapper(g.io(GryoIo.build(GryoVersion.V3_0)).mapper().version(GryoVersion.V3_0).addCustom(CustomId.class).create()).create()},
                {"gryo-v3d1", true,
                        (Function<Graph, GraphReader>) g -> g.io(GryoIo.build(GryoVersion.V3_1)).reader().mapper(g.io(GryoIo.build(GryoVersion.V3_1)).mapper().version(GryoVersion.V3_1).addCustom(CustomId.class).create()).create(),
                        (Function<Graph, GraphWriter>) g -> g.io(GryoIo.build(GryoVersion.V3_1)).writer().mapper(g.io(GryoIo.build(GryoVersion.V3_1)).mapper().version(GryoVersion.V3_1).addCustom(CustomId.class).create()).create()}
        });
    }

    @Parameterized.Parameter(value = 0)
    public String ioType;

    @Parameterized.Parameter(value = 1)
    public boolean assertIdDirectly;

    @Parameterized.Parameter(value = 2)
    public Function<Graph, GraphReader> readerMaker;

    @Parameterized.Parameter(value = 3)
    public Function<Graph, GraphWriter> writerMaker;

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_SERIALIZABLE_VALUES)
    public void shouldSupportUUID() throws Exception {
        final UUID id = UUID.randomUUID();
        final Vertex v1 = graph.addVertex(T.label, "person");
        final Vertex v2 = graph.addVertex(T.label, "person");
        final Edge e = v1.addEdge("friend", v2, "uuid", id);

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
                    assertEquals(e.keys().size(), IteratorUtils.count(detachedEdge.properties()));
                    assertEquals(id, detachedEdge.value("uuid"));

                    called.set(true);

                    return null;
                });
            }

            assertTrue(called.get());
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_ANY_IDS)
    public void shouldProperlySerializeCustomId() throws Exception {
        graph.addVertex(T.id, new CustomId("vertex", UUID.fromString("AF4B5965-B176-4552-B3C1-FBBE2F52C305")));

        final GraphWriter writer = writerMaker.apply(graph);
        final GraphReader reader = readerMaker.apply(graph);

        final Configuration configuration = graphProvider.newGraphConfiguration("readGraph", this.getClass(), name.getMethodName(), null);
        graphProvider.clear(configuration);
        final Graph g1 = graphProvider.openTestGraph(configuration);

        GraphMigrator.migrateGraph(graph, g1, reader, writer);

        final Vertex onlyVertex = g1.traversal().V().next();
        final CustomId id = (CustomId) onlyVertex.id();
        assertEquals("vertex", id.getCluster());
        assertEquals(UUID.fromString("AF4B5965-B176-4552-B3C1-FBBE2F52C305"), id.getElementId());

        // need to manually close the "g1" instance
        graphProvider.clear(g1, configuration);
    }
}
