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
package org.apache.tinkerpop.gremlin.structure.util.star;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.FeatureRequirement;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.javatuples.Pair;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StarGraphTest extends AbstractGremlinTest {

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CREW)
    public void shouldHashAndEqualsCorrectly() {
        final Vertex gremlin = g.V(convertToVertexId("gremlin")).next();
        final StarGraph gremlinStarGraph = StarGraph.of(gremlin);
        final StarGraph.StarVertex gremlinStar = gremlinStarGraph.getStarVertex();

        final Vertex marko = g.V(convertToVertexId("marko")).next();
        final StarGraph markoStarGraph = StarGraph.of(marko);
        final StarGraph.StarAdjacentVertex gremlinStarAdjacentGraph = (StarGraph.StarAdjacentVertex) IteratorUtils.filter(markoStarGraph.getStarVertex().edges(Direction.OUT, "uses"), x -> x.inVertex().id().equals(convertToVertexId("gremlin"))).next().inVertex();

        final Set<Vertex> set = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            set.add(gremlin);
            set.add(gremlinStar);
            set.add(gremlinStarAdjacentGraph);
        }
        assertEquals(1, set.size());
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CREW)
    public void shoudlValidateThatOriginalAndStarVerticesHaveTheSameTopology() {
        g.V().forEachRemaining(vertex -> TestHelper.validateEquality(vertex, StarGraph.of(vertex).getStarVertex()));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CREW)
    public void shouldSerializeCorrectlyUsingGryo() {
        g.V().forEachRemaining(vertex -> TestHelper.validateEquality(vertex, serializeDeserialize(StarGraph.of(vertex)).getValue0().getStarVertex()));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CREW)
    public void shouldAttacheWithGetMethod() {
        // vertex host
        g.V().forEachRemaining(vertex -> TestHelper.validateEquality(vertex, StarGraph.of(vertex).getStarVertex().attach(Attachable.Method.get(vertex))));
        g.V().forEachRemaining(vertex -> StarGraph.of(vertex).getStarVertex().properties().forEachRemaining(vertexProperty -> TestHelper.validateEquality(vertexProperty, ((Attachable<VertexProperty>) vertexProperty).attach(Attachable.Method.get(vertex)))));
        g.V().forEachRemaining(vertex -> StarGraph.of(vertex).getStarVertex().properties().forEachRemaining(vertexProperty -> vertexProperty.properties().forEachRemaining(property -> TestHelper.validateEquality(property, ((Attachable<Property>) property).attach(Attachable.Method.get(vertex))))));
        g.V().forEachRemaining(vertex -> StarGraph.of(vertex).getStarVertex().edges(Direction.OUT).forEachRemaining(edge -> TestHelper.validateEquality(edge, ((Attachable<Edge>) edge).attach(Attachable.Method.get(vertex)))));
        g.V().forEachRemaining(vertex -> StarGraph.of(vertex).getStarVertex().edges(Direction.OUT).forEachRemaining(edge -> edge.properties().forEachRemaining(property -> TestHelper.validateEquality(property, ((Attachable<Property>) property).attach(Attachable.Method.get(vertex))))));

        // graph host
        g.V().forEachRemaining(vertex -> TestHelper.validateEquality(vertex, StarGraph.of(vertex).getStarVertex().attach(Attachable.Method.get(graph))));
        g.V().forEachRemaining(vertex -> StarGraph.of(vertex).getStarVertex().properties().forEachRemaining(vertexProperty -> TestHelper.validateEquality(vertexProperty, ((Attachable<VertexProperty>) vertexProperty).attach(Attachable.Method.get(graph)))));
        g.V().forEachRemaining(vertex -> StarGraph.of(vertex).getStarVertex().properties().forEachRemaining(vertexProperty -> vertexProperty.properties().forEachRemaining(property -> TestHelper.validateEquality(property, ((Attachable<Property>) property).attach(Attachable.Method.get(graph))))));
        g.V().forEachRemaining(vertex -> StarGraph.of(vertex).getStarVertex().edges(Direction.OUT).forEachRemaining(edge -> TestHelper.validateEquality(edge, ((Attachable<Edge>) edge).attach(Attachable.Method.get(graph)))));
        g.V().forEachRemaining(vertex -> StarGraph.of(vertex).getStarVertex().edges(Direction.OUT).forEachRemaining(edge -> edge.properties().forEachRemaining(property -> TestHelper.validateEquality(property, ((Attachable<Property>) property).attach(Attachable.Method.get(graph))))));
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexPropertyFeatures.FEATURE_USER_SUPPLIED_IDS)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_USER_SUPPLIED_IDS)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_PROPERTY)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_META_PROPERTIES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_MULTI_PROPERTIES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_PROPERTY)
    public void shoudlAttachWithCreateMethod() {
        final Random random = new Random(234335l);
        StarGraph starGraph = StarGraph.open();
        Vertex starVertex = starGraph.addVertex(T.label, "person", "name", "stephen", "name", "spmallete");
        starVertex.property("acl", true, "timestamp", random.nextLong(), "creator", "marko");
        for (int i = 0; i < 100; i++) {
            starVertex.addEdge("knows", starGraph.addVertex("person", "name", new UUID(random.nextLong(), random.nextLong()), "since", random.nextLong()));
            starGraph.addVertex(T.label, "project").addEdge("developedBy", starVertex, "public", random.nextBoolean());
        }
        final Vertex createdVertex = starGraph.getStarVertex().attach(Attachable.Method.create(graph));
        starGraph.getStarVertex().edges(Direction.BOTH).forEachRemaining(edge -> ((Attachable<Edge>) edge).attach(Attachable.Method.create(random.nextBoolean() ? graph : createdVertex)));
        TestHelper.validateEquality(starVertex, createdVertex);
    }

    private Pair<StarGraph, Integer> serializeDeserialize(final StarGraph starGraph) {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            graph.io(IoCore.gryo()).writer().create().writeObject(outputStream, starGraph);
            return Pair.with(graph.io(IoCore.gryo()).reader().create().readObject(new ByteArrayInputStream(outputStream.toByteArray()), StarGraph.class), outputStream.size());
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }
}
