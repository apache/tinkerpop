/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.tinkerpop.gremlin.structure.util.star;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.FeatureRequirement;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoReader;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoWriter;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.javatuples.Pair;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StarGraphTest extends AbstractGremlinTest {

    private static Pair<StarGraph, Integer> serializeDeserialize(final StarGraph starGraph) {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        GryoWriter.build().create().writeObject(outputStream, starGraph);
        try {
            return Pair.with(GryoReader.build().create().readObject(new ByteArrayInputStream(outputStream.toByteArray()), StarGraph.class), outputStream.size());
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CREW)
    public void shouldHashAndEqualsCorrectly() {
        final Vertex gremlin = g.V(convertToVertexId("gremlin")).next();
        final StarGraph starGraph = StarGraph.of(gremlin);
        final StarGraph.StarVertex gremlinStar = starGraph.getStarVertex();
        final Set<Vertex> set = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            set.add(gremlin);
            set.add(gremlinStar);
        }
        assertEquals(1, set.size());
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CREW)
    public void originalAndStarVerticesShouldHaveTheSameTopology() {
        g.V().forEachRemaining(vertex -> validateVertex(vertex, StarGraph.of(vertex).getStarVertex()));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CREW)
    public void shouldSerializeCorrectlyUsingGryo() {
        g.V().forEachRemaining(vertex -> validateVertex(vertex, serializeDeserialize(StarGraph.of(vertex)).getValue0().getStarVertex()));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void testAttachableGetMethod() {
        // vertex host
        g.V().forEachRemaining(vertex -> StarGraph.of(vertex).getStarVertex().properties().forEachRemaining(vertexProperty -> validateVertexProperty(vertexProperty, ((Attachable<VertexProperty>) vertexProperty).attach(vertex, Attachable.Method.GET))));
        g.V().forEachRemaining(vertex -> vertex.properties().forEachRemaining(vertexProperty -> vertexProperty.properties().forEachRemaining(property -> ((Attachable<Property>) property).attach(vertex, Attachable.Method.GET))));
        g.V().forEachRemaining(vertex -> StarGraph.of(vertex).getStarVertex().edges(Direction.OUT).forEachRemaining(edge -> validateEdge(edge, ((Attachable<Edge>) edge).attach(vertex, Attachable.Method.GET))));
        g.V().forEachRemaining(vertex -> StarGraph.of(vertex).getStarVertex().edges(Direction.OUT).forEachRemaining(edge -> edge.properties().forEachRemaining(property -> validateProperty(property, ((Attachable<Property>) property).attach(vertex, Attachable.Method.GET)))));

        // graph host
        g.V().forEachRemaining(vertex -> validateVertex(vertex, StarGraph.of(vertex).getStarVertex().attach(graph, Attachable.Method.GET)));
        g.V().forEachRemaining(vertex -> StarGraph.of(vertex).getStarVertex().properties().forEachRemaining(vertexProperty -> validateVertexProperty(vertexProperty, ((Attachable<VertexProperty>) vertexProperty).attach(graph, Attachable.Method.GET))));
        g.V().forEachRemaining(vertex -> vertex.properties().forEachRemaining(vertexProperty -> vertexProperty.properties().forEachRemaining(property -> ((Attachable<Property>) property).attach(graph, Attachable.Method.GET))));
        g.V().forEachRemaining(vertex -> StarGraph.of(vertex).getStarVertex().edges(Direction.OUT).forEachRemaining(edge -> validateEdge(edge, ((Attachable<Edge>) edge).attach(graph, Attachable.Method.GET))));
        g.V().forEachRemaining(vertex -> StarGraph.of(vertex).getStarVertex().edges(Direction.OUT).forEachRemaining(edge -> edge.properties().forEachRemaining(property -> validateProperty(property, ((Attachable<Property>) property).attach(graph, Attachable.Method.GET)))));
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_PROPERTY)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_META_PROPERTIES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_MULTI_PROPERTIES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_PROPERTY)
    public void shouldHaveSizeOfStarGraphLessThanDetached() throws Exception {
        final Random random = new Random(95746498l);
        final Vertex vertex = graph.addVertex("person");
        for (int i = 0; i < 100; i++) { // vertex properties and meta properties
            vertex.property(VertexProperty.Cardinality.list, UUID.randomUUID().toString(), random.nextDouble(),
                    "acl", random.nextBoolean() ? "public" : "private",
                    "created", random.nextLong());
        }
        for (int i = 0; i < 50000; i++) {  // edges and edge properties
            vertex.addEdge("knows", graph.addVertex("person"), "since", random.nextLong(), "acl", random.nextBoolean() ? "public" : "private");
            graph.addVertex("software").addEdge("createdBy", vertex, "date", random.nextLong());
            graph.addVertex("group").addEdge("hasMember", vertex);
        }
        ///////////////
        Pair<StarGraph, Integer> pair = serializeDeserialize(StarGraph.of(vertex));
        int starGraphSize = pair.getValue1();
        validateVertex(vertex, pair.getValue0().getStarVertex());
        ///
        pair = serializeDeserialize(pair.getValue0());
        assertEquals(starGraphSize, pair.getValue1().intValue());
        starGraphSize = pair.getValue1();
        validateVertex(vertex, pair.getValue0().getStarVertex());
        ///
        pair = serializeDeserialize(pair.getValue0());
        assertEquals(starGraphSize, pair.getValue1().intValue());
        starGraphSize = pair.getValue1();
        validateVertex(vertex, pair.getValue0().getStarVertex());
        ///
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        GryoWriter.build().create().writeVertex(outputStream, vertex, Direction.BOTH);
        final int detachedVertexSize = outputStream.size();
        assertTrue(starGraphSize < detachedVertexSize);

        System.out.println("Size of star graph:        " + starGraphSize);
        System.out.println("Size of detached vertex:   " + detachedVertexSize);
        System.out.println("Size reduction:            " + (float) detachedVertexSize / (float) starGraphSize);
    }

    private void validateVertex(final Vertex originalVertex, final Vertex otherVertex) {
        assertEquals(originalVertex, otherVertex);
        assertEquals(otherVertex, originalVertex);
        assertEquals(originalVertex.id(), otherVertex.id());
        assertEquals(originalVertex.label(), otherVertex.label());
        assertEquals(originalVertex.keys().size(), otherVertex.keys().size());
        for (final String key : originalVertex.keys()) {
            final List<VertexProperty<Object>> originalVertexProperties = IteratorUtils.list(originalVertex.properties(key));
            final List<VertexProperty<Object>> otherVertexProperties = IteratorUtils.list(otherVertex.properties(key));
            assertEquals(originalVertexProperties.size(), otherVertexProperties.size());
            for (VertexProperty<Object> originalVertexProperty : originalVertexProperties) {
                final VertexProperty<Object> otherVertexProperty = otherVertexProperties.parallelStream().filter(vp -> vp.equals(originalVertexProperty)).findAny().get();
                validateVertexProperty(originalVertexProperty, otherVertexProperty);
            }
        }

        Iterator<Edge> originalEdges = IteratorUtils.set(originalVertex.edges(Direction.OUT)).iterator();
        Iterator<Edge> otherEdges = IteratorUtils.set(otherVertex.edges(Direction.OUT)).iterator();
        while (originalEdges.hasNext()) {
            validateEdge(originalEdges.next(), otherEdges.next());
        }
        assertFalse(otherEdges.hasNext());

        originalEdges = IteratorUtils.set(originalVertex.edges(Direction.IN)).iterator();
        otherEdges = IteratorUtils.set(otherVertex.edges(Direction.IN)).iterator();
        while (originalEdges.hasNext()) {
            validateEdge(originalEdges.next(), otherEdges.next());
        }
        assertFalse(otherEdges.hasNext());

    }

    private static void validateVertexProperty(final VertexProperty originalVertexProperty, final VertexProperty otherVertexProperty) {
        assertEquals(originalVertexProperty.isPresent(), otherVertexProperty.isPresent());
        if (originalVertexProperty.isPresent()) {
            assertEquals(originalVertexProperty.key(), otherVertexProperty.key());
            assertEquals(originalVertexProperty.value(), otherVertexProperty.value());
            assertEquals(originalVertexProperty.element(), otherVertexProperty.element());
            assertEquals(originalVertexProperty.keys().size(), otherVertexProperty.keys().size());
            for (final String key : originalVertexProperty.keys()) {
                validateProperty(originalVertexProperty.property(key), otherVertexProperty.property(key));
            }
        }
    }

    private static void validateProperty(final Property originalProperty, final Property otherProperty) {
        assertEquals(originalProperty.isPresent(), otherProperty.isPresent());
        if (originalProperty.isPresent()) {
            assertEquals(originalProperty.key(), otherProperty.key());
            assertEquals(originalProperty.value(), otherProperty.value());
            assertEquals(originalProperty.element(), otherProperty.element());
        }
    }

    private static void validateEdge(final Edge originalEdge, final Edge otherEdge) {
        assertEquals(originalEdge.id(), otherEdge.id());
        assertEquals(originalEdge.label(), otherEdge.label());
        assertEquals(originalEdge.inVertex(), otherEdge.inVertex());
        assertEquals(originalEdge.outVertex(), otherEdge.outVertex());
        assertEquals(originalEdge.keys().size(), otherEdge.keys().size());
        for (final String key : originalEdge.keys()) {
            validateProperty(originalEdge.property(key), otherEdge.property(key));
        }
    }

}
