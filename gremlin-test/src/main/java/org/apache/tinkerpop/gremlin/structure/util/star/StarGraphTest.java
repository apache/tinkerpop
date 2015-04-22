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
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoReader;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoWriter;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.javatuples.Pair;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StarGraphTest extends AbstractGremlinTest {

    private static Pair<StarGraph, Integer> serializeDeserialize(final StarGraph starGraph) {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        GryoWriter.build().create().writeObject(outputStream, starGraph);
        return Pair.with(GryoReader.build().create().readObject(new ByteArrayInputStream(outputStream.toByteArray())), outputStream.size());
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
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_PROPERTY)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_META_PROPERTIES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_MULTI_PROPERTIES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_PROPERTY)
    public void shouldHaveSizeOfStarGraphLessThanDetached() throws Exception {
        final Random random = new Random();
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

    private void validateVertex(final Vertex originalVertex, final StarGraph.StarVertex starVertex) {
        ////////////////  VALIDATE PROPERTIES
        final AtomicInteger originalPropertyCounter = new AtomicInteger(0);
        final AtomicInteger originalMetaPropertyCounter = new AtomicInteger(0);
        final AtomicInteger starPropertyCounter = new AtomicInteger(0);
        final AtomicInteger starMetaPropertyCounter = new AtomicInteger(0);

        originalVertex.properties().forEachRemaining(vertexProperty -> {
            originalPropertyCounter.incrementAndGet();
            starVertex.properties(vertexProperty.label()).forEachRemaining(starVertexProperty -> {
                if (starVertexProperty.equals(vertexProperty)) {
                    starPropertyCounter.incrementAndGet();
                    assertEquals(starVertexProperty.id(), vertexProperty.id());
                    assertEquals(starVertexProperty.label(), vertexProperty.label());
                    assertEquals(starVertexProperty.value(), vertexProperty.value());
                    assertEquals(starVertexProperty.key(), vertexProperty.key());
                    assertEquals(starVertexProperty.element(), vertexProperty.element());
                    //
                    vertexProperty.properties().forEachRemaining(p -> {
                        originalMetaPropertyCounter.incrementAndGet();
                        assertEquals(p.value(), starVertexProperty.property(p.key()).value());
                        assertEquals(p.key(), starVertexProperty.property(p.key()).key());
                        assertEquals(p.element(), starVertexProperty.property(p.key()).element());
                    });
                    starVertexProperty.properties().forEachRemaining(p -> starMetaPropertyCounter.incrementAndGet());
                }
            });
        });

        assertEquals(originalPropertyCounter.get(), starPropertyCounter.get());
        assertEquals(originalMetaPropertyCounter.get(), starMetaPropertyCounter.get());

        originalPropertyCounter.set(0);
        starPropertyCounter.set(0);
        originalMetaPropertyCounter.set(0);
        starMetaPropertyCounter.set(0);
        //
        starVertex.properties().forEachRemaining(starVertexProperty -> {
            starPropertyCounter.incrementAndGet();
            originalVertex.properties(starVertexProperty.label()).forEachRemaining(vertexProperty -> {
                if (starVertexProperty.equals(vertexProperty)) {
                    originalPropertyCounter.incrementAndGet();
                    assertEquals(vertexProperty.id(), starVertexProperty.id());
                    assertEquals(vertexProperty.label(), starVertexProperty.label());
                    assertEquals(vertexProperty.value(), starVertexProperty.value());
                    assertEquals(vertexProperty.key(), starVertexProperty.key());
                    assertEquals(vertexProperty.element(), starVertexProperty.element());
                    starVertexProperty.properties().forEachRemaining(p -> {
                        starMetaPropertyCounter.incrementAndGet();
                        assertEquals(p.value(), vertexProperty.property(p.key()).value());
                        assertEquals(p.key(), vertexProperty.property(p.key()).key());
                        assertEquals(p.element(), vertexProperty.property(p.key()).element());
                    });
                    vertexProperty.properties().forEachRemaining(p -> originalMetaPropertyCounter.incrementAndGet());
                }
            });
        });
        assertEquals(originalPropertyCounter.get(), starPropertyCounter.get());
        assertEquals(originalMetaPropertyCounter.get(), starMetaPropertyCounter.get());

        ////////////////  VALIDATE EDGES
        assertEquals(originalVertex, starVertex);
        assertEquals(starVertex, originalVertex);
        assertEquals(starVertex.id(), originalVertex.id());
        assertEquals(starVertex.label(), originalVertex.label());
        ///
        List<Edge> originalEdges = new ArrayList<>(IteratorUtils.set(originalVertex.edges(Direction.OUT)));
        List<Edge> starEdges = new ArrayList<>(IteratorUtils.set(starVertex.edges(Direction.OUT)));
        assertEquals(originalEdges.size(), starEdges.size());
        for (int i = 0; i < starEdges.size(); i++) {
            final Edge starEdge = starEdges.get(i);
            final Edge originalEdge = originalEdges.get(i);
            assertEquals(starEdge, originalEdge);
            assertEquals(starEdge.id(), originalEdge.id());
            assertEquals(starEdge.label(), originalEdge.label());
            assertEquals(starEdge.inVertex(), originalEdge.inVertex());
            assertEquals(starEdge.outVertex(), originalEdge.outVertex());
            originalEdge.properties().forEachRemaining(p -> assertEquals(p, starEdge.property(p.key())));
            starEdge.properties().forEachRemaining(p -> assertEquals(p, originalEdge.property(p.key())));
        }

        originalEdges = new ArrayList<>(IteratorUtils.set(originalVertex.edges(Direction.IN)));
        starEdges = new ArrayList<>(IteratorUtils.set(starVertex.edges(Direction.IN)));
        assertEquals(originalEdges.size(), starEdges.size());
        for (int i = 0; i < starEdges.size(); i++) {
            final Edge starEdge = starEdges.get(i);
            final Edge originalEdge = originalEdges.get(i);
            assertEquals(starEdge, originalEdge);
            assertEquals(starEdge.id(), originalEdge.id());
            assertEquals(starEdge.label(), originalEdge.label());
            assertEquals(starEdge.inVertex(), originalEdge.inVertex());
            assertEquals(starEdge.outVertex(), originalEdge.outVertex());
            originalEdge.properties().forEachRemaining(p -> assertEquals(p, starEdge.property(p.key())));
            starEdge.properties().forEachRemaining(p -> assertEquals(p, originalEdge.property(p.key())));
        }
    }
}
