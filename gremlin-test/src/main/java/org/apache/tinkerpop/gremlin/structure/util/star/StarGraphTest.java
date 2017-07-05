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

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.FeatureRequirement;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
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
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

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
    public void shouldValidateThatOriginalAndStarVerticesHaveTheSameTopology() {
        g.V().forEachRemaining(vertex -> TestHelper.validateEquality(vertex, StarGraph.of(vertex).getStarVertex()));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CREW)
    public void shouldSerializeCorrectlyUsingGryo() {
        g.V().forEachRemaining(vertex -> TestHelper.validateEquality(vertex, serializeDeserialize(StarGraph.of(vertex)).getValue0().getStarVertex()));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CREW)
    public void shouldAttachWithGetMethod() {
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
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldCopyFromGraphAToGraphB() throws Exception {
        final List<StarGraph> starGraphs = IteratorUtils.stream(graph.vertices()).map(StarGraph::of).collect(Collectors.toList());

        // via vertices and then edges
        final Configuration g1Configuration = graphProvider.newGraphConfiguration("readGraph", this.getClass(), name.getMethodName(), null);
        Graph g1 = graphProvider.openTestGraph(g1Configuration);
        starGraphs.stream().map(StarGraph::getStarVertex).forEach(vertex -> vertex.attach(Attachable.Method.getOrCreate(g1)));
        starGraphs.stream().forEach(starGraph -> starGraph.edges().forEachRemaining(edge -> ((Attachable<Edge>) edge).attach(Attachable.Method.getOrCreate(g1))));
        assertEquals(IteratorUtils.count(graph.vertices()), IteratorUtils.count(g1.vertices()));
        assertEquals(IteratorUtils.count(graph.edges()), IteratorUtils.count(g1.edges()));
        graph.vertices().forEachRemaining(vertex -> TestHelper.validateVertexEquality(vertex, g1.vertices(vertex.id()).next(), true));
        graphProvider.clear(g1, g1Configuration);

        // via edges only
        final Configuration g2Configuration = graphProvider.newGraphConfiguration("readGraph", this.getClass(), name.getMethodName(), null);
        final Graph g2 = graphProvider.openTestGraph(g2Configuration);
        starGraphs.stream().forEach(starGraph -> starGraph.edges().forEachRemaining(edge -> ((Attachable<Edge>) edge).attach(Attachable.Method.getOrCreate(g2))));
        assertEquals(IteratorUtils.count(graph.vertices()), IteratorUtils.count(g2.vertices()));
        assertEquals(IteratorUtils.count(graph.edges()), IteratorUtils.count(g2.edges()));
        // TODO: you can't get adjacent labels -- graph.vertices().forEachRemaining(vertex -> TestHelper.validateVertexEquality(vertex, g1.vertices(vertex.id()).next(), true));
        graphProvider.clear(g2, g2Configuration);
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
    public void shouldAttachWithCreateMethod() {
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

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_PROPERTY)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_PROPERTY)
    public void shouldHandleSelfLoops() {
        assertEquals(0l, IteratorUtils.count(graph.vertices()));
        assertEquals(0l, IteratorUtils.count(graph.edges()));
        final Vertex vertex = graph.addVertex("person");
        final VertexProperty<String> vertexProperty = vertex.property("name", "furnace");
        final Edge edge = vertex.addEdge("self", vertex);
        final Property<String> edgeProperty = edge.property("acl", "private");
        assertEquals(1l, IteratorUtils.count(graph.vertices()));
        assertEquals(1l, IteratorUtils.count(graph.edges()));
        assertEquals(1l, IteratorUtils.count(vertex.properties()));
        assertEquals(1l, IteratorUtils.count(edge.properties()));
        assertEquals(vertexProperty, vertex.properties().next());
        assertEquals(edgeProperty, edge.properties().next());
        ///
        final StarGraph starGraph = StarGraph.of(vertex);
        final StarGraph.StarVertex starVertex = starGraph.getStarVertex();
        final Edge starEdge = starVertex.edges(Direction.OUT).next();
        assertEquals(vertex, starVertex);
        assertEquals(edge, starEdge);
        assertEquals(1l, IteratorUtils.count(starVertex.properties()));
        assertEquals("furnace", starVertex.value("name"));
        assertEquals(2l, IteratorUtils.count(starVertex.vertices(Direction.BOTH, "self")));
        assertEquals(1l, IteratorUtils.count(starVertex.vertices(Direction.OUT, "self")));
        assertEquals(1l, IteratorUtils.count(starVertex.vertices(Direction.IN, "self")));
        Iterator<Vertex> vertexIterator = starVertex.vertices(Direction.BOTH, "self");
        assertEquals(starVertex, vertexIterator.next());
        assertEquals(starVertex, vertexIterator.next());
        assertFalse(vertexIterator.hasNext());
        assertEquals(starVertex, starVertex.vertices(Direction.OUT, "self").next());
        assertEquals(starVertex, starVertex.vertices(Direction.IN, "self").next());
        ///
        assertEquals(2l, IteratorUtils.count(starVertex.vertices(Direction.BOTH)));
        assertEquals(1l, IteratorUtils.count(starVertex.vertices(Direction.OUT)));
        assertEquals(1l, IteratorUtils.count(starVertex.vertices(Direction.IN)));
        vertexIterator = starVertex.vertices(Direction.BOTH);
        assertEquals(starVertex, vertexIterator.next());
        assertEquals(starVertex, vertexIterator.next());
        assertFalse(vertexIterator.hasNext());
        assertEquals(starVertex, starVertex.vertices(Direction.OUT).next());
        assertEquals(starVertex, starVertex.vertices(Direction.IN).next());
        ///
        assertEquals(2l, IteratorUtils.count(starVertex.edges(Direction.BOTH, "self", "nothing")));
        assertEquals(1l, IteratorUtils.count(starVertex.edges(Direction.OUT, "self", "nothing")));
        assertEquals(1l, IteratorUtils.count(starVertex.edges(Direction.IN, "self", "nothing")));
        Iterator<Edge> edgeIterator = starVertex.edges(Direction.BOTH, "self", "nothing");
        Edge tempEdge = edgeIterator.next();
        assertEquals(1l, IteratorUtils.count(tempEdge.properties()));
        assertEquals("private", tempEdge.value("acl"));
        assertEquals(starEdge, tempEdge);
        tempEdge = edgeIterator.next();
        assertEquals(1l, IteratorUtils.count(tempEdge.properties()));
        assertEquals("private", tempEdge.value("acl"));
        assertEquals(starEdge, tempEdge);
        assertFalse(edgeIterator.hasNext());
        assertEquals(starEdge, starVertex.edges(Direction.OUT, "self", "nothing").next());
        assertEquals(starEdge, starVertex.edges(Direction.IN, "self", "nothing").next());
        //
        final StarGraph starGraphCopy = serializeDeserialize(starGraph).getValue0();
        TestHelper.validateVertexEquality(vertex, starGraph.getStarVertex(), true);
        TestHelper.validateVertexEquality(vertex, starGraphCopy.getStarVertex(), true);
        TestHelper.validateVertexEquality(starGraph.getStarVertex(), starGraphCopy.getStarVertex(), true);
        // test native non-clone-based methods
        final StarGraph starGraphNative = StarGraph.open();
        Vertex v1 = starGraphNative.addVertex(T.label, "thing", T.id, "v1");
        assertEquals("v1", v1.id());
        assertEquals("thing", v1.label());
        Edge e1 = v1.addEdge("self", v1, "name", "pipes");
        assertEquals(2l, IteratorUtils.count(v1.vertices(Direction.BOTH, "self", "nothing")));
        assertEquals(1l, IteratorUtils.count(v1.vertices(Direction.OUT)));
        assertEquals(1l, IteratorUtils.count(v1.vertices(Direction.IN, "self")));
        edgeIterator = v1.edges(Direction.BOTH);
        TestHelper.validateEdgeEquality(e1, edgeIterator.next());
        TestHelper.validateEdgeEquality(e1, edgeIterator.next());
        assertFalse(edgeIterator.hasNext());
        TestHelper.validateEdgeEquality(e1, v1.edges(Direction.OUT, "self", "nothing").next());
        TestHelper.validateEdgeEquality(e1, v1.edges(Direction.IN).next());
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_PROPERTY)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_PROPERTY)
    public void shouldHandleBothEdgesGraphFilterOnSelfLoop() {
        assertEquals(0l, IteratorUtils.count(graph.vertices()));
        assertEquals(0l, IteratorUtils.count(graph.edges()));

        // these vertex label, edge label, and property names/values were copied from existing tests
        StarGraph starGraph = StarGraph.open();
        Vertex vertex = starGraph.addVertex(T.label, "person", "name", "furnace");
        Edge edge = vertex.addEdge("self", vertex);
        edge.property("acl", "private");

        // traversing a self-loop should yield the edge once for inE/outE
        // and the edge twice for bothE (one edge emitted two times, not two edges)
        assertEquals(1L, IteratorUtils.count(starGraph.traversal().V().inE()));
        assertEquals(1L, IteratorUtils.count(starGraph.traversal().V().outE()));
        assertEquals(2L, IteratorUtils.count(starGraph.traversal().V().bothE()));
        
        // Try a filter that retains BOTH
        GraphFilter graphFilter = new GraphFilter();
        graphFilter.setEdgeFilter(__.bothE("self"));
        starGraph = starGraph.applyGraphFilter(graphFilter).get();

        // Retest traversal counts after filtering
        assertEquals(1L, IteratorUtils.count(starGraph.traversal().V().inE()));
        assertEquals(1L, IteratorUtils.count(starGraph.traversal().V().outE()));
        assertEquals(2L, IteratorUtils.count(starGraph.traversal().V().bothE()));
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
