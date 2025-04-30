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
package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for TinkerGraph vector index functionality.
 */
public class TinkerGraphVectorIndexTest {

    private static final Map<String,Object> indexConfig = new HashMap<String,Object>() {{
        put(TinkerVectorIndex.CONFIG_DIMENSION, 3);
    }};

    @Test
    public void shouldCreateEdgeVectorIndex() {
        final TinkerGraph graph = TinkerGraph.open();
        final GraphTraversalSource g = traversal().withEmbedded(graph);
        graph.createIndex(TinkerIndexType.VECTOR,"embedding", Edge.class, indexConfig);
        assertThat(graph.getIndexedKeys(Edge.class), hasItem("embedding"));
    }

    @Test
    public void shouldCreateVectorIndex() {
        final TinkerGraph graph = TinkerGraph.open();
        final GraphTraversalSource g = traversal().withEmbedded(graph);
        graph.createIndex(TinkerIndexType.VECTOR,"embedding", Vertex.class, indexConfig);
        assertThat(graph.getIndexedKeys(Vertex.class), hasItem("embedding"));
    }

    @Test
    public void shouldFindNearestVertices() {
        final TinkerGraph graph = TinkerGraph.open();
        final GraphTraversalSource g = traversal().withEmbedded(graph);
        g.addV("person").property("name", "Alice").property("embedding", new float[]{1.0f, 0.0f, 0.0f}).iterate();
        g.addV("person").property("name", "Bob").property("embedding", new float[]{0.0f, 1.0f, 0.0f}).iterate();
        g.addV("person").property("name", "Charlie").property("embedding", new float[]{0.0f, 0.0f, 1.0f}).iterate();
        g.addV("person").property("name", "Dave").property("embedding", new float[]{0.9f, 0.1f, 0.0f}).iterate();

        graph.createIndex(TinkerIndexType.VECTOR,"embedding", Vertex.class, indexConfig);

        final List<Vertex> nearest = graph.findNearestVertices("embedding", new float[]{1.0f, 0.0f, 0.0f}, 2);
        assertNotNull(nearest);
        assertEquals(2, nearest.size());
        assertEquals("Alice", nearest.get(0).value("name"));
        assertEquals("Dave", nearest.get(1).value("name"));
    }

    @Test
    public void shouldUpdateVectorIndex() {
        final TinkerGraph graph = TinkerGraph.open();
        final GraphTraversalSource g = traversal().withEmbedded(graph);
        g.addV("person").property("name", "Alice").property("embedding", new float[]{1.0f, 0.0f, 0.0f}).iterate();
        g.addV("person").property("name", "Bob").property("embedding", new float[]{0.0f, 1.0f, 0.0f}).iterate();

        graph.createIndex(TinkerIndexType.VECTOR,"embedding", Vertex.class, indexConfig);

        // Update a vertex property
        g.V().has("name", "Bob").property("embedding", new float[]{0.9f, 0.1f, 0.0f}).iterate();

        final List<Vertex> nearest = graph.findNearestVertices("embedding", new float[]{1.0f, 0.0f, 0.0f}, 2);
        assertNotNull(nearest);
        assertEquals(2, nearest.size());
        assertEquals("Alice", nearest.get(0).value("name"));
        assertEquals("Bob", nearest.get(1).value("name"));
    }

    @Test
    public void shouldRemoveFromVectorIndex() {
        final TinkerGraph graph = TinkerGraph.open();
        final GraphTraversalSource g = traversal().withEmbedded(graph);
        g.addV("person").property("name", "Alice").property("embedding", new float[]{1.0f, 0.0f, 0.0f}).iterate();
        g.addV("person").property("name", "Bob").property("embedding", new float[]{0.0f, 1.0f, 0.0f}).iterate();
        g.addV("person").property("name", "Charlie").property("embedding", new float[]{0.0f, 0.0f, 1.0f}).iterate();

        graph.createIndex(TinkerIndexType.VECTOR,"embedding", Vertex.class, indexConfig);

        // Remove a vertex
        g.V().has("name", "Bob").drop().iterate();

        final List<Vertex> nearest = graph.findNearestVertices("embedding", new float[]{0.0f, 1.0f, 0.0f}, 2);
        assertNotNull(nearest);
        assertEquals(2, nearest.size());
        assertThat(nearest.stream().noneMatch(v -> v.value("name").equals("Bob")), is(true));
    }

    @Test
    public void shouldDropVectorIndex() {
        final TinkerGraph graph = TinkerGraph.open();
        final GraphTraversalSource g = traversal().withEmbedded(graph);
        g.addV("person").property("name", "Alice").property("embedding", new float[]{1.0f, 0.0f, 0.0f}).iterate();
        g.addV("person").property("name", "Bob").property("embedding", new float[]{0.0f, 1.0f, 0.0f}).iterate();

        graph.createIndex(TinkerIndexType.VECTOR,"embedding", Vertex.class, indexConfig);
        assertThat(graph.getIndexedKeys(Vertex.class), hasItem("embedding"));

        // Drop index
        graph.dropIndex("embedding", Vertex.class);
        assertThat(graph.getIndexedKeys(Vertex.class), not(hasItem("embedding")));

        // Search for nearest neighbors should return empty list
        final List<Vertex> nearest = graph.findNearestVertices("embedding", new float[]{1.0f, 0.0f, 0.0f}, 2);
        assertNotNull(nearest);
        assertEquals(0, nearest.size());
    }

    @Test
    public void shouldFindNearestEdges() {
        final TinkerGraph graph = TinkerGraph.open();
        final GraphTraversalSource g = traversal().withEmbedded(graph);
        final Vertex alice = g.addV("person").property("name", "Alice").next();
        final Vertex bob = g.addV("person").property("name", "Bob").next();
        final Vertex charlie = g.addV("person").property("name", "Charlie").next();
        final Vertex dave = g.addV("person").property("name", "Dave").next();
        g.addE("knows").from(alice).to(bob).property("embedding", new float[]{1.0f, 0.0f, 0.0f}).property("strength", 0.8f).iterate();
        g.addE("knows").from(bob).to(charlie).property("embedding", new float[]{0.0f, 1.0f, 0.0f}).property("strength", 0.6f).iterate();
        g.addE("knows").from(charlie).to(dave).property("embedding", new float[]{0.0f, 0.0f, 1.0f}).property("strength", 0.7f).iterate();
        g.addE("knows").from(alice).to(dave).property("embedding", new float[]{0.9f, 0.1f, 0.0f}).property("strength", 0.9f).iterate();

        graph.createIndex(TinkerIndexType.VECTOR,"embedding", Edge.class, indexConfig);

        final List<Edge> nearest = graph.findNearestEdges("embedding", new float[]{1.0f, 0.0f, 0.0f}, 2);
        assertNotNull(nearest);
        assertEquals(2, nearest.size());
        assertEquals(0.8f, (float) nearest.get(0).value("strength"), 0.0001f);
        assertEquals(0.9f, (float) nearest.get(1).value("strength"), 0.0001f);
    }

    @Test
    public void shouldUpdateEdgeVectorIndex() {
        final TinkerGraph graph = TinkerGraph.open();
        final GraphTraversalSource g = traversal().withEmbedded(graph);
        final Vertex alice = g.addV("person").property("name", "Alice").next();
        final Vertex bob = g.addV("person").property("name", "Bob").next();
        g.addE("knows").from(alice).to(bob).property("embedding", new float[]{1.0f, 0.0f, 0.0f}).property("strength", 0.8f).iterate();
        final Edge edge = g.addE("knows").from(bob).to(alice).property("embedding", new float[]{0.0f, 1.0f, 0.0f}).property("strength", 0.6f).next();

        graph.createIndex(TinkerIndexType.VECTOR,"embedding", Edge.class, indexConfig);

        // Update an edge property
        g.E(edge.id()).property("embedding", new float[]{0.9f, 0.1f, 0.0f}).iterate();

        final List<Edge> nearest = graph.findNearestEdges("embedding", new float[]{1.0f, 0.0f, 0.0f}, 2);
        assertNotNull(nearest);
        assertEquals(2, nearest.size());
        assertEquals(0.8f, (float) nearest.get(0).value("strength"), 0.0001f);
        assertEquals(0.6f, (float) nearest.get(1).value("strength"), 0.0001f);
    }

    @Test
    public void shouldRemoveEdgeFromVectorIndex() {
        final TinkerGraph graph = TinkerGraph.open();
        final GraphTraversalSource g = traversal().withEmbedded(graph);
        final Vertex alice = g.addV("person").property("name", "Alice").next();
        final Vertex bob = g.addV("person").property("name", "Bob").next();
        final Vertex charlie = g.addV("person").property("name", "Charlie").next();
        g.addE("knows").from(alice).to(bob).property("embedding", new float[]{1.0f, 0.0f, 0.0f}).property("strength", 0.8f).iterate();
        final Edge edge = g.addE("knows").from(bob).to(charlie).property("embedding", new float[]{0.0f, 1.0f, 0.0f}).property("strength", 0.6f).next();
        g.addE("knows").from(charlie).to(alice).property("embedding", new float[]{0.0f, 0.0f, 1.0f}).property("strength", 0.7f).iterate();

        graph.createIndex(TinkerIndexType.VECTOR, "embedding", Edge.class, indexConfig);

        // Remove an edge
        g.E(edge.id()).drop().iterate();

        final List<Edge> nearest = graph.findNearestEdges("embedding", new float[]{0.0f, 1.0f, 0.0f}, 2);
        assertNotNull(nearest);
        assertEquals(2, nearest.size());
        assertThat(nearest.stream().noneMatch(e -> e.value("strength").equals(0.6f)), is(true));
    }

    @Test
    public void shouldDropEdgeVectorIndex() {
        final TinkerGraph graph = TinkerGraph.open();
        final GraphTraversalSource g = traversal().withEmbedded(graph);
        final Vertex alice = g.addV("person").property("name", "Alice").next();
        final Vertex bob = g.addV("person").property("name", "Bob").next();
        g.addE("knows").from(alice).to(bob).property("embedding", new float[]{1.0f, 0.0f, 0.0f}).property("strength", 0.8f).iterate();
        g.addE("knows").from(bob).to(alice).property("embedding", new float[]{0.0f, 1.0f, 0.0f}).property("strength", 0.6f).iterate();

        graph.createIndex(TinkerIndexType.VECTOR,"embedding", Edge.class, indexConfig);
        assertThat(graph.getIndexedKeys(Edge.class), hasItem("embedding"));

        // Drop index
        graph.dropIndex("embedding", Edge.class);
        assertThat(graph.getIndexedKeys(Edge.class), not(hasItem("embedding")));

        // Search for nearest neighbors should return empty list
        final List<Edge> nearest = graph.findNearestEdges("embedding", new float[]{1.0f, 0.0f, 0.0f}, 2);
        assertNotNull(nearest);
        assertEquals(0, nearest.size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenVectorDimensionExceedsConfigured() {
        final TinkerGraph graph = TinkerGraph.open();
        final GraphTraversalSource g = traversal().withEmbedded(graph);

        // Create a vector index with dimension 3
        graph.createIndex(TinkerIndexType.VECTOR, "embedding", Vertex.class, indexConfig);

        // Try to add a vertex with a vector of dimension 4 (exceeds configured dimension 3)
        g.addV("person").property("name", "Alice").property("embedding", new float[]{1.0f, 0.0f, 0.0f, 0.0f}).iterate();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenVectorDimensionIsSmallerThanConfigured() {
        final TinkerGraph graph = TinkerGraph.open();
        final GraphTraversalSource g = traversal().withEmbedded(graph);

        // Create a vector index with dimension 3
        graph.createIndex(TinkerIndexType.VECTOR, "embedding", Vertex.class, indexConfig);

        // Try to add a vertex with a vector of dimension 2 (smaller than configured dimension 3)
        g.addV("person").property("name", "Alice").property("embedding", new float[]{1.0f, 0.0f}).iterate();
    }
}
