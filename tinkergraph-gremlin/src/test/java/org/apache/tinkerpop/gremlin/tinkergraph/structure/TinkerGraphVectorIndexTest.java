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
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Tests for TinkerGraph vector index functionality.
 */
@RunWith(Parameterized.class)
public class TinkerGraphVectorIndexTest {

    protected static final Map<String,Object> indexConfig = new HashMap<String,Object>() {{
        put(TinkerVectorIndex.CONFIG_DIMENSION, 3);
    }};

    @Parameterized.Parameter
    public AbstractTinkerGraph graph;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {TinkerGraph.open()},
                {TinkerTransactionGraph.open()}
        });
    }

    @Before
    public void setUp() throws Exception {
        graph.clear();
        tryCommitChanges(graph);
    }

    @Test
    public void shouldCreateEdgeVectorIndex() {
        graph.createIndex(TinkerIndexType.VECTOR, "embedding", Edge.class, indexConfig);
        assertThat(graph.getIndexedKeys(Edge.class).contains("embedding"), is(true));
    }

    @Test
    public void shouldCreateVectorIndex() {
        graph.createIndex(TinkerIndexType.VECTOR, "embedding", Vertex.class, indexConfig);
        assertThat(graph.getIndexedKeys(Vertex.class).contains("embedding"), is(true));
    }

    @Test
    public void shouldFindNearestVertices() {
        final GraphTraversalSource g = traversal().with(graph);
        g.addV("person").property("name", "Alice").property("embedding", new float[]{1.0f, 0.0f, 0.0f}).iterate();
        g.addV("person").property("name", "Bob").property("embedding", new float[]{0.0f, 1.0f, 0.0f}).iterate();
        g.addV("person").property("name", "Charlie").property("embedding", new float[]{0.0f, 0.0f, 1.0f}).iterate();
        g.addV("person").property("name", "Dave").property("embedding", new float[]{0.9f, 0.1f, 0.0f}).iterate();

        tryCommitChanges(graph);

        graph.createIndex(TinkerIndexType.VECTOR,"embedding", Vertex.class, indexConfig);

        final List<Vertex> nearest = graph.findNearestVertices("embedding", new float[]{1.0f, 0.0f, 0.0f}, 2);
        assertNotNull(nearest);
        assertEquals(2, nearest.size());
        assertEquals("Alice", nearest.get(0).value("name"));
        assertEquals("Dave", nearest.get(1).value("name"));
    }

    @Test
    public void shouldUpdateVectorIndex() {
        final GraphTraversalSource g = traversal().with(graph);
        g.addV("person").property("name", "Alice").property("embedding", new float[]{1.0f, 0.0f, 0.0f}).iterate();
        g.addV("person").property("name", "Bob").property("embedding", new float[]{0.0f, 1.0f, 0.0f}).iterate();
        tryCommitChanges(graph);

        graph.createIndex(TinkerIndexType.VECTOR,"embedding", Vertex.class, indexConfig);

        // Update a vertex property
        g.V().has("name", "Bob").property("embedding", new float[]{0.9f, 0.1f, 0.0f}).iterate();
        tryCommitChanges(graph);

        final List<Vertex> nearest = graph.findNearestVertices("embedding", new float[]{1.0f, 0.0f, 0.0f}, 2);
        assertNotNull(nearest);
        assertEquals(2, nearest.size());
        assertEquals("Alice", nearest.get(0).value("name"));
        assertEquals("Bob", nearest.get(1).value("name"));
    }

    @Test
    public void shouldRemoveFromVectorIndex() {
        final GraphTraversalSource g = traversal().with(graph);
        g.addV("person").property("name", "Alice").property("embedding", new float[]{1.0f, 0.0f, 0.0f}).iterate();
        g.addV("person").property("name", "Bob").property("embedding", new float[]{0.0f, 1.0f, 0.0f}).iterate();
        g.addV("person").property("name", "Charlie").property("embedding", new float[]{0.0f, 0.0f, 1.0f}).iterate();
        tryCommitChanges(graph);

        graph.createIndex(TinkerIndexType.VECTOR,"embedding", Vertex.class, indexConfig);

        // Remove a vertex
        g.V().has("name", "Bob").drop().iterate();
        tryCommitChanges(graph);

        final List<Vertex> nearest = graph.findNearestVertices("embedding", new float[]{0.0f, 1.0f, 0.0f}, 2);
        assertNotNull(nearest);
        assertEquals(2, nearest.size());
        assertThat(nearest.stream().noneMatch(v -> v.value("name").equals("Bob")), is(true));
    }

    @Test
    public void shouldDropVectorIndex() {
        final GraphTraversalSource g = traversal().with(graph);
        g.addV("person").property("name", "Alice").property("embedding", new float[]{1.0f, 0.0f, 0.0f}).iterate();
        g.addV("person").property("name", "Bob").property("embedding", new float[]{0.0f, 1.0f, 0.0f}).iterate();
        tryCommitChanges(graph);

        graph.createIndex(TinkerIndexType.VECTOR,"embedding", Vertex.class, indexConfig);
        assertThat(graph.getIndexedKeys(Vertex.class).contains("embedding"), is(true));

        // Drop index
        graph.dropIndex("embedding", Vertex.class);
        assertThat(graph.getIndexedKeys(Vertex.class).contains("embedding"), is(false));

        try {
            graph.findNearestVertices("embedding", new float[]{1.0f, 0.0f, 0.0f}, 2);
            fail("Should have thrown exception since the index was removed");
        } catch (IllegalArgumentException ex) { }
    }

    @Test
    public void shouldFindNearestEdges() {
        final GraphTraversalSource g = traversal().with(graph);
        final Vertex alice = g.addV("person").property("name", "Alice").next();
        final Vertex bob = g.addV("person").property("name", "Bob").next();
        final Vertex charlie = g.addV("person").property("name", "Charlie").next();
        final Vertex dave = g.addV("person").property("name", "Dave").next();
        g.addE("knows").from(alice).to(bob).property("embedding", new float[]{1.0f, 0.0f, 0.0f}).property("strength", 0.8f).iterate();
        g.addE("knows").from(bob).to(charlie).property("embedding", new float[]{0.0f, 1.0f, 0.0f}).property("strength", 0.6f).iterate();
        g.addE("knows").from(charlie).to(dave).property("embedding", new float[]{0.0f, 0.0f, 1.0f}).property("strength", 0.7f).iterate();
        g.addE("knows").from(alice).to(dave).property("embedding", new float[]{0.9f, 0.1f, 0.0f}).property("strength", 0.9f).iterate();
        tryCommitChanges(graph);

        graph.createIndex(TinkerIndexType.VECTOR,"embedding", Edge.class, indexConfig);

        final List<Edge> nearest = graph.findNearestEdges("embedding", new float[]{1.0f, 0.0f, 0.0f}, 2);
        assertNotNull(nearest);
        assertEquals(2, nearest.size());
        assertEquals(0.8f, (float) nearest.get(0).value("strength"), 0.0001f);
        assertEquals(0.9f, (float) nearest.get(1).value("strength"), 0.0001f);
    }

    @Test
    public void shouldUpdateEdgeVectorIndex() {
        final GraphTraversalSource g = traversal().with(graph);
        final Vertex alice = g.addV("person").property("name", "Alice").next();
        final Vertex bob = g.addV("person").property("name", "Bob").next();
        g.addE("knows").from(alice).to(bob).property("embedding", new float[]{1.0f, 0.0f, 0.0f}).property("strength", 0.8f).iterate();
        final Edge edge = g.addE("knows").from(bob).to(alice).property("embedding", new float[]{0.0f, 1.0f, 0.0f}).property("strength", 0.6f).next();
        tryCommitChanges(graph);

        graph.createIndex(TinkerIndexType.VECTOR,"embedding", Edge.class, indexConfig);

        // Update an edge property
        g.E(edge.id()).property("embedding", new float[]{0.9f, 0.1f, 0.0f}).iterate();
        tryCommitChanges(graph);

        final List<Edge> nearest = graph.findNearestEdges("embedding", new float[]{1.0f, 0.0f, 0.0f}, 2);
        assertNotNull(nearest);
        assertEquals(2, nearest.size());
        assertEquals(0.8f, (float) nearest.get(0).value("strength"), 0.0001f);
        assertEquals(0.6f, (float) nearest.get(1).value("strength"), 0.0001f);
    }

    @Test
    public void shouldRemoveEdgeFromVectorIndex() {
        final GraphTraversalSource g = traversal().with(graph);
        final Vertex alice = g.addV("person").property("name", "Alice").next();
        final Vertex bob = g.addV("person").property("name", "Bob").next();
        final Vertex charlie = g.addV("person").property("name", "Charlie").next();
        g.addE("knows").from(alice).to(bob).property("embedding", new float[]{1.0f, 0.0f, 0.0f}).property("strength", 0.8f).iterate();
        final Edge edge = g.addE("knows").from(bob).to(charlie).property("embedding", new float[]{0.0f, 1.0f, 0.0f}).property("strength", 0.6f).next();
        g.addE("knows").from(charlie).to(alice).property("embedding", new float[]{0.0f, 0.0f, 1.0f}).property("strength", 0.7f).iterate();
        tryCommitChanges(graph);

        graph.createIndex(TinkerIndexType.VECTOR,"embedding", Edge.class, indexConfig);

        // Remove an edge
        g.E(edge.id()).drop().iterate();
        tryCommitChanges(graph);

        final List<Edge> nearest = graph.findNearestEdges("embedding", new float[]{0.0f, 1.0f, 0.0f}, 2);
        assertNotNull(nearest);
        assertEquals(2, nearest.size());
        assertThat(nearest.stream().noneMatch(e -> e.value("strength").equals(0.6f)), is(true));
    }

    @Test
    public void shouldDropEdgeVectorIndex() {
        final GraphTraversalSource g = traversal().with(graph);
        final Vertex alice = g.addV("person").property("name", "Alice").next();
        final Vertex bob = g.addV("person").property("name", "Bob").next();
        g.addE("knows").from(alice).to(bob).property("embedding", new float[]{1.0f, 0.0f, 0.0f}).property("strength", 0.8f).iterate();
        g.addE("knows").from(bob).to(alice).property("embedding", new float[]{0.0f, 1.0f, 0.0f}).property("strength", 0.6f).iterate();
        tryCommitChanges(graph);

        graph.createIndex(TinkerIndexType.VECTOR,"embedding", Edge.class, indexConfig);
        assertThat(graph.getIndexedKeys(Edge.class).contains("embedding"), is(true));

        // Drop index
        graph.dropIndex("embedding", Edge.class);
        assertThat(graph.getIndexedKeys(Edge.class).contains("embedding"), is(false));

        try {
            graph.findNearestEdges("embedding", new float[]{1.0f, 0.0f, 0.0f}, 2);
            fail("Should have thrown exception since the index was removed");
        } catch (IllegalArgumentException ex) { }
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenVectorDimensionExceedsConfigured() {
        final GraphTraversalSource g = traversal().with(graph);

        // Create a vector index with dimension 3
        graph.createIndex(TinkerIndexType.VECTOR,"embedding", Vertex.class, indexConfig);

        // Try to add a vertex with a vector of dimension 4 (exceeds configured dimension 3)
        g.addV("person").property("name", "Alice").property("embedding", new float[]{1.0f, 0.0f, 0.0f, 0.0f}).iterate();
        tryCommitChanges(graph);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenVectorDimensionIsSmallerThanConfigured() {
        final GraphTraversalSource g = traversal().with(graph);

        // Create a vector index with dimension 3
        graph.createIndex(TinkerIndexType.VECTOR,"embedding", Vertex.class, indexConfig);

        // Try to add a vertex with a vector of dimension 2 (smaller than configured dimension 3)
        g.addV("person").property("name", "Alice").property("embedding", new float[]{1.0f, 0.0f}).iterate();
        tryCommitChanges(graph);
    }

    @Test
    public void shouldRollbackVectorIndexChanges() {
        final GraphTraversalSource g = traversal().with(graph);
        g.addV("person").property("name", "Alice").property("embedding", new float[]{1.0f, 0.0f, 0.0f}).iterate();
        g.addV("person").property("name", "Bob").property("embedding", new float[]{0.0f, 1.0f, 0.0f}).iterate();
        tryCommitChanges(graph);

        graph.createIndex(TinkerIndexType.VECTOR, "embedding", Vertex.class, indexConfig);

        // Update a vertex property but rollback
        g.V().has("name", "Bob").property("embedding", new float[]{0.9f, 0.1f, 0.0f}).iterate();
        tryRollbackChanges(graph);

        // Bob's embedding should still be [0.0f, 1.0f, 0.0f]
        final List<Vertex> nearest = graph.findNearestVertices("embedding", new float[]{0.0f, 1.0f, 0.0f}, 1);
        assertNotNull(nearest);
        assertEquals(1, nearest.size());
        assertEquals("Bob", nearest.get(0).value("name"));
    }

    @Test
    public void shouldHandleEmptyGraphForNearestVertices() {
        graph.createIndex(TinkerIndexType.VECTOR, "embedding", Vertex.class, indexConfig);
        final List<Vertex> nearest = graph.findNearestVertices("embedding", new float[]{1.0f, 0.0f, 0.0f}, 2);
        assertNotNull(nearest);
        assertEquals(0, nearest.size());
    }

    @Test
    public void shouldHandleEmptyGraphForNearestEdges() {
        graph.createIndex(TinkerIndexType.VECTOR, "embedding", Edge.class, indexConfig);
        final List<Edge> nearest = graph.findNearestEdges("embedding", new float[]{1.0f, 0.0f, 0.0f}, 2);
        assertNotNull(nearest);
        assertEquals(0, nearest.size());
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionWhenIndexNotCreatedForNearestVertices() {
        graph.findNearestVertices("embedding", new float[]{1.0f, 0.0f, 0.0f}, 2);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionWhenIndexNotCreatedForNearestEdges() {
        graph.findNearestEdges("embedding", new float[]{1.0f, 0.0f, 0.0f}, 2);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionWhenIndexNotCreatedForNearestVerticesNoDefaultCount() {
        graph.findNearestVertices("embedding", new float[]{1.0f, 0.0f, 0.0f});
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionWhenIndexNotCreatedForNearestEdgesNoDefaultCount() {
        graph.findNearestEdges("embedding", new float[]{1.0f, 0.0f, 0.0f});
    }

    private void tryCommitChanges(final Graph graph) {
        if (graph.features().graph().supportsTransactions())
            graph.tx().commit();
    }

    private void tryRollbackChanges(final Graph graph) {
        if (graph.features().graph().supportsTransactions())
            graph.tx().rollback();
    }
}