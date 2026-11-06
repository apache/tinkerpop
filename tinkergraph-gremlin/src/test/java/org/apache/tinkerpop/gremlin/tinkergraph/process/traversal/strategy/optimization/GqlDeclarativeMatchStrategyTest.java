/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.strategy.optimization;

import org.apache.tinkerpop.gremlin.gql.GqlDeclarativeMatchStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Integration tests for the full path through {@link GqlDeclarativeMatchStrategy}: uses a real
 * {@link TinkerGraph} traversal source (no substitute strategy) so the registered singleton
 * strategy is exercised end-to-end.
 */
public class GqlDeclarativeMatchStrategyTest {

    private TinkerGraph graph;
    private GraphTraversalSource g;

    @Before
    public void setUp() {
        graph = TinkerGraph.open();
        g = graph.traversal(); // no withStrategies — uses the auto-registered singleton
    }

    @After
    public void tearDown() {
        graph.close();
    }

    @Test
    public void testSingleNodePatternEmptyGraph() {
        final List<Vertex> results = g.<Integer>inject(1)
                .match("MATCH (n:Person)")
                .<Vertex>select("n")
                .toList();
        assertTrue(results.isEmpty());
    }

    @Test
    public void testSingleNodePatternBindsVariable() {
        final Vertex alice = graph.addVertex("Person");
        final Vertex bob = graph.addVertex("Person");
        graph.addVertex("Company");

        final List<Vertex> results = g.<Integer>inject(1)
                .match("MATCH (n:Person)")
                .<Vertex>select("n")
                .toList();

        assertEquals(2, results.size());
        assertTrue(results.contains(alice));
        assertTrue(results.contains(bob));
    }

    @Test
    public void testEdgePatternBindsBothEndpoints() {
        final Vertex alice = graph.addVertex("Person");
        final Vertex acme = graph.addVertex("Company");
        alice.addEdge("WORKS_AT", acme);

        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> results =
                (List<Map<String, Object>>) (List<?>) g.<Integer>inject(1)
                        .match("MATCH (a:Person)-[:WORKS_AT]->(c:Company)")
                        .select("a", "c")
                        .toList();

        assertEquals(1, results.size());
        assertEquals(alice, results.get(0).get("a"));
        assertEquals(acme, results.get(0).get("c"));
    }

    @Test
    public void testNoMatchingEdgeReturnsEmpty() {
        graph.addVertex("Person");
        graph.addVertex("Person");

        final List<Object> results = g.<Integer>inject(1)
                .match("MATCH (a:Person)-[:KNOWS]->(b:Person)")
                .select("a")
                .toList();
        assertTrue(results.isEmpty());
    }

    @Test
    public void testSharedPlanCacheAcrossTraversals() {
        // Two traversals with the same query string should share the compiled plan cache
        // (the singleton strategy caches the planner per graph instance). Verify correctness
        // on both executions; a shared plan cache means the second parse is a cache hit.
        graph.addVertex("Person");
        graph.addVertex("Person");

        final long count1 = g.<Integer>inject(1)
                .match("MATCH (n:Person)")
                .<Vertex>select("n")
                .toList().size();
        final long count2 = g.<Integer>inject(1)
                .match("MATCH (n:Person)")
                .<Vertex>select("n")
                .toList().size();

        assertEquals(2, count1);
        assertEquals(2, count2);
    }

    @Test
    public void testEvictAndReuseDoesNotCorruptResults() {
        // Evict the cache entry mid-session, then run another traversal. The strategy must
        // allocate a fresh planner/executor pair and produce correct results.
        graph.addVertex("Person");

        final long countBefore = g.<Integer>inject(1)
                .match("MATCH (n:Person)")
                .<Vertex>select("n")
                .toList().size();
        assertEquals(1, countBefore);

        GqlDeclarativeMatchStrategy.evict(graph);

        // Add a second vertex after eviction to make the test sensitive to state corruption.
        graph.addVertex("Person");

        final long countAfter = g.<Integer>inject(1)
                .match("MATCH (n:Person)")
                .<Vertex>select("n")
                .toList().size();
        assertEquals(2, countAfter);
    }

    @Test
    public void testSourceSpawnSingleNodePattern() {
        final Vertex alice = graph.addVertex("Person");
        final Vertex bob = graph.addVertex("Person");
        graph.addVertex("Company");

        final List<Vertex> results = g.match("MATCH (n:Person)")
                .<Vertex>select("n")
                .toList();

        assertEquals(2, results.size());
        assertTrue(results.contains(alice));
        assertTrue(results.contains(bob));
    }

    @Test
    public void testSourceSpawnEdgePattern() {
        final Vertex alice = graph.addVertex("Person");
        final Vertex acme = graph.addVertex("Company");
        alice.addEdge("WORKS_AT", acme);

        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> results =
                (List<Map<String, Object>>) (List<?>) g.match("MATCH (a:Person)-[:WORKS_AT]->(c:Company)")
                        .select("a", "c")
                        .toList();

        assertEquals(1, results.size());
        assertEquals(alice, results.get(0).get("a"));
        assertEquals(acme, results.get(0).get("c"));
    }

    @Test
    public void testTerminalMatchStepReturnsBindingMap() {
        final Vertex alice = graph.addVertex("Person");
        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> results =
                (List<Map<String, Object>>) (List<?>) g.<Integer>inject(1)
                        .match("MATCH (n:Person)")
                        .toList();
        assertEquals(1, results.size());
        assertEquals(alice, results.get(0).get("n"));
    }

    @Test
    public void testMultiHopPatternWithSharedVariable() {
        final Vertex alice = graph.addVertex("Person");
        final Vertex bob = graph.addVertex("Person");
        final Vertex acme = graph.addVertex("Company");
        alice.addEdge("KNOWS", bob);
        bob.addEdge("WORKS_AT", acme);

        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> results =
                (List<Map<String, Object>>) (List<?>) g.<Integer>inject(1)
                        .match("MATCH (a:Person)-[:KNOWS]->(b:Person), (b)-[:WORKS_AT]->(c:Company)")
                        .select("a", "b", "c")
                        .toList();

        assertEquals(1, results.size());
        assertEquals(alice, results.get(0).get("a"));
        assertEquals(bob, results.get(0).get("b"));
        assertEquals(acme, results.get(0).get("c"));
    }
}
