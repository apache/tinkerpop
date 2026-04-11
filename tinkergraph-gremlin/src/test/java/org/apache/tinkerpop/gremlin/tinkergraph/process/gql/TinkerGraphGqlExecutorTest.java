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
package org.apache.tinkerpop.gremlin.tinkergraph.process.gql;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link TinkerGraphGqlExecutor}: DFS backtracking pattern matching,
 * label constraints, equality constraints for shared variables, edge variable binding,
 * self-loops, and multi-hop paths.
 */
public class TinkerGraphGqlExecutorTest {

    private TinkerGraph graph;
    private TinkerGraphGqlPlanner planner;
    private TinkerGraphGqlExecutor executor;

    @Before
    public void setUp() {
        graph = TinkerGraph.open();
        planner = new TinkerGraphGqlPlanner(graph);
        executor = new TinkerGraphGqlExecutor(graph);
    }

    @After
    public void tearDown() {
        graph.close();
    }

    // -------------------------------------------------------------------------
    // Empty / no-match cases
    // -------------------------------------------------------------------------

    @Test
    public void testEmptyGraphReturnsNoResults() {
        final GqlMatchPlan plan = planner.plan("MATCH (n:Person)");
        final List<Map<String, Element>> results = executor.execute(plan);
        assertTrue(results.isEmpty());
    }

    @Test
    public void testSingleNodeNoLabelMatch() {
        final Vertex v = graph.addVertex("Person");
        final GqlMatchPlan plan = planner.plan("MATCH (n:Animal)");
        assertTrue(executor.execute(plan).isEmpty());
    }

    @Test
    public void testEdgePatternNoMatchWhenNoEdges() {
        graph.addVertex("Person");
        graph.addVertex("Person");
        final GqlMatchPlan plan = planner.plan("MATCH (a:Person)-[:KNOWS]->(b:Person)");
        assertTrue(executor.execute(plan).isEmpty());
    }

    // -------------------------------------------------------------------------
    // Single-node patterns
    // -------------------------------------------------------------------------

    @Test
    public void testSingleNodeWithLabel() {
        final Vertex alice = graph.addVertex("Person");
        final Vertex bob = graph.addVertex("Person");
        graph.addVertex("Company");

        final GqlMatchPlan plan = planner.plan("MATCH (n:Person)");
        final List<Map<String, Element>> results = executor.execute(plan);

        assertEquals(2, results.size());
        final List<Element> found = results.stream()
                .map(r -> r.get("n")).collect(Collectors.toList());
        assertTrue(found.contains(alice));
        assertTrue(found.contains(bob));
    }

    @Test
    public void testSingleNodeWithoutLabel() {
        final Vertex v1 = graph.addVertex("Person");
        final Vertex v2 = graph.addVertex("Company");

        final GqlMatchPlan plan = planner.plan("MATCH (n)");
        final List<Map<String, Element>> results = executor.execute(plan);
        assertEquals(2, results.size());
    }

    // -------------------------------------------------------------------------
    // Single-edge patterns
    // -------------------------------------------------------------------------

    @Test
    public void testSingleOutEdgeWithLabel() {
        final Vertex alice = graph.addVertex("Person");
        final Vertex acme = graph.addVertex("Company");
        final Edge e = alice.addEdge("WORKS_AT", acme);

        final GqlMatchPlan plan = planner.plan("MATCH (a:Person)-[:WORKS_AT]->(c:Company)");
        final List<Map<String, Element>> results = executor.execute(plan);

        assertEquals(1, results.size());
        final Map<String, Element> row = results.get(0);
        assertEquals(alice, row.get("a"));
        assertEquals(acme, row.get("c"));
    }

    @Test
    public void testSingleOutEdgeDoesNotMatchWrongDirection() {
        final Vertex alice = graph.addVertex("Person");
        final Vertex acme = graph.addVertex("Company");
        acme.addEdge("WORKS_AT", alice);  // reversed — acme→alice

        final GqlMatchPlan plan = planner.plan("MATCH (a:Person)-[:WORKS_AT]->(c:Company)");
        assertTrue(executor.execute(plan).isEmpty());
    }

    @Test
    public void testSingleInEdge() {
        final Vertex alice = graph.addVertex("Person");
        final Vertex bob = graph.addVertex("Person");
        alice.addEdge("KNOWS", bob);  // alice→bob, so bob<-KNOWS-alice

        // MATCH (a)<-[:KNOWS]-(b) means a is the in-vertex of KNOWS
        final GqlMatchPlan plan = planner.plan("MATCH (a:Person)<-[:KNOWS]-(b:Person)");
        final List<Map<String, Element>> results = executor.execute(plan);

        assertEquals(1, results.size());
        assertEquals(bob, results.get(0).get("a"));
        assertEquals(alice, results.get(0).get("b"));
    }

    @Test
    public void testUndirectedEdge() {
        final Vertex alice = graph.addVertex("Person");
        final Vertex bob = graph.addVertex("Person");
        alice.addEdge("KNOWS", bob);

        // Undirected matches both directions
        final GqlMatchPlan plan = planner.plan("MATCH (a:Person)-[:KNOWS]-(b:Person)");
        final List<Map<String, Element>> results = executor.execute(plan);

        // One edge, but both (alice,bob) and (bob,alice) should match
        assertEquals(2, results.size());
    }

    @Test
    public void testNamedEdgeVariable() {
        final Vertex alice = graph.addVertex("Person");
        final Vertex acme = graph.addVertex("Company");
        final Edge e = alice.addEdge("WORKS_AT", acme);

        final GqlMatchPlan plan = planner.plan("MATCH (a:Person)-[r:WORKS_AT]->(c:Company)");
        final List<Map<String, Element>> results = executor.execute(plan);

        assertEquals(1, results.size());
        final Map<String, Element> row = results.get(0);
        assertEquals(e, row.get("r"));
        assertEquals(alice, row.get("a"));
        assertEquals(acme, row.get("c"));
    }

    @Test
    public void testAnonymousEdge() {
        final Vertex alice = graph.addVertex("Person");
        final Vertex acme = graph.addVertex("Company");
        alice.addEdge("WORKS_AT", acme);

        final GqlMatchPlan plan = planner.plan("MATCH (a:Person)-[]->(c:Company)");
        final List<Map<String, Element>> results = executor.execute(plan);

        assertEquals(1, results.size());
        assertFalse(results.get(0).containsKey("r")); // no edge variable
    }

    @Test
    public void testWrongEdgeLabelNotMatched() {
        final Vertex alice = graph.addVertex("Person");
        final Vertex acme = graph.addVertex("Company");
        alice.addEdge("LIKES", acme);  // different label

        final GqlMatchPlan plan = planner.plan("MATCH (a:Person)-[:WORKS_AT]->(c:Company)");
        assertTrue(executor.execute(plan).isEmpty());
    }

    // -------------------------------------------------------------------------
    // Multi-hop paths
    // -------------------------------------------------------------------------

    @Test
    public void testTwoHopPath() {
        final Vertex alice = graph.addVertex("Person");
        final Vertex bob = graph.addVertex("Person");
        final Vertex acme = graph.addVertex("Company");
        alice.addEdge("KNOWS", bob);
        bob.addEdge("WORKS_AT", acme);

        final GqlMatchPlan plan = planner.plan(
                "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:WORKS_AT]->(c:Company)");
        final List<Map<String, Element>> results = executor.execute(plan);

        assertEquals(1, results.size());
        final Map<String, Element> row = results.get(0);
        assertEquals(alice, row.get("a"));
        assertEquals(bob, row.get("b"));
        assertEquals(acme, row.get("c"));
    }

    @Test
    public void testTwoHopPathMultipleMatches() {
        final Vertex alice = graph.addVertex("Person");
        final Vertex bob = graph.addVertex("Person");
        final Vertex carol = graph.addVertex("Person");
        final Vertex acme = graph.addVertex("Company");
        alice.addEdge("KNOWS", bob);
        carol.addEdge("KNOWS", bob);
        bob.addEdge("WORKS_AT", acme);

        final GqlMatchPlan plan = planner.plan(
                "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:WORKS_AT]->(c:Company)");
        final List<Map<String, Element>> results = executor.execute(plan);

        // Both alice and carol know bob, who works at acme
        assertEquals(2, results.size());
    }

    // -------------------------------------------------------------------------
    // Shared variable (equality constraint)
    // -------------------------------------------------------------------------

    @Test
    public void testSharedVariableAcrossPatterns() {
        final Vertex alice = graph.addVertex("Person");
        final Vertex bob = graph.addVertex("Person");
        final Vertex acme = graph.addVertex("Company");
        alice.addEdge("KNOWS", bob);
        alice.addEdge("WORKS_AT", acme);
        bob.addEdge("WORKS_AT", acme);

        // b must be both the target of KNOWS and the source of WORKS_AT
        final GqlMatchPlan plan = planner.plan(
                "MATCH (a:Person)-[:KNOWS]->(b:Person), (b)-[:WORKS_AT]->(c:Company)");
        final List<Map<String, Element>> results = executor.execute(plan);

        // Only alice→bob→acme matches (alice doesn't know itself as a KNOWS target for alice)
        assertEquals(1, results.size());
        assertEquals(alice, results.get(0).get("a"));
        assertEquals(bob, results.get(0).get("b"));
        assertEquals(acme, results.get(0).get("c"));
    }

    @Test
    public void testSelfLoopPattern() {
        final Vertex alice = graph.addVertex("Person");
        alice.addEdge("SELF", alice);  // self-loop

        final GqlMatchPlan plan = planner.plan("MATCH (n:Person)-[:SELF]->(n:Person)");
        final List<Map<String, Element>> results = executor.execute(plan);

        assertEquals(1, results.size());
        assertEquals(alice, results.get(0).get("n"));
    }

    @Test
    public void testSelfLoopNotMatchedWhenNoSelfEdge() {
        final Vertex alice = graph.addVertex("Person");
        final Vertex bob = graph.addVertex("Person");
        alice.addEdge("SELF", bob);  // alice → bob, not self-loop

        final GqlMatchPlan plan = planner.plan("MATCH (n:Person)-[:SELF]->(n:Person)");
        assertTrue(executor.execute(plan).isEmpty());
    }

    // -------------------------------------------------------------------------
    // Triangle (cycle with back-edge equality constraint)
    // -------------------------------------------------------------------------

    @Test
    public void testTrianglePattern() {
        final Vertex a = graph.addVertex("A");
        final Vertex b = graph.addVertex("B");
        final Vertex c = graph.addVertex("C");
        a.addEdge("AB", b);
        b.addEdge("BC", c);
        c.addEdge("CA", a);

        final GqlMatchPlan plan = planner.plan(
                "MATCH (a:A)-[:AB]->(b:B)-[:BC]->(c:C)-[:CA]->(a:A)");
        final List<Map<String, Element>> results = executor.execute(plan);

        assertEquals(1, results.size());
        assertEquals(a, results.get(0).get("a"));
        assertEquals(b, results.get(0).get("b"));
        assertEquals(c, results.get(0).get("c"));
    }

    @Test
    public void testTriangleNotMatchedWhenCycleIncomplete() {
        final Vertex a = graph.addVertex("A");
        final Vertex b = graph.addVertex("B");
        final Vertex c = graph.addVertex("C");
        a.addEdge("AB", b);
        b.addEdge("BC", c);
        // Missing c→a edge

        final GqlMatchPlan plan = planner.plan(
                "MATCH (a:A)-[:AB]->(b:B)-[:BC]->(c:C)-[:CA]->(a:A)");
        assertTrue(executor.execute(plan).isEmpty());
    }

    // -------------------------------------------------------------------------
    // Result immutability
    // -------------------------------------------------------------------------

    @Test
    public void testResultIsUnmodifiable() {
        graph.addVertex("Person");
        final GqlMatchPlan plan = planner.plan("MATCH (n:Person)");
        final List<Map<String, Element>> results = executor.execute(plan);

        try {
            results.add(new java.util.HashMap<>());
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException ex) {
            // expected
        }
    }
}
