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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
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
    // Test helper: materialise lazy iterator into a List<Map<String,Element>>
    // -------------------------------------------------------------------------

    /**
     * Drains the lazy result iterator into a list of named-variable maps, mirroring the
     * old List<Map> API so that existing assertions can be reused without change.
     * Entries whose variable name starts with {@code $anon} are excluded (anonymous nodes).
     */
    private List<Map<String, Element>> materialize(final Iterator<Element[]> iter,
                                                    final GqlMatchPlan plan) {
        final String[] variables = plan.getVariables();
        final List<Map<String, Element>> rows = new ArrayList<>();
        while (iter.hasNext()) {
            final Element[] row = iter.next();
            final Map<String, Element> map = new LinkedHashMap<>();
            for (int i = 0; i < variables.length; i++) {
                if (!variables[i].startsWith("$anon") && row[i] != null)
                    map.put(variables[i], row[i]);
            }
            rows.add(map);
        }
        return rows;
    }

    private List<Map<String, Element>> execute(final String query) {
        final GqlMatchPlan plan = planner.plan(query);
        return materialize(executor.execute(plan), plan);
    }

    // -------------------------------------------------------------------------
    // Empty / no-match cases
    // -------------------------------------------------------------------------

    @Test
    public void testEmptyGraphReturnsNoResults() {
        assertTrue(execute("MATCH (n:Person)").isEmpty());
    }

    @Test
    public void testSingleNodeNoLabelMatch() {
        graph.addVertex("Person");
        assertTrue(execute("MATCH (n:Animal)").isEmpty());
    }

    @Test
    public void testEdgePatternNoMatchWhenNoEdges() {
        graph.addVertex("Person");
        graph.addVertex("Person");
        assertTrue(execute("MATCH (a:Person)-[:KNOWS]->(b:Person)").isEmpty());
    }

    // -------------------------------------------------------------------------
    // Single-node patterns
    // -------------------------------------------------------------------------

    @Test
    public void testSingleNodeWithLabel() {
        final Vertex alice = graph.addVertex("Person");
        final Vertex bob = graph.addVertex("Person");
        graph.addVertex("Company");

        final List<Map<String, Element>> results = execute("MATCH (n:Person)");

        assertEquals(2, results.size());
        final List<Element> found = results.stream()
                .map(r -> r.get("n")).collect(Collectors.toList());
        assertTrue(found.contains(alice));
        assertTrue(found.contains(bob));
    }

    @Test
    public void testSingleNodeWithoutLabel() {
        graph.addVertex("Person");
        graph.addVertex("Company");
        assertEquals(2, execute("MATCH (n)").size());
    }

    // -------------------------------------------------------------------------
    // Single-edge patterns
    // -------------------------------------------------------------------------

    @Test
    public void testSingleOutEdgeWithLabel() {
        final Vertex alice = graph.addVertex("Person");
        final Vertex acme = graph.addVertex("Company");
        alice.addEdge("WORKS_AT", acme);

        final List<Map<String, Element>> results = execute("MATCH (a:Person)-[:WORKS_AT]->(c:Company)");

        assertEquals(1, results.size());
        assertEquals(alice, results.get(0).get("a"));
        assertEquals(acme, results.get(0).get("c"));
    }

    @Test
    public void testSingleOutEdgeDoesNotMatchWrongDirection() {
        final Vertex alice = graph.addVertex("Person");
        final Vertex acme = graph.addVertex("Company");
        acme.addEdge("WORKS_AT", alice);  // reversed — acme→alice
        assertTrue(execute("MATCH (a:Person)-[:WORKS_AT]->(c:Company)").isEmpty());
    }

    @Test
    public void testSingleInEdge() {
        final Vertex alice = graph.addVertex("Person");
        final Vertex bob = graph.addVertex("Person");
        alice.addEdge("KNOWS", bob);  // alice→bob, so bob<-KNOWS-alice

        final List<Map<String, Element>> results = execute("MATCH (a:Person)<-[:KNOWS]-(b:Person)");

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
        assertEquals(2, execute("MATCH (a:Person)-[:KNOWS]-(b:Person)").size());
    }

    @Test
    public void testNamedEdgeVariable() {
        final Vertex alice = graph.addVertex("Person");
        final Vertex acme = graph.addVertex("Company");
        final Edge e = alice.addEdge("WORKS_AT", acme);

        final List<Map<String, Element>> results = execute("MATCH (a:Person)-[r:WORKS_AT]->(c:Company)");

        assertEquals(1, results.size());
        assertEquals(e, results.get(0).get("r"));
        assertEquals(alice, results.get(0).get("a"));
        assertEquals(acme, results.get(0).get("c"));
    }

    @Test
    public void testAnonymousEdge() {
        final Vertex alice = graph.addVertex("Person");
        final Vertex acme = graph.addVertex("Company");
        alice.addEdge("WORKS_AT", acme);

        final List<Map<String, Element>> results = execute("MATCH (a:Person)-[]->(c:Company)");

        assertEquals(1, results.size());
        assertFalse(results.get(0).containsKey("r")); // no edge variable
    }

    @Test
    public void testWrongEdgeLabelNotMatched() {
        final Vertex alice = graph.addVertex("Person");
        final Vertex acme = graph.addVertex("Company");
        alice.addEdge("LIKES", acme);  // different label
        assertTrue(execute("MATCH (a:Person)-[:WORKS_AT]->(c:Company)").isEmpty());
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

        final List<Map<String, Element>> results = execute(
                "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:WORKS_AT]->(c:Company)");

        assertEquals(1, results.size());
        assertEquals(alice, results.get(0).get("a"));
        assertEquals(bob, results.get(0).get("b"));
        assertEquals(acme, results.get(0).get("c"));
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

        // Both alice and carol know bob, who works at acme
        assertEquals(2, execute("MATCH (a:Person)-[:KNOWS]->(b:Person)-[:WORKS_AT]->(c:Company)").size());
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

        final List<Map<String, Element>> results = execute(
                "MATCH (a:Person)-[:KNOWS]->(b:Person), (b)-[:WORKS_AT]->(c:Company)");

        assertEquals(1, results.size());
        assertEquals(alice, results.get(0).get("a"));
        assertEquals(bob, results.get(0).get("b"));
        assertEquals(acme, results.get(0).get("c"));
    }

    @Test
    public void testSelfLoopPattern() {
        final Vertex alice = graph.addVertex("Person");
        alice.addEdge("SELF", alice);

        final List<Map<String, Element>> results = execute("MATCH (n:Person)-[:SELF]->(n:Person)");

        assertEquals(1, results.size());
        assertEquals(alice, results.get(0).get("n"));
    }

    @Test
    public void testSelfLoopNotMatchedWhenNoSelfEdge() {
        final Vertex alice = graph.addVertex("Person");
        final Vertex bob = graph.addVertex("Person");
        alice.addEdge("SELF", bob);  // alice → bob, not self-loop
        assertTrue(execute("MATCH (n:Person)-[:SELF]->(n:Person)").isEmpty());
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

        final List<Map<String, Element>> results = execute(
                "MATCH (a:A)-[:AB]->(b:B)-[:BC]->(c:C)-[:CA]->(a:A)");

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
        assertTrue(execute("MATCH (a:A)-[:AB]->(b:B)-[:BC]->(c:C)-[:CA]->(a:A)").isEmpty());
    }

    // -------------------------------------------------------------------------
    // Lazy delivery: each row is an independent array snapshot
    // -------------------------------------------------------------------------

    @Test
    public void testResultRowsAreIndependentSnapshots() {
        // Verify that the Element[] arrays returned by the iterator are independent copies —
        // modifying one does not corrupt another (backtracking correctness).
        graph.addVertex("Person");
        graph.addVertex("Person");
        final GqlMatchPlan plan = planner.plan("MATCH (n:Person)");
        final Iterator<Element[]> iter = executor.execute(plan);

        final Element[] first = iter.next();
        final Element first0 = first[0];
        // Corrupt the first row's array
        first[0] = null;

        // Second row must be unaffected
        assertTrue(iter.hasNext());
        final Element[] second = iter.next();
        assertNotNull(second[0]);
        assertNotSame(first, second);
    }
}
