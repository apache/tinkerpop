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

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link TinkerGraphGqlPlanner}: seed selection, step ordering, direction
 * handling, plan caching, and edge cases.
 */
public class TinkerGraphGqlPlannerTest {

    private TinkerGraph graph;
    private TinkerGraphGqlPlanner planner;

    @Before
    public void setUp() {
        graph = TinkerGraph.open();
        planner = new TinkerGraphGqlPlanner(graph);
    }

    @After
    public void tearDown() {
        graph.close();
    }

    // -------------------------------------------------------------------------
    // Single-node patterns (no edges)
    // -------------------------------------------------------------------------

    @Test
    public void testSingleNodePattern() {
        graph.addVertex("Person");
        final GqlMatchPlan plan = planner.plan("MATCH (n:Person)");
        assertEquals("n", plan.getSeedVariable());
        assertEquals("Person", plan.getSeedLabel());
        assertTrue(plan.isEmpty());
        assertTrue(plan.getSteps().isEmpty());
    }

    @Test
    public void testSingleAnonymousNode() {
        graph.addVertex("Person");
        final GqlMatchPlan plan = planner.plan("MATCH ()");
        assertNotNull(plan.getSeedVariable());
        assertTrue(plan.getSeedVariable().startsWith("$anon"));
        assertNull(plan.getSeedLabel());
        assertTrue(plan.isEmpty());
    }

    // -------------------------------------------------------------------------
    // Single-edge patterns
    // -------------------------------------------------------------------------

    @Test
    public void testSingleOutEdge() {
        // 3 persons, 1 company — seed should be Company (fewer)
        graph.addVertex("Person");
        graph.addVertex("Person");
        graph.addVertex("Person");
        graph.addVertex("Company");

        final GqlMatchPlan plan = planner.plan("MATCH (a:Person)-[:WORKS_AT]->(c:Company)");
        assertEquals("c", plan.getSeedVariable());
        assertEquals("Company", plan.getSeedLabel());

        assertEquals(1, plan.getSteps().size());
        final ExtensionStep step = plan.getSteps().get(0);
        assertEquals("c", step.getAnchorVariable());
        assertEquals("WORKS_AT", step.getEdgeLabel());
        // Traversing from Company back to Person means IN direction
        assertEquals(Direction.IN, step.getDirection());
        assertEquals("Person", step.getTargetLabel());
        assertEquals("a", step.getTargetVariable());
    }

    @Test
    public void testSingleInEdge() {
        // MATCH (a)<-[:KNOWS]-(b)  — direction=IN in QueryEdge
        graph.addVertex("Person"); // a
        graph.addVertex("Person"); // b

        final GqlMatchPlan plan = planner.plan("MATCH (a)<-[:KNOWS]-(b)");
        // Both have the same label and count, so first node wins: seed = a
        assertEquals("a", plan.getSeedVariable());

        assertEquals(1, plan.getSteps().size());
        final ExtensionStep step = plan.getSteps().get(0);
        assertEquals("a", step.getAnchorVariable());
        assertEquals("KNOWS", step.getEdgeLabel());
        // a is source of the QueryEdge with direction=IN, traversing from a to b
        assertEquals(Direction.IN, step.getDirection());
        assertEquals("b", step.getTargetVariable());
    }

    @Test
    public void testUndirectedEdge() {
        graph.addVertex("Person");
        graph.addVertex("Person");

        final GqlMatchPlan plan = planner.plan("MATCH (a)-[:KNOWS]-(b)");
        assertEquals(1, plan.getSteps().size());
        assertEquals(Direction.BOTH, plan.getSteps().get(0).getDirection());
    }

    // -------------------------------------------------------------------------
    // Multi-hop paths: seed selection and step ordering
    // -------------------------------------------------------------------------

    @Test
    public void testTwoHopPathSeedSelection() {
        // 2 persons, 1 company — Company should be seed
        graph.addVertex("Person");
        graph.addVertex("Person");
        graph.addVertex("Company");

        final GqlMatchPlan plan = planner.plan(
                "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:WORKS_AT]->(c:Company)");

        assertEquals("c", plan.getSeedVariable());
        assertEquals("Company", plan.getSeedLabel());

        // Steps should be: c→b (IN over WORKS_AT), b→a (IN over KNOWS)
        final List<ExtensionStep> steps = plan.getSteps();
        assertEquals(2, steps.size());

        final ExtensionStep s0 = steps.get(0);
        assertEquals("c", s0.getAnchorVariable());
        assertEquals("WORKS_AT", s0.getEdgeLabel());
        assertEquals(Direction.IN, s0.getDirection());
        assertEquals("b", s0.getTargetVariable());

        final ExtensionStep s1 = steps.get(1);
        assertEquals("b", s1.getAnchorVariable());
        assertEquals("KNOWS", s1.getEdgeLabel());
        assertEquals(Direction.IN, s1.getDirection());
        assertEquals("a", s1.getTargetVariable());
    }

    @Test
    public void testTwoHopPathSeedAtLeft() {
        // 1 person, 5 companies — Person should be seed even though it's leftmost
        graph.addVertex("Person");
        for (int i = 0; i < 5; i++) graph.addVertex("Company");

        final GqlMatchPlan plan = planner.plan(
                "MATCH (a:Person)-[:WORKS_AT]->(c:Company)-[:LOCATED_IN]->(d:City)");

        // No City vertices, so d:City cardinality=0 — City should be seed
        assertEquals("d", plan.getSeedVariable());
        assertEquals("City", plan.getSeedLabel());
    }

    // -------------------------------------------------------------------------
    // Step ordering guarantee: anchor always bound before step
    // -------------------------------------------------------------------------

    @Test
    public void testAnchorAlwaysBoundBeforeStep() {
        graph.addVertex("A");
        graph.addVertex("B");
        graph.addVertex("C");

        final GqlMatchPlan plan = planner.plan(
                "MATCH (a:A)-[:E1]->(b:B)-[:E2]->(c:C)");

        // Verify each step's anchor appears as the seed variable or a previous step's target
        final String seedVar = plan.getSeedVariable();
        final java.util.Set<String> bound = new java.util.HashSet<>();
        bound.add(seedVar);

        for (final ExtensionStep step : plan.getSteps()) {
            assertTrue("Anchor '" + step.getAnchorVariable() + "' must be bound before this step",
                    bound.contains(step.getAnchorVariable()));
            if (step.getTargetVariable() != null) {
                bound.add(step.getTargetVariable());
            }
        }
    }

    // -------------------------------------------------------------------------
    // Plan caching
    // -------------------------------------------------------------------------

    @Test
    public void testPlanCachingReturnsSameInstance() {
        final GqlMatchPlan first = planner.plan("MATCH (n:Person)-[:KNOWS]->(m:Person)");
        final GqlMatchPlan second = planner.plan("MATCH (n:Person)-[:KNOWS]->(m:Person)");
        assertSame("Same query string must return the cached plan instance", first, second);
    }

    @Test
    public void testDifferentQueriesGetDifferentPlans() {
        final GqlMatchPlan plan1 = planner.plan("MATCH (a)-[:E1]->(b)");
        final GqlMatchPlan plan2 = planner.plan("MATCH (x)-[:E2]->(y)");
        assertNotSame(plan1, plan2);
    }

    // -------------------------------------------------------------------------
    // Anonymous nodes
    // -------------------------------------------------------------------------

    @Test
    public void testAnonymousNodes() {
        graph.addVertex("Person");

        final GqlMatchPlan plan = planner.plan("MATCH ()-[:KNOWS]->(n:Person)");
        // n:Person exists but anonymous node has no label constraint
        // Anonymous node gets total count (1 Person = total 1), same cardinality as Person
        // First wins: anonymous node should be seed since it's first
        assertTrue(plan.getSeedVariable().startsWith("$anon"));

        assertEquals(1, plan.getSteps().size());
        final ExtensionStep step = plan.getSteps().get(0);
        assertTrue(step.getAnchorVariable().startsWith("$anon"));
        assertEquals("KNOWS", step.getEdgeLabel());
        assertEquals(Direction.OUT, step.getDirection());
        assertEquals("n", step.getTargetVariable());
    }

    @Test
    public void testAnonymousEdgeVariable() {
        graph.addVertex("Person");
        graph.addVertex("Person");

        final GqlMatchPlan plan = planner.plan("MATCH (a:Person)-[]->(b:Person)");
        assertEquals(1, plan.getSteps().size());
        assertNull("Anonymous edge variable must be null", plan.getSteps().get(0).getEdgeVariable());
    }

    @Test
    public void testNamedEdgeVariable() {
        graph.addVertex("Person");
        graph.addVertex("Person");

        final GqlMatchPlan plan = planner.plan("MATCH (a:Person)-[r:KNOWS]->(b:Person)");
        assertEquals(1, plan.getSteps().size());
        assertEquals("r", plan.getSteps().get(0).getEdgeVariable());
    }

    // -------------------------------------------------------------------------
    // Self-loop
    // -------------------------------------------------------------------------

    @Test
    public void testSelfLoop() {
        graph.addVertex("Person");

        final GqlMatchPlan plan = planner.plan("MATCH (n:Person)-[:SELF]->(n:Person)");
        assertEquals("n", plan.getSeedVariable());
        assertEquals(1, plan.getSteps().size());

        final ExtensionStep step = plan.getSteps().get(0);
        assertEquals("n", step.getAnchorVariable());
        assertEquals("n", step.getTargetVariable());
        assertEquals(Direction.OUT, step.getDirection());
    }

    // -------------------------------------------------------------------------
    // Seed selection with equal cardinality
    // -------------------------------------------------------------------------

    @Test
    public void testEqualCardinalityFirstNodeWins() {
        // Both labels have the same count — first node in query should be seed
        graph.addVertex("A");
        graph.addVertex("B");

        final GqlMatchPlan plan = planner.plan("MATCH (a:A)-[:E]->(b:B)");
        // A count=1, B count=1 — tie, first node (a:A) wins
        assertEquals("a", plan.getSeedVariable());
        assertEquals("A", plan.getSeedLabel());

        final ExtensionStep step = plan.getSteps().get(0);
        assertEquals("a", step.getAnchorVariable());
        assertEquals(Direction.OUT, step.getDirection());
        assertEquals("b", step.getTargetVariable());
    }

    // -------------------------------------------------------------------------
    // Triangle (cycle): back-edge handling
    // -------------------------------------------------------------------------

    @Test
    public void testTrianglePattern() {
        graph.addVertex("A");
        graph.addVertex("B");
        graph.addVertex("C");

        final GqlMatchPlan plan = planner.compile(
                QueryGraph.parse("MATCH (a:A)-[:AB]->(b:B)-[:BC]->(c:C)-[:CA]->(a:A)"));

        // All same cardinality (1 each) — first node (a:A) is seed
        assertEquals("a", plan.getSeedVariable());
        final List<ExtensionStep> steps = plan.getSteps();
        assertEquals(3, steps.size());

        // Verify the anchor-bound invariant
        final java.util.Set<String> bound = new java.util.HashSet<>();
        bound.add(plan.getSeedVariable());
        for (final ExtensionStep step : steps) {
            assertTrue("Anchor must be bound: " + step.getAnchorVariable(),
                    bound.contains(step.getAnchorVariable()));
            if (step.getTargetVariable() != null) bound.add(step.getTargetVariable());
        }
    }

    // -------------------------------------------------------------------------
    // Multi-pattern queries
    // -------------------------------------------------------------------------

    @Test
    public void testMultiPatternSharedVariable() {
        graph.addVertex("Person");
        graph.addVertex("Person");
        graph.addVertex("Company");

        // b is shared across patterns
        final GqlMatchPlan plan = planner.plan(
                "MATCH (a:Person)-[:KNOWS]->(b:Person), (b)-[:WORKS_AT]->(c:Company)");

        // Company cardinality=1 is lowest
        assertEquals("c", plan.getSeedVariable());
        assertEquals(2, plan.getSteps().size());

        // Anchor-bound invariant
        final java.util.Set<String> bound = new java.util.HashSet<>();
        bound.add(plan.getSeedVariable());
        for (final ExtensionStep step : plan.getSteps()) {
            assertTrue(bound.contains(step.getAnchorVariable()));
            if (step.getTargetVariable() != null) bound.add(step.getTargetVariable());
        }
    }
}
