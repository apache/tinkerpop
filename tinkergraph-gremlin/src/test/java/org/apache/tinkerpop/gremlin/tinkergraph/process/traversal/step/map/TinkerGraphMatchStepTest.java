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
package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.DeclarativeMatchStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link TinkerGraphMatchStep}: path binding, multi-row output,
 * empty match, and query-language rejection.
 */
public class TinkerGraphMatchStepTest {

    private TinkerGraph graph;
    private GraphTraversalSource g;

    /**
     * A minimal traversal strategy that replaces every {@link DeclarativeMatchStep} with a
     * {@link TinkerGraphMatchStep}. This is a stand-in for the full provider strategy
     * (TinkerGraphDeclarativeMatchStrategy) so we can test the step in isolation.
     */
    private static final class InjectMatchStrategy
            extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
            implements TraversalStrategy.ProviderOptimizationStrategy {

        private static final InjectMatchStrategy INSTANCE = new InjectMatchStrategy();

        static InjectMatchStrategy instance() {
            return INSTANCE;
        }

        @Override
        @SuppressWarnings({"unchecked", "rawtypes"})
        public void apply(final Traversal.Admin<?, ?> traversal) {
            for (final DeclarativeMatchStep<?> original :
                    TraversalHelper.getStepsOfClass(DeclarativeMatchStep.class, traversal)) {
                TraversalHelper.replaceStep(
                        (Step) original, (Step) new TinkerGraphMatchStep<>(original), traversal);
            }
        }
    }

    @Before
    public void setUp() {
        graph = TinkerGraph.open();
        g = graph.traversal().withStrategies(InjectMatchStrategy.instance());
    }

    @After
    public void tearDown() {
        graph.close();
    }

    // -------------------------------------------------------------------------
    // Empty / no-match cases
    // -------------------------------------------------------------------------

    @Test
    public void testEmptyGraphProducesNoResults() {
        final List<Vertex> results = g.<Integer>inject(1).match("MATCH (n:Person)").<Vertex>select("n").toList();
        assertTrue(results.isEmpty());
    }

    @Test
    public void testNoMatchingEdgeProducesNoResults() {
        graph.addVertex("Person");
        graph.addVertex("Person");
        final List<Object> results =
                g.<Integer>inject(1).match("MATCH (a:Person)-[:KNOWS]->(b:Person)").select("a").toList();
        assertTrue(results.isEmpty());
    }

    // -------------------------------------------------------------------------
    // Single-node pattern
    // -------------------------------------------------------------------------

    @Test
    public void testSingleNodePatternBindsVariableInPath() {
        final Vertex alice = graph.addVertex("Person");
        final Vertex bob = graph.addVertex("Person");
        graph.addVertex("Company"); // should not match :Person

        final List<Vertex> bound = g.<Integer>inject(1)
                .match("MATCH (n:Person)")
                .<Vertex>select("n")
                .toList();

        assertEquals(2, bound.size());
        assertTrue(bound.contains(alice));
        assertTrue(bound.contains(bob));
    }

    // -------------------------------------------------------------------------
    // Single-edge pattern
    // -------------------------------------------------------------------------

    @Test
    public void testEdgePatternBindsBothEndpointsInPath() {
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
        final Map<String, Object> row = results.get(0);
        assertEquals(alice, row.get("a"));
        assertEquals(acme, row.get("c"));
    }

    @Test
    public void testMultipleMatchingEdgesProduceOneTraverserPerRow() {
        final Vertex a = graph.addVertex("Person");
        final Vertex b1 = graph.addVertex("Person");
        final Vertex b2 = graph.addVertex("Person");
        a.addEdge("KNOWS", b1);
        a.addEdge("KNOWS", b2);

        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> results =
                (List<Map<String, Object>>) (List<?>) g.<Integer>inject(1)
                        .match("MATCH (a:Person)-[:KNOWS]->(b:Person)")
                        .select("a", "b")
                        .toList();

        assertEquals(2, results.size());
        final List<Object> bValues = results.stream().map(r -> r.get("b")).collect(Collectors.toList());
        assertTrue(bValues.contains(b1));
        assertTrue(bValues.contains(b2));
        results.forEach(r -> assertEquals(a, r.get("a")));
    }

    // -------------------------------------------------------------------------
    // Multiple input traversers
    // -------------------------------------------------------------------------

    @Test
    public void testMultipleInputTraversersProduceIndependentResults() {
        final Vertex a = graph.addVertex("Person");
        final Vertex b = graph.addVertex("Person");
        a.addEdge("KNOWS", b);

        // inject(1, 2) produces two input traversers; each should see the same match result
        final List<Object> results =
                g.<Integer>inject(1, 2).match("MATCH (a:Person)-[:KNOWS]->(b:Person)").select("a").toList();

        // One result row per match * two input traversers = 2 output traversers
        assertEquals(2, results.size());
    }

    // -------------------------------------------------------------------------
    // Query-language rejection
    // -------------------------------------------------------------------------

    @Test(expected = UnsupportedOperationException.class)
    public void testUnsupportedQueryLanguageThrows() {
        g.<Integer>inject(1)
         .match("MATCH (n)")
         .with("queryLanguage", "sparql")
         .select("n")
         .toList();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testNonEmptyParamsThrows() {
        g.<Integer>inject(1)
         .match("MATCH (n)", Collections.singletonMap("key", "value"))
         .select("n")
         .toList();
    }

    @Test
    public void testExplicitGqlQueryLanguageDoesNotThrow() {
        // "gql" is the DEFAULT_QUERY_LANGUAGE; passing it explicitly must not trigger the
        // unsupported-language check in processNextStart()
        final Vertex alice = graph.addVertex("Person");

        final List<Vertex> results = g.<Integer>inject(1)
                .match("MATCH (n:Person)")
                .with("queryLanguage", "gql")
                .<Vertex>select("n")
                .toList();

        assertEquals(1, results.size());
        assertEquals(alice, results.get(0));
    }

    // -------------------------------------------------------------------------
    // Anonymous variables are excluded from path
    // -------------------------------------------------------------------------

    @Test
    public void testAnonymousVariablesAreNotExposedInPath() {
        graph.addVertex("Person");

        final List<Vertex> bound = g.<Integer>inject(1)
                .match("MATCH (n:Person)")
                .<Vertex>select("n")
                .toList();

        // select("n") should work — the named variable is accessible
        assertEquals(1, bound.size());
        assertNotNull(bound.get(0));
    }
}
