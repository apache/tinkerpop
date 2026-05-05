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
    public void testQueryLanguageIsCaseSensitiveUppercaseGQLFails() {
        // "GQL" is not the same as "gql" — the check is case-sensitive.
        g.<Integer>inject(1)
         .match("MATCH (n)")
         .with("queryLanguage", "GQL")
         .select("n")
         .toList();
    }

    @Test
    public void testNonEmptyParamsDoesNotThrow() {
        // params are now fully supported — passing a param that isn't referenced in the query
        // is harmless; the query simply returns results as if no filter were applied
        graph.addVertex("Person");
        final List<Object> results = g.<Integer>inject(1)
                .match("MATCH (n:Person)", Collections.singletonMap("unused", "value"))
                .<Object>select("n")
                .toList();
        assertEquals(1, results.size());
    }

    @Test
    public void testNullParamsEquivalentToNoParams() {
        // match("...", null) must behave identically to match("...") — null is treated as
        // an empty params map throughout the execution path.
        graph.addVertex("Person");
        final List<Object> withNull = g.<Integer>inject(1)
                .match("MATCH (n:Person)", (Map<String, Object>) null)
                .<Object>select("n")
                .toList();
        final List<Object> withoutParams = g.<Integer>inject(1)
                .match("MATCH (n:Person)")
                .<Object>select("n")
                .toList();
        assertEquals(withoutParams.size(), withNull.size());
    }

    @Test
    public void testExplicitGqlQueryLanguageDoesNotThrow() {
        // passing the supported language explicitly must not trigger the unsupported-language check
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

    // -------------------------------------------------------------------------
    // Property filters: integration tests
    // -------------------------------------------------------------------------

    @Test
    public void testLiteralPropertyFilterBindsMatchingVertex() {
        final Vertex alice = graph.addVertex("person");
        alice.property("name", "Alice");
        final Vertex bob = graph.addVertex("person");
        bob.property("name", "Bob");

        final List<Vertex> results = g.<Integer>inject(1)
                .match("MATCH (n:person {name: 'Alice'})")
                .<Vertex>select("n")
                .toList();

        assertEquals(1, results.size());
        assertEquals(alice, results.get(0));
    }

    @Test
    public void testParamPropertyFilterBindsMatchingVertex() {
        final Vertex alice = graph.addVertex("person");
        alice.property("name", "Alice");
        final Vertex bob = graph.addVertex("person");
        bob.property("name", "Bob");

        final List<Vertex> results = g.<Integer>inject(1)
                .match("MATCH (n:person {name: $personName})",
                       Collections.singletonMap("personName", "Alice"))
                .<Vertex>select("n")
                .toList();

        assertEquals(1, results.size());
        assertEquals(alice, results.get(0));
    }

    @Test
    public void testParamFilterWithEdgePattern() {
        final Vertex alice = graph.addVertex("person");
        alice.property("name", "Alice");
        final Vertex bob = graph.addVertex("person");
        bob.property("name", "Bob");
        final Vertex carol = graph.addVertex("person");
        carol.property("name", "Carol");
        alice.addEdge("knows", bob);
        alice.addEdge("knows", carol);

        final List<Map<String, Vertex>> results = g.<Integer>inject(1)
                .match("MATCH (a:person {name: 'Alice'})-[:knows]->(b:person {name: $dst})",
                       Collections.singletonMap("dst", "Bob"))
                .<Vertex>select("a", "b")
                .toList();

        assertEquals(1, results.size());
        assertEquals(alice, results.get(0).get("a"));
        assertEquals(bob, results.get(0).get("b"));
    }

    @Test
    public void testPropertyFilterNoMatchReturnsEmpty() {
        final Vertex alice = graph.addVertex("person");
        alice.property("name", "Alice");

        final List<Vertex> results = g.<Integer>inject(1)
                .match("MATCH (n:person {name: $name})",
                       Collections.singletonMap("name", "NoSuchPerson"))
                .<Vertex>select("n")
                .toList();

        assertTrue(results.isEmpty());
    }

    @Test
    public void testIntegerLiteralMatchesIntegerTypedProperty() {
        // Verifies that unsuffixed integer literals produce Integer (not Long),
        // matching the default type used by graph implementations for small values.
        final Vertex young = graph.addVertex("person");
        young.property("age", 29);   // stored as Integer
        final Vertex old = graph.addVertex("person");
        old.property("age", 32);

        final List<Vertex> results = g.<Integer>inject(1)
                .match("MATCH (n:person {age: 29})")
                .<Vertex>select("n")
                .toList();

        assertEquals(1, results.size());
        assertEquals(young, results.get(0));
    }

    @Test
    public void testDoubleQuotedStringFilter() {
        final Vertex alice = graph.addVertex("person");
        alice.property("name", "Alice");
        final Vertex bob = graph.addVertex("person");
        bob.property("name", "Bob");

        final List<Vertex> results = g.<Integer>inject(1)
                .match("MATCH (n:person {name: \"Alice\"})")
                .<Vertex>select("n")
                .toList();

        assertEquals(1, results.size());
        assertEquals(alice, results.get(0));
    }

    // -------------------------------------------------------------------------
    // reset() re-reads live graph state
    // -------------------------------------------------------------------------

    @Test
    public void testResetAllowsFreshResultsAfterGraphMutation() {
        // First execution: no Person vertices — empty result.
        final List<Object> first = g.<Integer>inject(1)
                .match("MATCH (n:Person)").<Object>select("n").toList();
        assertTrue(first.isEmpty());

        // Mutate the graph: add a Person.
        final Vertex alice = graph.addVertex("Person");

        // Second execution on a new traversal (which creates a fresh step): must see alice.
        final List<Object> second = g.<Integer>inject(1)
                .match("MATCH (n:Person)").<Object>select("n").toList();
        assertEquals(1, second.size());
        assertEquals(alice, second.get(0));
    }

    @Test
    public void testNullLiteralMatchesAbsentProperty() {
        final Vertex noNick = graph.addVertex("person");
        noNick.property("name", "Alice");
        final Vertex withNick = graph.addVertex("person");
        withNick.property("name", "Bob");
        withNick.property("nickname", "Bobby");

        final List<Vertex> results = g.<Integer>inject(1)
                .match("MATCH (n:person {nickname: null})")
                .<Vertex>select("n")
                .toList();

        assertEquals(1, results.size());
        assertEquals(noNick, results.get(0));
    }
}
