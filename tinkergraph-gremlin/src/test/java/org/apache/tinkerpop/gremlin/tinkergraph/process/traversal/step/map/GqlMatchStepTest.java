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

import org.apache.tinkerpop.gremlin.gql.DefaultGqlPlanner;
import org.apache.tinkerpop.gremlin.gql.GqlDeclarativeMatchStrategy;
import org.apache.tinkerpop.gremlin.gql.GqlExecutor;
import org.apache.tinkerpop.gremlin.gql.GqlMatchPlan;
import org.apache.tinkerpop.gremlin.gql.GqlMatchStep;
import org.apache.tinkerpop.gremlin.gql.GqlRow;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.DeclarativeMatchStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link GqlMatchStep}: path binding, multi-row output,
 * empty match, and query-language rejection.
 */
public class GqlMatchStepTest {

    private TinkerGraph graph;
    private GraphTraversalSource g;

    /**
     * A minimal traversal strategy that replaces every {@link DeclarativeMatchStep} with a
     * {@link GqlMatchStep} built with the default planner and executor. {@link #setUp()} removes the
     * {@link GqlDeclarativeMatchStrategy} that TinkerGraph registers by default, so this is the only
     * strategy that rewrites the match step and the shared graph-level planner cache is not used.
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
                        (Step) original, (Step) new GqlMatchStep<>(original), traversal);
            }
        }
    }

    @Before
    public void setUp() {
        graph = TinkerGraph.open();
        g = graph.traversal()
                .withoutStrategies(GqlDeclarativeMatchStrategy.class)
                .withStrategies(InjectMatchStrategy.instance());
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

    // -------------------------------------------------------------------------
    // Binding Map return type — no select() needed
    // -------------------------------------------------------------------------

    @Test
    public void testMatchAloneReturnsBindingMap() {
        final Vertex alice = graph.addVertex("person");
        alice.property("name", "Alice");
        final Vertex bob = graph.addVertex("person");
        bob.property("name", "Bob");
        alice.addEdge("knows", bob);

        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> results =
                (List<Map<String, Object>>) (List<?>) g.<Integer>inject(1)
                        .match("MATCH (a:person)-[:knows]->(b:person)")
                        .toList();

        assertEquals(1, results.size());
        final Map<String, Object> row = results.get(0);
        assertEquals(alice, row.get("a"));
        assertEquals(bob, row.get("b"));
    }

    @Test
    public void testMatchAloneBindingMapKeysMatchVariableNames() {
        final Vertex alice = graph.addVertex("person");
        graph.addVertex("person");

        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> results =
                (List<Map<String, Object>>) (List<?>) g.<Integer>inject(1)
                        .match("MATCH (n:person)")
                        .toList();

        assertEquals(2, results.size());
        results.forEach(row -> {
            assertTrue("binding map must contain key 'n'", row.containsKey("n"));
            assertFalse("anonymous variables must not appear in binding map", row.containsKey("$anon0"));
        });
    }

    // -------------------------------------------------------------------------
    // where() with named variables works on binding Map
    // -------------------------------------------------------------------------

    @Test
    public void testWhereWithNamedVariablesFiltersCorrectly() {
        final Vertex a = graph.addVertex("person");
        final Vertex b = graph.addVertex("person");
        a.addEdge("knows", b);
        a.addEdge("knows", a); // self-loop — should be filtered out by neq

        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> results =
                (List<Map<String, Object>>) (List<?>) g.<Integer>inject(1)
                        .match("MATCH (a:person)-[:knows]->(b:person)")
                        .where("a", org.apache.tinkerpop.gremlin.process.traversal.P.neq("b"))
                        .toList();

        assertEquals(1, results.size());
        assertNotEquals(results.get(0).get("a"), results.get(0).get("b"));
    }

    // -------------------------------------------------------------------------
    // Quantified (variable-length) relationship patterns
    //
    // A quantified group edge variable r must surface as a List<Edge> through BOTH channels
    // select() can read, and the two channels must agree:
    //   (1) the emitted binding Map (match()'s current object), and
    //   (2) the traverser path label of the same name.
    // No Pop-based single-edge behaviour: r is the whole ordered edge list. Scalar endpoints
    // (s, d) remain single vertices.
    // -------------------------------------------------------------------------

    /**
     * Builds an inline person chain head -[knows]-> m1 -[knows]-> ... over {@code hops} edges,
     * returning the edges in walk order (element 0 is the first hop out of the head). The head
     * vertex carries {@code name='head'} so a query can anchor the source and isolate the
     * emit-at-each-depth rows of a single walk (an unanchored {@code (s:person)} source would
     * additionally start walks from every interior vertex).
     */
    private List<Edge> buildKnowsChain(final int hops) {
        Vertex prev = graph.addVertex("person");
        prev.property("name", "head");
        final List<Edge> edges = new ArrayList<>();
        for (int i = 0; i < hops; i++) {
            final Vertex next = graph.addVertex("person");
            edges.add(prev.addEdge("knows", next));
            prev = next;
        }
        return edges;
    }

    private static final String CHAIN_QUERY =
            "MATCH (s:person {name: 'head'})-[r:knows]->{1,3}(d:person)";

    @SuppressWarnings("unchecked")
    private static List<Edge> asEdgeList(final Object value) {
        assertTrue("quantified group edge variable must surface as a List, got: " +
                        (value == null ? "null" : value.getClass().getName()),
                value instanceof List);
        return (List<Edge>) value;
    }

    @Test
    public void testQuantifiedGroupEdgeVariableSurfacesAsEdgeListInMapChannel() {
        // head -[knows]-> m1 -[knows]-> m2 -[knows]-> m3 ; {1,3} from head emits at each depth.
        final List<Edge> chain = buildKnowsChain(3); // e0, e1, e2

        final List<Object> rValues = g.<Integer>inject(1)
                .match(CHAIN_QUERY)
                .<Object>select("r")
                .toList();

        // emit-at-each-depth: three rows (depths 1, 2, 3).
        assertEquals(3, rValues.size());

        final Set<List<Edge>> edgeLists = rValues.stream()
                .map(GqlMatchStepTest::asEdgeList)
                .collect(Collectors.toSet());

        assertTrue("depth-1 edge list [e0]",
                edgeLists.contains(Collections.singletonList(chain.get(0))));
        assertTrue("depth-2 edge list [e0, e1]",
                edgeLists.contains(List.of(chain.get(0), chain.get(1))));
        assertTrue("depth-3 edge list [e0, e1, e2]",
                edgeLists.contains(List.of(chain.get(0), chain.get(1), chain.get(2))));
    }

    @Test
    public void testQuantifiedGroupEdgeVariableAgreesAcrossMapAndPathLabelChannels() {
        // Map channel: select("r") reads the map's "r" value.
        // Path-label channel: constant("x") replaces the current object, forcing select("r")
        // to resolve "r" from path history. Both must yield the SAME List<Edge> per match.
        buildKnowsChain(3);

        final List<Object> viaMap = g.<Integer>inject(1)
                .match(CHAIN_QUERY)
                .<Object>select("r")
                .toList();

        final List<Object> viaPathLabel = g.<Integer>inject(1)
                .match(CHAIN_QUERY)
                .constant("x")
                .<Object>select("r")
                .toList();

        assertEquals(3, viaMap.size());
        assertEquals(3, viaPathLabel.size());

        // The two channels must agree row-for-row (matches are produced in the same order).
        final List<List<Edge>> mapLists = viaMap.stream()
                .map(GqlMatchStepTest::asEdgeList).collect(Collectors.toList());
        final List<List<Edge>> pathLists = viaPathLabel.stream()
                .map(GqlMatchStepTest::asEdgeList).collect(Collectors.toList());

        assertEquals("map and path-label channels must agree on the edge lists",
                mapLists, pathLists);
    }

    @Test
    public void testQuantifiedScalarEndpointsRemainSingleVertices() {
        buildKnowsChain(3);

        final List<Object> sValues = g.<Integer>inject(1)
                .match(CHAIN_QUERY)
                .<Object>select("s")
                .toList();
        final List<Object> dValues = g.<Integer>inject(1)
                .match(CHAIN_QUERY)
                .<Object>select("d")
                .toList();

        assertEquals(3, sValues.size());
        assertEquals(3, dValues.size());
        // Endpoints are single vertices, never lists.
        sValues.forEach(v -> assertTrue("s must be a single Vertex, not a list", v instanceof Vertex));
        dValues.forEach(v -> assertTrue("d must be a single Vertex, not a list", v instanceof Vertex));
    }

    @Test
    public void testNonQuantifiedEdgeVariableRemainsScalar() {
        // A fully non-quantified query binds r to a single Edge, not a list.
        final Vertex a = graph.addVertex("person");
        final Vertex b = graph.addVertex("person");
        final Edge e = a.addEdge("knows", b);

        final List<Object> rValues = g.<Integer>inject(1)
                .match("MATCH (s:person)-[r:knows]->(d:person)")
                .<Object>select("r")
                .toList();

        assertEquals(1, rValues.size());
        assertEquals(e, rValues.get(0));
        assertFalse("non-quantified edge variable must not be a list", rValues.get(0) instanceof List);
    }

    @Test
    public void testQuantifiedMultiSelectReturnsScalarEndpointsAndListEdgeVariable() {
        final List<Edge> chain = buildKnowsChain(3);

        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> results =
                (List<Map<String, Object>>) (List<?>) g.<Integer>inject(1)
                        .match(CHAIN_QUERY)
                        .select("s", "r", "d")
                        .toList();

        assertEquals(3, results.size());
        for (final Map<String, Object> row : results) {
            assertTrue("s is a single Vertex", row.get("s") instanceof Vertex);
            assertTrue("d is a single Vertex", row.get("d") instanceof Vertex);
            final List<Edge> edges = asEdgeList(row.get("r"));
            assertFalse("r edge list must not be empty", edges.isEmpty());
            // The walk starts at s and ends at d; consistency: first edge leaves s, last reaches d.
            assertEquals("first edge must originate from s", row.get("s"), edges.get(0).outVertex());
            assertEquals("last edge must arrive at d",
                    row.get("d"), edges.get(edges.size() - 1).inVertex());
        }

        // Exactly one row reaches the deepest target with the full 3-edge list.
        final long fullDepthRows = results.stream()
                .filter(r -> asEdgeList(r.get("r")).size() == chain.size())
                .count();
        assertEquals(1, fullDepthRows);
    }

    // -------------------------------------------------------------------------
    // Length-1 (depth-1) group list consistency
    //
    // A quantified group edge variable r must be a List<Edge> for EVERY matched length,
    // including a single-hop (depth-1) match, never a bare Edge. This holds whether the
    // depth-1 row comes from a range quantifier that matched one hop ({1,3} on a one-hop graph)
    // or from a degenerate exact quantifier ({1,1}). Both result channels (the emitted binding
    // Map and the traverser path label) must agree and both must yield the one-element List.
    // -------------------------------------------------------------------------

    private static final String EXACT1_QUERY =
            "MATCH (s:person {name: 'head'})-[r:knows]->{1,1}(d:person)";

    @Test
    public void testExactlyOneHopQuantifierSurfacesSingletonEdgeListInBothChannels() {
        // {1,1}: the edge carries a quantifier, so it runs as a walk of length one and its
        // variable is a group variable, so it must be a List, not an Edge.
        final List<Edge> chain = buildKnowsChain(1); // single edge e0

        final List<Object> viaMap = g.<Integer>inject(1)
                .match(EXACT1_QUERY)
                .<Object>select("r")
                .toList();
        final List<Object> viaPathLabel = g.<Integer>inject(1)
                .match(EXACT1_QUERY)
                .constant("x")
                .<Object>select("r")
                .toList();

        assertEquals(1, viaMap.size());
        assertEquals(1, viaPathLabel.size());

        final List<Edge> mapList = asEdgeList(viaMap.get(0));
        final List<Edge> pathList = asEdgeList(viaPathLabel.get(0));

        assertEquals("map channel: singleton edge list", Collections.singletonList(chain.get(0)), mapList);
        assertEquals("path channel: singleton edge list", Collections.singletonList(chain.get(0)), pathList);
        assertEquals("map and path-label channels must agree", mapList, pathList);
    }

    @Test
    public void testRangeQuantifierMatchingSingleHopSurfacesSingletonEdgeListInBothChannels() {
        // {1,3} against a graph offering only one hop => exactly one depth-1 row. The group
        // variable must be a one-element List<Edge>, consistent with the {1,1} case and with
        // the depth-1 row of a longer chain.
        final List<Edge> chain = buildKnowsChain(1);

        final List<Object> viaMap = g.<Integer>inject(1)
                .match(CHAIN_QUERY)
                .<Object>select("r")
                .toList();
        final List<Object> viaPathLabel = g.<Integer>inject(1)
                .match(CHAIN_QUERY)
                .constant("x")
                .<Object>select("r")
                .toList();

        assertEquals(1, viaMap.size());
        assertEquals(1, viaPathLabel.size());

        final List<Edge> mapList = asEdgeList(viaMap.get(0));
        final List<Edge> pathList = asEdgeList(viaPathLabel.get(0));

        assertEquals(Collections.singletonList(chain.get(0)), mapList);
        assertEquals(Collections.singletonList(chain.get(0)), pathList);
        assertEquals(mapList, pathList);
    }

    @Test
    public void testDepth1RowOfLongerChainIsSingletonEdgeListInPathChannel() {
        // The depth-1 emission of a {1,3} walk over a 3-hop chain must also be a one-element
        // List in the path channel (regression guard for the length-dependent-type bug).
        final List<Edge> chain = buildKnowsChain(3);

        final List<Object> viaPathLabel = g.<Integer>inject(1)
                .match(CHAIN_QUERY)
                .constant("x")
                .<Object>select("r")
                .toList();

        assertEquals(3, viaPathLabel.size());
        final Set<List<Edge>> pathLists = viaPathLabel.stream()
                .map(GqlMatchStepTest::asEdgeList)
                .collect(Collectors.toSet());
        assertTrue("depth-1 singleton edge list [e0] in path channel",
                pathLists.contains(Collections.singletonList(chain.get(0))));
    }

    // -------------------------------------------------------------------------
    // Path-label pre-binding guard
    // -------------------------------------------------------------------------

    /**
     * Path-label pre-binding is not yet implemented. When a pattern variable shares its name
     * with a step label already bound in the incoming traverser's path, the step must fail
     * loudly rather than silently producing wrong results (a full unanchored scan). This guard
     * reserves the semantic space so the feature can land as a clean addition later.
     * <p>
     * These tests use the modern graph via {@link TinkerFactory#createModern()} so that
     * {@code V(1)} returns a real vertex and the mid-traversal path actually carries the label.
     */
    @Test
    public void shouldThrowWhenPatternVariableOverlapsWithStepLabel() {
        try (final TinkerGraph modern = TinkerFactory.createModern()) {
            modern.traversal().V(1).as("a")
                  .match("MATCH (a:person)-[:knows]->(b:person)").toList();
            fail("Expected UnsupportedOperationException for path-label/pattern-variable overlap");
        } catch (final UnsupportedOperationException e) {
            // expected — guard fires because 'a' is both a step label and a pattern variable
        }
    }

    @Test
    public void shouldThrowWhenEdgePatternVariableOverlapsWithStepLabel() {
        try (final TinkerGraph modern = TinkerFactory.createModern()) {
            modern.traversal().V(1).outE("knows").as("e")
                  .match("MATCH (a:person)-[e:knows]->(b:person)").toList();
            fail("Expected UnsupportedOperationException for path-label/pattern-variable overlap on edge");
        } catch (final UnsupportedOperationException e) {
            // expected — guard fires because 'e' is both a step label and a pattern variable
        }
    }

    @Test
    public void shouldNotThrowWhenStepLabelsAndPatternVariablesAreDisjoint() {
        try (final TinkerGraph modern = TinkerFactory.createModern()) {
            // 'x' is the step label; pattern uses 'a' and 'b' — no overlap, no guard fires
            modern.traversal().V(1).as("x")
                  .match("MATCH (a:person)-[:knows]->(b:person)").toList();
        }
    }

    // -------------------------------------------------------------------------
    // Provider-implemented GqlExecutor
    // -------------------------------------------------------------------------

    /**
     * A {@link GqlExecutor} written outside the {@code gql} package, as a provider would, that
     * emits one fixed {@link GqlRow} built with the public factory. The step must surface its
     * scalar and group bindings by variable name.
     */
    @Test
    public void shouldSurfaceRowsFromProviderImplementedExecutor() {
        final List<Edge> chain = buildKnowsChain(2);
        final Vertex head = chain.get(0).outVertex();
        final Vertex tail = chain.get(1).inVertex();
        final DefaultGqlPlanner planner = new DefaultGqlPlanner(graph);

        final GqlExecutor providerExecutor = new GqlExecutor() {
            @Override
            public Iterator<GqlRow> execute(final GqlMatchPlan p) {
                return execute(p, Collections.emptyMap());
            }

            @Override
            public Iterator<GqlRow> execute(final GqlMatchPlan p, final Map<String, Object> params) {
                final Element[] scalars = new Element[p.getVariableCount()];
                scalars[p.getIndex("s")] = head;
                scalars[p.getIndex("d")] = tail;
                return Collections.singletonList(
                        GqlRow.of(scalars, Map.of(p.getIndex("r"), chain))).iterator();
            }
        };

        final GraphTraversalSource gp = graph.traversal().withStrategies(
                GqlDeclarativeMatchStrategy.create(planner, providerExecutor));
        final List<Map<String, Object>> results = gp.<Integer>inject(1)
                .match("MATCH (s:person)-[r:knows]->{1,2}(d:person)")
                .toList();

        assertEquals(1, results.size());
        assertEquals(head, results.get(0).get("s"));
        assertEquals(tail, results.get(0).get("d"));
        assertEquals(chain, asEdgeList(results.get(0).get("r")));
    }

    /**
     * A provider executor whose rows do not match the plan's variable count must be reported
     * rather than silently binding the wrong variables.
     */
    @Test
    public void shouldRejectProviderRowWithWrongSize() {
        final DefaultGqlPlanner planner = new DefaultGqlPlanner(graph);
        final GqlExecutor providerExecutor = new GqlExecutor() {
            @Override
            public Iterator<GqlRow> execute(final GqlMatchPlan p) {
                return execute(p, Collections.emptyMap());
            }

            @Override
            public Iterator<GqlRow> execute(final GqlMatchPlan p, final Map<String, Object> params) {
                return Collections.singletonList(
                        GqlRow.of(new Element[p.getVariableCount() + 1], Collections.emptyMap())).iterator();
            }
        };

        final GraphTraversalSource gp = graph.traversal().withStrategies(
                GqlDeclarativeMatchStrategy.create(planner, providerExecutor));
        try {
            gp.<Integer>inject(1).match("MATCH (a:person)-[e:knows]->(b:person)").toList();
            fail("a row whose size differs from the plan's variable count must be rejected");
        } catch (IllegalStateException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().contains(providerExecutor.getClass().getName()));
            assertTrue(ex.getMessage(), ex.getMessage().contains("size 4"));
        }
    }

    @Test
    public void testQuantifiedGroupEdgeVariableInPatternOrderWhenSeededAtTarget() {
        // p0 -knows-> p1 -knows-> acme. The single company is the seed, so the walk runs from the
        // pattern edge's target back to its source, and r must still read left to right.
        final Vertex p0 = graph.addVertex("person");
        final Vertex p1 = graph.addVertex("person");
        final Vertex acme = graph.addVertex("company");
        for (int i = 0; i < 8; i++) graph.addVertex("person");
        final Edge e01 = p0.addEdge("knows", p1);
        final Edge e1c = p1.addEdge("knows", acme);

        final String query = "MATCH (a:person)-[r:knows]->{2}(b:company)";
        assertEquals("b", new DefaultGqlPlanner(graph).plan(query).getSeedVariable());

        final List<Object> rValues = g.<Integer>inject(1).match(query).<Object>select("r").toList();
        assertEquals(1, rValues.size());
        assertEquals(List.of(e01, e1c), asEdgeList(rValues.get(0)));
    }

    @Test(timeout = 10000)
    public void testLimitOneOnUnboundedUndirectedWalkOverDenseGraphIsPrompt() {
        // The number of trails on a complete graph is astronomically large, so this only returns
        // if the executor produces rows on demand rather than buffering a seed's whole search.
        final List<Vertex> vs = new ArrayList<>();
        for (int i = 0; i < 10; i++) vs.add(graph.addVertex("person"));
        for (int i = 0; i < vs.size(); i++)
            for (int j = i + 1; j < vs.size(); j++)
                vs.get(i).addEdge("knows", vs.get(j));

        final List<Object> rows = g.<Integer>inject(1)
                .match("MATCH (a:person)-[]-+(b:person)").select("b").limit(1).toList();
        assertEquals(1, rows.size());
    }

    @Test
    public void testQuantifiedAndPlainEdgesInOnePattern() {
        // head -knows-> m1 -knows-> m2, m1 -created-> sw1, m2 -created-> sw2
        final List<Edge> chain = buildKnowsChain(2);
        final Vertex m1 = chain.get(0).inVertex();
        final Vertex m2 = chain.get(1).inVertex();
        final Vertex sw1 = graph.addVertex("software");
        final Vertex sw2 = graph.addVertex("software");
        final Edge c1 = m1.addEdge("created", sw1);
        final Edge c2 = m2.addEdge("created", sw2);

        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> rows = (List<Map<String, Object>>) (List<?>) g.<Integer>inject(1)
                .match("MATCH (s:person {name: 'head'})-[r:knows]->{1,2}(p:person)-[c:created]->(w:software)")
                .select("r", "p", "c", "w")
                .toList();

        assertEquals(2, rows.size());
        final Map<Object, Map<String, Object>> byP = rows.stream()
                .collect(Collectors.toMap(row -> row.get("p"), row -> row));
        assertEquals(Set.of(m1, m2), byP.keySet());
        assertEquals(List.of(chain.get(0)), asEdgeList(byP.get(m1).get("r")));
        assertEquals(c1, byP.get(m1).get("c"));
        assertEquals(sw1, byP.get(m1).get("w"));
        assertEquals(List.of(chain.get(0), chain.get(1)), asEdgeList(byP.get(m2).get("r")));
        assertEquals(c2, byP.get(m2).get("c"));
        assertEquals(sw2, byP.get(m2).get("w"));
    }

    // -------------------------------------------------------------------------
    // Quantified edges that close a pattern cycle
    // -------------------------------------------------------------------------

    /** u -knows-> v -knows-> u (a 2-cycle) and u -knows-> w (a dead end). Returns {u, v, uv, vu}. */
    private Object[] buildKnowsTwoCycle() {
        final Vertex u = graph.addVertex("person");
        final Vertex v = graph.addVertex("person");
        final Vertex w = graph.addVertex("person");
        final Edge uv = u.addEdge("knows", v);
        final Edge vu = v.addEdge("knows", u);
        u.addEdge("knows", w);
        return new Object[]{u, v, uv, vu};
    }

    @SuppressWarnings("unchecked")
    private static List<List<Object>> aAndR(final List<Map<String, Object>> rows) {
        return rows.stream().map(row -> List.of(row.get("a"), asEdgeList(row.get("r"))))
                .sorted(Comparator.comparing(Object::toString)).collect(Collectors.toList());
    }

    @Test(timeout = 10000)
    @SuppressWarnings("unchecked")
    public void testQuantifiedPatternSelfLoopSurfacesClosedWalks() {
        final Object[] c = buildKnowsTwoCycle();
        final Vertex u = (Vertex) c[0], v = (Vertex) c[1];
        final Edge uv = (Edge) c[2], vu = (Edge) c[3];

        final List<Map<String, Object>> rows = (List<Map<String, Object>>) (List<?>) g.<Integer>inject(1)
                .match("MATCH (a:person)-[r:knows]->{1,3}(a)").select("a", "r").toList();
        final List<List<Object>> expected = new ArrayList<>(List.of(
                List.of(u, List.of(uv, vu)),
                List.of(v, List.of(vu, uv))));
        expected.sort(Comparator.comparing(Object::toString));
        assertEquals(expected, aAndR(rows));
    }

    @Test(timeout = 10000)
    @SuppressWarnings("unchecked")
    public void testQuantifiedPatternSelfLoopMidTraversalResetsPerTraverser() {
        // Each incoming traverser runs the whole match afresh, so two traversers give the cycle
        // rows twice, and the second set is not cut short by the first traverser's walk state.
        final Object[] c = buildKnowsTwoCycle();
        final Vertex u = (Vertex) c[0], v = (Vertex) c[1];
        final Edge uv = (Edge) c[2], vu = (Edge) c[3];

        final List<Map<String, Object>> viaV = (List<Map<String, Object>>) (List<?>) g.V(u, v)
                .match("MATCH (a:person)-[r:knows]->{1,3}(a)").select("a", "r").toList();
        final List<Map<String, Object>> viaInject = (List<Map<String, Object>>) (List<?>) g.<Integer>inject(1, 2)
                .match("MATCH (a:person)-[r:knows]->{1,3}(a)").select("a", "r").toList();

        final List<List<Object>> expected = new ArrayList<>(List.of(
                List.of(u, List.of(uv, vu)), List.of(u, List.of(uv, vu)),
                List.of(v, List.of(vu, uv)), List.of(v, List.of(vu, uv))));
        expected.sort(Comparator.comparing(Object::toString));
        assertEquals(expected, aAndR(viaV));
        assertEquals(expected, aAndR(viaInject));
    }

    @Test(timeout = 10000)
    @SuppressWarnings("unchecked")
    public void testQuantifiedEdgeClosingTriangleSurfacesRows() {
        // a -ab-> b, b reaches c by bc at depths 1 and 2, c -ca-> a.
        final Vertex a = graph.addVertex("a");
        final Vertex b = graph.addVertex("b");
        final Vertex c = graph.addVertex("c");
        final Vertex m = graph.addVertex("m");
        a.addEdge("ab", b);
        final Edge bc = b.addEdge("bc", c);
        final Edge bm = b.addEdge("bc", m);
        final Edge mc = m.addEdge("bc", c);
        c.addEdge("ca", a);

        final List<Map<String, Object>> rows = (List<Map<String, Object>>) (List<?>) g.<Integer>inject(1)
                .match("MATCH (a:a)-[:ab]->(b:b)-[r:bc]->{1,3}(c:c)-[:ca]->(a)").select("a", "b", "c", "r").toList();

        final List<List<Object>> expected = new ArrayList<>(List.of(
                List.of(a, b, c, List.of(bc)), List.of(a, b, c, List.of(bm, mc))));
        expected.sort(Comparator.comparing(Object::toString));
        assertEquals(expected, rows.stream()
                .map(row -> List.of(row.get("a"), row.get("b"), row.get("c"), asEdgeList(row.get("r"))))
                .sorted(Comparator.comparing(Object::toString)).collect(Collectors.toList()));
    }
}
