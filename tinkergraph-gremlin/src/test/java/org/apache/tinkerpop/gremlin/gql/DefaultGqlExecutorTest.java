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
package org.apache.tinkerpop.gremlin.gql;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.AbstractTinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link DefaultGqlExecutor}: DFS backtracking pattern matching,
 * label constraints, equality constraints for shared variables, edge variable binding,
 * self-loops, and multi-hop paths.
 */
public class DefaultGqlExecutorTest {

    private TinkerGraph graph;
    private DefaultGqlPlanner planner;
    private DefaultGqlExecutor executor;

    @Before
    public void setUp() {
        graph = TinkerGraph.open();
        planner = new DefaultGqlPlanner(graph);
        executor = new DefaultGqlExecutor(graph);
    }

    @After
    public void tearDown() {
        graph.close();
    }

    // -------------------------------------------------------------------------
    // Test helper: materialise lazy iterator into a List<Map<String,Element>>
    // -------------------------------------------------------------------------

    /**
     * A materialised result row. It <em>is</em> a {@code Map<String,Element>} of the row's scalar
     * bindings (variable name → graph element), so all existing scalar assertions
     * ({@code row.get("a")}, {@code row.containsKey(...)}) work unchanged. In addition it carries
     * the quantified group edge bindings (variable name → ordered {@code List<Edge>}) surfaced
     * from {@link GqlRow#getGroup(int)} via the plan's variable indices.
     */
    private static final class Row extends LinkedHashMap<String, Element> {
        final Map<String, List<Edge>> groups = new LinkedHashMap<>();
    }

    /**
     * Drains the lazy result iterator into a list of {@link Row} maps, mirroring the
     * old List<Map> API so that existing assertions can be reused without change.
     * Scalar entries whose variable name starts with {@code $anon} are excluded (anonymous
     * nodes); group edge lists are keyed back to their variable name via the plan index.
     */
    private List<Map<String, Element>> materialize(final Iterator<GqlRow> iter,
                                                    final GqlMatchPlan plan) {
        final String[] variables = plan.getVariables();
        final List<Map<String, Element>> rows = new ArrayList<>();
        while (iter.hasNext()) {
            final GqlRow gqlRow = iter.next();
            final Row map = new Row();
            for (int i = 0; i < variables.length; i++) {
                if (gqlRow.isGroup(i))
                    map.groups.put(variables[i], gqlRow.getGroup(i));
                else if (!variables[i].startsWith("$anon") && gqlRow.getScalar(i) != null)
                    map.put(variables[i], gqlRow.getScalar(i));
            }
            rows.add(map);
        }
        return rows;
    }

    private List<Map<String, Element>> execute(final String query) {
        return execute(query, Collections.emptyMap());
    }

    private List<Map<String, Element>> execute(final String query, final Map<String, Object> params) {
        final GqlMatchPlan plan = planner.plan(query);
        return materialize(executor.execute(plan, params), plan);
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
    // Property filters: literal values
    // -------------------------------------------------------------------------

    @Test
    public void testStringLiteralFilterMatchesSingleVertex() {
        final Vertex alice = graph.addVertex("Person");
        alice.property("name", "Alice");
        final Vertex bob = graph.addVertex("Person");
        bob.property("name", "Bob");

        final List<Map<String, Element>> results = execute("MATCH (n:Person {name: 'Alice'})");
        assertEquals(1, results.size());
        assertEquals(alice, results.get(0).get("n"));
    }

    @Test
    public void testStringLiteralFilterNoMatch() {
        final Vertex alice = graph.addVertex("Person");
        alice.property("name", "Alice");
        assertTrue(execute("MATCH (n:Person {name: 'Charlie'})").isEmpty());
    }

    @Test
    public void testIntegerLiteralFilter() {
        final Vertex alice = graph.addVertex("Person");
        alice.property("age", 30L);
        final Vertex bob = graph.addVertex("Person");
        bob.property("age", 25L);

        final List<Map<String, Element>> results = execute("MATCH (n:Person {age: 30})");
        assertEquals(1, results.size());
        assertEquals(alice, results.get(0).get("n"));
    }

    @Test
    public void testFloatLiteralFilter() {
        final Vertex a = graph.addVertex("Item");
        a.property("score", 9.5);
        final Vertex b = graph.addVertex("Item");
        b.property("score", 8.0);

        final List<Map<String, Element>> results = execute("MATCH (n:Item {score: 9.5})");
        assertEquals(1, results.size());
        assertEquals(a, results.get(0).get("n"));
    }

    @Test
    public void testBooleanTrueLiteralFilter() {
        final Vertex active = graph.addVertex("Person");
        active.property("active", true);
        final Vertex inactive = graph.addVertex("Person");
        inactive.property("active", false);

        final List<Map<String, Element>> results = execute("MATCH (n:Person {active: true})");
        assertEquals(1, results.size());
        assertEquals(active, results.get(0).get("n"));
    }

    @Test
    public void testBooleanFalseLiteralFilter() {
        final Vertex active = graph.addVertex("Person");
        active.property("active", true);
        final Vertex inactive = graph.addVertex("Person");
        inactive.property("active", false);

        final List<Map<String, Element>> results = execute("MATCH (n:Person {active: false})");
        assertEquals(1, results.size());
        assertEquals(inactive, results.get(0).get("n"));
    }

    @Test
    public void testMultiPropertyLiteralFilter() {
        final Vertex alice30 = graph.addVertex("Person");
        alice30.property("name", "Alice");
        alice30.property("age", 30L);
        final Vertex alice25 = graph.addVertex("Person");
        alice25.property("name", "Alice");
        alice25.property("age", 25L);

        final List<Map<String, Element>> results = execute("MATCH (n:Person {name: 'Alice', age: 30})");
        assertEquals(1, results.size());
        assertEquals(alice30, results.get(0).get("n"));
    }

    @Test
    public void testPropertyFilterMissingPropertyNoMatch() {
        // Vertex has no 'name' property — predicate must not match
        graph.addVertex("Person");
        assertTrue(execute("MATCH (n:Person {name: 'Alice'})").isEmpty());
    }

    // -------------------------------------------------------------------------
    // Property filters: parameter references
    // -------------------------------------------------------------------------

    @Test
    public void testStringParamFilter() {
        final Vertex alice = graph.addVertex("Person");
        alice.property("name", "Alice");
        final Vertex bob = graph.addVertex("Person");
        bob.property("name", "Bob");

        final Map<String, Object> params = Collections.singletonMap("n", "Alice");
        final List<Map<String, Element>> results = execute("MATCH (p:Person {name: $n})", params);
        assertEquals(1, results.size());
        assertEquals(alice, results.get(0).get("p"));
    }

    @Test
    public void testParamFilterNoMatch() {
        final Vertex alice = graph.addVertex("Person");
        alice.property("name", "Alice");

        final Map<String, Object> params = Collections.singletonMap("n", "Charlie");
        assertTrue(execute("MATCH (p:Person {name: $n})", params).isEmpty());
    }

    @Test
    public void testMissingParamYieldsNoMatch() {
        // $n not in params map — resolves to null, property is non-null → no match
        final Vertex alice = graph.addVertex("Person");
        alice.property("name", "Alice");
        assertTrue(execute("MATCH (p:Person {name: $n})", Collections.emptyMap()).isEmpty());
    }

    @Test
    public void testMultiParamFilter() {
        final Vertex alice30 = graph.addVertex("Person");
        alice30.property("name", "Alice");
        alice30.property("age", 30L);
        final Vertex alice25 = graph.addVertex("Person");
        alice25.property("name", "Alice");
        alice25.property("age", 25L);

        final Map<String, Object> params = new java.util.HashMap<>();
        params.put("name", "Alice");
        params.put("age", 30L);
        final List<Map<String, Element>> results =
                execute("MATCH (n:Person {name: $name, age: $age})", params);
        assertEquals(1, results.size());
        assertEquals(alice30, results.get(0).get("n"));
    }

    // -------------------------------------------------------------------------
    // Property filters: on target vertex in edge patterns
    // -------------------------------------------------------------------------

    @Test
    public void testPropertyFilterOnTargetVertex() {
        final Vertex alice = graph.addVertex("Person");
        alice.property("name", "Alice");
        final Vertex bob = graph.addVertex("Person");
        bob.property("name", "Bob");
        final Vertex carol = graph.addVertex("Person");
        carol.property("name", "Carol");
        alice.addEdge("KNOWS", bob);
        alice.addEdge("KNOWS", carol);

        final Map<String, Object> params = Collections.singletonMap("name", "Bob");
        final List<Map<String, Element>> results =
                execute("MATCH (a:Person)-[:KNOWS]->(b:Person {name: $name})", params);
        assertEquals(1, results.size());
        assertEquals(alice, results.get(0).get("a"));
        assertEquals(bob, results.get(0).get("b"));
    }

    // -------------------------------------------------------------------------
    // Property filters: vertex index integration
    // -------------------------------------------------------------------------

    @Test
    public void testIndexedLiteralFilterUsesIndex() {
        graph.createIndex("name", org.apache.tinkerpop.gremlin.structure.Vertex.class);
        final Vertex alice = graph.addVertex("Person");
        alice.property("name", "Alice");
        final Vertex bob = graph.addVertex("Person");
        bob.property("name", "Bob");

        final List<Map<String, Element>> results = execute("MATCH (n:Person {name: 'Alice'})");
        assertEquals(1, results.size());
        assertEquals(alice, results.get(0).get("n"));
    }

    @Test
    public void testIndexedParamFilterUsesIndex() {
        graph.createIndex("name", org.apache.tinkerpop.gremlin.structure.Vertex.class);
        final Vertex alice = graph.addVertex("Person");
        alice.property("name", "Alice");
        final Vertex bob = graph.addVertex("Person");
        bob.property("name", "Bob");

        final List<Map<String, Element>> results =
                execute("MATCH (n:Person {name: $n})", Collections.singletonMap("n", "Alice"));
        assertEquals(1, results.size());
        assertEquals(alice, results.get(0).get("n"));
    }

    @Test
    public void testIndexedFilterNoMatch() {
        graph.createIndex("name", org.apache.tinkerpop.gremlin.structure.Vertex.class);
        final Vertex alice = graph.addVertex("Person");
        alice.property("name", "Alice");

        assertTrue(execute("MATCH (n:Person {name: 'Nobody'})").isEmpty());
    }

    @Test
    public void testIndexedFilterMissingParamFallsBackToScan() {
        // $n not in params — index cannot be used; full scan applies; no vertex matches null
        graph.createIndex("name", org.apache.tinkerpop.gremlin.structure.Vertex.class);
        final Vertex alice = graph.addVertex("Person");
        alice.property("name", "Alice");

        assertTrue(execute("MATCH (n:Person {name: $n})", Collections.emptyMap()).isEmpty());
    }

    @Test
    public void testMostSelectiveIndexPredicateChosen() {
        // Two indexed keys: 'name' is more selective (1 match) than 'role' (4 matches).
        // The executor should use 'name' for the index lookup and return exactly 1 result.
        graph.createIndex("name", org.apache.tinkerpop.gremlin.structure.Vertex.class);
        graph.createIndex("role", org.apache.tinkerpop.gremlin.structure.Vertex.class);

        for (int i = 0; i < 3; i++) {
            final Vertex v = graph.addVertex("Person");
            v.property("name", "Person" + i);
            v.property("role", "employee");
        }
        final Vertex target = graph.addVertex("Person");
        target.property("name", "Alice");
        target.property("role", "employee");

        final Map<String, Object> params = new java.util.HashMap<>();
        params.put("name", "Alice");
        params.put("role", "employee");
        final List<Map<String, Element>> results =
                execute("MATCH (n:Person {name: $name, role: $role})", params);
        assertEquals(1, results.size());
        assertEquals(target, results.get(0).get("n"));
    }

    @Test
    public void testIndexedSeedWithEdgePattern() {
        graph.createIndex("name", org.apache.tinkerpop.gremlin.structure.Vertex.class);
        final Vertex alice = graph.addVertex("Person");
        alice.property("name", "Alice");
        final Vertex bob = graph.addVertex("Person");
        bob.property("name", "Bob");
        final Vertex carol = graph.addVertex("Person");
        carol.property("name", "Carol");
        alice.addEdge("KNOWS", bob);
        alice.addEdge("KNOWS", carol);

        final List<Map<String, Element>> results =
                execute("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person)");
        assertEquals(2, results.size());
        assertTrue(results.stream().allMatch(r -> alice.equals(r.get("a"))));
    }

    @Test
    public void testPropertyFilterOnSeedAndTarget() {
        final Vertex alice = graph.addVertex("Person");
        alice.property("name", "Alice");
        final Vertex bob = graph.addVertex("Person");
        bob.property("name", "Bob");
        final Vertex carol = graph.addVertex("Person");
        carol.property("name", "Carol");
        alice.addEdge("KNOWS", bob);
        carol.addEdge("KNOWS", bob);

        final Map<String, Object> params = new java.util.HashMap<>();
        params.put("src", "Alice");
        params.put("dst", "Bob");
        final List<Map<String, Element>> results =
                execute("MATCH (a:Person {name: $src})-[:KNOWS]->(b:Person {name: $dst})", params);
        assertEquals(1, results.size());
        assertEquals(alice, results.get(0).get("a"));
        assertEquals(bob, results.get(0).get("b"));
    }

    // -------------------------------------------------------------------------
    // Gremlin-compatible literal types
    // -------------------------------------------------------------------------

    @Test
    public void testIntegerLiteralDefaultsToInteger() {
        // Unsuffixed integer literal must produce Integer (not Long) to match
        // the type stored by graph implementations that use Integer for counts/ages.
        final Vertex v = graph.addVertex("person");
        v.property("age", 29);  // stored as Integer
        final Vertex other = graph.addVertex("person");
        other.property("age", 30);

        final List<Map<String, Element>> results = execute("MATCH (n:person {age: 29})");
        assertEquals(1, results.size());
        assertEquals(v, results.get(0).get("n"));
    }

    @Test
    public void testIntegerSuffixI() {
        final Vertex v = graph.addVertex("item");
        v.property("count", 5);  // Integer
        final List<Map<String, Element>> results = execute("MATCH (n:item {count: 5i})");
        assertEquals(1, results.size());
        assertEquals(v, results.get(0).get("n"));
    }

    @Test
    public void testIntegerSuffixL() {
        final Vertex v = graph.addVertex("item");
        v.property("count", 999999999999L);  // Long
        final List<Map<String, Element>> results = execute("MATCH (n:item {count: 999999999999l})");
        assertEquals(1, results.size());
        assertEquals(v, results.get(0).get("n"));
    }

    @Test
    public void testNegativeIntegerLiteral() {
        final Vertex v = graph.addVertex("account");
        v.property("balance", -100);  // stored as Integer
        final Vertex other = graph.addVertex("account");
        other.property("balance", 50);

        final List<Map<String, Element>> results = execute("MATCH (n:account {balance: -100})");
        assertEquals(1, results.size());
        assertEquals(v, results.get(0).get("n"));
    }

    @Test
    public void testFloatSuffixF() {
        final Vertex v = graph.addVertex("item");
        v.property("weight", 3.14f);  // Float
        final List<Map<String, Element>> results = execute("MATCH (n:item {weight: 3.14f})");
        assertEquals(1, results.size());
        assertEquals(v, results.get(0).get("n"));
    }

    @Test
    public void testByteSuffixFilter() {
        final Vertex v = graph.addVertex("item");
        v.property("small", (byte) 5);
        final Vertex other = graph.addVertex("item");
        other.property("small", (byte) 10);

        final List<Map<String, Element>> results = execute("MATCH (n:item {small: 5b})");
        assertEquals(1, results.size());
        assertEquals(v, results.get(0).get("n"));
    }

    @Test
    public void testShortSuffixFilter() {
        final Vertex v = graph.addVertex("item");
        v.property("medium", (short) 1000);
        final Vertex other = graph.addVertex("item");
        other.property("medium", (short) 2000);

        final List<Map<String, Element>> results = execute("MATCH (n:item {medium: 1000s})");
        assertEquals(1, results.size());
        assertEquals(v, results.get(0).get("n"));
    }

    @Test
    public void testBigIntegerSuffixFilter() {
        final Vertex v = graph.addVertex("item");
        v.property("big", new BigInteger("999999999999"));
        final Vertex other = graph.addVertex("item");
        other.property("big", new BigInteger("1"));

        final List<Map<String, Element>> results = execute("MATCH (n:item {big: 999999999999n})");
        assertEquals(1, results.size());
        assertEquals(v, results.get(0).get("n"));
    }

    @Test
    public void testBigDecimalSuffixFilter() {
        final Vertex v = graph.addVertex("item");
        v.property("price", new BigDecimal("1.99"));
        final Vertex other = graph.addVertex("item");
        other.property("price", new BigDecimal("9.99"));

        final List<Map<String, Element>> results = execute("MATCH (n:item {price: 1.99m})");
        assertEquals(1, results.size());
        assertEquals(v, results.get(0).get("n"));
    }

    @Test
    public void testDoubleSuffixFilter() {
        final Vertex v = graph.addVertex("item");
        v.property("score", 9.5);
        final Vertex other = graph.addVertex("item");
        other.property("score", 8.0);

        final List<Map<String, Element>> results = execute("MATCH (n:item {score: 9.5d})");
        assertEquals(1, results.size());
        assertEquals(v, results.get(0).get("n"));
    }

    @Test
    public void testNegativeFloatLiteral() {
        final Vertex v = graph.addVertex("sensor");
        v.property("temperature", -1.5);  // Double
        final Vertex other = graph.addVertex("sensor");
        other.property("temperature", 20.0);

        final List<Map<String, Element>> results = execute("MATCH (n:sensor {temperature: -1.5})");
        assertEquals(1, results.size());
        assertEquals(v, results.get(0).get("n"));
    }

    @Test
    public void testDoubleQuotedStringLiteral() {
        final Vertex v = graph.addVertex("person");
        v.property("name", "Alice");
        final Vertex other = graph.addVertex("person");
        other.property("name", "Bob");

        final List<Map<String, Element>> results = execute("MATCH (n:person {name: \"Alice\"})");
        assertEquals(1, results.size());
        assertEquals(v, results.get(0).get("n"));
    }

    @Test
    public void testStringWithEscapeSequence() {
        final Vertex v = graph.addVertex("item");
        v.property("desc", "line1\nline2");
        final Vertex other = graph.addVertex("item");
        other.property("desc", "plain");

        final List<Map<String, Element>> results = execute("MATCH (n:item {desc: 'line1\\nline2'})");
        assertEquals(1, results.size());
        assertEquals(v, results.get(0).get("n"));
    }

    @Test
    public void testNullLiteralMatchesAbsentProperty() {
        // null predicate should match vertices where the property is absent.
        final Vertex noName = graph.addVertex("person");
        final Vertex withName = graph.addVertex("person");
        withName.property("name", "Alice");

        final List<Map<String, Element>> results = execute("MATCH (n:person {name: null})");
        assertEquals(1, results.size());
        assertEquals(noName, results.get(0).get("n"));
    }

    @Test
    public void testNullLiteralDoesNotMatchPresentProperty() {
        final Vertex v = graph.addVertex("person");
        v.property("name", "Alice");

        assertTrue(execute("MATCH (n:person {name: null})").isEmpty());
    }

    @Test
    public void testNanLiteralMatchesNanProperty() {
        final Vertex v = graph.addVertex("sensor");
        v.property("reading", Double.NaN);
        final Vertex other = graph.addVertex("sensor");
        other.property("reading", 1.0);

        // NaN == NaN via Objects.equals on same Double.NaN constant
        final List<Map<String, Element>> results = execute("MATCH (n:sensor {reading: NaN})");
        assertEquals(1, results.size());
        assertEquals(v, results.get(0).get("n"));
    }

    @Test
    public void testInfinityLiteralMatchesInfinityProperty() {
        final Vertex v = graph.addVertex("node");
        v.property("limit", Double.POSITIVE_INFINITY);
        final Vertex other = graph.addVertex("node");
        other.property("limit", 1000.0);

        final List<Map<String, Element>> results = execute("MATCH (n:node {limit: Infinity})");
        assertEquals(1, results.size());
        assertEquals(v, results.get(0).get("n"));
    }

    @Test
    public void testNegativeInfinityLiteral() {
        final Vertex v = graph.addVertex("node");
        v.property("limit", Double.NEGATIVE_INFINITY);
        graph.addVertex("node").property("limit", 0.0);

        final List<Map<String, Element>> results = execute("MATCH (n:node {limit: -Infinity})");
        assertEquals(1, results.size());
        assertEquals(v, results.get(0).get("n"));
    }

    // -------------------------------------------------------------------------
    // Equality constraint short-circuit
    // -------------------------------------------------------------------------

    @Test
    public void testParallelEdgesWithAnonymousEdgeAndBoundTargetProduceOneResult() {
        // Multigraph: two parallel KNOWS edges from alice to bob. Pattern has a shared
        // variable (b) so the second step's target is already bound when evaluated.
        // The anonymous edge short-circuit must produce exactly one result, not two.
        final Vertex alice = graph.addVertex("Person");
        final Vertex bob = graph.addVertex("Person");
        final Vertex acme = graph.addVertex("Company");
        alice.addEdge("KNOWS", bob);
        alice.addEdge("KNOWS", bob); // parallel edge
        bob.addEdge("WORKS_AT", acme);

        final List<Map<String, Element>> results = execute(
                "MATCH (a:Person)-[:KNOWS]->(b:Person), (b)-[:WORKS_AT]->(c:Company)");
        assertEquals(1, results.size());
        assertEquals(alice, results.get(0).get("a"));
        assertEquals(bob, results.get(0).get("b"));
        assertEquals(acme, results.get(0).get("c"));
    }

    @Test
    public void testParallelEdgesWithNamedEdgeAndBoundTargetProduceOneResultPerEdge() {
        // When the edge IS named, each parallel edge is a distinct result.
        final Vertex alice = graph.addVertex("Person");
        final Vertex bob = graph.addVertex("Person");
        final Vertex acme = graph.addVertex("Company");
        alice.addEdge("KNOWS", bob);
        alice.addEdge("KNOWS", bob); // parallel edge
        bob.addEdge("WORKS_AT", acme);

        final List<Map<String, Element>> results = execute(
                "MATCH (a:Person)-[r:KNOWS]->(b:Person), (b)-[:WORKS_AT]->(c:Company)");
        assertEquals(2, results.size());
        assertEquals(2, results.stream().map(row -> row.get("r")).distinct().count());
    }

    @Test
    public void testTriangleWithParallelClosingEdgeProducesOneResult() {
        // Triangle pattern — the closing edge triggers the equality constraint path.
        // Parallel edges on the closing edge must not produce duplicates.
        final Vertex a = graph.addVertex("A");
        final Vertex b = graph.addVertex("B");
        final Vertex c = graph.addVertex("C");
        a.addEdge("AB", b);
        b.addEdge("BC", c);
        c.addEdge("CA", a);
        c.addEdge("CA", a); // parallel closing edge

        final List<Map<String, Element>> results = execute(
                "MATCH (a:A)-[:AB]->(b:B)-[:BC]->(c:C)-[:CA]->(a:A)");
        assertEquals(1, results.size());
    }

    // -------------------------------------------------------------------------
    // Adaptive step ordering: selectivityRatio() and estimatedCost
    // -------------------------------------------------------------------------

    @Test
    public void testAdaptiveCountersUpdateAfterExecution() {
        // After execution the step's attempts and hits counters must be non-zero,
        // confirming that recordAttempt/recordHit are wired correctly.
        final Vertex a = graph.addVertex("Person");
        final Vertex b = graph.addVertex("Person");
        a.addEdge("KNOWS", b);

        final GqlMatchPlan plan = planner.plan("MATCH (a:Person)-[:KNOWS]->(b:Person)");
        executor.execute(plan).forEachRemaining(row -> {}); // drain fully

        // The plan has one extension step; it must have been invoked at least once.
        final ExtensionStep step = plan.getSteps().get(0);
        final double ratio = step.selectivityRatio();
        assertTrue("selectivityRatio must be positive", ratio > 0.0);
        assertTrue("selectivityRatio must be finite", Double.isFinite(ratio));
    }

    @Test
    public void testMultipleEligibleStepsProduceCorrectResultsRegardlessOfOrder() {
        // Two independent branches from the seed: seed -[:A]-> x and seed -[:B]-> y.
        // Both steps are simultaneously eligible after the seed is bound.
        // The executor must try them in some order and produce all combinations.
        final Vertex seed = graph.addVertex("Hub");
        final Vertex x1   = graph.addVertex("X");
        final Vertex x2   = graph.addVertex("X");
        final Vertex y1   = graph.addVertex("Y");
        seed.addEdge("A", x1);
        seed.addEdge("A", x2);
        seed.addEdge("B", y1);

        // MATCH (s:Hub)-[:A]->(x:X), (s)-[:B]->(y:Y) — s is shared, both steps eligible once s is bound
        final List<Map<String, Element>> results = execute(
                "MATCH (s:Hub)-[:A]->(x:X), (s)-[:B]->(y:Y)");

        // 2 x-candidates × 1 y-candidate = 2 result rows
        assertEquals(2, results.size());
        results.forEach(r -> {
            assertEquals(seed, r.get("s"));
            assertEquals(y1, r.get("y"));
            assertTrue(r.get("x").equals(x1) || r.get("x").equals(x2));
        });
    }

    @Test
    public void testHighlySelectiveStepChosenFirstAdaptively() {
        // After several executions, the step that produces fewer results per attempt should
        // converge to a lower selectivityRatio and be preferred.
        final Vertex seed = graph.addVertex("Hub");
        final Vertex y = graph.addVertex("Y");
        seed.addEdge("B", y);
        for (int i = 0; i < 10; i++) {
            seed.addEdge("A", graph.addVertex("X"));
        }

        final GqlMatchPlan plan = planner.plan("MATCH (s:Hub)-[:A]->(x:X), (s)-[:B]->(y:Y)");

        // Execute multiple times so the adaptive counters accumulate.
        for (int i = 0; i < 5; i++) {
            executor.execute(plan).forEachRemaining(row -> {});
        }

        // The B-step produces 1 hit per attempt (ratio ≈ 1.0);
        // the A-step produces 10 hits per attempt (ratio > 1.0).
        // After convergence, B-step's ratio < A-step's ratio.
        ExtensionStep stepA = null, stepB = null;
        for (final ExtensionStep s : plan.getSteps()) {
            if ("A".equals(s.getEdgeLabel())) stepA = s;
            if ("B".equals(s.getEdgeLabel())) stepB = s;
        }
        assertNotNull(stepA);
        assertNotNull(stepB);
        assertTrue("B-step (1 hit/attempt) should have lower selectivityRatio than A-step (10 hits/attempt)",
                stepB.selectivityRatio() < stepA.selectivityRatio());
    }

    // -------------------------------------------------------------------------
    // Edge property filter evaluation
    // -------------------------------------------------------------------------

    @Test
    public void testEdgePropertyFilterLiteralFiltersResults() {
        // Two KNOWS edges: one with weight=1.0, one with weight=0.5.
        // The filter [e:KNOWS {weight: 0.5}] should return only the low-weight edge.
        final Vertex a = graph.addVertex("Person");
        final Vertex b = graph.addVertex("Person");
        final Vertex c = graph.addVertex("Person");
        final Edge highWeight = a.addEdge("KNOWS", b);
        highWeight.property("weight", 1.0);
        final Edge lowWeight = a.addEdge("KNOWS", c);
        lowWeight.property("weight", 0.5);

        final GqlMatchPlan plan = planner.plan(
                "MATCH (a:Person)-[e:KNOWS {weight: 0.5}]->(b:Person)");
        final List<GqlRow> results = new ArrayList<>();
        executor.execute(plan).forEachRemaining(results::add);

        assertEquals("Only the edge with weight=0.5 should match", 1, results.size());
        final int eIdx = plan.getIndex("e");
        assertEquals(lowWeight, results.get(0).getScalar(eIdx));
    }

    @Test
    public void testEdgePropertyFilterExcludesAllWhenNoMatch() {
        // KNOWS edge has weight=0.8 — filter for weight=0.1 matches nothing.
        final Vertex a = graph.addVertex("Person");
        final Vertex b = graph.addVertex("Person");
        final Edge edge = a.addEdge("KNOWS", b);
        edge.property("weight", 0.8);

        final GqlMatchPlan plan = planner.plan(
                "MATCH (a:Person)-[e:KNOWS {weight: 0.1}]->(b:Person)");
        final List<GqlRow> results = new ArrayList<>();
        executor.execute(plan).forEachRemaining(results::add);

        assertTrue("Filter matching no edge should return no results", results.isEmpty());
    }

    @Test
    public void testEdgePropertyFilterWithParamRef() {
        // Parameterized edge filter: [e:KNOWS {weight: $w}]
        final Vertex a = graph.addVertex("Person");
        final Vertex b = graph.addVertex("Person");
        final Vertex c = graph.addVertex("Person");
        final Edge heavy = a.addEdge("KNOWS", b);
        heavy.property("weight", 2.0);
        final Edge light = a.addEdge("KNOWS", c);
        light.property("weight", 0.1);

        final GqlMatchPlan plan = planner.plan(
                "MATCH (a:Person)-[e:KNOWS {weight: $w}]->(b:Person)");
        final List<GqlRow> results = new ArrayList<>();
        executor.execute(plan, Collections.singletonMap("w", 2.0)).forEachRemaining(results::add);

        assertEquals(1, results.size());
        final int eIdx = plan.getIndex("e");
        assertEquals(heavy, results.get(0).getScalar(eIdx));
    }

    @Test
    public void testEdgeFilterDoesNotAffectUnfilteredEdges() {
        // When no edge filter is specified, both edges match.
        final Vertex a = graph.addVertex("Person");
        final Vertex b = graph.addVertex("Person");
        final Vertex c = graph.addVertex("Person");
        a.addEdge("KNOWS", b);
        a.addEdge("KNOWS", c);

        final GqlMatchPlan plan = planner.plan(
                "MATCH (a:Person)-[:KNOWS]->(b:Person)");
        final List<GqlRow> results = new ArrayList<>();
        executor.execute(plan).forEachRemaining(results::add);

        assertEquals("Without an edge filter both edges match", 2, results.size());
    }

    // -------------------------------------------------------------------------
    // Reused edge variable equality constraint
    // -------------------------------------------------------------------------

    @Test
    public void testReusedEdgeVariableAcrossPatternsEnforcesEquality() {
        // MATCH (a)-[r:LOOP]->(a) — self-loop where 'r' connects 'a' to itself.
        // A non-loop edge and a self-loop exist. Only the self-loop satisfies the equality
        // constraint that anchor and target are both bound to 'a'.
        final Vertex x = graph.addVertex("Node");
        final Vertex y = graph.addVertex("Node");
        final Edge selfLoop = x.addEdge("LOOP", x);
        x.addEdge("LOOP", y); // non-loop — must not match MATCH (a)-[r:LOOP]->(a)

        final GqlMatchPlan plan = planner.plan("MATCH (a:Node)-[r:LOOP]->(a:Node)");
        final List<GqlRow> results = new ArrayList<>();
        executor.execute(plan).forEachRemaining(results::add);

        assertEquals("Only the self-loop satisfies the equality constraint on 'a'", 1, results.size());
        final int rIdx = plan.getIndex("r");
        assertEquals(selfLoop, results.get(0).getScalar(rIdx));
    }

    @Test
    public void testReusedEdgeVariableInMultiPatternEnforcesEquality() {
        // MATCH (a)-[r:E]->(b), (b)-[r:E]->(c)
        // 'r' cannot be the same edge for both traversals because they start from different
        // anchors. The result should be empty.
        final Vertex a = graph.addVertex("N");
        final Vertex b = graph.addVertex("N");
        final Vertex c = graph.addVertex("N");
        a.addEdge("E", b);
        b.addEdge("E", c);

        final GqlMatchPlan plan = planner.plan("MATCH (a:N)-[r:E]->(b:N), (b)-[r:E]->(c:N)");
        final List<GqlRow> results = new ArrayList<>();
        executor.execute(plan).forEachRemaining(results::add);

        assertTrue("No single edge can connect a→b and b→c simultaneously", results.isEmpty());
    }

    // -------------------------------------------------------------------------
    // GqlMatchPlan.getIndex() unknown variable
    // -------------------------------------------------------------------------

    @Test(expected = IllegalArgumentException.class)
    public void testGetIndexUnknownVariableThrowsIllegalArgument() {
        final GqlMatchPlan plan = planner.plan("MATCH (n:Person)");
        plan.getIndex("doesNotExist");
    }

    // -------------------------------------------------------------------------
    // Lazy delivery: each row is an independent snapshot
    // -------------------------------------------------------------------------

    @Test
    public void testResultRowsAreIndependentSnapshots() {
        // Rows are immutable snapshots: producing the next row from the same seed rebinds the
        // executor's working state, which must not change a row already emitted.
        final Vertex a = graph.addVertex("Person");
        final Vertex b1 = graph.addVertex("Person");
        final Vertex b2 = graph.addVertex("Person");
        a.addEdge("knows", b1);
        a.addEdge("knows", b2);

        final GqlMatchPlan plan = planner.plan("MATCH (a:Person)-[e:knows]->(b:Person)");
        assertEquals("both rows must come from one seed for the test to exercise working-state reuse",
                "a", plan.getSeedVariable());
        final int aIdx = plan.getIndex("a");
        final int eIdx = plan.getIndex("e");
        final int bIdx = plan.getIndex("b");
        final Iterator<GqlRow> iter = executor.execute(plan);

        final GqlRow first = iter.next();
        final Element firstA = first.getScalar(aIdx);
        final Element firstE = first.getScalar(eIdx);
        final Element firstB = first.getScalar(bIdx);
        assertSame(a, firstA);
        assertNotNull(firstE);
        assertNotNull(firstB);

        assertTrue(iter.hasNext());
        final GqlRow second = iter.next();
        assertSame("both rows must come from the same seed", a, second.getScalar(aIdx));
        assertNotEquals(firstE, second.getScalar(eIdx));
        assertNotEquals(firstB, second.getScalar(bIdx));

        assertSame(firstA, first.getScalar(aIdx));
        assertSame(firstE, first.getScalar(eIdx));
        assertSame(firstB, first.getScalar(bIdx));
        assertFalse(iter.hasNext());
    }

    @Test(timeout = 10000)
    public void testQuantifiedResultRowsAreIndependentSnapshots() {
        // a -> b -> c with {1,2} yields, from seed a, the one-hop walk [ab] and then the two-hop
        // walk [ab, bc]. Producing the second row from the same seed rebinds the group and target
        // of the working state, which must not change the first row's group list or scalars.
        final Vertex a = graph.addVertex("Person");
        final Vertex b = graph.addVertex("Person");
        final Vertex c = graph.addVertex("Person");
        final Edge ab = a.addEdge("knows", b);
        final Edge bc = b.addEdge("knows", c);

        final GqlMatchPlan plan = planner.plan("MATCH (s:Person)-[r:knows]->{1,2}(d:Person)");
        assertEquals("both rows must come from one seed for the test to exercise working-state reuse",
                "s", plan.getSeedVariable());
        final int sIdx = plan.getIndex("s");
        final int rIdx = plan.getIndex("r");
        final int dIdx = plan.getIndex("d");

        final Iterator<GqlRow> iter = executor.execute(plan);
        GqlRow first = null;
        while (iter.hasNext()) {
            final GqlRow row = iter.next();
            if (row.getScalar(sIdx) == a) {
                first = row;
                break;
            }
        }
        assertNotNull("seed a must produce a row", first);
        final List<Edge> firstGroup = new ArrayList<>(first.getGroup(rIdx));
        final Element firstD = first.getScalar(dIdx);

        assertTrue(iter.hasNext());
        final GqlRow second = iter.next();
        assertSame("both rows must come from the same seed", a, second.getScalar(sIdx));
        final List<Edge> secondGroup = second.getGroup(rIdx);
        assertNotEquals(firstGroup, secondGroup);
        assertEquals(Set.of(List.of(ab), List.of(ab, bc)), Set.of(firstGroup, secondGroup));
        assertNotEquals(firstD, second.getScalar(dIdx));
        assertSame(firstGroup.size() == 1 ? b : c, firstD);
        assertSame(secondGroup.size() == 1 ? b : c, second.getScalar(dIdx));

        assertEquals(firstGroup, first.getGroup(rIdx));
        assertSame(firstD, first.getScalar(dIdx));
        assertSame(a, first.getScalar(sIdx));
    }

    // -------------------------------------------------------------------------
    // Multi-property (list cardinality) predicate evaluation
    // -------------------------------------------------------------------------

    @Test
    public void testMultiPropertyAnyValueMatches() {
        // A vertex with list cardinality on 'lang' should match if any value equals the predicate.
        final Vertex v = graph.addVertex("Software");
        v.property(VertexProperty.Cardinality.list, "lang", "java");
        v.property(VertexProperty.Cardinality.list, "lang", "groovy");

        final GqlMatchPlan plan = planner.plan("MATCH (s:Software {lang: 'groovy'})");
        final List<GqlRow> results = new ArrayList<>();
        executor.execute(plan).forEachRemaining(results::add);

        assertEquals("vertex with 'groovy' among its lang values must match", 1, results.size());
    }

    @Test
    public void testMultiPropertyNoMatchWhenValueAbsent() {
        // A vertex with list cardinality on 'lang' must not match a value not in its list.
        final Vertex v = graph.addVertex("Software");
        v.property(VertexProperty.Cardinality.list, "lang", "java");
        v.property(VertexProperty.Cardinality.list, "lang", "groovy");

        final GqlMatchPlan plan = planner.plan("MATCH (s:Software {lang: 'python'})");
        final List<GqlRow> results = new ArrayList<>();
        executor.execute(plan).forEachRemaining(results::add);

        assertTrue("vertex without 'python' in its lang values must not match", results.isEmpty());
    }

    @Test
    public void testMultiPropertyNullValueMatchesNullPredicate() {
        // When a vertex has a null value among its list-cardinality properties, a {key: null}
        // predicate should match (any-value semantics apply to null too).
        final BaseConfiguration config = new BaseConfiguration();
        config.setProperty(AbstractTinkerGraph.GREMLIN_TINKERGRAPH_ALLOW_NULL_PROPERTY_VALUES, true);
        final TinkerGraph nullGraph = TinkerGraph.open(config);
        try {
            final DefaultGqlPlanner nullPlanner = new DefaultGqlPlanner(nullGraph);
            final DefaultGqlExecutor nullExecutor = new DefaultGqlExecutor(nullGraph);

            final Vertex v = nullGraph.addVertex("Item");
            v.property(VertexProperty.Cardinality.list, "tag", "java");
            v.property(VertexProperty.Cardinality.list, "tag", null);

            final GqlMatchPlan plan = nullPlanner.plan("MATCH (n:Item {tag: null})");
            final List<GqlRow> results = new ArrayList<>();
            nullExecutor.execute(plan).forEachRemaining(results::add);

            assertEquals("vertex with null among its tag values must match {tag: null}", 1, results.size());
        } finally {
            nullGraph.close();
        }
    }

    @Test
    public void testParamBindingWithMultiProperty() {
        // A $param reference predicate must use any-value-matches semantics for list cardinality.
        final Vertex v = graph.addVertex("Software");
        v.property(VertexProperty.Cardinality.list, "lang", "java");
        v.property(VertexProperty.Cardinality.list, "lang", "groovy");

        final GqlMatchPlan plan = planner.plan("MATCH (s:Software {lang: $lang})");
        final List<GqlRow> results = new ArrayList<>();
        executor.execute(plan, Collections.singletonMap("lang", "groovy")).forEachRemaining(results::add);

        assertEquals("param predicate must match against any value in a list-cardinality property", 1, results.size());
    }

    // -------------------------------------------------------------------------
    // Quantified (variable-length) relationship patterns
    //
    // Semantics under test:
    //   * per-walk TRAIL edge-uniqueness (an edge is used at most once per walk),
    //   * NO vertex gating (vertices may repeat via distinct edges),
    //   * emit-at-each-depth in [minHops, maxHops],
    //   * the edge variable is a group variable bound as an ordered List<Edge>.
    // -------------------------------------------------------------------------

    /** Extracts the ordered edge list bound to a quantified group edge variable in a result row. */
    private List<Edge> edgeList(final Map<String, Element> row, final String var) {
        assertTrue("group edge variable must be surfaced via the row's group side map",
                row instanceof Row);
        final List<Edge> edges = ((Row) row).groups.get(var);
        assertNotNull("expected a bound group edge variable '" + var + "'", edges);
        return edges;
    }

    /** Groups result rows by the target vertex bound to {@code var}. */
    private Map<Element, List<Map<String, Element>>> byTarget(final List<Map<String, Element>> rows,
                                                              final String var) {
        final Map<Element, List<Map<String, Element>>> grouped = new LinkedHashMap<>();
        for (final Map<String, Element> row : rows)
            grouped.computeIfAbsent(row.get(var), k -> new ArrayList<>()).add(row);
        return grouped;
    }

    @Test(timeout = 10000)
    public void testQuantifiedEmitsAtEachDepthWithEdgeLists() {
        // a-KNOWS->b-KNOWS->c-KNOWS->d ; {1,3} from a emits b,c,d with the exact edge list per depth.
        final Vertex a = graph.addVertex("Person");
        a.property("n", "a");
        final Vertex b = graph.addVertex("Person");
        final Vertex c = graph.addVertex("Person");
        final Vertex d = graph.addVertex("Person");
        final Edge e1 = a.addEdge("KNOWS", b);
        final Edge e2 = b.addEdge("KNOWS", c);
        final Edge e3 = c.addEdge("KNOWS", d);

        final List<Map<String, Element>> results =
                execute("MATCH (a:Person {n: 'a'})-[r:KNOWS]->{1,3}(x:Person)");

        assertEquals(3, results.size());
        final Map<Element, List<Map<String, Element>>> byX = byTarget(results, "x");
        assertEquals(List.of(e1), edgeList(byX.get(b).get(0), "r"));
        assertEquals(List.of(e1, e2), edgeList(byX.get(c).get(0), "r"));
        assertEquals(List.of(e1, e2, e3), edgeList(byX.get(d).get(0), "r"));
    }

    @Test(timeout = 10000)
    public void testQuantifiedLowerBoundGreaterThanOne() {
        // {2,3} from a emits only c and d (not b).
        final Vertex a = graph.addVertex("Person");
        a.property("n", "a");
        final Vertex b = graph.addVertex("Person");
        final Vertex c = graph.addVertex("Person");
        final Vertex d = graph.addVertex("Person");
        final Edge e1 = a.addEdge("KNOWS", b);
        final Edge e2 = b.addEdge("KNOWS", c);
        final Edge e3 = c.addEdge("KNOWS", d);

        final List<Map<String, Element>> results =
                execute("MATCH (a:Person {n: 'a'})-[r:KNOWS]->{2,3}(x:Person)");

        final Map<Element, List<Map<String, Element>>> byX = byTarget(results, "x");
        assertEquals(2, results.size());
        assertEquals(Set.of(c, d), byX.keySet());
        assertEquals(List.of(e1, e2), edgeList(byX.get(c).get(0), "r"));
        assertEquals(List.of(e1, e2, e3), edgeList(byX.get(d).get(0), "r"));
    }

    @Test(timeout = 10000)
    public void testQuantifiedExactBound() {
        // {2} exactly: only depth-2 target c.
        final Vertex a = graph.addVertex("Person");
        a.property("n", "a");
        final Vertex b = graph.addVertex("Person");
        final Vertex c = graph.addVertex("Person");
        final Vertex d = graph.addVertex("Person");
        final Edge e1 = a.addEdge("KNOWS", b);
        final Edge e2 = b.addEdge("KNOWS", c);
        c.addEdge("KNOWS", d);

        final List<Map<String, Element>> results =
                execute("MATCH (a:Person {n: 'a'})-[r:KNOWS]->{2}(x:Person)");

        assertEquals(1, results.size());
        assertEquals(c, results.get(0).get("x"));
        assertEquals(List.of(e1, e2), edgeList(results.get(0), "r"));
    }

    @Test(timeout = 10000)
    public void testQuantifiedUnboundedOverChain() {
        // {2,} over a 3-edge chain emits c and d only.
        final Vertex a = graph.addVertex("Person");
        a.property("n", "a");
        final Vertex b = graph.addVertex("Person");
        final Vertex c = graph.addVertex("Person");
        final Vertex d = graph.addVertex("Person");
        final Edge e1 = a.addEdge("KNOWS", b);
        final Edge e2 = b.addEdge("KNOWS", c);
        final Edge e3 = c.addEdge("KNOWS", d);

        final List<Map<String, Element>> results =
                execute("MATCH (a:Person {n: 'a'})-[r:KNOWS]->{2,}(x:Person)");

        final Map<Element, List<Map<String, Element>>> byX = byTarget(results, "x");
        assertEquals(2, results.size());
        assertEquals(Set.of(c, d), byX.keySet());
        assertEquals(List.of(e1, e2), edgeList(byX.get(c).get(0), "r"));
        assertEquals(List.of(e1, e2, e3), edgeList(byX.get(d).get(0), "r"));
    }

    @Test(timeout = 10000)
    public void testQuantifiedPlusOverChain() {
        // + (one or more) over a 3-edge chain emits b, c, d.
        final Vertex a = graph.addVertex("Person");
        a.property("n", "a");
        final Vertex b = graph.addVertex("Person");
        final Vertex c = graph.addVertex("Person");
        final Vertex d = graph.addVertex("Person");
        final Edge e1 = a.addEdge("KNOWS", b);
        final Edge e2 = b.addEdge("KNOWS", c);
        final Edge e3 = c.addEdge("KNOWS", d);

        final List<Map<String, Element>> results =
                execute("MATCH (a:Person {n: 'a'})-[r:KNOWS]->+(x:Person)");

        assertEquals(3, results.size());
        final Map<Element, List<Map<String, Element>>> byX = byTarget(results, "x");
        assertEquals(Set.of(b, c, d), byX.keySet());
        assertEquals(List.of(e1), edgeList(byX.get(b).get(0), "r"));
        assertEquals(List.of(e1, e2), edgeList(byX.get(c).get(0), "r"));
        assertEquals(List.of(e1, e2, e3), edgeList(byX.get(d).get(0), "r"));
    }

    @Test(timeout = 10000)
    public void testQuantifiedUnboundedTerminatesOnCycle() {
        // Cycle a->b->c->a. TRAIL edge-uniqueness makes an unbounded walk terminate.
        // From a with + the distinct trails are: a->b (b), a->b->c (c), a->b->c->a (a).
        final Vertex a = graph.addVertex("Person");
        a.property("n", "a");
        final Vertex b = graph.addVertex("Person");
        final Vertex c = graph.addVertex("Person");
        final Edge ab = a.addEdge("KNOWS", b);
        final Edge bc = b.addEdge("KNOWS", c);
        final Edge ca = c.addEdge("KNOWS", a);

        final List<Map<String, Element>> results =
                execute("MATCH (a:Person {n: 'a'})-[r:KNOWS]->+(x:Person)");

        // Exactly three trails from a; the deepest one returns to a (vertex repeats, edges do not).
        assertEquals(3, results.size());
        final Map<Element, List<Map<String, Element>>> byX = byTarget(results, "x");
        assertEquals(Set.of(a, b, c), byX.keySet());
        assertEquals(List.of(ab), edgeList(byX.get(b).get(0), "r"));
        assertEquals(List.of(ab, bc), edgeList(byX.get(c).get(0), "r"));
        // The length-3 trail ends back at a and uses all 3 distinct edges.
        assertEquals(List.of(ab, bc, ca), edgeList(byX.get(a).get(0), "r"));
    }

    @Test(timeout = 10000)
    public void testQuantifiedReverseDirection() {
        // <-[r:KNOWS]-{1,2} from d walks against KNOWS edges: d<-c<-b (a-b-c-d chain).
        final Vertex a = graph.addVertex("Person");
        final Vertex b = graph.addVertex("Person");
        final Vertex c = graph.addVertex("Person");
        final Vertex d = graph.addVertex("Person");
        d.property("n", "d");
        a.addEdge("KNOWS", b);
        final Edge bc = b.addEdge("KNOWS", c);
        final Edge cd = c.addEdge("KNOWS", d);

        final List<Map<String, Element>> results =
                execute("MATCH (d:Person {n: 'd'})<-[r:KNOWS]-{1,2}(x:Person)");

        final Map<Element, List<Map<String, Element>>> byX = byTarget(results, "x");
        assertEquals(2, results.size());
        assertEquals(Set.of(c, b), byX.keySet());
        assertEquals(List.of(cd), edgeList(byX.get(c).get(0), "r"));     // depth 1
        assertEquals(List.of(cd, bc), edgeList(byX.get(b).get(0), "r")); // depth 2
    }

    @Test(timeout = 10000)
    public void testQuantifiedUndirectedSingleEdgeDoesNotOscillate() {
        // Undirected -[r:KNOWS]-{1,2} over a single edge a-b must NOT oscillate:
        // the one edge is consumed once, so from a only b is reachable (depth 1), never back to a.
        final Vertex a = graph.addVertex("Person");
        a.property("n", "a");
        final Vertex b = graph.addVertex("Person");
        final Edge ab = a.addEdge("KNOWS", b);

        final List<Map<String, Element>> results =
                execute("MATCH (a:Person {n: 'a'})-[r:KNOWS]-{1,2}(x:Person)");

        assertEquals(1, results.size());
        assertEquals(b, results.get(0).get("x"));
        assertEquals(List.of(ab), edgeList(results.get(0), "r"));
    }

    @Test(timeout = 10000)
    public void testQuantifiedUndirectedParallelEdgesUseBothTrails() {
        // Two parallel KNOWS edges a=>b. Undirected {1,2} from a:
        //   depth1: a-b via e1, a-b via e2  (two rows, target b)
        //   depth2: a-b-a via {e1,e2} and via {e2,e1} (two rows, target a); vertex a repeats
        //           through a distinct second edge, which TRAIL permits.
        final Vertex a = graph.addVertex("Person");
        a.property("n", "a");
        final Vertex b = graph.addVertex("Person");
        final Edge e1 = a.addEdge("KNOWS", b);
        final Edge e2 = a.addEdge("KNOWS", b);

        assertEquals(expectedTuples(
                List.of(a, b, List.of(e1)),
                List.of(a, b, List.of(e2)),
                List.of(a, a, List.of(e1, e2)),
                List.of(a, a, List.of(e2, e1))),
                tuples(execute("MATCH (a:Person {n: 'a'})-[r:KNOWS]-{1,2}(x:Person)"), "a", "x", "r"));
    }

    @Test(timeout = 10000)
    public void testQuantifiedSelfLoop() {
        // Self-loop a-KNOWS->a. {1,2} handles the self-loop as a valid length-1 trail without
        // infinite looping: the single self-edge is consumed once.
        final Vertex a = graph.addVertex("Person");
        a.property("n", "a");
        final Edge self = a.addEdge("KNOWS", a);

        final List<Map<String, Element>> results =
                execute("MATCH (a:Person {n: 'a'})-[r:KNOWS]->{1,2}(x:Person)");

        assertEquals(1, results.size());
        assertEquals(a, results.get(0).get("x"));
        assertEquals(List.of(self), edgeList(results.get(0), "r"));
    }

    @Test(timeout = 10000)
    public void testExactlyOneHopQuantifierBindsGroupEdgeList() {
        // A {1,1} quantified edge variable is still a group variable: it surfaces r as a
        // one-element List<Edge> through the group channel, honoring the length-independent
        // group-list contract (see GqlMatchStepTest
        // testExactlyOneHopQuantifierSurfacesSingletonEdgeListInBothChannels).
        final Vertex a = graph.addVertex("Person");
        a.property("n", "a");
        final Vertex b = graph.addVertex("Person");
        final Edge e1 = a.addEdge("KNOWS", b);

        final List<Map<String, Element>> results =
                execute("MATCH (a:Person {n: 'a'})-[r:KNOWS]->{1,1}(x:Person)");

        assertEquals(1, results.size());
        assertEquals(b, results.get(0).get("x"));
        assertEquals(List.of(e1), edgeList(results.get(0), "r"));
    }

    @Test(timeout = 10000)
    public void testQuantifiedNoMatchWhenTargetLabelUnsatisfied() {
        // No path reaches a Company via KNOWS, so 0 rows.
        final Vertex a = graph.addVertex("Person");
        a.property("n", "a");
        final Vertex b = graph.addVertex("Person");
        a.addEdge("KNOWS", b);

        final List<Map<String, Element>> results =
                execute("MATCH (a:Person {n: 'a'})-[r:KNOWS]->{1,3}(x:Company)");

        assertTrue(results.isEmpty());
    }

    @Test(timeout = 10000)
    public void testQuantifiedNoMatchWhenMinBoundExceedsAvailablePath() {
        // Only a 1-edge path exists; {2,3} yields nothing.
        final Vertex a = graph.addVertex("Person");
        a.property("n", "a");
        final Vertex b = graph.addVertex("Person");
        a.addEdge("KNOWS", b);

        final List<Map<String, Element>> results =
                execute("MATCH (a:Person {n: 'a'})-[r:KNOWS]->{2,3}(x:Person)");

        assertTrue(results.isEmpty());
    }

    @Test(timeout = 10000)
    public void testQuantifiedTargetPropertyPredicateFiltersEmits() {
        // a-KNOWS->b-KNOWS->c ; only c has flag=true, so {1,2} with target filter emits just c.
        final Vertex a = graph.addVertex("Person");
        a.property("n", "a");
        final Vertex b = graph.addVertex("Person");
        final Vertex c = graph.addVertex("Person");
        c.property("flag", true);
        final Edge e1 = a.addEdge("KNOWS", b);
        final Edge e2 = b.addEdge("KNOWS", c);

        final List<Map<String, Element>> results =
                execute("MATCH (a:Person {n: 'a'})-[r:KNOWS]->{1,2}(x:Person {flag: true})");

        assertEquals(1, results.size());
        assertEquals(c, results.get(0).get("x"));
        assertEquals(List.of(e1, e2), edgeList(results.get(0), "r"));
    }

    @Test(timeout = 10000)
    public void testQuantifiedEdgeLabelConstrainsWalk() {
        // a-KNOWS->b, b-LIKES->c. {1,2} over KNOWS may only reach b (the LIKES edge is excluded).
        final Vertex a = graph.addVertex("Person");
        a.property("n", "a");
        final Vertex b = graph.addVertex("Person");
        final Vertex c = graph.addVertex("Person");
        a.addEdge("KNOWS", b);
        b.addEdge("LIKES", c);

        final List<Map<String, Element>> results =
                execute("MATCH (a:Person {n: 'a'})-[r:KNOWS]->{1,2}(x:Person)");

        assertEquals(1, results.size());
        assertEquals(b, results.get(0).get("x"));
    }

    // -------------------------------------------------------------------------
    // Group edge list order when the walk starts at the pattern edge's target
    // -------------------------------------------------------------------------

    /**
     * Adds Person vertices that touch no edge so that the single Company is the cheapest seed and
     * the planner anchors the quantified step at the pattern edge's target.
     */
    private void addIsolatedPersons(final int count) {
        for (int i = 0; i < count; i++) graph.addVertex("Person");
    }

    private void assertRightSeededGroupInPatternOrder(final String query, final List<Edge> expected,
                                                      final Vertex expectedA, final Vertex expectedB) {
        assertRightSeededGroupInPatternOrder(planner.plan(query), expected, expectedA, expectedB);
    }

    private void assertRightSeededGroupInPatternOrder(final GqlMatchPlan plan, final List<Edge> expected,
                                                      final Vertex expectedA, final Vertex expectedB) {
        assertEquals("the Company endpoint must be the seed for this test to start the walk at the target",
                "b", plan.getSeedVariable());
        assertTrue(plan.getSteps().get(0).isAnchoredAtPatternTarget());

        final List<Map<String, Element>> results = materialize(executor.execute(plan), plan);
        assertEquals(1, results.size());
        assertEquals(expectedA, results.get(0).get("a"));
        assertEquals(expectedB, results.get(0).get("b"));
        assertEquals(expected, edgeList(results.get(0), "r"));
    }

    @Test(timeout = 10000)
    public void testQuantifiedGroupListInPatternOrderWhenSeededAtTarget() {
        // p0 -L-> p1 -L-> company, walked from company. r must read left to right.
        final Vertex p0 = graph.addVertex("Person");
        final Vertex p1 = graph.addVertex("Person");
        final Vertex company = graph.addVertex("Company");
        addIsolatedPersons(8);
        final Edge e01 = p0.addEdge("L", p1);
        final Edge e1c = p1.addEdge("L", company);

        assertRightSeededGroupInPatternOrder("MATCH (a:Person)-[r:L]->{2}(b:Company)",
                List.of(e01, e1c), p0, company);
    }

    @Test(timeout = 10000)
    public void testQuantifiedReverseGroupListInPatternOrderWhenSeededAtTarget() {
        // company -L-> p1 -L-> p0 matched by (a)<-[r]-{2}(b), walked from company.
        final Vertex p0 = graph.addVertex("Person");
        final Vertex p1 = graph.addVertex("Person");
        final Vertex company = graph.addVertex("Company");
        addIsolatedPersons(8);
        final Edge e10 = p1.addEdge("L", p0);
        final Edge ec1 = company.addEdge("L", p1);

        assertRightSeededGroupInPatternOrder("MATCH (a:Person)<-[r:L]-{2}(b:Company)",
                List.of(e10, ec1), p0, company);
    }

    @Test(timeout = 10000)
    public void testQuantifiedUndirectedGroupListInPatternOrderWhenSeededAtTarget() {
        // Undirected edges keep Direction.BOTH when anchored at the target, so ordering must not
        // depend on the direction being flipped.
        final Vertex p0 = graph.addVertex("Person");
        final Vertex p1 = graph.addVertex("Person");
        final Vertex company = graph.addVertex("Company");
        addIsolatedPersons(8);
        final Edge e01 = p0.addEdge("L", p1);
        final Edge e1c = p1.addEdge("L", company);

        final GqlMatchPlan plan = planner.plan("MATCH (a:Person)-[r:L]-{2}(b:Company)");
        assertEquals(Direction.BOTH, plan.getSteps().get(0).getDirection());
        assertRightSeededGroupInPatternOrder(plan, List.of(e01, e1c), p0, company);
    }

    @Test(timeout = 10000)
    public void testQuantifiedGroupListsInPatternOrderAtEveryDepthWhenSeededAtTarget() {
        // p0 -L-> p1 -L-> p2 -L-> company and q -L-> p1, walked from company with {2,3}. Depth 2
        // ends at p1 and depth 3 ends at p0 or q; every r must read left to right.
        final Vertex p0 = graph.addVertex("Person");
        final Vertex q = graph.addVertex("Person");
        final Vertex p1 = graph.addVertex("Person");
        final Vertex p2 = graph.addVertex("Person");
        final Vertex company = graph.addVertex("Company");
        addIsolatedPersons(8);
        final Edge e01 = p0.addEdge("L", p1);
        final Edge eq1 = q.addEdge("L", p1);
        final Edge e12 = p1.addEdge("L", p2);
        final Edge e2c = p2.addEdge("L", company);

        final GqlMatchPlan plan = planner.plan("MATCH (a:Person)-[r:L]->{2,3}(b:Company)");
        assertEquals("b", plan.getSeedVariable());
        assertTrue(plan.getSteps().get(0).isAnchoredAtPatternTarget());

        final List<Map<String, Element>> results = materialize(executor.execute(plan), plan);
        assertEquals(3, results.size());
        final Map<Element, List<Map<String, Element>>> byA = byTarget(results, "a");
        assertEquals(Set.of(p0, q, p1), byA.keySet());
        for (final Map<String, Element> row : results) assertEquals(company, row.get("b"));
        assertEquals(List.of(e12, e2c), edgeList(byA.get(p1).get(0), "r"));
        assertEquals(List.of(e01, e12, e2c), edgeList(byA.get(p0).get(0), "r"));
        assertEquals(List.of(eq1, e12, e2c), edgeList(byA.get(q).get(0), "r"));
    }

    // -------------------------------------------------------------------------
    // Pull-based iteration: interruption, early termination, deep walks
    // -------------------------------------------------------------------------

    /** Adds a complete directed graph on {@code n} Person vertices, whose trail count explodes. */
    private void addCompleteDigraph(final int n) {
        final List<Vertex> vs = new ArrayList<>();
        for (int i = 0; i < n; i++) vs.add(graph.addVertex("Person"));
        for (final Vertex from : vs)
            for (final Vertex to : vs)
                if (from != to) from.addEdge("KNOWS", to);
    }

    @Test(timeout = 30000)
    public void testInterruptStopsUnboundedWalk() throws Exception {
        addCompleteDigraph(8);
        final GqlMatchPlan plan = planner.plan("MATCH (a:Person)-[r:KNOWS]->+(b:Person)");
        final CountDownLatch started = new CountDownLatch(1);
        final AtomicReference<Throwable> thrown =
                new AtomicReference<>();

        final Thread worker = new Thread(() -> {
            try {
                final Iterator<GqlRow> iter = executor.execute(plan);
                while (iter.hasNext()) {
                    iter.next();
                    started.countDown();
                }
            } catch (final Throwable t) {
                thrown.set(t);
            }
        });
        worker.setDaemon(true);
        try {
            worker.start();
            assertTrue("the walk must start producing rows", started.await(10, TimeUnit.SECONDS));
            worker.interrupt();
            worker.join(10000);
            assertFalse("the walk must stop after interruption", worker.isAlive());
            assertTrue("expected TraversalInterruptedException but got " + thrown.get(),
                    thrown.get() instanceof TraversalInterruptedException);
        } finally {
            worker.interrupt();
        }
    }

    @Test(timeout = 30000)
    public void testInterruptStopsUnboundedWalkThatNeverEmits() throws Exception {
        // No vertex matches the target predicate, so the walk explores every trail without
        // producing a row and only the checks inside the candidate loops can observe the interrupt.
        // The probe parks the worker inside the 20th edges() call, which is deep in the first
        // seed's walk, and the test interrupts it there, so the interrupt lands mid-walk.
        addCompleteDigraph(8);
        final CountDownLatch inWalk = new CountDownLatch(1);
        final ProbeGraph probe = new ProbeGraph(graph).parkAtEdgesCall(20, inWalk);
        final GqlMatchPlan plan = new DefaultGqlPlanner(probe.graph())
                .plan("MATCH (a:Person)-[r:KNOWS]->+(b:Person {missing: 1})");
        final AtomicReference<Throwable> thrown = new AtomicReference<>();
        final AtomicReference<Boolean> produced = new AtomicReference<>(false);

        final Thread worker = new Thread(() -> {
            try {
                produced.set(new DefaultGqlExecutor(probe.graph()).execute(plan).hasNext());
            } catch (final Throwable t) {
                thrown.set(t);
            }
        });
        worker.setDaemon(true);
        try {
            worker.start();
            assertTrue("the walk must reach the 20th edges() call", inWalk.await(10, TimeUnit.SECONDS));
            assertTrue("the walk must still be running before the interrupt", worker.isAlive());
            worker.interrupt();
            worker.join(10000);
            assertFalse("the walk must stop after interruption", worker.isAlive());
            assertFalse("the walk must not have produced a row", produced.get());
            assertTrue("expected TraversalInterruptedException but got " + thrown.get(),
                    thrown.get() instanceof TraversalInterruptedException);
            probe.assertAllClosedOnce("after the interrupt");
        } finally {
            worker.interrupt();
        }
    }

    @Test(timeout = 10000)
    public void testIteratorContract() {
        final Vertex a = graph.addVertex("Person");
        final Vertex b = graph.addVertex("Person");
        final Edge ab = a.addEdge("KNOWS", b);
        final GqlMatchPlan plan = planner.plan("MATCH (x:Person)-[r:KNOWS]->{1}(y:Person)");

        final Iterator<GqlRow> iter = executor.execute(plan);
        assertTrue(iter.hasNext());
        assertTrue("hasNext() must be idempotent", iter.hasNext());
        final GqlRow row = iter.next();
        assertEquals(a, row.getScalar(plan.getIndex("x")));
        assertEquals(b, row.getScalar(plan.getIndex("y")));
        assertEquals(List.of(ab), row.getGroup(plan.getIndex("r")));
        assertFalse(iter.hasNext());
        assertFalse(iter.hasNext());
        try {
            iter.next();
            fail("next() after exhaustion must throw NoSuchElementException");
        } catch (final NoSuchElementException expected) {
            // expected
        }
    }

    @Test(timeout = 10000)
    public void testFirstRowOfUnboundedWalkOnDenseGraphIsPrompt() {
        // The full result is astronomically large. Pulled lazily, the first row costs one seed
        // read from the vertex scan and one edge read from that seed's edges(): two provider
        // iterators opened and two elements read.
        addCompleteDigraph(10);
        final ProbeGraph probe = new ProbeGraph(graph);
        final GqlMatchPlan plan = new DefaultGqlPlanner(probe.graph()).plan("MATCH (a:Person)-[r:KNOWS]->+(b:Person)");
        final Iterator<GqlRow> iter = new DefaultGqlExecutor(probe.graph()).execute(plan);
        assertTrue(iter.hasNext());
        final GqlRow row = iter.next();
        assertEquals("provider iterators opened for the first row", 2, probe.opened());
        assertEquals("elements read for the first row", 2, probe.read());

        // The complete digraph has exactly one a -> b edge, so r is that edge alone.
        final Vertex a = (Vertex) row.getScalar(plan.getIndex("a"));
        final Vertex b = (Vertex) row.getScalar(plan.getIndex("b"));
        assertEquals(graph.traversal().V(a.id()).outE("KNOWS").where(__.inV().hasId(b.id())).toList(),
                row.getGroup(plan.getIndex("r")));
        CloseableIterator.closeIterator(iter);
        probe.assertAllClosedOnce("after closing early");
    }

    @Test(timeout = 60000)
    public void testUnboundedWalkOverLongChainDoesNotOverflowStack() {
        final int length = 50_000;
        final Vertex start = graph.addVertex("Start");
        Vertex prev = start;
        for (int i = 0; i < length; i++) {
            final Vertex v = graph.addVertex("Node");
            prev.addEdge("L", v);
            prev = v;
        }

        final GqlMatchPlan plan = planner.plan("MATCH (a:Start)-[:L]->+(b:Node)");
        assertEquals("a", plan.getSeedVariable());
        final int bIdx = plan.getIndex("b");
        final Iterator<GqlRow> iter = executor.execute(plan);
        int count = 0;
        Element last = null;
        while (iter.hasNext()) {
            last = iter.next().getScalar(bIdx);
            count++;
        }
        assertEquals(length, count);
        assertEquals(prev, last);
    }

    // -------------------------------------------------------------------------
    // {1} and {1,1} run through the quantified walk
    // -------------------------------------------------------------------------

    @Test(timeout = 10000)
    public void testAnonymousExactlyOneQuantifierReturnsOneRowPerParallelEdge() {
        // Two parallel KNOWS edges a=>b. A quantified {1} edge is a walk of length one, so each
        // edge is a distinct trail, matching the depth-1 rows of {1,2}. A plain edge keeps
        // collapsing anonymous parallel edges to one row per target.
        final Vertex a = graph.addVertex("Person");
        a.property("n", "a");
        final Vertex b = graph.addVertex("Person");
        a.addEdge("KNOWS", b);
        a.addEdge("KNOWS", b);

        assertEquals(2, execute("MATCH (a:Person {n: 'a'})-[:KNOWS]->{1}(x:Person)").size());
        assertEquals(2, execute("MATCH (a:Person {n: 'a'})-[:KNOWS]->{1,1}(x:Person)").size());
        assertEquals(2, execute("MATCH (a:Person {n: 'a'})-[:KNOWS]->{1,2}(x:Person)").stream()
                .filter(row -> row.get("x") == b).count());
        assertEquals(1, execute("MATCH (a:Person {n: 'a'})-[:KNOWS]->(x:Person)").size());
    }

    @Test(timeout = 10000)
    public void testNamedExactlyOneQuantifierBindsOneListPerParallelEdge() {
        final Vertex a = graph.addVertex("Person");
        a.property("n", "a");
        final Vertex b = graph.addVertex("Person");
        final Edge e1 = a.addEdge("KNOWS", b);
        final Edge e2 = a.addEdge("KNOWS", b);

        final List<Map<String, Element>> results =
                execute("MATCH (a:Person {n: 'a'})-[r:KNOWS]->{1}(x:Person)");
        assertEquals(2, results.size());
        assertEquals(Set.of(List.of(e1), List.of(e2)),
                results.stream().map(row -> edgeList(row, "r")).collect(Collectors.toSet()));
    }

    // -------------------------------------------------------------------------
    // Data self-loops on undirected edges
    // -------------------------------------------------------------------------

    @Test(timeout = 10000)
    public void testUndirectedSingleHopOverDataSelfLoopMatchesLoopOnce() {
        // An undirected edge pattern binds each edge once, so a self-loop is one row even though
        // TinkerGraph's edges(BOTH) reports it as both an out-edge and an in-edge.
        final Vertex a = graph.addVertex("P");
        final Edge loop = a.addEdge("L", a);

        final List<Map<String, Element>> named = execute("MATCH (a:P)-[r:L]-(b:P)");
        assertEquals(1, named.size());
        assertEquals(a, named.get(0).get("a"));
        assertEquals(a, named.get(0).get("b"));
        assertEquals(loop, named.get(0).get("r"));
        assertEquals(1, execute("MATCH (a:P)-[:L]-(b:P)").size());
    }

    @Test(timeout = 10000)
    public void testUndirectedQuantifiedWalkOverDataSelfLoopMatchesSingleHop() {
        // As with the single-hop step, the walk takes the self-loop once, binding r to the
        // one-edge list. TRAIL uniqueness stops the walk from taking the loop a second time, so
        // {1,2} adds no depth-2 rows.
        final Vertex a = graph.addVertex("P");
        final Edge loop = a.addEdge("L", a);

        for (final String q : new String[]{"{1}", "{1,2}", "+"}) {
            final List<Map<String, Element>> rows = execute("MATCH (a:P)-[r:L]-" + q + "(b:P)");
            assertEquals(q, 1, rows.size());
            assertEquals(q, a, rows.get(0).get("a"));
            assertEquals(q, a, rows.get(0).get("b"));
            assertEquals(q, List.of(loop), edgeList(rows.get(0), "r"));
        }
        assertEquals(1, execute("MATCH (a:P)-[:L]-{1,2}(b:P)").size());
    }

    @Test(timeout = 10000)
    public void testPlainPatternSelfLoopOverDataSelfLoopInEveryDirection() {
        // A plain pattern self-loop matches a data self-loop once in every direction.
        final Vertex a = graph.addVertex("P");
        final Edge loop = a.addEdge("L", a);
        graph.addVertex("P");

        for (final String query : new String[]{"MATCH (a:P)-[r:L]->(a)", "MATCH (a:P)<-[r:L]-(a)",
                "MATCH (a:P)-[r:L]-(a)"}) {
            final List<Map<String, Element>> rows = execute(query);
            assertEquals(query, 1, rows.size());
            assertEquals(query, a, rows.get(0).get("a"));
            assertEquals(query, loop, rows.get(0).get("r"));
        }
    }

    @Test(timeout = 10000)
    public void testUndirectedTwoHopWalkOverTwoDataSelfLoopsReturnsEachOrderOnce() {
        final Vertex a = graph.addVertex("P");
        final Edge s1 = a.addEdge("L", a);
        final Edge s2 = a.addEdge("L", a);

        final Map<List<Object>, Long> expected = expectedTuples(
                List.of(a, List.of(s1, s2)),
                List.of(a, List.of(s2, s1)));
        assertEquals(expected, tuples(execute("MATCH (a:P)-[r:L]-{2}(a)"), "a", "r"));
        // The same rows as the directed walk.
        assertEquals(expected, tuples(execute("MATCH (a:P)-[r:L]->{2}(a)"), "a", "r"));
    }

    @Test(timeout = 10000)
    public void testUndirectedWalkOverDataSelfLoopAndEdgeReturnsEachWalkOnce() {
        // a carries loop s and edge e: a -> b.
        final Vertex a = graph.addVertex("P");
        final Vertex b = graph.addVertex("P");
        final Edge s = a.addEdge("L", a);
        final Edge e = a.addEdge("L", b);

        assertEquals(expectedTuples(
                List.of(a, List.of(s), a),
                List.of(a, List.of(e), b),
                List.of(a, List.of(s, e), b),
                List.of(b, List.of(e), a),
                List.of(b, List.of(e, s), a)), tuples(execute("MATCH (a:P)-[r:L]-{1,2}(b:P)"), "a", "r", "b"));
    }

    @Test(timeout = 10000)
    public void testUndirectedStepOverParallelEdgesReturnsOneRowPerEdge() {
        // Only self-loops are deduplicated: parallel edges between distinct vertices, in either
        // direction, are each their own row.
        final Vertex a = graph.addVertex("P");
        final Vertex b = graph.addVertex("Q");
        final Edge p1 = a.addEdge("L", b);
        final Edge p2 = a.addEdge("L", b);
        final Edge p3 = b.addEdge("L", a);

        assertEquals(expectedTuples(
                List.of(a, p1, b),
                List.of(a, p2, b),
                List.of(a, p3, b)), tuples(execute("MATCH (a:P)-[r:L]-(b:Q)"), "a", "r", "b"));
        assertEquals(expectedTuples(
                List.of(a, List.of(p1), b),
                List.of(a, List.of(p2), b),
                List.of(a, List.of(p3), b)), tuples(execute("MATCH (a:P)-[r:L]-{1}(b:Q)"), "a", "r", "b"));
    }

    @Test(timeout = 10000)
    public void testUndirectedSelfLoopRowsSameWhenProviderReportsLoopOnce() {
        // A provider whose edges(BOTH) reports a self-loop once gets the same rows as TinkerGraph,
        // which reports it twice: nothing is lost by the dedup.
        final Vertex a = graph.addVertex("P");
        final Vertex b = graph.addVertex("P");
        final Edge s = a.addEdge("L", a);
        final Edge e = a.addEdge("L", b);

        // A separate probe for the check, so its unclosed iterators do not count against the match.
        assertEquals("the probe must report a's loop once under BOTH", 2, IteratorUtils.count(
                new ProbeGraph(graph).withSelfLoopsReportedOnce().graph().vertices(a.id()).next()
                        .edges(Direction.BOTH, "L")));
        assertEquals("TinkerGraph reports a's loop twice under BOTH", 3,
                IteratorUtils.count(a.edges(Direction.BOTH, "L")));

        final Map<List<Object>, Long> single = expectedTuples(
                List.of(a, s, a),
                List.of(a, e, b),
                List.of(b, e, a));
        final Map<List<Object>, Long> walk = expectedTuples(
                List.of(a, List.of(s), a),
                List.of(a, List.of(e), b),
                List.of(a, List.of(s, e), b),
                List.of(b, List.of(e), a),
                List.of(b, List.of(e, s), a));
        final ProbeGraph probe = new ProbeGraph(graph).withSelfLoopsReportedOnce();
        for (final Graph g : new Graph[]{graph, probe.graph()}) {
            final GqlMatchPlan singlePlan = new DefaultGqlPlanner(g).plan("MATCH (a:P)-[r:L]-(b:P)");
            assertEquals(single, tuples(materialize(new DefaultGqlExecutor(g).execute(singlePlan), singlePlan), "a", "r", "b"));
            final GqlMatchPlan walkPlan = new DefaultGqlPlanner(g).plan("MATCH (a:P)-[r:L]-{1,2}(b:P)");
            assertEquals(walk, tuples(materialize(new DefaultGqlExecutor(g).execute(walkPlan), walkPlan), "a", "r", "b"));
        }
        probe.assertAllClosedOnce("after the matches");
    }

    @Test(timeout = 10000)
    public void testUndirectedSelfLoopDedupedWhenProviderReportsIncidencesApart() {
        // A provider whose edges(BOTH) reports a self-loop's two incidences with other edges
        // between them gets the same rows: the dedup does not rely on the repeat being adjacent.
        // a carries loop s, e: a -> b and f: c -> a.
        final Vertex a = graph.addVertex("P");
        final Vertex b = graph.addVertex("P");
        final Vertex c = graph.addVertex("P");
        final Edge s = a.addEdge("L", a);
        final Edge e = a.addEdge("L", b);
        final Edge f = c.addEdge("L", a);

        final ProbeGraph probe = new ProbeGraph(graph).withSelfLoopsReportedAroundOtherEdges();
        // A separate probe for the check, so its unclosed iterators do not count against the match.
        assertEquals("the probe must report a's loop first and last, with e and f between", List.of(s, e, f, s),
                IteratorUtils.list(new ProbeGraph(graph).withSelfLoopsReportedAroundOtherEdges().graph()
                        .vertices(a.id()).next().edges(Direction.BOTH, "L")));

        final GqlMatchPlan singlePlan = new DefaultGqlPlanner(probe.graph()).plan("MATCH (a:P)-[r:L]-(b:P)");
        assertEquals(expectedTuples(
                List.of(a, s, a),
                List.of(a, e, b),
                List.of(a, f, c),
                List.of(b, e, a),
                List.of(c, f, a)),
                tuples(materialize(new DefaultGqlExecutor(probe.graph()).execute(singlePlan), singlePlan), "a", "r", "b"));

        final GqlMatchPlan walkPlan = new DefaultGqlPlanner(probe.graph()).plan("MATCH (a:P)-[r:L]-{1,2}(b:P)");
        assertEquals(expectedTuples(
                List.of(a, List.of(s), a),
                List.of(a, List.of(s, e), b),
                List.of(a, List.of(s, f), c),
                List.of(a, List.of(e), b),
                List.of(a, List.of(f), c),
                List.of(b, List.of(e), a),
                List.of(b, List.of(e, s), a),
                List.of(b, List.of(e, f), c),
                List.of(c, List.of(f), a),
                List.of(c, List.of(f, s), a),
                List.of(c, List.of(f, e), b)),
                tuples(materialize(new DefaultGqlExecutor(probe.graph()).execute(walkPlan), walkPlan), "a", "r", "b"));
        probe.assertAllClosedOnce("after the matches");
    }

    @Test(timeout = 10000)
    public void testUndirectedSelfLoopDedupFailsLoudlyWhenProviderThrowsWhileSkippingRepeat() {
        // a carries only loop s, which TinkerGraph's edges(BOTH) reports twice. The probe's
        // edges(BOTH) iterator for a throws from next() after both reports, so the failure comes
        // while the dedup is skipping s's repeat and reading on. It must propagate, a later
        // hasNext() or next() must fail rather than resume, and every provider iterator must be
        // closed once.
        final Vertex a = graph.addVertex("P");
        final Edge s = a.addEdge("L", a);
        final ProbeGraph probe = new ProbeGraph(graph).failNextBothEdgesAtEnd(a);
        final GqlMatchPlan plan = new DefaultGqlPlanner(probe.graph()).plan("MATCH (a:P)-[r:L]-(b:P)");
        final int aIdx = plan.getIndex("a");
        final int rIdx = plan.getIndex("r");
        final int bIdx = plan.getIndex("b");
        final Iterator<GqlRow> iter = new DefaultGqlExecutor(probe.graph()).execute(plan);

        assertTrue(iter.hasNext());
        final GqlRow row = iter.next();
        assertEquals(a, row.getScalar(aIdx));
        assertEquals(s, row.getScalar(rIdx));
        assertEquals(a, row.getScalar(bIdx));
        try {
            iter.hasNext();
            fail("the provider's next() exception must propagate");
        } catch (final ProbeGraph.InjectedFailure expected) {
            // expected
        }
        probe.assertAllClosedOnce("every provider iterator must be closed after the failure");

        for (int i = 0; i < 2; i++) {
            try {
                iter.hasNext();
                fail("hasNext() after a failure must not resume the search");
            } catch (final IllegalStateException expected) {
                assertTrue(expected.getCause() instanceof ProbeGraph.InjectedFailure);
            }
        }
        try {
            iter.next();
            fail("next() after a failure must not resume the search");
        } catch (final IllegalStateException expected) {
            assertTrue(expected.getCause() instanceof ProbeGraph.InjectedFailure);
        }
        probe.assertAllClosedOnce("no provider iterator may be opened or closed again after the failure");
    }

    @Test(timeout = 10000)
    public void testProviderIteratorsClosedUnderUndirectedSelfLoopDedup() {
        final Vertex a = graph.addVertex("P");
        final Vertex b = graph.addVertex("P");
        a.addEdge("L", a);
        a.addEdge("L", a);
        a.addEdge("L", b);
        final String query = "MATCH (x:P)-[:L]-(y:P)-[r:L]-{1,2}(z:P)";

        final ProbeGraph drained = new ProbeGraph(graph);
        final GqlMatchPlan plan = new DefaultGqlPlanner(drained.graph()).plan(query);
        assertFalse(materialize(new DefaultGqlExecutor(drained.graph()).execute(plan), plan).isEmpty());
        assertTrue(drained.opened() > 1);
        drained.assertAllClosedOnce("after the match");

        final ProbeGraph early = new ProbeGraph(graph);
        final GqlMatchPlan earlyPlan = new DefaultGqlPlanner(early.graph()).plan(query);
        final Iterator<GqlRow> iter = new DefaultGqlExecutor(early.graph()).execute(earlyPlan);
        assertTrue(iter.hasNext());
        iter.next();
        assertTrue("the search must hold provider iterators open mid-way", early.opened() > early.closed());
        CloseableIterator.closeIterator(iter);
        early.assertAllClosedOnce("after closing early");
    }

    // -------------------------------------------------------------------------
    // Quantified edges combined with other edges
    // -------------------------------------------------------------------------

    @Test(timeout = 10000)
    public void testTwoQuantifiedEdgesMayShareAnEdgeAcrossWalks() {
        // Edge uniqueness is per walk, so the same edge may appear in both r1 and r2.
        // Graph: a -L-> b -L-> c -L-> a. (x)-[r1]->{1,3}(m)-[r2]->{1,3}(y) seeded at a.
        final Vertex a = graph.addVertex("Start");
        final Vertex b = graph.addVertex("Node");
        final Vertex c = graph.addVertex("Node");
        final Edge ab = a.addEdge("L", b);
        final Edge bc = b.addEdge("L", c);
        final Edge ca = c.addEdge("L", a);

        final GqlMatchPlan plan = planner.plan(
                "MATCH (x:Start)-[r1:L]->{1,3}(m)-[r2:L]->{1,3}(y:Start)");
        assertEquals("x", plan.getSeedVariable());
        final List<Map<String, Element>> rows = materialize(executor.execute(plan), plan);

        final Set<List<List<Edge>>> pairs = rows.stream()
                .map(row -> List.of(edgeList(row, "r1"), edgeList(row, "r2")))
                .collect(Collectors.toSet());
        // r1 ends at m, r2 continues from m back to the Start vertex a.
        assertEquals(Set.of(
                List.of(List.of(ab), List.of(bc, ca)),
                List.of(List.of(ab, bc), List.of(ca)),
                // r1 covers the full cycle back to a, then r2 reuses all of its edges.
                List.of(List.of(ab, bc, ca), List.of(ab, bc, ca))), pairs);
        assertEquals(3, rows.size());
    }

    @Test(timeout = 10000)
    public void testQuantifiedEdgeFollowedByPlainEdge() {
        // a -KNOWS-> b -KNOWS-> c, b -WORKS-> co1, c -WORKS-> co2
        final Vertex a = graph.addVertex("Person");
        a.property("n", "a");
        final Vertex b = graph.addVertex("Person");
        final Vertex c = graph.addVertex("Person");
        final Vertex co1 = graph.addVertex("Company");
        final Vertex co2 = graph.addVertex("Company");
        final Edge ab = a.addEdge("KNOWS", b);
        final Edge bc = b.addEdge("KNOWS", c);
        final Edge w1 = b.addEdge("WORKS", co1);
        final Edge w2 = c.addEdge("WORKS", co2);

        final List<Map<String, Element>> rows =
                execute("MATCH (a:Person {n: 'a'})-[r:KNOWS]->{1,2}(p:Person)-[w:WORKS]->(co:Company)");

        assertEquals(2, rows.size());
        final Map<Element, List<Map<String, Element>>> byP = byTarget(rows, "p");
        assertEquals(Set.of(b, c), byP.keySet());
        assertEquals(List.of(ab), edgeList(byP.get(b).get(0), "r"));
        assertEquals(w1, byP.get(b).get(0).get("w"));
        assertEquals(co1, byP.get(b).get(0).get("co"));
        assertEquals(List.of(ab, bc), edgeList(byP.get(c).get(0), "r"));
        assertEquals(w2, byP.get(c).get(0).get("w"));
        assertEquals(co2, byP.get(c).get(0).get("co"));
    }

    @Test(timeout = 10000)
    public void testPlainEdgeFollowedByQuantifiedEdge() {
        // co <-WORKS- a -KNOWS-> b -KNOWS-> c
        final Vertex co = graph.addVertex("Company");
        final Vertex a = graph.addVertex("Person");
        final Vertex b = graph.addVertex("Person");
        final Vertex c = graph.addVertex("Person");
        final Edge w = a.addEdge("WORKS", co);
        final Edge ab = a.addEdge("KNOWS", b);
        final Edge bc = b.addEdge("KNOWS", c);

        final GqlMatchPlan plan = planner.plan(
                "MATCH (co:Company)<-[w:WORKS]-(a:Person)-[r:KNOWS]->{1,2}(x:Person)");
        assertEquals("co", plan.getSeedVariable());
        final List<Map<String, Element>> rows = materialize(executor.execute(plan), plan);

        assertEquals(2, rows.size());
        final Map<Element, List<Map<String, Element>>> byX = byTarget(rows, "x");
        assertEquals(Set.of(b, c), byX.keySet());
        for (final Map<String, Element> row : rows) {
            assertEquals(a, row.get("a"));
            assertEquals(w, row.get("w"));
        }
        assertEquals(List.of(ab), edgeList(byX.get(b).get(0), "r"));
        assertEquals(List.of(ab, bc), edgeList(byX.get(c).get(0), "r"));
    }

    @Test(timeout = 10000)
    public void testQuantifiedEdgePropertyFilterCutsPathPartway() {
        // a -ok-> b -ok-> c -blocked-> d -ok-> e : the walk stops at the first edge that fails the
        // filter, so d and e are unreachable even though the edges after the cut pass it.
        final Vertex a = graph.addVertex("Person");
        a.property("n", "a");
        final Vertex b = graph.addVertex("Person");
        final Vertex c = graph.addVertex("Person");
        final Vertex d = graph.addVertex("Person");
        final Vertex e = graph.addVertex("Person");
        final Edge ab = a.addEdge("KNOWS", b, "ok", true);
        final Edge bc = b.addEdge("KNOWS", c, "ok", true);
        c.addEdge("KNOWS", d, "ok", false);
        d.addEdge("KNOWS", e, "ok", true);

        final List<Map<String, Element>> rows =
                execute("MATCH (a:Person {n: 'a'})-[r:KNOWS {ok: true}]->+(x:Person)");

        assertEquals(2, rows.size());
        final Map<Element, List<Map<String, Element>>> byX = byTarget(rows, "x");
        assertEquals(Set.of(b, c), byX.keySet());
        assertEquals(List.of(ab), edgeList(byX.get(b).get(0), "r"));
        assertEquals(List.of(ab, bc), edgeList(byX.get(c).get(0), "r"));
    }

    @Test(timeout = 10000)
    public void testQuantifiedWalkPassesThroughVerticesNotMatchingTargetLabel() {
        // a -L-> h1:Hub -L-> p:Person -L-> h2:Hub -L-> q:Person. Hubs are intermediate vertices
        // the walk passes through, but only Person targets are emitted.
        final Vertex a = graph.addVertex("Person");
        a.property("n", "a");
        final Vertex h1 = graph.addVertex("Hub");
        final Vertex p = graph.addVertex("Person");
        final Vertex h2 = graph.addVertex("Hub");
        final Vertex q = graph.addVertex("Person");
        final Edge e1 = a.addEdge("L", h1);
        final Edge e2 = h1.addEdge("L", p);
        final Edge e3 = p.addEdge("L", h2);
        final Edge e4 = h2.addEdge("L", q);

        final List<Map<String, Element>> rows =
                execute("MATCH (a:Person {n: 'a'})-[r:L]->{1,4}(x:Person)");

        assertEquals(2, rows.size());
        final Map<Element, List<Map<String, Element>>> byX = byTarget(rows, "x");
        assertEquals(Set.of(p, q), byX.keySet());
        assertEquals(List.of(e1, e2), edgeList(byX.get(p).get(0), "r"));
        assertEquals(List.of(e1, e2, e3, e4), edgeList(byX.get(q).get(0), "r"));
    }

    // -------------------------------------------------------------------------
    // Quantified edges that close a pattern cycle
    //
    // A quantified back edge, including a pattern self-loop, runs as an ordinary walk from its
    // anchor. Each walk end is joined against the already-bound target, so only walks that end
    // at the already-matched vertex produce rows.
    // -------------------------------------------------------------------------

    private static final String TRIANGLE = "MATCH (a:A)-[:AB]->(b:B)-[r:BC]->{1,3}(c:C)-[:CA]->(a)";

    /**
     * Projects rows onto the named variables, reading group variables as their edge lists, and
     * counts each distinct tuple so duplicate rows are not hidden.
     */
    private static Map<List<Object>, Long> tuples(final List<Map<String, Element>> rows, final String... vars) {
        return rows.stream().map(row -> {
            final List<Object> tuple = new ArrayList<>();
            for (final String var : vars) {
                final List<Edge> group = ((Row) row).groups.get(var);
                tuple.add(group != null ? group : row.get(var));
            }
            return tuple;
        }).collect(Collectors.groupingBy(t -> t, LinkedHashMap::new, Collectors.counting()));
    }

    /** Counts each expected tuple, so a tuple listed twice expects two identical rows. */
    @SafeVarargs
    private static Map<List<Object>, Long> expectedTuples(final List<Object>... tuples) {
        return Arrays.stream(tuples)
                .collect(Collectors.groupingBy(t -> t, LinkedHashMap::new, Collectors.counting()));
    }

    /**
     * Whether the step's target variable is already bound by the seed or an earlier step in the
     * plan. This is a plan-level property: the executor may reorder steps at runtime, so a planned
     * back edge is not guaranteed to run with its target bound.
     */
    private static boolean isBackEdge(final GqlMatchPlan plan, final ExtensionStep step) {
        final Set<String> bound = new HashSet<>();
        bound.add(plan.getSeedVariable());
        for (final ExtensionStep s : plan.getSteps()) {
            if (s == step) return bound.contains(s.getTargetVariable());
            bound.add(s.getTargetVariable());
        }
        throw new IllegalArgumentException(step + " is not in the plan");
    }

    private static ExtensionStep stepByLabel(final GqlMatchPlan plan, final String label) {
        return plan.getSteps().stream()
                .filter(s -> label.equals(s.getEdgeLabel())).findFirst().orElseThrow();
    }

    /** Adds {@code count} edges with {@code label} between isolated X vertices to skew label density. */
    private static void addUnreachableEdges(final TinkerGraph g, final String label, final int count) {
        for (int i = 0; i < count; i++) g.addVertex("X").addEdge(label, g.addVertex("X"));
    }

    /**
     * a -AB-> b -BC-> c -CA-> a, where b reaches c by BC at depths 1, 2 and 3 through the M
     * vertices, and also reaches a decoy C vertex that has no CA edge back to a.
     */
    private static final class TriangleGraph {
        final Vertex a, b, c;
        final Edge bc, bm, mc, bn, nm;

        TriangleGraph(final TinkerGraph g) {
            a = g.addVertex("A");
            b = g.addVertex("B");
            c = g.addVertex("C");
            final Vertex decoy = g.addVertex("C");
            final Vertex m = g.addVertex("M");
            final Vertex n = g.addVertex("M");
            a.addEdge("AB", b);
            bc = b.addEdge("BC", c);
            bm = b.addEdge("BC", m);
            mc = m.addEdge("BC", c);
            bn = b.addEdge("BC", n);
            nm = n.addEdge("BC", m);
            b.addEdge("BC", decoy);
            c.addEdge("CA", a);
        }

        Map<List<Object>, Long> expected() {
            return expectedTuples(
                    List.of(a, b, c, List.of(bc)),
                    List.of(a, b, c, List.of(bm, mc)),
                    List.of(a, b, c, List.of(bn, nm, mc)));
        }
    }

    @Test(timeout = 10000)
    public void testQuantifiedEdgeClosingTriangleReturnsOnlyWalksEndingAtBoundTarget() {
        final TriangleGraph t = new TriangleGraph(graph);

        final GqlMatchPlan plan = planner.plan(TRIANGLE);
        assertEquals("a", plan.getSeedVariable());
        final ExtensionStep bc = stepByLabel(plan, "BC");
        assertTrue("BC must close the cycle for this test", isBackEdge(plan, bc));
        assertTrue(bc.isQuantified());

        assertEquals(t.expected(), tuples(materialize(executor.execute(plan), plan), "a", "b", "c", "r"));
    }

    @Test(timeout = 30000)
    public void testQuantifiedCycleRowsDoNotDependOnPatternOrEdgeOrder() {
        // The same triangle written with the quantified edge in each of the three positions, run
        // on graphs whose label counts move the seed and reorder the edges. The quantified edge is
        // a back edge in some of these plans and a forward edge in others, and the rows must not
        // change, whichever way the plan orders it.
        final String[] patterns = {
                TRIANGLE,
                "MATCH (b:B)-[r:BC]->{1,3}(c:C)-[:CA]->(a:A)-[:AB]->(b)",
                "MATCH (c:C)-[:CA]->(a:A)-[:AB]->(b:B)-[r:BC]->{1,3}(c)"};
        final Map<String, Consumer<TinkerGraph>> skews = new LinkedHashMap<>();
        skews.put("base", g -> { });
        skews.put("seed at b", g -> { for (int i = 0; i < 5; i++) g.addVertex("A"); });
        skews.put("seed at c", g -> { for (int i = 0; i < 5; i++) { g.addVertex("A"); g.addVertex("B"); } });
        skews.put("dense AB", g -> addUnreachableEdges(g, "AB", 10));
        skews.put("dense CA", g -> addUnreachableEdges(g, "CA", 10));
        skews.put("dense BC", g -> addUnreachableEdges(g, "BC", 10));

        final Map<String, String> actualShapes = new LinkedHashMap<>();
        for (int p = 0; p < patterns.length; p++) {
            for (final Map.Entry<String, Consumer<TinkerGraph>> skew : skews.entrySet()) {
                try (final TinkerGraph g = TinkerGraph.open()) {
                    final TriangleGraph t = new TriangleGraph(g);
                    skew.getValue().accept(g);
                    final GqlMatchPlan plan = new DefaultGqlPlanner(g).plan(patterns[p]);
                    final ExtensionStep bc = stepByLabel(plan, "BC");
                    actualShapes.put("p" + p + " / " + skew.getKey(), "seed=" + plan.getSeedVariable()
                            + " back=" + isBackEdge(plan, bc) + " anchor=" + bc.getAnchorVariable());
                    final List<Map<String, Element>> rows =
                            materialize(new DefaultGqlExecutor(g).execute(plan), plan);
                    assertEquals(patterns[p] + " / " + skew.getKey(),
                            t.expected(), tuples(rows, "a", "b", "c", "r"));
                }
            }
        }
        // The plan each case must produce: the seed, whether BC closes the cycle, and the vertex BC
        // is walked from. Equal label counts keep the first-written variable as the seed; the
        // "seed at" skews add A (and B) vertices to move it; the "dense" skews reorder the steps
        // around the seed. p1 writes BC first from b, so it never plans BC as a back edge.
        final Map<String, String> expectedShapes = new LinkedHashMap<>();
        expectedShapes.put("p0 / base", "seed=a back=true anchor=b");
        expectedShapes.put("p0 / seed at b", "seed=b back=false anchor=b");
        expectedShapes.put("p0 / seed at c", "seed=c back=false anchor=c");
        expectedShapes.put("p0 / dense AB", "seed=a back=true anchor=c");
        expectedShapes.put("p0 / dense CA", "seed=a back=true anchor=b");
        expectedShapes.put("p0 / dense BC", "seed=a back=true anchor=b");
        expectedShapes.put("p1 / base", "seed=b back=false anchor=b");
        expectedShapes.put("p1 / seed at b", "seed=b back=false anchor=b");
        expectedShapes.put("p1 / seed at c", "seed=c back=false anchor=c");
        expectedShapes.put("p1 / dense AB", "seed=b back=false anchor=b");
        expectedShapes.put("p1 / dense CA", "seed=b back=false anchor=b");
        expectedShapes.put("p1 / dense BC", "seed=b back=false anchor=b");
        expectedShapes.put("p2 / base", "seed=a back=true anchor=c");
        expectedShapes.put("p2 / seed at b", "seed=b back=false anchor=b");
        expectedShapes.put("p2 / seed at c", "seed=c back=false anchor=c");
        expectedShapes.put("p2 / dense AB", "seed=a back=true anchor=c");
        expectedShapes.put("p2 / dense CA", "seed=a back=true anchor=b");
        expectedShapes.put("p2 / dense BC", "seed=a back=true anchor=c");
        assertEquals(expectedShapes, actualShapes);
        assertEquals("the matrix must plan BC both as a back edge and as a forward edge",
                Set.of(true, false), actualShapes.values().stream()
                        .map(shape -> shape.contains("back=true")).collect(Collectors.toSet()));
    }

    @Test(timeout = 10000)
    public void testQuantifiedEdgeWithBothEndpointsPinnedByProperties() {
        // x -L-> y, x -L-> m -L-> y, x -L-> z, y -L-> p -L-> y. Only walks ending at y are rows,
        // including the depth-3 walk that passes through y and returns to it.
        final Vertex x = graph.addVertex("V");
        x.property("name", "x");
        final Vertex y = graph.addVertex("V");
        y.property("name", "y");
        final Vertex z = graph.addVertex("V");
        z.property("name", "z");
        final Vertex m = graph.addVertex("V");
        final Vertex p = graph.addVertex("V");
        final Edge xy = x.addEdge("L", y);
        final Edge xm = x.addEdge("L", m);
        final Edge my = m.addEdge("L", y);
        x.addEdge("L", z);
        final Edge yp = y.addEdge("L", p);
        final Edge py = p.addEdge("L", y);

        final List<Map<String, Element>> rows =
                execute("MATCH (a {name: 'x'})-[r:L]->{1,3}(b {name: 'y'})");
        assertEquals(expectedTuples(
                List.of(x, y, List.of(xy)),
                List.of(x, y, List.of(xm, my)),
                List.of(x, y, List.of(xy, yp, py))), tuples(rows, "a", "b", "r"));
    }

    /**
     * a -L-> b -L-> a (a 2-cycle) and a -L-> c -L-> d -L-> a (a 3-cycle), all labelled P.
     * Returns {a, b, c, d, ab, ba, ac, cd, da}.
     */
    private Element[] addTwoAndThreeCycleThroughA() {
        final Vertex a = graph.addVertex("P");
        final Vertex b = graph.addVertex("P");
        final Vertex c = graph.addVertex("P");
        final Vertex d = graph.addVertex("P");
        final Edge ab = a.addEdge("L", b);
        final Edge ba = b.addEdge("L", a);
        final Edge ac = a.addEdge("L", c);
        final Edge cd = c.addEdge("L", d);
        final Edge da = d.addEdge("L", a);
        return new Element[]{a, b, c, d, ab, ba, ac, cd, da};
    }

    @Test(timeout = 10000)
    public void testQuantifiedPatternSelfLoopReturnsClosedWalksInPatternOrder() {
        final Element[] e = addTwoAndThreeCycleThroughA();
        final Element a = e[0], b = e[1], c = e[2], d = e[3];
        final Edge ab = (Edge) e[4], ba = (Edge) e[5], ac = (Edge) e[6], cd = (Edge) e[7], da = (Edge) e[8];

        final GqlMatchPlan plan = planner.plan("MATCH (a:P)-[r:L]->{2,3}(a)");
        assertFalse(plan.getSteps().get(0).isAnchoredAtPatternTarget());
        // Every vertex on a cycle is a seed, so each closed walk appears once per start vertex.
        assertEquals(expectedTuples(
                List.of(a, List.of(ab, ba)),
                List.of(a, List.of(ac, cd, da)),
                List.of(b, List.of(ba, ab)),
                List.of(c, List.of(cd, da, ac)),
                List.of(d, List.of(da, ac, cd))), tuples(materialize(executor.execute(plan), plan), "a", "r"));
    }

    @Test(timeout = 10000)
    public void testReverseQuantifiedPatternSelfLoopReturnsClosedWalksInPatternOrder() {
        // (a)<-[r]-{2,3}(a) reads right to left against the edges, so r lists edges in the order
        // the walk follows them backwards from a.
        final Element[] e = addTwoAndThreeCycleThroughA();
        final Element a = e[0], b = e[1], c = e[2], d = e[3];
        final Edge ab = (Edge) e[4], ba = (Edge) e[5], ac = (Edge) e[6], cd = (Edge) e[7], da = (Edge) e[8];

        assertEquals(expectedTuples(
                List.of(a, List.of(ba, ab)),
                List.of(a, List.of(da, cd, ac)),
                List.of(b, List.of(ab, ba)),
                List.of(c, List.of(ac, da, cd)),
                List.of(d, List.of(cd, ac, da))), tuples(execute("MATCH (a:P)<-[r:L]-{2,3}(a)"), "a", "r"));
    }

    @Test(timeout = 10000)
    public void testUndirectedQuantifiedPatternSelfLoopReturnsClosedWalksInBothOrientations() {
        // Ignoring direction, a and b share the two edges ab and ba, and a, c and d form a
        // triangle. Each closed trail is a row once per start vertex and once per orientation:
        // a and b close through the two a-b edges in either order, and every triangle vertex goes
        // round the triangle both ways. No other vertex shares two edges with a neighbour, so
        // there are no other length-2 trails, and b has no length-3 trail.
        final Element[] e = addTwoAndThreeCycleThroughA();
        final Element a = e[0], b = e[1], c = e[2], d = e[3];
        final Edge ab = (Edge) e[4], ba = (Edge) e[5], ac = (Edge) e[6], cd = (Edge) e[7], da = (Edge) e[8];

        final GqlMatchPlan plan = planner.plan("MATCH (a:P)-[r:L]-{2,3}(a)");
        assertFalse(plan.getSteps().get(0).isAnchoredAtPatternTarget());
        assertEquals(expectedTuples(
                List.of(a, List.of(ab, ba)),
                List.of(a, List.of(ba, ab)),
                List.of(a, List.of(ac, cd, da)),
                List.of(a, List.of(da, cd, ac)),
                List.of(b, List.of(ab, ba)),
                List.of(b, List.of(ba, ab)),
                List.of(c, List.of(cd, da, ac)),
                List.of(c, List.of(ac, da, cd)),
                List.of(d, List.of(da, ac, cd)),
                List.of(d, List.of(cd, ac, da))), tuples(materialize(executor.execute(plan), plan), "a", "r"));
    }

    @Test(timeout = 10000)
    public void testUnboundedQuantifiedPatternSelfLoopReturnsEveryClosedTrail() {
        // Directed closed trails from each vertex: the cycle it lies on, and the length-5 trail
        // that also goes round the other cycle when it passes through a. a lies on both cycles,
        // so it has both cycles and both length-5 orders. Every edge is then used, so no trail is
        // longer. The graph has no data self-loop, so there is no length-1 trail and + and {2,}
        // return the same rows.
        final Element[] e = addTwoAndThreeCycleThroughA();
        final Element a = e[0], b = e[1], c = e[2], d = e[3];
        final Edge ab = (Edge) e[4], ba = (Edge) e[5], ac = (Edge) e[6], cd = (Edge) e[7], da = (Edge) e[8];

        final Map<List<Object>, Long> expected = expectedTuples(
                List.of(a, List.of(ab, ba)),
                List.of(a, List.of(ab, ba, ac, cd, da)),
                List.of(a, List.of(ac, cd, da)),
                List.of(a, List.of(ac, cd, da, ab, ba)),
                List.of(b, List.of(ba, ab)),
                List.of(b, List.of(ba, ac, cd, da, ab)),
                List.of(c, List.of(cd, da, ac)),
                List.of(c, List.of(cd, da, ab, ba, ac)),
                List.of(d, List.of(da, ac, cd)),
                List.of(d, List.of(da, ab, ba, ac, cd)));
        for (final String q : new String[]{"+", "{2,}"}) {
            assertEquals(q, expected, tuples(execute("MATCH (a:P)-[r:L]->" + q + "(a)"), "a", "r"));
        }
    }

    @Test(timeout = 10000)
    public void testExactlyOneQuantifierClosingCycleReturnsOneRowPerParallelEdge() {
        // Two parallel BC edges close the triangle. {1} and {1,1} are quantified walks, so each
        // parallel edge is its own row, named or anonymous.
        final Vertex a = graph.addVertex("A");
        final Vertex b = graph.addVertex("B");
        final Vertex c = graph.addVertex("C");
        a.addEdge("AB", b);
        final Edge p1 = b.addEdge("BC", c);
        final Edge p2 = b.addEdge("BC", c);
        c.addEdge("CA", a);

        for (final String q : new String[]{"{1}", "{1,1}"}) {
            final GqlMatchPlan plan = planner.plan("MATCH (a:A)-[:AB]->(b:B)-[r:BC]->" + q + "(c:C)-[:CA]->(a)");
            assertTrue(q, isBackEdge(plan, stepByLabel(plan, "BC")));
            assertEquals(q, expectedTuples(
                    List.of(a, b, c, List.of(p1)),
                    List.of(a, b, c, List.of(p2))), tuples(materialize(executor.execute(plan), plan), "a", "b", "c", "r"));

            assertEquals(q, expectedTuples(List.of(a, b, c), List.of(a, b, c)),
                    tuples(execute("MATCH (a:A)-[:AB]->(b:B)-[:BC]->" + q + "(c:C)-[:CA]->(a)"), "a", "b", "c"));
        }
    }

    @Test(timeout = 10000)
    public void testExactlyOneQuantifiedPatternSelfLoopReturnsOneRowPerParallelDataSelfLoop() {
        final Vertex a = graph.addVertex("P");
        final Edge s1 = a.addEdge("L", a);
        final Edge s2 = a.addEdge("L", a);
        graph.addVertex("P");

        for (final String q : new String[]{"{1}", "{1,1}"}) {
            assertEquals(q, expectedTuples(List.of(a, List.of(s1)), List.of(a, List.of(s2))),
                    tuples(execute("MATCH (a:P)-[r:L]->" + q + "(a)"), "a", "r"));
        }
    }

    /**
     * Triangle a -AB-> b ... c -CA-> a with extra unreachable AB edges, so the planner processes CA
     * before AB from the seed a, visits c before b, and anchors the quantified b-c edge at c, the
     * pattern edge's target.
     */
    private void assertQuantifiedBackEdgeAnchoredAtPatternTarget(final String query, final Direction direction,
                                                                 final Map<List<Object>, Long> expected) {
        final GqlMatchPlan plan = planner.plan(query);
        assertEquals("a", plan.getSeedVariable());
        final ExtensionStep step = plan.getSteps().stream().filter(ExtensionStep::isQuantified).findFirst().orElseThrow();
        assertTrue(isBackEdge(plan, step));
        assertEquals("c", step.getAnchorVariable());
        assertEquals("b", step.getTargetVariable());
        assertEquals(direction, step.getDirection());
        assertTrue(step.isAnchoredAtPatternTarget());

        assertEquals(expected, tuples(materialize(executor.execute(plan), plan), "a", "b", "c", "r"));
    }

    @Test(timeout = 10000)
    public void testQuantifiedBackEdgeAnchoredAtPatternTargetBindsGroupInPatternOrder() {
        final Vertex a = graph.addVertex("A");
        final Vertex b = graph.addVertex("B");
        final Vertex c = graph.addVertex("C");
        final Vertex m = graph.addVertex("M");
        a.addEdge("AB", b);
        final Edge bm = b.addEdge("BC", m);
        final Edge mc = m.addEdge("BC", c);
        final Edge bc = b.addEdge("BC", c);
        c.addEdge("CA", a);
        addUnreachableEdges(graph, "AB", 3);

        assertQuantifiedBackEdgeAnchoredAtPatternTarget(TRIANGLE, Direction.IN, expectedTuples(
                List.of(a, b, c, List.of(bc)),
                List.of(a, b, c, List.of(bm, mc))));
    }

    @Test(timeout = 10000)
    public void testReverseQuantifiedBackEdgeAnchoredAtPatternTargetBindsGroupInPatternOrder() {
        // Data edges run c -> m -> b; the pattern reads them from b, so r lists the b edge first.
        final Vertex a = graph.addVertex("A");
        final Vertex b = graph.addVertex("B");
        final Vertex c = graph.addVertex("C");
        final Vertex m = graph.addVertex("M");
        a.addEdge("AB", b);
        final Edge cm = c.addEdge("CB", m);
        final Edge mb = m.addEdge("CB", b);
        final Edge cb = c.addEdge("CB", b);
        c.addEdge("CA", a);
        addUnreachableEdges(graph, "AB", 3);

        assertQuantifiedBackEdgeAnchoredAtPatternTarget(
                "MATCH (a:A)-[:AB]->(b:B)<-[r:CB]-{1,3}(c:C)-[:CA]->(a)", Direction.OUT, expectedTuples(
                        List.of(a, b, c, List.of(cb)),
                        List.of(a, b, c, List.of(mb, cm))));
    }

    @Test(timeout = 10000)
    public void testUndirectedQuantifiedBackEdgeAnchoredAtPatternTargetBindsGroupInPatternOrder() {
        final Vertex a = graph.addVertex("A");
        final Vertex b = graph.addVertex("B");
        final Vertex c = graph.addVertex("C");
        final Vertex m = graph.addVertex("M");
        a.addEdge("AB", b);
        final Edge bm = b.addEdge("BC", m);
        final Edge mc = m.addEdge("BC", c);
        final Edge bc = b.addEdge("BC", c);
        c.addEdge("CA", a);
        addUnreachableEdges(graph, "AB", 3);

        // Undirected walks from c of length 1-3 that end at b: c-b and c-m-b. The length-3 walks
        // c-b-m-c and c-m-b-c end at c, so they are not rows.
        assertQuantifiedBackEdgeAnchoredAtPatternTarget(
                "MATCH (a:A)-[:AB]->(b:B)-[r:BC]-{1,3}(c:C)-[:CA]->(a)", Direction.BOTH, expectedTuples(
                        List.of(a, b, c, List.of(bc)),
                        List.of(a, b, c, List.of(bm, mc))));
    }

    @Test(timeout = 10000)
    public void testUnboundedQuantifiedEdgeWithBothEndpointsBoundTerminatesOnCycle() {
        // L cycle x -> y -> z -> x plus a parallel x -> y edge, and M edges y -> x and z -> y.
        // The plain M step binds b first (M is rarer than L), so the + walk from a joins against b.
        final Vertex x = graph.addVertex("V");
        final Vertex y = graph.addVertex("V");
        final Vertex z = graph.addVertex("V");
        final Edge l1 = x.addEdge("L", y);
        final Edge l2 = y.addEdge("L", z);
        final Edge l3 = z.addEdge("L", x);
        final Edge l4 = x.addEdge("L", y);
        y.addEdge("M", x);
        z.addEdge("M", y);

        final GqlMatchPlan plan = planner.plan("MATCH (a)-[r:L]->+(b), (b)-[:M]->(a)");
        assertTrue(isBackEdge(plan, stepByLabel(plan, "L")));

        // a = x, b = y: both one-hop edges, and both trails that go round the cycle and leave x
        // again by the other parallel edge. a = y, b = z: only the one-hop edge, because the
        // trails back to y have used y's only outgoing L edge.
        assertEquals(expectedTuples(
                List.of(x, y, List.of(l1)),
                List.of(x, y, List.of(l4)),
                List.of(x, y, List.of(l1, l2, l3, l4)),
                List.of(x, y, List.of(l4, l2, l3, l1)),
                List.of(y, z, List.of(l2))), tuples(materialize(executor.execute(plan), plan), "a", "b", "r"));
    }

    @Test(timeout = 10000)
    public void testTwoQuantifiedEdgesFormingTwoCycleMayShareEdges() {
        // u -K-> v -K-> u. Edge uniqueness is per walk, so r1 and r2 may share edges.
        final Vertex u = graph.addVertex("V");
        final Vertex v = graph.addVertex("V");
        final Edge uv = u.addEdge("K", v);
        final Edge vu = v.addEdge("K", u);

        final GqlMatchPlan plan = planner.plan("MATCH (a)-[r1:K]->{1,3}(b), (b)-[r2:K]->{1,3}(a)");
        final ExtensionStep r2 = plan.getSteps().stream()
                .filter(s -> "r2".equals(s.getEdgeVariable())).findFirst().orElseThrow();
        assertTrue(isBackEdge(plan, r2));

        // r1 and r2 are both anchored at a, and the executor reorders them by observed
        // selectivity once the first seed has updated the step counters. So r2 is not guaranteed
        // to run with b already bound for every seed; for some seeds r2 may run first and r1
        // then joins against the b that r2 bound. The rows are the same either way.
        // testTwoQuantifiedEdgesFormingTwoCycleJoinsR2AgainstBoundTarget covers the r2 join alone.
        final List<Map<String, Element>> rows = materialize(executor.execute(plan), plan);
        assertEquals(expectedTuples(
                List.of(u, v, List.of(uv), List.of(vu)),
                List.of(u, u, List.of(uv, vu), List.of(uv, vu)),
                List.of(v, u, List.of(vu), List.of(uv)),
                List.of(v, v, List.of(vu, uv), List.of(vu, uv))), tuples(rows, "a", "b", "r1", "r2"));
        assertTrue("some row must reuse an edge across r1 and r2", rows.stream().anyMatch(row ->
                edgeList(row, "r1").stream().anyMatch(edgeList(row, "r2")::contains)));
    }

    @Test(timeout = 10000)
    public void testTwoQuantifiedEdgesFormingTwoCycleJoinsR2AgainstBoundTarget() {
        // Same 2-cycle, with a pinned to u so there is a single seed. On a fresh plan both steps
        // have equal selectivity and cost, so the executor keeps plan order: r1 runs first and
        // binds b, and r2 runs as the back edge, joining each walk's end vertex against that b.
        final Vertex u = graph.addVertex("V");
        u.property("name", "u");
        final Vertex v = graph.addVertex("V");
        v.property("name", "v");
        final Edge uv = u.addEdge("K", v);
        final Edge vu = v.addEdge("K", u);

        final GqlMatchPlan plan = planner.plan("MATCH (a {name: 'u'})-[r1:K]->{1,3}(b), (b)-[r2:K]->{1,3}(a)");
        assertEquals("a", plan.getSeedVariable());
        assertEquals("r1", plan.getSteps().get(0).getEdgeVariable());
        final ExtensionStep r2 = plan.getSteps().get(1);
        assertEquals("r2", r2.getEdgeVariable());
        assertTrue(isBackEdge(plan, r2));
        assertEquals(r2.getEstimatedCost(), plan.getSteps().get(0).getEstimatedCost());

        assertEquals(expectedTuples(
                List.of(u, v, List.of(uv), List.of(vu)),
                List.of(u, u, List.of(uv, vu), List.of(uv, vu))),
                tuples(materialize(executor.execute(plan), plan), "a", "b", "r1", "r2"));
    }

    /** Drains the iterator, calling {@code hasNext()} {@code hasNextCalls} times before each row. */
    private static List<List<Object>> drain(final Iterator<GqlRow> iter, final GqlMatchPlan plan,
                                            final int hasNextCalls) {
        final int a = plan.getIndex("a"), b = plan.getIndex("b"), r = plan.getIndex("r");
        final List<List<Object>> rows = new ArrayList<>();
        while (true) {
            boolean more = false;
            for (int i = 0; i < hasNextCalls; i++) more = iter.hasNext();
            if (!more) return rows;
            final GqlRow row = iter.next();
            rows.add(List.of(row.getScalar(a), row.getScalar(b), row.getGroup(r)));
        }
    }

    @Test(timeout = 10000)
    public void testBoundTargetIsRestoredAcrossSiblingWalkRows() {
        // A plain M step binds b, then the L walk from a emits at several depths. Each sibling row
        // must still join against that b, including after walks that ended elsewhere.
        final Vertex a = graph.addVertex("A");
        final Vertex b1 = graph.addVertex("B");
        final Vertex b2 = graph.addVertex("B");
        a.addEdge("M", b1);
        a.addEdge("M", b2);
        final Edge l1 = a.addEdge("L", b1);
        final Edge l2 = b1.addEdge("L", b2);
        final Edge l3 = b2.addEdge("L", b1);
        final Edge l4 = a.addEdge("L", b2);

        final String query = "MATCH (a:A)-[:M]->(b:B), (a)-[r:L]->{1,3}(b)";
        final GqlMatchPlan plan = planner.plan(query);
        assertTrue(isBackEdge(plan, stepByLabel(plan, "L")));

        // Fresh plans, so the adaptive step counters start equal and the row order is comparable.
        final List<List<Object>> once = drain(executor.execute(planner.plan(query)), plan, 1);
        final List<List<Object>> twice = drain(executor.execute(planner.plan(query)), plan, 2);
        assertEquals("calling hasNext() twice between rows must not change the sequence", once, twice);
        assertEquals(expectedTuples(
                List.of(a, b1, List.of(l1)),
                List.of(a, b1, List.of(l1, l2, l3)),
                List.of(a, b1, List.of(l4, l3)),
                List.of(a, b2, List.of(l1, l2)),
                List.of(a, b2, List.of(l4)),
                List.of(a, b2, List.of(l4, l3, l2))),
                once.stream().collect(Collectors.groupingBy(t -> t, LinkedHashMap::new, Collectors.counting())));
    }

    @Test(timeout = 10000)
    public void testUndirectedQuantifiedPatternSelfLoopOverDataSelfLoop() {
        // TinkerGraph's edges(Direction.BOTH) reports a self-loop once as an out-edge and once as
        // an in-edge, but an undirected walk takes the loop edge once, as the single-hop step
        // does, so there is one row. TRAIL uniqueness prevents a depth-2 row.
        final Vertex a = graph.addVertex("P");
        final Edge loop = a.addEdge("L", a);
        graph.addVertex("P");

        assertEquals(expectedTuples(List.of(a, List.of(loop))),
                tuples(execute("MATCH (a:P)-[r:L]-{1,2}(a)"), "a", "r"));
        // A directed walk also sees the loop once.
        assertEquals(expectedTuples(List.of(a, List.of(loop))),
                tuples(execute("MATCH (a:P)-[r:L]->{1,2}(a)"), "a", "r"));
    }

    // -------------------------------------------------------------------------
    // Provider exceptions, provider iterator closing, plain-before-quantified tiebreak
    // -------------------------------------------------------------------------

    /** a -A-> b -B-> c. Returns {a, b, c}. */
    private Vertex[] addABChain() {
        final Vertex a = graph.addVertex("V");
        final Vertex b = graph.addVertex("V");
        final Vertex c = graph.addVertex("V");
        a.addEdge("A", b);
        b.addEdge("B", c);
        return new Vertex[]{a, b, c};
    }

    @Test(timeout = 10000)
    public void testProviderEdgesFailureFailsLoudlyOnLaterHasNext() {
        // b.edges() fails once, while the B step is being expanded from b with y = b bound. The
        // failed search cannot be resumed without dropping or repeating rows, so a later
        // hasNext() must not quietly continue, and every provider iterator must be closed.
        final Vertex[] v = addABChain();
        final ProbeGraph probe = new ProbeGraph(graph).withVerticesInIdOrder().failNextEdgesCall(v[1]);
        final GqlMatchPlan plan = new DefaultGqlPlanner(probe.graph()).plan("MATCH (x)-[:A]->(y)-[:B]->(z)");
        assertEquals("x", plan.getSeedVariable());
        final Iterator<GqlRow> iter = new DefaultGqlExecutor(probe.graph()).execute(plan);

        try {
            iter.hasNext();
            fail("the provider's edges() exception must propagate");
        } catch (final ProbeGraph.InjectedFailure expected) {
            // expected
        }
        assertTrue(probe.opened() > 0);
        probe.assertAllClosedOnce("every provider iterator must be closed after the failure");

        for (int i = 0; i < 2; i++) {
            try {
                iter.hasNext();
                fail("hasNext() after a failure must not resume the search");
            } catch (final IllegalStateException expected) {
                assertTrue(expected.getCause() instanceof ProbeGraph.InjectedFailure);
            }
        }
        try {
            iter.next();
            fail("next() after a failure must not resume the search");
        } catch (final IllegalStateException expected) {
            assertTrue(expected.getCause() instanceof ProbeGraph.InjectedFailure);
        }

        // The failure was injected once, so a fresh run of the same plan returns the one row.
        final List<Map<String, Element>> rows = materialize(new DefaultGqlExecutor(probe.graph()).execute(plan), plan);
        assertEquals(1, rows.size());
        assertEquals(v[0], rows.get(0).get("x"));
        assertEquals(v[1], rows.get(0).get("y"));
        assertEquals(v[2], rows.get(0).get("z"));
    }

    /** a -L-> b -L-> c -L-> d -L-> e, all labelled V. */
    private void addLChain() {
        Vertex prev = graph.addVertex("V");
        for (int i = 0; i < 4; i++) {
            final Vertex next = graph.addVertex("V");
            prev.addEdge("L", next);
            prev = next;
        }
    }

    /** A plain step followed by a quantified one, so both frame types open provider iterators. */
    private static final String PLAIN_THEN_QUANTIFIED = "MATCH (x:V)-[:L]->(y:V)-[r:L]->{1,2}(z:V)";

    @Test(timeout = 10000)
    public void testProviderIteratorsClosedWhenDrained() {
        addLChain();
        final ProbeGraph probe = new ProbeGraph(graph);
        final GqlMatchPlan plan = new DefaultGqlPlanner(probe.graph()).plan(PLAIN_THEN_QUANTIFIED);
        // x -> y is one of 4 edges, so y is b, c, d or e; from there the walk takes 1 or 2 further
        // edges: 1 + 1 + 1 + 0 one-hop and 1 + 1 + 0 + 0 two-hop walks, 5 rows in all.
        assertEquals(5, materialize(new DefaultGqlExecutor(probe.graph()).execute(plan), plan).size());
        assertTrue(probe.opened() > 1);
        probe.assertAllClosedOnce("after the match");
    }

    @Test(timeout = 10000)
    public void testProviderIteratorsClosedWhenClosedEarly() {
        addLChain();
        final ProbeGraph probe = new ProbeGraph(graph);
        final GqlMatchPlan plan = new DefaultGqlPlanner(probe.graph()).plan(PLAIN_THEN_QUANTIFIED);
        final Iterator<GqlRow> iter = new DefaultGqlExecutor(probe.graph()).execute(plan);

        assertTrue(iter.hasNext());
        iter.next();
        assertTrue("the search must hold provider iterators open mid-way", probe.opened() > probe.closed());
        assertTrue(iter instanceof CloseableIterator);
        CloseableIterator.closeIterator(iter);
        probe.assertAllClosedOnce("after the match");
        assertFalse("a closed search has no more rows", iter.hasNext());
        CloseableIterator.closeIterator(iter);
        probe.assertAllClosedOnce("close() must be idempotent");
    }

    @Test(timeout = 10000)
    public void testProviderIteratorsClosedOnInterrupt() {
        addLChain();
        final ProbeGraph probe = new ProbeGraph(graph);
        final GqlMatchPlan plan = new DefaultGqlPlanner(probe.graph()).plan(PLAIN_THEN_QUANTIFIED);
        final Iterator<GqlRow> iter = new DefaultGqlExecutor(probe.graph()).execute(plan);

        assertTrue(iter.hasNext());
        iter.next();
        assertTrue(probe.opened() > probe.closed());
        try {
            Thread.currentThread().interrupt();
            iter.hasNext();
            fail("expected TraversalInterruptedException");
        } catch (final TraversalInterruptedException expected) {
            // expected
        } finally {
            Thread.interrupted();
        }
        probe.assertAllClosedOnce("after the match");
        try {
            iter.hasNext();
            fail("hasNext() after an interrupt must not resume the search");
        } catch (final IllegalStateException expected) {
            assertTrue(expected.getCause() instanceof TraversalInterruptedException);
        }
    }

    @Test(timeout = 10000)
    public void testSeedScanFailureClosesSeedIterator() {
        // The seed scan fails on its first vertex. execute() must not read from the provider, so
        // the failure surfaces from hasNext(), whose failure path closes the seed iterator,
        // rather than escaping from execute() with the scan left open.
        addLChain();
        final ProbeGraph probe = new ProbeGraph(graph).failVertexScanAfter(0);
        final GqlMatchPlan plan = new DefaultGqlPlanner(probe.graph()).plan(PLAIN_THEN_QUANTIFIED);
        final Iterator<GqlRow> iter = new DefaultGqlExecutor(probe.graph()).execute(plan);
        assertEquals("execute() opens only the seed scan", 1, probe.opened());
        assertEquals("execute() must not read from the seed scan", 0, probe.read());

        try {
            iter.hasNext();
            fail("the seed scan's exception must propagate");
        } catch (final ProbeGraph.InjectedFailure expected) {
            // expected
        }
        probe.assertAllClosedOnce("after the seed scan failed");
        try {
            iter.hasNext();
            fail("hasNext() after a failure must not resume the search");
        } catch (final IllegalStateException expected) {
            assertTrue(expected.getCause() instanceof ProbeGraph.InjectedFailure);
        }
    }

    @Test(timeout = 10000)
    public void testSeedScanFailureAfterRowsClosesEveryIterator() {
        // The scan fails when asked for the second seed, after the first seed's rows, so both the
        // seed iterator and the frames' edge iterators have been opened before the failure.
        addLChain();
        final ProbeGraph probe = new ProbeGraph(graph).withVerticesInIdOrder().failVertexScanAfter(1);
        final GqlMatchPlan plan = new DefaultGqlPlanner(probe.graph()).plan(PLAIN_THEN_QUANTIFIED);
        assertEquals("x", plan.getSeedVariable());
        final Iterator<GqlRow> iter = new DefaultGqlExecutor(probe.graph()).execute(plan);

        // The first seed, a, gives y = b and walks b -> c and b -> c -> d.
        int rows = 0;
        try {
            while (iter.hasNext()) {
                iter.next();
                rows++;
            }
            fail("the seed scan's exception must propagate");
        } catch (final ProbeGraph.InjectedFailure expected) {
            // expected
        }
        assertEquals("the first seed's rows come before the failure", 2, rows);
        assertTrue(probe.opened() > 1);
        probe.assertAllClosedOnce("after the seed scan failed");
    }

    /** Indexes {@code n} and names the head of the L chain, so the seed comes from an index lookup. */
    private GqlMatchPlan indexSeededPlan(final ProbeGraph probe) {
        graph.createIndex("n", Vertex.class);
        addLChain();
        graph.traversal().V().not(__.inE("L")).next().property("n", "head");
        final GqlMatchPlan plan = new DefaultGqlPlanner(probe.graph())
                .plan("MATCH (x:V {n: 'head'})-[:L]->(y:V)-[r:L]->{1,2}(z:V)");
        assertEquals("x", plan.getSeedVariable());
        return plan;
    }

    @Test(timeout = 10000)
    public void testIndexSeededIteratorsClosedWhenDrained() {
        final ProbeGraph probe = new ProbeGraph(graph);
        final GqlMatchPlan plan = indexSeededPlan(probe);
        // head -> b, then b -> c and b -> c -> d.
        assertEquals(2, materialize(new DefaultGqlExecutor(probe.graph()).execute(plan), plan).size());
        assertEquals("the seed must come from the index", 1, probe.indexQueries());
        assertTrue(probe.opened() > 1);
        probe.assertAllClosedOnce("after the index-seeded match");
    }

    @Test(timeout = 10000)
    public void testIndexSeededIteratorsClosedWhenClosedEarly() {
        final ProbeGraph probe = new ProbeGraph(graph);
        final GqlMatchPlan plan = indexSeededPlan(probe);
        final Iterator<GqlRow> iter = new DefaultGqlExecutor(probe.graph()).execute(plan);

        assertTrue(iter.hasNext());
        iter.next();
        assertEquals("the seed must come from the index", 1, probe.indexQueries());
        assertTrue("the search must hold provider iterators open mid-way", probe.opened() > probe.closed());
        CloseableIterator.closeIterator(iter);
        probe.assertAllClosedOnce("after closing the index-seeded match early");
    }

    /**
     * m1 and m2 are M. m1 -B-> q1 -B-> q2 and has no A edge; m2 -B-> q3 -B-> q4 and m2 -A-> p1..p4.
     * A and B both label 4 edges, P and Q both label 4 vertices. Returns {m1, m2, q4}.
     */
    private Vertex[] addPlainAndQuantifiedFromTwoAnchors() {
        final Vertex m1 = graph.addVertex("M");
        final Vertex m2 = graph.addVertex("M");
        final Vertex q1 = graph.addVertex("Q");
        final Vertex q2 = graph.addVertex("Q");
        final Vertex q3 = graph.addVertex("Q");
        final Vertex q4 = graph.addVertex("Q");
        m1.addEdge("B", q1);
        q1.addEdge("B", q2);
        m2.addEdge("B", q3);
        q3.addEdge("B", q4);
        for (int i = 0; i < 4; i++) m2.addEdge("A", graph.addVertex("P"));
        return new Vertex[]{m1, m2, q4};
    }

    /**
     * Pulls the first row of the B-then-A pattern with seeds in id order (m1 first) and checks
     * the plain A step was expanded first at m1. There A has no candidate, so the walk is never
     * started: B is attempted once in all, at m2. Expanding B first would walk m1's chain to q2
     * and only then fail on A, attempting B twice before the first row.
     */
    private void assertPlainStepExpandedFirst(final ProbeGraph probe, final Vertex[] v) {
        final GqlMatchPlan plan = new DefaultGqlPlanner(probe.graph()).plan("MATCH (m:M)-[:B]->{2,3}(q:Q), (m)-[:A]->(p:P)");
        assertEquals("m", plan.getSeedVariable());
        final ExtensionStep a = plan.getSteps().stream().filter(s -> "A".equals(s.getEdgeLabel())).findFirst().orElseThrow();
        final ExtensionStep b = plan.getSteps().stream().filter(s -> "B".equals(s.getEdgeLabel())).findFirst().orElseThrow();
        assertFalse(a.isQuantified());
        assertTrue(b.isQuantified());

        final Iterator<GqlRow> iter = new DefaultGqlExecutor(probe.graph()).execute(plan);
        assertTrue(iter.hasNext());
        final GqlRow row = iter.next();
        assertEquals(v[1], row.getScalar(plan.getIndex("m")));
        assertEquals(v[2], row.getScalar(plan.getIndex("q")));
        // TinkerGraph does not order a vertex's edges of one label, so p is any of m2's four.
        assertEquals("P", row.getScalar(plan.getIndex("p")).label());
        assertTrue(graph.traversal().V(v[1]).out("A").toList().contains(row.getScalar(plan.getIndex("p"))));
        assertEquals("the plain step runs at both anchors", 2, a.attemptCount());
        assertEquals("the walk must not run at m1, where the plain step fails first", 1, b.attemptCount());
        CloseableIterator.closeIterator(iter);
    }

    @Test(timeout = 10000)
    public void testPlainStepExpandedBeforeQuantifiedStepWhenCheaper() {
        // Cost-driven, not the tiebreak: with real counts A costs min(4, 4) = 4 and the {2,3} walk
        // over B costs min(4, 4) * 3 = 12, so A is cheaper. The tiebreak is pinned by
        // testPlainStepExpandedBeforeQuantifiedStepWithDefaultLabelCounts, where both cost MAX.
        final Vertex[] v = addPlainAndQuantifiedFromTwoAnchors();
        final ProbeGraph probe = new ProbeGraph(graph).withVerticesInIdOrder();
        assertPlainStepExpandedFirst(probe, v);
    }

    @Test(timeout = 10000)
    public void testPlainStepExpandedBeforeQuantifiedStepWithDefaultLabelCounts() {
        // With the Graph defaults every count is Long.MAX_VALUE, so both steps cost MAX and the
        // planner keeps B, written first, ahead of A. Only the plain-before-quantified tiebreak
        // puts A first. M keeps its real count so the planner can choose a seed.
        final Vertex[] v = addPlainAndQuantifiedFromTwoAnchors();
        final ProbeGraph probe = new ProbeGraph(graph).withVerticesInIdOrder()
                .withUnknownEdgeCounts().withUnknownVertexCount("P").withUnknownVertexCount("Q");
        final GqlMatchPlan plan = new DefaultGqlPlanner(probe.graph()).plan("MATCH (m:M)-[:B]->{2,3}(q:Q), (m)-[:A]->(p:P)");
        assertEquals(List.of("B", "A"), plan.getSteps().stream().map(ExtensionStep::getEdgeLabel).collect(Collectors.toList()));
        for (final ExtensionStep s : plan.getSteps()) assertEquals(Long.MAX_VALUE, s.getEstimatedCost());
        assertPlainStepExpandedFirst(probe, v);
    }
}
