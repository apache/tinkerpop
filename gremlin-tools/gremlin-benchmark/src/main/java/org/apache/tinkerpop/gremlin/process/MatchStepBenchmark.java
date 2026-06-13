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
package org.apache.tinkerpop.gremlin.process;

import org.apache.tinkerpop.benchmark.util.AbstractBenchmarkBase;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoReader;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Level;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks comparing declarative {@code match(String)} (GQL) against imperative Gremlin
 * across a spectrum of query patterns using the air-routes dataset (~3,500 airports,
 * ~50,000 routes).
 *
 * <p>Seven sections cover progressively more structural scenarios:
 * <ol>
 *   <li>Label scan baseline — both forms do a full label-filtered vertex scan</li>
 *   <li>Property-filtered seed — both forms use the TinkerGraph vertex index</li>
 *   <li>Simple one-hop edge pattern — forward traversal, no filters</li>
 *   <li>Both-endpoint property filters — target predicate applied during DFS</li>
 *   <li>Selective target win — GQL planner picks the more selective endpoint as seed;
 *       compared against both natural and manually reversed imperative forms</li>
 *   <li>Multi-pattern hub win — GQL DAG executor reorders extension steps by cost;
 *       compared against sequential {@code filter()} chains</li>
 *   <li>Two-hop selective midpoint win — GQL planner picks the hub airport as seed
 *       and extends in both directions; compared against full left-to-right scan</li>
 * </ol>
 *
 * <p>Sections 2–7 use a TinkerGraph with vertex indexes on {@code code} and {@code country}
 * so both the imperative {@code has()} step and the GQL seed iterator can exploit them on
 * equal footing.
 */
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class MatchStepBenchmark extends AbstractBenchmarkBase {

    // Plain graph for label-scan and full edge-traversal tests (sections 1, 3).
    private Graph graph;
    private GraphTraversalSource g;

    // Indexed graph for property-filter and structural-win tests (sections 2, 4–7).
    private Graph indexedGraph;
    private GraphTraversalSource ig;

    @Setup(Level.Trial)
    public void prepare() throws IOException {
        graph = TinkerGraph.open();
        loadAirRoutes(graph);
        g = graph.traversal();

        final TinkerGraph tg = TinkerGraph.open();
        tg.createIndex("code", Vertex.class);
        tg.createIndex("country", Vertex.class);
        loadAirRoutes(tg);
        indexedGraph = tg;
        ig = tg.traversal();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        graph.close();
        indexedGraph.close();
    }

    private static void loadAirRoutes(final Graph target) throws IOException {
        final GraphReader reader = GryoReader.build().create();
        try (final InputStream stream = TinkerFactory.class.getResourceAsStream("air-routes.kryo")) {
            reader.readGraph(stream, target);
        }
    }

    // =========================================================================
    // Section 1 — Label scan baseline
    //
    // Both forms iterate all vertices filtered by label. Establishes the floor
    // and confirms match() adds no measurable overhead for the trivial case.
    // Expected: essentially equal.
    // =========================================================================

    @Benchmark
    public List<Vertex> imperative_labelScan() {
        return g.V().hasLabel("airport").toList();
    }

    @Benchmark
    public List<Object> gql_labelScan() {
        return g.match("MATCH (a:airport)").select("a").toList();
    }

    // =========================================================================
    // Section 2 — Property-filtered seed (index-backed)
    //
    // Tests the index-lookup path in TinkerGraphGqlExecutor.seedIterator().
    // Both forms resolve vertices via the TinkerGraph property index.
    // Expected: equal; validates GQL index integration is not slower.
    // =========================================================================

    @Benchmark
    public List<Vertex> imperative_singleAirportByCode() {
        return ig.V().has("airport", "code", "ATL").toList();
    }

    @Benchmark
    public List<Object> gql_singleAirportByCode() {
        return ig.match("MATCH (a:airport {code: 'ATL'})").select("a").toList();
    }

    @Benchmark
    public List<Vertex> imperative_airportsByCountry() {
        return ig.V().has("airport", "country", "US").toList();
    }

    @Benchmark
    public List<Object> gql_airportsByCountry() {
        return ig.match("MATCH (a:airport {country: 'US'})").select("a").toList();
    }

    // =========================================================================
    // Section 3 — Simple one-hop edge pattern
    //
    // Iterates all airport→route→airport pairs. Neither form has a selective
    // predicate, so this measures raw traversal cost.
    // Expected: close; match() array-based bindings may narrow the gap slightly.
    // =========================================================================

    @Benchmark
    @SuppressWarnings({"unchecked", "rawtypes"})
    public List<Map<String, Object>> imperative_routePattern() {
        return (List) g.V().hasLabel("airport").as("a").out("route").as("b").select("a", "b").toList();
    }

    @Benchmark
    @SuppressWarnings({"unchecked", "rawtypes"})
    public List<Map<String, Object>> gql_routePattern() {
        return (List) g.match("MATCH (a:airport)-[:route]->(b:airport)").select("a", "b").toList();
    }

    // =========================================================================
    // Section 4 — Both-endpoint property filters
    //
    // Finds routes from US airports to French airports. Seed is chosen by label
    // cardinality (both labels are 'airport'); target predicate is checked during
    // DFS before committing the binding.
    // Expected: equal or GQL slightly faster due to early target predicate pruning.
    // =========================================================================

    @Benchmark
    @SuppressWarnings({"unchecked", "rawtypes"})
    public List<Map<String, Object>> imperative_US_to_FR() {
        return (List) ig.V().has("airport", "country", "US").as("a")
                .out("route").has("country", "FR").as("b")
                .select("a", "b").toList();
    }

    @Benchmark
    @SuppressWarnings({"unchecked", "rawtypes"})
    public List<Map<String, Object>> gql_US_to_FR() {
        return (List) ig.match(
                "MATCH (a:airport {country: 'US'})-[:route]->(b:airport {country: 'FR'})")
                .select("a", "b").toList();
    }

    // =========================================================================
    // Section 5 — Structural win: selective target, wrong traversal direction
    //
    // Pattern: find all airports with a direct route TO ATL.
    //
    // Natural imperative: scans all ~3,500 airports, fans out along every route,
    //   then filters for ATL — O(|airports| × avg_degree).
    // Reversed imperative: user manually starts from ATL and walks in-edges —
    //   O(in-degree of ATL), optimal but requires knowledge of data distribution.
    // GQL: planner recognises 'b {code:ATL}' is more selective and picks it as
    //   seed automatically — same cost as the reversed imperative.
    //
    // Expected: gql ≈ imperative_reversed << imperative_natural.
    // =========================================================================

    @Benchmark
    @SuppressWarnings({"unchecked", "rawtypes"})
    public List<Map<String, Object>> imperative_toATL_natural() {
        return (List) ig.V().hasLabel("airport").as("a")
                .out("route").has("code", "ATL").as("b")
                .select("a", "b").toList();
    }

    @Benchmark
    @SuppressWarnings({"unchecked", "rawtypes"})
    public List<Map<String, Object>> imperative_toATL_reversed() {
        return (List) ig.V().has("airport", "code", "ATL").as("b")
                .in("route").as("a")
                .select("a", "b").toList();
    }

    @Benchmark
    @SuppressWarnings({"unchecked", "rawtypes"})
    public List<Map<String, Object>> gql_toATL() {
        return (List) ig.match("MATCH (a:airport)-[:route]->(b:airport {code: 'ATL'})")
                .select("a", "b").toList();
    }

    // =========================================================================
    // Section 6 — Structural win: multi-pattern hub query
    //
    // Pattern: find airports that serve both the US and France (at least one
    // direct route to each).
    //
    // Imperative: two sequential filter() steps — the first filter is applied
    //   over all airports; the second is applied over the survivors regardless
    //   of which country filter is cheaper.
    // GQL: the DAG executor evaluates both extension steps' selectivityRatio()
    //   and applies the more selective branch first, pruning the DFS earlier.
    //
    // Expected: gql faster; advantage grows as result selectivity diverges.
    // =========================================================================

    @Benchmark
    public List<Vertex> imperative_hubUS_and_FR() {
        return ig.V().hasLabel("airport")
                .filter(__.out("route").has("country", "US"))
                .filter(__.out("route").has("country", "FR"))
                .toList();
    }

    @Benchmark
    public List<Object> gql_hubUS_and_FR() {
        return ig.match(
                "MATCH (hub:airport)-[:route]->(a:airport {country: 'US'}), " +
                "(hub:airport)-[:route]->(b:airport {country: 'FR'})")
                .select("hub").toList();
    }

    // =========================================================================
    // Section 7 — Structural win: two-hop with selective midpoint
    //
    // Pattern: find all (origin, destination) pairs reachable via DFW.
    //
    // Natural imperative: scans all airports, follows outbound routes, filters
    //   for DFW, then follows DFW's outbound routes — O(|airports| × avg_degree).
    // GQL: planner picks DFW as seed (most selective node), extends IN for
    //   origins and OUT for destinations — O(in-degree(DFW) × out-degree(DFW)).
    //
    // Expected: gql significantly faster than natural imperative.
    // =========================================================================

    @Benchmark
    @SuppressWarnings({"unchecked", "rawtypes"})
    public List<Map<String, Object>> imperative_twoHop_viaDFW_natural() {
        return (List) ig.V().hasLabel("airport").as("a")
                .out("route").has("code", "DFW").as("hub")
                .out("route").as("b")
                .select("a", "hub", "b").toList();
    }

    @Benchmark
    @SuppressWarnings({"unchecked", "rawtypes"})
    public List<Map<String, Object>> gql_twoHop_viaDFW() {
        return (List) ig.match(
                "MATCH (a:airport)-[:route]->(hub:airport {code: 'DFW'})-[:route]->(b:airport)")
                .select("a", "hub", "b").toList();
    }
}
