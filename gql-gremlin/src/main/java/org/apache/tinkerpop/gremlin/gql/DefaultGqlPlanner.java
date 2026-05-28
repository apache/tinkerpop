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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * Compiles a {@link QueryGraph} into an executable {@link GqlMatchPlan}.
 *
 * <h3>Algorithm</h3>
 * <ol>
 *   <li><strong>Seed selection</strong> — each {@link QueryVertex} is scored via
 *       {@link Graph#countVerticesByLabel(String)}. The node with the fewest matching vertices is
 *       chosen as the seed; ties are broken by list order. Seed selection is recomputed on
 *       every call so that graph mutations are always reflected.</li>
 *   <li><strong>Plan compilation</strong> — a BFS is performed over the {@link QueryGraph}
 *       starting from the seed node. Within each BFS node, edges are sorted by edge-label
 *       density (ascending via {@link Graph#countEdgesByLabel(String)}) so that rarer edge
 *       labels produce extension steps earlier in the plan, pruning the DFS sooner.</li>
 *   <li><strong>Parse caching</strong> — the parsed {@link QueryGraph} is cached keyed by
 *       the original GQL query string in a Caffeine LRU cache bounded by
 *       {@link #PLAN_CACHE_MAX_SIZE}. Seed selection and step ordering are recomputed each
 *       call using live counts, avoiding stale plans after mutations.</li>
 * </ol>
 */
public final class DefaultGqlPlanner implements GqlPlanner {

    /** Maximum number of distinct GQL query strings whose parsed {@link QueryGraph} is retained. */
    static final int PLAN_CACHE_MAX_SIZE = 1_000;

    private final Graph graph;

    // Cache only the parsed QueryGraph — mutation-independent, safe to reuse across calls.
    // Seed selection and step ordering are recomputed on every call using live label counts.
    // Bounded to PLAN_CACHE_MAX_SIZE entries via Caffeine LRU eviction.
    private final Cache<String, QueryGraph> queryGraphCache;

    public DefaultGqlPlanner(final Graph graph) {
        this(graph, Caffeine.newBuilder().maximumSize(PLAN_CACHE_MAX_SIZE).build());
    }

    public DefaultGqlPlanner(final Graph graph, final Cache<String, QueryGraph> queryGraphCache) {
        this.graph = graph;
        this.queryGraphCache = queryGraphCache;
    }

    /**
     * Returns a {@link GqlMatchPlan} for the given GQL MATCH string. The parsed
     * {@link QueryGraph} is cached; seed selection and step ordering are recomputed
     * each call from live label-count data so that graph mutations are always reflected.
     *
     * @param gqlMatchString a GQL MATCH expression (e.g. {@code "MATCH (a:Person)-[:KNOWS]->(b)"})
     * @return the compiled execution plan
     * @throws IllegalArgumentException if the string cannot be parsed
     */
    @Override
    public GqlMatchPlan plan(final String gqlMatchString) {
        final QueryGraph queryGraph = queryGraphCache.get(gqlMatchString, QueryGraph::parse);
        return compile(queryGraph);
    }

    /**
     * Compiles a {@link QueryGraph} into a {@link GqlMatchPlan}. Package-private for testing.
     */
    GqlMatchPlan compile(final QueryGraph queryGraph) {
        final List<QueryVertex> nodes = queryGraph.getNodes();
        if (nodes.isEmpty()) {
            return new GqlMatchPlan(null, null, Collections.emptyList(),
                    Collections.emptyMap(), new String[0]);
        }

        // Assign effective variable names before any planning so anonymous nodes are trackable
        final Map<QueryVertex, String> effectiveVars = assignEffectiveVariables(nodes);

        // Select the seed node: lowest cardinality wins
        final QueryVertex seed = selectSeed(nodes);

        // BFS-order the edges into ExtensionSteps
        final List<ExtensionStep> steps = buildSteps(queryGraph, seed, effectiveVars);

        // Build the variable index: seed first (index 0), then edgeVar/targetVar per step.
        // Uses LinkedHashMap to keep insertion order for the inverse array.
        final Map<String, Integer> variableIndex = new java.util.LinkedHashMap<>();
        final String seedVar = effectiveVars.get(seed);
        variableIndex.put(seedVar, 0);
        for (final ExtensionStep step : steps) {
            if (step.getEdgeVariable() != null && !variableIndex.containsKey(step.getEdgeVariable()))
                variableIndex.put(step.getEdgeVariable(), variableIndex.size());
            if (step.getTargetVariable() != null && !variableIndex.containsKey(step.getTargetVariable()))
                variableIndex.put(step.getTargetVariable(), variableIndex.size());
        }
        final String[] variables = variableIndex.keySet().toArray(new String[0]);

        return new GqlMatchPlan(seedVar, seed.getLabel(), steps,
                Collections.unmodifiableMap(variableIndex), variables, seed.getPredicates());
    }

    // -------------------------------------------------------------------------
    // Seed selection
    // -------------------------------------------------------------------------

    private QueryVertex selectSeed(final List<QueryVertex> nodes) {
        QueryVertex best = null;
        long bestCount = Long.MAX_VALUE;
        for (final QueryVertex node : nodes) {
            final long count = countMatchingVertices(node);
            if (count < bestCount) {
                bestCount = count;
                best = node;
            }
        }
        return best;
    }

    private long countMatchingVertices(final QueryVertex node) {
        return estimateCardinality(node.getLabel(), node.getPredicates());
    }

    /**
     * Estimates the number of vertices matching the given label and predicates.
     * Uses the vertex label count as the base, then narrows with the most selective
     * indexed literal predicate if the graph supports index access.
     */
    private long estimateCardinality(final String label, final List<PropertyPredicate> predicates) {
        long count = graph.countVerticesByLabel(label);
        for (final PropertyPredicate p : predicates) {
            if (p.isParamRef()) continue;
            final Object value = p.getLiteralValue();
            if (value == null) continue;
            final long indexCount = graph.index().countVertexIndex(p.getKey(), value);
            if (indexCount < Long.MAX_VALUE) {
                count = Math.min(count, indexCount);
            }
        }
        return count;
    }

    /**
     * Estimates the cost of traversing an extension step. Bounded by both the edge label
     * density and the estimated cardinality of the target vertex set.
     */
    private long estimateStepCost(final QueryEdge edge, final QueryVertex targetNode) {
        final long edgeCost = graph.countEdgesByLabel(edge.getLabel());
        final long targetCost = estimateCardinality(targetNode.getLabel(), targetNode.getPredicates());
        return Math.min(edgeCost, targetCost);
    }

    // -------------------------------------------------------------------------
    // Variable assignment
    // -------------------------------------------------------------------------

    /**
     * Assigns an effective variable name to every node in the query. Named nodes keep their
     * declared variable; anonymous nodes receive a synthetic name of the form {@code $anonN}.
     * Uses identity-based map so distinct anonymous nodes are treated independently even if
     * they happen to share the same label.
     */
    private static Map<QueryVertex, String> assignEffectiveVariables(final List<QueryVertex> nodes) {
        final Map<QueryVertex, String> vars = new IdentityHashMap<>();
        int anonCounter = 0;
        for (final QueryVertex node : nodes) {
            if (node.getVariable() != null) {
                vars.put(node, node.getVariable());
            }
        }
        for (final QueryVertex node : nodes) {
            if (node.getVariable() == null) {
                vars.put(node, "$anon" + anonCounter++);
            }
        }
        return vars;
    }

    // -------------------------------------------------------------------------
    // Plan compilation (BFS edge ordering)
    // -------------------------------------------------------------------------

    /**
     * Performs a BFS over the query graph starting from {@code seed} and produces one
     * {@link ExtensionStep} per edge. The BFS visit order guarantees each step's anchor
     * variable is bound before the step is emitted.
     *
     * <p>Back edges (both endpoints already visited) are still emitted as steps so the
     * executor can verify the join constraint against the already-bound variable.
     */
    private List<ExtensionStep> buildSteps(final QueryGraph queryGraph,
                                           final QueryVertex seed,
                                           final Map<QueryVertex, String> effectiveVars) {
        final List<ExtensionStep> steps = new ArrayList<>();

        // visitOrder tracks the BFS discovery sequence; used to pick the anchor for back-edges
        final Map<QueryVertex, Integer> visitOrder = new IdentityHashMap<>();
        final Queue<QueryVertex> queue = new ArrayDeque<>();
        final Set<QueryEdge> processedEdges = Collections.newSetFromMap(new IdentityHashMap<>());

        visitOrder.put(seed, 0);
        queue.add(seed);
        int visitCounter = 1;

        while (!queue.isEmpty()) {
            final QueryVertex current = queue.poll();
            final int currentOrder = visitOrder.get(current);

            // Collect edges touching current, sort by edge-label density (ascending) so that
            // rarer edge labels produce extension steps earlier in the plan.
            final List<QueryEdge> eligible = new ArrayList<>();
            for (final QueryEdge edge : queryGraph.getEdges()) {
                if (!processedEdges.contains(edge) &&
                        (edge.getSource() == current || edge.getTarget() == current)) {
                    eligible.add(edge);
                }
            }
            eligible.sort(Comparator.comparingLong(e -> graph.countEdgesByLabel(e.getLabel())));

            for (final QueryEdge edge : eligible) {
                processedEdges.add(edge);

                final boolean isSource = edge.getSource() == current;
                final boolean isTarget = edge.getTarget() == current;

                final QueryVertex anchor;
                final QueryVertex targetNode;
                final Direction stepDir;

                if (isSource && isTarget) {
                    // Self-loop: traverse from current to itself
                    anchor = current;
                    targetNode = current;
                    stepDir = edge.getDirection();
                } else if (isSource) {
                    final QueryVertex other = edge.getTarget();
                    if (!visitOrder.containsKey(other)) {
                        // Forward edge: current → other
                        anchor = current;
                        targetNode = other;
                        stepDir = edge.getDirection();
                    } else {
                        // Back edge: pick the earlier-visited node as anchor
                        if (currentOrder <= visitOrder.get(other)) {
                            anchor = current;
                            targetNode = other;
                            stepDir = edge.getDirection();
                        } else {
                            anchor = other;
                            targetNode = current;
                            stepDir = flip(edge.getDirection());
                        }
                    }
                } else { // isTarget
                    final QueryVertex other = edge.getSource();
                    if (!visitOrder.containsKey(other)) {
                        // Forward edge (reversed): current ← other → emit as current traverses back
                        anchor = current;
                        targetNode = other;
                        stepDir = flip(edge.getDirection());
                    } else {
                        // Back edge: pick the earlier-visited node as anchor
                        if (visitOrder.get(other) <= currentOrder) {
                            anchor = other;
                            targetNode = current;
                            stepDir = edge.getDirection();
                        } else {
                            anchor = current;
                            targetNode = other;
                            stepDir = flip(edge.getDirection());
                        }
                    }
                }

                steps.add(new ExtensionStep(
                        effectiveVars.get(anchor),
                        edge.getLabel(),
                        stepDir,
                        edge.getVariable(),
                        edge.getPredicates(),
                        targetNode.getLabel(),
                        effectiveVars.get(targetNode),
                        targetNode.getPredicates(),
                        estimateStepCost(edge, targetNode)));

                if (!visitOrder.containsKey(targetNode)) {
                    visitOrder.put(targetNode, visitCounter++);
                    queue.add(targetNode);
                }
            }
        }

        // Detect disconnected pattern components.
        if (visitOrder.size() < queryGraph.getNodes().size()) {
            final List<String> unvisited = new ArrayList<>();
            for (final QueryVertex n : queryGraph.getNodes()) {
                if (!visitOrder.containsKey(n)) {
                    unvisited.add(n.getVariable() != null ? "(" + n.getVariable() + ")" : "(anonymous)");
                }
            }
            throw new IllegalArgumentException(
                    "MATCH pattern contains disconnected components — all patterns must share at least " +
                    "one variable to join them. Unconnected nodes: " + unvisited +
                    ". Use a shared variable (e.g. MATCH (a)-[:E1]->(b), (b)-[:E2]->(c)) to connect patterns.");
        }

        return steps;
    }

    private static Direction flip(final Direction direction) {
        if (direction == Direction.OUT) return Direction.IN;
        if (direction == Direction.IN) return Direction.OUT;
        return Direction.BOTH;
    }
}
