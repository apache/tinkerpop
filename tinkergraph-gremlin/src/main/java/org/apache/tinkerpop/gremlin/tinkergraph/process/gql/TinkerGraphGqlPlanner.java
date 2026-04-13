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
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.AbstractTinkerGraph;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Compiles a {@link QueryGraph} into an executable {@link GqlMatchPlan} for TinkerGraph.
 *
 * <h3>Algorithm</h3>
 * <ol>
 *   <li><strong>Seed selection</strong> — each {@link QueryNode} is scored by calling
 *       {@link TinkerGraph#vertices()} and counting vertices that match its label constraint
 *       (or using {@link TinkerGraph#getVerticesCount()} when the node has no label). The
 *       node with the fewest matching vertices is chosen as the seed; ties are broken by
 *       list order.</li>
 *   <li><strong>Plan compilation</strong> — a BFS is performed over the {@link QueryGraph}
 *       starting from the seed node. For each edge encountered, one {@link ExtensionStep} is
 *       emitted. The BFS ordering guarantees that every step's anchor variable is bound before
 *       the step executes. Anonymous nodes (no variable name) receive synthetic variable names
 *       with a {@code $anon} prefix so the executor can track their bindings.</li>
 *   <li><strong>Plan caching</strong> — compiled plans are cached keyed by the original GQL
 *       query string. Repeated calls with the same string return the cached plan in O(1).</li>
 * </ol>
 *
 * <p>This planner is the primary optimization surface for TinkerGraph GQL execution. Future
 * extensions can incorporate property-filter selectivity estimates and index hints to improve
 * seed selection.
 */
public final class TinkerGraphGqlPlanner {

    private final AbstractTinkerGraph graph;
    private final Map<String, GqlMatchPlan> planCache = new ConcurrentHashMap<>();

    public TinkerGraphGqlPlanner(final AbstractTinkerGraph graph) {
        this.graph = graph;
    }

    /**
     * Returns a {@link GqlMatchPlan} for the given GQL MATCH string, compiling and caching
     * on first access.
     *
     * @param gqlMatchString a GQL MATCH expression (e.g. {@code "MATCH (a:Person)-[:KNOWS]->(b)"})
     * @return the compiled execution plan
     * @throws IllegalArgumentException if the string cannot be parsed
     */
    public GqlMatchPlan plan(final String gqlMatchString) {
        return planCache.computeIfAbsent(gqlMatchString, q -> compile(QueryGraph.parse(q)));
    }

    /**
     * Compiles a {@link QueryGraph} into a {@link GqlMatchPlan}. Package-private for testing.
     */
    GqlMatchPlan compile(final QueryGraph queryGraph) {
        final List<QueryNode> nodes = queryGraph.getNodes();
        if (nodes.isEmpty()) {
            // Empty pattern — no-op plan; executor should produce zero results
            return new GqlMatchPlan(null, null, Collections.emptyList());
        }

        // Assign effective variable names before any planning so anonymous nodes are trackable
        final Map<QueryNode, String> effectiveVars = assignEffectiveVariables(nodes);

        // Select the seed node: lowest cardinality wins
        final QueryNode seed = selectSeed(nodes);

        // BFS-order the edges into ExtensionSteps
        final List<ExtensionStep> steps = buildSteps(queryGraph, seed, effectiveVars);

        return new GqlMatchPlan(effectiveVars.get(seed), seed.getLabel(), steps);
    }

    // -------------------------------------------------------------------------
    // Seed selection
    // -------------------------------------------------------------------------

    private QueryNode selectSeed(final List<QueryNode> nodes) {
        QueryNode best = null;
        long bestCount = Long.MAX_VALUE;
        for (final QueryNode node : nodes) {
            final long count = countMatchingVertices(node);
            if (count < bestCount) {
                bestCount = count;
                best = node;
            }
        }
        return best;
    }

    private long countMatchingVertices(final QueryNode node) {
        final String label = node.getLabel();
        if (label == null) {
            return graph.getVerticesCount();
        }
        long count = 0;
        final Iterator<Vertex> it = graph.vertices();
        while (it.hasNext()) {
            if (label.equals(it.next().label())) count++;
        }
        return count;
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
    private static Map<QueryNode, String> assignEffectiveVariables(final List<QueryNode> nodes) {
        final Map<QueryNode, String> vars = new IdentityHashMap<>();
        int anonCounter = 0;
        for (final QueryNode node : nodes) {
            if (node.getVariable() != null) {
                vars.put(node, node.getVariable());
            }
        }
        for (final QueryNode node : nodes) {
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
    private static List<ExtensionStep> buildSteps(final QueryGraph queryGraph,
                                                   final QueryNode seed,
                                                   final Map<QueryNode, String> effectiveVars) {
        final List<ExtensionStep> steps = new ArrayList<>();

        // visitOrder tracks the BFS discovery sequence; used to pick the anchor for back-edges
        final Map<QueryNode, Integer> visitOrder = new IdentityHashMap<>();
        final Queue<QueryNode> queue = new ArrayDeque<>();
        final Set<QueryEdge> processedEdges = Collections.newSetFromMap(new IdentityHashMap<>());

        visitOrder.put(seed, 0);
        queue.add(seed);
        int visitCounter = 1;

        while (!queue.isEmpty()) {
            final QueryNode current = queue.poll();
            final int currentOrder = visitOrder.get(current);

            for (final QueryEdge edge : queryGraph.getEdges()) {
                if (processedEdges.contains(edge)) continue;

                final boolean isSource = edge.getSource() == current;
                final boolean isTarget = edge.getTarget() == current;

                if (!isSource && !isTarget) continue;

                processedEdges.add(edge);

                final QueryNode anchor;
                final QueryNode targetNode;
                final Direction stepDir;

                if (isSource && isTarget) {
                    // Self-loop: traverse from current to itself
                    anchor = current;
                    targetNode = current;
                    stepDir = edge.getDirection();
                } else if (isSource) {
                    final QueryNode other = edge.getTarget();
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
                    final QueryNode other = edge.getSource();
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
                        targetNode.getLabel(),
                        effectiveVars.get(targetNode)));

                if (!visitOrder.containsKey(targetNode)) {
                    visitOrder.put(targetNode, visitCounter++);
                    queue.add(targetNode);
                }
            }
        }

        // Detect disconnected pattern components: every node in the query must have been
        // reached by the BFS. If any node was not visited, the query graph has more than one
        // connected component and the patterns cannot be joined by a shared variable.
        if (visitOrder.size() < queryGraph.getNodes().size()) {
            final List<String> unvisited = new ArrayList<>();
            for (final QueryNode n : queryGraph.getNodes()) {
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
