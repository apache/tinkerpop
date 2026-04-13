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
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.AbstractTinkerGraph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Executes a {@link GqlMatchPlan} against a {@link TinkerGraph} using a DFS backtracking
 * pattern matching algorithm.
 *
 * <h3>Algorithm</h3>
 * <ol>
 *   <li><strong>Seed iteration</strong> — vertices are scanned from the graph, filtered by the
 *       seed label (if present). Each matching vertex is bound to the seed variable and used
 *       as the starting point for DFS extension.</li>
 *   <li><strong>DFS extension</strong> — {@link #extend} is called recursively with the current
 *       binding map and the remaining {@link ExtensionStep} list. Each step:
 *       <ul>
 *         <li>Looks up the anchor vertex by its variable name in the current bindings.</li>
 *         <li>Calls {@link Vertex#edges(Direction, String...)} on the anchor to get candidate
 *             edges, optionally filtered by the step's edge label.</li>
 *         <li>For each candidate edge, determines the target vertex from the opposite end.</li>
 *         <li>Applies the target label constraint (if set) and the equality constraint (if the
 *             target variable is already bound in the current row).</li>
 *         <li>If all constraints pass, adds the edge and target vertex to a new binding map
 *             and recurses with the remaining steps.</li>
 *       </ul>
 *   </li>
 *   <li><strong>Result collection</strong> — when all steps have been consumed, the complete
 *       binding map is copied into the result list.</li>
 * </ol>
 *
 * <p>Variable reuse across patterns is handled naturally: if the target variable is already
 * bound, the candidate target must equal the existing binding (equality constraint). This
 * enables pattern joins such as
 * {@code MATCH (a)-[:KNOWS]->(b), (b)-[:WORKS_AT]->(c)} where {@code b} is shared.
 *
 * <p>This is a pure Java implementation using only the direct {@link TinkerGraph} and
 * {@link Vertex} APIs — no Gremlin traversal machinery is involved.
 */
public final class TinkerGraphGqlExecutor {

    private final AbstractTinkerGraph graph;

    public TinkerGraphGqlExecutor(final AbstractTinkerGraph graph) {
        this.graph = graph;
    }

    /**
     * Executes the given plan against the graph and returns all matching result rows.
     *
     * <p>Each result row is a {@link Map} from variable name to the matching {@link Element}
     * (either a {@link Vertex} or an {@link Edge}). Anonymous seed nodes (variables starting
     * with {@code $anon}) are included in the map but are typically filtered out by callers.
     *
     * @param plan the compiled execution plan produced by {@link TinkerGraphGqlPlanner}
     * @return an unmodifiable list of binding maps; empty if no matches are found
     */
    public List<Map<String, Element>> execute(final GqlMatchPlan plan) {
        if (plan.getSeedVariable() == null) {
            return Collections.emptyList();
        }

        final List<Map<String, Element>> results = new ArrayList<>();
        final Iterator<Vertex> seedVertices = seedIterator(plan.getSeedLabel());

        while (seedVertices.hasNext()) {
            final Vertex seed = seedVertices.next();
            final Map<String, Element> bindings = new HashMap<>();
            bindings.put(plan.getSeedVariable(), seed);
            extend(bindings, plan.getSteps(), 0, results);
        }

        return Collections.unmodifiableList(results);
    }

    // -------------------------------------------------------------------------
    // Seed vertex iteration
    // -------------------------------------------------------------------------

    private Iterator<Vertex> seedIterator(final String label) {
        final Iterator<Vertex> all = graph.vertices();
        if (label == null) {
            return all;
        }
        // Filter by label without pulling all vertices into a collection
        return new Iterator<Vertex>() {
            private Vertex next = advance();

            private Vertex advance() {
                while (all.hasNext()) {
                    final Vertex v = all.next();
                    if (label.equals(v.label())) return v;
                }
                return null;
            }

            @Override
            public boolean hasNext() {
                return next != null;
            }

            @Override
            public Vertex next() {
                final Vertex result = next;
                next = advance();
                return result;
            }
        };
    }

    // -------------------------------------------------------------------------
    // DFS extension
    // -------------------------------------------------------------------------

    /**
     * Recursively extends the current partial match by processing {@code steps[index]}.
     * When all steps have been consumed ({@code index == steps.size()}), the complete
     * binding map is added to {@code results}.
     *
     * @param bindings     the current partial binding map (not modified; a copy is made for each candidate)
     * @param steps        the full ordered list of extension steps from the plan
     * @param index        the index of the next step to process
     * @param results      accumulator for complete matches
     */
    private void extend(final Map<String, Element> bindings,
                        final List<ExtensionStep> steps,
                        final int index,
                        final List<Map<String, Element>> results) {
        if (index == steps.size()) {
            results.add(new HashMap<>(bindings));
            return;
        }

        final ExtensionStep step = steps.get(index);
        final Vertex anchor = (Vertex) bindings.get(step.getAnchorVariable());

        // Retrieve candidate edges from the anchor, filtered by edge label if present
        final Iterator<Edge> candidates;
        if (step.getEdgeLabel() != null) {
            candidates = anchor.edges(step.getDirection(), step.getEdgeLabel());
        } else {
            candidates = anchor.edges(step.getDirection());
        }

        while (candidates.hasNext()) {
            final Edge edge = candidates.next();
            final Vertex target = targetVertex(anchor, edge, step.getDirection());

            // Apply target label constraint
            if (step.getTargetLabel() != null && !step.getTargetLabel().equals(target.label())) {
                continue;
            }

            // Apply equality constraint: if the target variable is already bound, the
            // candidate target must be the same element
            final String targetVar = step.getTargetVariable();
            if (targetVar != null && bindings.containsKey(targetVar)) {
                if (!bindings.get(targetVar).equals(target)) {
                    continue;
                }
                // Constraint satisfied — no need to rebind; recurse with unchanged bindings
                extendWithEdge(bindings, edge, step.getEdgeVariable(), null, null, steps, index, results);
            } else {
                extendWithEdge(bindings, edge, step.getEdgeVariable(), target, targetVar, steps, index, results);
            }
        }
    }

    /**
     * Creates a new binding map that adds the edge (and optionally the target vertex)
     * to the current bindings, then recurses to the next step.
     */
    private void extendWithEdge(final Map<String, Element> bindings,
                                 final Edge edge,
                                 final String edgeVar,
                                 final Vertex target,
                                 final String targetVar,
                                 final List<ExtensionStep> steps,
                                 final int index,
                                 final List<Map<String, Element>> results) {
        final boolean needsNewMap = (edgeVar != null) || (targetVar != null && target != null);
        final Map<String, Element> next = needsNewMap ? new HashMap<>(bindings) : bindings;

        if (edgeVar != null) {
            next.put(edgeVar, edge);
        }
        if (targetVar != null && target != null) {
            next.put(targetVar, target);
        }

        extend(next, steps, index + 1, results);
    }

    // -------------------------------------------------------------------------
    // Direction-aware target vertex resolution
    // -------------------------------------------------------------------------

    /**
     * Returns the "other end" vertex of the edge relative to the anchor. For an
     * {@code OUT} step, the anchor is the out-vertex so the target is the in-vertex,
     * and vice versa. For {@code BOTH}, the end that is not the anchor is the target.
     */
    private static Vertex targetVertex(final Vertex anchor, final Edge edge, final Direction direction) {
        switch (direction) {
            case OUT:
                return edge.inVertex();
            case IN:
                return edge.outVertex();
            default: // BOTH
                final Vertex out = edge.outVertex();
                return out.equals(anchor) ? edge.inVertex() : out;
        }
    }
}
