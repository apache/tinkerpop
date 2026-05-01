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
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerIndexHelper;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Executes a {@link GqlMatchPlan} against a TinkerGraph using a DFS backtracking
 * pattern matching algorithm.
 *
 * <h3>Algorithm</h3>
 * <ol>
 *   <li><strong>Seed iteration</strong> — vertices are scanned from the graph, filtered by the
 *       seed label and seed property predicates (if present). Each matching vertex is bound to
 *       the seed variable and used as the starting point for DFS extension.</li>
 *   <li><strong>DFS extension</strong> — {@link #extend} is called recursively with a shared
 *       {@code Element[]} binding array. Steps are selected from the remaining list using DAG
 *       eligibility (anchor variable already bound), with BFS order as the tiebreaker. Target
 *       property predicates are applied after the label check. Bindings are set in the array
 *       before recursing and cleared (set to {@code null}) on backtrack, avoiding any
 *       per-candidate HashMap allocation. When the traversed edge is anonymous, a per-step
 *       {@code HashSet} deduplicates parallel edges to the same target vertex so that
 *       multigraph parallel edges do not produce duplicate result rows.</li>
 *   <li><strong>Result emission</strong> — when all steps are consumed, a shallow clone of the
 *       binding array is added to the per-seed buffer.</li>
 *   <li><strong>Lazy delivery</strong> — {@link #execute} returns an {@code Iterator} that
 *       advances the DFS one seed vertex at a time. Results for a seed are buffered in an
 *       {@code ArrayDeque} and drained before the next seed is processed, so callers that apply
 *       an early termination (e.g. {@code limit()}) pay only for the work they consume.</li>
 * </ol>
 *
 * <p>Variable reuse across patterns is handled by the equality constraint: if the target
 * variable is already non-null in the binding array, the candidate target must equal the
 * existing binding. This enables multi-pattern joins such as
 * {@code MATCH (a)-[:KNOWS]->(b), (b)-[:WORKS_AT]->(c)} where {@code b} is shared.
 *
 * <p>Property predicates (from inline filter maps or parameter references) are evaluated
 * during seed iteration, edge traversal, and target vertex filtering. Edge predicates are checked
 * immediately after each candidate edge is retrieved and before the target vertex is resolved,
 * which prunes the search space before computing the target. Parameter values are resolved from
 * the {@code params} map passed to {@link #execute(GqlMatchPlan, Map)}.
 *
 * <p>This is a pure Java implementation using only the direct TinkerGraph and {@link Vertex}
 * APIs — no Gremlin traversal machinery is involved.
 */
public final class TinkerGraphGqlExecutor {

    private final AbstractTinkerGraph graph;

    public TinkerGraphGqlExecutor(final AbstractTinkerGraph graph) {
        this.graph = graph;
    }

    /**
     * Returns a lazy {@link Iterator} of result rows using an empty params map.
     * Equivalent to {@code execute(plan, Collections.emptyMap())}.
     *
     * @param plan the compiled execution plan produced by {@link TinkerGraphGqlPlanner}
     * @return a lazy iterator of binding arrays; empty if the plan has no seed
     */
    public Iterator<Element[]> execute(final GqlMatchPlan plan) {
        return execute(plan, Collections.emptyMap());
    }

    /**
     * Returns a lazy {@link Iterator} of result rows. Each row is an {@code Element[]} whose
     * indices correspond to the variable index defined in the {@link GqlMatchPlan}; use
     * {@link GqlMatchPlan#getVariables()} to map indices back to variable names.
     *
     * <p>The iterator advances the DFS one seed vertex at a time. Results for a given seed
     * are fully computed before the iterator moves to the next seed, so memory usage is
     * bounded by the maximum number of results a single seed vertex can produce.
     *
     * @param plan   the compiled execution plan produced by {@link TinkerGraphGqlPlanner}
     * @param params the parameter bindings for {@code $name} references in property predicates;
     *               may be empty if the query contains no parameter references
     * @return a lazy iterator of binding arrays; empty if the plan has no seed
     */
    public Iterator<Element[]> execute(final GqlMatchPlan plan, final Map<String, Object> params) {
        if (plan.getSeedVariable() == null) {
            return Collections.emptyIterator();
        }

        final Iterator<Vertex> seedVertices = seedIterator(plan.getSeedLabel(),
                plan.getSeedPredicates(), params);
        final ArrayDeque<Element[]> buffer = new ArrayDeque<>();

        return new Iterator<Element[]>() {
            @Override
            public boolean hasNext() {
                while (buffer.isEmpty() && seedVertices.hasNext()) {
                    final Vertex seed = seedVertices.next();
                    final Element[] bindings = new Element[plan.getVariableCount()];
                    bindings[plan.getSeedVariableIndex()] = seed;
                    extend(bindings, plan, params, new ArrayList<>(plan.getSteps()), buffer);
                }
                return !buffer.isEmpty();
            }

            @Override
            public Element[] next() {
                if (!hasNext()) throw new NoSuchElementException();
                return buffer.poll();
            }
        };
    }

    // -------------------------------------------------------------------------
    // Seed vertex iteration
    // -------------------------------------------------------------------------

    /**
     * Returns an iterator over seed vertex candidates. When the graph has a vertex index and
     * at least one seed predicate's key is indexed with a resolvable value (literal or present
     * param), the most selective such predicate is used for an O(result-set) index lookup
     * instead of a full vertex scan. The label constraint and all remaining predicates are
     * applied as a post-filter over the index results. Falls back to a full scan when no
     * usable indexed predicate exists.
     */
    private Iterator<Vertex> seedIterator(final String label,
                                          final List<PropertyPredicate> predicates,
                                          final Map<String, Object> params) {
        // Attempt index-based lookup if any seed predicate key is indexed.
        if (!predicates.isEmpty()) {
            final Set<String> indexedKeys = graph.getIndexedKeys(Vertex.class);
            if (!indexedKeys.isEmpty()) {
                PropertyPredicate bestPredicate = null;
                Object bestValue = null;
                long bestCount = Long.MAX_VALUE;

                for (final PropertyPredicate p : predicates) {
                    if (!indexedKeys.contains(p.getKey())) continue;
                    final Object value = p.isParamRef() ? params.get(p.getParamName()) : p.getLiteralValue();
                    if (value == null) continue; // param absent — cannot use index
                    final long count = TinkerIndexHelper.countVertexIndex(graph, p.getKey(), value);
                    if (count < bestCount) {
                        bestCount = count;
                        bestPredicate = p;
                        bestValue = value;
                    }
                }

                if (bestPredicate != null) {
                    final List<TinkerVertex> candidates =
                            TinkerIndexHelper.queryVertexIndex(graph, bestPredicate.getKey(), bestValue);
                    return candidates.stream()
                            .filter(v -> label == null || label.equals(v.label()))
                            .filter(v -> matchesPredicates(v, predicates, params))
                            .map(v -> (Vertex) v)
                            .iterator();
                }
            }
        }

        // Full scan fallback: filter by label and all predicates.
        final Iterator<Vertex> all = graph.vertices();
        return new Iterator<Vertex>() {
            private Vertex next = advance();

            private Vertex advance() {
                while (all.hasNext()) {
                    final Vertex v = all.next();
                    if (label != null && !label.equals(v.label())) continue;
                    if (!matchesPredicates(v, predicates, params)) continue;
                    return v;
                }
                return null;
            }

            @Override public boolean hasNext() { return next != null; }

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
     * Recursively extends the current partial match using DAG-aware step selection and
     * array-based bindings with in-place backtracking.
     *
     * <p>Among all steps in {@code remaining} whose anchor variable slot is non-null in
     * {@code bindings} (simultaneously eligible steps), the one with the lowest
     * {@link ExtensionStep#selectivityRatio()} is chosen first, with
     * {@link ExtensionStep#getEstimatedCost()} as a tiebreaker. This combines adaptive
     * runtime reordering (learned from observed hits/attempts ratios) with the planner's
     * static property-aware cost estimate. New variable bindings are written directly into
     * the shared {@code bindings} array and cleared on unwind, avoiding any HashMap
     * allocation during the DFS.</p>
     *
     * @param bindings  shared binding array (index → Element); mutated in place, backtracked on unwind
     * @param plan      the compiled plan supplying the variable index
     * @param params    parameter bindings for predicate resolution
     * @param remaining steps not yet executed; modified in place and restored on return
     * @param results   buffer into which complete binding-array snapshots are added
     */
    private void extend(final Element[] bindings, final GqlMatchPlan plan,
                        final Map<String, Object> params,
                        final List<ExtensionStep> remaining, final ArrayDeque<Element[]> results) {
        if (remaining.isEmpty()) {
            results.add(bindings.clone());
            return;
        }

        // Find the best eligible step: anchor slot non-null, lowest selectivityRatio() first,
        // estimatedCost as tiebreaker. This implements both static (planner-cost) and adaptive
        // (observed hits/attempts) step ordering when multiple DAG steps are simultaneously eligible.
        int chosenIdx = -1;
        double chosenRatio = Double.MAX_VALUE;
        long chosenCost = Long.MAX_VALUE;
        for (int i = 0; i < remaining.size(); i++) {
            final ExtensionStep candidate = remaining.get(i);
            if (bindings[plan.getIndex(candidate.getAnchorVariable())] == null) continue;
            final double ratio = candidate.selectivityRatio();
            final long cost = candidate.getEstimatedCost();
            if (chosenIdx < 0
                    || ratio < chosenRatio
                    || (ratio == chosenRatio && cost < chosenCost)) {
                chosenIdx = i;
                chosenRatio = ratio;
                chosenCost = cost;
            }
        }
        if (chosenIdx < 0) {
            // No eligible step — partial match is unsatisfiable at this point; backtrack.
            return;
        }

        final ExtensionStep step = remaining.remove(chosenIdx);
        step.recordAttempt();
        final Vertex anchor = (Vertex) bindings[plan.getIndex(step.getAnchorVariable())];

        final Iterator<Edge> candidates = step.getEdgeLabel() != null
                ? anchor.edges(step.getDirection(), step.getEdgeLabel())
                : anchor.edges(step.getDirection());

        // Resolve binding-array indices for this step's variables once (outside the loop).
        final int edgeIdx   = step.getEdgeVariable()   != null ? plan.getIndex(step.getEdgeVariable())   : -1;
        final int targetIdx = step.getTargetVariable() != null ? plan.getIndex(step.getTargetVariable()) : -1;

        // When the edge is anonymous, parallel edges between the same two vertices are
        // indistinguishable as bindings.  Track visited targets so we recurse at most once
        // per distinct target vertex via this step.
        final Set<Vertex> seenAnonymousTargets = edgeIdx < 0 ? new HashSet<>() : null;

        while (candidates.hasNext()) {
            final Edge edge = candidates.next();

            if (!matchesPredicates(edge, step.getEdgePredicates(), params))
                continue;

            // Enforce edge equality constraint: if the edge variable is already bound (from a
            // prior pattern that reuses the same variable name), the candidate edge must be
            // the same object.
            if (edgeIdx >= 0 && bindings[edgeIdx] != null && !bindings[edgeIdx].equals(edge))
                continue;

            final Vertex target = targetVertex(anchor, edge, step.getDirection());

            if (step.getTargetLabel() != null && !step.getTargetLabel().equals(target.label()))
                continue;

            if (!matchesPredicates(target, step.getTargetPredicates(), params))
                continue;

            // Track whether we are writing the edge binding ourselves (vs. it already being set).
            final boolean writeEdge = edgeIdx >= 0 && bindings[edgeIdx] == null;

            if (targetIdx >= 0 && bindings[targetIdx] != null) {
                // Equality constraint: target variable already bound — candidate must match.
                if (!bindings[targetIdx].equals(target)) continue;

                if (writeEdge) {
                    // Named edge not yet bound: bind, recurse, clear.
                    bindings[edgeIdx] = edge;
                    step.recordHit();
                    extend(bindings, plan, params, remaining, results);
                    bindings[edgeIdx] = null;
                } else {
                    // Edge already bound (equality checked above) or anonymous edge.
                    // Anonymous edge: one matching edge is sufficient to establish
                    // connectivity — break after the first.
                    step.recordHit();
                    extend(bindings, plan, params, remaining, results);
                    if (edgeIdx < 0) break;
                }
            } else {
                // Anonymous edge: parallel edges to the same target vertex yield identical
                // bindings — skip if this target was already reached via an earlier parallel edge.
                if (seenAnonymousTargets != null && !seenAnonymousTargets.add(target)) continue;

                // New target binding (and edge binding if not yet set): write, recurse, clear.
                if (writeEdge)      bindings[edgeIdx]  = edge;
                if (targetIdx >= 0) bindings[targetIdx] = target;
                step.recordHit();
                extend(bindings, plan, params, remaining, results);
                if (writeEdge)      bindings[edgeIdx]  = null;
                if (targetIdx >= 0) bindings[targetIdx] = null;
            }
        }

        remaining.add(chosenIdx, step);
    }

    // -------------------------------------------------------------------------
    // Predicate evaluation
    // -------------------------------------------------------------------------

    private static boolean matchesPredicates(final Element element,
                                             final List<PropertyPredicate> predicates,
                                             final Map<String, Object> params) {
        for (final PropertyPredicate p : predicates) {
            if (!p.test(element, params)) return false;
        }
        return true;
    }

    // -------------------------------------------------------------------------
    // Direction-aware target vertex resolution
    // -------------------------------------------------------------------------

    private static Vertex targetVertex(final Vertex anchor, final Edge edge, final Direction direction) {
        switch (direction) {
            case OUT:  return edge.inVertex();
            case IN:   return edge.outVertex();
            default:   // BOTH
                final Vertex out = edge.outVertex();
                return out.equals(anchor) ? edge.inVertex() : out;
        }
    }
}
