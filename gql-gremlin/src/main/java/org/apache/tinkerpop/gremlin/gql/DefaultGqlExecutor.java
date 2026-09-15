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

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

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
 * Executes a {@link GqlMatchPlan} against any TinkerPop {@link Graph} using a DFS
 * backtracking pattern matching algorithm.
 *
 * <h3>Algorithm</h3>
 * <ol>
 *   <li><strong>Seed iteration</strong> — vertices are scanned from the graph (or looked up
 *       via {@link Graph.Index} when available), filtered by the seed label and seed property
 *       predicates. Each matching vertex is bound to the seed variable and used as the
 *       starting point for DFS extension.</li>
 *   <li><strong>DFS extension</strong>: {@link #extend} is called recursively with a shared
 *       {@link MatchState} holding the current partial match. Steps are selected from the
 *       remaining list using DAG eligibility (anchor variable already bound), with BFS order as
 *       the tiebreaker.</li>
 *   <li><strong>Lazy delivery</strong> — {@link #execute} returns an {@code Iterator} that
 *       advances the DFS one seed vertex at a time.</li>
 * </ol>
 *
 * <p>Index access is provided via {@link Graph#index()}. When
 * {@link Graph.Index#countVertexIndex(String, Object)} returns a value less than
 * {@code Long.MAX_VALUE} for a seed predicate key, an index lookup replaces the full vertex
 * scan. When all counts equal {@code Long.MAX_VALUE} (no index), a full scan is used.</p>
 */
public final class DefaultGqlExecutor implements GqlExecutor {

    private final Graph graph;

    public DefaultGqlExecutor(final Graph graph) {
        this.graph = graph;
    }

    /**
     * Returns a lazy {@link Iterator} of result rows using an empty params map.
     */
    @Override
    public Iterator<GqlRow> execute(final GqlMatchPlan plan) {
        return execute(plan, Collections.emptyMap());
    }

    /**
     * Returns a lazy {@link Iterator} of result rows. Each {@link GqlRow} carries a scalar
     * {@code Element[]} ({@code row.scalars}) whose indices correspond to the variable index
     * defined in the {@link GqlMatchPlan}, plus a side-channel map ({@code row.groups}) from
     * variable index to the ordered {@code List<Edge>} bound to a quantified group edge variable.
     */
    @Override
    public Iterator<GqlRow> execute(final GqlMatchPlan plan, final Map<String, Object> params) {
        if (plan.getSeedVariable() == null) {
            return Collections.emptyIterator();
        }

        final Iterator<Vertex> seedVertices = seedIterator(plan.getSeedLabel(),
                plan.getSeedPredicates(), params);
        final ArrayDeque<GqlRow> buffer = new ArrayDeque<>();

        return new Iterator<GqlRow>() {
            @Override
            public boolean hasNext() {
                while (buffer.isEmpty() && seedVertices.hasNext()) {
                    final Vertex seed = seedVertices.next();
                    final MatchState state = new MatchState(plan.getVariableCount());
                    state.setScalar(plan.getSeedVariableIndex(), seed);
                    extend(state, plan, params,
                            new ArrayList<>(plan.getSteps()), buffer);
                }
                return !buffer.isEmpty();
            }

            @Override
            public GqlRow next() {
                if (!hasNext()) throw new NoSuchElementException();
                return buffer.poll();
            }
        };
    }

    // -------------------------------------------------------------------------
    // Seed vertex iteration
    // -------------------------------------------------------------------------

    /**
     * Returns an iterator over seed vertex candidates. When {@link Graph#index()} returns
     * a count less than {@code Long.MAX_VALUE} for a seed predicate key, the most selective
     * such predicate is used for an index lookup instead of a full vertex scan. Falls back
     * to a full scan when no usable indexed predicate exists.
     */
    private Iterator<Vertex> seedIterator(final String label,
                                          final List<PropertyPredicate> predicates,
                                          final Map<String, Object> params) {
        // Attempt index-based lookup if any seed predicate key is indexed.
        if (!predicates.isEmpty()) {
            PropertyPredicate bestPredicate = null;
            Object bestValue = null;
            long bestCount = Long.MAX_VALUE;

            for (final PropertyPredicate p : predicates) {
                final Object value = p.isParamRef() ? params.get(p.getParamName()) : p.getLiteralValue();
                if (value == null) continue; // param absent or null literal — cannot use index
                final long count = graph.index().countVertexIndex(p.getKey(), value);
                if (count < bestCount) { // Long.MAX_VALUE means "not indexed"
                    bestCount = count;
                    bestPredicate = p;
                    bestValue = value;
                }
            }

            if (bestPredicate != null && bestCount < Long.MAX_VALUE) {
                final Iterator<Vertex> indexResults =
                        graph.index().queryVertexIndex(bestPredicate.getKey(), bestValue);
                return new Iterator<Vertex>() {
                    private Vertex next = advance();

                    private Vertex advance() {
                        while (indexResults.hasNext()) {
                            final Vertex v = indexResults.next();
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
     * array-based bindings with in-place backtracking. The current match is carried in a single
     * {@link MatchState}, which keeps scalar variable bindings in a flat array and group
     * edge-variable bindings in a side map.
     */
    private void extend(final MatchState state,
                        final GqlMatchPlan plan, final Map<String, Object> params,
                        final List<ExtensionStep> remaining, final ArrayDeque<GqlRow> results) {
        if (remaining.isEmpty()) {
            results.add(state.snapshot());
            return;
        }

        // Find the best eligible step: anchor slot non-null, lowest selectivityRatio() first,
        // estimatedCost as tiebreaker.
        int chosenIdx = -1;
        double chosenRatio = Double.MAX_VALUE;
        long chosenCost = Long.MAX_VALUE;
        for (int i = 0; i < remaining.size(); i++) {
            final ExtensionStep candidate = remaining.get(i);
            if (state.getScalar(plan.getIndex(candidate.getAnchorVariable())) == null) continue;
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
        final Vertex anchor = (Vertex) state.getScalar(plan.getIndex(step.getAnchorVariable()));

        // Quantified (variable-length) step: expand as a bounded/unbounded multi-hop walk with
        // per-walk TRAIL edge-uniqueness and emit-at-each-depth. See extendQuantified(...).
        if (step.isQuantified()) {
            final int qEdgeIdx   = step.getEdgeVariable()   != null ? plan.getIndex(step.getEdgeVariable())   : -1;
            final int qTargetIdx = step.getTargetVariable() != null ? plan.getIndex(step.getTargetVariable()) : -1;
            extendQuantified(state, plan, params, remaining, results, step, anchor,
                    qEdgeIdx, qTargetIdx, new ArrayDeque<>(), new HashSet<>(), 0);
            remaining.add(chosenIdx, step);
            return;
        }

        final Iterator<Edge> candidates = step.getEdgeLabel() != null
                ? anchor.edges(step.getDirection(), step.getEdgeLabel())
                : anchor.edges(step.getDirection());

        final int edgeIdx   = step.getEdgeVariable()   != null ? plan.getIndex(step.getEdgeVariable())   : -1;
        final int targetIdx = step.getTargetVariable() != null ? plan.getIndex(step.getTargetVariable()) : -1;

        // A group edge variable always surfaces as a List<Edge>, so it is written through the
        // group channel and never participates in the scalar edge-reuse join below.
        final boolean edgeIsGroup = step.isEdgeVariableGroup() && edgeIdx >= 0;

        final Set<Vertex> seenAnonymousTargets = edgeIdx < 0 ? new HashSet<>() : null;

        while (candidates.hasNext()) {
            final Edge edge = candidates.next();

            if (!matchesPredicates(edge, step.getEdgePredicates(), params))
                continue;

            if (!edgeIsGroup && edgeIdx >= 0 && state.getScalar(edgeIdx) != null && !state.getScalar(edgeIdx).equals(edge))
                continue;

            final Vertex target = targetVertex(anchor, edge, step.getDirection());

            if (step.getTargetLabel() != null && !step.getTargetLabel().equals(target.label()))
                continue;

            if (!matchesPredicates(target, step.getTargetPredicates(), params))
                continue;

            final boolean writeEdge = !edgeIsGroup && edgeIdx >= 0 && state.getScalar(edgeIdx) == null;

            if (targetIdx >= 0 && state.getScalar(targetIdx) != null) {
                if (!state.getScalar(targetIdx).equals(target)) continue;

                if (edgeIsGroup) {
                    state.putGroup(edgeIdx, java.util.List.of(edge));
                    step.recordHit();
                    extend(state, plan, params, remaining, results);
                    state.removeGroup(edgeIdx);
                } else if (writeEdge) {
                    state.setScalar(edgeIdx, edge);
                    step.recordHit();
                    extend(state, plan, params, remaining, results);
                    state.clearScalar(edgeIdx);
                } else {
                    step.recordHit();
                    extend(state, plan, params, remaining, results);
                    if (edgeIdx < 0) break;
                }
            } else {
                if (seenAnonymousTargets != null && !seenAnonymousTargets.add(target)) continue;

                if (edgeIsGroup)    state.putGroup(edgeIdx, java.util.List.of(edge));
                else if (writeEdge) state.setScalar(edgeIdx, edge);
                if (targetIdx >= 0) state.setScalar(targetIdx, target);
                step.recordHit();
                extend(state, plan, params, remaining, results);
                if (edgeIsGroup)    state.removeGroup(edgeIdx);
                else if (writeEdge) state.clearScalar(edgeIdx);
                if (targetIdx >= 0) state.clearScalar(targetIdx);
            }
        }

        remaining.add(chosenIdx, step);
    }

    /**
     * Expands a quantified (variable-length) {@link ExtensionStep} from {@code anchor} as a
     * depth-first walk, emitting a match at every depth in {@code [minHops, maxHops]}.
     *
     * <p>The walk enforces per-walk TRAIL edge-uniqueness: an edge is never traversed twice
     * within the same walk (tracked in {@code visitedEdges}, with {@code path} preserving order).
     * Vertices are deliberately not gated, so they may repeat via distinct edges such as parallel
     * edges, triangles, and self-loops. Because edges cannot repeat, an unbounded walk always
     * terminates on a finite graph, so no depth cap is needed.</p>
     *
     * <p>The group edge list is carried in the {@link MatchState} side map keyed by
     * {@code edgeIdx} rather than in the scalar array, so the scalar bindings stay homogeneous.
     * Its save/restore mirrors the scalar save/restore so backtracking never leaks a walk's edge
     * list into a sibling branch.</p>
     *
     * @param depth number of edges already on {@code path} (walk depth of {@code current})
     */
    private void extendQuantified(final MatchState state,
                                  final GqlMatchPlan plan, final Map<String, Object> params,
                                  final List<ExtensionStep> remaining, final ArrayDeque<GqlRow> results,
                                  final ExtensionStep step, final Vertex current,
                                  final int edgeIdx, final int targetIdx,
                                  final ArrayDeque<Edge> path, final Set<Edge> visitedEdges,
                                  final int depth) {
        final int minHops = step.getMinHops();
        final int maxHops = step.getMaxHops();

        final Iterator<Edge> candidates = step.getEdgeLabel() != null
                ? current.edges(step.getDirection(), step.getEdgeLabel())
                : current.edges(step.getDirection());

        while (candidates.hasNext()) {
            final Edge edge = candidates.next();

            // TRAIL edge-uniqueness (walk-local): never traverse the same edge twice.
            if (visitedEdges.contains(edge))
                continue;

            if (!matchesPredicates(edge, step.getEdgePredicates(), params))
                continue;

            final Vertex next = targetVertex(current, edge, step.getDirection());

            path.addLast(edge);
            visitedEdges.add(edge);
            final int newDepth = depth + 1;

            // Emit at this depth when it is within [minHops, maxHops] and the target constraints
            // on `next` pass. Emit-at-each-depth: a single branch may emit at multiple depths.
            if (newDepth >= minHops
                    && (step.getTargetLabel() == null || step.getTargetLabel().equals(next.label()))
                    && matchesPredicates(next, step.getTargetPredicates(), params)) {

                final boolean targetPrebound = targetIdx >= 0 && state.getScalar(targetIdx) != null;
                if (!targetPrebound || state.getScalar(targetIdx).equals(next)) {
                    final Element savedTarget = targetIdx >= 0 ? state.getScalar(targetIdx) : null;

                    if (targetIdx >= 0) state.setScalar(targetIdx, next);

                    // Group edge variable: carry the ordered walk edges in the side map, not in
                    // the scalar array. Skip entirely for an anonymous quantified edge.
                    final boolean writeGroup = edgeIdx >= 0;
                    final List<Edge> savedGroup = writeGroup ? state.getGroup(edgeIdx) : null;
                    if (writeGroup) state.putGroup(edgeIdx, List.copyOf(path));

                    step.recordHit();
                    extend(state, plan, params, remaining, results);

                    if (writeGroup) {
                        if (savedGroup != null) state.putGroup(edgeIdx, savedGroup);
                        else                    state.removeGroup(edgeIdx);
                    }
                    if (targetIdx >= 0) state.setScalar(targetIdx, savedTarget);
                }
            }

            // Descend one hop deeper when below the (possibly unbounded) upper bound. A shorter
            // path that failed the target constraint may still be a prefix of a longer match.
            if (maxHops == ExtensionStep.UNBOUNDED || newDepth < maxHops) {
                extendQuantified(state, plan, params, remaining, results, step, next,
                        edgeIdx, targetIdx, path, visitedEdges, newDepth);
            }

            // Backtrack the walk.
            visitedEdges.remove(edge);
            path.removeLast();
        }
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
