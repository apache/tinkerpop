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

import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
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
 *   <li><strong>Seed iteration</strong>: vertices are scanned from the graph (or looked up
 *       via {@link Graph.Index} when available), filtered by the seed label and seed property
 *       predicates. Each matching vertex is bound to the seed variable and used as the
 *       starting point for DFS extension.</li>
 *   <li><strong>DFS extension</strong>: the search runs on an explicit stack rather than by
 *       recursion, with a shared {@link MatchState} holding the current partial match. Each
 *       stack frame is either one pending plain step expansion or one hop of a quantified walk,
 *       and holds its candidate edge iterator plus what to undo on backtrack. Steps are selected
 *       from the remaining list using DAG eligibility (anchor variable already bound), with
 *       observed selectivity and then estimated cost as the tiebreakers.</li>
 *   <li><strong>Lazy delivery</strong>: {@link #execute} returns an {@code Iterator} that runs the
 *       DFS only until the next complete match, so each {@code hasNext()}/{@code next()} pair
 *       does the work for one row and nothing is buffered. Stack depth is bounded by heap, not
 *       by the thread stack, so long variable-length walks do not overflow.</li>
 * </ol>
 *
 * <p>The search checks {@link Thread#interrupted()} as it iterates and throws
 * {@link TraversalInterruptedException} when the thread is interrupted, so a traversal timeout
 * can stop a long-running match.</p>
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
     * Returns a lazy {@link Iterator} of result rows. Each {@link GqlRow} is keyed by the variable
     * indices defined in the {@link GqlMatchPlan}; see {@link GqlRow} for the binding contract.
     *
     * <p>Rows are produced on demand: each {@code hasNext()} advances the search only until the
     * next match, so a consumer that stops early (for example under {@code limit(1)}) does not pay
     * for the rest of the search. Iteration throws {@link TraversalInterruptedException} if the
     * calling thread is interrupted.</p>
     */
    @Override
    public Iterator<GqlRow> execute(final GqlMatchPlan plan, final Map<String, Object> params) {
        if (plan.getSeedVariable() == null) {
            return Collections.emptyIterator();
        }
        return new MatchIterator(plan, params,
                seedIterator(plan.getSeedLabel(), plan.getSeedPredicates(), params));
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
                if (value == null) continue; // param absent or null literal: cannot use index
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
                            if (Thread.interrupted()) throw new TraversalInterruptedException();
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
                    if (Thread.interrupted()) throw new TraversalInterruptedException();
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
     * Pull-based DFS over the seeds of one plan. The partial match lives in a single
     * {@link MatchState}, which keeps scalar variable bindings in a flat array and group
     * edge-variable bindings in a side map, and the search position lives in {@link #stack}.
     * {@link #hasNext()} advances the top frame until a complete match is snapshotted into
     * {@link #pending}, then returns, leaving the stack positioned to resume from that match.
     */
    private final class MatchIterator implements Iterator<GqlRow> {
        private final GqlMatchPlan plan;
        private final Map<String, Object> params;
        private final Iterator<Vertex> seeds;
        private final ArrayDeque<Frame> stack = new ArrayDeque<>();
        private MatchState state;
        private List<ExtensionStep> remaining;
        private GqlRow pending;

        private MatchIterator(final GqlMatchPlan plan, final Map<String, Object> params,
                              final Iterator<Vertex> seeds) {
            this.plan = plan;
            this.params = params;
            this.seeds = seeds;
        }

        @Override
        public boolean hasNext() {
            while (pending == null) {
                if (Thread.interrupted()) throw new TraversalInterruptedException();
                if (stack.isEmpty()) {
                    if (!seeds.hasNext()) return false;
                    state = new MatchState(plan.getVariableCount());
                    state.setScalar(plan.getSeedVariableIndex(), seeds.next());
                    remaining = new ArrayList<>(plan.getSteps());
                    extend();
                } else {
                    final Frame top = stack.peek();
                    if (!top.advance()) {
                        stack.pop();
                        top.close();
                    }
                }
            }
            return true;
        }

        @Override
        public GqlRow next() {
            if (!hasNext()) throw new NoSuchElementException();
            final GqlRow row = pending;
            pending = null;
            return row;
        }

        /**
         * Extends the current partial match by one step. With no steps remaining the match is
         * complete and is snapshotted into {@link #pending}. Otherwise the best eligible step
         * (anchor bound, lowest {@code selectivityRatio()}, then lowest {@code estimatedCost}) is
         * removed from {@link #remaining} and a frame that expands it is pushed. When no step is
         * eligible the partial match is unsatisfiable and nothing is pushed, which backtracks.
         */
        private void extend() {
            if (remaining.isEmpty()) {
                pending = state.snapshot();
                return;
            }

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
            if (chosenIdx < 0) return;

            final ExtensionStep step = remaining.remove(chosenIdx);
            step.recordAttempt();
            final Vertex anchor = (Vertex) state.getScalar(plan.getIndex(step.getAnchorVariable()));
            final int edgeIdx   = step.getEdgeVariable()   != null ? plan.getIndex(step.getEdgeVariable())   : -1;
            final int targetIdx = step.getTargetVariable() != null ? plan.getIndex(step.getTargetVariable()) : -1;

            // Every quantified step, including {1} and {1,1}, is expanded as a walk with per-walk
            // TRAIL edge-uniqueness and emit-at-each-depth. See WalkFrame.
            if (step.isQuantified())
                stack.push(new WalkFrame(new Walk(step, chosenIdx, edgeIdx, targetIdx), anchor, 0));
            else
                stack.push(new StepFrame(step, chosenIdx, anchor, edgeIdx, targetIdx));
        }

        /** One level of the explicit DFS stack. */
        private abstract class Frame {
            /**
             * Undoes the bindings made for the previous candidate and binds the next one,
             * calling {@link #extend()} for it. Returns {@code false} once the frame has no more
             * candidates, with its own bindings already undone.
             */
            abstract boolean advance();

            /** Called once when the frame is popped, after {@link #advance()} returned {@code false}. */
            abstract void close();
        }

        /** Expands a plain, single-hop step from its anchor, one candidate edge per advance. */
        private final class StepFrame extends Frame {
            private final ExtensionStep step;
            private final int stepIdx;
            private final Vertex anchor;
            private final int edgeIdx;
            private final int targetIdx;
            private final Iterator<Edge> candidates;
            private final Set<Vertex> seenAnonymousTargets;
            private boolean boundEdge;
            private boolean boundTarget;
            private boolean stopped;

            StepFrame(final ExtensionStep step, final int stepIdx, final Vertex anchor,
                      final int edgeIdx, final int targetIdx) {
                this.step = step;
                this.stepIdx = stepIdx;
                this.anchor = anchor;
                this.edgeIdx = edgeIdx;
                this.targetIdx = targetIdx;
                this.candidates = candidateEdges(anchor, step);
                this.seenAnonymousTargets = edgeIdx < 0 ? new HashSet<>() : null;
            }

            @Override
            boolean advance() {
                if (boundEdge)   state.clearScalar(edgeIdx);
                if (boundTarget) state.clearScalar(targetIdx);
                boundEdge = false;
                boundTarget = false;
                if (stopped) return false;

                while (candidates.hasNext()) {
                    if (Thread.interrupted()) throw new TraversalInterruptedException();
                    final Edge edge = candidates.next();

                    if (!matchesPredicates(edge, step.getEdgePredicates(), params))
                        continue;

                    if (edgeIdx >= 0 && state.getScalar(edgeIdx) != null && !state.getScalar(edgeIdx).equals(edge))
                        continue;

                    final Vertex target = targetVertex(anchor, edge, step.getDirection());

                    if (!matchesTarget(target, step, params))
                        continue;

                    final boolean writeEdge = edgeIdx >= 0 && state.getScalar(edgeIdx) == null;

                    if (targetIdx >= 0 && state.getScalar(targetIdx) != null) {
                        if (!state.getScalar(targetIdx).equals(target)) continue;

                        if (writeEdge) {
                            state.setScalar(edgeIdx, edge);
                            boundEdge = true;
                        } else if (edgeIdx < 0) {
                            // Anonymous edge into an already-bound target: one row is enough.
                            stopped = true;
                        }
                    } else {
                        if (seenAnonymousTargets != null && !seenAnonymousTargets.add(target)) continue;

                        if (writeEdge) {
                            state.setScalar(edgeIdx, edge);
                            boundEdge = true;
                        }
                        if (targetIdx >= 0) {
                            state.setScalar(targetIdx, target);
                            boundTarget = true;
                        }
                    }
                    step.recordHit();
                    extend();
                    return true;
                }
                return false;
            }

            @Override
            void close() {
                remaining.add(stepIdx, step);
            }
        }

        /** State shared by every hop of one quantified walk. */
        private final class Walk {
            private final ExtensionStep step;
            private final int stepIdx;
            private final int edgeIdx;
            private final int targetIdx;
            private final ArrayDeque<Edge> path = new ArrayDeque<>();
            private final Set<Edge> visitedEdges = new HashSet<>();

            Walk(final ExtensionStep step, final int stepIdx, final int edgeIdx, final int targetIdx) {
                this.step = step;
                this.stepIdx = stepIdx;
                this.edgeIdx = edgeIdx;
                this.targetIdx = targetIdx;
            }
        }

        /**
         * One hop of a quantified (variable-length) step: the walk is at {@code current} after
         * {@code depth} edges, and each advance moves it through the next candidate edge. A
         * match is emitted at every depth in {@code [minHops, maxHops]}.
         *
         * <p>The walk enforces per-walk TRAIL edge-uniqueness: an edge is never traversed twice
         * within the same walk (tracked in {@code visitedEdges}, with {@code path} preserving
         * order). Vertices are deliberately not gated, so they may repeat via distinct edges such
         * as parallel edges, triangles, and self-loops. Because edges cannot repeat, an unbounded
         * walk always terminates on a finite graph. The number of distinct walks, and so the
         * cost, can still grow exponentially for unbounded forms on dense or cyclic graphs.</p>
         *
         * <p>For each candidate edge the frame first emits at the new depth, running the rest of
         * the pattern against that binding, and then descends one hop deeper by pushing a child
         * frame. A shorter path that fails the target check may still be a prefix of a longer
         * match, so the walk descends regardless of whether it emitted.</p>
         *
         * <p>The group edge list is carried in the {@link MatchState} side map keyed by
         * {@code edgeIdx} rather than in the scalar array, so the scalar bindings stay homogeneous.
         * Its save/restore mirrors the scalar save/restore so backtracking never leaks a walk's
         * edge list into a sibling branch. The list is always in pattern order, left to right:
         * when the step is {@linkplain ExtensionStep#isAnchoredAtPatternTarget() anchored at the
         * pattern edge's target} the walk runs right to left, so each new edge is prepended to
         * {@code path} rather than appended.</p>
         */
        private final class WalkFrame extends Frame {
            private static final int NEXT_EDGE = 0;
            private static final int EMIT = 1;
            private static final int DESCEND = 2;

            private final Walk walk;
            private final Vertex current;
            private final int depth;
            private final Iterator<Edge> candidates;
            private int phase = NEXT_EDGE;
            private Edge edge;
            private Vertex next;
            private boolean emitted;
            private Element savedTarget;
            private List<Edge> savedGroup;

            WalkFrame(final Walk walk, final Vertex current, final int depth) {
                this.walk = walk;
                this.current = current;
                this.depth = depth;
                this.candidates = candidateEdges(current, walk.step);
            }

            @Override
            boolean advance() {
                final ExtensionStep step = walk.step;
                while (true) {
                    switch (phase) {
                        case EMIT:
                            phase = DESCEND;
                            if (emit()) {
                                step.recordHit();
                                extend();
                                return true;
                            }
                            break;
                        case DESCEND:
                            unbindEmit();
                            phase = NEXT_EDGE;
                            if (step.getMaxHops() == QueryEdge.UNBOUNDED || depth + 1 < step.getMaxHops()) {
                                stack.push(new WalkFrame(walk, next, depth + 1));
                                return true;
                            }
                            break;
                        default:
                            popEdge();
                            if (!pushNextEdge()) return false;
                            phase = EMIT;
                    }
                }
            }

            /** Moves the walk onto the next untraversed candidate edge that passes the edge predicates. */
            private boolean pushNextEdge() {
                final ExtensionStep step = walk.step;
                while (candidates.hasNext()) {
                    if (Thread.interrupted()) throw new TraversalInterruptedException();
                    final Edge candidate = candidates.next();

                    // TRAIL edge-uniqueness (walk-local): never traverse the same edge twice.
                    if (walk.visitedEdges.contains(candidate))
                        continue;

                    if (!matchesPredicates(candidate, step.getEdgePredicates(), params))
                        continue;

                    edge = candidate;
                    next = targetVertex(current, candidate, step.getDirection());
                    if (step.isAnchoredAtPatternTarget()) walk.path.addFirst(candidate);
                    else                                  walk.path.addLast(candidate);
                    walk.visitedEdges.add(candidate);
                    return true;
                }
                return false;
            }

            private void popEdge() {
                if (edge == null) return;
                walk.visitedEdges.remove(edge);
                if (walk.step.isAnchoredAtPatternTarget()) walk.path.removeFirst();
                else                                       walk.path.removeLast();
                edge = null;
                next = null;
            }

            /**
             * Binds the target and group edge list for the walk ending at {@code next}, when the
             * new depth is in range and {@code next} passes the target constraints, including the
             * join against an already-bound target. Returns whether anything was emitted.
             */
            private boolean emit() {
                final ExtensionStep step = walk.step;
                final int targetIdx = walk.targetIdx;
                final int edgeIdx = walk.edgeIdx;
                if (depth + 1 < step.getMinHops() || !matchesTarget(next, step, params))
                    return false;

                if (targetIdx >= 0 && state.getScalar(targetIdx) != null && !state.getScalar(targetIdx).equals(next))
                    return false;

                if (targetIdx >= 0) {
                    savedTarget = state.getScalar(targetIdx);
                    state.setScalar(targetIdx, next);
                }
                // Skip the group entirely for an anonymous quantified edge.
                if (edgeIdx >= 0) {
                    savedGroup = state.getGroup(edgeIdx);
                    state.putGroup(edgeIdx, List.copyOf(walk.path));
                }
                emitted = true;
                return true;
            }

            private void unbindEmit() {
                if (!emitted) return;
                if (walk.edgeIdx >= 0) {
                    if (savedGroup != null) state.putGroup(walk.edgeIdx, savedGroup);
                    else                    state.removeGroup(walk.edgeIdx);
                }
                if (walk.targetIdx >= 0) state.setScalar(walk.targetIdx, savedTarget);
                emitted = false;
                savedTarget = null;
                savedGroup = null;
            }

            @Override
            void close() {
                if (depth == 0) remaining.add(walk.stepIdx, walk.step);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Candidate edges and target checks shared by plain steps and walks
    // -------------------------------------------------------------------------

    private static Iterator<Edge> candidateEdges(final Vertex from, final ExtensionStep step) {
        return step.getEdgeLabel() != null
                ? from.edges(step.getDirection(), step.getEdgeLabel())
                : from.edges(step.getDirection());
    }

    /** Whether {@code target} satisfies the step's target label and target property predicates. */
    private static boolean matchesTarget(final Vertex target, final ExtensionStep step,
                                         final Map<String, Object> params) {
        return (step.getTargetLabel() == null || step.getTargetLabel().equals(target.label()))
                && matchesPredicates(target, step.getTargetPredicates(), params);
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
