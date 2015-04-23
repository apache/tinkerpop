/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.traversal.step.map.match;

import org.apache.tinkerpop.gremlin.process.traversal.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Stack;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public final class MatchStep<S, E> extends AbstractStep<S, Map<String, E>> implements TraversalParent {

    static final BiConsumer<String, Object> TRIVIAL_CONSUMER = (s, t) -> {
    };

    private static final String ANON_LABEL_PREFIX = "_";

    // optimize before processing each start object, by default
    private static final int DEFAULT_STARTS_PER_OPTIMIZE = 1;

    private final String startLabel;
    private final Map<String, List<TraversalWrapper<S, S>>> traversalsByStartAs;
    private final List<Traversal> traversals = new ArrayList<>();

    private int startsPerOptimize = DEFAULT_STARTS_PER_OPTIMIZE;
    private int optimizeCounter = -1;
    private int anonLabelCounter = 0;

    private Enumerator<S> currentSolution;
    private int currentIndex;

    // initial value allows MatchStep to be used as a stand-alone query engine
    private Traverser.Admin<S> currentStart;

    public MatchStep(final Traversal.Admin traversal, final String startLabel, final Traversal... traversals) {
        super(traversal);
        this.startLabel = startLabel;
        this.traversalsByStartAs = new HashMap<>();
        this.currentStart = new B_O_S_SE_SL_Traverser<>(null, this, 1l);    // TODO: bad? P?
        for (final Traversal tl : traversals) {
            addTraversalPrivate(tl);
            this.integrateChild(tl.asAdmin());
            this.traversals.add(tl);
        }
        checkSolvability();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements();
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.traversalsByStartAs);
    }

    /**
     * Adds an individual traversal to an already-constructed MatchStep.
     * The query must be solvable after addition (i.e. should not require the addition of
     * further traversals in order to be solvable)
     * This method should be called before the query is first executed.
     *
     * @param traversal the traversal to add
     */
    public void addTraversal(final Traversal<S, S> traversal) {
        addTraversalPrivate(traversal);
        this.traversals.add(traversal);
        this.integrateChild(traversal.asAdmin());
        checkSolvability();
    }

    public void setStartsPerOptimize(final int startsPerOptimize) {
        if (startsPerOptimize < 1) {
            throw new IllegalArgumentException();
        }
        this.startsPerOptimize = startsPerOptimize;
    }

    @Override
    protected Traverser<Map<String, E>> processNextStart() throws NoSuchElementException {
        final Map<String, E> map = new HashMap<>();
        final Traverser<Map<String, E>> result = this.currentStart.split(map, this);
        final BiConsumer<String, S> resultSetter = (name, value) -> map.put(name, (E) value);

        while (true) { // break out when the current solution is exhausted and there are no more starts
            if (null == this.currentSolution) {
                if (this.starts.hasNext()) {
                    this.optimizeCounter = (this.optimizeCounter + 1) % this.startsPerOptimize;
                    if (0 == this.optimizeCounter) {
                        optimize();
                    }

                    this.currentStart = this.starts.next();
                    this.currentSolution = solveFor(IteratorUtils.of(this.currentStart.get()));
                    this.currentIndex = 0;
                } else {
                    throw FastNoSuchElementException.instance();
                }
            }

            map.clear();
            if (this.currentSolution.visitSolution(this.currentIndex++, resultSetter)) {
                return result;
            } else {
                this.currentSolution = null;
            }
        }
    }

    /**
     * @return a description of the current state of this step, including the query plan and gathered statistics
     */
    public String summarize() {
        final StringBuilder sb = new StringBuilder("match \"")
                .append(this.startLabel)
                .append("\":\t")
                .append(findCost(this.startLabel))
                .append('\n');
        summarize(this.startLabel, sb, new HashSet<>(), 1);
        return sb.toString();
    }

    private void summarize(final String outLabel,
                           final StringBuilder sb,
                           final Set<String> visited,
                           final int indent) {
        if (!visited.contains(outLabel)) {
            visited.add(outLabel);
            final List<TraversalWrapper<S, S>> outs = traversalsByStartAs.get(outLabel);
            if (null != outs) {
                for (final TraversalWrapper<S, S> w : outs) {
                    for (int i = 0; i < indent; i++) sb.append('\t');
                    sb.append(outLabel).append("->").append(w.endLabel).append(":\t");
                    sb.append(findCost(w));
                    sb.append('\t').append(w);
                    sb.append('\n');
                    summarize(w.endLabel, sb, visited, indent + 1);
                }
            }
        }
    }

    private void addTraversalPrivate(final Traversal<S, S> traversal) {

        String startAs = traversal.asAdmin().getStartStep().getLabel().orElseThrow(()
                -> new IllegalArgumentException("All match traversals must have their start step labeled with as()"));
        String endAs = traversal.asAdmin().getEndStep().getLabel().orElse(null);
        checkAs(startAs);
        if (null == endAs) {
            endAs = createAnonymousAs();
        } else {
            checkAs(endAs);
        }

        final TraversalWrapper<S, S> wrapper = new TraversalWrapper<>(traversal, startAs, endAs);
        // index all wrapped traversals by their startLabel
        List<TraversalWrapper<S, S>> l2 = this.traversalsByStartAs.get(startAs);
        if (null == l2) {
            l2 = new LinkedList<>();
            this.traversalsByStartAs.put(startAs, l2);
        }
        l2.add(wrapper);
    }

    // given all the wrapped traversals, determine bad patterns in the set and throw exceptions if not solvable
    private void checkSolvability() {
        final Set<String> pathSet = new HashSet<>();
        final Stack<String> stack = new Stack<>();
        stack.push(this.startLabel);
        int countTraversals = 0;
        while (!stack.isEmpty()) {
            final String outAs = stack.peek();
            if (pathSet.contains(outAs)) {
                stack.pop();
                pathSet.remove(outAs);
            } else {
                pathSet.add(outAs);
                final List<TraversalWrapper<S, S>> l = traversalsByStartAs.get(outAs);
                if (null != l) {
                    for (final TraversalWrapper<S, S> tw : l) {
                        countTraversals++;
                        if (pathSet.contains(tw.endLabel)) {
                            throw new IllegalArgumentException("The provided traversal set contains a cycle due to '"
                                    + tw.endLabel + '\'');
                        }
                        stack.push(tw.endLabel);
                    }
                }
            }
        }

        int totalTraversals = 0;
        for (List<TraversalWrapper<S, S>> l : this.traversalsByStartAs.values()) {
            totalTraversals += l.size();
        }

        if (countTraversals < totalTraversals) {
            throw new IllegalArgumentException("The provided traversal set contains unreachable as-label(s)");
        }
    }

    private static void checkAs(final String as) {
        // note: this won't happen so long as the anon prefix is the same as Traversal.UNDERSCORE
        if (isAnonymousAs(as)) {
            throw new IllegalArgumentException("The step named '" + as + "' uses reserved prefix '"
                    + ANON_LABEL_PREFIX + '\'');
        }
    }

    private static boolean isAnonymousAs(final String as) {
        return as.startsWith(ANON_LABEL_PREFIX);
    }

    private String createAnonymousAs() {
        return ANON_LABEL_PREFIX + ++this.anonLabelCounter;
    }

    /**
     * Directly applies this match query to a sequence of inputs
     *
     * @param inputs a sequence of inputs
     * @return an enumerator over all solutions
     */
    public Enumerator<S> solveFor(final Iterator<S> inputs) {
        return solveFor(startLabel, inputs);
    }

    private Enumerator<S> solveFor(final String localStartAs,
                                   final Iterator<S> inputs) {
        List<TraversalWrapper<S, S>> outs = traversalsByStartAs.get(localStartAs);
        if (null == outs) {
            // no out-traversals from here; just enumerate the values bound to localStartAs
            return isAnonymousAs(localStartAs)
                    ? new SimpleEnumerator<>(localStartAs, inputs)
                    : new IteratorEnumerator<>(localStartAs, inputs);
        } else {
            // for each value bound to localStartAs, feed it into all out-traversals in parallel and join the results
            return new SerialEnumerator<>(localStartAs, inputs, o -> {
                Enumerator<S> result = null;
                Set<String> leftLabels = new HashSet<>();

                for (TraversalWrapper<S, S> w : outs) {
                    TraversalUpdater<S, S> updater
                            = new TraversalUpdater<>(w, IteratorUtils.of(o), currentStart, this.getId());

                    Set<String> rightLabels = new HashSet<>();
                    addVariables(w.endLabel, rightLabels);
                    Enumerator<S> ie = solveFor(w.endLabel, updater);
                    result = null == result ? ie : crossJoin(result, ie, leftLabels, rightLabels);
                    leftLabels.addAll(rightLabels);
                }

                return result;
            });
        }
    }

    private static <T> Enumerator<T> crossJoin(final Enumerator<T> left,
                                               final Enumerator<T> right,
                                               final Set<String> leftLabels,
                                               final Set<String> rightLabels) {
        Set<String> shared = new HashSet<>();
        for (String s : rightLabels) {
            if (leftLabels.contains(s)) {
                shared.add(s);
            }
        }

        Enumerator<T> cj = new CrossJoinEnumerator<>(left, right);
        return shared.size() > 0 ? new InnerJoinEnumerator<>(cj, shared) : cj;
    }

    // recursively add all non-anonymous variables from a starting point in the query
    private void addVariables(final String localStartAs,
                              final Set<String> variables) {
        if (!isAnonymousAs(localStartAs)) {
            variables.add(localStartAs);
        }

        List<TraversalWrapper<S, S>> outs = traversalsByStartAs.get(localStartAs);
        if (null != outs) {
            for (TraversalWrapper<S, S> w : outs) {
                String endAs = w.endLabel;
                if (!variables.contains(endAs)) {
                    addVariables(endAs, variables);
                }
            }
        }
    }

    // applies a visitor, skipping anonymous variables
    static <T> void visit(final String name,
                          final T value,
                          final BiConsumer<String, T> visitor) {
        if (!isAnonymousAs(name)) {
            visitor.accept(name, value);
        }
    }

    /**
     * Computes and applies a new query plan based on gathered statistics about traversal inputs and outputs.
     */
    // note: optimize() is never called from within a solution iterator, as it changes the query plan
    public void optimize() {
        optimizeAt(startLabel);
    }

    private void optimizeAt(final String outAs) {
        List<TraversalWrapper<S, S>> outs = traversalsByStartAs.get(outAs);
        if (null != outs) {
            for (TraversalWrapper<S, S> t : outs) {
                optimizeAt(t.endLabel);
                updateOrderingFactor(t);
            }
            Collections.sort(outs);
        }
    }

    private double findCost(final TraversalWrapper<S, S> root) {
        double bf = root.findBranchFactor();
        return bf + findCost(root.endLabel, root.findBranchFactor());
    }

    private double findCost(final String outAs,
                            final double branchFactor) {
        double bf = branchFactor;

        double cost = 0;

        List<TraversalWrapper<S, S>> outs = traversalsByStartAs.get(outAs);
        if (null != outs) {
            for (TraversalWrapper<S, S> child : outs) {
                cost += bf * findCost(child);
                bf *= child.findBranchFactor();
            }
        }

        return cost;
    }

    /**
     * @param outLabel the out-label of one or more traversals in the query
     * @return the expected cost, in the current query plan, of applying the branch of the query plan at
     * the given out-label to one start value
     */
    public double findCost(final String outLabel) {
        return findCost(outLabel, 1.0);
    }

    private void updateOrderingFactor(final TraversalWrapper<S, S> w) {
        w.orderingFactor = ((w.findBranchFactor() - 1) / findCost(w));
    }

    @Override
    public List<Traversal> getLocalChildren() {
        return this.traversals;
    }

    /**
     * A wrapper for a traversal in a query which maintains statistics about the traversal as
     * it consumes inputs and produces outputs.
     * The "branch factor" of the traversal is an important factor in determining its place in the query plan.
     */
    // note: input and output counts are never "refreshed".
    // The position of a traversal in a query never changes, although its priority / likelihood of being executed does.
    // Priority in turn affects branch factor.
    // However, with sufficient inputs and optimizations,the branch factor is expected to converge on a stable value.
    public static class TraversalWrapper<A, B> implements Comparable<TraversalWrapper<A, B>> {
        private final Traversal<A, B> traversal;
        private final String startLabel, endLabel;
        private int totalInputs = 0;
        private int totalOutputs = 0;
        private double orderingFactor;

        public TraversalWrapper(final Traversal<A, B> traversal,
                                final String startLabel,
                                final String endLabel) {
            this.traversal = traversal;
            this.startLabel = startLabel;
            this.endLabel = endLabel;
        }

        public void incrementInputs() {
            this.totalInputs++;
        }

        public void incrementOutputs(int outputs) {
            this.totalOutputs += outputs;
        }

        // TODO: take variance into account, to avoid penalizing traversals for early encounters with super-inputs,
        // or simply for never having been tried
        public double findBranchFactor() {
            return 0 == this.totalInputs ? 1 : this.totalOutputs / ((double) this.totalInputs);
        }

        @Override
        public int compareTo(final TraversalWrapper<A, B> other) {
            return ((Double) this.orderingFactor).compareTo(other.orderingFactor);
        }

        public Traversal<A, B> getTraversal() {
            return this.traversal;
        }

        public void reset() {
            this.traversal.asAdmin().reset();
        }

        @Override
        public String toString() {
            return this.traversal.toString();
            //return "[" + this.startLabel + "->" + this.endLabel + "," + findBranchFactor() + ","
            // + this.totalInputs + "," + this.totalOutputs + "," + this.traversal + "]";
        }
    }

    /**
     * A helper object which wraps a traversal, submitting starts and counting results per start
     */
    public static class TraversalUpdater<A, B> implements Iterator<B> {
        private final TraversalWrapper<A, B> w;
        private int outputs = -1;

        public TraversalUpdater(final TraversalWrapper<A, B> w,
                                final Iterator<A> inputs,
                                final Traverser<A> start,
                                final String as) {
            this.w = w;

            Iterator<A> seIter = new SideEffectIterator<>(inputs, ignored -> {
                // only increment traversal input and output counts once an input
                // has been completely processed by the traversal
                if (-1 != outputs) {
                    w.incrementInputs();
                    w.incrementOutputs(outputs);
                }
                outputs = 0;
            });
            Iterator<Traverser<A>> starts = new MapIterator<>(seIter,
                    o -> {
                        final Traverser.Admin<A> traverser = ((Traverser.Admin<A>) start).split();
                        traverser.set((A) o);
                        return traverser;
                    });

            w.reset();

            // with the traversal "empty" and ready for re-use, add new starts
            w.traversal.asAdmin().addStarts(starts);
        }

        // note: may return true after first returning false (inheriting this behavior from e.g. DefaultTraversal)
        @Override
        public boolean hasNext() {
            return w.traversal.hasNext();
        }

        @Override
        public B next() {
            outputs++;
            B b = w.traversal.next();

            // immediately check hasNext(), possibly updating the traverser's statistics
            // even if we otherwise abandon the iterator
            w.traversal.hasNext();

            return b;
        }
    }

    // an iterator which executes a side-effect the first time hasNext() is called before a next()
    private static class SideEffectIterator<T> implements Iterator<T> {
        private final Consumer onHasNext;
        private final Iterator<T> baseIterator;
        private boolean ready = true;

        private SideEffectIterator(final Iterator<T> baseIterator,
                                   final Consumer onHasNext) {
            this.onHasNext = onHasNext;
            this.baseIterator = baseIterator;
        }

        @Override
        public boolean hasNext() {
            if (this.ready) {
                this.onHasNext.accept(null);
                this.ready = false;
            }
            return this.baseIterator.hasNext();
        }

        @Override
        public T next() {
            T value = this.baseIterator.next();
            this.ready = true;
            return value;
        }
    }

    private static class MapIterator<A, B> implements Iterator<B> {
        private final Function<A, B> map;
        private final Iterator<A> baseIterator;

        public MapIterator(final Iterator<A> baseIterator, final Function<A, B> map) {
            this.map = map;
            this.baseIterator = baseIterator;
        }

        @Override
        public boolean hasNext() {
            return this.baseIterator.hasNext();
        }

        @Override
        public B next() {
            return this.map.apply(this.baseIterator.next());
        }
    }
}