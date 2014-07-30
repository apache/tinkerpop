package com.tinkerpop.gremlin.process.graph.step.map.match;

import com.tinkerpop.gremlin.process.SimpleTraverser;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;
import com.tinkerpop.gremlin.process.util.SingleIterator;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class MatchStepNew<S, E> extends AbstractStep<S, Map<String, E>> {

    static final BiConsumer<String, Object> TRIVIAL_CONSUMER = (s, t) -> {
    };

    private static final String ANON_LABEL_PREFIX = "_";

    // optimize before processing each start object, by default
    private static final int DEFAULT_STARTS_PER_OPTIMIZE = 1;

    private final String inAs;
    private final LinkedHashSet<String> asLabels;
    private final Map<String, List<TraversalWrapper<S, S>>> traversalsOut;

    private int startsPerOptimize = DEFAULT_STARTS_PER_OPTIMIZE;
    private int optimizeCounter = -1;

    private int anonLabelCounter = 0;

    public MatchStepNew(final Traversal traversal, final String inAs, final Traversal... traversals) {
        super(traversal);

        this.inAs = inAs;
        asLabels = new LinkedHashSet<>();
        traversalsOut = new HashMap<>();

        for (final Traversal tl : traversals) {
            addTraversal(tl);
        }
    }

    public void setStartsPerOptimize(final int startsPerOptimize) {
        if (startsPerOptimize < 1) {
            throw new IllegalArgumentException();
        }

        this.startsPerOptimize = startsPerOptimize;
    }

    /**
     * @return a description of the current state of this step, including the query plan and gathered statistics
     */
    public String summarize() {
        final StringBuilder sb = new StringBuilder("match \"")
                .append(this.inAs)
                .append("\":\t")
                .append(findCost(this.inAs))
                .append("\n");
        summarize(this.inAs, sb, new HashSet<>(), 1);
        return sb.toString();
    }

    private void summarize(final String outLabel,
                           final StringBuilder sb,
                           final Set<String> visited,
                           final int indent) {
        if (!visited.contains(outLabel)) {
            visited.add(outLabel);
            final List<TraversalWrapper<S, S>> outs = traversalsOut.get(outLabel);
            if (null != outs) {
                for (final TraversalWrapper<S, S> w : outs) {
                    for (int i = 0; i < indent; i++) sb.append("\t");
                    sb.append(outLabel).append("->").append(w.inLabel).append(":\t");
                    sb.append(findCost(w));
                    sb.append("\t").append(w);
                    sb.append("\n");
                    summarize(w.inLabel, sb, visited, indent + 1);
                }
            }
        }
    }

    private Enumerator<S> currentSolution;
    private int currentIndex;
    // initial value allows MatchStep to be used as a non-Step
    private Traverser<S> currentStart = new SimpleTraverser<>(null);

    @Override
    protected Traverser<Map<String, E>> processNextStart() throws NoSuchElementException {
        if (null == this.currentSolution || (this.currentIndex >= this.currentSolution.size() && this.currentSolution.isComplete())) {
            if (this.starts.hasNext()) {
                this.optimizeCounter = (this.optimizeCounter + 1) % this.startsPerOptimize;
                if (0 == this.optimizeCounter) {
                    optimize();
                }

                this.currentStart = this.starts.next();
                this.currentSolution = solveFor(new SingleIterator<>(currentStart.get()));
                this.currentIndex = 0;
            } else {
                throw FastNoSuchElementException.instance();
            }
        }

        final Map<String, E> map = new HashMap<>();
        final Traverser<Map<String, E>> result = this.currentStart.makeChild(this.getAs(), map);
        final BiConsumer<String, S> resultSetter = (name, value) -> {
            //result.set(value);
            //result.getPath().add(name, value);
            map.put(name, (E) value);
        };

        if (this.currentSolution.visitSolution(this.currentIndex++, resultSetter)) {
            return result;
        } else {
            throw FastNoSuchElementException.instance();
        }
    }

    private void addTraversal(final Traversal<S, S> traversal) {
        String outAs, inAs;
        final String startAs = TraversalHelper.getStart(traversal).getAs();
        final String endAs = TraversalHelper.getEnd(traversal).getAs();
        if (!TraversalHelper.isLabeled(startAs)) {
            throw new IllegalArgumentException("All match traversals must have their start step named with as()");
        } else {
            outAs = startAs;
        }
        inAs = TraversalHelper.isLabeled(endAs) ? endAs : null;
        checkAs(outAs);
        if (null == inAs) {
            inAs = createAnonymousAs();
        } else {
            checkAs(inAs);
        }
        this.asLabels.add(outAs);
        this.asLabels.add(inAs);

        final TraversalWrapper<S, S> wrapper = new TraversalWrapper<>(traversal, outAs, inAs);
        List<TraversalWrapper<S, S>> l2 = this.traversalsOut.get(outAs);
        if (null == l2) {
            l2 = new LinkedList<>();
            this.traversalsOut.put(outAs, l2);
        }
        l2.add(wrapper);
    }

    private void checkAs(final String as) {
        // note: this won't happen so long as the anon prefix is the same as Traversal.UNDERSCORE
        if (isAnonymousAs(as)) {
            throw new IllegalArgumentException("The step named '" + as + "' uses reserved prefix '" + ANON_LABEL_PREFIX + "'");
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
     * @return an enumeration of solution bindings
     */
    public Enumerator<S> solveFor(final Iterator<S> inputs) {
        return solveFor(inAs, inputs);
    }

    private Enumerator<S> solveFor(final String outLabel,
                                   final Iterator<S> inputs) {
        List<TraversalWrapper<S, S>> outs = traversalsOut.get(outLabel);
        if (null == outs) {
            // no out-traversals from here; just enumerate the values bound to outLabel
            return new IteratorEnumerator<>(outLabel, inputs);
        } else {
            // for each value bound to outLabel, feed it into all out-traversals in parallel and join the results
            return new SerialEnumerator<>(outLabel, inputs, o -> {
                Enumerator<S> result = null;
                Set<String> leftLabels = new HashSet<>();
                for (TraversalWrapper<S, S> w : outs) {
                    TraversalUpdater<S, S> updater = new TraversalUpdater<>(w, new SingleIterator<S>(o), currentStart, this.getAs());

                    Set<String> rightLabels = new HashSet<>();
                    addVariables(w.inLabel, rightLabels);
                    Enumerator<S> ie = solveFor(w.inLabel, updater);
                    result = null == result ? ie : crossJoin(result, ie, leftLabels, rightLabels);
                    leftLabels.addAll(rightLabels);
                }

                return result;
            });
        }
    }

    private <T> Enumerator<T> crossJoin(final Enumerator<T> left,
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

    private void addVariables(final String outLabel,
                              final Set<String> variables) {
        variables.add(outLabel);

        List<TraversalWrapper<S, S>> outs = traversalsOut.get(outLabel);
        if (null != outs) {
            for (TraversalWrapper<S, S> w : outs) {
                String inLabel = w.inLabel;
                if (!variables.contains(inLabel)) {
                    addVariables(inLabel, variables);
                }
            }
        }
    }

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
        optimizeAt(inAs);
    }

    private void optimizeAt(final String outLabel) {
        List<TraversalWrapper<S, S>> outs = traversalsOut.get(outLabel);
        if (null != outs) {
            for (TraversalWrapper<S, S> t : outs) {
                optimizeAt(t.inLabel);
                updateOrderingFactor(t);
            }
            Collections.sort(outs);
        }
    }

    private double findCost(final TraversalWrapper<S, S> root) {
        double bf = root.findBranchFactor();
        return bf + findCost(root.inLabel, root.findBranchFactor());
    }

    private double findCost(final String outLabel,
                            final double branchFactor) {
        double bf = branchFactor;

        double cost = 0;

        List<TraversalWrapper<S, S>> outs = traversalsOut.get(outLabel);
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
        private final String outLabel, inLabel;
        private int totalInputs = 0;
        private int totalOutputs = 0;
        private double orderingFactor;

        public TraversalWrapper(final Traversal<A, B> traversal,
                                final String outLabel,
                                final String inLabel) {
            this.traversal = traversal;
            this.outLabel = outLabel;
            this.inLabel = inLabel;
        }

        public void incrementInputs() {
            totalInputs++;
        }

        public void incrementOutputs(int outputs) {
            totalOutputs += outputs;
        }

        // TODO: take variance into account, to avoid penalizing traversals for early encounters with super-inputs, or simply for never having been tried
        public double findBranchFactor() {
            return 0 == totalInputs ? 1 : totalOutputs / ((double) totalInputs);
        }

        public int compareTo(final TraversalWrapper<A, B> other) {
            return ((Double) orderingFactor).compareTo(other.orderingFactor);
        }

        public Traversal<A, B> getTraversal() {
            return traversal;
        }

        public void exhaust() {
            // TODO: we need a Traversal.reset() to make exhausting the traversal unnecessary;
            // the latter defeats the purpose of joins which consume only as many iterator elements as necessary
            while (traversal.hasNext()) traversal.next();
        }

        @Override
        public String toString() {
            return "[" + outLabel + "->" + inLabel + "," + findBranchFactor() + "," + totalInputs + "," + totalOutputs + "," + traversal + "]";
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
                // only increment traversal input and output counts once an input has been completely processed by the traversal
                if (-1 != outputs) {
                    w.incrementInputs();
                    w.incrementOutputs(outputs);
                }
                outputs = 0;
            });
            Iterator<Traverser<A>> starts = new MapIterator<>(seIter,
                    o -> {
                        Traverser<A> t = start.makeChild(as, o);
                        return t;
                    });

            w.exhaust();

            // with the traversal "empty" and ready for re-use, add new starts
            w.traversal.addStarts(starts);
        }

        // note: may return true after first returning false (inheriting this behavior from e.g. DefaultTraversal)
        public boolean hasNext() {
            return w.traversal.hasNext();
        }

        public B next() {
            outputs++;
            B b = w.traversal.next();

            // immediately check hasNext(), possibly updating the traverser's statistics even if we otherwise abandon the iterator
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

        public boolean hasNext() {
            if (this.ready) {
                this.onHasNext.accept(null);
                this.ready = false;
            }
            return this.baseIterator.hasNext();
        }

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

        public boolean hasNext() {
            return this.baseIterator.hasNext();
        }

        public B next() {
            return this.map.apply(this.baseIterator.next());
        }
    }
}