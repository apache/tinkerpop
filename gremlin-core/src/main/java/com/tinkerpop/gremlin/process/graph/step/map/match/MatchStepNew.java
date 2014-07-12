package com.tinkerpop.gremlin.process.graph.step.map.match;

import com.tinkerpop.gremlin.process.SimpleTraverser;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.SingleIterator;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.ArrayList;
import java.util.Collection;
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
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class MatchStepNew<S, E> extends AbstractStep<S, E> {
    public static final BiPredicate<String, Object> TRIVIAL_VISITOR = (s, t) -> true;

    private static final String ANON_LABEL_PREFIX = "_";

    // optimize before processing each start object, by default
    private static final int DEFAULT_STARTS_PER_OPTIMIZE = 1;

    private final String startLabel;
    private final LinkedHashSet<String> labels;
    private final Map<String, List<TraversalWrapper<S, S>>> traversalsOut;

    private int startsPerOptimize = DEFAULT_STARTS_PER_OPTIMIZE;
    private int optimizeCounter = -1;

    private int anonLabelCounter = 0;

    public MatchStepNew(final Traversal traversal,
                        final String startLabel,
                        final Traversal... traversals) {
        super(traversal);

        this.startLabel = startLabel;
        labels = new LinkedHashSet<>();
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

    public String summarize() {
        StringBuilder sb = new StringBuilder("match \"")
                .append(startLabel).append("\":\t").append(findCost(startLabel)).append("\n");
        summarize(startLabel, sb, new HashSet<>(), 1);

        return sb.toString();
    }

    private void summarize(final String outLabel,
                           final StringBuilder sb,
                           final Set<String> visited,
                           final int indent) {
        if (!visited.contains(outLabel)) {
            visited.add(outLabel);
            List<TraversalWrapper<S, S>> outs = traversalsOut.get(outLabel);
            if (null != outs) {
                for (TraversalWrapper<S, S> w : outs) {
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

    private Enumerator<S> curSolution;
    private int curIndex;

    @Override
    protected Traverser<E> processNextStart() throws NoSuchElementException {
        if (null == curSolution || (curIndex >= curSolution.size() && curSolution.isComplete())) {
            if (this.starts.hasNext()) {
                    /*
        optimizeCounter = (optimizeCounter + 1) % startsPerOptimize;
        if (0 == optimizeCounter) {
            optimize();
        }
        */

                curSolution = solve(new TraverserAsIterator<>(this.starts.next()));
                curIndex = 0;
            } else {
                throw new NoSuchElementException();
            }
        }

        //final Traversal t = GraphTraversal.of().id();
        final SimpleTraverser<S> result = new SimpleTraverser<>(null);
        BiPredicate<String, S> resultSetter = (name, value) -> {
            //result.set(value);
            //t.addStarts(new SingleIterator<>(new SimpleTraverser<>(value)));
            return true;
        };
        curSolution.visitSolution(curIndex++, resultSetter);
        return (Traverser<E>) result;
        //return (Traverser<E>) TraversalHelper.getEnd(t).next();
    }

    private void addTraversal(final Traversal<S, S> traversal) {
        String outLabel, inLabel;
        String start = TraversalHelper.getStart(traversal).getAs();
        String end = TraversalHelper.getEnd(traversal).getAs();
        if (!TraversalHelper.isLabeled(start)) {
            throw new IllegalArgumentException("All match traversals must have their start pipe labeled");
        } else {
            outLabel = start;
        }
        inLabel = TraversalHelper.isLabeled(end) ? end : null;
        checkLabel(outLabel);
        if (null == inLabel) {
            inLabel = createAnonymousLabel();
        } else {
            checkLabel(inLabel);
        }
        labels.add(outLabel);
        labels.add(inLabel);

        TraversalWrapper<S, S> wrapper = new TraversalWrapper<>(traversal, outLabel, inLabel);
        List<TraversalWrapper<S, S>> l2 = traversalsOut.get(outLabel);
        if (null == l2) {
            l2 = new LinkedList<>();
            traversalsOut.put(outLabel, l2);
        }
        l2.add(wrapper);
    }

    public Collection<TraversalWrapper<S, S>> getTraversals() {
        Collection<TraversalWrapper<S, S>> traversals = new LinkedList<>();
        for (List<TraversalWrapper<S, S>> l : traversalsOut.values()) {
            traversals.addAll(l);
        }
        return traversals;
    }

    public Enumerator<S> solve(final Iterator<S> inputs) {
        return solveFor(startLabel, inputs, true);
    }

    private void checkLabel(final String label) {
        // note: this won't happen so long as the anon prefix is the same as Traversal.UNDERSCORE
        if (isAnonymousLabel(label)) {
            throw new IllegalArgumentException("label '" + label + "' uses reserved prefix '" + ANON_LABEL_PREFIX + "'");
        }
    }

    public static boolean isAnonymousLabel(final String label) {
        return label.startsWith(ANON_LABEL_PREFIX);
    }

    private String createAnonymousLabel() {
        return ANON_LABEL_PREFIX + ++anonLabelCounter;
    }

    private Enumerator<S> solveFor(final String outLabel,
                                   final Iterator<S> inputs,
                                   final boolean optimizeFirst) {
        List<TraversalWrapper<S, S>> outs = traversalsOut.get(outLabel);
        if (null == outs) {
            return new IteratorEnumerator<>(outLabel, inputs);
        } else {
            Iterator<S> inputIterator = optimizeFirst ? new MapIterator<>(inputs, o -> {
                // optimize if and only if a new start object is about to be handled
                optimize();
                return o;
            }) : inputs;

            return new SerialEnumerator<>(outLabel, inputIterator, o -> {
                Enumerator<S> result = null;
                for (TraversalWrapper<S, S> w : outs) {
                    TraversalUpdater<S, S> updater = new TraversalUpdater<>(w, new SingleIterator<S>(o));

                    Enumerator<S> ie = solveFor(
                            w.inLabel,
                            updater,
                            false);  // do not optimize recursively; this happens once for each top-level start object
                    if (null == result) {
                        result = ie;
                    } else {
                        result = new CartesianEnumerator<>(result, ie);
                    }
                }

                int i = 0;
                while (result.visitSolution(i++, (BiPredicate<String, S>) TRIVIAL_VISITOR)) ;

                return result;
            });
        }
    }

    public static <T> void visit(final String name,
                                 final T value,
                                 final BiPredicate<String, T> visitor) {
        if (!isAnonymousLabel(name)) {
            visitor.test(name, value);
        }
    }

    public void visitSolutions(final S input,
                               final Predicate<Map<String, S>> visitor) {
        optimize();

        visitSolutions(input, startLabel, visitor, new HashMap<>());
    }

    private boolean visitSolutions(final S o,
                                   final String outLabel,
                                   final Predicate<Map<String, S>> visitor,
                                   final Map<String, S> bindings) {
        bindings.put(outLabel, o);

        try {
            List<TraversalWrapper<S, S>> outs = traversalsOut.get(outLabel);
            if (null == outs) {
                visitor.test(bindings);
            } else {
                for (TraversalWrapper<S, S> w : outs) {
                    Traversal<S, S> t = w.getTraversal();
                    String inLabel = w.inLabel;
                    t.addStarts(new SingleIterator<>(new SimpleTraverser<>(o)));

                    int outputs = 0;

                    // we always exhaust the traversal
                    // ideally, this puts it into a fresh state w.r.t. the next set of starts
                    while (t.hasNext()) {
                        outputs++;
                        S result = t.next();

                        visitSolutions(result, inLabel, visitor, bindings);
                    }

                    w.incrementInputs();
                    w.incrementOutputs(outputs);

                    if (0 == outputs) {
                        return false;
                    }
                }
            }
        } finally {
            bindings.remove(outLabel);
        }

        return true;
    }

    public void optimize() {
        optimizeAt(startLabel);
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

    public List<String> getLabels() {
        List<String> labelList = new ArrayList<>();
        labelList.addAll(labels);
        return labelList;
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

    public double findCost(final String outLabel) {
        return findCost(outLabel, 1.0);
    }

    private void updateOrderingFactor(final TraversalWrapper<S, S> w) {
        w.orderingFactor = ((w.findBranchFactor() - 1) / findCost(w));
    }

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

        @Override
        public String toString() {
            return "[" + outLabel + "->" + inLabel + "," + findBranchFactor() + "," + totalInputs + "," + totalOutputs + "," + traversal + "]";
        }
    }

    // wraps a traversal, submitting starts and counting results per start
    public static class TraversalUpdater<A, B> implements Iterator<B> {
        private final TraversalWrapper<A, B> w;
        private int outputs = -1;

        public TraversalUpdater(final TraversalWrapper<A, B> w,
                                final Iterator<A> inputs) {
            this.w = w;

            Iterator<A> triggerIter = new TriggerIterator<>(inputs, ignored -> {
                // only increment traversal input and output counts once an input has been completely processed by the traversal
                if (-1 != outputs) {
                    w.incrementInputs();
                    w.incrementOutputs(outputs);
                }
                outputs = 0;

                return true;
            });
            Iterator<Traverser<A>> starts = new MapIterator<>(triggerIter, SimpleTraverser::new);

            w.traversal.addStarts(starts);
        }

        public boolean hasNext() {
            return w.traversal.hasNext();
        }

        public B next() {
            B value = w.traversal.next();
            outputs++;
            return value;
        }
    }

    private static class TriggerIterator<T> implements Iterator<T> {
        private final Predicate trigger;
        private final Iterator<T> baseIterator;
        private boolean ready = true;

        private TriggerIterator(final Iterator<T> baseIterator,
                                final Predicate trigger) {
            this.trigger = trigger;
            this.baseIterator = baseIterator;
        }

        public boolean hasNext() {
            if (ready) {
                trigger.test(null);
                ready = false;
            }
            return baseIterator.hasNext();
        }

        public T next() {
            T value = baseIterator.next();
            ready = true;
            return value;
        }
    }

    private static class MapIterator<A, B> implements Iterator<B> {
        private final Function<A, B> map;
        private final Iterator<A> baseIterator;

        public MapIterator(Iterator<A> baseIterator, Function<A, B> map) {
            this.map = map;
            this.baseIterator = baseIterator;
        }

        public boolean hasNext() {
            return baseIterator.hasNext();
        }

        public B next() {
            return map.apply(baseIterator.next());
        }
    }

    private static class NullEnumerator<T> implements Enumerator<T> {
        public int size() {
            return 0;
        }

        public boolean isComplete() {
            return true;
        }

        public boolean visitSolution(int i, BiPredicate<String, T> visitor) {
            return false;
        }
    }

    private class SingleEnumerator<T> implements Enumerator<T> {
        private final String name;
        private final T value;

        private SingleEnumerator(String name, T value) {
            this.name = name;
            this.value = value;
        }

        public int size() {
            return 1;
        }

        public boolean isComplete() {
            return true;
        }

        public boolean visitSolution(int i, BiPredicate<String, T> visitor) {
            if (i > 0) {
                return false;
            } else {
                visit(name, value, visitor);
                return true;
            }
        }
    }

    private class Memoizer<T> {
        private final Iterator<T> baseIterator;
        private final List<T> memory = new ArrayList<>();

        private Memoizer(Iterator<T> baseIterator) {
            this.baseIterator = baseIterator;
        }

        public Iterator<T> iterator() {
            return new Iterator<T>() {
                private int nextIndex = 0;

                public boolean hasNext() {
                    return nextIndex < memory.size() || baseIterator.hasNext();
                }

                public T next() {
                    T value;
                    if (nextIndex < memory.size()) {
                        value = memory.get(nextIndex);
                    } else if (baseIterator.hasNext()) {
                        value = baseIterator.next();
                        memory.add(value);
                    } else {
                        throw new NoSuchElementException();
                    }
                    nextIndex++;

                    return value;
                }
            };
        }
    }

    private class TraverserAsIterator<T> implements Iterator<T> {
        private final Traverser<T> traverser;

        private TraverserAsIterator(Traverser<T> traverser) {
            this.traverser = traverser;
        }

        public boolean hasNext() {
            return !traverser.isDone();
        }

        public T next() {
            return traverser.get();
        }
    }
}