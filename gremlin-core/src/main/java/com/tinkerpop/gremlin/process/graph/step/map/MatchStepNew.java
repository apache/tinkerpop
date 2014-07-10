package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.SimpleTraverser;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.SingleIterator;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class MatchStepNew<S, E> extends AbstractStep<S, E> {
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

    private Enumerator<S> curSolution;
    private int curIndex;

    @Override
    protected Traverser<E> processNextStart() throws NoSuchElementException {
        if (null == curSolution || (curIndex >= curSolution.size() && curSolution.isComplete())) {
            if (this.starts.hasNext()) {
                System.out.println("@@@@@@@@ processing next start");
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
        final String start = TraversalHelper.getStart(traversal).getAs();
        final String end = TraversalHelper.getEnd(traversal).getAs();

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

        TraversalWrapper<S, S> wrapper = new TraversalWrapper<>(traversal, outLabel, inLabel);

        List<TraversalWrapper<S, S>> l2 = traversalsOut.get(outLabel);
        if (null == l2) {
            l2 = new LinkedList<>();
            traversalsOut.put(outLabel, l2);
        }
        l2.add(wrapper);

        labels.add(outLabel);
        labels.add(inLabel);
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

    private boolean isAnonymousLabel(final String label) {
        return label.startsWith(ANON_LABEL_PREFIX);
    }

    private String createAnonymousLabel() {
        return ANON_LABEL_PREFIX + ++anonLabelCounter;
    }

    private Enumerator<S> solveFor(final String outLabel,
                                   final Iterator<S> inputs,
                                   boolean optimizeFirst) {
        System.out.println("solveFor(" + outLabel + ")");
        List<TraversalWrapper<S, S>> outs = traversalsOut.get(outLabel);
        if (null == outs) {
            return new IteratorEnumerator<>(outLabel, inputs);
        } else {
            Iterator<S> inputIterator = optimizeFirst ? new MapIterator<>(inputs, o -> {
                // optimize if and only if a new start object is about to be handled
                optimize();
                return o;
            }) : inputs;
            System.out.println("\tinputs = " + inputs);
            System.out.println("\tinputIterator = " + inputIterator);

            return new SerialEnumerator<>(outLabel, inputIterator, o -> {
                Enumerator<S> result = null;
                for (TraversalWrapper<S, S> w : outs) {
                    System.out.println("\t" + outLabel + ": " + w.outLabel + "->" + w.inLabel);
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
                while (result.visitSolution(i++, (BiPredicate<String, S>) trivialVisitor)) ;
                System.out.println("\tsolutions at " + outLabel + "=" + o + ": " + (i - 1));

                return result;
            });
        }
    }

    private <T> void visit(final String name,
                           final T value,
                           final BiPredicate<String, T> visitor) {
        if (!isAnonymousLabel(name)) {
            visitor.test(name, value);
        }
    }

    public void visitSolutions(final S input,
                               final Predicate<Map<String, S>> visitor) {
        System.out.println("### trying: " + input);
        System.out.flush();
        optimize();

        visitSolutions(input, startLabel, visitor, new HashMap<>());
        System.out.println("\t### done");
        System.out.println();
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
                    System.out.println("applying to " + o + ": " + t);
                    System.out.flush();
                    t.addStarts(new SingleIterator<>(new SimpleTraverser<>(o)));

                    int outputs = 0;

                    // we always exhaust the traversal
                    // ideally, this puts it into a fresh state w.r.t. the next set of starts
                    while (t.hasNext()) {
                        outputs++;
                        S result = t.next();
                        System.out.println("\t" + result);
                        System.out.flush();

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
        System.out.println("optimizeAt(" + outLabel + ")");
        System.out.flush();
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

        double cost = bf;

        List<TraversalWrapper<S, S>> outs = traversalsOut.get(root.inLabel);
        if (null != outs) {
            for (TraversalWrapper<S, S> child : outs) {
                cost += bf * findCost(child);
                bf *= child.findBranchFactor();
            }
        }

        return cost;
    }

    public void updateOrderingFactor(final TraversalWrapper<S, S> w) {
        w.orderingFactor = ((w.findBranchFactor() - 1) / findCost(w));
    }

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

    public static interface Enumerator<T> {
        int size();

        boolean isComplete();

        boolean visitSolution(int i, BiPredicate<String, T> visitor);
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

    private class IteratorEnumerator<T> implements Enumerator<T> {
        private final String name;
        private final Iterator<T> iterator;
        private final List<T> memory = new ArrayList<>();

        public IteratorEnumerator(final String name,
                                  final Iterator<T> iterator) {
            this.name = name;
            this.iterator = iterator;
        }

        public int size() {
            return memory.size();
        }

        public boolean isComplete() {
            return !iterator.hasNext();
        }

        public boolean visitSolution(int i, BiPredicate<String, T> visitor) {
            T value;

            if (i < size()) {
                value = memory.get(i);
            } else do {
                if (isComplete()) {
                    return false;
                }

                value = iterator.next();
                memory.add(value);
            } while (i >= size());

            System.out.println(name + " visiting " + i + ": " + value);
            visit(name, value, visitor);

            return true;
        }
    }

    private static final BiPredicate<String, Object> trivialVisitor = (s, t) -> true;

    /**
     * An Enumerator which finds the Cartesian product of two other Enumerators, expanding at the same rate in either dimension.
     * This maximizes the size of the product with respect to the number of expansions of the base Enumerators.
     */
    private class CartesianEnumerator<T> implements Enumerator<T> {
        private final Enumerator<T> xEnum, yEnum;

        public CartesianEnumerator(final Enumerator<T> xEnum,
                                   final Enumerator<T> yEnum) {
            this.xEnum = xEnum;
            this.yEnum = yEnum;
        }

        public int size() {
            return xEnum.size() * yEnum.size();
        }

        public boolean isComplete() {
            boolean xc = xEnum.isComplete(), yc = yEnum.isComplete();
            return xc && 0 == xEnum.size()
                    || yc && 0 == yEnum.size()
                    || xc && yc;
        }

        public boolean visitSolution(final int i,
                                     final BiPredicate<String, T> visitor) {
            int sq = (int) Math.sqrt(i);

            // choose x and y such that the solution represented by i
            // remains constant as this Enumerator expands
            int x, y;

            if (0 == i) {
                x = y = 0;
            } else {
                int r = i - sq * sq;
                if (r < sq) {
                    x = sq;
                    y = r;
                } else {
                    x = r - sq;
                    y = sq;
                }
            }

            // expand x
            while (sq >= xEnum.size()) {
                // ran into x limit
                if (xEnum.isComplete()) {
                    if (0 == xEnum.size()) {
                        return false;
                    }
                    x = i % xEnum.size();
                    y = i / xEnum.size();
                    break;
                }
                if (!xEnum.visitSolution(xEnum.size(), (BiPredicate<String, T>) trivialVisitor)) return false;
            }

            int height = i / Math.min(1 + sq, xEnum.size());

            // expand y
            while (height >= yEnum.size()) {
                // ran into y limit; expand x again
                if (yEnum.isComplete()) {
                    if (0 == yEnum.size()) {
                        return false;
                    }
                    height = yEnum.size();
                    int width = i / height;
                    while (width >= xEnum.size()) {
                        if (!xEnum.visitSolution(xEnum.size(), (BiPredicate<String, T>) trivialVisitor)) return false;
                    }
                    x = i / yEnum.size();
                    y = i % yEnum.size();
                    break;
                }

                if (!yEnum.visitSolution(yEnum.size(), (BiPredicate<String, T>) trivialVisitor)) return false;
            }

            // solutions are visited completely (if we have reached this point), else not at all
            return xEnum.visitSolution(x, visitor) && yEnum.visitSolution(y, visitor);
        }
    }

    private class SerialEnumerator<T> implements Enumerator<T> {
        private final String name;
        private final Iterator<T> iterator;
        private final Function<T, Enumerator<T>> constructor;
        private final List<Enumerator<T>> memory = new ArrayList<>();
        private final List<T> values = new ArrayList<>();

        private SerialEnumerator(final String name,
                                 final Iterator<T> iterator,
                                 final Function<T, Enumerator<T>> constructor) {
            this.name = name;
            this.iterator = iterator;
            this.constructor = constructor;
        }

        public int size() {
            // TODO: replace with an incremental size when done debugging (i.e. when size is under the control of this enumerator)
            int size = 0;
            for (Enumerator<T> e : memory) size += e.size();
            return size;
        }

        public boolean isComplete() {
            System.out.println(name + " is complete: " + (!iterator.hasNext() && (memory.isEmpty() || memory.get(memory.size() - 1).isComplete())));
            return !iterator.hasNext() && (memory.isEmpty() || memory.get(memory.size() - 1).isComplete());
        }

        public boolean visitSolution(final int i,
                                     final BiPredicate<String, T> visitor) {
            System.out.println(name + " visitSolution(" + i + ")");

            // TODO: temporary; replace with binary search for efficient random access
            int totalSize = 0;
            int index = 0;
            while (true) {
                if (index < memory.size()) {
                    Enumerator<T> e = memory.get(index);

                    if ((!e.isComplete() || e.isComplete() && i < totalSize + e.size()) && e.visitSolution(i - totalSize, visitor)) {
                        System.out.println("\tputting as " + name + ": " + values.get(index));
                        System.out.println("\t\tvisitor = " + visitor);
                        System.out.println("\t\tindex = " + index);
                        visit(name, values.get(index), visitor);

                        //if (values.get(index).toString().equals("v[2]")) {
                        System.out.println("\t\t### b memory.size() = " + memory.size());
                        for (int k = 0; k < memory.size(); k++) {
                            System.out.println("\t\t### \t" + k + ":\t" + values.get(k) + "\t" + memory.get(k).size() + "\t" + memory.get(k).isComplete());
                        }
                        //}

                        return true;
                    }

                    totalSize += e.size();
                    index++;
                } else {
                    if (!iterator.hasNext()) {
                        System.out.println("\titerator exhausted!");
                        return false;
                    }

                    // first remove the head enumeration if it exists and is empty
                    // only the head will ever be empty, avoiding wasted space
                    if (!memory.isEmpty() && 0 == memory.get(index - 1).size()) {
                        index--;
                        memory.remove(index);
                        values.remove(index);
                    }

                    T value = iterator.next();
                    System.out.println("\t" + name + " = " + value);
                    values.add(value);
                    Enumerator<T> e = constructor.apply(value);
                    System.out.println("\te = " + e);
                    System.out.println("\te.isComplete() = " + e.isComplete());
                    memory.add(memory.size(), e);
                }
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

    private void doTest() {
        String[] a1 = new String[]{"a", "b", "c"};
        String[] a2 = new String[]{"1", "2", "3", "4"};
        String[] a3 = new String[]{"@", "#"};

        Enumerator<String> e1 = new IteratorEnumerator<>("letter", Arrays.asList(a1).iterator());
        Enumerator<String> e2 = new IteratorEnumerator<>("number", Arrays.asList(a2).iterator());
        Enumerator<String> e3 = new IteratorEnumerator<>("punc", Arrays.asList(a3).iterator());

        Enumerator<String> e1e2 = new CartesianEnumerator<>(e1, e2);
        BiPredicate<String, String> visitor = (name, value) -> {
            System.out.println("\t" + name + ":\t" + value);
            return true;
        };
        Enumerator<String> e1e2e3 = new CartesianEnumerator<>(e1e2, e3);

        int i = 0;
        Enumerator<String> e
                = e1e2e3; //e1e2;
        while (e.visitSolution(i, visitor)) {
            System.out.println("solution #" + (i + 1) + "^^");
            i++;
        }
    }

    public static void main(final String[] args) {
        new MatchStepNew<>(GraphTraversal.of(), "a").doTest();
    }
}