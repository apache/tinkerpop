package com.tinkerpop.gremlin.process.graph.step.branch;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.EngineDependent;
import com.tinkerpop.gremlin.process.graph.marker.FunctionHolder;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.EmptyTraverser;
import com.tinkerpop.gremlin.process.util.FunctionRing;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.process.util.TraverserSet;
import com.tinkerpop.gremlin.structure.Compare;
import com.tinkerpop.gremlin.structure.Graph;

import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BranchStep<S> extends AbstractStep<S, S> implements EngineDependent, FunctionHolder<Traverser<S>, String> {

    public static final String EMPTY_LABEL = Graph.Hidden.hide("emptyLabel");
    public static final String THIS_LABEL = Graph.Hidden.hide("thisLabel");
    public static final String THIS_BREAK_LABEL = Graph.Hidden.hide("thisBreakLabel");

    private FunctionRing<Traverser<S>, String> functionRing = new FunctionRing<>();
    private boolean onGraphComputer = false;
    private TraverserSet<S> graphComputerQueue;

    public BranchStep(final Traversal traversal) {
        super(traversal);
        this.futureSetByChild = true;
    }

    @Override
    public Traverser<S> processNextStart() {
        return this.onGraphComputer ? this.computerAlgorithm() : this.standardAlgorithm();
    }

    private final Traverser<S> computerAlgorithm() {
        while (true) {
            if (!this.graphComputerQueue.isEmpty())
                return this.graphComputerQueue.remove();
            final Traverser<S> traverser = this.starts.next();
            while (!this.functionRing.roundComplete()) {
                final String goTo = this.functionRing.next().apply(traverser);
                if (THIS_BREAK_LABEL.equals(goTo)) {
                    final Traverser.Admin<S> sibling = traverser.asAdmin().split();
                    sibling.setFuture(this.getNextStep().getLabel());
                    this.graphComputerQueue.add(sibling);
                    break;
                } else if (THIS_LABEL.equals(goTo)) {
                    final Traverser.Admin<S> sibling = traverser.asAdmin().split();
                    sibling.setFuture(this.getNextStep().getLabel());
                    this.graphComputerQueue.add(sibling);
                } else if (!EMPTY_LABEL.equals(goTo)) {
                    final Traverser.Admin<S> sibling = traverser.asAdmin().split();
                    sibling.setFuture(goTo);
                    this.graphComputerQueue.add(sibling);
                }
            }
            this.functionRing.reset();
        }
    }

    private final Traverser<S> standardAlgorithm() {
        final Traverser<S> traverser = this.starts.next();
        while (!this.functionRing.roundComplete()) {
            final String goTo = this.functionRing.next().apply(traverser);
            if (THIS_BREAK_LABEL.equals(goTo)) {
                final Traverser.Admin<S> sibling = traverser.asAdmin().split();
                sibling.setFuture(this.getNextStep().getLabel());
                this.getNextStep().addStart(sibling);
                break;
            } else if (THIS_LABEL.equals(goTo)) {
                final Traverser.Admin<S> sibling = traverser.asAdmin().split();
                sibling.setFuture(this.getNextStep().getLabel());
                this.getNextStep().addStart(sibling);
            } else if (!EMPTY_LABEL.equals(goTo)) {
                final Traverser.Admin<S> sibling = traverser.asAdmin().split();
                sibling.setFuture(goTo);
                TraversalHelper.<S, Object>getStep(goTo, this.getTraversal())
                        .getNextStep()
                        .addStart((Traverser) sibling);
            }
        }
        this.functionRing.reset();
        return EmptyTraverser.instance();
    }

    @Override
    public void onEngine(final TraversalEngine traversalEngine) {
        if (traversalEngine.equals(TraversalEngine.COMPUTER)) {
            this.onGraphComputer = true;
            this.graphComputerQueue = new TraverserSet<>();
        } else {
            this.onGraphComputer = false;
            this.graphComputerQueue = null;
        }
    }

    @Override
    public BranchStep<S> clone() throws CloneNotSupportedException {
        final BranchStep<S> clone = (BranchStep<S>) super.clone();
        if (this.onGraphComputer) clone.graphComputerQueue = new TraverserSet<S>();
        clone.functionRing = this.functionRing.clone();
        return clone;
    }

    @Override
    public void addFunction(final Function<Traverser<S>, String> function) {
        this.functionRing.addFunction(function);
    }

    @Override
    public List<Function<Traverser<S>, String>> getFunctions() {
        return this.functionRing.getFunctions();
    }

    /*@Override
    public String toString() {
      // TODO
    }*/

    public static class GoToLabel<S> implements Function<Traverser<S>, String> {

        private final String goToLabel;

        public GoToLabel(final String goToLabel) {
            this.goToLabel = goToLabel;
        }

        public String apply(final Traverser<S> traverser) {
            return this.goToLabel;
        }
    }

    public static class GoToLabelWithLoops<S> implements Function<Traverser<S>, String> {

        private final String goToLabel;
        private final String breakLabel;
        private final Compare loopCompare;
        private final short loops;

        public GoToLabelWithLoops(final String goToLabel, final Compare loopCompare, final short loops, final String breakLabel) {
            this.goToLabel = goToLabel;
            this.breakLabel = breakLabel;
            this.loops = loops;
            this.loopCompare = loopCompare;
        }

        public String apply(final Traverser<S> traverser) {
            return this.loopCompare.test(traverser.loops(), this.loops) ? this.goToLabel : this.breakLabel;
        }
    }

    public static class GoToLabelWithPredicate<S> implements Function<Traverser<S>, String> {

        private final String goToLabel;
        private final String breakLabel;
        private final Predicate<Traverser<S>> goToPredicate;

        public GoToLabelWithPredicate(final String goToLabel, final Predicate<Traverser<S>> goToPredicate, final String breakLabel) {
            this.goToLabel = goToLabel;
            this.breakLabel = breakLabel;
            this.goToPredicate = goToPredicate;
        }

        public String apply(final Traverser<S> traverser) {
            return this.goToPredicate.test(traverser) ? this.goToLabel : this.breakLabel;

        }
    }
}
