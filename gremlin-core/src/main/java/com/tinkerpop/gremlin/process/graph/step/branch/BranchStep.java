package com.tinkerpop.gremlin.process.graph.step.branch;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.EngineDependent;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.EmptyTraverser;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.process.util.TraverserSet;
import com.tinkerpop.gremlin.util.function.CloneableFunction;

import java.util.Collection;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BranchStep<S> extends AbstractStep<S, S> implements EngineDependent {

    private Function<Traverser<S>, Collection<String>> branchFunction;
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
            for (final String stepLabel : this.branchFunction.apply(traverser)) {
                final Traverser.Admin<S> sibling = traverser.asAdmin().split();
                sibling.setFuture(stepLabel.isEmpty() ? this.getNextStep().getLabel() : TraversalHelper.getStep(stepLabel, this.getTraversal()).getNextStep().getLabel());
                this.graphComputerQueue.add(sibling);
            }
        }
    }

    private final Traverser<S> standardAlgorithm() {
        final Traverser<S> traverser = this.starts.next();
        for (final String stepLabel : this.branchFunction.apply(traverser)) {
            final Traverser.Admin<S> sibling = traverser.asAdmin().split();
            if (stepLabel.isEmpty()) {
                sibling.setFuture(this.getNextStep().getLabel());
                this.getNextStep().addStart(sibling);
            } else {
                sibling.setFuture(stepLabel);
                TraversalHelper.<S, Object>getStep(stepLabel, this.getTraversal()).getNextStep().addStart((Traverser) sibling);
            }
        }
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
        if (this.branchFunction instanceof CloneableFunction)
            clone.branchFunction = CloneableFunction.clone((CloneableFunction<Traverser<S>, Collection<String>>) this.branchFunction);
        return clone;
    }

    public void setFunction(final Function<Traverser<S>, Collection<String>> function) {
        this.branchFunction = function;
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.branchFunction);
    }

    public static class GoToLabels<S> implements Function<Traverser<S>, Collection<String>> {

        private final Collection<String> stepLabels;

        public GoToLabels(final Collection<String> stepLabels) {
            this.stepLabels = stepLabels;
        }

        public Collection<String> apply(final Traverser<S> traverser) {
            return this.stepLabels;
        }

        public String toString() {
            return "goTo(" + this.stepLabels.toString().replace("[", "").replace("]", "") + ")";
        }
    }

    /*public static class GoToLabelsWithPredicate<S> implements Function<Traverser<S>, Collection<String>> {

        private final Collection<String> stepLabels;
        private final Predicate<Traverser<S>> goToPredicate;

        public GoToLabelsWithPredicate(final Collection<String> stepLabels, final Predicate<Traverser<S>> goToPredicate) {
            this.stepLabels = stepLabels;
            this.goToPredicate = goToPredicate;
        }

        public Collection<String> apply(final Traverser<S> traverser) {
            return this.goToPredicate.test(traverser) ? this.stepLabels : Collections.emptyList();
        }
    }*/
}
