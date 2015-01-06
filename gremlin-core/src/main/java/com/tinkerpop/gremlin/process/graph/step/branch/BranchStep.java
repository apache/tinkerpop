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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BranchStep<S> extends AbstractStep<S, S> implements EngineDependent, FunctionHolder<Traverser<S>, Collection<String>> {

    private FunctionRing<Traverser<S>, Collection<String>> functionRing = new FunctionRing<>();
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
                final Collection<String> stepLabels = this.functionRing.next().apply(traverser);
                for (final String stepLabel : stepLabels) {
                    final Traverser.Admin<S> sibling = traverser.asAdmin().split();
                    sibling.setFuture(stepLabel.isEmpty() ? this.getNextStep().getLabel() : TraversalHelper.getStep(stepLabel, this.getTraversal()).getNextStep().getLabel());
                    this.graphComputerQueue.add(sibling);
                }
            }
            this.functionRing.reset();
        }
    }

    private final Traverser<S> standardAlgorithm() {
        final Traverser<S> traverser = this.starts.next();
        while (!this.functionRing.roundComplete()) {
            final Collection<String> stepLabels = this.functionRing.next().apply(traverser);
            for (final String stepLabel : stepLabels) {
                final Traverser.Admin<S> sibling = traverser.asAdmin().split();
                if (stepLabel.isEmpty()) {
                    sibling.setFuture(this.getNextStep().getLabel());
                    this.getNextStep().addStart(sibling);
                } else {
                    sibling.setFuture(stepLabel);
                    TraversalHelper.<S, Object>getStep(stepLabel, this.getTraversal()).getNextStep().addStart((Traverser) sibling);
                }
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
    public void addFunction(final Function<Traverser<S>, Collection<String>> function) {
        this.functionRing.addFunction(function);
    }

    @Override
    public List<Function<Traverser<S>, Collection<String>>> getFunctions() {
        return this.functionRing.getFunctions();
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.functionRing);
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
            return "goTo(" + this.stepLabels.toString().replace("[","").replace("]","") + ")";
        }
    }

    public static class GoToLabelsWithPredicate<S> implements Function<Traverser<S>, Collection<String>> {

        private final Collection<String> stepLabels;
        private final Predicate<Traverser<S>> goToPredicate;

        public GoToLabelsWithPredicate(final Collection<String> stepLabels, final Predicate<Traverser<S>> goToPredicate) {
            this.stepLabels = stepLabels;
            this.goToPredicate = goToPredicate;
        }

        public Collection<String> apply(final Traverser<S> traverser) {
            return this.goToPredicate.test(traverser) ? this.stepLabels : Collections.emptyList();
        }
    }
}
