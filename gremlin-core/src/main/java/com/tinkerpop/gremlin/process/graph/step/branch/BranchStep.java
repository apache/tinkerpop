package com.tinkerpop.gremlin.process.graph.step.branch;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.EngineDependent;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.EmptyTraverser;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.process.util.TraverserSet;
import com.tinkerpop.gremlin.util.function.CloneableFunction;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BranchStep<S> extends AbstractStep<S, S> implements EngineDependent {

    private static final Set<TraverserRequirement> REQUIREMENTS = new HashSet<>(Arrays.asList(
            TraverserRequirement.BULK,
            TraverserRequirement.OBJECT
    ));

    private Function<Traverser<S>, Collection<String>> branchFunction;
    private boolean onGraphComputer = false;
    private TraverserSet<S> graphComputerQueue;

    public BranchStep(final Traversal traversal) {
        super(traversal);
        this.futureSetByChild = true;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return REQUIREMENTS;
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
                String future;
                if (stepLabel.isEmpty()) {
                    future = this.getNextStep().getId();
                } else {
                    try {
                        future = TraversalHelper.getStep(stepLabel, this.getTraversal()).getNextStep().getId();
                    } catch (IllegalArgumentException e) {
                        future = TraversalHelper.getStepById(stepLabel, this.getTraversal()).getNextStep().getId();
                    }
                }
                sibling.setFutureId(future);
                this.graphComputerQueue.add(sibling);
            }
        }
    }

    private final Traverser<S> standardAlgorithm() {
        final Traverser<S> traverser = this.starts.next();
        for (final String stepLabel : this.branchFunction.apply(traverser)) {
            final Traverser.Admin<S> sibling = traverser.asAdmin().split();
            if (stepLabel.isEmpty()) {
                sibling.setFutureId(this.getNextStep().getId());
                this.getNextStep().addStart(sibling);
            } else {
                sibling.setFutureId(stepLabel);
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
}
