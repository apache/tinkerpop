package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.Reducing;
import com.tinkerpop.gremlin.process.util.step.AbstractStep;
import com.tinkerpop.gremlin.process.util.traversal.TraversalHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ReducingStrategy extends AbstractTraversalStrategy {

    private static final ReducingStrategy INSTANCE = new ReducingStrategy();

    private ReducingStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal, final TraversalEngine engine) {
        if (engine.equals(TraversalEngine.STANDARD))
            return;

        final Step endStep = traversal.getEndStep();
        if (endStep instanceof Reducing)
            TraversalHelper.replaceStep(endStep, new ReducingIdentity(traversal, (Reducing) endStep), traversal);
    }

    public static ReducingStrategy instance() {
        return INSTANCE;
    }

    private static class ReducingIdentity extends AbstractStep implements Reducing {

        private final Reducer reducer;
        private String reducingStepString;

        public ReducingIdentity(final Traversal traversal, final Reducing reducingStep) {
            super(traversal);
            this.reducer = reducingStep.getReducer();
            this.reducingStepString = reducingStep.toString();
        }

        @Override
        public String toString() {
            return TraversalHelper.makeStepString(this, this.reducingStepString);
        }

        public Reducer getReducer() {
            return this.reducer;
        }

        public Traverser processNextStart() {
            return this.starts.next();
        }

    }
}
