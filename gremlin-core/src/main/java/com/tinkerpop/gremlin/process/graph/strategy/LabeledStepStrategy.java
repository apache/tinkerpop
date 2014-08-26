package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.step.util.LabelIdentityStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LabeledStepStrategy implements TraversalStrategy {

    private static final LabeledStepStrategy INSTANCE = new LabeledStepStrategy();

    private LabeledStepStrategy() {
    }

    public void apply(final Traversal traversal) {
        final Step step = TraversalHelper.getEnd(traversal);
        if (TraversalHelper.isLabeled(step))
            TraversalHelper.insertStep(new LabelIdentityStep<>(traversal), traversal.getSteps().size(), traversal);
    }

    public static LabeledStepStrategy instance() {
        return INSTANCE;
    }

    public int compareTo(final TraversalStrategy traversalStrategy) {
        return traversalStrategy instanceof TraverserSourceStrategy ? -1 : 1;
    }
}
