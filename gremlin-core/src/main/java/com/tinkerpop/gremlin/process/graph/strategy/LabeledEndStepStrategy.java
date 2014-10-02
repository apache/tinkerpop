package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.step.util.MarkerIdentityStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LabeledEndStepStrategy implements TraversalStrategy.NoDependencies {

    private static final LabeledEndStepStrategy INSTANCE = new LabeledEndStepStrategy();

    private LabeledEndStepStrategy() {
    }

    @Override
    public void apply(final Traversal<?,?> traversal) {
        final Step step = TraversalHelper.getEnd(traversal);
        if (TraversalHelper.isLabeled(step))
            TraversalHelper.insertStep(new MarkerIdentityStep<>(traversal), traversal.getSteps().size(), traversal);
    }

    public static LabeledEndStepStrategy instance() {
        return INSTANCE;
    }
}
