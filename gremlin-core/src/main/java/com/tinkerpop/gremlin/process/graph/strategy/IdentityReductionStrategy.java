package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class IdentityReductionStrategy extends AbstractTraversalStrategy implements TraversalStrategy.NoDependencies {

    private static final IdentityReductionStrategy INSTANCE = new IdentityReductionStrategy();

    private IdentityReductionStrategy() {
    }

    @Override
    public void apply(final Traversal<?, ?> traversal) {
        if (!TraversalHelper.hasStepOfClass(IdentityStep.class, traversal))
            return;

        TraversalHelper.getStepsOfClass(IdentityStep.class, traversal)
                .stream()
                .filter(step -> !TraversalHelper.isLabeled(step))
                .forEach(step -> TraversalHelper.removeStep((Step) step, traversal));
    }

    public static IdentityReductionStrategy instance() {
        return INSTANCE;
    }
}
