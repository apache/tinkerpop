package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class IdentityRemovalStrategy extends AbstractTraversalStrategy {

    private static final IdentityRemovalStrategy INSTANCE = new IdentityRemovalStrategy();

    private IdentityRemovalStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal, final TraversalEngine engine) {
        if (!TraversalHelper.hasStepOfClass(IdentityStep.class, traversal))
            return;

        TraversalHelper.getStepsOfClass(IdentityStep.class, traversal).stream()
                .filter(step -> !step.getLabel().isPresent())
                .forEach(traversal::removeStep);
    }

    public static IdentityRemovalStrategy instance() {
        return INSTANCE;
    }
}
