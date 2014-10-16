package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.ProfileStep;
import com.tinkerpop.gremlin.process.util.GlobalMetrics;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

public class ProfileStrategy implements TraversalStrategy {
    private static final ProfileStrategy INSTANCE = new ProfileStrategy();

    private ProfileStrategy() {
    }

    @Override
    public void apply(final Traversal<?, ?> traversal) {
        if (!TraversalHelper.hasStepOfClass(ProfileStep.class, traversal)) {
            return;
        }
        traversal.sideEffects().set(ProfileStep.METRICS_KEY, new GlobalMetrics()); // TODO: is this needed? No probably.
        traversal.getSteps().forEach(step -> step.setProfilingEnabled(true));
    }

    @Override
    public int compareTo(final TraversalStrategy traversalStrategy) {
        return 1;
    }

    public static ProfileStrategy instance() {
        return INSTANCE;
    }
}
