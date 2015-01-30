package com.tinkerpop.gremlin.process.graph.traversal.strategy;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectRegistrar;
import com.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.Collections;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SideEffectRegistrationStrategy extends AbstractTraversalStrategy implements TraversalStrategy {

    private static final SideEffectRegistrationStrategy INSTANCE = new SideEffectRegistrationStrategy();

    private static final Set<Class<? extends TraversalStrategy>> PRIORS = Collections.singleton(SideEffectCapStrategy.class);

    private SideEffectRegistrationStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal, final TraversalEngine engine) {
        TraversalHelper.getStepsOfAssignableClass(SideEffectRegistrar.class, traversal).forEach(SideEffectRegistrar::registerSideEffects);
    }

    @Override
    public Set<Class<? extends TraversalStrategy>> applyPrior() {
        return PRIORS;
    }

    public static SideEffectRegistrationStrategy instance() {
        return INSTANCE;
    }
}
