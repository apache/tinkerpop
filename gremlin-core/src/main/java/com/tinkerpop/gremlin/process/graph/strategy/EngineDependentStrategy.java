package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.marker.EngineDependent;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EngineDependentStrategy extends AbstractTraversalStrategy implements TraversalStrategy {

    private static final EngineDependentStrategy INSTANCE = new EngineDependentStrategy();

    private EngineDependentStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal, final TraversalEngine traversalEngine) {
        traversal.asAdmin().getSteps().stream()
                .filter(step -> step instanceof EngineDependent)
                .forEach(step -> ((EngineDependent) step).onEngine(traversalEngine));
    }

    public static EngineDependentStrategy instance() {
        return INSTANCE;
    }
}
