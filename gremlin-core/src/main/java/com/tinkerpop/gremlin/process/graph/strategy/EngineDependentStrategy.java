package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.marker.EngineDependent;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EngineDependentStrategy extends AbstractTraversalStrategy implements TraversalStrategy {

    private static final EngineDependentStrategy INSTANCE = new EngineDependentStrategy();

    private EngineDependentStrategy() {
    }

    @Override
    public void apply(final Traversal<?, ?> traversal, final TraversalEngine traversalEngine) {
        if (traversalEngine.equals(TraversalEngine.COMPUTER))
            traversal.sideEffects().removeGraph();
        traversal.getSteps().stream()
                .filter(step -> step instanceof EngineDependent)
                .forEach(step -> ((EngineDependent) step).onEngine(traversalEngine));
    }

    public static EngineDependentStrategy instance() {
        return INSTANCE;
    }

    /*public int compareTo(final TraversalStrategy traversalStrategy) {
        return traversalStrategy instanceof TraverserSourceStrategy ? -1 : 1;
    }*/
}
