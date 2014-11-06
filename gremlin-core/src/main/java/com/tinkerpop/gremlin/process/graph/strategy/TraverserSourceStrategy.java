package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.marker.TraverserSource;
import com.tinkerpop.gremlin.process.TraverserGenerator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraverserSourceStrategy extends AbstractTraversalStrategy implements TraversalStrategy {

    private static final TraverserSourceStrategy INSTANCE = new TraverserSourceStrategy();

    private TraverserSourceStrategy() {
    }

    @Override
    public void apply(final Traversal<?, ?> traversal, final TraversalEngine engine) {
        if (engine.equals(TraversalEngine.COMPUTER))
            return;

        final TraverserGenerator traverserGenerator = traversal.getStrategies().getTraverserGenerator();
        traversal.getSteps()
                .stream()
                .filter(step -> step instanceof TraverserSource)
                .forEach(step -> ((TraverserSource) step).generateTraversers(traverserGenerator));
    }

    @Override
    public int compareTo(final TraversalStrategy traversalStrategy) {
        return 1;
    }

    public static TraverserSourceStrategy instance() {
        return INSTANCE;
    }
}
