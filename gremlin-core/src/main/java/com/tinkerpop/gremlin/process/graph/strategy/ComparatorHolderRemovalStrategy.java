package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.graph.marker.ComparatorHolder;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ComparatorHolderRemovalStrategy extends AbstractTraversalStrategy {

    private static final ComparatorHolderRemovalStrategy INSTANCE = new ComparatorHolderRemovalStrategy();

    private ComparatorHolderRemovalStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal, final TraversalEngine engine) {
        if (engine.equals(TraversalEngine.STANDARD))
            return;

        if (TraversalHelper.hasStepOfAssignableClass(ComparatorHolder.class, traversal)) {
            final Step endStep = TraversalHelper.getEnd(traversal);
            TraversalHelper.getStepsOfAssignableClass(ComparatorHolder.class, traversal)
                    .stream()
                    .filter(step -> step != endStep)
                    .forEach(step -> traversal.removeStep(step));
        }
    }

    public static ComparatorHolderRemovalStrategy instance() {
        return INSTANCE;
    }
}
