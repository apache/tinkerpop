package com.tinkerpop.gremlin.process.strategy;

import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.HolderSource;
import com.tinkerpop.gremlin.process.util.PathConsumer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HolderTraversalStrategy implements TraversalStrategy.FinalTraversalStrategy {

    public void apply(final Traversal traversal) {
        final boolean trackPaths = HolderTraversalStrategy.trackPaths(traversal);
        traversal.getSteps().forEach(step -> {
            if (step instanceof HolderSource)
                ((HolderSource) step).generateHolderIterator(trackPaths);
        });
    }

    public static <S, E> boolean trackPaths(final Traversal<S, E> traversal) {
        return traversal.getSteps().stream()
                .filter(step -> step instanceof PathConsumer)
                .findFirst()
                .isPresent();
    }

    public static <S, E> void doPathTracking(final Traversal<S, E> traversal) {
        traversal.getSteps().forEach(step -> {
            if (step instanceof HolderSource)
                ((HolderSource) step).generateHolderIterator(true);
        });
    }
}
