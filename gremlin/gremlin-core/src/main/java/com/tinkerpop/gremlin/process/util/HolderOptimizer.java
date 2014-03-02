package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Optimizer;
import com.tinkerpop.gremlin.process.Traversal;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HolderOptimizer implements Optimizer.FinalOptimizer {

    public void optimize(final Traversal traversal) {
        final boolean trackPaths = HolderOptimizer.trackPaths(traversal);
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
