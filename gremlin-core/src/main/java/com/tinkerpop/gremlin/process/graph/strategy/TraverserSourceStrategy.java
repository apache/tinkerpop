package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.marker.TraverserSource;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraverserSourceStrategy extends AbstractTraversalStrategy implements TraversalStrategy {

    private static final TraverserSourceStrategy INSTANCE = new TraverserSourceStrategy();

    private TraverserSourceStrategy() {
    }

    @Override
    public void apply(final Traversal<?,?> traversal) {
        final boolean trackPaths = TraversalHelper.trackPaths(traversal);
        traversal.getSteps().forEach(step -> {
            if (step instanceof TraverserSource)
                ((TraverserSource) step).generateTraverserIterator(trackPaths);
        });
    }

    @Override
    public int compareTo(final TraversalStrategy traversalStrategy) {
        return 1;
    }

    public static TraverserSourceStrategy instance() {
        return INSTANCE;
    }
}
