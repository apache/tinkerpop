package com.tinkerpop.gremlin.tinkergraph.process.graph.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.marker.TraverserSource;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ClearTraverserSourceStrategy implements TraversalStrategy.FinalTraversalStrategy {

    public void apply(final Traversal traversal) {
        final Step step = (Step) traversal.getSteps().get(0);
        if (step instanceof TraverserSource) {
            ((TraverserSource) step).clear();
        }
    }
}