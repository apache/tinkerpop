package com.tinkerpop.tinkergraph.process.graph.util.optimizers;

import com.tinkerpop.gremlin.process.Optimizer;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.tinkergraph.process.graph.map.TinkerGraphStep;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphStepSerializationOptimizer implements Optimizer.FinalOptimizer {

    public void optimize(final Traversal traversal) {

        if (traversal.getSteps().get(0) instanceof TinkerGraphStep) {
            TinkerGraphStep step = (TinkerGraphStep) traversal.getSteps().get(0);
            step.clear();
            step.clearGraph();
        }
    }
}
