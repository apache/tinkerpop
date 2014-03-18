package com.tinkerpop.gremlin.process.graph.util.optimizers;

import com.tinkerpop.gremlin.process.Optimizer;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.sideEffect.SideEffectCapStep;
import com.tinkerpop.gremlin.process.graph.sideEffect.SideEffectCapable;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SideEffectCapOptimizer implements Optimizer.FinalOptimizer {

    public void optimize(final Traversal traversal) {
        if (TraversalHelper.getEnd(traversal) instanceof SideEffectCapable)
            traversal.addStep(new SideEffectCapStep(traversal));
    }
}
