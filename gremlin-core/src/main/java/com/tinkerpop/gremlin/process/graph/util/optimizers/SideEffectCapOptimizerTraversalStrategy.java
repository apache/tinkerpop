package com.tinkerpop.gremlin.process.graph.util.optimizers;

import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.sideEffect.SideEffectCapStep;
import com.tinkerpop.gremlin.process.graph.sideEffect.SideEffectCapable;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SideEffectCapOptimizerTraversalStrategy implements TraversalStrategy.FinalTraversalStrategy {

    public void apply(final Traversal traversal) {
        if (TraversalHelper.getEnd(traversal) instanceof SideEffectCapable)
            traversal.addStep(new SideEffectCapStep(traversal));
    }
}
