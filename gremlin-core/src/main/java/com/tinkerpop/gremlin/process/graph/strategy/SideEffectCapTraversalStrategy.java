package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectCapStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectCapable;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SideEffectCapTraversalStrategy implements TraversalStrategy.FinalTraversalStrategy {

    public void apply(final Traversal traversal) {
        if (TraversalHelper.getEnd(traversal) instanceof SideEffectCapable)
            traversal.addStep(new SideEffectCapStep(traversal, SideEffectCapable.CAP_VARIABLE));
    }
}
