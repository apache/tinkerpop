package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.CountCapStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.CountStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CountCapStrategy implements TraversalStrategy.NoDependencies {

    private static final CountCapStrategy INSTANCE = new CountCapStrategy();

    private CountCapStrategy() {
    }


    @Override
    public void apply(final Traversal traversal) {
        if (TraversalHelper.getEnd(traversal) instanceof CountStep) {
            TraversalHelper.replaceStep(TraversalHelper.getEnd(traversal), new CountCapStep<>(traversal), traversal);
        }
    }

    public static CountCapStrategy instance() {
        return INSTANCE;
    }
}