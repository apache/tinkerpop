package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.step.filter.AsIdentityStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.strategy.Strategy;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AsStrategy implements TraversalStrategy.NoDependencies {

    private static final AsStrategy INSTANCE = new AsStrategy();

    private AsStrategy() {
    }

    public void apply(final Traversal traversal) {
        final Step step = TraversalHelper.getEnd(traversal);
        if (TraversalHelper.isLabeled(step))
            TraversalHelper.insertStep(new AsIdentityStep<>(traversal), traversal.getSteps().size(), traversal);
    }

    public static AsStrategy instance() {
        return INSTANCE;
    }

}
