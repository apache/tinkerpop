package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.computer.traversal.step.map.BackComputerStep;
import com.tinkerpop.gremlin.process.graph.step.map.BackStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BackComputerStrategy implements TraversalStrategy {

    private static final BackComputerStrategy INSTANCE = new BackComputerStrategy();

    private BackComputerStrategy() {
    }

    public void apply(final Traversal traversal) {

        TraversalHelper.getStepsOfClass(BackStep.class, traversal)
                .forEach(step -> TraversalHelper.replaceStep(step, new BackComputerStep<>(traversal, step), traversal));

    }

    public int compareTo(final TraversalStrategy traversalStrategy) {
        return traversalStrategy instanceof TraverserSourceStrategy ? -1 : 1;
    }

    public static BackComputerStrategy instance() {
        return INSTANCE;
    }
}

