package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.computer.traversal.step.sideEffect.SideEffectCapComputerStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectCapStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SideEffectCapComputerStrategy implements TraversalStrategy {

    private static final SideEffectCapComputerStrategy INSTANCE = new SideEffectCapComputerStrategy();

    private SideEffectCapComputerStrategy() {
    }


    @Override
    public void apply(final Traversal traversal) {
        TraversalHelper.getStepsOfClass(SideEffectCapStep.class, traversal)
                .forEach(step -> TraversalHelper.replaceStep(step, new SideEffectCapComputerStep<>(traversal, step), traversal));
    }

    @Override
    public int compareTo(final TraversalStrategy traversalStrategy) {
        return traversalStrategy instanceof TraverserSourceStrategy ? -1 : 1;
    }

    public static SideEffectCapComputerStrategy instance() {
        return INSTANCE;
    }
}


