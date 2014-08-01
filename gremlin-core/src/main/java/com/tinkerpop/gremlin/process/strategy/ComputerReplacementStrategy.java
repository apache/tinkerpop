package com.tinkerpop.gremlin.process.strategy;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.computer.traversal.step.map.JumpComputerStep;
import com.tinkerpop.gremlin.process.computer.traversal.step.sideEffect.SideEffectCapComputerStep;
import com.tinkerpop.gremlin.process.graph.step.map.JumpStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectCapStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ComputerReplacementStrategy implements TraversalStrategy {

    private static final ComputerReplacementStrategy INSTANCE = new ComputerReplacementStrategy();

    private ComputerReplacementStrategy() {
    }

    public void apply(final Traversal traversal) {

        TraversalHelper.getStepsOfClass(JumpStep.class, traversal)
                .forEach(step -> TraversalHelper.replaceStep(step, new JumpComputerStep(traversal, step), traversal));

        TraversalHelper.getStepsOfClass(SideEffectCapStep.class, traversal)
                .forEach(step -> TraversalHelper.replaceStep(step, new SideEffectCapComputerStep<>(traversal, step), traversal));
    }

    public int compareTo(final TraversalStrategy traversalStrategy) {
        if (traversalStrategy instanceof SideEffectCapStrategy)
            return 1;
        else if (traversalStrategy instanceof UnrollJumpStrategy || traversalStrategy instanceof TraverserSourceStrategy)
            return -1;
        else
            return 0;
    }

    public static ComputerReplacementStrategy instance() {
        return INSTANCE;
    }
}


