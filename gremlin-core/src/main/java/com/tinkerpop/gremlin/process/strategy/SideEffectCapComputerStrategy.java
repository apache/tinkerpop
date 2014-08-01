package com.tinkerpop.gremlin.process.strategy;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.computer.traversal.step.map.JumpComputerStep;
import com.tinkerpop.gremlin.process.graph.step.map.JumpStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SideEffectCapComputerStrategy implements TraversalStrategy {

    private static final SideEffectCapComputerStrategy INSTANCE = new SideEffectCapComputerStrategy();

    private SideEffectCapComputerStrategy() {
    }


    public void apply(final Traversal traversal) {
        TraversalHelper.getStepsOfClass(JumpStep.class, traversal)
                .forEach(step -> TraversalHelper.replaceStep(step, new JumpComputerStep(traversal, step), traversal));
    }

    public int compareTo(final TraversalStrategy traversalStrategy) {
        if (traversalStrategy instanceof TraverserSourceStrategy)
            return -1;
        else if (traversalStrategy instanceof SideEffectCapStrategy)
            return 1;
        else
            return 1;


    }

    public static SideEffectCapComputerStrategy instance() {
        return INSTANCE;
    }
}


