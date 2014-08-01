package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.computer.traversal.step.map.JumpComputerStep;
import com.tinkerpop.gremlin.process.graph.step.map.JumpStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class JumpComputerStrategy implements TraversalStrategy {

    private static final JumpComputerStrategy INSTANCE = new JumpComputerStrategy();

    private JumpComputerStrategy() {
    }

    public void apply(final Traversal traversal) {

        TraversalHelper.getStepsOfClass(JumpStep.class, traversal)
                .forEach(step -> TraversalHelper.replaceStep(step, new JumpComputerStep(traversal, step), traversal));

    }

    public int compareTo(final TraversalStrategy traversalStrategy) {
        if (traversalStrategy instanceof TraverserSourceStrategy || traversalStrategy instanceof UnrollJumpStrategy)
            return -1;
        else
            return 1;
    }

    public static JumpComputerStrategy instance() {
        return INSTANCE;
    }
}

