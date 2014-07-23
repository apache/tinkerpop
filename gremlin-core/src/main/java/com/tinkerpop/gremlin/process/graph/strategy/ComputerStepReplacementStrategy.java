package com.tinkerpop.gremlin.process.graph.strategy;

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
public class ComputerStepReplacementStrategy implements TraversalStrategy.FinalTraversalStrategy {

    public void apply(final Traversal traversal) {

        new CountCapStrategy().apply(traversal);

        new SideEffectCapStrategy().apply(traversal);

        TraversalHelper.getStepsOfClass(JumpStep.class, traversal)
                .forEach(step -> TraversalHelper.replaceStep(step, new JumpComputerStep(traversal, step), traversal));

        TraversalHelper.getStepsOfClass(SideEffectCapStep.class, traversal)
                .forEach(step -> TraversalHelper.replaceStep(step, new SideEffectCapComputerStep<>(traversal, step), traversal));
    }
}


