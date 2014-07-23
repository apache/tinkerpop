package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.computer.traversal.step.map.JumpComputerStep;
import com.tinkerpop.gremlin.process.computer.traversal.step.sideEffect.AggregateComputerStep;
import com.tinkerpop.gremlin.process.computer.traversal.step.sideEffect.CountCapComputerStep;
import com.tinkerpop.gremlin.process.computer.traversal.step.sideEffect.GroupByComputerStep;
import com.tinkerpop.gremlin.process.computer.traversal.step.sideEffect.GroupCountComputerStep;
import com.tinkerpop.gremlin.process.computer.traversal.step.sideEffect.SideEffectCapComputerStep;
import com.tinkerpop.gremlin.process.computer.traversal.step.sideEffect.StoreComputerStep;
import com.tinkerpop.gremlin.process.graph.step.map.JumpStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.AggregateStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.CountCapStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroupByStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroupCountStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectCapStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StoreStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ComputerStepReplacementStrategy implements TraversalStrategy.FinalTraversalStrategy {

    public void apply(final Traversal traversal) {

        new CountCapStrategy().apply(traversal);

        new SideEffectCapStrategy().apply(traversal);

        TraversalHelper.getStepsOfClass(CountCapStep.class, traversal)
                .forEach(step -> TraversalHelper.replaceStep(step, new CountCapComputerStep<>(traversal, step), traversal));

        TraversalHelper.getStepsOfClass(JumpStep.class, traversal)
                .forEach(step -> TraversalHelper.replaceStep(step, new JumpComputerStep(traversal, step), traversal));

        TraversalHelper.getStepsOfClass(AggregateStep.class, traversal)
                .forEach(step -> TraversalHelper.replaceStep(step, new AggregateComputerStep<>(traversal, step), traversal));

        TraversalHelper.getStepsOfClass(GroupByStep.class, traversal)
                .forEach(step -> TraversalHelper.replaceStep(step, new GroupByComputerStep(traversal, step), traversal));

        TraversalHelper.getStepsOfClass(GroupCountStep.class, traversal)
                .forEach(step -> TraversalHelper.replaceStep(step, new GroupCountComputerStep(traversal, step), traversal));

        TraversalHelper.getStepsOfClass(StoreStep.class, traversal)
                .forEach(step -> TraversalHelper.replaceStep(step, new StoreComputerStep<>(traversal, step), traversal));

        TraversalHelper.getStepsOfClass(SideEffectCapStep.class, traversal)
                .forEach(step -> TraversalHelper.replaceStep(step, new SideEffectCapComputerStep<>(traversal, step), traversal));
    }
}


