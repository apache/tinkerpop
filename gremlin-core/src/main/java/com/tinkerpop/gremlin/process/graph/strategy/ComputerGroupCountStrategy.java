package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.computer.traversal.step.sideEffect.GroupCountComputerStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroupCountStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ComputerGroupCountStrategy implements TraversalStrategy.FinalTraversalStrategy {

    public void apply(final Traversal traversal) {
        TraversalHelper.getStepsOfClass(GroupCountStep.class, traversal)
                .forEach(step -> TraversalHelper.replaceStep(step, new GroupCountComputerStep(traversal, step), traversal));
    }
}
