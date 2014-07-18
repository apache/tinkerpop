package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.step.map.JumpStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class UnrollJumpStrategy implements TraversalStrategy.FinalTraversalStrategy {

    public void apply(final Traversal traversal) {
        for (final JumpStep toStep : TraversalHelper.getStepsOfClass(JumpStep.class, traversal)) {
            if (toStep.unRollable()) {
                final Step fromStep = TraversalHelper.getAs(toStep.jumpAs, traversal);
                final List<Step> steps = TraversalHelper.isolateSteps(fromStep, toStep);
                for (int i = 0; i < toStep.loops - 1; i++) {
                    for (int j = steps.size() - 1; j >= 0; j--) {
                        final Step clonedStep = TraversalHelper.cloneStep(steps.get(j));
                        TraversalHelper.insertStep(clonedStep, traversal.getSteps().indexOf(fromStep) + 1, traversal);
                    }
                }
                TraversalHelper.removeStep(toStep, traversal);
            }
        }
    }
}
