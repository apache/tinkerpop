package com.tinkerpop.gremlin.process.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.step.map.JumpStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class UnrollJumpStrategy implements TraversalStrategy {

    private static final UnrollJumpStrategy INSTANCE = new UnrollJumpStrategy();

    private UnrollJumpStrategy() {
    }

    public void apply(final Traversal traversal) {

        TraversalHelper.getStepsOfClass(JumpStep.class, traversal).stream()
                .filter(JumpStep::unRollable)
                .forEach(toStep -> {
                    final Step fromStep = TraversalHelper.getAs(toStep.jumpAs, traversal);
                    final List<Step> stepsToClone = TraversalHelper.isolateSteps(fromStep, toStep);
                    stepsToClone.forEach(stepToClone -> TraversalHelper.removeStep(stepToClone, traversal));
                    for (int i = 0; i < toStep.loops; i++) {
                        for (int j = stepsToClone.size() - 1; j >= 0; j--) {
                            try {
                                final Step clonedStep = (Step) stepsToClone.get(j).clone();
                                TraversalHelper.insertStep(clonedStep, traversal.getSteps().indexOf(fromStep) + 1, traversal);
                            } catch (final CloneNotSupportedException e) {
                                throw new IllegalStateException(e.getMessage(), e);
                            }
                        }
                    }
                    if (TraversalHelper.isLabeled(toStep))
                        toStep.getPreviousStep().setAs(toStep.getAs());
                    TraversalHelper.removeStep(toStep, traversal);
                });
    }

    public int compareTo(final TraversalStrategy traversalStrategy) {
        return traversalStrategy instanceof TraverserSourceStrategy ? -1 : 1;
    }

    public static UnrollJumpStrategy instance() {
        return INSTANCE;
    }
}
