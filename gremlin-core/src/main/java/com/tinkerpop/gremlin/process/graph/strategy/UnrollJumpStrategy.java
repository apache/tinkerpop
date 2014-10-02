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
public class UnrollJumpStrategy implements TraversalStrategy {

    private static final UnrollJumpStrategy INSTANCE = new UnrollJumpStrategy();

    private UnrollJumpStrategy() {
    }

    @Override
    public void apply(final Traversal<?,?> traversal) {

        TraversalHelper.getStepsOfClass(JumpStep.class, traversal).stream()
                .filter(JumpStep::unRollable)
                .forEach(toStep -> {
                    if (toStep.isDoWhile()) {
                        // DO WHILE SEMANTICS
                        final Step fromStep = TraversalHelper.getStep(toStep.jumpLabel, traversal);
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
                            toStep.getPreviousStep().setLabel(toStep.getLabel());
                        TraversalHelper.removeStep(toStep, traversal);
                    } else {
                        // WHILE DO SEMANTICS
                        final JumpStep fromStep = (JumpStep) TraversalHelper.getStep(toStep.jumpLabel, traversal);
                        final List<Step> stepsToClone = TraversalHelper.isolateSteps(toStep, fromStep);
                        stepsToClone.forEach(stepToClone -> TraversalHelper.removeStep(stepToClone, traversal));
                        for (int i = 0; i < (toStep.loops + 1); i++) {
                            for (int j = stepsToClone.size() - 1; j >= 0; j--) {
                                try {
                                    final Step clonedStep = (Step) stepsToClone.get(j).clone();
                                    TraversalHelper.insertStep(clonedStep, traversal.getSteps().indexOf(fromStep) + 1, traversal);
                                } catch (final CloneNotSupportedException e) {
                                    throw new IllegalStateException(e.getMessage(), e);
                                }
                            }
                        }
                        // System.out.println(toStep + "::" + fromStep + "::" + fromStep.jumpLabel);
                        TraversalHelper.removeStep(TraversalHelper.getStep(fromStep.jumpLabel, traversal), traversal); // identity marker
                        TraversalHelper.removeStep(toStep, traversal); // left hand jump
                        TraversalHelper.removeStep(fromStep, traversal); // right hand jump
                        if (TraversalHelper.isLabeled(toStep))
                            toStep.getPreviousStep().setLabel(toStep.getLabel());
                        TraversalHelper.removeStep(toStep, traversal);
                    }
                });
    }

    @Override
    public int compareTo(final TraversalStrategy traversalStrategy) {
        return traversalStrategy instanceof TraverserSourceStrategy ? -1 : 1;
    }

    public static UnrollJumpStrategy instance() {
        return INSTANCE;
    }
}
