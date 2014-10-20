package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.step.branch.JumpStep;
import com.tinkerpop.gremlin.process.graph.step.map.match.MatchStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class UnrollJumpStrategy extends AbstractTraversalStrategy implements TraversalStrategy {

    private static final UnrollJumpStrategy INSTANCE = new UnrollJumpStrategy();

    private UnrollJumpStrategy() {
    }

    @Override
    public void apply(final Traversal<?, ?> traversal) {
        if (!TraversalHelper.hasStepOfClass(JumpStep.class, traversal))
            return;
        TraversalHelper.getStepsOfClass(JumpStep.class, traversal).stream()
                .filter(JumpStep::unRollable)
                // TODO: filter() when do unroll and when not to depending on the depth of looping?
                .forEach(toStep -> {
                    if (toStep.isDoWhile()) {
                        // DO WHILE SEMANTICS
                        final Step fromStep = TraversalHelper.getStep(toStep.getJumpLabel(), traversal);
                        final List<Step> stepsToClone = TraversalHelper.isolateSteps(fromStep, toStep);
                        stepsToClone.forEach(stepToClone -> TraversalHelper.removeStep(stepToClone, traversal));
                        for (int i = 0; i < (short) toStep.getJumpLoops().getValue0(); i++) {
                            for (int j = stepsToClone.size() - 1; j >= 0; j--) {
                                try {
                                    final Step clonedStep = stepsToClone.get(j).clone();
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
                        final JumpStep fromStep = (JumpStep) TraversalHelper.getStep(toStep.getJumpLabel(), traversal);
                        final List<Step> stepsToClone = TraversalHelper.isolateSteps(toStep, fromStep);
                        stepsToClone.forEach(stepToClone -> TraversalHelper.removeStep(stepToClone, traversal));
                        for (int i = 0; i < ((short) toStep.getJumpLoops().getValue0() + 1); i++) {
                            for (int j = stepsToClone.size() - 1; j >= 0; j--) {
                                try {
                                    final Step clonedStep = stepsToClone.get(j).clone();
                                    TraversalHelper.insertStep(clonedStep, traversal.getSteps().indexOf(fromStep) + 1, traversal);
                                } catch (final CloneNotSupportedException e) {
                                    throw new IllegalStateException(e.getMessage(), e);
                                }
                            }
                        }
                        // System.out.println(toStep + "::" + fromStep + "::" + fromStep.jumpLabel);
                        TraversalHelper.removeStep(TraversalHelper.getStep(fromStep.getJumpLabel(), traversal), traversal); // identity marker
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