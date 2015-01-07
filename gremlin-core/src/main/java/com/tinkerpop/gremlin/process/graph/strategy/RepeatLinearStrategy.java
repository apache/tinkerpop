package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.step.branch.BranchStep;
import com.tinkerpop.gremlin.process.graph.step.branch.RepeatStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RepeatLinearStrategy extends AbstractTraversalStrategy {

    private static final RepeatLinearStrategy INSTANCE = new RepeatLinearStrategy();

    private static final Set<Class<? extends TraversalStrategy>> POSTS = new HashSet<>(Arrays.asList(
            ChooseLinearStrategy.class,
            UnionLinearStrategy.class,
            EngineDependentStrategy.class));

    private RepeatLinearStrategy() {
    }


    public void apply(final Traversal<?, ?> traversal, final TraversalEngine engine) {
        if (engine.equals(TraversalEngine.STANDARD) || !TraversalHelper.hasStepOfClass(RepeatStep.class, traversal))
            return;

        for (final RepeatStep<?> repeatStep : TraversalHelper.getStepsOfClass(RepeatStep.class, traversal)) {
            if (null == repeatStep.getRepeatTraversal())
                throw new IllegalStateException("No traversal to repeat was provided to RepeatStep");

            Step currentStep = repeatStep.getPreviousStep();
            final Step firstStep = currentStep;
            TraversalHelper.removeStep(repeatStep, traversal);
            currentStep = TraversalHelper.insertTraversal(repeatStep.getRepeatTraversal(), currentStep, traversal);
            final SideEffectStep<?> incrLoopStep = new SideEffectStep<>(traversal);
            incrLoopStep.setConsumer(traverser -> traverser.asAdmin().incrLoops());
            TraversalHelper.insertAfterStep(incrLoopStep, currentStep, traversal);
            /////////
            final BranchStep<?> leftBranchStep = new BranchStep<>(traversal);
            final BranchStep<?> rightBranchStep = new BranchStep<>(traversal);
            TraversalHelper.insertAfterStep(leftBranchStep, firstStep, traversal);
            TraversalHelper.insertAfterStep(rightBranchStep, incrLoopStep, traversal);
            /////////
            final SideEffectStep<?> resetLoopStep = new SideEffectStep<>(traversal);
            resetLoopStep.setConsumer(traverser -> traverser.asAdmin().resetLoops());
            TraversalHelper.insertAfterStep(resetLoopStep, rightBranchStep, traversal);

            leftBranchStep.setFunction(traverser -> {
                final List<String> stepLabels = new ArrayList<>(2);
                if (repeatStep.isUntilFirst()) {    // left until
                    if (repeatStep.doRepeat((Traverser) traverser)) {
                        stepLabels.add(resetLoopStep.getPreviousStep().getLabel());
                        return stepLabels;
                    } else {
                        stepLabels.add("");
                        if (repeatStep.isEmitFirst() && repeatStep.doEmit((Traverser) traverser))
                            stepLabels.add(resetLoopStep.getPreviousStep().getLabel());
                        return stepLabels;
                    }
                } else {  // right until
                    stepLabels.add("");
                    if (repeatStep.isEmitFirst() && repeatStep.doEmit((Traverser) traverser))
                        stepLabels.add(resetLoopStep.getPreviousStep().getLabel());
                    return stepLabels;
                }
            });

            rightBranchStep.setFunction(traverser -> {
                final List<String> stepLabels = new ArrayList<>(2);
                if (!repeatStep.isUntilFirst()) {      // right until
                    if (repeatStep.doRepeat((Traverser) traverser)) {
                        stepLabels.add("");
                        return stepLabels;
                    } else {
                        stepLabels.add(leftBranchStep.getPreviousStep().getLabel());
                        if (!repeatStep.isEmitFirst() && repeatStep.doEmit((Traverser) traverser))
                            stepLabels.add("");
                        return stepLabels;
                    }

                } else { // left until
                    stepLabels.add(leftBranchStep.getPreviousStep().getLabel());
                    if (!repeatStep.isEmitFirst() && repeatStep.doEmit((Traverser) traverser)) {
                        stepLabels.add("");
                    }
                    return stepLabels;
                }
            });
        }
    }

    public static RepeatLinearStrategy instance() {
        return INSTANCE;
    }

    @Override
    public Set<Class<? extends TraversalStrategy>> applyPost() {
        return POSTS;
    }
}
