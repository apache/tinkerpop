package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.step.branch.BranchStep;
import com.tinkerpop.gremlin.process.graph.step.branch.RepeatStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RepeatLinearStrategy extends AbstractTraversalStrategy {

    private static final RepeatLinearStrategy INSTANCE = new RepeatLinearStrategy();

    private static final Set<Class<? extends TraversalStrategy>> POSTS = new HashSet<>(Arrays.asList(
            ChooseLinearStrategy.class,
            UnionLinearStrategy.class,
            UntilStrategy.class,
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
            for (final Step<?, ?> step : repeatStep.getRepeatTraversal().asAdmin().getSteps()) {
                TraversalHelper.insertAfterStep(step, currentStep, traversal);
                currentStep = step;
            }
            final SideEffectStep<?> incrLoopStep = new SideEffectStep<>(traversal);
            incrLoopStep.setConsumer(traverser -> traverser.asAdmin().incrLoops());
            TraversalHelper.insertAfterStep(incrLoopStep, currentStep, traversal);
            /////////
            final BranchStep leftBranchStep = new BranchStep(traversal);
            final BranchStep rightBranchStep = new BranchStep(traversal);
            TraversalHelper.insertAfterStep(leftBranchStep, firstStep, traversal);
            TraversalHelper.insertAfterStep(rightBranchStep, incrLoopStep, traversal);
            /////////
            final SideEffectStep<?> resetLoopStep = new SideEffectStep<>(traversal);
            resetLoopStep.setConsumer(traverser -> traverser.asAdmin().resetLoops());
            TraversalHelper.insertAfterStep(resetLoopStep, rightBranchStep, traversal);

            // HANDLE UNTIL
            if (repeatStep.isUntilFirst()) {
                leftBranchStep.addFunction(new BranchStep.GoToLabelWithPredicate<>(resetLoopStep.getLabel(), repeatStep.getUntilPredicate(), BranchStep.THIS_LABEL));
                rightBranchStep.addFunction(t -> leftBranchStep.getLabel());
            } else {
                rightBranchStep.addFunction(new BranchStep.GoToLabelWithPredicate<>(BranchStep.THIS_BREAK_LABEL, repeatStep.getUntilPredicate(), leftBranchStep.getLabel()));
                leftBranchStep.addFunction(t -> BranchStep.THIS_LABEL);
            }

            // HANDLE EMIT
            if (null != repeatStep.getEmitPredicate()) {
                if (repeatStep.isEmitFirst()) {
                    leftBranchStep.addFunction(new BranchStep.GoToLabelWithPredicate<>(resetLoopStep.getLabel(), repeatStep.getEmitPredicate().and((Predicate) repeatStep.getUntilPredicate().negate()), BranchStep.EMPTY_LABEL));
                } else {
                    rightBranchStep.addFunction(new BranchStep.GoToLabelWithPredicate<>(resetLoopStep.getLabel(), repeatStep.getEmitPredicate(), BranchStep.EMPTY_LABEL));
                }
            }
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
