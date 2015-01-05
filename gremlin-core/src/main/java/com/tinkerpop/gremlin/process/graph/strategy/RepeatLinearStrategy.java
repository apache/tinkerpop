package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.step.branch.RepeatStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

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
        if (!TraversalHelper.hasStepOfClass(RepeatStep.class, traversal))
            return;

        for (final RepeatStep<?> repeatStep : TraversalHelper.getStepsOfClass(RepeatStep.class, traversal)) {
            if (null == repeatStep.getRepeatTraversal())
                throw new IllegalStateException("No traversal to repeat was provided to RepeatStep");

            Step currentStep = repeatStep.getPreviousStep();
            final Step firstStep = currentStep;
            // TODO: ?? currentStep.setLabel("AAA");
            TraversalHelper.removeStep(repeatStep, traversal);
            for (final Step<?, ?> step : repeatStep.getRepeatTraversal().asAdmin().getSteps()) {
                TraversalHelper.insertAfterStep(step, currentStep, traversal);
                currentStep = step;
            }
            //////
            if (repeatStep.isUntilFirst()) {
                TraversalHelper.insertAfterStep(repeatStep.createUntilStep(currentStep.getLabel()), firstStep, traversal);
            } else {
                TraversalHelper.insertAfterStep(repeatStep.createJumpStep(firstStep.getLabel()), currentStep, traversal);
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
