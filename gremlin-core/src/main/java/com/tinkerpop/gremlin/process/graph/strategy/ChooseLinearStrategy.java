package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.step.branch.BranchStep;
import com.tinkerpop.gremlin.process.graph.step.branch.ChooseStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ChooseLinearStrategy extends AbstractTraversalStrategy implements TraversalStrategy.NoDependencies {

    // TODO: recursively linearlize as a branch could have a choose() step (so forth and so on)

    private static final ChooseLinearStrategy INSTANCE = new ChooseLinearStrategy();

    private static final String CHOOSE_PREFIX = "gremlin.choose.";
    private static final String CHOOSE_PREFIX_END = "gremlin.choose.end";

    private ChooseLinearStrategy() {
    }

    // x.choose(t -> M){a}{b}.y
    // x.branch(mapFunction.next().toString()).a.branch(end).as(z).b.as(end).y
    public void apply(final Traversal<?, ?> traversal) {
        int chooseStepCounter = 0;
        for (final ChooseStep chooseStep : TraversalHelper.getStepsOfClass(ChooseStep.class, traversal)) {
            final int currentStepCount = chooseStepCounter;
            final String endLabel = CHOOSE_PREFIX_END + currentStepCount;

            final BranchStep<?> branchStep = new BranchStep<>(traversal);
            branchStep.setFunctions(traverser -> {
                final String goTo = objectToString(chooseStep.getMapFunction().apply(traverser), currentStepCount);
                return TraversalHelper.hasLabel(goTo, traversal) ? goTo : BranchStep.EMPTY_LABEL;
            });
            TraversalHelper.replaceStep(chooseStep, branchStep, traversal);

            Step currentStep = branchStep;
            for (final Map.Entry<?, Traversal<?, ?>> entry : (Set<Map.Entry>) chooseStep.getChoices().entrySet()) {
                int c = 0;
                for (final Step mapStep : entry.getValue().getSteps()) {
                    TraversalHelper.insertAfterStep(mapStep, currentStep, traversal);
                    currentStep = mapStep;
                    if (c++ == 0) currentStep.setLabel(objectToString(entry.getKey(), currentStepCount));
                }
                final BranchStep breakStep = new BranchStep(traversal);
                breakStep.setFunctions(new BranchStep.GoToLabel(endLabel));
                TraversalHelper.insertAfterStep(breakStep, currentStep, traversal);
                currentStep = breakStep;
            }

            final IdentityStep finalStep = new IdentityStep(traversal);
            finalStep.setLabel(endLabel);
            TraversalHelper.insertAfterStep(finalStep, currentStep, traversal);

        }
    }

    private static final String objectToString(final Object object, final int currentStepCount) {
        return CHOOSE_PREFIX + object.toString() + ":" + object.getClass().getCanonicalName() + ":" + currentStepCount;
    }

    public static ChooseLinearStrategy instance() {
        return INSTANCE;
    }
}
