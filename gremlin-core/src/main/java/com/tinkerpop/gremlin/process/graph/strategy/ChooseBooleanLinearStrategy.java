package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.step.branch.BranchStep;
import com.tinkerpop.gremlin.process.graph.step.branch.ChooseBooleanStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Graph;

import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ChooseBooleanLinearStrategy implements TraversalStrategy.NoDependencies {

    // TODO: recursively linearlize as a branch could have a choose() step (so forth and so on)

    private static final ChooseBooleanLinearStrategy INSTANCE = new ChooseBooleanLinearStrategy();

    private ChooseBooleanLinearStrategy() {
    }

    // x.choose(p){a}{b}.y
    // x.branch(p ? this : z).a.branch(end).as(z).b.as(end).y
    public void apply(final Traversal<?, ?> traversal) {
        int chooseStepCounter = 0;
        for (final ChooseBooleanStep chooseStep : TraversalHelper.getStepsOfClass(ChooseBooleanStep.class, traversal)) {
            final String cFalseLabel = Graph.System.system("cFalse" + chooseStepCounter);
            final String cEndLabel = Graph.System.system("cEnd" + chooseStepCounter);
            chooseStepCounter++;

            final BranchStep<?> branchStep = new BranchStep<>(traversal);
            branchStep.setFunctions(traverser -> chooseStep.getChoosePredicate().test(traverser) ? BranchStep.THIS_LABEL : cFalseLabel);
            TraversalHelper.replaceStep(chooseStep, branchStep, traversal);

            Step currentStep = branchStep;
            for (final Step trueStep : (List<Step>) chooseStep.getTrueChoice().getSteps()) {
                TraversalHelper.insertAfterStep(trueStep, currentStep, traversal);
                currentStep = trueStep;
            }
            final BranchStep breakStep = new BranchStep(traversal);
            breakStep.setFunctions(new BranchStep.GoToLabel(cEndLabel));
            TraversalHelper.insertAfterStep(breakStep, currentStep, traversal);

            currentStep = breakStep;
            int c = 0;
            for (final Step falseStep : (List<Step>) chooseStep.getFalseChoice().getSteps()) {
                TraversalHelper.insertAfterStep(falseStep, currentStep, traversal);
                currentStep = falseStep;
                if (c++ == 0) falseStep.setLabel(cFalseLabel);

            }
            final IdentityStep finalStep = new IdentityStep(traversal);
            finalStep.setLabel(cEndLabel);
            TraversalHelper.insertAfterStep(finalStep, currentStep, traversal);
        }
    }

    public static ChooseBooleanLinearStrategy instance() {
        return INSTANCE;
    }
}
