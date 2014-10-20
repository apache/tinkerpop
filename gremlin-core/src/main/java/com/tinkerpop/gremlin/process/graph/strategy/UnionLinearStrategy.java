package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.step.branch.BranchStep;
import com.tinkerpop.gremlin.process.graph.step.branch.UnionStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class UnionLinearStrategy extends AbstractTraversalStrategy implements TraversalStrategy.NoDependencies {

    private static final UnionLinearStrategy INSTANCE = new UnionLinearStrategy();

    private static final String UNION = "gremlin.union.";
    private static final String UNION_END = "gremlin.union.end.";

    private UnionLinearStrategy() {
    }

    // x.union(a,b).y
    // x.branch(t->a,t->b).a.branch(end).as(z).b.as(end).y
    public void apply(final Traversal<?, ?> traversal) {
        if (!TraversalHelper.hasStepOfClass(UnionStep.class, traversal))
            return;

        int unionStepCounter = 0;
        for (final UnionStep<?, ?> unionStep : TraversalHelper.getStepsOfClass(UnionStep.class, traversal)) {
            final String endLabel = UNION_END + unionStepCounter;
            final List<Function<Traverser, String>> branchFunctions = new ArrayList<>();
            for (int i = 0; i < unionStep.getTraversals().length; i++) {
                final String unionBranchStart = UNION + unionStepCounter + "." + i;
                branchFunctions.add(traverser -> unionBranchStart);
            }

            final BranchStep<?> branchStep = new BranchStep<>(traversal);
            branchStep.setFunctions(branchFunctions.toArray(new Function[branchFunctions.size()]));
            TraversalHelper.replaceStep(unionStep, branchStep, traversal);

            Step currentStep = branchStep;
            for (int i = 0; i < unionStep.getTraversals().length; i++) {
                final String unionBranchStart = UNION + unionStepCounter + "." + i;
                int c = 0;
                for (final Step branchstep : unionStep.getTraversals()[i].getSteps()) {
                    TraversalHelper.insertAfterStep(branchstep, currentStep, traversal);
                    currentStep = branchstep;
                    if (c++ == 0) currentStep.setLabel(unionBranchStart);
                }
                final BranchStep breakStep = new BranchStep(traversal);
                breakStep.setFunctions(new BranchStep.GoToLabel(endLabel));
                TraversalHelper.insertAfterStep(breakStep, currentStep, traversal);
                currentStep = breakStep;
            }

            final IdentityStep finalStep = new IdentityStep(traversal);
            finalStep.setLabel(endLabel);
            TraversalHelper.insertAfterStep(finalStep, currentStep, traversal);
            unionStepCounter++;
        }
    }

    public static UnionLinearStrategy instance() {
        return INSTANCE;
    }
}