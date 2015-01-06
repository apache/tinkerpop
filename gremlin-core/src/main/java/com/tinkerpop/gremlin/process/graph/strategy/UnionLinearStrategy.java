package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.graph.step.branch.BranchStep;
import com.tinkerpop.gremlin.process.graph.step.branch.UnionStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class UnionLinearStrategy extends AbstractTraversalStrategy {

    private static final UnionLinearStrategy INSTANCE = new UnionLinearStrategy();

    private static final String UNION = "gremlin.union.";
    private static final String UNION_END = "gremlin.union.end.";

    private UnionLinearStrategy() {
    }

    // x.union(a,b).y
    // x.branch(t->a,t->b).a.branch(end).as(z).b.as(end).y
    public void apply(final Traversal<?, ?> traversal, final TraversalEngine engine) {
        if (engine.equals(TraversalEngine.STANDARD) || !TraversalHelper.hasStepOfClass(UnionStep.class, traversal))
            return;

        int unionStepCounter = 0;
        for (final UnionStep<?, ?> unionStep : TraversalHelper.getStepsOfClass(UnionStep.class, traversal)) {
            final String endLabel = UNION_END + unionStepCounter;
            final Collection<String> branchLabels = new ArrayList<>();
            for (int i = 0; i < unionStep.getTraversals().length; i++) {
                branchLabels.add(UNION + unionStepCounter + "." + i);
            }

            BranchStep branchStep = new BranchStep<>(traversal);
            branchStep.addFunction(new BranchStep.GoToLabels<>(branchLabels));
            TraversalHelper.replaceStep(unionStep, branchStep, traversal);

            Step currentStep = branchStep;
            for (int i = 0; i < unionStep.getTraversals().length; i++) {
                currentStep.setLabel(UNION + unionStepCounter + "." + i);
                currentStep = TraversalHelper.insertTraversal(unionStep.getTraversals()[i], currentStep, traversal);
                if (i == unionStep.getTraversals().length - 1) {
                    final IdentityStep finalStep = new IdentityStep(traversal);
                    finalStep.setLabel(endLabel);
                    TraversalHelper.insertAfterStep(finalStep, currentStep, traversal);
                    break;
                } else {
                    branchStep = new BranchStep(traversal);
                    branchStep.addFunction(new BranchStep.GoToLabels(Collections.singletonList(endLabel)));
                    TraversalHelper.insertAfterStep(branchStep, currentStep, traversal);
                    currentStep = branchStep;
                }
            }
            unionStepCounter++;
        }
    }

    public static UnionLinearStrategy instance() {
        return INSTANCE;
    }
}