package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.graph.step.branch.BranchStep;
import com.tinkerpop.gremlin.process.graph.step.branch.ChooseStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ChooseLinearStrategy extends AbstractTraversalStrategy {

    // TODO: recursively linearlize as a branch could have a choose() step (so forth and so on)

    private static final ChooseLinearStrategy INSTANCE = new ChooseLinearStrategy();

    private static final String CHOOSE_PREFIX = "gremlin.choose.";
    private static final String CHOOSE_PREFIX_END = "gremlin.choose.end.";

    private ChooseLinearStrategy() {
    }

    // x.choose(t -> M){a}{b}.y
    // x.branch(mapFunction.next().toString()).a.branch(end).as(z).b.as(end).y
    public void apply(final Traversal<?, ?> traversal, final TraversalEngine engine) {
        if (engine.equals(TraversalEngine.STANDARD) || !TraversalHelper.hasStepOfClass(ChooseStep.class, traversal))
            return;

        int chooseStepCounter = 0;
        for (final ChooseStep chooseStep : TraversalHelper.getStepsOfClass(ChooseStep.class, traversal)) {
            final int currentStepCounter = chooseStepCounter;
            final String endLabel = CHOOSE_PREFIX_END + chooseStepCounter;
            BranchStep<?> branchStep = new BranchStep<>(traversal);
            branchStep.setFunction(traverser -> {
                final String goTo = objectToString(currentStepCounter, chooseStep.getMapFunction().apply(traverser.get()));
                return TraversalHelper.hasLabel(goTo, traversal) ? Collections.singletonList(goTo) : Collections.emptyList();
            });
            TraversalHelper.replaceStep(chooseStep, branchStep, traversal);

            Step currentStep = branchStep;
            final Iterator<Map.Entry<?, Traversal<?, ?>>> traversalIterator = chooseStep.getChoices().entrySet().iterator();
            while (traversalIterator.hasNext()) {
                final Map.Entry<?, Traversal<?, ?>> entry = traversalIterator.next();
                currentStep.setLabel(objectToString(currentStepCounter, entry.getKey()));
                currentStep = TraversalHelper.insertTraversal(entry.getValue(), currentStep, traversal);
                if (traversalIterator.hasNext()) {
                    branchStep = new BranchStep(traversal);
                    branchStep.setFunction(new BranchStep.GoToLabels(Collections.singletonList(endLabel)));
                    TraversalHelper.insertAfterStep(branchStep, currentStep, traversal);
                    currentStep = branchStep;
                } else {
                    final IdentityStep finalStep = new IdentityStep(traversal);
                    finalStep.setLabel(endLabel);
                    TraversalHelper.insertAfterStep(finalStep, currentStep, traversal);
                    break;
                }
            }

            chooseStepCounter++;
        }
    }

    private static final String objectToString(final int chooseStepCounter, final Object object) {
        return CHOOSE_PREFIX + chooseStepCounter + "." + object.toString() + ":" + object.getClass().getCanonicalName();
    }

    public static ChooseLinearStrategy instance() {
        return INSTANCE;
    }
}
