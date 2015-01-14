package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.graph.step.branch.BranchStep;
import com.tinkerpop.gremlin.process.graph.step.branch.ChooseStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ChooseLinearStrategy extends AbstractTraversalStrategy {

    private static final ChooseLinearStrategy INSTANCE = new ChooseLinearStrategy();

    private ChooseLinearStrategy() {
    }

    // x.choose(t -> M){a}{b}.y
    // x.branch(mapFunction.next().toString()).a.branch(end).as(z).b.as(end).y
    public void apply(final Traversal<?, ?> traversal, final TraversalEngine engine) {
        if (engine.equals(TraversalEngine.STANDARD) || !TraversalHelper.hasStepOfClass(ChooseStep.class, traversal))
            return;

        for (final ChooseStep chooseStep : TraversalHelper.getStepsOfClass(ChooseStep.class, traversal)) {

            final IdentityStep finalStep = new IdentityStep(traversal);
            TraversalHelper.insertAfterStep(finalStep, chooseStep, traversal);

            final Map<Object, String> chooseBranchLabels = new HashMap<>();

            final Iterator<Map.Entry<?, Traversal<?, ?>>> traversalIterator = chooseStep.getChoices().entrySet().iterator();
            BranchStep<?> branchStep = new BranchStep<>(traversal);
            TraversalHelper.replaceStep(chooseStep, branchStep, traversal);
            Step currentStep = branchStep;
            while (traversalIterator.hasNext()) {
                final Map.Entry<?, Traversal<?, ?>> entry = traversalIterator.next();
                chooseBranchLabels.put(entry.getKey(), currentStep.getLabel());
                currentStep = TraversalHelper.insertTraversal(entry.getValue(), currentStep, traversal);
                if (traversalIterator.hasNext()) {
                    final BranchStep chooseBreakBranchStep = new BranchStep(traversal);
                    chooseBreakBranchStep.setFunction(new BranchStep.GoToLabels(Collections.singleton(finalStep.getLabel())));
                    TraversalHelper.insertAfterStep(chooseBreakBranchStep, currentStep, traversal);
                    currentStep = chooseBreakBranchStep;
                }
            }

            branchStep.setFunction(traverser -> {
                final String goTo = chooseBranchLabels.get(chooseStep.getMapFunction().apply(traverser.get()));
                return null != goTo && TraversalHelper.hasLabel(goTo, traversal) ? Collections.singletonList(goTo) : Collections.emptyList();
            });

        }
    }

    /*private static final String objectToString(final int chooseStepCounter, final Object object) {
        return CHOOSE_PREFIX + chooseStepCounter + "." + object.toString() + ":" + object.getClass().getCanonicalName();
    }*/

    public static ChooseLinearStrategy instance() {
        return INSTANCE;
    }
}
