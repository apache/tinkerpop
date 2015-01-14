package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.step.branch.BranchStep;
import com.tinkerpop.gremlin.process.graph.step.branch.UnionStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class UnionLinearStrategy extends AbstractTraversalStrategy {

    private static final UnionLinearStrategy INSTANCE = new UnionLinearStrategy();

    private UnionLinearStrategy() {
    }

    // x.union(a,b).y
    // x.branch(t->a,t->b).a.branch(end).as(z).b.as(end).y
    public void apply(final Traversal.Admin<?, ?> traversal, final TraversalEngine engine) {
        if (engine.equals(TraversalEngine.STANDARD) || !TraversalHelper.hasStepOfClass(UnionStep.class, traversal))
            return;

        for (final UnionStep<?, ?> unionStep : TraversalHelper.getStepsOfClass(UnionStep.class, traversal)) {
            final IdentityStep finalStep = new IdentityStep(traversal);
            TraversalHelper.insertAfterStep(finalStep, unionStep, traversal);

            final Set<String> branchLabels = new HashSet<>();
            BranchStep branchStep = new BranchStep<>(traversal);
            branchStep.setFunction(new BranchStep.GoToLabels<>(branchLabels));
            TraversalHelper.replaceStep(unionStep, branchStep, traversal);

            Step currentStep = branchStep;
            final Iterator<Traversal> unionTraversals = (Iterator) unionStep.getTraversals().iterator();
            while (unionTraversals.hasNext()) {
                final Traversal unionTraversal = unionTraversals.next();
                branchLabels.add(currentStep.getLabel());
                currentStep = TraversalHelper.insertTraversal(currentStep, unionTraversal, traversal);
                if (unionTraversals.hasNext()) {
                    branchStep = new BranchStep(traversal);
                    branchStep.setFunction(new BranchStep.GoToLabels(Collections.singletonList(finalStep.getLabel())));
                    TraversalHelper.insertAfterStep(branchStep, currentStep, traversal);
                    currentStep = branchStep;
                }
            }
        }
    }

    @Override
    public Set<Class<? extends TraversalStrategy>> applyPost() {
        return Collections.singleton(EngineDependentStrategy.class);
    }

    public static UnionLinearStrategy instance() {
        return INSTANCE;
    }
}