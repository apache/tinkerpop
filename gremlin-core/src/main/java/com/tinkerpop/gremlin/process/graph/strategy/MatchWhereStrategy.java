package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.step.filter.DedupStep;
import com.tinkerpop.gremlin.process.graph.step.filter.WhereStep;
import com.tinkerpop.gremlin.process.graph.step.map.SelectStep;
import com.tinkerpop.gremlin.process.graph.step.map.match.MatchStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MatchWhereStrategy extends AbstractTraversalStrategy implements TraversalStrategy {

    private static final MatchWhereStrategy INSTANCE = new MatchWhereStrategy();

    private MatchWhereStrategy() {
    }

    @Override
    public void apply(final Traversal<?, ?> traversal) {
        if (!TraversalHelper.hasStepOfClass(MatchStep.class, traversal))
            return;

        final List<MatchStep> matchSteps = TraversalHelper.getStepsOfClass(MatchStep.class, traversal);
        for (final MatchStep matchStep : matchSteps) {
            boolean foundWhereWithNoTraversal = false;
            Step currentStep = matchStep.getNextStep();
            while (currentStep instanceof WhereStep || currentStep instanceof SelectStep || currentStep instanceof IdentityStep) {
                if (currentStep instanceof WhereStep) {
                    if (!((WhereStep) currentStep).hasBiPredicate()) {
                        matchStep.addTraversal(((WhereStep) currentStep).getConstraint());
                        TraversalHelper.removeStep(currentStep, traversal);
                    } else {
                        foundWhereWithNoTraversal = true;
                    }
                } else if (currentStep instanceof SelectStep) {
                    if (((SelectStep) currentStep).hasStepFunctions() || foundWhereWithNoTraversal)
                        break;
                }  // else is the identity step
                currentStep = currentStep.getNextStep();
            }
        }
    }

    public static MatchWhereStrategy instance() {
        return INSTANCE;
    }

    @Override
    public int compareTo(final TraversalStrategy traversalStrategy) {
        return traversalStrategy instanceof IdentityReductionStrategy ? 1 : 0;
    }
}
