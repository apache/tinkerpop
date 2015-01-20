package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.step.filter.WhereStep;
import com.tinkerpop.gremlin.process.graph.step.map.SelectStep;
import com.tinkerpop.gremlin.process.graph.step.map.match.MatchStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MatchWhereStrategy extends AbstractTraversalStrategy implements TraversalStrategy {

    private static final MatchWhereStrategy INSTANCE = new MatchWhereStrategy();
    private static final Set<Class<? extends TraversalStrategy>> PRIORS = new HashSet<>();

    static {
        PRIORS.add(IdentityRemovalStrategy.class);
    }

    private MatchWhereStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal, TraversalEngine engine) {
        if (!TraversalHelper.hasStepOfClass(MatchStep.class, traversal))
            return;

        final List<MatchStep> matchSteps = TraversalHelper.getStepsOfClass(MatchStep.class, traversal);
        for (final MatchStep matchStep : matchSteps) {
            boolean foundWhereWithNoTraversal = false;
            Step currentStep = matchStep.getNextStep();
            while (currentStep instanceof WhereStep || currentStep instanceof SelectStep || currentStep instanceof IdentityStep) {
                if (currentStep instanceof WhereStep) {
                    if (!((WhereStep) currentStep).hasBiPredicate()) {
                        matchStep.addTraversal(((WhereStep<?>) currentStep).getTraversals().get(0));
                        traversal.removeStep(currentStep);
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
    public Set<Class<? extends TraversalStrategy>> applyPrior() {
        return PRIORS;
    }

}
