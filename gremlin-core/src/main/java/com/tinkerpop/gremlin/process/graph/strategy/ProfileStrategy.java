package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.ProfileStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Bob Briody (http://bobbriody.com)
 */
public class ProfileStrategy extends AbstractTraversalStrategy {

    private static final ProfileStrategy INSTANCE = new ProfileStrategy();
    private static final Set<Class<? extends TraversalStrategy>> PRIORS = new HashSet<>();

    static {
        // Ensure that this strategy is applied last.
        PRIORS.add(ComparatorHolderRemovalStrategy.class);
        PRIORS.add(DedupOptimizerStrategy.class);
        PRIORS.add(EngineDependentStrategy.class);
        PRIORS.add(IdentityRemovalStrategy.class);
        PRIORS.add(LabeledEndStepStrategy.class);
        PRIORS.add(MatchWhereStrategy.class);
        PRIORS.add(ReducingStrategy.class);
        PRIORS.add(RouteStrategy.class);
        PRIORS.add(SideEffectCapStrategy.class);
        PRIORS.add(SideEffectRegistrationStrategy.class);
    }

    private ProfileStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal, final TraversalEngine traversalEngine) {
        if (!TraversalHelper.hasStepOfClass(ProfileStep.class, traversal))
            return;

        // Remove user-specified .profile() steps
        final List<ProfileStep> profileSteps = TraversalHelper.getStepsOfClass(ProfileStep.class, traversal);
        for (ProfileStep step : profileSteps) {
            traversal.removeStep(step);
        }

        // Add .profile() step after every pre-existing step.
        final List<Step> steps = traversal.getSteps();
        for (int ii = 0; ii < steps.size(); ii++) {
            traversal.addStep(ii + 1, new ProfileStep(traversal, steps.get(ii)));
            ii++;
        }
    }

    @Override
    public Set<Class<? extends TraversalStrategy>> applyPrior() {
        return PRIORS;
    }

    public static ProfileStrategy instance() {
        return INSTANCE;
    }
}
