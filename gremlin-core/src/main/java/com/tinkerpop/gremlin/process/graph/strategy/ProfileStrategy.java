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
        PRIORS.add(ChooseLinearStrategy.class);
        PRIORS.add(DedupOptimizerStrategy.class);
        PRIORS.add(EngineDependentStrategy.class);
        PRIORS.add(IdentityRemovalStrategy.class);
        PRIORS.add(LabeledEndStepStrategy.class);
        PRIORS.add(MatchWhereStrategy.class);
        PRIORS.add(ReducingStrategy.class);
        PRIORS.add(SideEffectCapStrategy.class);
        PRIORS.add(UnionLinearStrategy.class);
        PRIORS.add(UnrollJumpStrategy.class);
    }

    private ProfileStrategy() {
    }

    @Override
    public void apply(final Traversal<?, ?> traversal, final TraversalEngine engine) {
        if (!TraversalHelper.hasStepOfClass(ProfileStep.class, traversal))
            return;

        // Remove user-specified .profile() steps
        List<ProfileStep> profileSteps = TraversalHelper.getStepsOfClass(ProfileStep.class, traversal);
        for (ProfileStep step : profileSteps) {
            TraversalHelper.removeStep(step, traversal);
        }

        // Add .profile() step after every pre-existing step.
        final List<Step> steps = traversal.asAdmin().getSteps();
        for (int ii = 0; ii < steps.size(); ii++) {
            TraversalHelper.insertStep(new ProfileStep(traversal, steps.get(ii)), ii + 1, traversal);
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
