package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.ProfileStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.List;

/**
 * @author Bob Briody (http://bobbriody.com)
 */
public class ProfileStrategy extends AbstractTraversalStrategy {

    private static final ProfileStrategy INSTANCE = new ProfileStrategy();

    private ProfileStrategy() {
    }

    @Override
    public void apply(final Traversal<?, ?> traversal, final TraversalEngine engine) {
        if (!TraversalHelper.hasStepOfClass(ProfileStep.class, traversal))
            return;

        // Determine if this is a Standard or Computer traversal
        boolean timingEnabled = true;
        if (TraversalEngine.COMPUTER.equals(engine)) {
            timingEnabled = false;
        }

        // Remove user-specified .profile() steps
        List<ProfileStep> profileSteps = TraversalHelper.getStepsOfClass(ProfileStep.class, traversal);
        for (ProfileStep step : profileSteps) {
            TraversalHelper.removeStep(step, traversal);
        }

        // Add .profile() step after every pre-existing step.
        final List<Step> steps = traversal.asAdmin().getSteps();
        for (int ii = 0; ii < steps.size(); ii++) {
            TraversalHelper.insertStep(new ProfileStep(traversal, steps.get(ii), timingEnabled), ii + 1, traversal);
            ii++;
        }
    }

    public static ProfileStrategy instance() {
        return INSTANCE;
    }
}
