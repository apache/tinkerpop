/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileMarkerStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.ProfileSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.List;

/**
 * @author Bob Briody (http://bobbriody.com)
 */
public final class ProfileStrategy extends AbstractTraversalStrategy<TraversalStrategy.FinalizationStrategy> implements TraversalStrategy.FinalizationStrategy {

    private static final ProfileStrategy INSTANCE = new ProfileStrategy();

    private ProfileStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (TraversalHelper.hasStepOfClass(ProfileSideEffectStep.class, traversal)) {
            prepTraversalForProfiling(traversal, TraversalHelper.getFirstStepOfAssignableClass(ProfileSideEffectStep.class, traversal).get().getSideEffectKey());
        } else if (TraversalHelper.hasStepOfClass(ProfileMarkerStep.class, traversal)) {
            prepTraversalForProfiling(traversal, ProfileMarkerStep.METRICS_KEY);
        }
    }

    // Walk the traversal steps and inject the .profile()-steps.
    private void prepTraversalForProfiling(Traversal.Admin<?, ?> traversal, String sideEffectKey) {
        // Remove user-specified ProfileSideEffectStep steps
        final List<ProfileSideEffectStep> profileSideEffectSteps = TraversalHelper.getStepsOfClass(ProfileSideEffectStep.class, traversal);
        for (ProfileSideEffectStep step : profileSideEffectSteps) {
            traversal.removeStep(step);
        }

        // Add .profile() step after every pre-existing step.
        final List<Step> steps = traversal.getSteps();
        final int numSteps = steps.size();
        for (int ii = 0; ii < numSteps; ii++) {
            // Get the original step
            Step step = steps.get(ii * 2);

            // Do not inject profiling after ProfileStep
            if (step instanceof ProfileMarkerStep) {
                break;
            }

            // Create and inject ProfileStep
            ProfileSideEffectStep profileSideEffectStep = new ProfileSideEffectStep(traversal, sideEffectKey);
            traversal.addStep((ii * 2) + 1, profileSideEffectStep);

            // Handle nested traversal
            if (step instanceof TraversalParent) {
                for (Traversal.Admin<?, ?> t : ((TraversalParent) step).getLocalChildren()) {
                    t.addStep(new ProfileSideEffectStep(t, sideEffectKey));
                }
                for (Traversal.Admin<?, ?> t : ((TraversalParent) step).getGlobalChildren()) {
                    t.addStep(new ProfileSideEffectStep(t, sideEffectKey));
                }
            }
        }
    }

    public static ProfileStrategy instance() {
        return INSTANCE;
    }
}
