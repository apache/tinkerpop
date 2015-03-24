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
package org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.ProfileStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ComparatorHolderRemovalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.EngineDependentStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.LabeledEndStepStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Bob Briody (http://bobbriody.com)
 */
public final class ProfileStrategy extends AbstractTraversalStrategy {


    private static final ProfileStrategy INSTANCE = new ProfileStrategy();
    private static final Set<Class<? extends TraversalStrategy>> PRIORS = new HashSet<>();

    static {
        // Ensure that this strategy is applied last.
        PRIORS.add(ComparatorHolderRemovalStrategy.class);
        PRIORS.add(ConjunctionStrategy.class);
        PRIORS.add(DedupOptimizerStrategy.class);
        PRIORS.add(EngineDependentStrategy.class);
        PRIORS.add(IdentityRemovalStrategy.class);
        PRIORS.add(LabeledEndStepStrategy.class);
        PRIORS.add(MatchWhereStrategy.class);
        PRIORS.add(RangeByIsCountStrategy.class);
    }

    private ProfileStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (!(traversal.getParent() instanceof EmptyStep)) {
            // This strategy is applied to the top-level traversal only
            return;
        }

        if (!TraversalHelper.hasStepOfClass(ProfileStep.class, traversal)) {
            // No ProfileStep present
            return;
        }

        prepTraversalForProfiling(traversal);
    }

    // Walk the traversal steps and inject the .profile()-steps.
    private void prepTraversalForProfiling(Traversal.Admin<?, ?> traversal) {
        // Remove user-specified .profile() steps
        final List<ProfileStep> profileSteps = TraversalHelper.getStepsOfClass(ProfileStep.class, traversal);
        for (ProfileStep step : profileSteps) {
            traversal.removeStep(step);
        }

        // Add .profile() step after every pre-existing step.
        final List<Step> steps = traversal.getSteps();
        final int numSteps = steps.size();
        for (int ii = 0; ii < numSteps; ii++) {
            // Get the original step
            Step step = steps.get(ii * 2);

            // Create and inject ProfileStep
            ProfileStep profileStep = new ProfileStep(traversal);
            traversal.addStep((ii * 2) + 1, profileStep);

            // Handle nested traversal
            if (step instanceof TraversalParent) {
                for (Traversal.Admin<?, ?> t : ((TraversalParent) step).getLocalChildren()) {
                    prepTraversalForProfiling(t);
                }
                for (Traversal.Admin<?, ?> t : ((TraversalParent) step).getGlobalChildren()) {
                    prepTraversalForProfiling(t);
                }
            }
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
