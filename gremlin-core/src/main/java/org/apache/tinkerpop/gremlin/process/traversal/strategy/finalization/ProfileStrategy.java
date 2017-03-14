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
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.ProfileSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ProfileStep;
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
    public boolean isApplicable(final Traversal.Admin<?, ?> rootTraversal) {
        return TraversalHelper.hasStepOfAssignableClassRecursively(ProfileSideEffectStep.class, rootTraversal);
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        // Add .profile() step after every pre-existing step.
        final List<Step> steps = traversal.getSteps();
        final int numSteps = steps.size();
        for (int ii = 0; ii < numSteps; ii++) {
            // Get the original step
            final Step step = steps.get(ii * 2);

            // Do not inject profiling after ProfileSideEffectStep as this will be the last step on the root traversal.
            if (step instanceof ProfileSideEffectStep) {
                break;
            }

            // Create and inject ProfileStep
            ProfileStep profileStep = new ProfileStep(traversal);
            traversal.addStep((ii * 2) + 1, profileStep);
        }
    }

    public static ProfileStrategy instance() {
        return INSTANCE;
    }
}
