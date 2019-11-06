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

import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.VertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.ProfilingAware;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.ProfileSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ProfileStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.List;

/**
 * @author Bob Briody (http://bobbriody.com)
 */
public final class ProfileStrategy extends AbstractTraversalStrategy<TraversalStrategy.FinalizationStrategy> implements TraversalStrategy.FinalizationStrategy {

    private static final ProfileStrategy INSTANCE = new ProfileStrategy();
    private static final String MARKER = Graph.Hidden.hide("gremlin.profile");

    private ProfileStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (!traversal.getEndStep().getLabels().contains(MARKER) &&
                (traversal.isRoot() || traversal.getParent() instanceof VertexProgramStep) &&
                TraversalHelper.hasStepOfAssignableClassRecursively(ProfileSideEffectStep.class, traversal))
            TraversalHelper.applyTraversalRecursively(t -> t.getEndStep().addLabel(MARKER), traversal);

        if (traversal.getEndStep().getLabels().contains(MARKER)) {
            traversal.getEndStep().removeLabel(MARKER);
            // Add .profile() step after every pre-existing step.
            final List<Step> steps = traversal.getSteps();
            final int numSteps = steps.size();
            for (int i = 0; i < numSteps; i++) {
                // Do not inject profiling after ProfileSideEffectStep as this will be the last step on the root traversal.
                if (steps.get(i * 2) instanceof ProfileSideEffectStep)
                    break;
                // Create and inject ProfileStep
                final ProfileStep profileStepToAdd = new ProfileStep(traversal);
                traversal.addStep((i * 2) + 1, profileStepToAdd);

                final Step stepToBeProfiled = traversal.getSteps().get(i * 2);
                if (stepToBeProfiled instanceof ProfilingAware) {
                    ((ProfilingAware) stepToBeProfiled).prepareForProfiling();
                }
            }
        }
    }

    public static ProfileStrategy instance() {
        return INSTANCE;
    }
}
