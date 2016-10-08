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
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

/**
 * {@code IdentityRemovalStrategy} looks for {@link IdentityStep} instances and removes them.
 * If the identity step is labeled, its labels are added to the previous step.
 * If the identity step is labeled and its the first step in the traversal, it stays.
 * <p/>
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @example <pre>
 * __.out().identity().count()            // is replaced by __.out().count()
 * __.in().identity().as("a")             // is replaced by __.in().as("a")
 * __.identity().as("a").out()            // is replaced by __.identity().as("a").out()
 * </pre>
 */
public final class IdentityRemovalStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {

    private static final IdentityRemovalStrategy INSTANCE = new IdentityRemovalStrategy();

    private IdentityRemovalStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (traversal.getSteps().size() <= 1 || !TraversalHelper.hasStepOfClass(IdentityStep.class, traversal))
            return;

        TraversalHelper.getStepsOfClass(IdentityStep.class, traversal).stream().forEach(identityStep -> {
            final Step<?, ?> previousStep = identityStep.getPreviousStep();
            if (!(previousStep instanceof EmptyStep) || identityStep.getLabels().isEmpty()) {
                ((IdentityStep<?>) identityStep).getLabels().forEach(previousStep::addLabel);
                traversal.removeStep(identityStep);
            }
        });
    }

    public static IdentityRemovalStrategy instance() {
        return INSTANCE;
    }
}
