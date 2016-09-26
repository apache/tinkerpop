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
package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.strategy.optimization;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.step.sideEffect.TinkerGraphStep;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TinkerGraphStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {

    private static final TinkerGraphStepStrategy INSTANCE = new TinkerGraphStepStrategy();

    private TinkerGraphStepStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (TraversalHelper.onGraphComputer(traversal))
            return;

        TraversalHelper.getStepsOfClass(GraphStep.class, traversal).forEach(originalGraphStep -> {
            final TinkerGraphStep<?, ?> tinkerGraphStep = new TinkerGraphStep<>(originalGraphStep);
            TraversalHelper.replaceStep(originalGraphStep, (Step) tinkerGraphStep, traversal);
            Step<?, ?> lastNonHolderStep = tinkerGraphStep;
            Step<?, ?> currentStep = tinkerGraphStep.getNextStep();
            while (currentStep instanceof HasContainerHolder || currentStep instanceof FilterStep) {
                if (currentStep instanceof HasContainerHolder) {
                    for (final HasContainer hasContainer : ((HasContainerHolder) currentStep).getHasContainers()) {
                        if (!GraphStep.processHasContainerIds(tinkerGraphStep, hasContainer))
                            tinkerGraphStep.addHasContainer(hasContainer);
                    }
                    TraversalHelper.copyLabels(currentStep, lastNonHolderStep, false);
                    traversal.removeStep(currentStep);
                } else
                    lastNonHolderStep = currentStep;
                currentStep = currentStep.getNextStep();
            }
        });
    }

    public static TinkerGraphStepStrategy instance() {
        return INSTANCE;
    }
}
