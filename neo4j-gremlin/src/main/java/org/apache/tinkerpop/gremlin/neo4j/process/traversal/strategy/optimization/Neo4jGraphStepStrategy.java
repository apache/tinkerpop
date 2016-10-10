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
package org.apache.tinkerpop.gremlin.neo4j.process.traversal.strategy.optimization;

import org.apache.tinkerpop.gremlin.neo4j.process.traversal.step.sideEffect.Neo4jGraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

/**
 * @author Pieter Martin
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class Neo4jGraphStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {

    private static final Neo4jGraphStepStrategy INSTANCE = new Neo4jGraphStepStrategy();

    private Neo4jGraphStepStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        for (final GraphStep originalGraphStep : TraversalHelper.getStepsOfClass(GraphStep.class, traversal)) {
            final Neo4jGraphStep<?, ?> neo4jGraphStep = new Neo4jGraphStep<>(originalGraphStep);
            TraversalHelper.replaceStep(originalGraphStep, neo4jGraphStep, traversal);
            Step<?, ?> currentStep = neo4jGraphStep.getNextStep();
            while (currentStep instanceof HasStep) {
                for (final HasContainer hasContainer : ((HasContainerHolder) currentStep).getHasContainers()) {
                    if (!GraphStep.processHasContainerIds(neo4jGraphStep, hasContainer))
                        neo4jGraphStep.addHasContainer(hasContainer);
                }
                TraversalHelper.copyLabels(currentStep, neo4jGraphStep, false);
                traversal.removeStep(currentStep);
                currentStep = currentStep.getNextStep();
            }
        }
    }

    public static Neo4jGraphStepStrategy instance() {
        return INSTANCE;
    }

}