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
package com.apache.tinkerpop.gremlin.neo4j.process.graph.traversal.strategy;

import com.apache.tinkerpop.gremlin.neo4j.process.graph.traversal.step.sideEffect.Neo4jGraphStep;
import com.apache.tinkerpop.gremlin.process.Step;
import com.apache.tinkerpop.gremlin.process.Traversal;
import com.apache.tinkerpop.gremlin.process.TraversalEngine;
import com.apache.tinkerpop.gremlin.process.graph.traversal.step.HasContainerHolder;
import com.apache.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.IdentityStep;
import com.apache.tinkerpop.gremlin.process.graph.traversal.strategy.AbstractTraversalStrategy;
import com.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

/**
 * @author Pieter Martin
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Neo4jGraphStepStrategy extends AbstractTraversalStrategy {

    private static final Neo4jGraphStepStrategy INSTANCE = new Neo4jGraphStepStrategy();

    private Neo4jGraphStepStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal, final TraversalEngine traversalEngine) {
        if (traversal.getStartStep() instanceof Neo4jGraphStep) {
            final Neo4jGraphStep neo4jGraphStep = (Neo4jGraphStep) traversal.getStartStep();
            Step<?, ?> currentStep = neo4jGraphStep.getNextStep();
            while (true) {
                if (currentStep instanceof HasContainerHolder) {
                    neo4jGraphStep.hasContainers.addAll(((HasContainerHolder) currentStep).getHasContainers());
                    if (currentStep.getLabel().isPresent()) {
                        final IdentityStep identityStep = new IdentityStep<>(traversal);
                        identityStep.setLabel(currentStep.getLabel().get());
                        TraversalHelper.insertAfterStep(identityStep, currentStep, traversal);
                    }
                    traversal.removeStep(currentStep);
                } else if (currentStep instanceof IdentityStep) {
                    // do nothing
                } else {
                    break;
                }
                currentStep = currentStep.getNextStep();
            }
        }
    }

    public static Neo4jGraphStepStrategy instance() {
        return INSTANCE;
    }

}
