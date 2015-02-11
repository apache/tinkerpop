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
package com.apache.tinkerpop.gremlin.process.graph.traversal.strategy;

import com.apache.tinkerpop.gremlin.process.Step;
import com.apache.tinkerpop.gremlin.process.Traversal;
import com.apache.tinkerpop.gremlin.process.TraversalEngine;
import com.apache.tinkerpop.gremlin.process.graph.traversal.step.filter.DedupStep;
import com.apache.tinkerpop.gremlin.process.graph.traversal.step.map.OrderGlobalStep;
import com.apache.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.IdentityStep;
import com.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import com.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.Arrays;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class DedupOptimizerStrategy extends AbstractTraversalStrategy {

    private static final DedupOptimizerStrategy INSTANCE = new DedupOptimizerStrategy();

    private DedupOptimizerStrategy() {
    }

    private static final List<Class<? extends Step>> BIJECTIVE_PIPES = Arrays.asList(IdentityStep.class, OrderGlobalStep.class);

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal, final TraversalEngine engine) {
        if (!TraversalHelper.hasStepOfClass(DedupStep.class, traversal))
            return;

        boolean done = false;
        while (!done) {
            done = true;
            for (int i = 0; i < traversal.getSteps().size(); i++) {
                final Step step1 = traversal.getSteps().get(i);
                if (step1 instanceof DedupStep && !(((DedupStep) step1).getLocalChildren().get(0) instanceof IdentityTraversal)) {
                    for (int j = i; j >= 0; j--) {
                        final Step step2 = traversal.getSteps().get(j);
                        if (BIJECTIVE_PIPES.stream().filter(c -> c.isAssignableFrom(step2.getClass())).findAny().isPresent()) {
                            traversal.removeStep(step1);
                            traversal.addStep(j, step1);
                            done = false;
                            break;
                        }
                    }
                }
                if (!done)
                    break;
            }
        }
    }

    public static DedupOptimizerStrategy instance() {
        return INSTANCE;
    }
}
