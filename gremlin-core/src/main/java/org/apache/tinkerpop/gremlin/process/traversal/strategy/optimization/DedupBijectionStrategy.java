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
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * {@code DedupBijectionStrategy} does deduplication prior to a non-mutating bijective step if there is no
 * {@code by()}-modulation on {@code dedup()}. Given that {@link DedupGlobalStep} reduces the number of
 * objects in the stream, it is cheaper to dedup prior.
 * <p/>
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Daniel Kuppitz (http://gremlin.guru)
 * @example <pre>
 * __.order().dedup()            // is replaced by __.dedup().order()
 * </pre>
 */
public final class DedupBijectionStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {

    private static final DedupBijectionStrategy INSTANCE = new DedupBijectionStrategy();
    private static final Set<Class<? extends OptimizationStrategy>> PRIORS = new HashSet<>();

    static {
        PRIORS.add(IdentityRemovalStrategy.class);
    }


    private DedupBijectionStrategy() {
    }


    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (!TraversalHelper.hasStepOfClass(DedupGlobalStep.class, traversal))
            return;

        final List<Step> steps = traversal.getSteps();
        boolean exchangeable = false;
        for (int i = steps.size() - 1; i >= 0; i--) {
            final Step curr = steps.get(i);
            if (exchangeable) {
                if (curr instanceof OrderGlobalStep) {
                    final Step next = curr.getNextStep();
                    traversal.removeStep(next);
                    traversal.addStep(i, next);
                }
                exchangeable = curr instanceof IdentityStep;
            } else {
                exchangeable = curr instanceof DedupGlobalStep && ((DedupGlobalStep) curr).getLocalChildren().isEmpty();
            }
        }
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPrior() {
        return PRIORS;
    }

    public static DedupBijectionStrategy instance() {
        return INSTANCE;
    }
}
