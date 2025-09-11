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
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueHolder;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.provider.ProviderGValueReductionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Converts placeholder steps that hold {@link GValue} objects to their concrete implementations. While not an
 * optimization in and of itself, the {@link GValue} functionality in general, provides a mechanism for traversal
 * optimization so falls in this category. In addition, converting to concrete steps at this stage also allows provider
 * optimization strategies to execute on concrete steps, rather than step interfaces. Concrete steps are much easier to
 * reason about and keep strategy application simple for the vast majority of providers.
 * <p/>
 * Providers hoping to do more advanced optimizations that require {@link GValue} objects to be present for their
 * strategies will need to remove {@link GValueReductionStrategy} and offer their own mechanism for converting step
 * placeholders to concrete steps. {@link ProviderGValueReductionStrategy} is a base class for helping with this need.
 */
public final class GValueReductionStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {

    private static final GValueReductionStrategy INSTANCE = new GValueReductionStrategy();

    private GValueReductionStrategy() {}

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        final List<Step> steps = traversal.getSteps();
        for (int i = 0; i < steps.size(); i++) {
            if (steps.get(i) instanceof GValueHolder) {
                ((GValueHolder) steps.get(i)).reduce();
            }
        }
    }

    public static GValueReductionStrategy instance() {
        return INSTANCE;
    }

    /**
     * No TinkerPop optimization strategy should be applied following this one. The default implementation adds
     * {@link GValueReductionStrategy} itself so retaining that implementation would create a circular reference so
     * returning an empty list here removes that dependency.
     */
    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPost() {
        return Collections.emptySet();
    }
}
