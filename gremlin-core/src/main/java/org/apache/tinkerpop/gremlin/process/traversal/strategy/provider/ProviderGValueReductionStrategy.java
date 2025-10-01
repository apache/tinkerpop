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
package org.apache.tinkerpop.gremlin.process.traversal.strategy.provider;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueHolder;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;

import java.util.List;

/**
 * Converts placeholder steps that hold {@link GValue} objects to their concrete implementations. While not an
 * optimization in and of itself, the {@link GValue} functionality in general, provides a mechanism for traversal
 * optimization so falls in this category. In addition, converting to concrete steps at this stage also allows provider
 * optimization strategies to execute on concrete steps, rather than step interfaces. Concrete steps are much easier to
 * reason about and keep strategy application simple for the vast majority of providers.
 * <p/>
 * Providers hoping to do more advanced optimizations that require {@link GValue} objects to be present for their
 * strategies will need to remove {@link ProviderGValueReductionStrategy} and offer their own mechanism for converting step
 * placeholders to concrete steps.
 */
public class ProviderGValueReductionStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {

    private static final ProviderGValueReductionStrategy INSTANCE = new ProviderGValueReductionStrategy();

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        final List<Step> steps = traversal.getSteps();
        for (int i = 0; i < steps.size(); i++) {
            if (steps.get(i) instanceof GValueHolder) {
                ((GValueHolder) steps.get(i)).reduce();
            }
        }
    }

    public static ProviderGValueReductionStrategy instance() {
        return INSTANCE;
    }
}
