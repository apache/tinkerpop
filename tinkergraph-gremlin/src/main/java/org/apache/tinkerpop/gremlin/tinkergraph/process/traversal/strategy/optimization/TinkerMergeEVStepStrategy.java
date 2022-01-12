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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MergeEdgeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MergeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.step.map.TinkerMergeEdgeStep;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.step.map.TinkerMergeVertexStep;

/**
 * Optimizes {@code mergeV()} and {@code mergeE()} search lookups by using {@link TinkerMergeVertexStep} and
 * {@link TinkerMergeEdgeStep} respectively.
 */
public final class TinkerMergeEVStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
        implements TraversalStrategy.ProviderOptimizationStrategy {

    private static final TinkerMergeEVStepStrategy INSTANCE = new TinkerMergeEVStepStrategy();

    private TinkerMergeEVStepStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (TraversalHelper.onGraphComputer(traversal))
            return;

        for (final MergeVertexStep originalMergeVertexStep : TraversalHelper.getStepsOfClass(MergeVertexStep.class, traversal)) {
            final TinkerMergeVertexStep tinkerMergeVertexStep = new TinkerMergeVertexStep(originalMergeVertexStep);
            TraversalHelper.replaceStep(originalMergeVertexStep, tinkerMergeVertexStep, traversal);
        }

        for (final MergeEdgeStep originalMergeEdgeStep : TraversalHelper.getStepsOfClass(MergeEdgeStep.class, traversal)) {
            final TinkerMergeEdgeStep tinkerMergeEdgeStep = new TinkerMergeEdgeStep(originalMergeEdgeStep);
            TraversalHelper.replaceStep(originalMergeEdgeStep, tinkerMergeEdgeStep, traversal);
        }
    }

    public static TinkerMergeEVStepStrategy instance() {
        return INSTANCE;
    }
}
