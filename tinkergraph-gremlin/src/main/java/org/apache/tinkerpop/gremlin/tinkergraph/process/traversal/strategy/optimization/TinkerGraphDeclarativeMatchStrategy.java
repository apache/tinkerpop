/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.tinkerpop.gremlin.process.traversal.step.map.DeclarativeMatchStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.tinkergraph.process.gql.TinkerGraphGqlExecutor;
import org.apache.tinkerpop.gremlin.tinkergraph.process.gql.TinkerGraphGqlPlanner;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.step.map.TinkerGraphMatchStep;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerTransactionGraph;

/**
 * Replaces every {@link DeclarativeMatchStep} in a traversal with a
 * {@link TinkerGraphMatchStep}, which is the TinkerGraph-specific executable
 * implementation backed by the GQL pattern-matching engine.
 *
 * <p>This strategy is registered as a default provider-optimization strategy for
 * {@link org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph} and
 * {@link org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerTransactionGraph}.
 * It follows the same pattern as {@link TinkerGraphStepStrategy}.</p>
 *
 * @since 4.0.0
 */
public final class TinkerGraphDeclarativeMatchStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
        implements TraversalStrategy.ProviderOptimizationStrategy {

    private static final TinkerGraphDeclarativeMatchStrategy INSTANCE = new TinkerGraphDeclarativeMatchStrategy();

    private TinkerGraphDeclarativeMatchStrategy() {
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (TraversalHelper.onGraphComputer(traversal))
            return;

        // Resolve the graph-level singleton planner and executor so the plan cache is
        // shared across all traversals on this graph instance.
        TinkerGraphGqlPlanner planner = null;
        TinkerGraphGqlExecutor executor = null;
        if (traversal.getGraph().isPresent()) {
            final Object graph = traversal.getGraph().get();
            if (graph instanceof TinkerGraph) {
                planner = ((TinkerGraph) graph).getGqlPlanner();
                executor = ((TinkerGraph) graph).getGqlExecutor();
            } else if (graph instanceof TinkerTransactionGraph) {
                planner = ((TinkerTransactionGraph) graph).getGqlPlanner();
                executor = ((TinkerTransactionGraph) graph).getGqlExecutor();
            }
        }

        for (final DeclarativeMatchStep originalStep : TraversalHelper.getStepsOfClass(DeclarativeMatchStep.class, traversal)) {
            final TinkerGraphMatchStep tinkerGraphMatchStep = new TinkerGraphMatchStep(originalStep, planner, executor);
            TraversalHelper.replaceStep(originalStep, tinkerGraphMatchStep, traversal);
        }
    }

    public static TinkerGraphDeclarativeMatchStrategy instance() {
        return INSTANCE;
    }
}
