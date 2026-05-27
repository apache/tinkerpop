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
package org.apache.tinkerpop.gremlin.gql;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.DeclarativeMatchStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;

/**
 * {@link TraversalStrategy.ProviderOptimizationStrategy} that replaces every
 * {@link DeclarativeMatchStep} in a traversal with a concrete {@link GqlMatchStep} backed
 * by a {@link DefaultGqlPlanner} and {@link DefaultGqlExecutor}.
 *
 * <p>Providers that want the reference GQL engine register this strategy directly:</p>
 * <pre>
 *   graph.traversal().withStrategies(GqlDeclarativeMatchStrategy.create(graph))
 * </pre>
 *
 * <p>The static factory constructs a {@link DefaultGqlPlanner} and {@link DefaultGqlExecutor}
 * over the supplied graph. Both delegate to the graph's {@code countVerticesByLabel()},
 * {@code countEdgesByLabel()}, and {@code index()} methods, which return conservative defaults
 * unless the graph overrides them. The planner and executor are created once and shared across
 * all traversals using this strategy instance, so the planner's parse cache is reused.</p>
 */
public final class GqlDeclarativeMatchStrategy
        extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
        implements TraversalStrategy.ProviderOptimizationStrategy {

    private final GqlPlanner planner;
    private final GqlExecutor executor;

    private GqlDeclarativeMatchStrategy(final GqlPlanner planner, final GqlExecutor executor) {
        this.planner = planner;
        this.executor = executor;
    }

    /**
     * Creates a strategy backed by {@link DefaultGqlPlanner} and {@link DefaultGqlExecutor}
     * over the supplied graph.
     *
     * @param graph the graph to plan and execute against
     * @return a new strategy instance
     */
    public static GqlDeclarativeMatchStrategy create(final Graph graph) {
        return new GqlDeclarativeMatchStrategy(new DefaultGqlPlanner(graph), new DefaultGqlExecutor(graph));
    }

    /**
     * Creates a strategy with explicit planner and executor instances. Useful when the caller
     * wants to supply custom implementations.
     *
     * @param planner  the planner to use
     * @param executor the executor to use
     * @return a new strategy instance
     */
    public static GqlDeclarativeMatchStrategy create(final GqlPlanner planner, final GqlExecutor executor) {
        return new GqlDeclarativeMatchStrategy(planner, executor);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (TraversalHelper.onGraphComputer(traversal)) return;
        for (final DeclarativeMatchStep step :
                TraversalHelper.getStepsOfClass(DeclarativeMatchStep.class, traversal)) {
            TraversalHelper.replaceStep(step, new GqlMatchStep(step, planner, executor), traversal);
        }
    }
}
