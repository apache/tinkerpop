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

import java.util.concurrent.ConcurrentHashMap;

/**
 * {@link TraversalStrategy.ProviderOptimizationStrategy} that replaces every
 * {@link DeclarativeMatchStep} in a traversal with a concrete {@link GqlMatchStep} backed
 * by a {@link DefaultGqlPlanner} and {@link DefaultGqlExecutor}.
 *
 * <h3>Singleton usage (recommended)</h3>
 * <p>The no-arg {@link #instance()} singleton maintains a static
 * {@link ConcurrentHashMap} keyed by graph identity. The first traversal executed against
 * a given {@link Graph} instance allocates one {@link DefaultGqlPlanner} and one
 * {@link DefaultGqlExecutor} for that graph; all subsequent traversals reuse them, so the
 * planner's parse cache is shared across all traversals on the same graph.</p>
 *
 * <p>Graph providers should register the singleton in their global strategy cache:</p>
 * <pre>
 *   TraversalStrategies.GlobalCache.registerStrategies(
 *       AcmeGraph.class,
 *       TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone()
 *           .addStrategies(GqlDeclarativeMatchStrategy.instance()));
 * </pre>
 *
 * <p>When the graph is closed call {@link #evict(Graph)} to release the cached pair:</p>
 * <pre>
 *   &#64;Override
 *   public void close() {
 *       GqlDeclarativeMatchStrategy.evict(this);
 *       // ... other cleanup
 *   }
 * </pre>
 *
 * <h3>Custom planner / executor</h3>
 * <p>Providers that supply their own {@link GqlPlanner} or {@link GqlExecutor} implementations
 * can use {@link #create(GqlPlanner, GqlExecutor)} to construct a per-instance strategy. That
 * strategy instance holds the supplied objects directly and does not interact with the shared
 * cache.</p>
 *
 * <h3>OLAP / GraphComputer limitation</h3>
 * <p>{@code match(String)} is <strong>not supported</strong> in OLAP (GraphComputer) mode.
 * The strategy's {@link #apply} method detects a graph-computer traversal context via
 * {@link org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper#onGraphComputer}
 * and returns immediately without replacing the placeholder step. Attempting to execute
 * {@code match(String)} in OLAP mode will therefore result in an
 * {@link UnsupportedOperationException} at traversal execution time.</p>
 *
 * <p>The limitation exists because TinkerGQL execution relies on direct, lazy vertex and edge
 * iteration against an OLTP graph (seed scans, DFS backtracking), which has no equivalent in
 * the scatter-gather message-passing model of OLAP. Providers seeking to support declarative
 * pattern matching in OLAP would need to implement it as a
 * {@link org.apache.tinkerpop.gremlin.process.computer.VertexProgram}.</p>
 */
public final class GqlDeclarativeMatchStrategy
        extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
        implements TraversalStrategy.ProviderOptimizationStrategy {

    // -------------------------------------------------------------------------
    // Singleton + shared cache
    // -------------------------------------------------------------------------

    private static final GqlDeclarativeMatchStrategy INSTANCE = new GqlDeclarativeMatchStrategy();

    /**
     * Per-graph-instance planner and executor cache. Keyed by graph identity so that all
     * traversals on the same graph share the planner's parse cache. Entries are removed when
     * {@link #evict(Graph)} is called (typically from {@code Graph.close()}).
     */
    private static final ConcurrentHashMap<Graph, PlannerExecutorPair> CACHE =
            new ConcurrentHashMap<>();

    // -------------------------------------------------------------------------
    // Per-instance state (only set when constructed via create(planner, executor))
    // -------------------------------------------------------------------------

    /** Non-null only for the custom-planner/executor path; null for the singleton. */
    private final GqlPlanner planner;
    /** Non-null only for the custom-planner/executor path; null for the singleton. */
    private final GqlExecutor executor;

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    /** Singleton constructor — uses the shared cache. */
    private GqlDeclarativeMatchStrategy() {
        this.planner = null;
        this.executor = null;
    }

    /** Custom-implementation constructor. */
    private GqlDeclarativeMatchStrategy(final GqlPlanner planner, final GqlExecutor executor) {
        this.planner = planner;
        this.executor = executor;
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Returns the singleton strategy that uses {@link DefaultGqlPlanner} and
     * {@link DefaultGqlExecutor} cached per graph instance. This is the recommended way for
     * providers to register TinkerGQL support.
     */
    public static GqlDeclarativeMatchStrategy instance() {
        return INSTANCE;
    }

    /**
     * Creates a per-instance strategy with explicit planner and executor objects. Use this when
     * you want to supply custom implementations; the strategy holds {@code planner} and
     * {@code executor} directly and does not interact with the shared cache.
     *
     * @param planner  the planner to use for every traversal that goes through this strategy
     * @param executor the executor to use for every traversal that goes through this strategy
     * @return a new strategy instance
     */
    public static GqlDeclarativeMatchStrategy create(final GqlPlanner planner,
                                                     final GqlExecutor executor) {
        return new GqlDeclarativeMatchStrategy(planner, executor);
    }

    /**
     * Removes the cached {@link GqlPlanner} / {@link GqlExecutor} pair for the given graph
     * instance. Graph implementations should call this from their {@code close()} method to
     * release the cache entry and allow the planner and executor to be garbage-collected.
     *
     * @param graph the graph whose cached pair should be removed
     */
    public static void evict(final Graph graph) {
        CACHE.remove(graph);
    }

    // -------------------------------------------------------------------------
    // Strategy application
    // -------------------------------------------------------------------------

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void apply(final Traversal.Admin<?, ?> traversal) {
        // match(String) is OLTP-only: TinkerGQL relies on direct vertex/edge iteration and DFS
        // backtracking, which have no equivalent in the OLAP scatter-gather model.
        if (TraversalHelper.onGraphComputer(traversal)) return;

        for (final DeclarativeMatchStep step :
                TraversalHelper.getStepsOfClass(DeclarativeMatchStep.class, traversal)) {

            GqlPlanner p = this.planner;
            GqlExecutor e = this.executor;

            if (p == null) {
                // Singleton path: resolve or create the pair from the traversal's graph.
                if (!traversal.getGraph().isPresent()) {
                    // No graph attached — leave the step unreplaced; it will fail at execution
                    // time with a clear error if the graph is still absent.
                    continue;
                }
                final Graph graph = traversal.getGraph().get();
                final PlannerExecutorPair pair = CACHE.computeIfAbsent(graph,
                        g -> new PlannerExecutorPair(
                                new DefaultGqlPlanner(g),
                                new DefaultGqlExecutor(g)));
                p = pair.planner;
                e = pair.executor;
            }

            TraversalHelper.replaceStep(step, new GqlMatchStep(step, p, e), traversal);
        }
    }

    // -------------------------------------------------------------------------
    // Internal types
    // -------------------------------------------------------------------------

    private static final class PlannerExecutorPair {
        final GqlPlanner planner;
        final GqlExecutor executor;

        PlannerExecutorPair(final GqlPlanner planner, final GqlExecutor executor) {
            this.planner = planner;
            this.executor = executor;
        }
    }
}
