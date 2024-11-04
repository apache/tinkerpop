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
package org.apache.tinkerpop.gremlin.process.traversal;

import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.decoration.VertexProgramStrategy;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.finalization.ComputerFinalizationStrategy;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.optimization.GraphFilterStrategy;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.optimization.MessagePassingReductionStrategy;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.verification.VertexProgramRestrictionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ConnectiveStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.HaltedTraverserStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.PartitionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SackStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SeedStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization.MatchAlgorithmStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization.ProfileStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization.ReferenceElementStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.AdjacentToIncidentStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.ByModulatorOptimizationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.CountStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.EarlyLimitStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.FilterRankingStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.IdentityRemovalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.IncidentToAdjacentStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.InlineFilterStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.LazyBarrierStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.MatchPredicateStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.OrderLimitStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.PathProcessorStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.PathRetractionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.ProductiveByStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.RepeatUnrollStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ComputerVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.EdgeLabelVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.LambdaRestrictionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReservedKeysVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.StandardVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.util.MultiMap;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * A {@link Traversal} maintains a set of {@link TraversalStrategy} instances within a {@code }TraversalStrategies}
 * object. Of particular importance is the {@link GlobalCache} which maintains a set of default strategies to be applied
 * and a registry of available strategies.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface TraversalStrategies extends Serializable, Cloneable, Iterable<TraversalStrategy<?>> {

    static List<Class<? extends TraversalStrategy>> STRATEGY_CATEGORIES = Collections.unmodifiableList(Arrays.asList(
            TraversalStrategy.DecorationStrategy.class, TraversalStrategy.OptimizationStrategy.class,
            TraversalStrategy.ProviderOptimizationStrategy.class, TraversalStrategy.FinalizationStrategy.class,
            TraversalStrategy.VerificationStrategy.class));

    /**
     * Return an immutable list of the {@link TraversalStrategy} instances.
     */
    public default List<TraversalStrategy<?>> toList() {
        return Collections.unmodifiableList(IteratorUtils.list(iterator()));
    }

    /**
     * Return an {@code Iterator} of the {@link TraversalStrategy} instances.
     */
    @Override
    public Iterator<TraversalStrategy<?>> iterator();

    /**
     * Return the {@link TraversalStrategy} instance associated with the provided class.
     *
     * @param traversalStrategyClass the class of the strategy to get
     * @param <T>                    the strategy class type
     * @return an optional containing the strategy instance or not
     */
    public default <T extends TraversalStrategy> Optional<T> getStrategy(final Class<T> traversalStrategyClass) {
        return (Optional<T>) IteratorUtils.stream(iterator()).
                filter(s -> traversalStrategyClass.isAssignableFrom(s.getClass())).findAny();
    }

    /**
     * Add all the provided {@link TraversalStrategy} instances to the current collection. When all the provided
     * strategies have been added, the collection is resorted. If a strategy class is found to already be defined, it
     * is removed and replaced by the newly added one.
     *
     * @param strategies the traversal strategies to add
     * @return the newly updated/sorted traversal strategies collection
     */
    public TraversalStrategies addStrategies(final TraversalStrategy<?>... strategies);

    /**
     * Remove all the provided {@link TraversalStrategy} classes from the current collection.
     * When all the provided strategies have been removed, the collection is resorted.
     *
     * @param strategyClasses the traversal strategies to remove by their class
     * @return the newly updated/sorted traversal strategies collection
     */
    @SuppressWarnings({"unchecked", "varargs"})
    public TraversalStrategies removeStrategies(final Class<? extends TraversalStrategy>... strategyClasses);

    @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
    public TraversalStrategies clone();

    /**
     * Sorts the list of provided strategies such that the {@link TraversalStrategy#applyPost()}
     * and {@link TraversalStrategy#applyPrior()} dependencies are respected.
     * <p/>
     * Note, that the order may not be unique.
     *
     * @param strategies the traversal strategies to sort
     */
    public static Set<TraversalStrategy<?>> sortStrategies(final Set<TraversalStrategy<?>> strategies) {
        final Map<Class<? extends TraversalStrategy>, Set<Class<? extends TraversalStrategy>>> dependencyMap = new HashMap<>();
        final Map<Class<? extends TraversalStrategy>, Set<Class<? extends TraversalStrategy>>> strategiesByCategory = new HashMap<>();
        final Set<Class<? extends TraversalStrategy>> strategyClasses = new HashSet<>(strategies.size());
        //Initialize data structure
        strategies.forEach(s -> {
            strategyClasses.add(s.getClass());
            MultiMap.put(strategiesByCategory, s.getTraversalCategory(), s.getClass());
        });

        //Initialize all the dependencies
        strategies.forEach(strategy -> {
            strategy.applyPrior().forEach(s -> {
                if (strategyClasses.contains(s)) MultiMap.put(dependencyMap, strategy.getClass(), s);
            });
            strategy.applyPost().forEach(s -> {
                if (strategyClasses.contains(s)) MultiMap.put(dependencyMap, s, strategy.getClass());
            });
        });

        //Add dependencies by category
        final List<Class<? extends TraversalStrategy>> strategiesInPreviousCategories = new ArrayList<>();
        for (Class<? extends TraversalStrategy> category : STRATEGY_CATEGORIES) {
            final Set<Class<? extends TraversalStrategy>> strategiesInThisCategory = MultiMap.get(strategiesByCategory, category);
            for (Class<? extends TraversalStrategy> strategy : strategiesInThisCategory) {
                for (Class<? extends TraversalStrategy> previousStrategy : strategiesInPreviousCategories) {
                    MultiMap.put(dependencyMap, strategy, previousStrategy);
                }
            }
            strategiesInPreviousCategories.addAll(strategiesInThisCategory);
        }

        //Finally sort via t-sort
        final List<Class<? extends TraversalStrategy>> unprocessedStrategyClasses = new ArrayList<>(strategies.stream().map(s -> s.getClass()).collect(Collectors.toSet()));
        final List<Class<? extends TraversalStrategy>> sortedStrategyClasses = new ArrayList<>();
        final Set<Class<? extends TraversalStrategy>> seenStrategyClasses = new HashSet<>();

        while (!unprocessedStrategyClasses.isEmpty()) {
            final Class<? extends TraversalStrategy> strategy = unprocessedStrategyClasses.get(0);
            visit(dependencyMap, sortedStrategyClasses, seenStrategyClasses, unprocessedStrategyClasses, strategy);
        }

        final Set<TraversalStrategy<?>> sortedStrategies = new LinkedHashSet<>();
        //We now have a linked set of sorted strategy classes
        for (Class<? extends TraversalStrategy> strategyClass : sortedStrategyClasses) {
            for (TraversalStrategy strategy : strategies) {
                if (strategy.getClass().equals(strategyClass)) {
                    sortedStrategies.add(strategy);
                }
            }
        }

        return sortedStrategies;
    }

    static void visit(final Map<Class<? extends TraversalStrategy>, Set<Class<? extends TraversalStrategy>>> dependencyMap,
                      final List<Class<? extends TraversalStrategy>> sortedStrategyClasses,
                      final Set<Class<? extends TraversalStrategy>> seenStrategyClases,
                      final List<Class<? extends TraversalStrategy>> unprocessedStrategyClasses, Class<? extends TraversalStrategy> strategyClass) {
        if (seenStrategyClases.contains(strategyClass)) {
            throw new IllegalStateException("Cyclic dependency between traversal strategies: ["
                    + seenStrategyClases + ']');
        }

        if (unprocessedStrategyClasses.contains(strategyClass)) {
            seenStrategyClases.add(strategyClass);
            for (Class<? extends TraversalStrategy> dependency : MultiMap.get(dependencyMap, strategyClass)) {
                visit(dependencyMap, sortedStrategyClasses, seenStrategyClases, unprocessedStrategyClasses, dependency);
            }
            seenStrategyClases.remove(strategyClass);
            unprocessedStrategyClasses.remove(strategyClass);
            sortedStrategyClasses.add(strategyClass);
        }
    }

    public static final class GlobalCache {

        /**
         * Keeps track of {@link GraphComputer} and/or {@link Graph} classes that have been initialized to the
         * classloader so that they do not have to be reflected again.
         */
        private static final Set<Class<?>> LOADED = ConcurrentHashMap.newKeySet();

        private static final Map<Class<? extends Graph>, TraversalStrategies> GRAPH_CACHE = new HashMap<>();
        private static final Map<Class<? extends GraphComputer>, TraversalStrategies> GRAPH_COMPUTER_CACHE = new HashMap<>();

        /**
         * A register of the simple names for all strategies.
         */
        private static final Map<String, Class<? extends TraversalStrategy>> GLOBAL_REGISTRY = new HashMap<String, Class<? extends TraversalStrategy>>() {{
            // decorations
            put(ConnectiveStrategy.class.getSimpleName(), ConnectiveStrategy.class);
            put(ElementIdStrategy.class.getSimpleName(), ElementIdStrategy.class);
            put(EventStrategy.class.getSimpleName(), EventStrategy.class);
            put(HaltedTraverserStrategy.class.getSimpleName(), HaltedTraverserStrategy.class);
            put(OptionsStrategy.class.getSimpleName(), OptionsStrategy.class);
            put(PartitionStrategy.class.getSimpleName(), PartitionStrategy.class);
            put(SeedStrategy.class.getSimpleName(), SeedStrategy.class);
            put(SubgraphStrategy.class.getSimpleName(), SubgraphStrategy.class);
            put(VertexProgramStrategy.class.getSimpleName(), VertexProgramStrategy.class);

            // finalization
            put(MatchAlgorithmStrategy.class.getSimpleName(), MatchAlgorithmStrategy.class);
            put(ReferenceElementStrategy.class.getSimpleName(), ReferenceElementStrategy.class);

            // optimizations
            put(ProductiveByStrategy.class.getSimpleName(), ProductiveByStrategy.class);
            put(PathRetractionStrategy.class.getSimpleName(), PathRetractionStrategy.class);
            put(RepeatUnrollStrategy.class.getSimpleName(), RepeatUnrollStrategy.class);

            // verification
            put(EdgeLabelVerificationStrategy.class.getSimpleName(), EdgeLabelVerificationStrategy.class);
            put(LambdaRestrictionStrategy.class.getSimpleName(), LambdaRestrictionStrategy.class);
            put(ReadOnlyStrategy.class.getSimpleName(), ReadOnlyStrategy.class);
            put(ReservedKeysVerificationStrategy.class.getSimpleName(), ReservedKeysVerificationStrategy.class);
            put(VertexProgramRestrictionStrategy.class.getSimpleName(), VertexProgramRestrictionStrategy.class);
        }};

        static {
            final TraversalStrategies graphStrategies = new DefaultTraversalStrategies();
            graphStrategies.addStrategies(
                    IdentityRemovalStrategy.instance(),
                    ConnectiveStrategy.instance(),
                    EarlyLimitStrategy.instance(),
                    InlineFilterStrategy.instance(),
                    IncidentToAdjacentStrategy.instance(),
                    AdjacentToIncidentStrategy.instance(),
                    ByModulatorOptimizationStrategy.instance(),
                    FilterRankingStrategy.instance(),
                    MatchPredicateStrategy.instance(),
                    RepeatUnrollStrategy.instance(),
                    CountStrategy.instance(),
                    PathRetractionStrategy.instance(),
                    LazyBarrierStrategy.instance(),
                    ProfileStrategy.instance(),
                    StandardVerificationStrategy.instance());
            registerStrategies(Graph.class, graphStrategies);
            registerStrategies(EmptyGraph.class, new DefaultTraversalStrategies());

            /////////////////////

            final TraversalStrategies graphComputerStrategies = new DefaultTraversalStrategies();
            graphComputerStrategies.addStrategies(
                    GraphFilterStrategy.instance(),
                    MessagePassingReductionStrategy.instance(),
                    OrderLimitStrategy.instance(),
                    PathProcessorStrategy.instance(),
                    ComputerFinalizationStrategy.instance(),
                    ComputerVerificationStrategy.instance());
            registerStrategies(GraphComputer.class, graphComputerStrategies);
        }

        /**
         * Register a set of strategies for a particular graph or graph computer class. This is typically done by the
         * graph or graph computer class itself when it is loaded. Strategy names should be globally unique and are
         * added to the {@link #GLOBAL_REGISTRY} such that duplicates will overwrite the previous registration.
         */
        public static void registerStrategies(final Class graphOrGraphComputerClass, final TraversalStrategies traversalStrategies) {
            if (Graph.class.isAssignableFrom(graphOrGraphComputerClass))
                GRAPH_CACHE.put(graphOrGraphComputerClass, traversalStrategies);
            else if (GraphComputer.class.isAssignableFrom(graphOrGraphComputerClass))
                GRAPH_COMPUTER_CACHE.put(graphOrGraphComputerClass, traversalStrategies);
            else
                throw new IllegalArgumentException("The TraversalStrategies.GlobalCache only supports Graph and GraphComputer strategy caching: " + graphOrGraphComputerClass.getCanonicalName());

            // add the strategies in the traversalStrategy to the global registry
            traversalStrategies.toList().forEach(strategy -> GLOBAL_REGISTRY.put(strategy.getClass().getSimpleName(), strategy.getClass()));
        }

        /**
         * Registers a strategy by its simple name, but does not cache an instance of it. Choose this method if you
         * don't want the strategy to be included as part of the default strategy set, but do want it available to
         * the grammar when parsing Gremlin.
         */
        public static void registerStrategy(final Class<? extends TraversalStrategy> clazz) {
            GLOBAL_REGISTRY.put(clazz.getSimpleName(), clazz);
        }

        /**
         * Unregisters a strategy by its simple name. If the strategy is not in the registry then the grammar cannot
         * reference it which means that it cannot be removed from execution using
         * {{@link GraphTraversalSource#withoutStrategies(Class[])}}.
         */
        public static void unregisterStrategy(final Class<? extends TraversalStrategy> clazz) {
            GLOBAL_REGISTRY.remove(clazz.getSimpleName());
        }

        /**
         * Looks up a strategy by its simple name.
         */
        public static Optional<? extends Class<? extends TraversalStrategy>> getRegisteredStrategyClass(final String strategyName) {
            if (GLOBAL_REGISTRY.containsKey(strategyName))
                return Optional.of(GLOBAL_REGISTRY.get(strategyName));

            return Optional.empty();
        }

        public static TraversalStrategies getStrategies(final Class graphOrGraphComputerClass) {
            try {
                // be sure to load the class so that its static{} traversal strategy registration component is loaded.
                // this is more important for GraphComputer classes as they are typically not instantiated prior to
                // strategy usage like Graph classes.
                if (!LOADED.contains(graphOrGraphComputerClass)) {
                    final String graphComputerClassName = null != graphOrGraphComputerClass.getDeclaringClass() ?
                            graphOrGraphComputerClass.getCanonicalName().replace("." + graphOrGraphComputerClass.getSimpleName(), "$" + graphOrGraphComputerClass.getSimpleName()) :
                            graphOrGraphComputerClass.getCanonicalName();
                    Class.forName(graphComputerClassName);

                    // keep track of stuff we already loaded once - stuff in this if/statement isn't cheap and this
                    // method gets called a lot, basically every time a new traversal gets spun up (that includes
                    // child traversals. perhaps it is possible to just check the cache keys for this information, but
                    // it's not clear if this method will be called with something not in the cache and if it is and
                    // it results in error, then we'd probably not want to deal with this block again anyway
                    LOADED.add(graphOrGraphComputerClass);
                }
            } catch (final ClassNotFoundException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }

            if (GRAPH_CACHE.containsKey(graphOrGraphComputerClass)) {
                return GRAPH_CACHE.get(graphOrGraphComputerClass);
            } else if (Graph.class.isAssignableFrom(graphOrGraphComputerClass)) {
                return GRAPH_CACHE.get(Graph.class);
            } else if (GRAPH_COMPUTER_CACHE.containsKey(graphOrGraphComputerClass)) {
                return GRAPH_COMPUTER_CACHE.get(graphOrGraphComputerClass);
            } else if (GraphComputer.class.isAssignableFrom(graphOrGraphComputerClass)) {
                return GRAPH_COMPUTER_CACHE.get(GraphComputer.class);
            } else {
                throw new IllegalArgumentException("The TraversalStrategies.GlobalCache only supports Graph and GraphComputer strategy caching: " + graphOrGraphComputerClass.getCanonicalName());
            }
        }
    }
}
