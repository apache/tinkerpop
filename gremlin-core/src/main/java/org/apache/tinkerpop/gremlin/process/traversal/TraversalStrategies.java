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

import org.apache.tinkerpop.gremlin.process.computer.util.ShellGraph;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.ConjunctionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.DedupOptimizerStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.IdentityRemovalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.MatchWhereStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.ProfileStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.RangeByIsCountStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ComparatorHolderRemovalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.EngineDependentStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.LabeledEndStepStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.TraversalVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserGeneratorFactory;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.util.tools.MultiMap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface TraversalStrategies extends Serializable, Cloneable {

    /**
     * Return all the {@link TraversalStrategy} singleton instances associated with this {@link TraversalStrategies}.
     */
    public List<TraversalStrategy> toList();

    /**
     * Apply all the {@link TraversalStrategy} optimizers to the {@link Traversal} for the stated {@link TraversalEngine}.
     * This method must ensure that the strategies are sorted prior to application.
     *
     * @param traversal the traversal to apply the strategies to
     * @param engine    the engine that the traversal is going to be executed on
     */
    public void applyStrategies(final Traversal.Admin<?, ?> traversal);

    /**
     * Add all the provided {@link TraversalStrategy} instances to the current collection.
     * When all the provided strategies have been added, the collection is resorted.
     *
     * @param strategies the traversal strategies to add
     * @return the newly updated/sorted traversal strategies collection
     */
    public TraversalStrategies addStrategies(final TraversalStrategy... strategies);

    /**
     * Remove all the provided {@link TraversalStrategy} classes from the current collection.
     * When all the provided strategies have been removed, the collection is resorted.
     *
     * @param strategyClasses the traversal strategies to remove by their class
     * @return the newly updated/sorted traversal strategies collection
     */
    @SuppressWarnings("unchecked")
    public TraversalStrategies removeStrategies(final Class<? extends TraversalStrategy>... strategyClasses);

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
    public TraversalStrategies clone();

    /**
     * Get the {@link TraverserGeneratorFactory} to use to generate traversers.
     */
    public TraverserGeneratorFactory getTraverserGeneratorFactory();

    /**
     * Set the {@link TraverserGeneratorFactory} to use for determining which {@link Traverser} type to generate for the {@link Traversal}.
     *
     * @param traverserGeneratorFactory the factory to use
     */
    public void setTraverserGeneratorFactory(final TraverserGeneratorFactory traverserGeneratorFactory);

    /**
     * Sorts the list of provided strategies such that the {@link TraversalStrategy#applyPost()}
     * and {@link TraversalStrategy#applyPrior()} dependencies are respected.
     * <p/>
     * Note, that the order may not be unique.
     *
     * @param strategies the traversal strategies to sort
     */
    public static void sortStrategies(final List<? extends TraversalStrategy> strategies) {
        final Map<Class<? extends TraversalStrategy>, Set<Class<? extends TraversalStrategy>>> dependencyMap = new HashMap<>();
        final Set<Class<? extends TraversalStrategy>> strategyClass = new HashSet<>(strategies.size());
        //Initialize data structure
        strategies.forEach(s -> strategyClass.add(s.getClass()));

        //Initialize all the dependencies
        strategies.forEach(strategy -> {
            strategy.applyPrior().forEach(s -> {
                if (strategyClass.contains(s)) MultiMap.put(dependencyMap, s, strategy.getClass());
            });
            strategy.applyPost().forEach(s -> {
                if (strategyClass.contains(s)) MultiMap.put(dependencyMap, strategy.getClass(), s);
            });
        });
        //Now, compute transitive closure until convergence
        boolean updated;
        do {
            updated = false;
            for (final Class<? extends TraversalStrategy> sc : strategyClass) {
                List<Class<? extends TraversalStrategy>> toAdd = null;
                for (Class<? extends TraversalStrategy> before : MultiMap.get(dependencyMap, sc)) {
                    final Set<Class<? extends TraversalStrategy>> beforeDep = MultiMap.get(dependencyMap, before);
                    if (!beforeDep.isEmpty()) {
                        if (toAdd == null) toAdd = new ArrayList<>(beforeDep.size());
                        toAdd.addAll(beforeDep);
                    }
                }
                if (toAdd != null && MultiMap.putAll(dependencyMap, sc, toAdd)) updated = true;
            }
        } while (updated);
        Collections.sort(strategies, new Comparator<TraversalStrategy>() {
            @Override
            public int compare(final TraversalStrategy s1, final TraversalStrategy s2) {
                boolean s1Before = MultiMap.containsEntry(dependencyMap, s1.getClass(), s2.getClass());
                boolean s2Before = MultiMap.containsEntry(dependencyMap, s2.getClass(), s1.getClass());
                if (s1Before && s2Before)
                    throw new IllegalStateException("Cyclic dependency between traversal strategies: ["
                            + s1.getClass().getName() + ", " + s2.getClass().getName() + ']');
                if (s1Before) return -1;
                else if (s2Before) return 1;
                else return 0;
            }
        });
    }

    public static final class GlobalCache {

        private static final Map<Class<? extends Graph>, TraversalStrategies> CACHE = new HashMap<>();

        static {
            final TraversalStrategies coreStrategies = new DefaultTraversalStrategies();
            coreStrategies.addStrategies(
                    DedupOptimizerStrategy.instance(),
                    RangeByIsCountStrategy.instance(),
                    IdentityRemovalStrategy.instance(),
                    MatchWhereStrategy.instance(),
                    ComparatorHolderRemovalStrategy.instance(),
                    LabeledEndStepStrategy.instance(),
                    //LambdaRestrictionStrategy.instance(),
                    EngineDependentStrategy.instance(),
                    ProfileStrategy.instance(),
                    TraversalVerificationStrategy.instance(),
                    ConjunctionStrategy.instance());
            CACHE.put(Graph.class, coreStrategies.clone());
            CACHE.put(EmptyGraph.class, new DefaultTraversalStrategies());
        }

        public static void registerStrategies(final Class<? extends Graph> graphClass, final TraversalStrategies traversalStrategies) {
            CACHE.put(graphClass, traversalStrategies);
        }

        public static TraversalStrategies getStrategies(final Class<? extends Graph> graphClass) {
            final TraversalStrategies traversalStrategies = CACHE.get(graphClass);
            if (null == traversalStrategies) {
                if (EmptyGraph.class.isAssignableFrom(graphClass))
                    return CACHE.get(EmptyGraph.class);
                else return CACHE.get(Graph.class);
            }
            return traversalStrategies;
        }

        public static Class<? extends Graph> getGraphClass(final Graph graph) {
            return graph instanceof ShellGraph ? ((ShellGraph) graph).getGraphClass() : graph.getClass();
        }
    }


}
