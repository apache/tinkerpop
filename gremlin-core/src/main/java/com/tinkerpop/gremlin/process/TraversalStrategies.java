package com.tinkerpop.gremlin.process;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.tinkerpop.gremlin.process.graph.strategy.GraphTraversalStrategyRegistry;
import com.tinkerpop.gremlin.process.traversers.TraverserGeneratorFactory;

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
public interface TraversalStrategies {

    /**
     * Return all the {@link TraversalStrategy} singleton instances associated with this traversal.
     */
    public List<TraversalStrategy> toList();

    /**
     * Register a {@link TraversalStrategy} with this traversal.
     *
     * @param traversalStrategy the traversal strategy to register
     */
    public void register(final TraversalStrategy traversalStrategy);

    /**
     * Unregister a {@link TraversalStrategy} associated with this traversal.
     * Given that all traversal strategies are singletons, the class is sufficient to unregister it.
     *
     * @param traversalStrategyClass the class of the traversal strategy to unregister
     */
    public void unregister(final Class<? extends TraversalStrategy> traversalStrategyClass);

    /**
     * Apply all the {@link TraversalStrategy} optimizers to the traversal for the stated {@link TraversalEngine}.
     * This method should sort the strategies prior to application.
     *
     * @param engine the engine that the traversal is going to be executed on
     */
    public void apply(final Traversal traversal, final TraversalEngine engine);

    /**
     * A helper method to remove all the {@link TraversalStrategy} instances from the traversal.
     */
    public default void clear() {
        this.toList().forEach(strategy -> this.unregister(strategy.getClass()));
    }

    public void registerTraverserGeneratorFactory(final TraverserGeneratorFactory traverserGeneratorFactory);

    public TraverserGenerator getTraverserGenerator(final Traversal traversal, final TraversalEngine engine);

    public static void sortStrategies(final List<? extends TraversalStrategy> strategies) {
        final SetMultimap<Class<? extends TraversalStrategy>, Class<? extends TraversalStrategy>> dependencyMap = HashMultimap.create();
        final Set<Class<? extends TraversalStrategy>> strategyClass = new HashSet<>(strategies.size());
        //Initialize data structure
        strategies.forEach(s -> strategyClass.add(s.getClass()));

        //Initialize all the dependencies
        strategies.forEach(strategy -> {
            strategy.applyPrior().forEach(s -> {
                if (strategyClass.contains(s)) dependencyMap.put(s, strategy.getClass());
            });
            strategy.applyPost().forEach(s -> {
                if (strategyClass.contains(s)) dependencyMap.put(strategy.getClass(), s);
            });
        });
        //Now, compute transitive closure until convergence
        boolean updated = false;
        do {
            for (final Class<? extends TraversalStrategy> sc : strategyClass) {
                List<Class<? extends TraversalStrategy>> toAdd = null;
                for (Class<? extends TraversalStrategy> before : dependencyMap.get(sc)) {
                    final Set<Class<? extends TraversalStrategy>> beforeDep = dependencyMap.get(before);
                    if (!beforeDep.isEmpty()) {
                        if (toAdd == null) toAdd = new ArrayList<>(beforeDep.size());
                        toAdd.addAll(beforeDep);
                    }
                }
                if (toAdd != null && dependencyMap.putAll(sc, toAdd)) updated = true;
            }
        } while (updated);
        Collections.sort(strategies, new Comparator<TraversalStrategy>() {
            @Override
            public int compare(final TraversalStrategy s1, final TraversalStrategy s2) {
                boolean s1Before = dependencyMap.containsEntry(s1.getClass(), s2.getClass());
                boolean s2Before = dependencyMap.containsEntry(s2.getClass(), s1.getClass());
                if (s1Before && s2Before)
                    throw new IllegalStateException("Cyclic dependency between traversal strategies: ["
                            + s1.getClass().getName() + ", " + s2.getClass().getName() + "]");
                if (s1Before) return -1;
                else if (s2Before) return 1;
                else return 0;
            }
        });
    }

    public static final class GlobalCache {

        private static final Map<Class<? extends Traversal>, TraversalStrategies> CACHE = new HashMap<>();

        public static void registerStrategies(final Class<? extends Traversal> traversalClass, final TraversalStrategies traversalStrategies) {
            CACHE.put(traversalClass, traversalStrategies);
        }

        public static TraversalStrategies getStrategies(final Class<? extends Traversal> traversalClass) {
            final TraversalStrategies ts = CACHE.get(traversalClass);
            return null == ts ? GraphTraversalStrategyRegistry.instance() : ts;
        }
    }

}
