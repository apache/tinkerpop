package com.tinkerpop.gremlin.process;

import java.util.*;

import com.tinkerpop.gremlin.util.tools.MultiMap;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface TraversalStrategies {

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
    public void apply(final Traversal traversal, final TraversalEngine engine);

    /**
     * Get the {@link TraverserGenerator} to use to generate traversers in the {@link Traversal}.
     *
     * @param traversal the traversal that will have traversers generated for it
     */
    public TraverserGenerator getTraverserGenerator(final Traversal traversal);

    /**
     * Sorts the list of provided strategies such that the {@link com.tinkerpop.gremlin.process.TraversalStrategy#applyPost()}
     * and {@link TraversalStrategy#applyPrior()} dependencies are respected.
     *
     * Note, that the order may not be unique.
     *
     * @param strategies
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
                for (Class<? extends TraversalStrategy> before : MultiMap.get(dependencyMap,sc)) {
                    final Set<Class<? extends TraversalStrategy>> beforeDep = MultiMap.get(dependencyMap,before);
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
                boolean s1Before = MultiMap.containsEntry(dependencyMap,s1.getClass(), s2.getClass());
                boolean s2Before = MultiMap.containsEntry(dependencyMap,s2.getClass(), s1.getClass());
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
            final TraversalStrategies traversalStrategies = CACHE.get(traversalClass);
            if (null == traversalStrategies)
                throw new IllegalArgumentException("The provided traversal class does not have a cached strategies: " + traversalClass.getCanonicalName());
            return traversalStrategies;
        }
    }


}
