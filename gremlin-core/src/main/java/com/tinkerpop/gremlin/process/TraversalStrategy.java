package com.tinkerpop.gremlin.process;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A {@link TraversalStrategy} defines a particular atomic operation for mutating a {@link Traversal} prior to its evaluation.
 * Traversal strategies are typically used for optimizing a traversal for the particular underlying graph engine.
 * Traversal strategies implement {@link Comparable} and thus are sorted to determine their evaluation order.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface TraversalStrategy {

    // A TraversalStrategy should not have a public constructor
    // Make use of a singleton instance() object to reduce object creation on the JVM

    public void apply(final Traversal<?, ?> traversal, final TraversalEngine traversalEngine);

    public default Set<Class<? extends TraversalStrategy>> applyPrior() {
        return Collections.emptySet();
    }

    public default Set<Class<? extends TraversalStrategy>> applyPost() {
        return Collections.emptySet();
    }

    public static void sortStrategies(List<? extends TraversalStrategy> strategies) {
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
}
