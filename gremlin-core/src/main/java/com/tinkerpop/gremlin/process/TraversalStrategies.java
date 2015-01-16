package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.process.graph.AnonymousGraphTraversal;
import com.tinkerpop.gremlin.process.graph.strategy.ComparatorHolderRemovalStrategy;
import com.tinkerpop.gremlin.process.graph.strategy.DedupOptimizerStrategy;
import com.tinkerpop.gremlin.process.graph.strategy.EngineDependentStrategy;
import com.tinkerpop.gremlin.process.graph.strategy.IdentityRemovalStrategy;
import com.tinkerpop.gremlin.process.graph.strategy.LabeledEndStepStrategy;
import com.tinkerpop.gremlin.process.graph.strategy.MatchWhereStrategy;
import com.tinkerpop.gremlin.process.graph.strategy.ReducingStrategy;
import com.tinkerpop.gremlin.process.graph.strategy.RouteStrategy;
import com.tinkerpop.gremlin.process.graph.strategy.SideEffectCapStrategy;
import com.tinkerpop.gremlin.process.traverser.TraverserGeneratorFactory;
import com.tinkerpop.gremlin.process.util.DefaultTraversalStrategies;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.util.tools.MultiMap;

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
public interface TraversalStrategies extends Cloneable {

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
    public void applyStrategies(final Traversal traversal, final TraversalEngine engine);

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
    public TraversalStrategies clone() throws CloneNotSupportedException;

    /**
     * Get the {@link TraverserGenerator} to use to generate traversers in the {@link Traversal}.
     *
     * @param traversal the traversal that will have traversers generated for it
     */
    public TraverserGenerator getTraverserGenerator(final Traversal traversal);

    /**
     * Set the {@link TraverserGeneratorFactory} to use for determining which {@link Traverser} type to generate for the {@link Traversal}.
     *
     * @param traverserGeneratorFactory the factory to use
     */
    public void setTraverserGeneratorFactory(final TraverserGeneratorFactory traverserGeneratorFactory);

    /**
     * Sorts the list of provided strategies such that the {@link com.tinkerpop.gremlin.process.TraversalStrategy#applyPost()}
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
                            + s1.getClass().getName() + ", " + s2.getClass().getName() + "]");
                if (s1Before) return -1;
                else if (s2Before) return 1;
                else return 0;
            }
        });
    }

    public static final class GlobalCache {

        private static final Map<Class, TraversalStrategies> CACHE = new HashMap<>();

        static {
            final TraversalStrategies coreStrategies = new DefaultTraversalStrategies();
            coreStrategies.addStrategies(
                    DedupOptimizerStrategy.instance(),
                    IdentityRemovalStrategy.instance(),
                    SideEffectCapStrategy.instance(),
                    MatchWhereStrategy.instance(),
                    ComparatorHolderRemovalStrategy.instance(),
                    ReducingStrategy.instance(),
                    LabeledEndStepStrategy.instance(),
                    EngineDependentStrategy.instance(),
                    RouteStrategy.instance());

            try {
                CACHE.put(Graph.class, coreStrategies.clone());
                CACHE.put(Vertex.class, coreStrategies.clone());
                CACHE.put(Edge.class, coreStrategies.clone());
                CACHE.put(VertexProperty.class, coreStrategies.clone());
                CACHE.put(AnonymousGraphTraversal.class, new DefaultTraversalStrategies());
            } catch (final CloneNotSupportedException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }

        public static void registerStrategies(final Class emanatingClass, final TraversalStrategies traversalStrategies) {
            CACHE.put(emanatingClass, traversalStrategies);
        }

        public static TraversalStrategies getStrategies(final Class emanatingClass) {
            final TraversalStrategies traversalStrategies = CACHE.get(emanatingClass);
            if (null == traversalStrategies) {
                if (AnonymousGraphTraversal.class.isAssignableFrom(emanatingClass))
                    return CACHE.get(AnonymousGraphTraversal.class);
                else if (Graph.class.isAssignableFrom(emanatingClass))
                    return CACHE.get(Graph.class);
                else if (Vertex.class.isAssignableFrom(emanatingClass))
                    return CACHE.get(Vertex.class);
                else if (Edge.class.isAssignableFrom(emanatingClass))
                    return CACHE.get(Edge.class);
                else if (VertexProperty.class.isAssignableFrom(emanatingClass))
                    return CACHE.get(VertexProperty.class);
                else
                    return new DefaultTraversalStrategies();
                // throw new IllegalStateException("The provided class has no registered traversal strategies: " + emanatingClass);
            }
            return traversalStrategies;
        }
    }


}
