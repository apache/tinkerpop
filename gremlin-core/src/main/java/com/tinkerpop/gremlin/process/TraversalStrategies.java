package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.process.graph.strategy.GraphTraversalStrategyRegistry;
import com.tinkerpop.gremlin.process.traversers.TraverserGeneratorFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
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
