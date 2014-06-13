package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.Traversal;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultTraversalStrategies implements TraversalStrategies {

    private final List<TraversalStrategy> traversalStrategies = new ArrayList<>();

    public List<TraversalStrategy> get() {
        return this.traversalStrategies;
    }

    public void register(final TraversalStrategy traversalStrategy) {
        this.traversalStrategies.add(0, traversalStrategy);   // TODO: eek around PathConsumerStrategy
    }

    public void unregister(final Class<? extends TraversalStrategy> optimizerClass) {
        this.traversalStrategies.stream().filter(o -> optimizerClass.isAssignableFrom(o.getClass()))
                .collect(Collectors.toList())
                .forEach(this.traversalStrategies::remove);
    }

    public void applyFinalOptimizers(final Traversal traversal) {
        this.traversalStrategies.stream()
                .filter(o -> o instanceof TraversalStrategy.FinalTraversalStrategy)
                .forEach(o -> ((TraversalStrategy.FinalTraversalStrategy) o).apply(traversal));
    }


}
