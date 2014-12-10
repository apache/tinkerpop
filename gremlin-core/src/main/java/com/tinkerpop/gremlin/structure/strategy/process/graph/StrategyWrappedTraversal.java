package com.tinkerpop.gremlin.structure.strategy.process.graph;

import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.graph.strategy.GraphTraversalStrategyRegistry;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.util.DefaultTraversalStrategies;
import com.tinkerpop.gremlin.structure.strategy.StrategyGraph;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StrategyWrappedTraversal<S, E> extends DefaultGraphTraversal<S, E> {

    static {
        final DefaultTraversalStrategies traversalStrategies = new DefaultTraversalStrategies();
        GraphTraversalStrategyRegistry.instance().getTraversalStrategies().forEach(traversalStrategies::addStrategy);
        TraversalStrategies.GlobalCache.registerStrategies(StrategyWrappedTraversal.class, traversalStrategies);
    }

    public StrategyWrappedTraversal(final StrategyGraph graph) {
        super(graph);
    }
}
