package com.tinkerpop.gremlin.structure.strategy.process.graph;

import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.graph.strategy.GraphTraversalStrategyRegistry;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.util.DefaultTraversalStrategies;
import com.tinkerpop.gremlin.structure.strategy.StrategyGraph;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StrategyTraversal<S, E> extends DefaultGraphTraversal<S, E> {

    static {
        final DefaultTraversalStrategies traversalStrategies = new DefaultTraversalStrategies();
        GraphTraversalStrategyRegistry.instance().getTraversalStrategies().forEach(traversalStrategies::addStrategy);
        TraversalStrategies.GlobalCache.registerStrategies(StrategyTraversal.class, traversalStrategies);
    }

    public StrategyTraversal(final StrategyGraph graph) {
        super(graph);
    }
}
