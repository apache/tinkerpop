package com.tinkerpop.gremlin.structure.strategy.process.graph;

import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.strategy.GraphTraversalStrategyRegistry;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.util.DefaultTraversalStrategies;
import com.tinkerpop.gremlin.structure.strategy.StrategyWrappedGraph;
import com.tinkerpop.gremlin.structure.strategy.process.graph.step.sideEffect.StrategyWrappedGraphStep;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StrategyWrappedGraphTraversal<S, E> extends DefaultGraphTraversal<S, E> {

    static {
        final DefaultTraversalStrategies traversalStrategies = new DefaultTraversalStrategies();
        GraphTraversalStrategyRegistry.instance().getTraversalStrategies().forEach(traversalStrategies::addStrategy);
        TraversalStrategies.GlobalCache.registerStrategies(StrategyWrappedGraphTraversal.class, traversalStrategies);
    }

    public StrategyWrappedGraphTraversal(final Class<E> returnClass, final GraphTraversal<S, E> graphTraversal, final StrategyWrappedGraph strategyWrappedGraph) {
        super(strategyWrappedGraph);
        this.addStep(new StrategyWrappedGraphStep(this, returnClass, graphTraversal, strategyWrappedGraph));
    }
}
