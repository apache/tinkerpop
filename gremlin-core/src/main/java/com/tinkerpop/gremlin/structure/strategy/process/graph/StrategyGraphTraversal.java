package com.tinkerpop.gremlin.structure.strategy.process.graph;

import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.strategy.GraphTraversalStrategyRegistry;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.util.DefaultTraversalStrategies;
import com.tinkerpop.gremlin.structure.strategy.StrategyGraph;
import com.tinkerpop.gremlin.structure.strategy.process.graph.step.sideEffect.StrategyGraphStep;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StrategyGraphTraversal<S, E> extends DefaultGraphTraversal<S, E> {

    static {
        final DefaultTraversalStrategies traversalStrategies = new DefaultTraversalStrategies();
        GraphTraversalStrategyRegistry.instance().getTraversalStrategies().forEach(traversalStrategies::addStrategy);
        TraversalStrategies.GlobalCache.registerStrategies(StrategyGraphTraversal.class, traversalStrategies);
    }

    public StrategyGraphTraversal(final Class<E> returnClass, final GraphTraversal<S, E> graphTraversal, final StrategyGraph strategyGraph) {
        super(strategyGraph);
        this.addStep(new StrategyGraphStep(this, strategyGraph, returnClass, graphTraversal));
    }
}
