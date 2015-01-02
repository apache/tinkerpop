package com.tinkerpop.gremlin.tinkergraph.process.graph.util;

import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.process.graph.strategy.GraphTraversalStrategyRegistry;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.util.DefaultTraversalStrategies;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.tinkergraph.process.graph.strategy.TinkerElementStepStrategy;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultTinkerElementTraversal<S, E> extends DefaultGraphTraversal<S, E> {

    static {
        final DefaultTraversalStrategies traversalStrategies = new DefaultTraversalStrategies();
        GraphTraversalStrategyRegistry.instance().getTraversalStrategies().forEach(traversalStrategies::addStrategy);
        traversalStrategies.addStrategy(TinkerElementStepStrategy.instance());
        TraversalStrategies.GlobalCache.registerStrategies(DefaultTinkerElementTraversal.class, traversalStrategies);
    }

    public DefaultTinkerElementTraversal(final Element element, final TinkerGraph graph) {
        super(graph);
        this.addStep(new StartStep<>(this, element));
    }
}
