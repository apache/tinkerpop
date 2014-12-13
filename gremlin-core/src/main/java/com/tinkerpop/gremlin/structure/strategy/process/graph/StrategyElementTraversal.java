package com.tinkerpop.gremlin.structure.strategy.process.graph;

import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.process.graph.strategy.GraphTraversalStrategyRegistry;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.util.DefaultTraversalStrategies;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.strategy.StrategyElement;
import com.tinkerpop.gremlin.structure.strategy.StrategyGraph;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StrategyElementTraversal<A extends Element> extends DefaultGraphTraversal<A, A> {

    static {
        final DefaultTraversalStrategies traversalStrategies = new DefaultTraversalStrategies();
        GraphTraversalStrategyRegistry.instance().getTraversalStrategies().forEach(traversalStrategies::addStrategy);
        TraversalStrategies.GlobalCache.registerStrategies(StrategyElementTraversal.class, traversalStrategies);
    }

    public StrategyElementTraversal(final StrategyElement element, final StrategyGraph graph) {
        super(graph);
        this.addStep(new StartStep<>(this, element));
    }
}
