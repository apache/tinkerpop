package com.tinkerpop.gremlin.structure.strategy.process.graph;

import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.process.graph.strategy.GraphTraversalStrategyRegistry;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.util.DefaultTraversalStrategies;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.strategy.StrategyWrappedElement;
import com.tinkerpop.gremlin.structure.strategy.StrategyWrappedGraph;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StrategyWrappedElementTraversal<A extends Element> extends DefaultGraphTraversal<A, A> {

    static {
        final DefaultTraversalStrategies traversalStrategies = new DefaultTraversalStrategies();
        GraphTraversalStrategyRegistry.instance().toList().forEach(traversalStrategies::register);
        TraversalStrategies.GlobalCache.registerStrategies(StrategyWrappedElementTraversal.class, traversalStrategies);
    }

    public StrategyWrappedElementTraversal(final StrategyWrappedElement element, final StrategyWrappedGraph graph) {
        super(graph);
        this.addStep(new StartStep<>(this, element));
    }
}
