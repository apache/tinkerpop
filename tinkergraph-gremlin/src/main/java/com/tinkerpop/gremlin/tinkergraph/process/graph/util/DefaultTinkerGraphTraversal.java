package com.tinkerpop.gremlin.tinkergraph.process.graph.util;

import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.graph.strategy.GraphTraversalStrategyRegistry;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.util.DefaultTraversalStrategies;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.tinkergraph.process.graph.step.sideEffect.TinkerGraphStep;
import com.tinkerpop.gremlin.tinkergraph.process.graph.strategy.TinkerGraphStepStrategy;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultTinkerGraphTraversal<S, E> extends DefaultGraphTraversal<S, E> {

    static {
        final DefaultTraversalStrategies traversalStrategies = new DefaultTraversalStrategies();
        GraphTraversalStrategyRegistry.instance().getTraversalStrategies().forEach(traversalStrategies::addStrategy);
        traversalStrategies.addStrategy(TinkerGraphStepStrategy.instance());
        TraversalStrategies.GlobalCache.registerStrategies(DefaultTinkerGraphTraversal.class, traversalStrategies);
    }

    public DefaultTinkerGraphTraversal() {
        super();
    }

    public DefaultTinkerGraphTraversal(final TinkerGraph graph, final Class<? extends Element> returnClass, final Object... ids) {
        super(graph);
        this.addStep(new TinkerGraphStep<>(this, graph, returnClass, ids));
    }
}
