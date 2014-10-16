package com.tinkerpop.gremlin.process.graph.util;

import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.strategy.GraphTraversalStrategyRegistry;
import com.tinkerpop.gremlin.process.util.DefaultTraversal;
import com.tinkerpop.gremlin.structure.Graph;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultGraphTraversal<S, E> extends DefaultTraversal<S, E> implements GraphTraversal<S, E> {

    public DefaultGraphTraversal() {
        GraphTraversalStrategyRegistry.populate(this.strategies);
    }

    public DefaultGraphTraversal(final Graph graph) {
        this();
        this.sideEffects().setGraph(graph);
    }
}
