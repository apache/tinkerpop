package com.tinkerpop.gremlin.process.graph;

import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.graph.step.map.StartStep;
import com.tinkerpop.gremlin.process.graph.strategy.DedupOptimizerTraversalStrategy;
import com.tinkerpop.gremlin.process.graph.strategy.IdentityOptimizerTraversalStrategy;
import com.tinkerpop.gremlin.process.graph.strategy.SideEffectCapTraversalStrategy;
import com.tinkerpop.gremlin.process.util.DefaultTraversal;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultGraphTraversal<S, E> extends DefaultTraversal<S, E> implements GraphTraversal<S, E> {

    public DefaultGraphTraversal() {
        super();
        this.traversalStrategies.register(new DedupOptimizerTraversalStrategy());
        this.traversalStrategies.register(new IdentityOptimizerTraversalStrategy());
        this.traversalStrategies.register(new SideEffectCapTraversalStrategy());
    }
}
