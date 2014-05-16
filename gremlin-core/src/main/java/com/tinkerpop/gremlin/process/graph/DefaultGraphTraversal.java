package com.tinkerpop.gremlin.process.graph;

import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.graph.map.StartStep;
import com.tinkerpop.gremlin.process.strategy.DedupOptimizerTraversalStrategy;
import com.tinkerpop.gremlin.process.strategy.IdentityOptimizerTraversalStrategy;
import com.tinkerpop.gremlin.process.strategy.SideEffectCapOptimizerTraversalStrategy;
import com.tinkerpop.gremlin.process.util.DefaultTraversal;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultGraphTraversal<S, E> extends DefaultTraversal<S, E> implements GraphTraversal<S, E> {

    public DefaultGraphTraversal() {
        super();
        this.traversalStrategies.register(new DedupOptimizerTraversalStrategy());
        this.traversalStrategies.register(new IdentityOptimizerTraversalStrategy());
        this.traversalStrategies.register(new SideEffectCapOptimizerTraversalStrategy());
    }

    public GraphTraversal<S, E> submit(final TraversalEngine engine) {
        final GraphTraversal<S, E> traversal = new DefaultGraphTraversal<>();
        traversal.addStep(new StartStep<>(traversal, engine.execute(this)));
        return traversal;
    }
}
