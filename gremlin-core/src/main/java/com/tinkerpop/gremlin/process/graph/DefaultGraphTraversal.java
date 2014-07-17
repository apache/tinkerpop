package com.tinkerpop.gremlin.process.graph;

import com.tinkerpop.gremlin.process.graph.strategy.DedupOptimizerStrategy;
import com.tinkerpop.gremlin.process.graph.strategy.IdentityReductionStrategy;
import com.tinkerpop.gremlin.process.graph.strategy.SideEffectCapStrategy;
import com.tinkerpop.gremlin.process.graph.strategy.UnrollJumpStrategy;
import com.tinkerpop.gremlin.process.util.DefaultTraversal;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultGraphTraversal<S, E> extends DefaultTraversal<S, E> implements GraphTraversal<S, E> {

    public DefaultGraphTraversal() {
        super();
        this.traversalStrategies.register(new DedupOptimizerStrategy());
        this.traversalStrategies.register(new IdentityReductionStrategy());
        this.traversalStrategies.register(new SideEffectCapStrategy());
        this.traversalStrategies.register(new UnrollJumpStrategy());
    }
}
