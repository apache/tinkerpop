package com.tinkerpop.gremlin.process.graph.util;

import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.strategy.DedupOptimizerStrategy;
import com.tinkerpop.gremlin.process.graph.strategy.IdentityReductionStrategy;
import com.tinkerpop.gremlin.process.graph.strategy.MatchWhereStrategy;
import com.tinkerpop.gremlin.process.graph.strategy.SideEffectCapStrategy;
import com.tinkerpop.gremlin.process.graph.strategy.UntilStrategy;
import com.tinkerpop.gremlin.process.util.DefaultTraversal;
import com.tinkerpop.gremlin.structure.Graph;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultGraphTraversal<S, E> extends DefaultTraversal<S, E> implements GraphTraversal<S, E> {

    public DefaultGraphTraversal() {
        super();
        this.strategies.register(UntilStrategy.instance());
        this.strategies.register(DedupOptimizerStrategy.instance());
        this.strategies.register(IdentityReductionStrategy.instance());
        this.strategies.register(SideEffectCapStrategy.instance());
        this.strategies.register(MatchWhereStrategy.instance());
    }

    public DefaultGraphTraversal(final Graph graph) {
        this();
        this.sideEffects().setGraph(graph);
    }
}
