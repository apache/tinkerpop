package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.wrapped.WrappedEdge;

import java.util.Iterator;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class StrategyWrappedEdge extends StrategyWrappedElement implements Edge, StrategyWrapped, WrappedEdge<Edge> {
    private final Edge baseEdge;
    private final Strategy.Context<StrategyWrappedEdge> strategyContext;

    public StrategyWrappedEdge(final Edge baseEdge, final StrategyWrappedGraph strategyWrappedGraph) {
        super(baseEdge, strategyWrappedGraph);
        this.baseEdge = baseEdge;
        strategyContext = new Strategy.Context<>(this.strategyWrappedGraph.getBaseGraph(), this);
    }

    public Edge getBaseEdge() {
        return this.baseEdge;
    }

    public Iterator<Vertex> vertices(final Direction direction) {
        return this.baseEdge.vertices(direction);
    }

    public GraphTraversal<Edge, Vertex> toV(final Direction direction) {
        return applyStrategy(this.baseEdge.toV(direction));
    }
}
