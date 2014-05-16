package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class StrategyWrappedEdge extends StrategyWrappedElement implements Edge, StrategyWrapped {
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

    public GraphTraversal<Edge, Vertex> inV() {
        return this.baseEdge.inV();
    }

    public GraphTraversal<Edge, Vertex> outV() {
        return this.baseEdge.outV();
    }

    public GraphTraversal<Edge, Vertex> bothV() {
        return this.baseEdge.bothV();
    }
}
