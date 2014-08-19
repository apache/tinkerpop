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

    public StrategyWrappedEdge(final Edge baseEdge, final StrategyWrappedGraph strategyWrappedGraph) {
        super(baseEdge, strategyWrappedGraph);
        this.baseEdge = baseEdge;
    }

    public Edge getBaseEdge() {
        return this.baseEdge;
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction) {
        return this.baseEdge.vertices(direction);
    }

    @Override
    public GraphTraversal<Edge, Edge> start() {
        return applyStrategy(this.baseEdge.start());
    }
}
