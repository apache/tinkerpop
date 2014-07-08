package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.function.SConsumer;

import java.util.Iterator;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class StrategyWrappedVertex extends StrategyWrappedElement implements Vertex, StrategyWrapped {
    private final Vertex baseVertex;
    private final Strategy.Context<StrategyWrappedVertex> strategyContext;

    public StrategyWrappedVertex(final Vertex baseVertex, final StrategyWrappedGraph strategyWrappedGraph) {
        super(baseVertex, strategyWrappedGraph);
        this.strategyContext = new Strategy.Context<>(strategyWrappedGraph.getBaseGraph(), this);
        this.baseVertex = baseVertex;
    }

    public Vertex getBaseVertex() {
        return this.baseVertex;
    }

    @Override
    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        final Vertex baseInVertex = (inVertex instanceof StrategyWrappedVertex) ? ((StrategyWrappedVertex) inVertex).getBaseVertex() : inVertex;
        return new StrategyWrappedEdge(this.strategyWrappedGraph.strategy().compose(
                s -> s.getAddEdgeStrategy(strategyContext),
                this.baseVertex::addEdge)
                .apply(label, baseInVertex, keyValues), this.strategyWrappedGraph);
    }

    @Override
    public Iterator<Vertex> toIterator(final Direction direction, final int branchFactor, final String... labels) {
        return this.baseVertex.toIterator(direction, branchFactor, labels);
    }

    @Override
    public Iterator<Edge> toEIterator(final Direction direction, final int branchFactor, final String... labels) {
        return this.baseVertex.toEIterator(direction, branchFactor, labels);
    }

    @Override
    public GraphTraversal<Vertex, Vertex> to(final Direction direction, final int branchFactor, final String... labels) {
        return applyStrategy(this.getBaseVertex().to(direction, branchFactor, labels));
    }

    @Override
    public GraphTraversal<Vertex, Edge> toE(final Direction direction, final int branchFactor, final String... labels) {
        return applyStrategy(this.getBaseVertex().toE(direction, branchFactor, labels));
    }

    @Override
    public GraphTraversal<Vertex, Vertex> start() {
        return applyStrategy(this.baseVertex.start());
    }

    @Override
    public GraphTraversal<Vertex, Vertex> as(final String as) {
        return applyStrategy(this.baseVertex.as(as));
    }

    @Override
    public GraphTraversal<Vertex, Vertex> with(final Object... variableValues) {
        return applyStrategy(this.baseVertex.with(variableValues));
    }

    @Override
    public GraphTraversal<Vertex, Vertex> sideEffect(final SConsumer<Traverser<Vertex>> consumer) {
        return applyStrategy(this.baseVertex.sideEffect(consumer));
    }
}
