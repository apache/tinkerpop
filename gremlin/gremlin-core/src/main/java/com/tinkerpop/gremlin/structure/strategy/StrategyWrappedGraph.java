package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.olap.GraphComputer;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Transaction;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.Optional;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class StrategyWrappedGraph implements Graph, StrategyWrapped {
    private final Graph baseGraph;
    protected Strategy strategy = new Strategy.Simple();
    private Strategy.Context<StrategyWrappedGraph> graphContext;

    public StrategyWrappedGraph(final Graph baseGraph) {
        this.baseGraph = baseGraph;
        this.graphContext = new Strategy.Context<>(baseGraph, this);
    }

    public Graph getBaseGraph() {
        return this.baseGraph;
    }

    @Override
    public Vertex addVertex(final Object... keyValues) {
        final Optional<Vertex> v = Optional.ofNullable(strategy.compose(
                s -> s.getAddVertexStrategy(graphContext),
                this.baseGraph::addVertex).apply(keyValues));
        return v.isPresent() ? new StrategyWrappedVertex(v.get(), this) : null;
    }

    @Override
    public Vertex v(final Object id) {
        return new StrategyWrappedVertex(strategy().compose(
                s -> s.getGraphvStrategy(graphContext),
                this.baseGraph::v).apply(id), this);
    }

    @Override
    public Edge e(final Object id) {
        return strategy().compose(
                s -> s.getGrapheStrategy(graphContext),
                this.baseGraph::e).apply(id);
    }

    @Override
    public Traversal<Vertex, Vertex> V() {
        return this.baseGraph.V();
    }

    @Override
    public Traversal<Edge, Edge> E() {
        return this.baseGraph.E();
    }

    @Override
    public GraphComputer compute() {
        return this.baseGraph.compute();
    }

    @Override
    public Transaction tx() {
        return this.baseGraph.tx();
    }

    public Strategy strategy() {
        return this.strategy;
    }

    @Override
    public Annotations annotations() {
        return this.baseGraph.annotations();
    }

    @Override
    public Features getFeatures() {
        return this.baseGraph.getFeatures();
    }

    @Override
    public void close() throws Exception {
        this.baseGraph.close();
    }
}
