package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
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

    public Strategy strategy() {
        return this.strategy;
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
        return new StrategyWrappedEdge(strategy().compose(
                s -> s.getGrapheStrategy(graphContext),
                this.baseGraph::e).apply(id), this);
    }

    @Override
    public GraphTraversal<Vertex, Vertex> V() {
        return strategy().compose(s -> s.getVStrategy(graphContext), this.baseGraph::V).get();
    }

    @Override
    public GraphTraversal<Edge, Edge> E() {
        return strategy().compose(s -> s.getEStrategy(graphContext), this.baseGraph::E).get();
    }

    @Override
    public <T extends Traversal> T traversal(final Class<T> traversalClass) {
        return this.baseGraph.traversal(traversalClass);
    }

    @Override
    public <C extends GraphComputer> C compute(final Class<C>... graphComputerClass) {
        return this.baseGraph.compute(graphComputerClass);
    }

    @Override
    public Transaction tx() {
        return this.baseGraph.tx();
    }

    @Override
    public Variables variables() {
        return this.baseGraph.variables();
    }

    @Override
    public Features getFeatures() {
        return this.baseGraph.getFeatures();
    }

    @Override
    public void close() throws Exception {
        this.baseGraph.close();
    }

	@Override
	public String toString() {
		final GraphStrategy strategy = this.strategy.getGraphStrategy().orElse(DoNothingGraphStrategy.INSTANCE);
		return String.format("[%s[%s]]", strategy, baseGraph.toString());
	}
}
