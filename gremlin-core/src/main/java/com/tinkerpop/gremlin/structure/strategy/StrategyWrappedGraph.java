package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Transaction;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.wrapped.WrappedGraph;

import java.util.Optional;

/**
 * A wrapper class for {@link Graph} instances that host and apply a {@link GraphStrategy}.  The wrapper implements
 * {@link Graph} itself and intercepts calls made to the hosted instance and then applies the strategy.  Methods
 * that return an extension of {@link com.tinkerpop.gremlin.structure.Element} or a
 * {@link com.tinkerpop.gremlin.structure.Property} will be automatically wrapped in a {@link StrategyWrapped}
 * implementation.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class StrategyWrappedGraph implements Graph, StrategyWrapped, WrappedGraph<Graph> {
    private final Graph baseGraph;
    protected Strategy strategy = new Strategy.Simple();
    private Strategy.Context<StrategyWrappedGraph> graphContext;

    public StrategyWrappedGraph(final Graph baseGraph) {
        this.baseGraph = baseGraph;
        this.graphContext = new Strategy.Context<>(baseGraph, this);
    }

    /**
     * Gets the underlying base {@link Graph} that is being hosted within this wrapper.
     */
    public Graph getBaseGraph() {
        return this.baseGraph;
    }

    /**
     * Gets the strategy hosted within the wrapper.
     */
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
        return applyStrategy(baseGraph.V());
    }

    @Override
    public GraphTraversal<Edge, Edge> E() {
        return applyStrategy(baseGraph.E());
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
    public <V extends Variables> V variables() {
        return (V) new StrategyWrappedVariables(this.baseGraph.variables(), this);
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
        final GraphStrategy strategy = this.strategy.getGraphStrategy().orElse(GraphStrategy.DoNothingGraphStrategy.INSTANCE);
        return String.format("[%s[%s]]", strategy, baseGraph.toString());
    }

    private <S, E> GraphTraversal<S, E> applyStrategy(final GraphTraversal<S, E> traversal) {
        traversal.strategies().register(new StrategyWrappedTraversalStrategy(this));
        this.strategy.getGraphStrategy().ifPresent(s -> s.applyStrategyToTraversal(traversal));
        return traversal;
    }
}
