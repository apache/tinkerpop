package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Transaction;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.strategy.process.graph.StrategyWrappedGraphTraversal;
import com.tinkerpop.gremlin.structure.strategy.process.graph.StrategyWrappedTraversal;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.structure.util.wrapped.WrappedGraph;
import com.tinkerpop.gremlin.util.function.FunctionUtils;
import org.apache.commons.configuration.Configuration;

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
        if (baseGraph instanceof StrategyWrapped) throw new IllegalArgumentException(
                String.format("The graph %s is already StrategyWrapped and must be a base Graph", baseGraph));

        this.baseGraph = baseGraph;
        this.graphContext = new Strategy.Context<>(baseGraph, this);
    }

    /**
     * Gets the underlying base {@link Graph} that is being hosted within this wrapper.
     */
    @Override
    public Graph getBaseGraph() {
        return this.baseGraph;
    }

    /**
     * Gets the strategy hosted within the wrapper.
     */
    public Strategy getStrategy() {
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
        return new StrategyWrappedVertex(getStrategy().compose(
                s -> s.getGraphvStrategy(graphContext),
                this.baseGraph::v).apply(id), this);
    }

    @Override
    public Edge e(final Object id) {
        return new StrategyWrappedEdge(getStrategy().compose(
                s -> s.getGrapheStrategy(graphContext),
                this.baseGraph::e).apply(id), this);
    }

    @Override
    public GraphTraversal<Vertex, Vertex> V() {
        final GraphTraversal<Vertex, Vertex> traversal = new StrategyWrappedGraphTraversal<>(Vertex.class, getStrategy().compose(
                s -> s.getGraphVStrategy(graphContext),
                this.baseGraph::V).get(), this);
        return traversal;
    }

    @Override
    public GraphTraversal<Edge, Edge> E() {
        return new StrategyWrappedGraphTraversal<>(Edge.class, getStrategy().compose(
                s -> s.getGraphEStrategy(graphContext),
                this.baseGraph::E).get(), this);
    }

    @Override
    public <S> GraphTraversal<S, S> of() {
        return new StrategyWrappedTraversal<>(this);
    }

    @Override
    public <T extends Traversal<S, S>, S> T of(final Class<T> traversalClass) {
        return this.baseGraph.of(traversalClass);  // TODO: wrap the users traversal in StrategyWrappedTraversal
    }

    @Override
    public GraphComputer compute(final Class... graphComputerClass) {
        return this.baseGraph.compute(graphComputerClass);
    }

    @Override
    public Transaction tx() {
        return this.baseGraph.tx();
    }

    @Override
    public Variables variables() {
        return new StrategyWrappedVariables(this.baseGraph.variables(), this);
    }

    @Override
    public Configuration configuration() {
        return this.baseGraph.configuration();
    }

    @Override
    public Features features() {
        return this.baseGraph.features();
    }

    @Override
    public void close() throws Exception {
        // compose function doesn't seem to want to work here even though it works with other Supplier<Void>
        // strategy functions. maybe the "throws Exception" is hosing it up.......
        if (strategy.getGraphStrategy().isPresent()) {
            strategy.getGraphStrategy().get().getGraphCloseStrategy(this.graphContext).apply(FunctionUtils.wrapSupplier(() -> {
                baseGraph.close();
                return null;
            })).get();
        } else
            baseGraph.close();
    }

    @Override
    public String toString() {
        final GraphStrategy strategy = this.strategy.getGraphStrategy().orElse(GraphStrategy.DefaultGraphStrategy.INSTANCE);
        return StringFactory.graphStrategyString(strategy, this.baseGraph);
    }
}
