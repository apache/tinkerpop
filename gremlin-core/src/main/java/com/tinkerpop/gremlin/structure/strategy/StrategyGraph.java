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

import java.util.Iterator;
import java.util.Optional;

/**
 * A wrapper class for {@link Graph} instances that host and apply a {@link GraphStrategy}.  The wrapper implements
 * {@link Graph} itself and intercepts calls made to the hosted instance and then applies the strategy.  Methods
 * that return an extension of {@link com.tinkerpop.gremlin.structure.Element} or a
 * {@link com.tinkerpop.gremlin.structure.Property} will be automatically wrapped in a {@link StrategyWrapped}
 * implementation.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class StrategyGraph implements Graph, Graph.Iterators, StrategyWrapped, WrappedGraph<Graph> {
    private final Graph baseGraph;
    private Strategy strategy;
    private Strategy.Context<StrategyGraph> graphContext;

    public StrategyGraph(final Graph baseGraph) {
        this(baseGraph, new Strategy.Simple());
    }

    public StrategyGraph(final Graph baseGraph, final Strategy strategy) {
        if (baseGraph instanceof StrategyWrapped) throw new IllegalArgumentException(
                String.format("The graph %s is already StrategyWrapped and must be a base Graph", baseGraph));
        if (null == strategy) throw new IllegalArgumentException("Strategy cannot be null");

        this.strategy = strategy;
        this.baseGraph = baseGraph;
        this.graphContext = new Strategy.Context<>(this, this);
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

    public Strategy.Context<StrategyGraph> getGraphContext() {
        return this.graphContext;
    }

    @Override
    public Vertex addVertex(final Object... keyValues) {
        final Optional<Vertex> v = Optional.ofNullable(getStrategy().compose(
                s -> s.getAddVertexStrategy(this.graphContext),
                this.baseGraph::addVertex).apply(keyValues));
        return v.isPresent() ? new StrategyVertex(v.get(), this) : null;
    }

    @Override
    public GraphTraversal<Vertex, Vertex> V(final Object... vertexIds) {
        return new StrategyWrappedGraphTraversal<>(Vertex.class, this.strategy.compose(
                s -> s.getGraphVStrategy(this.graphContext),
                this.baseGraph::V).apply(vertexIds), this);
    }

    @Override
    public GraphTraversal<Edge, Edge> E(final Object... edgeIds) {
        return new StrategyWrappedGraphTraversal<>(Edge.class, this.strategy.compose(
                s -> s.getGraphEStrategy(this.graphContext),
                this.baseGraph::E).apply(edgeIds), this);
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
        return new StrategyVariables(this.baseGraph.variables(), this);
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
    public Iterators iterators() {
        return this;
    }

    @Override
    public Iterator<Vertex> vertexIterator(final Object... vertexIds) {
        return new StrategyVertex.StrategyWrappedVertexIterator(getStrategy().compose(s -> s.getGraphIteratorsVertexIteratorStrategy(this.graphContext), this.baseGraph.iterators()::vertexIterator).apply(vertexIds), this);
    }

    @Override
    public Iterator<Edge> edgeIterator(final Object... edgeIds) {
        return new StrategyEdge.StrategyWrappedEdgeIterator(getStrategy().compose(s -> s.getGraphIteratorsEdgeIteratorStrategy(this.graphContext), this.baseGraph.iterators()::edgeIterator).apply(edgeIds), this);
    }

    @Override
    public void close() throws Exception {
        // compose function doesn't seem to want to work here even though it works with other Supplier<Void>
        // strategy functions. maybe the "throws Exception" is hosing it up.......
        if (this.strategy.getGraphStrategy().isPresent()) {
            this.strategy.getGraphStrategy().get().getGraphCloseStrategy(this.graphContext).apply(FunctionUtils.wrapSupplier(() -> {
                baseGraph.close();
                return null;
            })).get();
        } else
            baseGraph.close();
    }

    @Override
    public String toString() {
        final GraphStrategy strategy = this.strategy.getGraphStrategy().orElse(IdentityStrategy.instance());
        return StringFactory.graphStrategyString(strategy, this.baseGraph);
    }
}
