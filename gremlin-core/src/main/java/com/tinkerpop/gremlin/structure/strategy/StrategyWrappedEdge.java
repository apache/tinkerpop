package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.wrapped.WrappedEdge;
import com.tinkerpop.gremlin.util.StreamFactory;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class StrategyWrappedEdge extends StrategyWrappedElement implements Edge, StrategyWrapped, WrappedEdge<Edge> {
    private final Edge baseEdge;
    private final Strategy.Context<StrategyWrappedEdge> strategyContext;
    private final StrategyWrappedEdgeIterators iterators;

    public StrategyWrappedEdge(final Edge baseEdge, final StrategyWrappedGraph strategyWrappedGraph) {
        super(baseEdge, strategyWrappedGraph);
        this.strategyContext = new Strategy.Context<>(strategyWrappedGraph.getBaseGraph(), this);
        this.baseEdge = baseEdge;
        this.iterators = new StrategyWrappedEdgeIterators();
    }

    @Override
    public Edge.Iterators iterators() {
        return this.iterators;
    }

    @Override
    public Object id() {
        return this.strategyWrappedGraph.strategy().compose(
                s -> s.getEdgeIdStrategy(strategyContext),
                this.baseEdge::id).get();
    }

    @Override
    public String label() {
        return this.strategyWrappedGraph.strategy().compose(
                s -> s.getEdgeLabelStrategy(strategyContext),
                this.baseEdge::label).get();
    }

    @Override
    public <V> V value(final String key) throws NoSuchElementException {
        return this.strategyWrappedGraph.strategy().compose(
                s -> s.<V>getEdgeValueStrategy(strategyContext),
                this.baseEdge::value).apply(key);
    }

    @Override
    public Set<String> keys() {
        return this.strategyWrappedGraph.strategy().compose(
                s -> s.getEdgeKeysStrategy(strategyContext),
                this.baseEdge::keys).get();
    }

    @Override
    public Edge getBaseEdge() {
        return this.baseEdge;
    }

    @Override
    public <V> Property<V> property(final String key) {
        return new StrategyWrappedProperty<>(this.strategyWrappedGraph.strategy().compose(
                s -> s.<V>getEdgeGetPropertyStrategy(strategyContext),
                this.baseEdge::property).apply(key), this.strategyWrappedGraph);
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        return new StrategyWrappedProperty<>(this.strategyWrappedGraph.strategy().compose(
                s -> s.<V>getEdgePropertyStrategy(strategyContext),
                this.baseEdge::property).apply(key, value), this.strategyWrappedGraph);
    }

    @Override
    public void remove() {
        this.strategyWrappedGraph.strategy().compose(
                s -> s.getRemoveEdgeStrategy(strategyContext),
                () -> {
                    this.baseEdge.remove();
                    return null;
                }).get();
    }

    @Override
    public GraphTraversal<Edge, Edge> start() {
        return applyStrategy(this.baseEdge.start());
    }

    public class StrategyWrappedEdgeIterators extends StrategyWrappedElementIterators implements Edge.Iterators {
        @Override
        public Iterator<Vertex> vertices(final Direction direction) {
            return new StrategyWrappedVertex.StrategyWrappedVertexIterator(baseEdge.iterators().vertices(direction), strategyWrappedGraph);
        }

        @Override
        public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
            return StreamFactory.stream(strategyWrappedGraph.strategy().compose(
                    s -> s.<V>getEdgeIteratorsPropertiesStrategy(strategyContext),
                    (String[] pks) -> ((Edge) baseElement).iterators().properties(pks)).apply(propertyKeys))
                    .map(property -> (Property<V>) new StrategyWrappedProperty<>(property, strategyWrappedGraph)).iterator();
        }

        @Override
        public <V> Iterator<Property<V>> hiddens(final String... propertyKeys) {
            return StreamFactory.stream(strategyWrappedGraph.strategy().compose(
                    s -> s.<V>getEdgeIteratorsHiddensStrategy(strategyContext),
                    (String[] pks) -> ((Edge) baseElement).iterators().hiddens(pks)).apply(propertyKeys))
                    .map(property -> (Property<V>) new StrategyWrappedProperty<>(property, strategyWrappedGraph)).iterator();
        }
    }

    public static class StrategyWrappedEdgeIterator implements Iterator<Edge> {
        private final Iterator<Edge> edges;
        private final StrategyWrappedGraph strategyWrappedGraph;

        public StrategyWrappedEdgeIterator(final Iterator<Edge> itty,
                                           final StrategyWrappedGraph strategyWrappedGraph) {
            this.edges = itty;
            this.strategyWrappedGraph = strategyWrappedGraph;
        }

        @Override
        public boolean hasNext() {
            return this.edges.hasNext();
        }

        @Override
        public Edge next() {
            return new StrategyWrappedEdge(this.edges.next(), this.strategyWrappedGraph);
        }
    }
}
