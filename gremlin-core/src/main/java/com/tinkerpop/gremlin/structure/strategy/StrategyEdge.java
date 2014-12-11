package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.strategy.process.graph.StrategyWrappedElementTraversal;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.structure.util.wrapped.WrappedEdge;
import com.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class StrategyEdge extends StrategyElement implements Edge, Edge.Iterators, StrategyWrapped, WrappedEdge<Edge> {

    private final StrategyContext<StrategyEdge> strategyContext;

    public StrategyEdge(final Edge baseEdge, final StrategyGraph strategyGraph) {
        super(baseEdge, strategyGraph);
        this.strategyContext = new StrategyContext<>(strategyGraph, this);
    }

    public StrategyContext<StrategyEdge> getStrategyContext() {
        return strategyContext;
    }

    @Override
    public Edge.Iterators iterators() {
        return this;
    }

    @Override
    public Graph graph() {
        return this.strategyGraph.compose(
                s -> s.getEdgeGraphStrategy(this.strategyContext),
                () -> this.strategyGraph).get();
    }

    @Override
    public Object id() {
        return this.strategyGraph.compose(
                s -> s.getEdgeIdStrategy(this.strategyContext),
                this.getBaseEdge()::id).get();
    }

    @Override
    public String label() {
        return this.strategyGraph.compose(
                s -> s.getEdgeLabelStrategy(this.strategyContext),
                this.getBaseEdge()::label).get();
    }

    @Override
    public <V> V value(final String key) throws NoSuchElementException {
        return this.strategyGraph.compose(
                s -> s.<V>getEdgeValueStrategy(this.strategyContext),
                this.getBaseEdge()::value).apply(key);
    }

    @Override
    public Set<String> keys() {
        return this.strategyGraph.compose(
                s -> s.getEdgeKeysStrategy(this.strategyContext),
                this.getBaseEdge()::keys).get();
    }

    @Override
    public Edge getBaseEdge() {
        return (Edge) this.baseElement;
    }

    @Override
    public <V> Property<V> property(final String key) {
        return new StrategyProperty<>(this.strategyGraph.compose(
                s -> s.<V>getEdgeGetPropertyStrategy(this.strategyContext),
                this.getBaseEdge()::property).apply(key), this.strategyGraph);
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        return new StrategyProperty<>(this.strategyGraph.compose(
                s -> s.<V>getEdgePropertyStrategy(this.strategyContext),
                this.getBaseEdge()::property).apply(key, value), this.strategyGraph);
    }

    @Override
    public void remove() {
        this.strategyGraph.compose(
                s -> s.getRemoveEdgeStrategy(this.strategyContext),
                () -> {
                    this.getBaseEdge().remove();
                    return null;
                }).get();
    }

    @Override
    public GraphTraversal<Edge, Edge> start() {
        return new StrategyWrappedElementTraversal<>(this, this.strategyGraph);
    }

    @Override
    public String toString() {
        return StringFactory.graphStrategyElementString(this);
    }


    @Override
    public Iterator<Vertex> vertexIterator(final Direction direction) {
        return new StrategyVertex.StrategyWrappedVertexIterator(this.strategyGraph.compose(
                s -> s.getEdgeIteratorsVertexIteratorStrategy(this.strategyContext),
                (Direction d) -> this.getBaseEdge().iterators().vertexIterator(d)).apply(direction), this.strategyGraph);
    }

    @Override
    public <V> Iterator<V> valueIterator(final String... propertyKeys) {
        return this.strategyGraph.compose(
                s -> s.<V>getEdgeIteratorsValueIteratorStrategy(this.strategyContext),
                (String[] pks) -> this.getBaseEdge().iterators().valueIterator(pks)).apply(propertyKeys);
    }

    @Override
    public <V> Iterator<Property<V>> propertyIterator(final String... propertyKeys) {
        return IteratorUtils.map(this.strategyGraph.compose(
                        s -> s.<V>getEdgeIteratorsPropertyIteratorStrategy(this.strategyContext),
                        (String[] pks) -> this.getBaseEdge().iterators().propertyIterator(pks)).apply(propertyKeys),
                property -> new StrategyProperty<>(property, this.strategyGraph));
    }


    public static class StrategyWrappedEdgeIterator implements Iterator<Edge> {
        private final Iterator<Edge> edges;
        private final StrategyGraph strategyGraph;

        public StrategyWrappedEdgeIterator(final Iterator<Edge> itty,
                                           final StrategyGraph strategyGraph) {
            this.edges = itty;
            this.strategyGraph = strategyGraph;
        }

        @Override
        public boolean hasNext() {
            return this.edges.hasNext();
        }

        @Override
        public Edge next() {
            return new StrategyEdge(this.edges.next(), this.strategyGraph);
        }
    }
}
