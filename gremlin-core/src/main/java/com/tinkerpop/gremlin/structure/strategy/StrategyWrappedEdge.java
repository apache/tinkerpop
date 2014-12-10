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
public final class StrategyWrappedEdge extends StrategyWrappedElement implements Edge, Edge.Iterators, StrategyWrapped, WrappedEdge<Edge> {

    private final Strategy.Context<StrategyWrappedEdge> strategyContext;

    public StrategyWrappedEdge(final Edge baseEdge, final StrategyWrappedGraph strategyWrappedGraph) {
        super(baseEdge, strategyWrappedGraph);
        this.strategyContext = new Strategy.Context<>(strategyWrappedGraph, this);
    }

    public Strategy.Context<StrategyWrappedEdge> getStrategyContext() {
        return strategyContext;
    }

    @Override
    public Edge.Iterators iterators() {
        return this;
    }

    @Override
    public Graph graph() {
        return this.strategyWrappedGraph.getStrategy().compose(
                s -> s.getEdgeGraphStrategy(this.strategyContext),
                () -> this.strategyWrappedGraph).get();
    }

    @Override
    public Object id() {
        return this.strategyWrappedGraph.getStrategy().compose(
                s -> s.getEdgeIdStrategy(this.strategyContext),
                this.getBaseEdge()::id).get();
    }

    @Override
    public String label() {
        return this.strategyWrappedGraph.getStrategy().compose(
                s -> s.getEdgeLabelStrategy(this.strategyContext),
                this.getBaseEdge()::label).get();
    }

    @Override
    public <V> V value(final String key) throws NoSuchElementException {
        return this.strategyWrappedGraph.getStrategy().compose(
                s -> s.<V>getEdgeValueStrategy(this.strategyContext),
                this.getBaseEdge()::value).apply(key);
    }

    @Override
    public Set<String> keys() {
        return this.strategyWrappedGraph.getStrategy().compose(
                s -> s.getEdgeKeysStrategy(this.strategyContext),
                this.getBaseEdge()::keys).get();
    }

    @Override
    public Edge getBaseEdge() {
        return (Edge) this.baseElement;
    }

    @Override
    public <V> Property<V> property(final String key) {
        return new StrategyWrappedProperty<>(this.strategyWrappedGraph.getStrategy().compose(
                s -> s.<V>getEdgeGetPropertyStrategy(this.strategyContext),
                this.getBaseEdge()::property).apply(key), this.strategyWrappedGraph);
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        return new StrategyWrappedProperty<>(this.strategyWrappedGraph.getStrategy().compose(
                s -> s.<V>getEdgePropertyStrategy(this.strategyContext),
                this.getBaseEdge()::property).apply(key, value), this.strategyWrappedGraph);
    }

    @Override
    public void remove() {
        this.strategyWrappedGraph.getStrategy().compose(
                s -> s.getRemoveEdgeStrategy(this.strategyContext),
                () -> {
                    this.getBaseEdge().remove();
                    return null;
                }).get();
    }

    @Override
    public GraphTraversal<Edge, Edge> start() {
        return new StrategyWrappedElementTraversal<>(this, this.strategyWrappedGraph);
    }

    @Override
    public String toString() {
        return StringFactory.graphStrategyElementString(this);
    }


    @Override
    public Iterator<Vertex> vertexIterator(final Direction direction) {
        return new StrategyWrappedVertex.StrategyWrappedVertexIterator(this.strategyWrappedGraph.getStrategy().compose(
                s -> s.getEdgeIteratorsVerticesStrategy(this.strategyContext),
                (Direction d) -> this.getBaseEdge().iterators().vertexIterator(d)).apply(direction), this.strategyWrappedGraph);
    }

    @Override
    public <V> Iterator<V> valueIterator(final String... propertyKeys) {
        return this.strategyWrappedGraph.getStrategy().compose(
                s -> s.<V>getEdgeIteratorsValuesStrategy(this.strategyContext),
                (String[] pks) -> this.getBaseEdge().iterators().valueIterator(pks)).apply(propertyKeys);
    }

    @Override
    public <V> Iterator<Property<V>> propertyIterator(final String... propertyKeys) {
        return IteratorUtils.map(this.strategyWrappedGraph.getStrategy().compose(
                        s -> s.<V>getEdgeIteratorsPropertiesStrategy(this.strategyContext),
                        (String[] pks) -> this.getBaseEdge().iterators().propertyIterator(pks)).apply(propertyKeys),
                property -> new StrategyWrappedProperty<>(property, this.strategyWrappedGraph));
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
