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
    public Graph graph() {
        return this.strategyWrappedGraph.getStrategy().compose(
                s -> s.getEdgeGraphStrategy(strategyContext),
                () -> this.strategyWrappedGraph).get();
    }

    @Override
    public Object id() {
        return this.strategyWrappedGraph.getStrategy().compose(
                s -> s.getEdgeIdStrategy(strategyContext),
                this.baseEdge::id).get();
    }

    @Override
    public String label() {
        return this.strategyWrappedGraph.getStrategy().compose(
                s -> s.getEdgeLabelStrategy(strategyContext),
                this.baseEdge::label).get();
    }

    @Override
    public <V> V value(final String key) throws NoSuchElementException {
        return this.strategyWrappedGraph.getStrategy().compose(
                s -> s.<V>getEdgeValueStrategy(strategyContext),
                this.baseEdge::value).apply(key);
    }

    @Override
    public Set<String> keys() {
        return this.strategyWrappedGraph.getStrategy().compose(
                s -> s.getEdgeKeysStrategy(strategyContext),
                this.baseEdge::keys).get();
    }

    @Override
    public Set<String> hiddenKeys() {
        return this.strategyWrappedGraph.getStrategy().compose(
                s -> s.getEdgeHiddenKeysStrategy(strategyContext),
                this.baseEdge::hiddenKeys).get();
    }

    @Override
    public Edge getBaseEdge() {
        return this.baseEdge;
    }

    @Override
    public <V> Property<V> property(final String key) {
        return new StrategyWrappedProperty<>(this.strategyWrappedGraph.getStrategy().compose(
                s -> s.<V>getEdgeGetPropertyStrategy(strategyContext),
                this.baseEdge::property).apply(key), this.strategyWrappedGraph);
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        return new StrategyWrappedProperty<>(this.strategyWrappedGraph.getStrategy().compose(
                s -> s.<V>getEdgePropertyStrategy(strategyContext),
                this.baseEdge::property).apply(key, value), this.strategyWrappedGraph);
    }

    @Override
    public void remove() {
        this.strategyWrappedGraph.getStrategy().compose(
                s -> s.getRemoveEdgeStrategy(strategyContext),
                () -> {
                    this.baseEdge.remove();
                    return null;
                }).get();
    }

    @Override
    public GraphTraversal<Edge, Edge> start() {
        return new StrategyWrappedElementTraversal<>(this, strategyWrappedGraph);
    }

    @Override
    public String toString() {
        final GraphStrategy strategy = strategyWrappedGraph.getStrategy().getGraphStrategy().orElse(GraphStrategy.DefaultGraphStrategy.INSTANCE);
        return StringFactory.graphStrategyEdgeString(strategy, this.baseEdge);
    }

    public class StrategyWrappedEdgeIterators implements Edge.Iterators {
        @Override
        public Iterator<Vertex> vertexIterator(final Direction direction) {
            return new StrategyWrappedVertex.StrategyWrappedVertexIterator(strategyWrappedGraph.getStrategy().compose(
                    s -> s.getEdgeIteratorsVerticesStrategy(strategyContext),
                    (Direction d) -> baseEdge.iterators().vertexIterator(d)).apply(direction), strategyWrappedGraph);
        }

        @Override
        public <V> Iterator<V> valueIterator(final String... propertyKeys) {
            return strategyWrappedGraph.getStrategy().compose(
                    s -> s.<V>getEdgeIteratorsValuesStrategy(strategyContext),
                    (String[] pks) -> baseEdge.iterators().valueIterator(pks)).apply(propertyKeys);
        }

        @Override
        public <V> Iterator<V> hiddenValueIterator(final String... propertyKeys) {
            return strategyWrappedGraph.getStrategy().compose(
                    s -> s.<V>getEdgeIteratorsHiddenValuesStrategy(strategyContext),
                    (String[] pks) -> baseEdge.iterators().hiddenValueIterator(pks)).apply(propertyKeys);
        }

        @Override
        public <V> Iterator<Property<V>> propertyIterator(final String... propertyKeys) {
            return StreamFactory.stream(strategyWrappedGraph.getStrategy().compose(
                    s -> s.<V>getEdgeIteratorsPropertiesStrategy(strategyContext),
                    (String[] pks) -> baseEdge.iterators().propertyIterator(pks)).apply(propertyKeys))
                    .map(property -> (Property<V>) new StrategyWrappedProperty<>(property, strategyWrappedGraph)).iterator();
        }

        @Override
        public <V> Iterator<Property<V>> hiddenPropertyIterator(final String... propertyKeys) {
            return StreamFactory.stream(strategyWrappedGraph.getStrategy().compose(
                    s -> s.<V>getEdgeIteratorsHiddensStrategy(strategyContext),
                    (String[] pks) -> baseEdge.iterators().hiddenPropertyIterator(pks)).apply(propertyKeys))
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
