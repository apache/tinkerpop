package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.structure.util.wrapped.WrappedVertex;
import com.tinkerpop.gremlin.util.StreamFactory;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class StrategyWrappedVertex extends StrategyWrappedElement implements Vertex, StrategyWrapped, WrappedVertex<Vertex> {
    private final Vertex baseVertex;
    private final Strategy.Context<StrategyWrappedVertex> strategyContext;
    private final StrategyWrappedVertexIterators iterators;

    public StrategyWrappedVertex(final Vertex baseVertex, final StrategyWrappedGraph strategyWrappedGraph) {
        super(baseVertex, strategyWrappedGraph);
        this.strategyContext = new Strategy.Context<>(strategyWrappedGraph.getBaseGraph(), this);
        this.baseVertex = baseVertex;
        this.iterators = new StrategyWrappedVertexIterators();
    }

    @Override
    public Graph graph() {
        return this.strategyWrappedGraph.strategy().compose(
                s -> s.getVertexGraphStrategy(strategyContext),
                () -> this.strategyWrappedGraph).get();
    }

    @Override
    public Object id() {
        return this.strategyWrappedGraph.strategy().compose(
                s -> s.getVertexIdStrategy(strategyContext),
                this.baseVertex::id).get();
    }

    @Override
    public String label() {
        return this.strategyWrappedGraph.strategy().compose(
                s -> s.getVertexLabelStrategy(strategyContext),
                this.baseVertex::label).get();
    }

    @Override
    public Set<String> keys() {
        return this.strategyWrappedGraph.strategy().compose(
                s -> s.getVertexKeysStrategy(strategyContext),
                this.baseVertex::keys).get();
    }

    @Override
    public Set<String> hiddenKeys() {
        return this.strategyWrappedGraph.strategy().compose(
                s -> s.getVertexHiddenKeysStrategy(strategyContext),
                this.baseVertex::hiddenKeys).get();
    }

    @Override
    public Vertex.Iterators iterators() {
        return this.iterators;
    }

    @Override
    public <V> V value(final String key) throws NoSuchElementException {
        return this.strategyWrappedGraph.strategy().compose(
                s -> s.<V>getVertexValueStrategy(strategyContext),
                this.baseVertex::value).apply(key);
    }

    @Override
    public void remove() {
        this.strategyWrappedGraph.strategy().compose(
                s -> s.getRemoveVertexStrategy(strategyContext),
                () -> {
                    this.baseVertex.remove();
                    return null;
                }).get();
    }

    @Override
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
    public <V> VertexProperty<V> property(final String key, final V value) {
        return new StrategyWrappedVertexProperty<>(this.strategyWrappedGraph.strategy().compose(
                s -> s.<V>getVertexPropertyStrategy(strategyContext),
                this.baseVertex::property).apply(key, value), this.strategyWrappedGraph);
    }

    @Override
    public <V> VertexProperty<V> property(final String key) {
        return new StrategyWrappedVertexProperty<>(this.strategyWrappedGraph.strategy().compose(
                s -> s.<V>getVertexGetPropertyStrategy(strategyContext),
                this.baseVertex::property).apply(key), this.strategyWrappedGraph);
    }

    @Override
    public GraphTraversal<Vertex, Vertex> start() {
        return applyStrategy(this.baseVertex.start());
    }

    @Override
    public String toString() {
        final GraphStrategy strategy = strategyWrappedGraph.strategy().getGraphStrategy().orElse(GraphStrategy.DefaultGraphStrategy.INSTANCE);
        return StringFactory.graphStrategyVertexString(strategy, this.baseVertex);
    }

    public class StrategyWrappedVertexIterators implements Vertex.Iterators {
        @Override
        public Iterator<Edge> edgeIterator(final Direction direction, final int branchFactor, final String... labels) {
            return new StrategyWrappedEdge.StrategyWrappedEdgeIterator(strategyWrappedGraph.strategy().compose(
                    s -> s.getVertexIteratorsEdgesStrategy(strategyContext),
                    (Direction d, Integer bf, String[] l) -> baseVertex.iterators().edgeIterator(d, bf, l)).apply(direction, branchFactor, labels), strategyWrappedGraph);
        }

        @Override
        public Iterator<Vertex> vertexIterator(final Direction direction, final int branchFactor, final String... labels) {
            return new StrategyWrappedVertexIterator(strategyWrappedGraph.strategy().compose(
                    s -> s.getVertexIteratorsVerticesStrategy(strategyContext),
                    (Direction d, Integer bf, String[] l) -> baseVertex.iterators().vertexIterator(d, bf, l)).apply(direction, branchFactor, labels), strategyWrappedGraph);
        }

        @Override
        public <V> Iterator<V> valueIterator(final String... propertyKeys) {
            return strategyWrappedGraph.strategy().compose(
                    s -> s.<V>getVertexIteratorsValuesStrategy(strategyContext),
                    (String[] pks) -> baseVertex.iterators().valueIterator(pks)).apply(propertyKeys);
        }

        @Override
        public <V> Iterator<V> hiddenValueIterator(final String... propertyKeys) {
            return strategyWrappedGraph.strategy().compose(
                    s -> s.<V>getVertexIteratorsHiddenValuesStrategy(strategyContext),
                    (String[] pks) -> baseVertex.iterators().hiddenValueIterator(pks)).apply(propertyKeys);
        }

        @Override
        public <V> Iterator<VertexProperty<V>> propertyIterator(final String... propertyKeys) {
            return StreamFactory.stream(strategyWrappedGraph.strategy().compose(
                    s -> s.<V>getVertexIteratorsPropertiesStrategy(strategyContext),
                    (String[] pks) -> baseVertex.iterators().propertyIterator(pks)).apply(propertyKeys))
                    .map(property -> (VertexProperty<V>) new StrategyWrappedVertexProperty<>(property, strategyWrappedGraph)).iterator();
        }

        @Override
        public <V> Iterator<VertexProperty<V>> hiddenPropertyIterator(final String... propertyKeys) {
            return StreamFactory.stream(strategyWrappedGraph.strategy().compose(
                    s -> s.<V>getVertexIteratorsHiddensStrategy(strategyContext),
                    (String[] pks) -> baseVertex.iterators().hiddenPropertyIterator(pks)).apply(propertyKeys))
                    .map(property -> (VertexProperty<V>) new StrategyWrappedVertexProperty<>(property, strategyWrappedGraph)).iterator();
        }
    }

    public static class StrategyWrappedVertexIterator implements Iterator<Vertex> {
        private final Iterator<Vertex> vertices;
        private final StrategyWrappedGraph strategyWrappedGraph;

        public StrategyWrappedVertexIterator(final Iterator<Vertex> itty,
                                             final StrategyWrappedGraph strategyWrappedGraph) {
            this.vertices = itty;
            this.strategyWrappedGraph = strategyWrappedGraph;
        }

        @Override
        public boolean hasNext() {
            return this.vertices.hasNext();
        }

        @Override
        public Vertex next() {
            return new StrategyWrappedVertex(this.vertices.next(), this.strategyWrappedGraph);
        }
    }
}
