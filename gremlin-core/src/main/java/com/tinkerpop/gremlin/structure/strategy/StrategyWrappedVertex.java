package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.structure.strategy.process.graph.StrategyWrappedElementTraversal;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.structure.util.wrapped.WrappedVertex;
import com.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StrategyWrappedVertex extends StrategyWrappedElement implements Vertex, StrategyWrapped, WrappedVertex<Vertex>, Vertex.Iterators {

    private final Strategy.Context<StrategyWrappedVertex> strategyContext;

    public StrategyWrappedVertex(final Vertex baseVertex, final StrategyWrappedGraph strategyWrappedGraph) {
        super(baseVertex, strategyWrappedGraph);
        this.strategyContext = new Strategy.Context<>(strategyWrappedGraph, this);
    }

    public Strategy.Context<StrategyWrappedVertex> getStrategyContext() {
        return this.strategyContext;
    }

    @Override
    public Graph graph() {
        return this.strategyWrappedGraph.getStrategy().compose(
                s -> s.getVertexGraphStrategy(this.strategyContext),
                () -> this.strategyWrappedGraph).get();
    }

    @Override
    public Object id() {
        return this.strategyWrappedGraph.getStrategy().compose(
                s -> s.getVertexIdStrategy(this.strategyContext),
                this.getBaseVertex()::id).get();
    }

    @Override
    public String label() {
        return this.strategyWrappedGraph.getStrategy().compose(
                s -> s.getVertexLabelStrategy(this.strategyContext),
                this.getBaseVertex()::label).get();
    }

    @Override
    public Set<String> keys() {
        return this.strategyWrappedGraph.getStrategy().compose(
                s -> s.getVertexKeysStrategy(this.strategyContext),
                this.getBaseVertex()::keys).get();
    }

    @Override
    public Vertex.Iterators iterators() {
        return this;
    }

    @Override
    public <V> V value(final String key) throws NoSuchElementException {
        return this.strategyWrappedGraph.getStrategy().compose(
                s -> s.<V>getVertexValueStrategy(this.strategyContext),
                this.getBaseVertex()::value).apply(key);
    }

    @Override
    public void remove() {
        this.strategyWrappedGraph.getStrategy().compose(
                s -> s.getRemoveVertexStrategy(this.strategyContext),
                () -> {
                    this.getBaseVertex().remove();
                    return null;
                }).get();
    }

    @Override
    public Vertex getBaseVertex() {
        return (Vertex) this.baseElement;
    }

    @Override
    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        final Vertex baseInVertex = (inVertex instanceof StrategyWrappedVertex) ? ((StrategyWrappedVertex) inVertex).getBaseVertex() : inVertex;
        return new StrategyWrappedEdge(this.strategyWrappedGraph.getStrategy().compose(
                s -> s.getAddEdgeStrategy(this.strategyContext),
                this.getBaseVertex()::addEdge)
                .apply(label, baseInVertex, keyValues), this.strategyWrappedGraph);
    }

    @Override
    public <V> VertexProperty<V> property(final String key, final V value) {
        return new StrategyWrappedVertexProperty<>(this.strategyWrappedGraph.getStrategy().compose(
                s -> s.<V>getVertexPropertyStrategy(this.strategyContext),
                this.getBaseVertex()::property).apply(key, value), this.strategyWrappedGraph);
    }

    @Override
    public <V> VertexProperty<V> property(final String key) {
        return new StrategyWrappedVertexProperty<>(this.strategyWrappedGraph.getStrategy().compose(
                s -> s.<V>getVertexGetPropertyStrategy(this.strategyContext),
                this.getBaseVertex()::property).apply(key), this.strategyWrappedGraph);
    }

    @Override
    public GraphTraversal<Vertex, Vertex> start() {
        return new StrategyWrappedElementTraversal<>(this, strategyWrappedGraph);
    }

    @Override
    public String toString() {
        return StringFactory.graphStrategyElementString(this);
    }


    @Override
    public Iterator<Edge> edgeIterator(final Direction direction, final String... edgeLabels) {
        return new StrategyWrappedEdge.StrategyWrappedEdgeIterator(this.strategyWrappedGraph.getStrategy().compose(
                s -> s.getVertexIteratorsEdgesStrategy(this.strategyContext),
                (Direction d, String[] l) -> this.getBaseVertex().iterators().edgeIterator(d, l)).apply(direction, edgeLabels), this.strategyWrappedGraph);
    }

    @Override
    public Iterator<Vertex> vertexIterator(final Direction direction, final String... labels) {
        return new StrategyWrappedVertexIterator(this.strategyWrappedGraph.getStrategy().compose(
                s -> s.getVertexIteratorsVerticesStrategy(strategyContext),
                (Direction d, String[] l) -> this.getBaseVertex().iterators().vertexIterator(d, l)).apply(direction, labels), this.strategyWrappedGraph);
    }

    @Override
    public <V> Iterator<V> valueIterator(final String... propertyKeys) {
        return this.strategyWrappedGraph.getStrategy().compose(
                s -> s.<V>getVertexIteratorsValuesStrategy(strategyContext),
                (String[] pks) -> this.getBaseVertex().iterators().valueIterator(pks)).apply(propertyKeys);
    }

    @Override
    public <V> Iterator<VertexProperty<V>> propertyIterator(final String... propertyKeys) {
        return IteratorUtils.map(this.strategyWrappedGraph.getStrategy().compose(
                        s -> s.<V>getVertexIteratorsPropertiesStrategy(this.strategyContext),
                        (String[] pks) -> this.getBaseVertex().iterators().propertyIterator(pks)).apply(propertyKeys),
                property -> new StrategyWrappedVertexProperty<>(property, this.strategyWrappedGraph));
    }

    public static class StrategyWrappedVertexIterator implements Iterator<Vertex> {
        private final Iterator<Vertex> vertices;
        private final StrategyWrappedGraph strategyWrappedGraph;

        public StrategyWrappedVertexIterator(final Iterator<Vertex> iterator, final StrategyWrappedGraph strategyWrappedGraph) {
            this.vertices = iterator;
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
