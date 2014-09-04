package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.MetaProperty;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.wrapped.WrappedVertex;
import com.tinkerpop.gremlin.util.StreamFactory;

import java.util.Iterator;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class StrategyWrappedVertex extends StrategyWrappedElement implements Vertex, StrategyWrapped, WrappedVertex<Vertex> {
    private final Vertex baseVertex;
    private final Strategy.Context<StrategyWrappedVertex> strategyContext;

    public StrategyWrappedVertex(final Vertex baseVertex, final StrategyWrappedGraph strategyWrappedGraph) {
        super(baseVertex, strategyWrappedGraph);
        this.strategyContext = new Strategy.Context<>(strategyWrappedGraph.getBaseGraph(), this);
        this.baseVertex = baseVertex;
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
    public Iterator<Vertex> vertices(final Direction direction, final int branchFactor, final String... labels) {
        return new StrategyWrappedVertexIterator(this.baseVertex.vertices(direction, branchFactor, labels), this.strategyWrappedGraph);
    }

    @Override
    public Iterator<Edge> edges(final Direction direction, final int branchFactor, final String... labels) {
        return new StrategyWrappedEdge.StrategyWrappedEdgeIterator(this.baseVertex.edges(direction, branchFactor, labels), this.strategyWrappedGraph);
    }

    @Override
    public <V> Iterator<MetaProperty<V>> properties(final String... propertyKeys) {
        return (Iterator) StreamFactory.stream(this.strategyWrappedGraph.strategy().compose(
                s -> s.getElementPropertiesGetter(strategyContext),
                () -> this.baseVertex.properties(propertyKeys)).get())
                .map(property -> new StrategyWrappedProperty<>((Property<V>) property, strategyWrappedGraph)).iterator();
    }

    @Override
    public <V> Iterator<MetaProperty<V>> hiddens(final String... propertyKeys) {
        return (Iterator) StreamFactory.stream(this.strategyWrappedGraph.strategy().compose(
                s -> s.getElementHiddens(strategyContext),
                () -> this.baseVertex.hiddens(propertyKeys)).get())
                .map(property -> new StrategyWrappedProperty<>((Property<V>) property, strategyWrappedGraph)).iterator();
    }

    @Override
    public <V> MetaProperty<V> property(final String key, final V value) {
        return (MetaProperty) this.strategyWrappedGraph.strategy().compose(
                s -> s.<V>getElementProperty(strategyContext),
                this.baseVertex::property).apply(key, value);
    }

    @Override
    public <V> MetaProperty<V> property(final String key) {
        return (MetaProperty) this.strategyWrappedGraph.strategy().compose(
                s -> s.<V>getElementGetProperty(strategyContext),
                this.baseVertex::property).apply(key);
    }

    @Override
    public <V> Iterator<V> values(final String... propertyKeys) {
        return this.strategyWrappedGraph.strategy().compose(
                s -> s.getElementValues(strategyContext),
                this.baseVertex.values(propertyKeys)).get();
    }

    @Override
    public <V> Iterator<V> hiddenValues(final String... propertyKeys) {
        return this.strategyWrappedGraph.strategy().compose(
                s -> s.getElementHiddenValues(strategyContext),
                this.baseVertex.hiddenValues(propertyKeys)).get();
    }

    @Override
    public GraphTraversal<Vertex, Vertex> start() {
        return applyStrategy(this.baseVertex.start());
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
