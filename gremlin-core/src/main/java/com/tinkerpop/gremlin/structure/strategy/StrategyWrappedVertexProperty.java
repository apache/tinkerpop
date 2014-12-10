package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.structure.strategy.process.graph.StrategyWrappedElementTraversal;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.structure.util.wrapped.WrappedVertexProperty;
import com.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StrategyWrappedVertexProperty<V> extends StrategyWrappedElement implements VertexProperty<V>, StrategyWrapped, WrappedVertexProperty<VertexProperty<V>>, VertexProperty.Iterators {

    private final Strategy.Context<StrategyWrappedVertexProperty<V>> strategyContext;

    public StrategyWrappedVertexProperty(final VertexProperty<V> baseVertexProperty, final StrategyWrappedGraph strategyWrappedGraph) {
        super(baseVertexProperty, strategyWrappedGraph);
        this.strategyContext = new Strategy.Context<>(strategyWrappedGraph, this);
    }

    public Strategy.Context<StrategyWrappedVertexProperty<V>> getStrategyContext() {
        return strategyContext;
    }

    @Override
    public GraphTraversal<VertexProperty, VertexProperty> start() {
        return new StrategyWrappedElementTraversal<>(this, strategyWrappedGraph);
    }


    @Override
    public Graph graph() {
        return this.strategyWrappedGraph.getStrategy().compose(
                s -> s.getVertexPropertyGraphStrategy(strategyContext),
                () -> this.strategyWrappedGraph).get();
    }

    @Override
    public Object id() {
        return this.strategyWrappedGraph.getStrategy().compose(
                s -> s.getVertexPropertyIdStrategy(strategyContext),
                this.getBaseVertexProperty()::id).get();
    }

    @Override
    public String label() {
        return this.strategyWrappedGraph.getStrategy().compose(
                s -> s.getVertexPropertyLabelStrategy(strategyContext),
                this.getBaseVertexProperty()::label).get();
    }

    @Override
    public Set<String> keys() {
        return this.strategyWrappedGraph.getStrategy().compose(
                s -> s.getVertexPropertyKeysStrategy(strategyContext),
                this.getBaseVertexProperty()::keys).get();
    }

    @Override
    public Vertex element() {
        return new StrategyWrappedVertex(this.strategyWrappedGraph.getStrategy().compose(
                s -> s.getVertexPropertyGetElementStrategy(strategyContext),
                this.getBaseVertexProperty()::element).get(), strategyWrappedGraph);
    }

    @Override
    public VertexProperty.Iterators iterators() {
        return this;
    }

    @Override
    public <U> Property<U> property(final String key, final U value) {
        return new StrategyWrappedProperty<U>(this.strategyWrappedGraph.getStrategy().compose(
                s -> s.<U, V>getVertexPropertyPropertyStrategy(strategyContext),
                this.getBaseVertexProperty()::property).<String, U>apply(key, value), this.strategyWrappedGraph);
    }

    @Override
    public String key() {
        return this.strategyWrappedGraph.getStrategy().compose(
                s -> s.getVertexPropertyKeyStrategy(this.strategyContext), this.getBaseVertexProperty()::key).get();
    }

    @Override
    public V value() throws NoSuchElementException {
        return this.strategyWrappedGraph.getStrategy().compose(
                s -> s.getVertexPropertyValueStrategy(this.strategyContext), this.getBaseVertexProperty()::value).get();
    }

    @Override
    public boolean isPresent() {
        return this.getBaseVertexProperty().isPresent();
    }

    @Override
    public void remove() {
        this.strategyWrappedGraph.getStrategy().compose(
                s -> s.getRemoveVertexPropertyStrategy(this.strategyContext),
                () -> {
                    this.getBaseVertexProperty().remove();
                    return null;
                }).get();
    }

    @Override
    public VertexProperty<V> getBaseVertexProperty() {
        return (VertexProperty<V>) this.baseElement;
    }

    @Override
    public String toString() {
        return StringFactory.graphStrategyElementString(this);
    }


    @Override
    public <U> Iterator<Property<U>> propertyIterator(final String... propertyKeys) {
        return IteratorUtils.map(this.strategyWrappedGraph.getStrategy().compose(
                        s -> s.<U, V>getVertexPropertyIteratorsPropertiesStrategy(this.strategyContext),
                        (String[] pks) -> this.getBaseVertexProperty().iterators().propertyIterator(pks)).apply(propertyKeys),
                property -> new StrategyWrappedProperty<>(property, this.strategyWrappedGraph));
    }

    @Override
    public <U> Iterator<U> valueIterator(final String... propertyKeys) {
        return this.strategyWrappedGraph.getStrategy().compose(
                s -> s.<U, V>getVertexPropertyIteratorsValuesStrategy(this.strategyContext),
                (String[] pks) -> this.getBaseVertexProperty().iterators().valueIterator(pks)).apply(propertyKeys);
    }
}
