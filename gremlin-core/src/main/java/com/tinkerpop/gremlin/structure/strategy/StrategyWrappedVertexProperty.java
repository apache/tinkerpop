package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class StrategyWrappedVertexProperty<V> extends StrategyWrappedElement implements VertexProperty<V>, StrategyWrapped {
    private final VertexProperty<V> baseVertexProperty;
    private final Strategy.Context<StrategyWrappedVertexProperty<V>> strategyContext;

    public StrategyWrappedVertexProperty(final VertexProperty<V> baseVertexProperty, final StrategyWrappedGraph strategyWrappedGraph) {
        super(baseVertexProperty, strategyWrappedGraph);
        this.strategyContext = new Strategy.Context<>(strategyWrappedGraph.getBaseGraph(), this);
        this.baseVertexProperty = baseVertexProperty;
    }

    @Override
    public Object id() {
        return this.strategyWrappedGraph.strategy().compose(
                s -> s.getVertexPropertyIdStrategy(strategyContext),
                this.baseVertexProperty::id).get();
    }

    @Override
    public String label() {
        return this.strategyWrappedGraph.strategy().compose(
                s -> s.getVertexPropertyLabelStrategy(strategyContext),
                this.baseVertexProperty::label).get();
    }

    @Override
    public Set<String> keys() {
        return this.strategyWrappedGraph.strategy().compose(
                s -> s.getVertexPropertyKeysStrategy(strategyContext),
                this.baseVertexProperty::keys).get();
    }

    @Override
    public Set<String> hiddenKeys() {
        return this.strategyWrappedGraph.strategy().compose(
                s -> s.getVertexPropertyHiddenKeysStrategy(strategyContext),
                this.baseVertexProperty::hiddenKeys).get();
    }

    @Override
    public Vertex getElement() {
        return new StrategyWrappedVertex(this.strategyWrappedGraph.strategy().compose(
                s -> s.getVertexPropertyGetElementStrategy(strategyContext),
                this.baseVertexProperty::getElement).get(), strategyWrappedGraph);
    }

    @Override
    public VertexProperty.Iterators iterators() {
        return this.baseVertexProperty.iterators();
    }

    @Override
    public <U> Property<U> property(final String key, final U value) {
        return new StrategyWrappedProperty<U>(this.strategyWrappedGraph.strategy().compose(
                s -> s.<U, V>getVertexPropertyPropertyStrategy(strategyContext),
                this.baseVertexProperty::property).<String,U>apply(key, value), this.strategyWrappedGraph);
    }

    @Override
    public String key() {
        return this.baseVertexProperty.key();
    }

    @Override
    public V value() throws NoSuchElementException {
        return this.baseVertexProperty.value();
    }

    @Override
    public boolean isHidden() {
        return this.baseVertexProperty.isHidden();
    }

    @Override
    public boolean isPresent() {
        return this.baseVertexProperty.isPresent();
    }

    @Override
    public void remove() {
        this.strategyWrappedGraph.strategy().compose(
                s -> s.getRemoveVertexPropertyStrategy(strategyContext),
                () -> {
                    this.baseVertexProperty.remove();
                    return null;
                }).get();
    }

    public class StrategyWrappedVertexPropertyIterators implements VertexProperty.Iterators {
        @Override
        public <U> Iterator<Property<U>> properties(final String... propertyKeys) {
            return null;
        }

        @Override
        public <U> Iterator<Property<U>> hiddens(final String... propertyKeys) {
            return null;
        }

        @Override
        public <V> Iterator<V> values(final String... propertyKeys) {
            return null;
        }

        @Override
        public <V> Iterator<V> hiddenValues(final String... propertyKeys) {
            return null;
        }
    }
}
