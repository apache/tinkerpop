package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.structure.Vertex;

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
        return this.baseVertexProperty.label();
    }

    @Override
    public Set<String> keys() {
        return this.baseVertexProperty.keys();
    }

    @Override
    public Set<String> hiddenKeys() {
        return this.baseVertexProperty.hiddenKeys();
    }

    @Override
    public Vertex getElement() {
        return this.baseVertexProperty.getElement();
    }

    @Override
    public VertexProperty.Iterators iterators() {
        return this.baseVertexProperty.iterators();
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        return this.baseVertexProperty.property(key, value);
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
}
