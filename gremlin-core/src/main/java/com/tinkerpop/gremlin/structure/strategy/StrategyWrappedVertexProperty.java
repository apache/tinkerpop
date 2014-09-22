package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.NoSuchElementException;

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
        return this.baseVertexProperty.id();
    }

    @Override
    public String label() {
        return this.baseVertexProperty.label();
    }

    @Override
    public Vertex getElement() {
        return this.baseVertexProperty.getElement();
    }

    @Override
    public VertexProperty.Iterators iterators() {
        // todo: iterators on vertex properties
        return this.baseVertexProperty.iterators();
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        // todo: gotta add a strategy here and cover readonly cases....
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
