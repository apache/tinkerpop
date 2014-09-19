package com.tinkerpop.gremlin.structure.strategy;

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
    public Vertex getElement() {
        return null;
    }

    @Override
    public VertexProperty.Iterators iterators() {
        // todo: iterators on vertex properties
        return null;
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
