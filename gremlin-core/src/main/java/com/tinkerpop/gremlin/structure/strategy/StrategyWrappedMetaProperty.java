package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.structure.MetaProperty;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.NoSuchElementException;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class StrategyWrappedMetaProperty<V> extends StrategyWrappedElement implements MetaProperty<V>, StrategyWrapped {
    private final MetaProperty<V> baseMetaProperty;
    private final Strategy.Context<StrategyWrappedMetaProperty<V>> strategyContext;

    public StrategyWrappedMetaProperty(final MetaProperty<V> baseMetaProperty, final StrategyWrappedGraph strategyWrappedGraph) {
        super(baseMetaProperty, strategyWrappedGraph);
        this.strategyContext = new Strategy.Context<>(strategyWrappedGraph.getBaseGraph(), this);
        this.baseMetaProperty = baseMetaProperty;
    }

    @Override
    public Vertex getElement() {
        return null;
    }

    @Override
    public MetaProperty.Iterators iterators() {
        // todo: iterators on metaproperties
        return null;
    }

    @Override
    public String key() {
        return this.baseMetaProperty.key();
    }

    @Override
    public V value() throws NoSuchElementException {
        return this.baseMetaProperty.value();
    }

    @Override
    public boolean isPresent() {
        return this.baseMetaProperty.isPresent();
    }

    @Override
    public void remove() {
        this.strategyWrappedGraph.strategy().compose(
                s -> s.getRemoveMetaPropertyStrategy(strategyContext),
                () -> {
                    this.baseMetaProperty.remove();
                    return null;
                }).get();
    }
}
