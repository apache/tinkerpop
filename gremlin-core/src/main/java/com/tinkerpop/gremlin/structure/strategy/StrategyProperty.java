package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.structure.util.wrapped.WrappedProperty;

import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class StrategyProperty<V> implements Property<V>, StrategyWrapped, WrappedProperty<Property<V>> {

    private final Property<V> baseProperty;
    private final StrategyContext<StrategyProperty<V>, Property<V>> strategyContext;
    private final StrategyGraph strategyGraph;
    private final GraphStrategy strategy;

    public StrategyProperty(final Property<V> baseProperty, final StrategyGraph strategyGraph) {
        if (baseProperty instanceof StrategyWrapped) throw new IllegalArgumentException(
                String.format("The property %s is already StrategyWrapped and must be a base Property", baseProperty));
        this.baseProperty = baseProperty;
        this.strategyContext = new StrategyContext<>(strategyGraph, this, baseProperty);
        this.strategyGraph = strategyGraph;
        this.strategy = strategyGraph.getStrategy();
    }

    @Override
    public String key() {
        return this.strategyGraph.compose(
                s -> s.getPropertyKeyStrategy(this.strategyContext, strategy), this.baseProperty::key).get();
    }

    @Override
    public V value() throws NoSuchElementException {
        return this.strategyGraph.compose(
                s -> s.getPropertyValueStrategy(this.strategyContext, strategy), this.baseProperty::value).get();
    }

    @Override
    public boolean isPresent() {
        return this.baseProperty.isPresent();
    }

    @Override
    public Element element() {
        final Element baseElement = this.baseProperty.element();
        return (baseElement instanceof Vertex ? new StrategyVertex((Vertex) baseElement, this.strategyGraph) :
                new StrategyEdge((Edge) baseElement, this.strategyGraph));
    }

    @Override
    public <E extends Throwable> V orElseThrow(final Supplier<? extends E> exceptionSupplier) throws E {
        return this.baseProperty.orElseThrow(exceptionSupplier);
    }

    @Override
    public V orElseGet(final Supplier<? extends V> valueSupplier) {
        return this.baseProperty.orElseGet(valueSupplier);
    }

    @Override
    public V orElse(final V otherValue) {
        return this.baseProperty.orElse(otherValue);
    }

    @Override
    public void ifPresent(final Consumer<? super V> consumer) {
        this.baseProperty.ifPresent(consumer);
    }

    @Override
    public void remove() {
        this.strategyGraph.compose(
                s -> s.getRemovePropertyStrategy(strategyContext, strategy),
                () -> {
                    this.baseProperty.remove();
                    return null;
                }).get();
    }

    @Override
    public String toString() {
        return StringFactory.graphStrategyPropertyString(this);
    }

    @Override
    public Property<V> getBaseProperty() {
        if (strategyGraph.isSafe()) throw StrategyGraph.Exceptions.strategyGraphIsSafe();
        return this.baseProperty;
    }

    Property<V> getBasePropertySafe() {
        return this.baseProperty;
    }
}
