package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class StrategyWrappedProperty<V> implements Property<V>, StrategyWrapped {
    private final Property<V> baseProperty;
    private final Strategy.Context<StrategyWrappedProperty<V>> strategyContext;
    private final StrategyWrappedGraph strategyWrappedGraph;

    public StrategyWrappedProperty(final Property<V> baseProperty, final StrategyWrappedGraph strategyWrappedGraph) {
        if (baseProperty instanceof StrategyWrapped) throw new IllegalArgumentException(
                String.format("The property %s is already StrategyWrapped and must be a base Property", baseProperty));
        this.baseProperty = baseProperty;
        this.strategyContext = new Strategy.Context<>(strategyWrappedGraph.getBaseGraph(), this);
        this.strategyWrappedGraph = strategyWrappedGraph;
    }

    @Override
    public String key() {
        return this.baseProperty.key();
    }

    @Override
    public V value() throws NoSuchElementException {
        return this.baseProperty.value();
    }

    @Override
    public boolean isPresent() {
        return this.baseProperty.isPresent();
    }

    @Override
    public <E extends Element> E element() {
        final Element baseElement = this.baseProperty.element();
        return (E) (baseElement instanceof Vertex ? new StrategyWrappedVertex((Vertex) baseElement, strategyWrappedGraph) :
                new StrategyWrappedEdge((Edge) baseElement, strategyWrappedGraph));
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
    public boolean isHidden() {
        return this.baseProperty.isHidden();
    }

    @Override
    public void remove() {
        this.strategyWrappedGraph.strategy().compose(
                s -> s.getRemovePropertyStrategy(strategyContext),
                () -> {
                    this.baseProperty.remove();
                    return null;
                }).get();
    }

    @Override
    public String toString() {
        final GraphStrategy strategy = strategyWrappedGraph.strategy().getGraphStrategy().orElse(GraphStrategy.DefaultGraphStrategy.INSTANCE);
        return StringFactory.graphStrategyPropertyString(strategy, this.baseProperty);
    }
}
