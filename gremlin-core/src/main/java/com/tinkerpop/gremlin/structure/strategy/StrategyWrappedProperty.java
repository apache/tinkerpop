package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;

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
        this.baseProperty = baseProperty;
        this.strategyContext = new Strategy.Context<>(strategyWrappedGraph.getBaseGraph(), this);
        this.strategyWrappedGraph = strategyWrappedGraph;
    }

    @Override
    public String getKey() {
        return this.baseProperty.getKey();
    }

    @Override
    public V get() throws NoSuchElementException {
        return this.baseProperty.get();
    }

    @Override
    public boolean isPresent() {
        return this.baseProperty.isPresent();
    }

    @Override
    public <E extends Element> E getElement() {
        return this.baseProperty.getElement();
    }

    @Override
    public <E extends Throwable> V orElseThrow(final Supplier<? extends E> exceptionSupplier) throws E {
        return this.baseProperty.orElseThrow(exceptionSupplier);
    }

    @Override
    public V orElseGet(final Supplier<? extends V> edgeSupplier) {
        return this.baseProperty.orElseGet(edgeSupplier);
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
        this.strategyWrappedGraph.strategy().compose(
                s -> s.getRemovePropertyStrategy(strategyContext),
                () -> {
                    this.baseProperty.remove();
                    return null;
                }).get();
    }

	@Override
	public String toString() {
		final GraphStrategy strategy = this.strategyWrappedGraph.strategy().getGraphStrategy().orElse(GraphStrategy.DoNothingGraphStrategy.INSTANCE);
		return String.format("[%s[%s]]",strategy, baseProperty.toString());
	}
}
