package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class StrategyWrappedElement implements Element, StrategyWrapped {
    protected final StrategyWrappedGraph strategyWrappedGraph;
    private final Element baseElement;
    private final Strategy.Context<StrategyWrappedElement> elementStrategyContext;

    protected StrategyWrappedElement(final Element baseElement, final StrategyWrappedGraph strategyWrappedGraph) {
        this.strategyWrappedGraph = strategyWrappedGraph;
        this.baseElement = baseElement;
        this.elementStrategyContext = new Strategy.Context<>(strategyWrappedGraph.getBaseGraph(), this);
    }

    public Element getBaseElement() {
        return this.baseElement;
    }

    @Override
    public <V> V value(final String key) throws NoSuchElementException {
        return this.baseElement.value(key);
    }

    @Override
    public void properties(final Object... keyValues) {
        this.baseElement.properties(keyValues);
    }

    @Override
    public <V> Property<V> property(final String key) {
        return this.strategyWrappedGraph.strategy().compose(
                s -> s.<V>getElementGetProperty(elementStrategyContext),
                this.baseElement::property).apply(key);
    }

    @Override
    public Map<String, Property> properties() {
        return this.baseElement.properties();
    }

    @Override
    public Map<String, Property> hiddens() {
        return this.baseElement.hiddens();
    }

    @Override
    public Set<String> keys() {
        return this.baseElement.keys();
    }

    @Override
    public String label() {
        return this.baseElement.label();
    }

    @Override
    public Object id() {
        return this.strategyWrappedGraph.strategy().compose(
                s -> s.getElementGetId(elementStrategyContext),
                this.baseElement::id).get().toString();
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        return this.strategyWrappedGraph.strategy().compose(
                s -> s.<V>getElementSetProperty(elementStrategyContext),
                this.baseElement::property).apply(key, value);
    }

    @Override
    public void remove() {
        this.strategyWrappedGraph.strategy().compose(
                s -> s.getRemoveElementStrategy(elementStrategyContext),
                () -> {
                    this.baseElement.remove();
                    return null;
                }).get();
    }

	@Override
	public String toString() {
		final GraphStrategy strategy = this.strategyWrappedGraph.strategy().getGraphStrategy().orElse(DoNothingGraphStrategy.INSTANCE);
		return String.format("[%s[%s]]", strategy, baseElement.toString());
	}
}
