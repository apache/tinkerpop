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
    public <V> V getValue(final String key) throws NoSuchElementException {
        return this.baseElement.getValue(key);
    }

    @Override
    public void setProperties(final Object... keyValues) {
        this.baseElement.setProperties(keyValues);
    }

    @Override
    public <V> Property<V> getProperty(final String key) {
        return this.strategyWrappedGraph.strategy().compose(
                s -> s.<V>getElementGetProperty(elementStrategyContext),
                this.baseElement::getProperty).apply(key);
    }

    @Override
    public Map<String, Property> getProperties() {
        return this.baseElement.getProperties();
    }

    @Override
    public Map<String, Property> getHiddens() {
        return this.baseElement.getHiddens();
    }

    @Override
    public Set<String> getPropertyKeys() {
        return this.baseElement.getPropertyKeys();
    }

    @Override
    public String getLabel() {
        return this.baseElement.getLabel();
    }

    @Override
    public Object getId() {
        return this.strategyWrappedGraph.strategy().compose(
                s -> s.getElementGetId(elementStrategyContext),
                this.baseElement::getId).get().toString();
    }

    @Override
    public <V> Property<V> setProperty(final String key, final V value) {
        this.strategyWrappedGraph.strategy().compose(
                s -> s.<V>getElementSetProperty(elementStrategyContext),
                this.baseElement::setProperty).accept(key, value);
        return null; // TODO: Stephen, help please! This needs to return the created Property<V>
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
}
