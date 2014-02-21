package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Strategy;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class StrategyWrappedElement implements Element {
    protected final StrategyWrappedGraph strategyWrappedGraph;
    private final Element baseElement;
    private transient final Strategy.Context<Element> elementStrategyContext;


    protected StrategyWrappedElement(final StrategyWrappedGraph strategyWrappedGraph, final Element baseElement) {
        this.strategyWrappedGraph = strategyWrappedGraph;
        this.baseElement = baseElement;
        this.elementStrategyContext = new Strategy.Context<Element>(strategyWrappedGraph, this);
    }

    @Override
    public <V> V getValue(String key) throws NoSuchElementException {
        return this.baseElement.getValue(key);
    }

    @Override
    public void setProperties(Object... keyValues) {
        this.baseElement.setProperties(keyValues);
    }

    @Override
    public <V> Property<V> getProperty(String key) {
        return this.strategyWrappedGraph.strategy().compose(
                s -> s.<V>getElementGetProperty(elementStrategyContext),
                this.baseElement::getProperty).apply(key);
    }

    @Override
    public Map<String, Property> getProperties() {
        return this.baseElement.getProperties();
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
    public <V> void setProperty(String key, V value) {
        this.strategyWrappedGraph.strategy().compose(
                s -> s.<V>getElementSetProperty(elementStrategyContext),
                this.baseElement::setProperty).accept(key, value);
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
