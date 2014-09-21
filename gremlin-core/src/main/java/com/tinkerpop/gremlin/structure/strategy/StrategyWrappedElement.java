package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class StrategyWrappedElement implements Element, StrategyWrapped {
    protected final StrategyWrappedGraph strategyWrappedGraph;
    protected final Element baseElement;
    protected final Strategy.Context<StrategyWrappedElement> elementStrategyContext;

    protected StrategyWrappedElement(final Element baseElement, final StrategyWrappedGraph strategyWrappedGraph) {
        if (baseElement instanceof StrategyWrapped) throw new IllegalArgumentException(
                String.format("The element %s is already StrategyWrapped and must be a base Element", baseElement));
        this.strategyWrappedGraph = strategyWrappedGraph;
        this.baseElement = baseElement;
        this.elementStrategyContext = new Strategy.Context<>(strategyWrappedGraph.getBaseGraph(), this);
    }

    public Element getBaseElement() {
        return this.baseElement;
    }

    @Override
    public <V> V value(final String key) throws NoSuchElementException {
        return this.strategyWrappedGraph.strategy().compose(
                s -> s.<V>getElementValueStrategy(elementStrategyContext),
                this.baseElement::value).apply(key);
    }

    @Override
    public Set<String> keys() {
        return this.strategyWrappedGraph.strategy().compose(
                s -> s.getElementKeysStrategy(elementStrategyContext),
                this.baseElement::keys).get();
    }

    @Override
    public Set<String> hiddenKeys() {
        return this.strategyWrappedGraph.strategy().compose(
                s -> s.getElementHiddenKeysStrategy(elementStrategyContext),
                this.baseElement::hiddenKeys).get();
    }

    @Override
    public String label() {
        return this.strategyWrappedGraph.strategy().compose(
                s -> s.getElementLabelStrategy(elementStrategyContext),
                this.baseElement::label).get();
    }

    @Override
    public Object id() {
        return this.strategyWrappedGraph.strategy().compose(
                s -> s.getElementIdStrategy(elementStrategyContext),
                this.baseElement::id).get();
    }

    @Override
    public String toString() {
        return baseElement.toString();
    }

    @Override
    public int hashCode() {
        return this.id().hashCode();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    protected <S, E> GraphTraversal<S, E> applyStrategy(final GraphTraversal<S, E> traversal) {
        traversal.strategies().register(new StrategyWrappedTraversalStrategy(this.strategyWrappedGraph));
        this.strategyWrappedGraph.strategy().getGraphStrategy().ifPresent(s -> s.applyStrategyToTraversal(traversal));
        return traversal;
    }

    public abstract class StrategyWrappedElementIterators implements Iterators {
        // todo: multi-property concerns
        @Override
        public <V> Iterator<V> values(final String... propertyKeys) {
            return strategyWrappedGraph.strategy().compose(
                    s -> s.<V>getElementValuesStrategy(elementStrategyContext),
                    (String[] pks) -> baseElement.iterators().values(pks)).apply(propertyKeys);
        }

        @Override
        public <V> Iterator<V> hiddenValues(final String... propertyKeys) {
            return strategyWrappedGraph.strategy().compose(
                    s -> s.<V>getElementHiddenValuesStrategy(elementStrategyContext),
                    (String[] pks) -> baseElement.iterators().hiddenValues(pks)).apply(propertyKeys);
        }
    }
}
