package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.util.ElementHelper;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class StrategyElement implements Element, StrategyWrapped {
    protected final StrategyGraph strategyGraph;
    protected final Element baseElement;
    protected final Strategy.StrategyContext<StrategyElement> elementStrategyContext;

    protected StrategyElement(final Element baseElement, final StrategyGraph strategyGraph) {
        if (baseElement instanceof StrategyWrapped) throw new IllegalArgumentException(
                String.format("The element %s is already StrategyWrapped and must be a base Element", baseElement));
        this.strategyGraph = strategyGraph;
        this.baseElement = baseElement;
        this.elementStrategyContext = new Strategy.StrategyContext<>(strategyGraph, this);
    }

    public Strategy.StrategyContext<StrategyElement> getElementStrategyContext() {
        return elementStrategyContext;
    }

    public Element getBaseElement() {
        return this.baseElement;
    }

    @Override
    public int hashCode() {
        return this.baseElement.hashCode();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }
}
