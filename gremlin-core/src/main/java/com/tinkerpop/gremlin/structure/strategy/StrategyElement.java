package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.util.ElementHelper;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class StrategyElement implements Element, StrategyWrapped {
    protected final StrategyGraph strategyGraph;
    protected final GraphStrategy strategy;
    protected final Element baseElement;
    protected final StrategyContext<StrategyElement, Element> elementStrategyContext;

    protected StrategyElement(final Element baseElement, final StrategyGraph strategyGraph) {
        if (baseElement instanceof StrategyWrapped) throw new IllegalArgumentException(
                String.format("The element %s is already StrategyWrapped and must be a base Element", baseElement));
        this.strategyGraph = strategyGraph;
        this.strategy = strategyGraph.getStrategy();
        this.baseElement = baseElement;
        this.elementStrategyContext = new StrategyContext<>(strategyGraph, this, baseElement);
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
