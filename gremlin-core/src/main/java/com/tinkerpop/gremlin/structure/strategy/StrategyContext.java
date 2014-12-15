package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.structure.Graph;

/**
 * The {@link StrategyContext} object is provided to the methods of {@link GraphStrategy} so that the strategy functions
 * it constructs have some knowledge of the environment.
 *
 * @param <T> represents the object that is triggering the strategy (i.e. the vertex on which addEdge was called).
 * @param <B> represents the base object that is triggering the strategy
 */
public final class StrategyContext<T extends StrategyWrapped, B> {
    private final StrategyGraph g;
    private final Graph baseGraph;
    private final T current;
    private final B currentBase;

    public StrategyContext(final StrategyGraph g, final T current, final B currentBase) {
        if (null == g) throw Graph.Exceptions.argumentCanNotBeNull("g");
        if (null == current) throw Graph.Exceptions.argumentCanNotBeNull("current");
        if (null == currentBase) throw Graph.Exceptions.argumentCanNotBeNull("currentBase");

        this.g = g;
        this.baseGraph = g.getBaseGraph();
        this.current = current;
        this.currentBase = currentBase;
    }

    /**
     * Gets the {@link StrategyWrapped} instance that is triggering the {@link GraphStrategy} method.
     */
    public T getCurrent() {
        return current;
    }

    /**
     * Gets the base object of the {@link StrategyWrapped} instances that is triggering the {@link GraphStrategy}
     * method.  If a {@link GraphStrategy} implementation needs this instance.
     */
    public B getBaseCurrent() {
        return currentBase;
    }

    /**
     * Gets the base {@link Graph} of the {@link StrategyGraph} instance.
     */
    public Graph getBaseGraph() {
        return baseGraph;
    }

    /**
     * Gets the current {@link com.tinkerpop.gremlin.structure.strategy.StrategyGraph} instance.
     */
    public StrategyGraph getStrategyGraph() {
        return g;
    }
}
