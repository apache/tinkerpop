package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.structure.Graph;

/**
 * The {@link StrategyContext} object is provided to the methods of {@link GraphStrategy} so that the strategy functions
 * it constructs have some knowledge of the environment.
 *
 * @param <T> represents the object that is triggering the strategy (i.e. the vertex on which addEdge was called).
 */
public final class StrategyContext<T extends StrategyWrapped> {
    private final StrategyGraph g;
    private final T current;

    public StrategyContext(final StrategyGraph g, final T current) {
        if (null == g) throw Graph.Exceptions.argumentCanNotBeNull("g");
        if (null == current) throw Graph.Exceptions.argumentCanNotBeNull("current");

        this.g = g;
        this.current = current;
    }

    /**
     * Gets the {@link StrategyWrapped} instance that is triggering the {@link GraphStrategy} method.
     */
    public T getCurrent() {
        return current;
    }

    /**
     * Gets the current {@link com.tinkerpop.gremlin.structure.strategy.StrategyGraph} instance.
     */
    public StrategyGraph getStrategyGraph() {
        return g;
    }
}
