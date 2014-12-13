package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.structure.Graph;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * The {@link StrategyContext} object is provided to the methods of {@link GraphStrategy} so that the strategy functions
 * it constructs have some knowledge of the environment.
 *
 * @param <T> represents the object that is calling the strategy (i.e. the vertex on which addEdge was called).
 */
public final class StrategyContext<T extends StrategyWrapped, B> {
    private final StrategyGraph g;
    private final Graph baseGraph;
    private final Map<String, Object> environment;
    private final T current;
    private final B currentBase;

    public StrategyContext(final StrategyGraph g, final T current, final B currentBase) {
        this(g, current, currentBase, null);
    }

    public StrategyContext(final StrategyGraph g, final T current, final B currentBase, final Map<String, Object> environment) {
        if (null == g) throw Graph.Exceptions.argumentCanNotBeNull("g");
        if (null == current) throw Graph.Exceptions.argumentCanNotBeNull("current");
        if (null == currentBase) throw Graph.Exceptions.argumentCanNotBeNull("currentBase");

        this.g = g;
        this.baseGraph = g.getBaseGraphSafe();
        this.current = current;
        this.currentBase = currentBase;
        this.environment = null == environment ? new HashMap<>() : environment;
    }

    public T getCurrent() {
        return current;
    }

    public B getCurrentBase() {
        return currentBase;
    }

    public Graph getBaseGraph() {
        return baseGraph;
    }

    public StrategyGraph getStrategyGraph() {
        return g;
    }

    public Map<String, Object> getEnvironment() {
        return Collections.unmodifiableMap(environment);
    }
}
