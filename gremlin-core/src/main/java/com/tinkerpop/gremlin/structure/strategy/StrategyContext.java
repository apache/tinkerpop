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
public class StrategyContext<T extends StrategyWrapped> {
    private final StrategyGraph g;
    private final Map<String, Object> environment;
    private final T current;

    public StrategyContext(final StrategyGraph g, final T current) {
        this(g, current, null);
    }

    public StrategyContext(final StrategyGraph g, final T current, final Map<String, Object> environment) {
        if (null == g) throw Graph.Exceptions.argumentCanNotBeNull("g");
        if (null == current) throw Graph.Exceptions.argumentCanNotBeNull("current");

        this.g = g;
        this.current = current;
        this.environment = null == environment ? new HashMap<>() : environment;
    }

    public T getCurrent() {
        return current;
    }

    public Graph getBaseGraph() {
        return g.getBaseGraph();
    }

    public StrategyGraph getStrategyGraph() {
        return g;
    }

    public Map<String, Object> getEnvironment() {
        return Collections.unmodifiableMap(environment);
    }
}
