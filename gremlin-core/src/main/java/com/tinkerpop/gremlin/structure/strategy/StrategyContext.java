package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.structure.Graph;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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
    private final Map<String, Object> environment;
    private final T current;
    private final B currentBase;

    StrategyContext(final StrategyGraph g, final T current, final B currentBase) {
        this(g, current, currentBase, null);
    }

    StrategyContext(final StrategyGraph g, final T current, final B currentBase, final Map<String, Object> environment) {
        if (null == g) throw Graph.Exceptions.argumentCanNotBeNull("g");
        if (null == current) throw Graph.Exceptions.argumentCanNotBeNull("current");
        if (null == currentBase) throw Graph.Exceptions.argumentCanNotBeNull("currentBase");

        this.g = g;
        this.baseGraph = g.getBaseGraphSafe();
        this.current = current;
        this.currentBase = currentBase;
        this.environment = null == environment ? new HashMap<>() : environment;
    }

    /**
     * Gets the {@link StrategyWrapped} instance that is triggering the {@link GraphStrategy} method.
     */
    public T getCurrent() {
        return current;
    }

    /**
     * Gets the base object of the {@link StrategyWrapped} instances that is triggering the {@link GraphStrategy}
     * method.  If a {@link GraphStrategy} implementation needs this instance, it is better to grab the reference
     * from this method than to try to access it via the instance returned from {@link #getCurrent} as a "safe"
     * {@link StrategyGraph} will throw an exception
     * (see {@link com.tinkerpop.gremlin.structure.strategy.SafeStrategy}).
     */
    public B getCurrentBase() {
        return currentBase;
    }

    /**
     * Gets the base {@link Graph} of the {@link StrategyGraph} instance.  If a {@link GraphStrategy} implementation
     * needs this instance, it is better to grab the reference from this method than to try to access it via the
     * instance returned from {@link #getCurrent} as a "safe" {@link StrategyGraph} will throw an exception
     * (see {@link com.tinkerpop.gremlin.structure.strategy.SafeStrategy}).
     */
    public Graph getBaseGraph() {
        return baseGraph;
    }

    /**
     * Gets the current {@link com.tinkerpop.gremlin.structure.strategy.StrategyGraph} instance.
     * @return
     */
    public StrategyGraph getStrategyGraph() {
        return g;
    }

    public Map<String, Object> getEnvironment() {
        return Collections.unmodifiableMap(environment);
    }
}
