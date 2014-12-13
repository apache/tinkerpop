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

    StrategyContext(final StrategyGraph g, final T current, final B currentBase) {
        if (null == g) throw Graph.Exceptions.argumentCanNotBeNull("g");
        if (null == current) throw Graph.Exceptions.argumentCanNotBeNull("current");
        if (null == currentBase) throw Graph.Exceptions.argumentCanNotBeNull("currentBase");

        this.g = g;
        this.baseGraph = g.getBaseGraphSafe();
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
     */
    public StrategyGraph getStrategyGraph() {
        return g;
    }

    /**
     * Constructs a new {@code StrategyContext}.  This odd construction method makes it possible to only be able
     * to create a {@code StrategyContext} if an instance is available (or if the code is in the same package).
     * By adding this security, it becomes possible for {@link com.tinkerpop.gremlin.structure.strategy.SafeStrategy}
     * to work.  If anyone could create a {@link com.tinkerpop.gremlin.structure.strategy.StrategyContext} outside
     * of usage within a {@link com.tinkerpop.gremlin.structure.strategy.GraphStrategy} implementation it would
     * be possible to access the underlying non-strategy enabled elements.
     */
    public <SW extends StrategyWrapped, BASE> StrategyContext<SW, BASE> from(final StrategyGraph g, final SW current, final BASE currentBase) {
        return new StrategyContext<>(g, current, currentBase);
    }
}
