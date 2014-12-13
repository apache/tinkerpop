package com.tinkerpop.gremlin.structure.strategy;

/**
 * A marker {@link GraphStrategy} that makes it so that the {@link StrategyGraph} goes into "safe mode" thus
 * preventing access to the underlying {@link com.tinkerpop.gremlin.structure.Graph}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class SafeStrategy implements GraphStrategy {
    private static final SafeStrategy instance = new SafeStrategy();

    public static SafeStrategy instance() {
        return instance;
    }
}
