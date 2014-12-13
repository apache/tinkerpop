package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.structure.util.StringFactory;

/**
 * A pass through implementation of {@link GraphStrategy} where all strategy functions are simply executed as
 * they were originally implemented.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class IdentityStrategy implements GraphStrategy {
    private static final IdentityStrategy instance = new IdentityStrategy();

    private IdentityStrategy() {
    }

    @Override
    public String toString() {
        return StringFactory.graphStrategyString(this);
    }

    public static final IdentityStrategy instance() {
        return instance;
    }
}
