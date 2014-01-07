package com.tinkerpop.blueprints.strategy;

import com.tinkerpop.blueprints.Strategy;

import java.util.Optional;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DefaultStrategy implements Strategy {
    private Optional<GraphStrategy> strategy;

    @Override
    public void set(final Optional<GraphStrategy> strategy) {
        this.strategy = strategy;
    }

    @Override
    public Optional<GraphStrategy> get() {
        return strategy;
    }
}
