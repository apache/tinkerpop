package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.strategy.GraphStrategy;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface Strategy {
    public void set(final Optional<GraphStrategy> strategy);
    public Optional<GraphStrategy> get();

    public default <T> T ifPresent(final Function<GraphStrategy, T> f, final T args) {
        return get().isPresent() ? f.apply(get().get()) : args;
    }
}
