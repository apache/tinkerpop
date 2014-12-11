package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.structure.Graph;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * A traverser of a {@link com.tinkerpop.gremlin.structure.strategy.GraphStrategy} object owned by a {@link com.tinkerpop.gremlin.structure.Graph} instance.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface Strategy {

    /**
     * Set the {@link com.tinkerpop.gremlin.structure.strategy.GraphStrategy} to utilized in the various Gremlin Structure methods that it supports.
     */
    public void setGraphStrategy(final GraphStrategy strategy);

    /**
     * Gets the {@link com.tinkerpop.gremlin.structure.strategy.GraphStrategy} for the {@link com.tinkerpop.gremlin.structure.Graph}.
     */
    public Optional<GraphStrategy> getGraphStrategy();

    /**
     * If a {@link Strategy} is present, then return a {@link com.tinkerpop.gremlin.structure.strategy.GraphStrategy} function that takes the function of the
     * Gremlin Structure implementation denoted by {@code T} as an argument and returns back a function with {@code T}. If
     * no {@link Strategy} is present then it simply returns the {@code impl} as the default.
     *
     * @param f    a function to execute if a {@link com.tinkerpop.gremlin.structure.strategy.GraphStrategy} is present.
     * @param impl the base implementation of an operation that does something in a Gremlin Structure implementation.
     * @return a function that will be applied in the Gremlin Structure implementation
     */
    public default <T> T compose(final Function<GraphStrategy, UnaryOperator<T>> f, final T impl) {
        return getGraphStrategy().isPresent() ? f.apply(getGraphStrategy().get()).apply(impl) : impl;
    }

    /**
     * Basic {@link Strategy} implementation where the {@link com.tinkerpop.gremlin.structure.strategy.GraphStrategy} can be get or set.
     */
    public static class Simple implements Strategy {
        private GraphStrategy strategy = null;

        @Override
        public void setGraphStrategy(final GraphStrategy strategy) {
            this.strategy = strategy;
        }

        @Override
        public Optional<GraphStrategy> getGraphStrategy() {
            return Optional.ofNullable(strategy);
        }
    }

    /**
     * A {@link Strategy} implementation where the {@link com.tinkerpop.gremlin.structure.strategy.GraphStrategy} can be get or set as {@link ThreadLocal}.
     */
    public static class Local implements Strategy {
        private ThreadLocal<GraphStrategy> strategy = new ThreadLocal<GraphStrategy>() {
            @Override
            protected GraphStrategy initialValue() {
                return null;
            }
        };

        @Override
        public void setGraphStrategy(final GraphStrategy strategy) {
            this.strategy.set(strategy);
        }

        @Override
        public Optional<GraphStrategy> getGraphStrategy() {
            return Optional.ofNullable(this.strategy.get());
        }
    }
}
