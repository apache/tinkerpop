package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.strategy.GraphStrategy;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * A holder of a {@link com.tinkerpop.blueprints.strategy.GraphStrategy} object owned by a {@link Graph} instance.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface Strategy  {

    /**
     * Set the {@link com.tinkerpop.blueprints.strategy.GraphStrategy} to utilized in the various Blueprints methods that it supports.  Set to
     * {@link java.util.Optional#EMPTY} by default.
     */
    public void setGraphStrategy(final Optional<? extends GraphStrategy> strategy);

    /**
     * Gets the {@link com.tinkerpop.blueprints.strategy.GraphStrategy} for the {@link Graph}.
     */
    public Optional<? extends GraphStrategy> getGraphStrategy();

    /**
     * If a {@link Strategy} is present, then return a {@link com.tinkerpop.blueprints.strategy.GraphStrategy} function that takes the function of the
     * Blueprints implementation denoted by {@code T} as an argument and returns back a function with {@code T}. If
     * no {@link Strategy} is present then it simply returns the {@code impl} as the default.
     *
     * @param f a function to execute if a {@link com.tinkerpop.blueprints.strategy.GraphStrategy} is present.
     * @param impl the base implementation of an operation that does something in a Blueprints implementation.
     * @return a function that will be applied in the Blueprints implementation
     */
    public default <T> T compose(final Function<GraphStrategy, UnaryOperator<T>> f, final T impl) {
        return getGraphStrategy().isPresent() ? f.apply(getGraphStrategy().get()).apply(impl) : impl;
    }

    /**
     * The {@link Context} object is provided to the methods of {@link com.tinkerpop.blueprints.strategy.GraphStrategy} so that the strategy functions
     * it constructs have some knowledge of the environment.
     *
     * @param <T> represents the object that is calling the strategy (i.e. the vertex on which addEdge was called).
     */
    public static class Context<T>  {
        private final Graph g;
        private final Map<String,Object> environment;
        private final T current;

        public Context(final Graph g, final T current) {
            this(g, current, Optional.empty());
        }

        public Context(final Graph g, final T current, final Optional<Map<String,Object>> environment) {
            if (null == g)
                throw Graph.Exceptions.argumentCanNotBeNull("g");
            if (null == current)
                throw Graph.Exceptions.argumentCanNotBeNull("current");
            if (null == environment)
                throw Graph.Exceptions.argumentCanNotBeNull("environment");

            this.g = g;
            this.current = current;
            this.environment = environment.orElse(new HashMap<>());
        }

        public T getCurrent() {
            return current;
        }

        public Graph getGraph() {
            return g;
        }

        public Map<String, Object> getEnvironment() {
            return Collections.unmodifiableMap(environment);
        }
    }

    /**
     * Basic {@link Strategy} implementation where the {@link com.tinkerpop.blueprints.strategy.GraphStrategy} can be get or set.
     */
    public static class Simple implements Strategy {
        private Optional<? extends GraphStrategy> strategy = Optional.empty();

        @Override
        public void setGraphStrategy(final Optional<? extends GraphStrategy> strategy) {
            if (null == strategy)
                throw Graph.Exceptions.argumentCanNotBeNull("strategy");
            this.strategy = strategy;
        }

        @Override
        public Optional<? extends GraphStrategy> getGraphStrategy() {
            return strategy;
        }
    }

    /**
     * A {@link Strategy} implementation where the {@link com.tinkerpop.blueprints.strategy.GraphStrategy} can be get or set as {@link ThreadLocal}.
     */
    public static class Local implements Strategy {
        private ThreadLocal<Optional<? extends GraphStrategy>> strategy = new ThreadLocal<Optional<? extends GraphStrategy>>(){
            @Override
            protected Optional<GraphStrategy> initialValue() {
                return Optional.empty();
            }
        };

        @Override
        public void setGraphStrategy(final Optional<? extends GraphStrategy> strategy) {
            if (null == strategy)
                throw Graph.Exceptions.argumentCanNotBeNull("strategy");
            this.strategy.set(strategy);
        }

        @Override
        public Optional<? extends GraphStrategy> getGraphStrategy() {
            return this.strategy.get();
        }
    }
}
