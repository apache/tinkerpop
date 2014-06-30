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
     * Set the {@link com.tinkerpop.gremlin.structure.strategy.GraphStrategy} to utilized in the various Blueprints methods that it supports.
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
     * @param impl the base implementation of an operation that does something in a Blueprints implementation.
     * @return a function that will be applied in the Blueprints implementation
     */
    public default <T> T compose(final Function<GraphStrategy, UnaryOperator<T>> f, final T impl) {
        return getGraphStrategy().isPresent() ? f.apply(getGraphStrategy().get()).apply(impl) : impl;
    }

    /**
     * The {@link Context} object is provided to the methods of {@link com.tinkerpop.gremlin.structure.strategy.GraphStrategy} so that the strategy functions
     * it constructs have some knowledge of the environment.
     *
     * @param <T> represents the object that is calling the strategy (i.e. the vertex on which addEdge was called).
     */
    public static class Context<T extends StrategyWrapped> {
        private final Graph g;
        private final Map<String, Object> environment;
        private final T current;

        public Context(final Graph g, final T current) {
            this(g, current, null);
        }

        public Context(final Graph g, final T current, final Map<String, Object> environment) {
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
            return g;
        }

        public Map<String, Object> getEnvironment() {
            return Collections.unmodifiableMap(environment);
        }
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
