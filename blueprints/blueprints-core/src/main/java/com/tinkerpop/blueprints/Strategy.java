package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.strategy.GraphStrategy;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface Strategy {
    public void set(final Optional<? extends GraphStrategy> strategy);
    public Optional<? extends GraphStrategy> get();

    /**
     * If a {@link Strategy} is present, then return a {@link GraphStrategy} function that takes the function of the
     * Blueprints implementation denoted by {@code T} as an argument and returns back a function with {@code T}. If
     * no {@link Strategy} is present then it simply returns the {@code impl} as the default.
     *
     * @param f a function to execute if a {@link GraphStrategy} is present.
     * @param impl the base implementation of an operation that does something in a Blueprints implementation.
     * @return a function that will be applied in the Blueprints implementation
     */
    public default <T> T compose(final Function<GraphStrategy, UnaryOperator<T>> f, final T impl) {
        return get().isPresent() ? f.apply(get().get()).apply(impl) : impl;
    }

    public static class Context<T> {
        private final Graph g;
        private final Map<String,Object> environment;
        private final T current;

        public Context(final Graph g, final T current) {
            this(g, current, Optional.empty());
        }

        public Context(final Graph g, final T current, final Optional<Map<String,Object>> environment) {
            if (null == g)
                throw new IllegalArgumentException("g");

            this.g = g;
            this.current = current;
            this.environment = environment.orElse(new HashMap<>());
        }

        public T getCurrent() {
            return current;
        }

        public Graph getG() {
            return g;
        }

        public Map<String, Object> getEnvironment() {
            return Collections.unmodifiableMap(environment);
        }
    }

    public static class Simple implements Strategy {
        private Optional<? extends GraphStrategy> strategy;

        @Override
        public void set(final Optional<? extends GraphStrategy> strategy) {
            this.strategy = strategy;
        }

        @Override
        public Optional<? extends GraphStrategy> get() {
            return strategy;
        }
    }

    public static class None implements Strategy {
        @Override
        public void set(final Optional<? extends GraphStrategy> strategy) {
            throw new UnsupportedOperationException("Strategy is not supported by this implementation");
        }

        @Override
        public Optional<GraphStrategy> get() {
            return Optional.empty();
        }
    }

    public static class Local implements Strategy {
        private ThreadLocal<Optional<? extends GraphStrategy>> strategy = new ThreadLocal<Optional<? extends GraphStrategy>>(){
            @Override
            protected Optional<GraphStrategy> initialValue() {
                return Optional.empty();
            }
        };

        @Override
        public void set(final Optional<? extends GraphStrategy> strategy) {
            this.strategy.set(strategy);
        }

        @Override
        public Optional<? extends GraphStrategy> get() {
            return this.strategy.get();
        }
    }
}
