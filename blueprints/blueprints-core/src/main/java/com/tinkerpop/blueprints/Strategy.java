package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.strategy.GraphStrategy;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface Strategy {
    public void set(final Optional<? extends GraphStrategy> strategy);
    public Optional<? extends GraphStrategy> get();

    /**
     * A variation on {@link Optional#ifPresent(java.util.function.Consumer)} where the argument is a
     * {@link Function} instead of a {@link java.util.function.Consumer} that can only handle side-effects. The
     * {@link Function} is only applied if a {@link GraphStrategy} is present.   When applied the {@link GraphStrategy}
     * becomes the first argument to this function and the return value should be equivalent to the {@code defaultVal}
     * that will be returned if the {@link GraphStrategy} is empty.
     *
     * @param f a function to execute if a {@link GraphStrategy} is present.
     * @param defaultVal the value to return if no {@link GraphStrategy} is present.
     */
    public default <T> T ifPresent(final Function<GraphStrategy, T> f, final T defaultVal) {
        return get().isPresent() ? f.apply(get().get()) : defaultVal;
    }

    public default void ifPresent(final Consumer<GraphStrategy> f) {
        get().ifPresent(f);
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
