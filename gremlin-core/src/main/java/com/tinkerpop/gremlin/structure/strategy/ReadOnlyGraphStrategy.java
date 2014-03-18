package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.function.TriFunction;

import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * This {@link GraphStrategy} prevents the graph from being modified and will throw a
 * {@link UnsupportedOperationException} if an attempt is made to do so.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ReadOnlyGraphStrategy implements GraphStrategy {
    @Override
    public UnaryOperator<Function<Object[], Vertex>> getAddVertexStrategy(final Strategy.Context<StrategyWrappedGraph> ctx) {
        return readOnlyFunction();
    }

    @Override
    public UnaryOperator<TriFunction<String, Vertex, Object[], Edge>> getAddEdgeStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return readOnlyTriFunction();
    }

    @Override
    public <V> UnaryOperator<BiConsumer<String, V>> getElementSetProperty(Strategy.Context<? extends StrategyWrappedElement> ctx) {
        return readOnlyBiConsumer();
    }

    @Override
    public UnaryOperator<Supplier<Void>> getRemoveElementStrategy(final Strategy.Context<? extends StrategyWrappedElement> ctx) {
        return readOnlySupplier();
    }

    @Override
    public <V> UnaryOperator<Supplier<Void>> getRemovePropertyStrategy(final Strategy.Context<StrategyWrappedProperty<V>> ctx) {
        return readOnlySupplier();
    }

    public static <T> UnaryOperator<Supplier<T>> readOnlySupplier() {
        return (f) -> () -> { throw Exceptions.graphUsesReadOnlyStrategy(); };
    }

    public static <T, U> UnaryOperator<Function<T, U>> readOnlyFunction() {
        return (f) -> (t) -> { throw Exceptions.graphUsesReadOnlyStrategy(); };
    }

    public static <T, U> UnaryOperator<BiConsumer<T, U>> readOnlyBiConsumer() {
        return (f) -> (t,u) -> { throw Exceptions.graphUsesReadOnlyStrategy(); };
    }

    public static <T, U, V, W> UnaryOperator<TriFunction<T, U, V, W>> readOnlyTriFunction() {
        return (f) -> (t, u, v) -> { throw Exceptions.graphUsesReadOnlyStrategy(); };
    }

    public static class Exceptions {
        public static UnsupportedOperationException graphUsesReadOnlyStrategy() {
            return new UnsupportedOperationException(String.format("Graph uses %s and is therefore unmodifiable", ReadOnlyGraphStrategy.class));
        }
    }
}
