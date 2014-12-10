package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.util.function.TriFunction;

import java.util.Collections;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * This {@link GraphStrategy} prevents the graph from being modified and will throw a
 * {@link UnsupportedOperationException} if an attempt is made to do so.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ReadOnlyStrategy implements GraphStrategy {
    @Override
    public UnaryOperator<Function<Object[], Vertex>> getAddVertexStrategy(final Strategy.Context<StrategyGraph> ctx) {
        return readOnlyFunction();
    }

    @Override
    public UnaryOperator<TriFunction<String, Vertex, Object[], Edge>> getAddEdgeStrategy(final Strategy.Context<StrategyVertex> ctx) {
        return readOnlyTriFunction();
    }

    @Override
    public <V> UnaryOperator<BiFunction<String, V, Property<V>>> getEdgePropertyStrategy(final Strategy.Context<StrategyEdge> ctx) {
        return (f) -> (t, u) -> {
            throw Exceptions.graphUsesReadOnlyStrategy();
        };
    }

    @Override
    public <V> UnaryOperator<BiFunction<String, V, VertexProperty<V>>> getVertexPropertyStrategy(final Strategy.Context<StrategyVertex> ctx) {
        return (f) -> (t, u) -> {
            throw Exceptions.graphUsesReadOnlyStrategy();
        };
    }

    @Override
    public <V, U> UnaryOperator<BiFunction<String, V, Property<V>>> getVertexPropertyPropertyStrategy(final Strategy.Context<StrategyVertexProperty<U>> ctx) {
        return (f) -> (t, u) -> {
            throw Exceptions.graphUsesReadOnlyStrategy();
        };
    }

    @Override
    public UnaryOperator<Supplier<Void>> getRemoveEdgeStrategy(final Strategy.Context<StrategyEdge> ctx) {
        return readOnlySupplier();
    }

    @Override
    public UnaryOperator<Supplier<Void>> getRemoveVertexStrategy(final Strategy.Context<StrategyVertex> ctx) {
        return readOnlySupplier();
    }

    @Override
    public <V> UnaryOperator<Supplier<Void>> getRemovePropertyStrategy(final Strategy.Context<StrategyProperty<V>> ctx) {
        return readOnlySupplier();
    }

    @Override
    public UnaryOperator<BiConsumer<String, Object>> getVariableSetStrategy(final Strategy.Context<StrategyVariables> ctx) {
        return (f) -> (k, v) -> {
            throw Exceptions.graphUsesReadOnlyStrategy();
        };
    }

    @Override
    public UnaryOperator<Supplier<Map<String, Object>>> getVariableAsMapStrategy(final Strategy.Context<StrategyVariables> ctx) {
        return (f) -> () -> Collections.unmodifiableMap(f.get());
    }

    @Override
    public <V> UnaryOperator<Supplier<Void>> getRemoveVertexPropertyStrategy(final Strategy.Context<StrategyVertexProperty<V>> ctx) {
        return readOnlySupplier();
    }

    @Override
    public String toString() {
        return StringFactory.graphStrategyString(this);
    }

    public static <T> UnaryOperator<Supplier<T>> readOnlySupplier() {
        return (f) -> () -> {
            throw Exceptions.graphUsesReadOnlyStrategy();
        };
    }

    public static <T, U> UnaryOperator<Function<T, U>> readOnlyFunction() {
        return (f) -> (t) -> {
            throw Exceptions.graphUsesReadOnlyStrategy();
        };
    }

    public static <T, U, V, W> UnaryOperator<TriFunction<T, U, V, W>> readOnlyTriFunction() {
        return (f) -> (t, u, v) -> {
            throw Exceptions.graphUsesReadOnlyStrategy();
        };
    }

    public static class Exceptions {
        public static UnsupportedOperationException graphUsesReadOnlyStrategy() {
            return new UnsupportedOperationException(String.format("Graph uses %s and is therefore unmodifiable", ReadOnlyStrategy.class));
        }
    }
}
