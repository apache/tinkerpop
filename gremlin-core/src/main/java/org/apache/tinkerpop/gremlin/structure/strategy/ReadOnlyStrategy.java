/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.structure.strategy;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.function.TriFunction;

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
public final class ReadOnlyStrategy implements GraphStrategy {
    private static final ReadOnlyStrategy instance = new ReadOnlyStrategy();

    private ReadOnlyStrategy() {
    }

    public static final ReadOnlyStrategy instance() {
        return instance;
    }

    @Override
    public UnaryOperator<Function<Object[], Vertex>> getAddVertexStrategy(final StrategyContext<StrategyGraph> ctx, final GraphStrategy composingStrategy) {
        return readOnlyFunction();
    }

    @Override
    public UnaryOperator<TriFunction<String, Vertex, Object[], Edge>> getAddEdgeStrategy(final StrategyContext<StrategyVertex> ctx, final GraphStrategy composingStrategy) {
        return readOnlyTriFunction();
    }

    @Override
    public <V> UnaryOperator<BiFunction<String, V, Property<V>>> getEdgePropertyStrategy(final StrategyContext<StrategyEdge> ctx, final GraphStrategy composingStrategy) {
        return (f) -> (t, u) -> {
            throw Exceptions.graphUsesReadOnlyStrategy();
        };
    }

    @Override
    public <V> UnaryOperator<BiFunction<String, V, VertexProperty<V>>> getVertexPropertyStrategy(final StrategyContext<StrategyVertex> ctx, final GraphStrategy composingStrategy) {
        return (f) -> (t, u) -> {
            throw Exceptions.graphUsesReadOnlyStrategy();
        };
    }

    @Override
    public <V, U> UnaryOperator<BiFunction<String, V, Property<V>>> getVertexPropertyPropertyStrategy(final StrategyContext<StrategyVertexProperty<U>> ctx, final GraphStrategy composingStrategy) {
        return (f) -> (t, u) -> {
            throw Exceptions.graphUsesReadOnlyStrategy();
        };
    }

    @Override
    public UnaryOperator<Supplier<Void>> getRemoveEdgeStrategy(final StrategyContext<StrategyEdge> ctx, final GraphStrategy composingStrategy) {
        return readOnlySupplier();
    }

    @Override
    public UnaryOperator<Supplier<Void>> getRemoveVertexStrategy(final StrategyContext<StrategyVertex> ctx, final GraphStrategy composingStrategy) {
        return readOnlySupplier();
    }

    @Override
    public <V> UnaryOperator<Supplier<Void>> getRemovePropertyStrategy(final StrategyContext<StrategyProperty<V>> ctx, final GraphStrategy composingStrategy) {
        return readOnlySupplier();
    }

    @Override
    public UnaryOperator<BiConsumer<String, Object>> getVariableSetStrategy(final StrategyContext<StrategyVariables> ctx, final GraphStrategy composingStrategy) {
        return (f) -> (k, v) -> {
            throw Exceptions.graphUsesReadOnlyStrategy();
        };
    }

    @Override
    public UnaryOperator<Supplier<Map<String, Object>>> getVariableAsMapStrategy(final StrategyContext<StrategyVariables> ctx, final GraphStrategy composingStrategy) {
        return (f) -> () -> Collections.unmodifiableMap(f.get());
    }

    @Override
    public <V> UnaryOperator<Supplier<Void>> getRemoveVertexPropertyStrategy(final StrategyContext<StrategyVertexProperty<V>> ctx, final GraphStrategy composingStrategy) {
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
