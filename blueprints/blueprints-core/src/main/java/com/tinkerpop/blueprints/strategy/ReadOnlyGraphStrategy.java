package com.tinkerpop.blueprints.strategy;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Strategy;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.function.TriFunction;

import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ReadOnlyGraphStrategy implements GraphStrategy {
    @Override
    public UnaryOperator<Function<Object[], Vertex>> getAddVertexStrategy(final Strategy.Context<Graph> ctx) {
        return (f) -> (keyValues) -> { throw new UnsupportedOperationException("readonly"); };
    }

    @Override
    public UnaryOperator<TriFunction<String, Vertex, Object[], Edge>> getAddEdgeStrategy(final Strategy.Context<Vertex> ctx) {
        return (f) -> (label, v, keyValues) -> { throw new UnsupportedOperationException("readonly"); };
    }

    @Override
    public UnaryOperator<Supplier<Void>> getRemoveVertexStrategy(final Strategy.Context<Vertex> ctx) {
        return (f) -> () -> { throw new UnsupportedOperationException("readonly"); };
    }
}
