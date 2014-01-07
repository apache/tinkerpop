package com.tinkerpop.blueprints.strategy;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.TriFunction;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ReadOnlyGraphStrategy implements GraphStrategy {
    @Override
    public UnaryOperator<Function<Object[], Vertex>> getAddVertexStrategy() {
        return (f) -> (keyValues) -> { throw new UnsupportedOperationException("readonly"); };
    }

    @Override
    public UnaryOperator<TriFunction<String, Vertex, Object[], Edge>> getAddEdgeStrategy() {
        return (f) -> (label, v, keyValues) -> { throw new UnsupportedOperationException("readonly"); };
    }

    @Override
    public UnaryOperator<Consumer<Vertex>> getRemoveVertexStrategy() {
        return (f) -> (v) -> { throw new UnsupportedOperationException("readonly"); };
    }
}
