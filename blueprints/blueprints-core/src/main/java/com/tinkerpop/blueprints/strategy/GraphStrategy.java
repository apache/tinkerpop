package com.tinkerpop.blueprints.strategy;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Strategy;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.TriFunction;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface GraphStrategy {
    public default UnaryOperator<Function<Object[],Vertex>> getAddVertexStrategy(final Strategy.Context ctx) {
        return UnaryOperator.identity();
    }

    public default UnaryOperator<TriFunction<String, Vertex, Object[], Edge>> getAddEdgeStrategy(final Strategy.Context ctx) {
        return UnaryOperator.identity();
    }

    public default UnaryOperator<Consumer<Vertex>> getRemoveVertexStrategy(final Strategy.Context ctx) {
        return UnaryOperator.identity();
    }
}
