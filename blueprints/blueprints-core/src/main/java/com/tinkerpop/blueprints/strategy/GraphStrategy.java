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
public interface GraphStrategy {
    public default UnaryOperator<Function<Object[],Vertex>> getAddVertexStrategy() {
        return UnaryOperator.identity();
    }

    public default UnaryOperator<TriFunction<String, Vertex, Object[], Edge>> getAddEdgeStrategy() {
        return UnaryOperator.identity();
    }

    public default UnaryOperator<Consumer<Vertex>> getRemoveVertexStrategy() {
        return UnaryOperator.identity();
    }
}
