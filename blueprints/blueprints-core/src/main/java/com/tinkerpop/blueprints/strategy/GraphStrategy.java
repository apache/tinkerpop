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
    public default UnaryOperator<Function<Object[],Vertex>> getWrapAddVertex() {
        return UnaryOperator.identity();
    }

    public default UnaryOperator<TriFunction<String, Vertex, Object[], Edge>> getWrapAddEdge() {
        return UnaryOperator.identity();
    }

    public default Consumer<Vertex> getPreRemoveVertex() {
        return GraphStrategy::noOpConsumer;
    }

    public default Consumer<Vertex> getPostRemoveVertex() {
        return GraphStrategy::noOpConsumer;
    }

    static <T> void noOpConsumer(final T arg) {}
}
