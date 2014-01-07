package com.tinkerpop.blueprints.strategy;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.javatuples.Triplet;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface GraphStrategy {
    public default UnaryOperator<Object[]> getPreAddVertex() {
        return UnaryOperator.identity();
    }

    public default UnaryOperator<Function<Object[],Vertex>> getWrapAddVertex() {
        return UnaryOperator.identity();
    }

    public default UnaryOperator<Vertex> getPostAddVertex() {
        return UnaryOperator.identity();
    }

    public default UnaryOperator<Triplet<String, Vertex, Object[]>> getPreAddEdge() {
        return UnaryOperator.identity();
    }

    public default UnaryOperator<Edge> getPostAddEdge() {
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
