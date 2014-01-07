package com.tinkerpop.blueprints.strategy;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.javatuples.Triplet;

import java.util.function.UnaryOperator;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface GraphStrategy {
    public default UnaryOperator<Object[]> getPreAddVertex() {
        return (f) -> f;
    }

    public default UnaryOperator<Vertex> getPostAddVertex() {
        return (f) -> f;
    }

    public default UnaryOperator<Triplet<String, Vertex, Object[]>> getPreAddEdge() {
        return (f) -> f;
    }

    public default UnaryOperator<Edge> getPostAddEdge() {
        return (f) -> f;
    }
}
