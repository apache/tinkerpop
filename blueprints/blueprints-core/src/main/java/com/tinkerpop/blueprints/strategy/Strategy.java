package com.tinkerpop.blueprints.strategy;

import com.tinkerpop.blueprints.Vertex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface Strategy {
    public default Function<Object[], Object[]> getPreAddVertex() {
        return (f) -> f;
    }

    public default Function<Vertex, Vertex> getPostAddVertex() {
        return (f) -> f;
    }
}
