package com.tinkerpop.blueprints.strategy;

import com.tinkerpop.blueprints.Vertex;

import java.util.function.Consumer;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ReadOnlyGraphStrategy implements GraphStrategy {
    @Override
    public Consumer<Vertex> getPreRemoveVertex() {
        throw new UnsupportedOperationException("readonly");
    }
}
