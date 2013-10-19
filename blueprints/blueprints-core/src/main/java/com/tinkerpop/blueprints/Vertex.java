package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.query.VertexQuery;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Vertex extends Element {

    public <T> Property<T, Vertex> getProperty(String key);

    public <T> Property<T, Vertex> setProperty(String key, T value);

    public <T> Property<T, Vertex> removeProperty(String key);

    public VertexQuery query();

    public Edge addEdge(String label, Vertex inVertex, Property... properties);

    public default Vertex.Features getFeatures() {
        return new Features() {
        };
    }

    public interface Features extends com.tinkerpop.blueprints.Features {

        public default boolean supportsUserSuppliedIds() {
            return true;
        }

    }
}
