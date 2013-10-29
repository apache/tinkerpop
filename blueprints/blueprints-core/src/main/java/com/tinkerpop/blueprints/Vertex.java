package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.query.VertexQuery;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Vertex extends Element {

    public <V> Property<V, Vertex> getProperty(String key);

    public <V> Property<V, Vertex> setProperty(String key, V value);

    public <V> Property<V, Vertex> removeProperty(String key);

    public VertexQuery query();

    public Edge addEdge(String label, Vertex inVertex, Property... properties);

    public static Vertex.Features getFeatures() {
        return new Features() {
        };
    }

    public interface Features extends com.tinkerpop.blueprints.Features {

        public default boolean supportsUserSuppliedIds() {
            return true;
        }

    }
}
