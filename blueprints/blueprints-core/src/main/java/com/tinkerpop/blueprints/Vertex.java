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
}
