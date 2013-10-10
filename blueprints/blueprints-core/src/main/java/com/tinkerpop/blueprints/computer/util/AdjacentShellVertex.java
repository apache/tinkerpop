package com.tinkerpop.blueprints.computer.util;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.computer.VertexSystemMemory;
import com.tinkerpop.blueprints.query.VertexQuery;

import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AdjacentShellVertex implements Vertex {

    private final VertexSystemMemory vertexMemory;
    private final Vertex baseVertex;

    public AdjacentShellVertex(final Vertex baseVertex, final VertexSystemMemory vertexMemory) {
        this.baseVertex = baseVertex;
        this.vertexMemory = vertexMemory;
    }

    public Object getId() {
        return this.baseVertex.getId();
    }

    public String toString() {
        return this.baseVertex.toString();
    }

    public boolean equals(Object object) {
        return (object instanceof Vertex) && ((Element) object).getId().equals(this.getId());
    }

    public int hashCode() {
        return this.baseVertex.hashCode();
    }

    public <T> Property<T, Vertex> getProperty(final String key) {
        if (this.vertexMemory.isComputeKey(key))
            return this.vertexMemory.getProperty(this, key);
        else
            throw new IllegalArgumentException("The provided key is not a compute key: " + key);
    }

    public <T> Property<T, Vertex> removeProperty(final String key) {
        throw new UnsupportedOperationException();
    }

    public final Set<String> getPropertyKeys() {
        return this.vertexMemory.getComputeKeys().keySet();
    }

    public <T> Property<T, Vertex> setProperty(final String key, final T value) {
        throw new UnsupportedOperationException();
    }

    public Edge addEdge(final String label, final Vertex inVertex, final Property... properties) {
        throw new UnsupportedOperationException();
    }

    public VertexQuery query() {
        throw new UnsupportedOperationException();
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }


}
