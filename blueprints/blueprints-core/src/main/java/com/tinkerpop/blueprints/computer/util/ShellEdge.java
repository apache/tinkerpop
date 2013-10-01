package com.tinkerpop.blueprints.computer.util;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.computer.VertexSystemMemory;

import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ShellEdge implements Edge {

    private final VertexSystemMemory vertexMemory;
    private Edge baseEdge;

    public ShellEdge(final Edge baseEdge, final VertexSystemMemory vertexMemory) {
        this.baseEdge = baseEdge;
        this.vertexMemory = vertexMemory;
    }

    public Object getId() {
        return this.baseEdge.getId();
    }

    public String toString() {
        return this.baseEdge.toString();
    }

    public boolean equals(Object object) {
        return (object instanceof Edge) && ((Element) object).getId().equals(this.getId());
    }

    public int hashCode() {
        return this.baseEdge.hashCode();
    }

    public String getLabel() {
        return this.baseEdge.getLabel();
    }

    public Set<String> getPropertyKeys() {
        return this.baseEdge.getPropertyKeys();
    }

    public <T> Property<T, Edge> removeProperty(final String key) {
        throw new UnsupportedOperationException();
    }

    public <T> Property<T, Edge> getProperty(final String key) {
        return this.baseEdge.getProperty(key);
    }

    public <T> Property<T, Edge> setProperty(final String key, final T value) {
        throw new UnsupportedOperationException();
    }

    public Vertex getVertex(final Direction direction) {
        return new AdjacentShellVertex(this.baseEdge.getVertex(direction), this.vertexMemory);
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }


}
