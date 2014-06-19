package com.tinkerpop.gremlin.structure.util.cached;

import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Map;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class CachedVertex extends CachedElement implements Vertex {

    public CachedVertex(final Object id, final String label) {
        super(id, label);
    }

    public CachedVertex(final Object id, final String label, final Map<String, Object> properties, final Map<String, Object> hiddenProperties) {
        super(id, label, properties, hiddenProperties);
    }

    public CachedVertex(final Vertex vertex) {
        super(vertex);
    }

    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        throw new UnsupportedOperationException("Cached vertices do not store edges: " + this.toString());
    }

    public String toString() {
        return StringFactory.vertexString(this);
    }

    public GraphTraversal<Vertex, Vertex> as(final String as) {
        throw new IllegalStateException();
    }

    public GraphTraversal<Vertex, Edge> toE(final Direction direction, final int branchFactor, final String... labels) {
        throw new IllegalStateException();
    }

    public GraphTraversal<Vertex, Vertex> to(final Direction direction, final int branchFactor, final String... labels) {
        throw new IllegalStateException();
    }
}
