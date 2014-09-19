package com.tinkerpop.gremlin.structure.util.referenced;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.MetaProperty;
import com.tinkerpop.gremlin.structure.Vertex;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ReferencedVertex extends ReferencedElement implements Vertex {

    protected ReferencedVertex() {

    }

    public ReferencedVertex(final Vertex vertex) {
        super(vertex);
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
        throw new IllegalStateException("ReferencedVertices can not have edges:" + this);
    }

    @Override
    public <V> MetaProperty<V> property(String key) {
        throw new IllegalStateException("ReferencedVertices do not have properties:" + this);
    }

    @Override
    public <V> MetaProperty<V> property(String key, V value) {
        throw new IllegalStateException("ReferencedVertices can not have properties:" + this);
    }

    @Override
    public Vertex.Iterators iterators() {
        throw new IllegalStateException("ReferencedVertices do not have iterators:" + this);
    }
}
