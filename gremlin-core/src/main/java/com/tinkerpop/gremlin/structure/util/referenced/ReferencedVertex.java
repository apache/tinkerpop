package com.tinkerpop.gremlin.structure.util.referenced;

import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.structure.util.detached.Attachable;

import java.util.Collections;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ReferencedVertex extends ReferencedElement implements Vertex, Vertex.Iterators, Attachable<Vertex> {

    protected ReferencedVertex() {

    }

    public ReferencedVertex(final Vertex vertex) {
        super(vertex);
    }

    @Override
    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        throw new IllegalStateException("Referenced vertices can not have edges:" + this);
    }

    @Override
    public <V> VertexProperty<V> property(final String key) {
        throw new IllegalStateException("Referenced vertices do not have properties:" + this);
    }

    @Override
    public <V> VertexProperty<V> property(final String key, V value) {
        throw new IllegalStateException("Referenced vertices can not have properties:" + this);
    }

    @Override
    public Vertex.Iterators iterators() {
        return this;
    }

    @Override
    public Vertex attach(final Graph hostGraph) {
        return hostGraph.v(this.id());
    }

    @Override
    public Vertex attach(final Vertex hostVertex) {
        if (hostVertex.equals(this))
            return hostVertex;
        else
            throw new IllegalStateException("The host vertex must be the referenced vertex to attach: " + this);
    }

    @Override
    public GraphTraversal<Vertex, Vertex> start() {
        throw new UnsupportedOperationException("Referenced vertices cannot be traversed: " + this);
    }


    @Override
    public Iterator<Edge> edgeIterator(final Direction direction, final String... edgeLabels) {
        return Collections.emptyIterator();
    }

    @Override
    public Iterator<Vertex> vertexIterator(final Direction direction, final String... labels) {
        return Collections.emptyIterator();
    }

    @Override
    public <V> Iterator<VertexProperty<V>> propertyIterator(final String... propertyKeys) {
        return Collections.emptyIterator();
    }

    @Override
    public <V> Iterator<VertexProperty<V>> hiddenPropertyIterator(final String... propertyKeys) {
        return Collections.emptyIterator();
    }
}
