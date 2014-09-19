package com.tinkerpop.gremlin.structure.util.referenced;

import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.detached.Attachable;

import java.util.Collections;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ReferencedVertex extends ReferencedElement implements Vertex, Attachable<Vertex> {

    protected ReferencedVertex() {

    }

    public ReferencedVertex(final Vertex vertex) {
        super(vertex);
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
        throw new IllegalStateException("Referenced vertices can not have edges:" + this);
    }

    @Override
    public <V> VertexProperty<V> property(String key) {
        throw new IllegalStateException("Referenced vertices do not have properties:" + this);
    }

    @Override
    public <V> VertexProperty<V> property(String key, V value) {
        throw new IllegalStateException("Referenced vertices can not have properties:" + this);
    }

    @Override
    public Vertex.Iterators iterators() {
        return Iterators.ITERATORS;
    }

    @Override
    public Vertex attach(final Graph hostGraph) {
        return hostGraph.v(this.id());
    }

    @Override
    public Vertex attach(final Vertex hostVertex) {
        if (hostVertex.id().toString().equals(this.id().toString()))
            return hostVertex;
        else
            throw new IllegalStateException("The host vertex must be the referenced vertex to attach: " + this);
    }

    private static final class Iterators implements Vertex.Iterators {

        protected static final Iterators ITERATORS = new Iterators();

        @Override
        public Iterator<Edge> edges(Direction direction, int branchFactor, String... labels) {
            return Collections.emptyIterator();
        }

        @Override
        public Iterator<Vertex> vertices(Direction direction, int branchFactor, String... labels) {
            return Collections.emptyIterator();
        }

        @Override
        public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
            return Collections.emptyIterator();
        }

        @Override
        public <V> Iterator<VertexProperty<V>> hiddens(String... propertyKeys) {
            return Collections.emptyIterator();
        }
    }
}
