package com.tinkerpop.gremlin.structure.util.referenced;

import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.util.DoubleIterator;
import com.tinkerpop.gremlin.process.util.SingleIterator;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.detached.Attachable;
import com.tinkerpop.gremlin.util.StreamFactory;

import java.util.Collections;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ReferencedEdge extends ReferencedElement implements Edge, Edge.Iterators, Attachable<Edge> {

    protected ReferencedVertex inVertex;
    protected ReferencedVertex outVertex;

    public ReferencedEdge() {

    }

    public ReferencedEdge(final Edge edge) {
        super(edge);
        this.inVertex = ReferencedFactory.detach(edge.iterators().vertexIterator(Direction.IN).next());
        this.outVertex = ReferencedFactory.detach(edge.iterators().vertexIterator(Direction.OUT).next());
    }

    @Override
    public Edge.Iterators iterators() {
        return this;
    }

    @Override
    public Edge attach(final Graph hostGraph) {
        return hostGraph.e(this.id());
    }

    @Override
    public Edge attach(final Vertex hostVertex) {
        return StreamFactory.stream(hostVertex.iterators().edgeIterator(Direction.OUT, this.label()))
                .filter(edge -> edge.equals(this))
                .findAny().orElseThrow(() -> new IllegalStateException("The referenced edge does not reference an edge on the host vertex"));
    }

    @Override
    public GraphTraversal<Edge, Edge> start() {
        throw new UnsupportedOperationException("Referenced edges cannot be traversed: " + this);
    }

    @Override
    public Iterator<Vertex> vertexIterator(final Direction direction) {
        switch (direction) {
            case OUT:
                return new SingleIterator<>(this.outVertex);
            case IN:
                return new SingleIterator<>(this.inVertex);
            default:
                return new DoubleIterator<>(this.outVertex, this.inVertex);
        }
    }

    @Override
    public <V> Iterator<Property<V>> propertyIterator(final String... propertyKeys) {
        return Collections.emptyIterator();
    }
}
