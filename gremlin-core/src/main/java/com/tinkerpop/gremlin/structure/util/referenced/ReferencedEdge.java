package com.tinkerpop.gremlin.structure.util.referenced;

import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.detached.Attachable;
import com.tinkerpop.gremlin.util.StreamFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ReferencedEdge extends ReferencedElement implements Edge, Attachable<Edge> {

    protected ReferencedVertex inVertex;
    protected ReferencedVertex outVertex;

    public ReferencedEdge() {

    }

    public ReferencedEdge(final Edge edge) {
        super(edge);
        this.inVertex = ReferencedFactory.detach(edge.iterators().vertices(Direction.IN).next());
        this.outVertex = ReferencedFactory.detach(edge.iterators().vertices(Direction.OUT).next());
    }

    @Override
    public Edge.Iterators iterators() {
        return ITERATORS;
    }

    @Override
    public Edge attach(final Graph hostGraph) {
        return hostGraph.e(this.id());
    }

    @Override
    public Edge attach(final Vertex hostVertex) {
        return StreamFactory.stream(hostVertex.iterators().edges(Direction.OUT, Integer.MAX_VALUE, this.label()))
                .filter(edge -> edge.equals(this))
                .findFirst().orElseThrow(() -> new IllegalStateException("The referenced edge does not reference an edge on the host vertex"));
    }

    private final Iterators ITERATORS = new Iterators();

    private class Iterators implements Edge.Iterators, Serializable {

        @Override
        public Iterator<Vertex> vertices(final Direction direction) {
            final List<Vertex> vertices = new ArrayList<>();
            if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
                 vertices.add(ReferencedEdge.this.inVertex);
            }
            if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
                vertices.add(ReferencedEdge.this.outVertex);
            }
            return vertices.iterator();
        }

        @Override
        public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
            return Collections.emptyIterator();
        }

        @Override
        public <V> Iterator<Property<V>> hiddens(final String... propertyKeys) {
            return Collections.emptyIterator();
        }
    }
}
