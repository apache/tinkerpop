package com.tinkerpop.gremlin.structure.util.referenced;

import com.tinkerpop.gremlin.process.util.SingleIterator;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.detached.Attachable;
import com.tinkerpop.gremlin.util.StreamFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ReferencedEdge extends ReferencedElement implements Edge, Attachable<Edge> {

    public ReferencedEdge() {

    }

    public ReferencedEdge(final Edge edge) {
        super(edge);
    }

    @Override
    public Edge.Iterators iterators() {
        return Iterators.ITERATORS;
    }

    @Override
    public Edge attach(final Graph hostGraph) {
        return hostGraph.e(this.id());
    }

    @Override
    public Edge attach(final Vertex hostVertex) {
        return StreamFactory.stream(hostVertex.iterators().edges(Direction.OUT, Integer.MAX_VALUE, this.label()))
                .filter(edge -> edge.id().equals(this.id()))
                .findFirst().orElseThrow(() -> new IllegalStateException("The referenced edge does not reference an edge on the host vertex"));
    }

    private static final class Iterators implements Edge.Iterators {

        private static final ReferencedVertex DUMMY_VERTEX = new ReferencedVertex();

        static {
            DUMMY_VERTEX.id = '.';
            DUMMY_VERTEX.label = ".";
        }

        private static final List<Vertex> DUMMY_TWO_VERTEX = Arrays.asList(DUMMY_VERTEX, DUMMY_VERTEX);
        protected static final Iterators ITERATORS = new Iterators();

        @Override
        public Iterator<Vertex> vertices(final Direction direction) {
            if (direction.equals(Direction.BOTH))
                return DUMMY_TWO_VERTEX.iterator();
            else
                return new SingleIterator<>(DUMMY_VERTEX);
        }

        @Override
        public <V> Iterator<Property<V>> properties(String... propertyKeys) {
            return Collections.emptyIterator();
        }

        @Override
        public <V> Iterator<Property<V>> hiddens(String... propertyKeys) {
            return Collections.emptyIterator();
        }
    }
}
