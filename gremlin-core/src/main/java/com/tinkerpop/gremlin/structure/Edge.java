package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.process.graph.EdgeTraversal;

import java.util.Iterator;

/**
 * An {@link Edge} links two {@link Vertex} objects. Along with its {@link Property} objects, an {@link Edge} has both
 * a {@link Direction} and a {@code label}. The {@link Direction} determines which {@link Vertex} is the tail
 * {@link Vertex} (out {@link Vertex}) and which {@link Vertex} is the head {@link Vertex}
 * (in {@link Vertex}). The {@link Edge} {@code label} determines the type of relationship that exists between the
 * two vertices.
 * <p/>
 * Diagrammatically:
 * <pre>
 * outVertex ---label---> inVertex.
 * </pre>
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface Edge extends Element, EdgeTraversal {

    /**
     * The default label to use for an edge.
     * This is typically never used as when an edge is created, an edge label is required to be specified.
     */
    public static final String DEFAULT_LABEL = "edge";

    /**
     * Gets the {@link Edge.Iterators} set.
     *
     * {@inheritDoc}
     */
    @Override
    public Edge.Iterators iterators();

    /**
     * An interface that provides access to iterators over properties and vertices, without constructing a
     * {@link com.tinkerpop.gremlin.process.Traversal} object.
     */
    public interface Iterators extends Element.Iterators {

        /**
         * Retrieve the vertex (or vertices) associated with this edge as defined by the direction.
         *
         * @param direction Get the incoming vertex, outgoing vertex, or both vertices
         * @return An iterator with 1 or 2 vertices
         */
        public Iterator<Vertex> vertexIterator(final Direction direction);

        /**
         * {@inheritDoc}
         */
        @Override
        public <V> Iterator<Property<V>> propertyIterator(final String... propertyKeys);

        /**
         * {@inheritDoc}
         */
        @Override
        public <V> Iterator<Property<V>> hiddenPropertyIterator(final String... propertyKeys);
    }

    /**
     * Common exceptions to use with an edge.
     */
    public static class Exceptions extends Element.Exceptions {

        public static UnsupportedOperationException userSuppliedIdsNotSupported() {
            return new UnsupportedOperationException("Edge does not support user supplied identifiers");
        }

        public static UnsupportedOperationException userSuppliedIdsOfThisTypeNotSupported() {
            return new UnsupportedOperationException("Edge does not support user supplied identifiers of this type");
        }

        public static IllegalStateException edgeRemovalNotSupported() {
            return new IllegalStateException("Edge removal are not supported");
        }
    }
}
