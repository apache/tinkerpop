package com.tinkerpop.gremlin.structure;

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
 */
public interface Edge extends Element {

    public Vertex getVertex(final Direction direction) throws IllegalArgumentException;

    public static class Exceptions extends Element.Exceptions {
        public static IllegalArgumentException edgeLabelCanNotBeNull() {
            return new IllegalArgumentException("Edge label can not be null");
        }

        public static UnsupportedOperationException userSuppliedIdsNotSupported() {
            return new UnsupportedOperationException("Edge does not support user supplied identifiers");
        }
    }
}
