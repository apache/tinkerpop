package com.tinkerpop.blueprints;

/**
 * An Edge links two vertices. Along with its Property objects, an edge has both a directionality and a label.
 * The directionality determines which vertex is the tail vertex (out vertex) and which vertex is the head vertex
 * (in vertex). The edge label determines the type of relationship that exists between the two vertices.
 * <p/>
 * Diagrammatically, outVertex ---label---> inVertex.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Edge extends Element {

    public Vertex getVertex(final Direction direction) throws IllegalArgumentException;

    public static class Exceptions extends Element.Exceptions {
        public static IllegalArgumentException edgeLabelCanNotBeNull() {
            return new IllegalArgumentException("Edge label can not be null");
        }

        public static IllegalStateException edgePropertiesCanNotHaveProperties() {
            return new IllegalStateException("Edge properties can not have properties");
        }
    }
}
