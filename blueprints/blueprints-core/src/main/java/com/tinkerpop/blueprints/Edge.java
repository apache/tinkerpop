package com.tinkerpop.blueprints;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

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

    public Vertex getVertex(Direction direction) throws IllegalArgumentException;

    public default Set<String> getPropertyKeys() {
        return this.getProperties().keySet();
    }

    public Map<String, Edge.Property> getProperties();

    public <V> Edge.Property<V> getProperty(String key);

    public <V> Edge.Property<V> setProperty(String key, V value);

    public interface Property<V> extends com.tinkerpop.blueprints.Property<V> {

        public Edge getEdge();

        public static <V> Edge.Property<V> empty() {
            return new Edge.Property<V>() {
                @Override
                public String getKey() {
                    throw Property.Exceptions.propertyDoesNotExist();
                }

                @Override
                public V getValue() throws NoSuchElementException {
                    throw Property.Exceptions.propertyDoesNotExist();
                }

                @Override
                public boolean isPresent() {
                    return false;
                }

                @Override
                public void remove() {
                    throw Property.Exceptions.propertyDoesNotExist();
                }

                @Override
                public Edge getEdge() {
                    throw Property.Exceptions.propertyDoesNotExist();
                }
            };
        }
    }

    public static class Exceptions extends Element.Exceptions {
        public static IllegalArgumentException edgeLabelCanNotBeNull() {
            return new IllegalArgumentException("Edge label can not be null");
        }

        public static IllegalStateException edgePropertiesCanNotHaveProperties() {
            return new IllegalStateException("Edge properties can not have properties");
        }
    }
}
