package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.query.VertexQuery;
import com.tinkerpop.blueprints.util.StreamFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * A vertex maintains pointers to both a set of incoming and outgoing edges. The outgoing edges are those edges for
 * which the vertex is the tail. The incoming edges are those edges for which the vertex is the head.
 * <p/>
 * Diagrammatically, ---inEdges---> vertex ---outEdges--->.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Vertex extends Element {

    public default Set<String> getPropertyKeys() {
        return this.getProperties().keySet();
    }

    public Map<String, Iterable<Vertex.Property>> getProperties();

    public <V> Iterable<Vertex.Property<V>> getProperties(final String key);

    public default <V> Iterable<V> getValues(final String key) {
        return () -> (Iterator) StreamFactory.stream(getProperties(key)).filter(p -> p.isPresent()).map(p -> p.<V>getValue()).iterator();
    }


    public <V> Vertex.Property<V> getProperty(final String key);

    public <V> Vertex.Property<V> setProperty(final String key, V value);

    public <V> Vertex.Property<V> addProperty(final String key, V value);

    public VertexQuery query();

    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues);

    public interface Property<V> extends com.tinkerpop.blueprints.Property<V> {

        public Set<String> getPropertyKeys();

        public Map<String, com.tinkerpop.blueprints.Property> getProperties();

        public <V2> com.tinkerpop.blueprints.Property<V2> setProperty(final String key, final V2 value);

        public <V2> com.tinkerpop.blueprints.Property<V2> getProperty(final String key);

        public Vertex getVertex();

        public static <V> Vertex.Property<V> empty() {
            return new Vertex.Property<V>() {
                @Override
                public String getKey() {
                    throw Exceptions.propertyDoesNotExist();
                }

                @Override
                public V getValue() throws NoSuchElementException {
                    throw Exceptions.propertyDoesNotExist();
                }

                @Override
                public boolean isPresent() {
                    return false;
                }

                @Override
                public void remove() {
                    throw Exceptions.propertyDoesNotExist();
                }

                @Override
                public Vertex getVertex() {
                    throw Exceptions.propertyDoesNotExist();
                }

                @Override
                public Set<String> getPropertyKeys() {
                    throw Exceptions.propertyDoesNotExist();
                }

                @Override
                public <V2> com.tinkerpop.blueprints.Property<V2> setProperty(final String key, final V2 value) {
                    throw Exceptions.propertyDoesNotExist();
                }

                @Override
                public <V2> com.tinkerpop.blueprints.Property<V2> getProperty(final String key) {
                    throw Exceptions.propertyDoesNotExist();
                }

                @Override
                public Map<String, com.tinkerpop.blueprints.Property> getProperties() {
                    throw Exceptions.propertyDoesNotExist();
                }
            };

        }

    }

    public static class Exceptions extends Element.Exceptions {
        public static IllegalArgumentException propertyKeyReferencesMultipleProperties(final String key) {
            return new IllegalArgumentException("Provided property key references multiple properties: " + key);
        }

        public static IllegalStateException adjacentVerticesCanNotBeQueried() {
            return new IllegalStateException("It is not possible to query() an adjacent vertex in a vertex program");
        }
    }
}
