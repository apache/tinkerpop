package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.query.VertexQuery;
import com.tinkerpop.blueprints.util.StreamFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Vertex extends Element {

    public default Set<String> getPropertyKeys() {
        return this.getProperties().keySet();
    }

    public Map<String, Iterable<Vertex.Property>> getProperties();

    public <V> Iterable<Vertex.Property<V>> getProperties(String key);

    public default <V> Iterable<V> getValues(String key) {
        return () -> (Iterator) StreamFactory.stream(getProperties(key)).filter(p -> p.isPresent()).map(p -> p.<V>getValue()).iterator();
    }


    public <V> Vertex.Property<V> getProperty(String key);

    public <V> Vertex.Property<V> setProperty(String key, V value);

    public <V> Vertex.Property<V> addProperty(String key, V value);

    public VertexQuery query();

    public Edge addEdge(String label, Vertex inVertex, Object... keyValues);

    public static Vertex.Features getFeatures() {
        return new Features() {
        };
    }

    public interface Property<V> extends com.tinkerpop.blueprints.Property<V> {

        public Set<String> getPropertyKeys();

        public Map<String, com.tinkerpop.blueprints.Property> getProperties();

        public <V2> com.tinkerpop.blueprints.Property<V2> setProperty(String key, V2 value);

        public <V2> com.tinkerpop.blueprints.Property<V2> getProperty(String key);

        public Vertex getVertex();

        public static <V> Vertex.Property<V> empty() {
            return new Vertex.Property<V>() {
                @Override
                public String getKey() {
                    throw Features.propertyDoesNotExist();
                }

                @Override
                public V getValue() throws NoSuchElementException {
                    throw Features.propertyDoesNotExist();
                }

                @Override
                public boolean isPresent() {
                    return false;
                }

                @Override
                public void remove() {
                    throw Features.propertyDoesNotExist();
                }

                @Override
                public Vertex getVertex() {
                    throw Features.propertyDoesNotExist();
                }

                @Override
                public Set<String> getPropertyKeys() {
                    throw Features.propertyDoesNotExist();
                }

                @Override
                public <V2> com.tinkerpop.blueprints.Property<V2> setProperty(String key, V2 value) {
                    throw Features.propertyDoesNotExist();
                }

                @Override
                public <V2> com.tinkerpop.blueprints.Property<V2> getProperty(String key) {
                    throw Features.propertyDoesNotExist();
                }


                @Override
                public Map<String, com.tinkerpop.blueprints.Property> getProperties() {
                    throw Features.propertyDoesNotExist();
                }
            };

        }

    }

    public interface Features extends com.tinkerpop.blueprints.Features {
        public default boolean supportsUserSuppliedIds() {
            return true;
        }

        public static IllegalArgumentException propertyKeyReferencesMultipleProperties(final String key) {
            return new IllegalArgumentException("Provided property key references multiple properties: " + key);
        }

        public static IllegalStateException adjacentVerticesCanNotBeQueried() {
            return new IllegalStateException("It is not possible to query() an adjacent vertex in a vertex program");
        }
    }
}
