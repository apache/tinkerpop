package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.query.VertexQuery;
import com.tinkerpop.blueprints.util.StreamFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Vertex extends Element {

    public default Set<String> getPropertyKeys() {
        return this.getProperties().keySet();
    }

    public Map<String, Iterable<Property<?, Vertex>>> getProperties();

    public <V> Iterable<Property<V, Vertex>> getProperties(String key);

    public default <V> Iterable<V> getValues(String key) {
        return new Iterable<V>() {
            @Override
            public Iterator<V> iterator() {
                return StreamFactory.stream((Iterable<Property<V, Vertex>>) getProperties(key)).filter(p -> p.isPresent()).map(p -> p.getValue()).iterator();
            }
        };
    }

    public <V> Property<V, Vertex> getProperty(String key);

    public <V> Property<V, Vertex> setProperty(String key, V value);

    public <V> Property<V, Vertex> addProperty(String key, V value);

    public VertexQuery query();

    public Edge addEdge(String label, Vertex inVertex, Property... properties);

    public static Vertex.Features getFeatures() {
        return new Features() {
        };
    }

    public interface Features extends com.tinkerpop.blueprints.Features {
        public default boolean supportsUserSuppliedIds() {
            return true;
        }

        public static IllegalArgumentException propertyKeyReferencesMultipleProperties(final String key) {
            return new IllegalArgumentException("Provided property key references multiple properties: " + key);
        }
    }
}
