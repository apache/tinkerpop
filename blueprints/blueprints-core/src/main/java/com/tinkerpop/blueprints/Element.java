package com.tinkerpop.blueprints;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * An Element is the base class for both vertices and edges. An element has an identifier that must be unique to its
 * inheriting classes (vertex or edges). An element can maintain a collection of Property objects.  Typically, objects
 * are Java primitives (e.g. String, long, int, boolean, etc.)
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract interface Element {

    public Object getId();

    public String getLabel();

    public void remove();

    public default Set<String> getPropertyKeys() {
        return this.getProperties().keySet();
    }

    public Map<String, ?> getProperties();

    public <V> Property<V> getProperty(final String key);

    public <V> Property<V> setProperty(final String key, final V value);

    public default <V> V getValue(String key) throws NoSuchElementException {
        final Property<V> property = this.getProperty(key);
        if (property.isPresent())
            return property.getValue();
        else throw Property.Exceptions.propertyDoesNotExist();
    }

    public static class Exceptions {

        public static IllegalArgumentException bothIsNotSupported() {
            return new IllegalArgumentException("A direction of BOTH is not supported");
        }

        public static IllegalArgumentException providedKeyValuesMustBeAMultipleOfTwo() {
            return new IllegalArgumentException("The provided key/value array must be a multiple of two");
        }

        public static IllegalArgumentException providedKeyValuesMustHaveALegalKeyOnEvenIndices() {
            return new IllegalArgumentException("The provided key/value array must have a String key or Property.Key on even array indices");
        }
    }

}
