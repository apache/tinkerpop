package com.tinkerpop.blueprints;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
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

    public <V> Property<V> getProperty(String key);

    public <V> Property<V> setProperty(String key, V value);

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
