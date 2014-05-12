package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * An {@link Element} is the base class for both {@link Vertex} and {@link Edge}. An {@link Element} has an identifier
 * that must be unique to its inheriting classes ({@link Vertex} or {@link Edge}). An {@link Element} can maintain a
 * collection of {@link Property} objects.  Typically, objects are Java primitives (e.g. String, long, int, boolean,
 * etc.)
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract interface Element {

    public static final String ID = "id";
    public static final String LABEL = "label";
    public static final String DEFAULT_LABEL = "default";

    public Object getId();

    public String getLabel();

    public void remove();

    // todo: make sure id/label get returned as properties

    public default Set<String> getPropertyKeys() {
        return this.getProperties().keySet();
    }

    public default Set<String> getHiddenKeys() {
        return this.getHiddens().keySet();
    }

    public Map<String, Property> getProperties();

    public Map<String, Property> getHiddens();

    public <V> Property<V> getProperty(final String key);

    public <V> Property<V> setProperty(final String key, final V value);

    public default void setProperties(final Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        ElementHelper.attachProperties(this, keyValues);
    }

    public default <V> V getValue(final String key) throws NoSuchElementException {
        final Property<V> property = this.getProperty(key);
        if (property.isPresent())
            return property.get();
        else throw Property.Exceptions.propertyDoesNotExist(key);
    }

    /*
    // TODO: Are we going down the right road with property as a first-class citizen?
    public default <V> V getValue(final String key, final V orElse) {
        final Property<V> property = this.getProperty(key);
        return property.orElse(orElse);
    }*/

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

        public static IllegalStateException elementHasAlreadyBeenRemovedOrDoesNotExist(final Class<? extends Element> type, final Object id) {
            return new IllegalStateException(String.format("The %s with id [%s] has already been removed or does not exist", type.getClass().getSimpleName(), id));
        }
    }

}
