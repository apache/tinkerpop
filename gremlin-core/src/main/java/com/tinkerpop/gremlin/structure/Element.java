package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.HashMap;
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

    /**
     * Gets the unique identifier for the graph {@code Element}.
     */
    public Object id();

    /**
     * Gets the label for the graph {@code Element} which helps categorize it.
     */
    public String label();

    /**
     * Removes the {@code Element} from the graph.
     */
    public void remove();

    /**
     * Get the keys from non-hidden properties.
     */
    public default Set<String> keys() {
        return this.properties().keySet();
    }

    /**
     * Get the keys of hidden properties.
     */
    public default Set<String> hiddenKeys() {
        return this.hiddens().keySet();
    }

    /**
     * Get the values of non-hidden properties as a {@link Map} of keys and values.
     */
    public default Map<String, Object> values() {
        final Map<String, Object> values = new HashMap<>();
        this.properties().forEach((k, p) -> values.put(k, p.value()));
        return values;
    }

    /**
     * Get the values of hidden properties as a {@link Map} of keys and values.
     */
    public default Map<String, Object> hiddenValues() {
        final Map<String, Object> values = new HashMap<>();
        this.hiddens().forEach((k, p) -> values.put(k, p.value()));
        return values;
    }

    /**
     * Get a {@link Map} of non-hidden properties.
     */
    public Map<String, Property> properties();

    /**
     * Get a {@link Map} of hidden properties.
     */
    public Map<String, Property> hiddens();

    /**
     * Get a {@link Property} for the {@code Element} given its key.  Hidden properties can be retrieved by specifying
     * the key as {@link com.tinkerpop.gremlin.structure.Graph.Key#hidden}.
     */
    public <V> Property<V> property(final String key);

    /**
     * Add or set a property value for the {@code Element} given its key.  Hidden properties can be set by specifying
     * the key as {@link com.tinkerpop.gremlin.structure.Graph.Key#hidden}.
     */
    public <V> Property<V> property(final String key, final V value);

    /**
     * Set a series of properties on the {@code Element} by specifying a series of key/value pairs.  These key/values
     * must be provided in an even number where the odd numbered arguments are {@link String} key values and the
     * even numbered arguments are the related property values.  Hidden properties can be set by specifying
     * the key as {@link com.tinkerpop.gremlin.structure.Graph.Key#hidden}.
     */
    public default void properties(final Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        ElementHelper.attachProperties(this, keyValues);
    }

    /**
     * Get the value of a {@link Property} given it's key.
     *
     * @throws NoSuchElementException if the property does not exist on the {@code Element}.
     */
    public default <V> V value(final String key) throws NoSuchElementException {
        final Property<V> property = this.property(key);
        return property.orElseThrow(() -> Property.Exceptions.propertyDoesNotExist(key));
    }

    public default <V> V value(final String key, final V orElse) {
        final Property<V> property = this.property(key);
        return property.orElse(orElse);
    }

    public static class Exceptions {

        public static IllegalArgumentException providedKeyValuesMustBeAMultipleOfTwo() {
            return new IllegalArgumentException("The provided key/value array must be a multiple of two");
        }

        public static IllegalArgumentException providedKeyValuesMustHaveALegalKeyOnEvenIndices() {
            return new IllegalArgumentException("The provided key/value array must have a String key on even array indices");
        }

        public static IllegalStateException propertyAdditionNotSupported() {
            return new IllegalStateException("Property additions are not supported");
        }

        public static IllegalStateException propertyRemovalNotSupported() {
            return new IllegalStateException("Property removal are not supported");
        }
    }

}
