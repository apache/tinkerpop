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
    public static final String DEFAULT_LABEL = "default";

    public Object id();

    public String label();

    public void remove();

    // todo: make sure id/label get returned as properties

    public default Set<String> keys() {
        return this.properties().keySet();
    }

    public default Set<String> hiddenKeys() {
        return this.hiddens().keySet();
    }

    public default Map<String, Object> values() {
        final Map<String, Object> values = new HashMap<>();
        this.properties().forEach((k, p) -> values.put(k, p.value()));
        return values;
    }

	public default Map<String, Object> hiddenValues() {
		final Map<String, Object> values = new HashMap<>();
		this.hiddens().forEach((k, p) -> values.put(k, p.value()));
		return values;
	}

    public Map<String, Property> properties();

    public Map<String, Property> hiddens();

    public <V> Property<V> property(final String key);

    public <V> Property<V> property(final String key, final V value);

    public default void properties(final Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        ElementHelper.attachProperties(this, keyValues);
    }

    public default <V> V value(final String key) throws NoSuchElementException {
        final Property<V> property = this.property(key);
        if (property.isPresent())
            return property.value();
        else throw Property.Exceptions.propertyDoesNotExist(key);
    }

    /*
    // TODO: Are we going down the right road with property as a first-class citizen?
    public default <V> V value(final String key, final V orElse) {
        final Property<V> property = this.property(key);
        return property.orElse(orElse);
    }*/

    public static class Exceptions {

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
