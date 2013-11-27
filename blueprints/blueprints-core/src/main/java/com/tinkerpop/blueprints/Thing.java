package com.tinkerpop.blueprints;

import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Thing {

    public Set<String> getPropertyKeys();

    public <V> Property<V, ? extends Thing> getProperty(String key);

    public <V> Property<V, ? extends Thing> setProperty(String key, V value);

    public void removeProperty(String key);

    public default <V> V getValue(String key) throws NoSuchElementException {
        final Property<V, ? extends Thing> property = this.getProperty(key);
        if (property.isPresent())
            return property.getValue();
        else throw Property.Features.propertyHasNoValue();
    }

    public default boolean is(final Class thingClass) {
        return thingClass.isAssignableFrom(this.getClass());
    }
}
