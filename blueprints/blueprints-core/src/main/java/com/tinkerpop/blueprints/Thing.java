package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.util.ExceptionFactory;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Thing {

    public default Set<String> getPropertyKeys() {
        return this.getProperties().keySet();
    }

    public Map<String, Property> getProperties();

    public <V> Property<V, ? extends Thing> getProperty(String key);

    public <V> Property<V, ? extends Thing> setProperty(String key, V value);

    public <V> Property<V, ? extends Thing> removeProperty(String key);

    public default <V> V getValue(String key) throws NoSuchElementException {
        final Property<V, ? extends Thing> property = this.getProperty(key);
        if (property.isPresent())
            return property.getValue();
        else throw ExceptionFactory.propertyHasNoValue();
    }
}
