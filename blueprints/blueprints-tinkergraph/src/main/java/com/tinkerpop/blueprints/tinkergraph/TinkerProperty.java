package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Thing;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerProperty<T, E extends Thing> implements Property<T, E> {

    private final String key;
    private final T value;
    private final E thing;
    private Map<String, Property> properties = new HashMap<>();

    protected TinkerProperty(String key, T value, final E thing) {
        this.key = key;
        this.value = value;
        this.thing = thing;
    }

    public Map<String, Property> getProperties() {
        return this.properties;
    }

    public E getThing() {
        return this.thing;
    }

    public String getKey() {
        return this.key;
    }

    public T getValue() {
        return this.value;
    }

    public boolean isPresent() {
        return this.value != null;
    }

    public <R> Property<R, Property> setProperty(String key, R value) throws IllegalStateException {
        final Property<R, Property> property = this.properties.put(key, new TinkerProperty<R, TinkerProperty>(key, value, this));
        return null == property ? Property.empty() : property;
    }

    public <R> Property<R, Property> getProperty(String key) throws IllegalStateException {
        final Property<R, Property> property = this.properties.get(key);
        return null == property ? Property.empty() : property;
    }

    public <R> Property<R, Property> removeProperty(String key) throws IllegalStateException {
        final Property<R, Property> property = this.properties.remove(key);
        return null == property ? Property.empty() : property;
    }
}
