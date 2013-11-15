package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Thing;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerProperty<V, T extends Thing> implements Property<V, T> {

    private final String key;
    private final V value;
    private final T thing;
    private Map<String, Property> properties = new HashMap<>();

    protected TinkerProperty(String key, V value, final T thing) {
        this.key = key;
        this.value = value;
        this.thing = thing;
    }

    public Map<String, Property> getProperties() {
        return this.properties;
    }

    public T getThing() {
        return this.thing;
    }

    public String getKey() {
        return this.key;
    }

    public V getValue() {
        if (null == this.value) throw Property.Features.propertyHasNoValue();
        return this.value;
    }

    public boolean isPresent() {
        return this.value != null;
    }

    public <V2> Property<V2, Property> setProperty(String key, V2 value) throws IllegalStateException {
        final Property<V2, Property> property = new TinkerProperty<>(key, value, (Property) this);
        this.properties.put(key, property);
        return null == property ? Property.empty() : property;
    }

    public <V2> Property<V2, Property> getProperty(String key) throws IllegalStateException {
        final Property<V2, Property> property = this.properties.get(key);
        return null == property ? Property.empty() : property;
    }

    public <V2> Property<V2, Property> removeProperty(String key) throws IllegalStateException {
        final Property<V2, Property> property = this.properties.remove(key);
        return null == property ? Property.empty() : property;
    }
}
