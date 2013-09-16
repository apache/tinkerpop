package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Property;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerProperty<T> implements Property<T> {

    private final String key;
    private final T value;
    private Map<String, Object> metas = new HashMap<>();

    protected TinkerProperty(String key, T value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return this.key;
    }

    public T getValue() {
        return this.value;
    }

    public <T> void setMetaValue(final String key, final T value) {
        this.metas.put(key, value);
    }

    public <T> T getMetaValue(final String key) {
        return (T) this.metas.get(key);
    }

    public <T> T removeMetaValue(final String key) {
        return (T) this.metas.remove(key);
    }

    public static Property[] make(final Object... keyValues) {
        if (keyValues.length % 2 != 0)
            throw new IllegalArgumentException("The provided arguments must have a size that is a factor of 2");
        final Property[] properties = new Property[keyValues.length / 2];
        for (int i = 0; i < keyValues.length; i = i + 2) {
            properties[i / 2] = new TinkerProperty((String) keyValues[i], keyValues[i + 1]);
        }
        return properties;
    }
}
