package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Property;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerProperty<T, E extends Element> implements Property<T, E> {

    private final String key;
    private final T value;
    private final E element;
    private Map<String, Object> metas = new HashMap<>();

    protected TinkerProperty(String key, T value, final E element) {
        this.key = key;
        this.value = value;
        this.element = element;
    }

    public E getElement() {
        return this.element;
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

    public <T> void setMetaValue(final String key, final T value) {
        this.metas.put(key, value);
    }

    public <T> T getMetaValue(final String key) {
        return (T) this.metas.get(key);
    }

    public <T> T removeMetaValue(final String key) {
        return (T) this.metas.remove(key);
    }
}
