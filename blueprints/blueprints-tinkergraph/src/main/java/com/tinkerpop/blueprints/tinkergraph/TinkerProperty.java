package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Property;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class TinkerProperty<V> implements Property<V> {

    private final Map<String, Object> annotations = new HashMap<>();

    private final Element element;
    private final String key;
    private final V value;

    public TinkerProperty(final Element element, final String key, final V value) {
        this.element = element;
        this.key = key;
        this.value = value;

    }

    public <E extends Element> E getElement() {
        return (E) this.element;
    }

    public String getKey() {
        return this.key;
    }

    public V getValue() {
        return this.value;
    }

    public boolean isPresent() {
        return null != this.value;
    }

    public <V> void setAnnotation(final String key, final V value) {
        this.annotations.put(key, value);
    }

    public <V> Optional<V> getAnnotation(final String key) {
        return Optional.ofNullable((V)this.annotations.get(key));
    }

    public abstract void remove();
}
