package com.tinkerpop.blueprints.tinkergraph;


import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.util.ElementHelper;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
abstract class TinkerElement implements Element, Serializable {

    protected Map<String, Property> properties = new HashMap<>();
    protected Map<String, Object> annotations = new HashMap<>();
    protected final String id;
    protected final String label;
    protected final TinkerGraph graph;

    protected TinkerElement(final String id, final String label, final TinkerGraph graph) {
        this.graph = graph;
        this.id = id;
        this.label = label;
    }

    public int hashCode() {
        return this.id.hashCode();
    }

    public String getId() {
        return this.id;
    }

    public String getLabel() {
        return this.label;
    }

    public Map<String, Property> getProperties() {
        return new HashMap<>(this.properties);
    }

    public <V> Property<V> getProperty(final String key) {
        return this.properties.getOrDefault(key, Property.empty());
    }

    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    public <V> Optional<V> getAnnotation(final String key) {
        return Optional.ofNullable((V)this.annotations.get(key));
    }

    public <V> void setAnnotation(final String key, final V value) {
        this.annotations.put(key, value);
    }

    public abstract void remove();
}
