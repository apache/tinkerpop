package com.tinkerpop.blueprints.tinkergraph;


import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.util.ElementHelper;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
abstract class TinkerElement implements Element, Serializable {

    protected Map<String, Property> properties = new HashMap<>();
    protected final String id;
    protected final TinkerGraph graph;

    protected TinkerElement(final String id, final TinkerGraph graph) {
        this.graph = graph;
        this.id = id;
    }

    public Set<String> getPropertyKeys() {
        return new HashSet<>(this.properties.keySet());
    }

    public <T> Property<T> getProperty(final String key) {
        final Property<T> t = this.properties.get(key);
        return (null == t) ? Property.<T>empty() : t;
    }

    public <T> Property<T> setProperty(final String key, final T value) {
        ElementHelper.validateProperty(this, key, value);
        Property<T> oldValue = this.properties.put(key, new TinkerProperty<>(key, value));
        if (this instanceof TinkerVertex)
            this.graph.vertexIndex.autoUpdate(key, value, oldValue, (TinkerVertex) this);
        else
            this.graph.edgeIndex.autoUpdate(key, value, oldValue, (TinkerEdge) this);
        return oldValue;
    }

    public <T> Property<T> removeProperty(final String key) {
        Property<T> oldValue = this.properties.remove(key);
        if (this instanceof TinkerVertex)
            this.graph.vertexIndex.autoRemove(key, oldValue, (TinkerVertex) this);
        else
            this.graph.edgeIndex.autoRemove(key, oldValue, (TinkerEdge) this);
        return oldValue;
    }


    public int hashCode() {
        return this.id.hashCode();
    }

    public String getId() {
        return this.id;
    }

    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    public abstract void remove();
}
