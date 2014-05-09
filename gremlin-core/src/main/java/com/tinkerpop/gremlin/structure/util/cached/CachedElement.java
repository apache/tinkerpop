package com.tinkerpop.gremlin.structure.util.cached;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import org.javatuples.Pair;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class CachedElement implements Element {

    protected final Object id;
    protected final String label;
    protected final Optional<Map<String, Property>> properties;

    protected CachedElement(final Object id, final String label) {
        this(id, label, null);
    }

    protected CachedElement(final Object id, final String label, final Map<String, Object> properties) {
        if (null == id) throw Graph.Exceptions.argumentCanNotBeNull("id");
        if (null == label) throw Graph.Exceptions.argumentCanNotBeNull("label");

        this.id = id;
        this.label = label;
        if (null == properties)
            this.properties = Optional.empty();
        else {
            this.properties = Optional.of(properties.entrySet().stream()
                    .map(entry -> {
                        if (entry.getValue() instanceof Property)
                            return Pair.with(entry.getKey(), new CachedProperty((Property) entry.getValue()));
                        else
                            return Pair.with(entry.getKey(), new CachedProperty(entry.getKey(), entry.getValue(), this));
                    }).collect(Collectors.toMap(p -> p.getValue0(), p -> p.getValue1())));
        }

    }

    protected CachedElement(final Element element) {
        if (null == element) throw Graph.Exceptions.argumentCanNotBeNull("element");

        this.id = element.getId();
        this.label = element.getLabel();
        this.properties = Optional.ofNullable(element.getProperties());
    }

    public Object getId() {
        return this.id;
    }

    public String getLabel() {
        return this.label;
    }

    public void setProperty(final String key, final Object value) {
        throw new UnsupportedOperationException("Cached elements are read-only: " + this.toString());
    }

    public <V> Property<V> getProperty(final String key) {
        return this.properties.orElseThrow(() -> new IllegalStateException("Properties not assigned to this element which means it is likely a CachedVertex on a CachedEdge"))
                .getOrDefault(key, Property.empty());
    }

    public Map<String, Property> getProperties() {
        final Map<String, Property> temp = new HashMap<>();
        this.properties.orElseThrow(() -> new IllegalStateException("Properties not assigned to this element which means it is likely a CachedVertex on a CachedEdge"));
        this.properties.get().forEach((key, property) -> {
            if (!key.startsWith(Graph.HIDDEN_PREFIX))
                temp.put(key, property);
        });
        return temp;
    }

    public Map<String, Property> getHiddens() {
        final Map<String, Property> temp = new HashMap<>();
        this.properties.orElseThrow(() -> new IllegalStateException("Properties not assigned to this element which means it is likely a CachedVertex on a CachedEdge"));
        this.properties.get().forEach((key, property) -> {
            if (key.startsWith(Graph.HIDDEN_PREFIX))
                temp.put(key, property);
        });
        return temp;
    }

    public void remove() {
        throw new UnsupportedOperationException("Cached elements are read-only: " + this.toString());
    }

    public int hashCode() {
        return this.id.hashCode();
    }

    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }
}
