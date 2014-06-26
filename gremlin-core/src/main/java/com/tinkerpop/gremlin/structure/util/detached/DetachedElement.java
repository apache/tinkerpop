package com.tinkerpop.gremlin.structure.util.detached;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import org.javatuples.Pair;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class DetachedElement implements Element {

    Object id;
    String label;
    Map<String, DetachedProperty> properties;
    Map<String, DetachedProperty> hiddenProperties;

    protected DetachedElement() {

    }

    protected DetachedElement(final Object id, final String label) {
        this(id, label, null, null);
    }

    protected DetachedElement(final Object id, final String label,
                              final Map<String, Object> properties,
                              final Map<String, Object> hiddenProperties) {
        if (null == id) throw Graph.Exceptions.argumentCanNotBeNull("id");
        if (null == label) throw Graph.Exceptions.argumentCanNotBeNull("label");

        this.id = id;
        this.label = label;
        if (null == properties)
            this.properties = null;
        else {
            this.properties = properties.entrySet().stream()
                    .map(entry -> {
                        if (entry.getValue() instanceof Property)
                            return Pair.with(entry.getKey(), DetachedProperty.detach((Property) entry.getValue()));
                        else
                            return Pair.with(entry.getKey(), new DetachedProperty(entry.getKey(), entry.getValue(), this));
                    }).collect(Collectors.toMap(p -> p.getValue0(), p -> p.getValue1()));
        }

        if (null == hiddenProperties)
            this.hiddenProperties = null;
        else {
            this.hiddenProperties = hiddenProperties.entrySet().stream()
                    .map(entry -> {
                        if (entry.getValue() instanceof Property)
                            return Pair.with(entry.getKey(), DetachedProperty.detach((Property) entry.getValue()));
                        else
                            return Pair.with(entry.getKey(), new DetachedProperty(entry.getKey(), entry.getValue(), this));
                    }).collect(Collectors.toMap(p -> p.getValue0(), p -> p.getValue1()));
        }
    }

    protected DetachedElement(final Element element) {
        this(element.id(), element.label(), null, null);
    }

    public Object id() {
        return this.id;
    }

    public String label() {
        return this.label;
    }

    public <V> Property<V> property(final String key, final V value) {
        throw new UnsupportedOperationException("Detached elements are readonly: " + this);
    }

    public <V> Property<V> property(final String key) {
        if (null == this.properties)
            return Property.empty();
        else {
            return this.properties.containsKey(key) ? this.properties.get(key) : Property.empty();
        }
    }

    public Map<String, Property> properties() {
        final Map<String, Property> temp = new HashMap<>();
        if (null == this.properties)
            return Collections.EMPTY_MAP;
        this.properties.forEach((key, property) -> {
            if (!Graph.Key.isHidden(key))
                temp.put(key, property);
        });
        return temp;
    }

    public Map<String, Property> hiddens() {
        final Map<String, Property> temp = new HashMap<>();
        if (null == this.properties)
            return Collections.EMPTY_MAP;
        this.properties.forEach((key, property) -> {
            if (Graph.Key.isHidden(key))
                temp.put(key, property);
        });
        return temp;
    }

    public void remove() {
        throw new UnsupportedOperationException("Detached elements are readonly: " + this);
    }

    public int hashCode() {
        return this.id.hashCode();
    }

    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }
}
