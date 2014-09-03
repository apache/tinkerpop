package com.tinkerpop.gremlin.structure.util.detached;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import org.javatuples.Pair;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class DetachedElement implements Element, Serializable {

    Object id;
    String label;
    Map<String, ? extends Property> properties = Collections.emptyMap();
    Map<String, ? extends Property> hiddenProperties = Collections.emptyMap();

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

        if (null != properties)
            this.properties = properties.entrySet().stream()
                    .map(entry -> {
                        if (entry.getValue() instanceof Property)
                            return Pair.with(entry.getKey(), DetachedProperty.detach((Property) entry.getValue()));
                        else
                            return Pair.with(entry.getKey(), new DetachedProperty(entry.getKey(), entry.getValue(), this));
                    }).collect(Collectors.toMap(p -> p.getValue0(), p -> p.getValue1()));

        if (null != hiddenProperties)
            this.hiddenProperties = hiddenProperties.entrySet().stream()
                    .map(entry -> {
                        if (entry.getValue() instanceof Property)
                            return Pair.with(entry.getKey(), DetachedProperty.detach((Property) entry.getValue()));
                        else
                            return Pair.with(entry.getKey(), new DetachedProperty(entry.getKey(), entry.getValue(), this));
                    }).collect(Collectors.toMap(p -> p.getValue0(), p -> p.getValue1()));

    }

    protected DetachedElement(final Element element) {
        this(element.id(), element.label(), null, null);
    }

    @Override
    public Object id() {
        return this.id;
    }

    @Override
    public String label() {
        return this.label;
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        throw new UnsupportedOperationException("Detached elements are readonly: " + this);
    }

    @Override
    public <V> Property<V> property(final String key) {
        return this.properties.containsKey(key) ? this.properties.get(key) : Property.empty();
    }

    @Override
    public <V> Iterator<? extends Property<V>> properties(final String... propertyKeys) {
        return this.properties.entrySet().stream()
                .filter(entry -> propertyKeys.length == 0 || Arrays.binarySearch(propertyKeys, entry.getKey()) >= 0)
                .map(entry -> (Property<V>) entry.getValue()).iterator();
    }

    @Override
    public <V> Iterator<? extends Property<V>> hiddens(final String... propertyKeys) {
        return this.hiddenProperties.entrySet().stream()
                .filter(entry -> propertyKeys.length == 0 || Arrays.binarySearch(propertyKeys, entry.getKey()) >= 0)
                .map(entry -> (Property<V>) entry.getValue()).iterator();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Detached elements are readonly: " + this);
    }

    @Override
    public int hashCode() {
        return this.id.hashCode();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }
}
