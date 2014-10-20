package com.tinkerpop.gremlin.structure.util.detached;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.PropertyFilterIterator;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class DetachedElement<E> implements Element, Element.Iterators, Serializable, Attachable<E> {

    Object id;
    String label;
    Map<String, List<? extends Property>> properties = new HashMap<>();

    protected DetachedElement() {

    }

    protected DetachedElement(final Object id, final String label) {
        if (null == id) throw Graph.Exceptions.argumentCanNotBeNull("id");
        if (null == label) throw Graph.Exceptions.argumentCanNotBeNull("label");

        this.id = id;
        this.label = label;
    }

    protected DetachedElement(final Element element) {
        this(element.id(), element.label());
    }

    @Override
    public Graph graph() {
        throw new UnsupportedOperationException("The element is no longer attached to a graph");
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
        return this.properties.containsKey(key) ? this.properties.get(key).get(0) : Property.empty();
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

    @Override
    public Element.Iterators iterators() {
        return this;
    }

    @Override
    public <V> Iterator<? extends Property<V>> propertyIterator(final String... propertyKeys) {
        return new PropertyFilterIterator<>(properties.values().stream().flatMap(list -> list.stream()).collect(Collectors.toList()).iterator(), false, propertyKeys);
    }

    @Override
    public <V> Iterator<? extends Property<V>> hiddenPropertyIterator(final String... propertyKeys) {
        return new PropertyFilterIterator<>(properties.values().stream().flatMap(list -> list.stream()).collect(Collectors.toList()).iterator(), true, propertyKeys);
    }
}
