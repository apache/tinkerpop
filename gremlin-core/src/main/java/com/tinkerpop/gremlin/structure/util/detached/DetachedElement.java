package com.tinkerpop.gremlin.structure.util.detached;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.PropertyFilterIterator;
import org.javatuples.Pair;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class DetachedElement implements Element, Serializable {

    Object id;
    String label;
    Map<String, List<Property>> properties = Collections.emptyMap();
    Map<String, List<Property>> hiddens = Collections.emptyMap();

    protected DetachedElement() {

    }

    protected DetachedElement(final Object id, final String label) {
        if (null == id) throw Graph.Exceptions.argumentCanNotBeNull("id");
        if (null == label) throw Graph.Exceptions.argumentCanNotBeNull("label");

        this.id = id;
        this.label = label;

        /*
        if (null != properties)
            this.properties = properties.entrySet().stream()
                    .map(entry -> {
                        if (entry.getValue() instanceof Property)
                            return Pair.with(entry.getKey(), (Property)  DetachedProperty.detach((Property) entry.getValue()));
                        else {
                            // todo: maybe get rid of some of this complexity by having a separate constructor for vertex/edge
                            if (this instanceof Vertex)
                                return Pair.with(entry.getKey(), (Property) new DetachedMetaProperty(entry.getKey(), entry.getValue(), (DetachedVertex) this));
                            else
                                return Pair.with(entry.getKey(), (Property) new DetachedProperty(entry.getKey(), entry.getValue(), this));
                        }
                    }).collect(Collectors.toMap(p -> p.getValue0(), p -> p.getValue1()));

        if (null != hiddens)
            this.hiddens = hiddens.entrySet().stream()
                    .map(entry -> {
                        if (entry.getValue() instanceof Property)
                            return Pair.with(entry.getKey(), DetachedProperty.detach((Property) entry.getValue()));
                        else
                            return Pair.with(entry.getKey(), new DetachedProperty(entry.getKey(), entry.getValue(), this));
                    }).collect(Collectors.toMap(p -> p.getValue0(), p -> p.getValue1()));
        */
    }

    protected DetachedElement(final Element element) {
        this(element.id(), element.label());
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
        // todo: fix for lists of property
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
        return this.iterators;
    }

    private final Element.Iterators iterators = new Iterators();

    protected class Iterators implements Element.Iterators, Serializable {
        @Override
        public <V> Iterator<? extends Property<V>> properties(final String... propertyKeys) {
            return new PropertyFilterIterator<>(properties.values().stream().flatMap(list -> list.stream()).collect(Collectors.toList()).iterator(), false, propertyKeys);
        }

        @Override
        public <V> Iterator<? extends Property<V>> hiddens(final String... propertyKeys) {
            return new PropertyFilterIterator<>(hiddens.values().stream().flatMap(list -> list.stream()).collect(Collectors.toList()).iterator(), false, propertyKeys);
        }
    }
}
