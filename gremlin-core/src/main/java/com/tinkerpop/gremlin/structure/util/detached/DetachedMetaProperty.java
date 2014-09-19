package com.tinkerpop.gremlin.structure.util.detached;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.MetaProperty;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.util.StreamFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DetachedMetaProperty<V> extends DetachedElement<Property<V>> implements MetaProperty<V> {

    String key;
    V value;
    transient DetachedVertex vertex;

    private final transient MetaProperty.Iterators iterators = new Iterators();

    private DetachedMetaProperty() {

    }

    public DetachedMetaProperty(final Object id, final String label, final String key, final V value,
                                final Map<String, Object> properties, final Map<String, Object> hiddenProperties,
                                final DetachedVertex vertex) {
        super(id, label);
        if (null == key) throw Graph.Exceptions.argumentCanNotBeNull("key");
        if (null == value) throw Graph.Exceptions.argumentCanNotBeNull("value");
        if (null == vertex) throw Graph.Exceptions.argumentCanNotBeNull("vertex");

        this.key = key;
        this.value = value;
        this.vertex = vertex;

        if (properties != null) properties.entrySet().iterator().forEachRemaining(kv -> putToList(kv.getKey(), new DetachedProperty(kv.getKey(), kv.getValue(), this)));
        if (hiddenProperties != null) hiddenProperties.entrySet().iterator().forEachRemaining(kv -> putToList(Graph.Key.hide(kv.getKey()), new DetachedProperty(kv.getKey(), kv.getValue(), this)));
    }

    // todo: straighten out all these constructors and their scopes - what do we really need here?

    private DetachedMetaProperty(final MetaProperty property) {
        super(property);
        if (null == property) throw Graph.Exceptions.argumentCanNotBeNull("property");

        this.key = property.isHidden() ? Graph.Key.hide(property.key()) : property.key();
        this.value = (V) property.value();
        this.vertex = property.getElement() instanceof DetachedVertex ? (DetachedVertex) property.getElement() : DetachedVertex.detach(property.getElement());

        property.iterators().properties().forEachRemaining(p -> putToList(p.key(), p instanceof DetachedProperty ? p : new DetachedProperty(p, this)));
        property.iterators().hiddens().forEachRemaining(p -> putToList(Graph.Key.hide(p.key()), p instanceof DetachedProperty ? p : new DetachedProperty(p, this)));
    }

    DetachedMetaProperty(final MetaProperty property, final DetachedVertex detachedVertex) {
        super(property);
        if (null == property) throw Graph.Exceptions.argumentCanNotBeNull("property");

        this.key = property.isHidden() ? Graph.Key.hide(property.key()) : property.key();
        this.value = (V) property.value();
        this.vertex = detachedVertex;

        property.iterators().properties().forEachRemaining(p -> putToList(p.key(), p instanceof DetachedProperty ? p : new DetachedProperty(p, this)));
        property.iterators().hiddens().forEachRemaining(p -> putToList(Graph.Key.hide(p.key()), p instanceof DetachedProperty ? p : new DetachedProperty(p, this)));
    }

    @Override
    public boolean isPresent() {
        return true;
    }

    @Override
    public boolean isHidden() {
        return Graph.Key.isHidden(this.key);
    }

    @Override
    public String key() {
        return Graph.Key.unHide(this.key);
    }

    @Override
    public V value() {
        return this.value;
    }

    @Override
    public Vertex getElement() {
        return this.vertex;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Detached properties are readonly: " + this.toString());
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual((MetaProperty) this, object);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public Property<V> attach(final Vertex hostVertex) {
        if (this.getElement() instanceof Vertex) {
            return Optional.<Property<V>>of(hostVertex.property(this.key)).orElseThrow(() -> new IllegalStateException("The detached property could not be be found at the provided vertex: " + this));
        } else {
            final String label = this.getElement().label();
            final Object id = this.getElement().id();
            return StreamFactory.stream((Iterator<Edge>) hostVertex.outE(label))
                    .filter(e -> e.id().equals(id))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("The detached property could not be be found at the provided vertex's edges: " + this))
                    .property(this.key());

        }
    }

    @Override
    public Property<V> attach(final Graph graph) {
        final Element element = (this.getElement() instanceof Vertex) ?
                graph.v(this.getElement().id()) :
                graph.e(this.getElement().id());
        return Optional.<Property<V>>of(element.property(this.key)).orElseThrow(() -> new IllegalStateException("The detached property could not be found in the provided graph: " + this));
    }

    public static DetachedMetaProperty detach(final MetaProperty property) {
        if (null == property) throw Graph.Exceptions.argumentCanNotBeNull("property");
        if (property instanceof DetachedMetaProperty) throw new IllegalArgumentException("MetaProperty is already detached");
        return new DetachedMetaProperty(property);
    }

    @Override
    public MetaProperty.Iterators iterators() {
        return this.iterators;
    }

    protected class Iterators extends DetachedElement<V>.Iterators implements MetaProperty.Iterators {

        @Override
        public <U> Iterator<Property<U>> properties(final String... propertyKeys) {
            return (Iterator) super.properties(propertyKeys);
        }

        @Override
        public <U> Iterator<Property<U>> hiddens(final String... propertyKeys) {
            return (Iterator) super.hiddens(propertyKeys);
        }
    }

    private void putToList(final String key, final Property p) {
        if (!this.properties.containsKey(key))
            this.properties.put(key, new ArrayList<>());

        ((List) this.properties.get(key)).add(p);
    }
}
