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

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DetachedMetaProperty<V> extends DetachedElement<Property<V>> implements MetaProperty<V> {

    String key;
    V value;
    DetachedVertex vertex;
    int hashCode;

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
        this.hashCode = super.hashCode();

        if (properties != null) properties.entrySet().iterator().forEachRemaining(kv -> this.properties.put(kv.getKey(), Arrays.asList(new DetachedProperty(kv.getKey(), kv.getValue(), this))));
        if (hiddenProperties != null) hiddenProperties.entrySet().iterator().forEachRemaining(kv -> this.properties.put(Graph.Key.hide(kv.getKey()), Arrays.asList(new DetachedProperty(kv.getKey(), kv.getValue(), this))));
    }

    private DetachedMetaProperty(final MetaProperty property) {
        super(property);
        if (null == property) throw Graph.Exceptions.argumentCanNotBeNull("property");

        this.key = property.key();
        this.value = (V) property.value();
        this.hashCode = property.hashCode();
        this.vertex = DetachedVertex.detach(property.getElement());
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
        return this.hashCode;
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

    private final MetaProperty.Iterators iterators = new Iterators();

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
}
