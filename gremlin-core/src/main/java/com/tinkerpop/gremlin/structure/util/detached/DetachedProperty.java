package com.tinkerpop.gremlin.structure.util.detached;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.io.Serializable;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DetachedProperty<V> implements Property, Serializable, Attachable<Property<V>> {

    String key;
    V value;
    transient DetachedElement element;

    /**
     * Construct a {@code DetachedProperty} during manual deserialization.
     */
    public DetachedProperty(final String key, final V value, final DetachedElement element) {
        if (null == key) throw Graph.Exceptions.argumentCanNotBeNull("key");
        if (null == value) throw Graph.Exceptions.argumentCanNotBeNull("value");
        if (null == element) throw Graph.Exceptions.argumentCanNotBeNull("element");

        this.key = key;
        this.value = value;
        this.element = element;
    }

    /**
     * Construct a {@code DetachedProperty} internally when a {@link DetachedEdge} is being constructed.
     */
    DetachedProperty(final Property property, final DetachedEdge element) {
        this(property, (DetachedElement) element);
    }

    /**
     * Construct a {@code DetachedProperty} internally when a {@link DetachedVertexProperty} is being constructed.
     */
    DetachedProperty(final Property property, final DetachedVertexProperty element) {
        this(property, (DetachedElement) element);
    }

    private DetachedProperty(final Property property, final DetachedElement element) {
        if (null == property) throw Graph.Exceptions.argumentCanNotBeNull("property");
        if (element instanceof Vertex) throw new IllegalArgumentException("Element cannot be of type " + Vertex.class.getSimpleName());

        this.key = property.isHidden() ? Graph.Key.hide(property.key()) : property.key();
        this.value = (V) property.value();
        this.element = element;
    }

    private DetachedProperty() {
        // no implementation
    }

    private DetachedProperty(final Property property) {
        if (null == property) throw Graph.Exceptions.argumentCanNotBeNull("property");

        this.key = property.isHidden() ? Graph.Key.hide(property.key()) : property.key();
        this.value = (V) property.value();
        final Element element = property.element();

        if (element instanceof Vertex)
            this.element = DetachedVertex.detach((Vertex) element);
        else if (element instanceof VertexProperty)
            this.element = DetachedVertexProperty.detach((VertexProperty) element);
        else
            this.element = DetachedEdge.detach((Edge) element);
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
    public Element element() {
        return this.element;
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
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return this.element.id.hashCode() + this.key.hashCode() + this.value.hashCode();
    }

    @Override
    public Property<V> attach(final Vertex hostVertex) {
        final Element hostElement = (Element) ((DetachedElement) this.element()).attach(hostVertex);
        final Property<V> property = hostElement.property(this.isHidden() ? Graph.Key.hide(this.key) : this.key);
        if (property.isPresent()) // && property.value().equals(this.value))
            return property;
        else
            throw new IllegalStateException("The detached property could not be be found at the provided vertex: " + this);
    }

    @Override
    public Property<V> attach(final Graph hostGraph) {
        final Element hostElement = (this.element() instanceof Vertex) ?
                hostGraph.v(this.element().id()) :
                hostGraph.e(this.element().id());
        final Property<V> property = hostElement.property(this.isHidden() ? Graph.Key.hide(this.key) : this.key);
        if (property.isPresent()) // && property.value().equals(this.value))
            return property;
        else
            throw new IllegalStateException("The detached property could not be be found at the provided vertex: " + this);
    }

    public static DetachedProperty detach(final Property property) {
        if (null == property) throw Graph.Exceptions.argumentCanNotBeNull("property");
        return (property instanceof DetachedProperty) ? (DetachedProperty) property : new DetachedProperty(property);
    }
}
