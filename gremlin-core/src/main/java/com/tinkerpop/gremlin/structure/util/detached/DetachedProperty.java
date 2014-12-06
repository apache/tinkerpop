package com.tinkerpop.gremlin.structure.util.detached;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.io.Serializable;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DetachedProperty<V> implements Property, Serializable, Attachable<Property<V>> {

    private String key;
    private V value;
    private transient DetachedElement element;

    private DetachedProperty() {
    }

    protected DetachedProperty(final Property<V> property) {
        this.key = property.key();
        this.value = property.value();
        this.element = DetachedFactory.detach(property.element(), true);
    }

    public DetachedProperty(final String key, final V value, final Element element) {
        this.key = key;
        this.value = value;
        this.element = DetachedFactory.detach(element, true);
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
        return this.key;
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
        final Property<V> property = hostElement.property(this.key);
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
        final Property<V> property = hostElement.property(this.key);
        if (property.isPresent()) // && property.value().equals(this.value))
            return property;
        else
            throw new IllegalStateException("The detached property could not be be found at the provided vertex: " + this);
    }
}
