package com.tinkerpop.gremlin.structure.util.micro;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.util.StreamFactory;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MicroProperty<V> implements Property, Serializable {

    private String key;
    private V value;
    private MicroElement element;
    private int hashCode;

    private MicroProperty() {

    }

    private MicroProperty(final Property property) {
        if (null == property)
            throw Graph.Exceptions.argumentCanNotBeNull("property");

        this.key = property.getKey();
        this.value = (V) property.get();
        this.hashCode = property.hashCode();
        this.element = property.getElement() instanceof Vertex ?
                MicroVertex.deflate((Vertex) property.getElement()) :
                MicroEdge.deflate((Edge) property.getElement());
    }

    public boolean isPresent() {
        return true;
    }

    public String getKey() {
        return this.key;
    }

    public V get() {
        return this.value;
    }

    public Element getElement() {
        return this.element;
    }

    public void remove() {
        throw new UnsupportedOperationException("Micro properties can not be removed (inflate): " + this.toString());
    }

    public String toString() {
        return StringFactory.propertyString(this);
    }

    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    public int hashCode() {
        return this.hashCode;
    }

    public Property<V> inflate(final Vertex hostVertex) {
        if (this.getElement() instanceof Vertex) {
            return Optional.<Property<V>>of(hostVertex.property(this.key)).orElseThrow(() -> new IllegalStateException("The micro property could not be be found at the provided vertex"));
        } else {
            final String label = this.getElement().label();
            final Object id = this.getElement().id();
            return StreamFactory.stream((Iterator<Edge>) hostVertex.outE(label))
                    .filter(e -> e.id().equals(id))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("The micro property could not be be found at the provided vertex's edges"))
                    .property(this.getKey());

        }
    }

    public Property<V> inflate(final Graph graph) {
        final Element element = (this.getElement() instanceof Vertex) ?
                graph.v(this.getElement().id()) :
                graph.e(this.getElement().id());
        return Optional.<Property<V>>of(element.property(this.key)).orElseThrow(() -> new IllegalStateException("The micro property could not be found at the provided graph"));
    }

    public static MicroProperty deflate(final Property property) {
        return new MicroProperty(property);
    }
}
