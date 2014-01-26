package com.tinkerpop.blueprints.util.micro;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ElementHelper;
import com.tinkerpop.blueprints.util.StreamFactory;
import com.tinkerpop.blueprints.util.StringFactory;

import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MicroProperty<V> implements Property, Serializable {

    private final String key;
    private final V value;
    private final MicroElement element;
    private final int hashCode;

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
            final Property property = hostVertex.getProperty(this.key);
            if (!property.isPresent())
                throw new IllegalStateException("The micro property could not be be found at the provided vertex");
            else
                return property;
        } else {
            final String label = this.getElement().getLabel();
            final Object id = this.getElement().getId();
            return StreamFactory.stream(hostVertex.query().direction(Direction.OUT).labels(label).edges())
                    .filter(e -> e.getId().equals(id))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("The micro property could not be be found at the provided vertex's edges"))
                    .getProperty(this.getKey());

        }
    }

    public Property<V> inflate(final Graph graph) {
        final Property<V> property = (this.getElement() instanceof Vertex) ?
                graph.query().ids(this.getElement().getId()).vertices().iterator().next().getProperty(this.key) :
                graph.query().ids(this.getElement().getId()).edges().iterator().next().getProperty(this.key);
        if (!this.isPresent())
            throw new IllegalStateException("The micro property could not be found at the provided graph");
        else
            return property;
    }

    public static MicroProperty deflate(final Property property) {
        return new MicroProperty(property);
    }
}
