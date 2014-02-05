package com.tinkerpop.gremlin.structure.util.micro;

import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StreamFactory;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.io.Serializable;
import java.util.Optional;

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
            return Optional.<Property<V>>of(hostVertex.getProperty(this.key)).orElseThrow(() -> new IllegalStateException("The micro property could not be be found at the provided vertex"));
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
        final Element element = (this.getElement() instanceof Vertex) ?
            graph.v(this.getElement().getId()).orElseThrow(() -> new IllegalStateException("The micro vertex could not be found at the provided graph")) :
            graph.e(this.getElement().getId()).orElseThrow(() -> new IllegalStateException("The micro edge could not be found at the provided graph"));
        return Optional.<Property<V>>of(element.getProperty(this.key)).orElseThrow(() -> new IllegalStateException("The micro property could not be found at the provided graph"));
    }

    public static MicroProperty deflate(final Property property) {
        return new MicroProperty(property);
    }
}
