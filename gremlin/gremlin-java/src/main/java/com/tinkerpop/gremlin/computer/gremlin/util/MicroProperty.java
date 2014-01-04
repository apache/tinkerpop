package com.tinkerpop.gremlin.computer.gremlin.util;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.StringFactory;

import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MicroProperty implements Property {

    private final String key;
    private final Object value;
    private final MicroElement element;

    public MicroProperty(final Property property) {
        this.key = property.getKey();
        this.value = property.getValue();
        this.element = property.getElement() instanceof Vertex ?
                new MicroVertex((Vertex) property.getElement()) :
                new MicroEdge((Edge) property.getElement());
    }

    public boolean isPresent() {
        return true;
    }

    public String getKey() {
        return this.key;
    }

    public Object getValue() {
        return this.value;
    }

    public Element getElement() {
        return this.element;
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }

    public void setAnnotation(final String key, final Object value) {
        throw new UnsupportedOperationException();
    }

    public <T> Optional<T> getAnnotation(final String key) {
        throw new UnsupportedOperationException();
    }

    public String toString() {
        return StringFactory.propertyString(this) + ".";
    }
}
