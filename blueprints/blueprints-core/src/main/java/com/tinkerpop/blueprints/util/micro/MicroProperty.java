package com.tinkerpop.blueprints.util.micro;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ElementHelper;
import com.tinkerpop.blueprints.util.StringFactory;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MicroProperty implements Property {

    private final String key;
    private final Object value;
    private final MicroElement element;
    private final int hashCode;

    public MicroProperty(final Property property) {
        if (null == property)
            throw Graph.Exceptions.argumentCanNotBeNull("property");

        this.key = property.getKey();
        this.value = property.get();
        this.hashCode = property.hashCode();
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

    public Object get() {
        return this.value;
    }

    public Element getElement() {
        return this.element;
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }

    public String toString() {
        return StringFactory.propertyString(this) + ".";
    }

    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    public int hashCode() {
        return this.hashCode;
    }
}
