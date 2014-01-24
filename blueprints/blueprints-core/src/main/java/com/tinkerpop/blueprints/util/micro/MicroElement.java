package com.tinkerpop.blueprints.util.micro;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.util.ElementHelper;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class MicroElement implements Element, Serializable {

    final Object id;
    final String label;

    public MicroElement(final Element element) {
        if (null == element)
            throw Graph.Exceptions.argumentCanNotBeNull("element");

        this.id = element.getId();
        this.label = element.getLabel();
    }

    public Object getId() {
        return this.id;
    }

    public String getLabel() {
        return this.label;
    }

    public void setProperty(final String key, final Object value) {
        throw new UnsupportedOperationException();
    }

    public <V> Property<V> getProperty(final String key) {
        throw new UnsupportedOperationException();
    }

    public Map<String, Property> getProperties() {
        throw new UnsupportedOperationException();
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }

    public int hashCode() {
        return this.id.hashCode();
    }

    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }
}
