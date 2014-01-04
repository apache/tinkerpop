package com.tinkerpop.gremlin.computer.gremlin.util;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Property;

import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class MicroElement implements Element {

    final Object id;
    final String label;

    public MicroElement(final Element element) {
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

}
