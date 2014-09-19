package com.tinkerpop.gremlin.structure.util.referenced;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;

import java.io.Serializable;
import java.util.NoSuchElementException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ReferencedProperty<V> implements Property<V>, Serializable {

    protected String key;
    protected V value;
    protected boolean hidden;
    protected ReferencedElement element;

    public ReferencedProperty() {

    }

    public ReferencedProperty(final Property<V> property) {
        this.key = property.key();
        this.value = property.value();
        this.hidden = property.isHidden();
        this.element = property.getElement();
    }

    @Override
    public String key() {
        return this.key;
    }

    @Override
    public V value() throws NoSuchElementException {
        return this.value;
    }

    @Override
    public boolean isPresent() {
        return null != this.value;
    }

    @Override
    public boolean isHidden() {
        return this.hidden;
    }

    @Override
    public void remove() {
        throw new IllegalStateException("ReferencedProperties can not be removed:" + this);
    }

    @Override
    public <E extends Element> E getElement() {
        return (E) this.element;
    }
}
