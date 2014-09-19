package com.tinkerpop.gremlin.structure.util.referenced;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;

import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ReferencedElement implements Element, Serializable {

    protected Object id;
    protected String label;

    protected ReferencedElement() {

    }

    public ReferencedElement(final Element element) {
        this.id = element.id();
        this.label = element.label();
    }

    @Override
    public Object id() {
        return this.id;
    }

    @Override
    public String label() {
        return this.label;
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        throw new IllegalStateException("ReferencedElements do not have properties:" + this);
    }

    @Override
    public void remove() {
        throw new IllegalStateException("ReferencedElements can not be removed:" + this);
    }

    @Override
    public Iterators iterators() {
        throw new IllegalStateException("ReferencedElements do not have iterators:" + this);
    }
}
