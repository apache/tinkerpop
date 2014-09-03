package com.tinkerpop.gremlin.structure.util;

import com.tinkerpop.gremlin.structure.MetaProperty;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EmptyMetaProperty<V> implements MetaProperty<V> {
    @Override
    public Vertex getElement() {
        return null;
    }

    @Override
    public Object id() {
        return null;
    }

    @Override
    public <U> Iterator<Property<U>> properties(String... propertyKeys) {
        return null;
    }

    @Override
    public <U> Iterator<Property<U>> hiddens(String... propertyKeys) {
        return null;
    }

    @Override
    public <V> Property<V> property(String key) {
        return null;
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        return null;
    }

    @Override
    public String key() {
        return null;
    }

    @Override
    public V value() throws NoSuchElementException {
        return null;
    }

    @Override
    public boolean isPresent() {
        return false;
    }

    @Override
    public boolean isHidden() {
        return false;
    }

    @Override
    public void remove() {

    }
}
