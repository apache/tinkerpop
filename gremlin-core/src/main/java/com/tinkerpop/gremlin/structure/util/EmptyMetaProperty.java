package com.tinkerpop.gremlin.structure.util;

import com.tinkerpop.gremlin.structure.MetaProperty;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EmptyMetaProperty<V> implements MetaProperty<V> {

    private static final EmptyMetaProperty INSTANCE = new EmptyMetaProperty();

    public static <U> MetaProperty<U> instance() {
        return INSTANCE;
    }

    @Override
    public Vertex getElement() {
        throw Property.Exceptions.propertyDoesNotExist();
    }

    @Override
    public Object id() {
        throw Property.Exceptions.propertyDoesNotExist();
    }

    @Override
    public <U> Property<U> property(String key) {
        return Property.<U>empty();
    }

    @Override
    public <U> Property<U> property(String key, U value) {
        return Property.<U>empty();
    }

    @Override
    public String key() {
        throw Property.Exceptions.propertyDoesNotExist();
    }

    @Override
    public V value() throws NoSuchElementException {
        throw Property.Exceptions.propertyDoesNotExist();
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

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

    @Override
    public MetaProperty.Iterators iterators() {
        return new Iterators() {
            @Override
            public <U> Iterator<Property<U>> properties(String... propertyKeys) {
                return Collections.emptyIterator();
            }

            @Override
            public <U> Iterator<Property<U>> hiddens(String... propertyKeys) {
                return Collections.emptyIterator();
            }
        };
    }
}
