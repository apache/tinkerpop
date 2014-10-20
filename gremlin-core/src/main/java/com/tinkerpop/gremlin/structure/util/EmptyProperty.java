package com.tinkerpop.gremlin.structure.util;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;

import java.util.NoSuchElementException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EmptyProperty<V> implements Property<V> {

    private static final EmptyProperty INSTANCE = new EmptyProperty();

    private EmptyProperty() {

    }

    @Override
    public String key() {
        throw Exceptions.propertyDoesNotExist();
    }

    @Override
    public V value() throws NoSuchElementException {
        throw Exceptions.propertyDoesNotExist();
    }

    @Override
    public boolean isPresent() {
        return false;
    }

    @Override
    public boolean isHidden() {
        throw Exceptions.propertyDoesNotExist();
    }

    @Override
    public <E extends Element> E element() {
        throw Exceptions.propertyDoesNotExist();
    }

    @Override
    public void remove() {

    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object object) {
        return object instanceof EmptyProperty;
    }

    public int hashCode() {
        return 1281483122;
    }

    public static <V> Property<V> instance() {
        return INSTANCE;
    }
}
