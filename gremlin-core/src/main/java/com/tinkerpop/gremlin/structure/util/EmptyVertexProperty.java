package com.tinkerpop.gremlin.structure.util;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EmptyVertexProperty<V> implements VertexProperty<V> {

    private static final EmptyVertexProperty INSTANCE = new EmptyVertexProperty();

    public static <U> VertexProperty<U> instance() {
        return INSTANCE;
    }

    @Override
    public Vertex element() {
        throw Property.Exceptions.propertyDoesNotExist();
    }

    @Override
    public Object id() {
        throw Property.Exceptions.propertyDoesNotExist();
    }

    @Override
    public Graph graph() {
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
    public VertexProperty.Iterators iterators() {
        return new Iterators() {
            @Override
            public <U> Iterator<Property<U>> propertyIterator(String... propertyKeys) {
                return Collections.emptyIterator();
            }

            @Override
            public <U> Iterator<Property<U>> hiddenPropertyIterator(String... propertyKeys) {
                return Collections.emptyIterator();
            }
        };
    }
}
