package com.tinkerpop.gremlin.giraph.structure;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.wrapped.WrappedVertexProperty;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerVertexProperty;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerProperty;
import com.tinkerpop.gremlin.util.StreamFactory;

import java.io.Serializable;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphVertexProperty<V> implements VertexProperty<V>, WrappedVertexProperty<TinkerVertexProperty<V>> {

    private final TinkerVertexProperty<V> tinkerVertexProperty;
    private final GiraphVertex giraphVertex;

    public GiraphVertexProperty(final TinkerVertexProperty<V> tinkerVertexProperty, final GiraphVertex giraphVertex) {
        this.tinkerVertexProperty = tinkerVertexProperty;
        this.giraphVertex = giraphVertex;
    }

    @Override
    public Object id() {
        return this.tinkerVertexProperty.id();
    }

    @Override
    public V value() {
        return this.tinkerVertexProperty.value();
    }

    @Override
    public String key() {
        return this.tinkerVertexProperty.key();
    }

    @Override
    public void remove() {
        this.tinkerVertexProperty.remove();
    }

    @Override
    public boolean isHidden() {
        return this.tinkerVertexProperty.isHidden();
    }

    @Override
    public boolean isPresent() {
        return this.tinkerVertexProperty.isPresent();
    }

    @Override
    public <U> Property<U> property(final String key) {
        return this.tinkerVertexProperty.property(key);
    }

    @Override
    public <U> Property<U> property(final String key, final U value) {
        throw Element.Exceptions.propertyAdditionNotSupported();
    }

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return this.tinkerVertexProperty.hashCode();
    }

    @Override
    public String toString() {
        return this.tinkerVertexProperty.toString();
    }

    @Override
    public VertexProperty.Iterators iterators() {
        return this.iterators;
    }

    @Override
    public Vertex getElement() {
        return this.giraphVertex;
    }

    @Override
    public TinkerVertexProperty<V> getBaseVertexProperty() {
        return this.tinkerVertexProperty;
    }

    private final VertexProperty.Iterators iterators = new Iterators();

    protected class Iterators implements VertexProperty.Iterators, Serializable {

        @Override
        public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
            return (Iterator) StreamFactory.stream(getBaseVertexProperty().iterators().properties(propertyKeys))
                    .map(property -> new GiraphProperty<>((TinkerProperty<V>) property, GiraphVertexProperty.this)).iterator();
        }

        @Override
        public <V> Iterator<Property<V>> hiddens(final String... propertyKeys) {
            return (Iterator) StreamFactory.stream(getBaseVertexProperty().iterators().hiddens(propertyKeys))
                    .map(property -> new GiraphProperty<>((TinkerProperty<V>) property, GiraphVertexProperty.this)).iterator();
        }
    }
}
