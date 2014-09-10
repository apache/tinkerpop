package com.tinkerpop.gremlin.giraph.structure;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.MetaProperty;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.wrapped.WrappedMetaProperty;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerMetaProperty;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerProperty;
import com.tinkerpop.gremlin.util.StreamFactory;

import java.io.Serializable;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphMetaProperty<V> implements MetaProperty<V>, WrappedMetaProperty<TinkerMetaProperty<V>>, Serializable {

    private final TinkerMetaProperty<V> tinkerMetaProperty;
    private final GiraphVertex giraphVertex;

    public GiraphMetaProperty(final TinkerMetaProperty<V> tinkerMetaProperty, final GiraphVertex giraphVertex) {
        this.tinkerMetaProperty = tinkerMetaProperty;
        this.giraphVertex = giraphVertex;
    }

    @Override
    public Object id() {
        return this.tinkerMetaProperty.id();
    }

    @Override
    public V value() {
        return this.tinkerMetaProperty.value();
    }

    @Override
    public String key() {
        return this.tinkerMetaProperty.key();
    }

    @Override
    public void remove() {
        this.tinkerMetaProperty.remove();
    }

    @Override
    public boolean isHidden() {
        return this.tinkerMetaProperty.isHidden();
    }

    @Override
    public boolean isPresent() {
        return this.tinkerMetaProperty.isPresent();
    }

    @Override
    public <U> Property<U> property(final String key) {
        return this.tinkerMetaProperty.property(key);
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
        return this.tinkerMetaProperty.hashCode();
    }

    @Override
    public String toString() {
        return this.tinkerMetaProperty.toString();
    }

    @Override
    public MetaProperty.Iterators iterators() {
        return this.iterators;
    }

    @Override
    public Vertex getElement() {
        return this.giraphVertex;
    }

    @Override
    public TinkerMetaProperty<V> getBaseMetaProperty() {
        return this.tinkerMetaProperty;
    }

    private final MetaProperty.Iterators iterators = new Iterators();

    protected class Iterators implements MetaProperty.Iterators, Serializable {

        @Override
        public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
            return (Iterator) StreamFactory.stream(getBaseMetaProperty().iterators().properties(propertyKeys))
                    .map(property -> new GiraphProperty<>((TinkerProperty<V>) property, GiraphMetaProperty.this)).iterator();
        }

        @Override
        public <V> Iterator<Property<V>> hiddens(final String... propertyKeys) {
            return (Iterator) StreamFactory.stream(getBaseMetaProperty().iterators().hiddens(propertyKeys))
                    .map(property -> new GiraphProperty<>((TinkerProperty<V>) property, GiraphMetaProperty.this)).iterator();
        }
    }
}
