package com.tinkerpop.gremlin.hadoop.structure;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.wrapped.WrappedVertexProperty;
import com.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HadoopVertexProperty<V> implements VertexProperty<V>, VertexProperty.Iterators, WrappedVertexProperty<VertexProperty<V>> {

    private final VertexProperty<V> baseVertexProperty;
    private final HadoopVertex hadoopVertex;

    public HadoopVertexProperty(final VertexProperty<V> baseVertexProperty, final HadoopVertex hadoopVertex) {
        this.baseVertexProperty = baseVertexProperty;
        this.hadoopVertex = hadoopVertex;
    }

    @Override
    public Object id() {
        return this.baseVertexProperty.id();
    }

    @Override
    public V value() {
        return this.baseVertexProperty.value();
    }

    @Override
    public String key() {
        return this.baseVertexProperty.key();
    }

    @Override
    public void remove() {
        this.baseVertexProperty.remove();
    }

    @Override
    public boolean isPresent() {
        return this.baseVertexProperty.isPresent();
    }

    @Override
    public <U> Property<U> property(final String key) {
        return this.baseVertexProperty.property(key);
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
        return this.baseVertexProperty.hashCode();
    }

    @Override
    public String toString() {
        return this.baseVertexProperty.toString();
    }

    @Override
    public Vertex element() {
        return this.hadoopVertex;
    }

    @Override
    public VertexProperty<V> getBaseVertexProperty() {
        return this.baseVertexProperty;
    }

    @Override
    public VertexProperty.Iterators iterators() {
        return this;
    }

    @Override
    public <U> Iterator<Property<U>> propertyIterator(final String... propertyKeys) {
        return IteratorUtils.<Property<U>, Property<U>>map(this.getBaseVertexProperty().iterators().propertyIterator(propertyKeys), property -> new HadoopProperty<>(property, HadoopVertexProperty.this));
    }
}
