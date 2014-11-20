package com.tinkerpop.gremlin.hadoop.structure;

import com.tinkerpop.gremlin.hadoop.process.graph.HadoopElementTraversal;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.wrapped.WrappedVertexProperty;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerProperty;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerVertexProperty;
import com.tinkerpop.gremlin.util.StreamFactory;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HadoopVertexProperty <V> implements VertexProperty<V>, VertexProperty.Iterators, WrappedVertexProperty<TinkerVertexProperty<V>> {

    private final TinkerVertexProperty<V> tinkerVertexProperty;
    private final HadoopVertex hadoopVertex;

    public HadoopVertexProperty(final TinkerVertexProperty<V> tinkerVertexProperty, final HadoopVertex hadoopVertex) {
        this.tinkerVertexProperty = tinkerVertexProperty;
        this.hadoopVertex = hadoopVertex;
    }

    @Override
    public GraphTraversal<VertexProperty, VertexProperty> start() {
        return new HadoopElementTraversal<>(this, this.hadoopVertex.graph);
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
    public Vertex element() {
        return this.hadoopVertex;
    }

    @Override
    public TinkerVertexProperty<V> getBaseVertexProperty() {
        return this.tinkerVertexProperty;
    }

    @Override
    public VertexProperty.Iterators iterators() {
        return this;
    }

    @Override
    public <U> Iterator<Property<U>> propertyIterator(final String... propertyKeys) {
        return (Iterator) StreamFactory.stream(getBaseVertexProperty().iterators().propertyIterator(propertyKeys))
                .map(property -> new HadoopProperty<>((TinkerProperty<V>) property, this)).iterator();
    }
}
