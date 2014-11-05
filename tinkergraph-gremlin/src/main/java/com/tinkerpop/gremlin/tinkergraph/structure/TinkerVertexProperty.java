package com.tinkerpop.gremlin.tinkergraph.structure;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerVertexProperty<V> extends TinkerElement implements VertexProperty<V>, VertexProperty.Iterators {

    private final TinkerVertex vertex;
    private final String key;
    private final V value;

    public TinkerVertexProperty(final TinkerVertex vertex, final String key, final V value, final Object... propertyKeyValues) {
        super(TinkerHelper.getNextId(vertex.graph), key, vertex.graph);
        this.vertex = vertex;
        this.key = key;
        this.value = value;
        ElementHelper.legalPropertyKeyValueArray(propertyKeyValues);
        ElementHelper.attachProperties(this, propertyKeyValues);
    }

    public TinkerVertexProperty(final Object id, final TinkerVertex vertex, final String key, final V value, final Object... propertyKeyValues) {
        super(id, key, vertex.graph);
        this.vertex = vertex;
        this.key = key;
        this.value = value;
        ElementHelper.legalPropertyKeyValueArray(propertyKeyValues);
        ElementHelper.attachProperties(this, propertyKeyValues);
    }

    @Override
    public String key() {
        return Graph.Key.unHide(this.key);
    }

    @Override
    public V value() {
        return this.value;
    }

    @Override
    public boolean isPresent() {
        return true;
    }

    @Override
    public boolean isHidden() {
        return Graph.Key.isHidden(this.key);
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

    @Override
    public int hashCode() {
        return this.key.hashCode() + this.value.hashCode() + this.vertex.hashCode() + this.properties.hashCode();
    }

    @Override
    public Object id() {
        return this.id;
    }

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public <U> Property<U> property(final String key, final U value) {
        final Property<U> property = new TinkerProperty<U>(this, key, value);
        this.properties.put(key, Arrays.asList(property));
        return property;
    }

    @Override
    public Vertex element() {
        return this.vertex;
    }

    @Override
    public void remove() {
        if (this.vertex.properties.containsKey(this.key)) {
            this.vertex.properties.get(this.key).remove(this);
            if (this.vertex.properties.get(this.key).size() == 0) {
                this.vertex.properties.remove(this.key);
                this.graph.vertexIndex.remove(this.key, this.value, this.vertex);
            }
            final AtomicBoolean delete = new AtomicBoolean(true);
            this.vertex.propertyIterator(this.key).forEachRemaining(property -> {
                if (property.value().equals(this.value))
                    delete.set(false);
            });
            if (delete.get()) this.graph.vertexIndex.remove(this.key, this.value, this.vertex);
            this.properties.clear();
            this.removed = true;
        }
    }

    //////////////////////////////////////////////

    public VertexProperty.Iterators iterators() {
        return this;
    }

    @Override
    public <U> Iterator<Property<U>> propertyIterator(final String... propertyKeys) {
        return (Iterator) super.propertyIterator(propertyKeys);
    }

    @Override
    public <U> Iterator<Property<U>> hiddenPropertyIterator(final String... propertyKeys) {
        return (Iterator) super.hiddenPropertyIterator(propertyKeys);
    }

}
