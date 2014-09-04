package com.tinkerpop.gremlin.tinkergraph.structure;

import com.tinkerpop.gremlin.structure.MetaProperty;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerMetaProperty<V> extends TinkerElement implements MetaProperty<V>, Serializable {

    private final TinkerVertex vertex;
    private final String key;
    private final V value;

    public TinkerMetaProperty(final TinkerVertex vertex, final String key, final V value, final Object... propertyKeyValues) {
        super(TinkerHelper.getNextId(vertex.graph), META_PROPERTY, vertex.graph);
        this.vertex = vertex;
        this.key = key;
        this.value = value;
        ElementHelper.legalPropertyKeyValueArray(propertyKeyValues);
        ElementHelper.attachProperties(this, propertyKeyValues);
    }

    @Override
    public String key() {
        return this.key;
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
    public String toString() {
        return StringFactory.metaPropertyString(this);
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
        return ElementHelper.areEqual((MetaProperty) this, object);
    }

    @Override
    public <U> Property<U> property(final String key, final U value) {
        final Property<U> property = new TinkerProperty<U>(this, key, value);
        this.properties.put(key, Arrays.asList(property));
        return property;
    }

    @Override
    public Vertex getElement() {
        return this.vertex;
    }

    @Override
    public void remove() {
        this.vertex.properties.get(this.key).remove(this);
        if (this.vertex.properties.get(this.key).size() == 0) {
            this.vertex.properties.remove(this.key);
            this.graph.vertexIndex.remove(this.key, this.value, this.vertex);
        }
        final AtomicBoolean delete = new AtomicBoolean(true);
        this.vertex.properties(this.key).forEachRemaining(property -> {
            if (property.value().equals(this.value))
                delete.set(false);
        });
        if (delete.get()) this.graph.vertexIndex.remove(this.key, this.value, this.vertex);

    }

    public MetaProperty.Iterators iterators() {
        return this.iterators;
    }

    private final MetaProperty.Iterators iterators = new Iterators(this);

    protected class Iterators extends TinkerElement.Iterators implements MetaProperty.Iterators {

        public Iterators(final TinkerMetaProperty metaProperty) {
            super(metaProperty);
        }

        @Override
        public <U> Iterator<Property<U>> properties(final String... propertyKeys) {
            return (Iterator) super.properties(propertyKeys);
        }

        @Override
        public <U> Iterator<Property<U>> hiddens(final String... propertyKeys) {
            return (Iterator) super.hiddens(propertyKeys);
        }
    }
}
