package com.tinkerpop.gremlin.tinkergraph.structure;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.MetaProperty;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerMetaProperty<V> extends TinkerElement implements MetaProperty<V> {

    private final Vertex vertex;
    private final String key;
    private final V value;

    public TinkerMetaProperty(final TinkerVertex vertex, final String key, final V value, final Object... propertyKeyValues) {
        super(key.hashCode() + value.hashCode(), META_PROPERTY, vertex.graph);
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
    public boolean isHidden() {
        return Graph.Key.isHidden(this.key);
    }

    public String toString() {
        return StringFactory.propertyString(this);
    }

    public int hashCode() {
        return this.key.hashCode() + this.value.hashCode() + this.vertex.hashCode();
    }

    @Override
    public Object id() {
        return this.key.hashCode() + this.value.hashCode();
    }


    public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
        return (Iterator) super.properties(propertyKeys);
    }

    public <V> Iterator<Property<V>> hiddens(final String... propertyKeys) {
        return (Iterator) super.hiddens(propertyKeys);
    }


    @Override
    public <U> Property<U> property(final String key) {
        return this.properties.containsKey(key) ? this.properties.get(key).get(0) : Property.empty();
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
        ((TinkerVertex) this.vertex).properties.get(this.key).remove(this);
    }
}
