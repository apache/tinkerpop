package com.tinkerpop.gremlin.tinkergraph.structure;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.MetaProperty;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.HashMap;
import java.util.Map;

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

    @Override
    public String label() {
        return META_PROPERTY;
    }

    @Override
    public <U> Property<U> property(final String key, final U value) {
        ElementHelper.validateProperty(key, value);
        final Property newProperty = new TinkerProperty<>(this, key, value);
        this.properties.put(key, newProperty);
        return newProperty;
    }

    @Override
    public Map<String, Property> hiddens() {
        final Map<String, Property> temp = new HashMap<>();
        this.properties.forEach((key, property) -> {
            if (Graph.Key.isHidden(key))
                temp.put(Graph.Key.unHide(key), property);
        });
        return temp;
    }

    @Override
    public Map<String, Property> properties() {
        final Map<String, Property> temp = new HashMap<>();
        this.properties.forEach((key, property) -> {
            if (!Graph.Key.isHidden(key))
                temp.put(key, property);
        });
        return temp;
    }


    @Override
    public <U> Property<U> property(final String key) {
        return this.properties.getOrDefault(key, Property.empty());
    }

    @Override
    public Vertex getElement() {
        return this.vertex;
    }

    @Override
    public void remove() {
        ((TinkerVertex) this.vertex).metaProperties.get(this.key).remove(this);
    }
}
