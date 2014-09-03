package com.tinkerpop.gremlin.tinkergraph.structure;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.MetaProperty;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerMetaProperty<V> extends TinkerProperty<V> implements MetaProperty<V> {

    protected Map<String, Property> properties = new HashMap<>();

    public TinkerMetaProperty(final Vertex vertex, final String key, final V value, final Object... propertyKeyValues) {
        super(vertex, key, value);
        ElementHelper.legalPropertyKeyValueArray(propertyKeyValues);
        ElementHelper.attachProperties(this, propertyKeyValues);
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
        final Property oldProperty = this.property(key);
        final Property newProperty = new TinkerProperty<>(this, key, value);
        this.properties.put(key, newProperty);
        this.graph.vertexIndex.autoUpdate(key, value, oldProperty.isPresent() ? oldProperty.value() : null, (TinkerVertex) this.element);
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

}
