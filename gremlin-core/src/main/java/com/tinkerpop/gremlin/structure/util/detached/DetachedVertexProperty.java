package com.tinkerpop.gremlin.structure.util.detached;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DetachedVertexProperty<V> extends DetachedElement<Property<V>> implements VertexProperty<V>, VertexProperty.Iterators {

    protected String key;
    protected V value;
    protected transient DetachedVertex vertex;

    private DetachedVertexProperty() {
    }

    public DetachedVertexProperty(final VertexProperty vertexProperty, final boolean asReference) {
        super(vertexProperty);
        this.key = vertexProperty.key();
        this.value = (V) vertexProperty.value();
        this.vertex = DetachedFactory.detach(vertexProperty.element(), true);

        if (!asReference & vertexProperty.graph().features().vertex().supportsMetaProperties()) {
            this.properties = new HashMap<>();
            vertexProperty.iterators().propertyIterator().forEachRemaining(property -> this.properties.put(property.key(), DetachedElement.makeSinglePropertyList(DetachedFactory.detach(property))));
        }
    }

    public DetachedVertexProperty(final Object id, final String label, final String key, final V value,
                                  final Map<String, Object> properties,
                                  final Vertex vertex) {
        super(id, label);
        if (null == key) throw Graph.Exceptions.argumentCanNotBeNull("key");
        if (null == value) throw Graph.Exceptions.argumentCanNotBeNull("value");
        if (null == vertex) throw Graph.Exceptions.argumentCanNotBeNull("vertex");

        this.key = key;
        this.value = value;
        this.vertex = DetachedFactory.detach(vertex, true);

        if (!properties.isEmpty()) {
            this.properties = new HashMap<>();
            properties.entrySet().iterator().forEachRemaining(entry -> this.properties.put(entry.getKey(), DetachedElement.makeSinglePropertyList(new DetachedProperty(entry.getKey(), entry.getValue(), this))));
        }
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
    public String key() {
        return this.key;
    }

    @Override
    public V value() {
        return this.value;
    }

    @Override
    public Vertex element() {
        return this.vertex;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Detached properties are readonly: " + this.toString());
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public VertexProperty<V> attach(final Vertex hostVertex) {
        final VertexProperty<V> hostVertexProperty = hostVertex.property(this.key);
        if (hostVertexProperty.isPresent())
            return hostVertexProperty;
        else
            throw new IllegalStateException("The detached vertex property could not be be found at the provided vertex: " + this);
    }

    @Override
    public VertexProperty<V> attach(final Graph hostGraph) {
        return this.attach(this.vertex.attach(hostGraph));
    }


    @Override
    public VertexProperty.Iterators iterators() {
        return this;
    }

    @Override
    public <U> Iterator<Property<U>> propertyIterator(final String... propertyKeys) {
        return (Iterator) super.propertyIterator(propertyKeys);
    }
}
