package com.tinkerpop.gremlin.structure.util.detached;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DetachedVertexProperty<V> extends DetachedElement<Property<V>> implements VertexProperty<V>, VertexProperty.Iterators {

    String key;
    V value;
    transient DetachedVertex vertex;

    /**
     * Construct a {@code DetachedVertexProperty} during manual deserialization.
     */
    public DetachedVertexProperty(final Object id, final String label, final String key, final V value,
                                  final Map<String, Object> properties, final Map<String, Object> hiddenProperties,
                                  final DetachedVertex vertex) {
        super(id, label);
        if (null == key) throw Graph.Exceptions.argumentCanNotBeNull("key");
        if (null == value) throw Graph.Exceptions.argumentCanNotBeNull("value");
        if (null == vertex) throw Graph.Exceptions.argumentCanNotBeNull("vertex");

        this.key = key;
        this.value = value;
        this.vertex = vertex;

        if (properties != null)
            properties.entrySet().iterator().forEachRemaining(kv -> putToList(kv.getKey(), new DetachedProperty(kv.getKey(), kv.getValue(), this)));
        if (hiddenProperties != null)
            hiddenProperties.entrySet().iterator().forEachRemaining(kv -> putToList(Graph.Key.hide(kv.getKey()), new DetachedProperty(kv.getKey(), kv.getValue(), this)));
    }

    /**
     * Construct a {@code DetachedVertexProperty} internally when a {@link DetachedVertex} is being constructed.
     */
    DetachedVertexProperty(final VertexProperty property, final DetachedVertex detachedVertex) {
        super(property);
        if (null == property) throw Graph.Exceptions.argumentCanNotBeNull("property");

        this.key = property.isHidden() ? Graph.Key.hide(property.key()) : property.key();
        this.value = (V) property.value();
        this.vertex = detachedVertex;

        if (property.graph().features().vertex().supportsMetaProperties()) {
            property.iterators().propertyIterator().forEachRemaining(p -> putToList(p.key(), p instanceof DetachedProperty ? p : new DetachedProperty(p, this)));
            property.iterators().hiddenPropertyIterator().forEachRemaining(p -> putToList(Graph.Key.hide(p.key()), p instanceof DetachedProperty ? p : new DetachedProperty(p, this)));
        }
    }

    private DetachedVertexProperty(final VertexProperty property) {
        this(property, property.element() instanceof DetachedVertex ? (DetachedVertex) property.element() : DetachedVertex.detach(property.element()));
    }

    private DetachedVertexProperty() {
        // no implementation
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
        return Graph.Key.unHide(this.key);
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

    public static DetachedVertexProperty detach(final VertexProperty vertexProperty) {
        if (null == vertexProperty) throw Graph.Exceptions.argumentCanNotBeNull("vertexProperty");
        return (vertexProperty instanceof DetachedVertexProperty) ? (DetachedVertexProperty) vertexProperty : new DetachedVertexProperty(vertexProperty);
    }

    @Override
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

    private void putToList(final String key, final Property p) {
        if (!this.properties.containsKey(key))
            this.properties.put(key, new ArrayList<>());

        ((List) this.properties.get(key)).add(p);
    }
}
