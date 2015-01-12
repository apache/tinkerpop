package com.tinkerpop.gremlin.structure.util.detached;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Represents a {@link Vertex} that is disconnected from a {@link Graph}.  "Disconnection" can mean detachment from
 * a {@link Graph} in the sense that a {@link Vertex} was constructed from a {@link Graph} instance and this reference
 * was removed or it can mean that the {@code DetachedVertex} could have been constructed independently of a
 * {@link Graph} instance in the first place.
 * <br/>
 * A {@code DetachedVertex} only has reference to the properties that are associated with it at the time of detachment
 * (or construction) and is not traversable or mutable.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DetachedVertex extends DetachedElement<Vertex> implements Vertex, Vertex.Iterators {

    private static final String ID = "id";
    private static final String LABEL = "label";
    private static final String VALUE = "value";
    private static final String PROPERTIES = "properties";

    private DetachedVertex() {
    }

    protected DetachedVertex(final Vertex vertex, final boolean withProperties) {
        super(vertex);
        if (withProperties) {
            this.properties = new HashMap<>();
            vertex.iterators().propertyIterator().forEachRemaining(property -> {
                final List<VertexProperty<?>> list = (List<VertexProperty<?>>) this.properties.getOrDefault(property.key(), new ArrayList<>());
                list.add(DetachedFactory.detach(property, true));
                this.properties.put(property.key(), list);
            });
        }
    }

    public DetachedVertex(final Object id, final String label, final Map<String, Object> properties) {
        super(id, label);
        if (!properties.isEmpty()) {
            this.properties = new HashMap<>();
            properties.entrySet().stream().forEach(
                    entry -> this.properties.put(entry.getKey(), ((List<Map<String, Object>>) entry.getValue()).stream()
                            .map(m -> (Property) new DetachedVertexProperty<>(m.get(ID), entry.getKey(), m.get(VALUE), (Map<String, Object>) m.getOrDefault(PROPERTIES, new HashMap<>()), this))
                            .collect(Collectors.toList())));
        }
    }

    @Override
    public <V> VertexProperty<V> property(final String key, final V value) {
        throw new UnsupportedOperationException("Detached vertices are readonly: " + this);
    }

    @Override
    public <V> VertexProperty<V> property(final String key) {
        if (this.properties.containsKey(key)) {
            final List<VertexProperty> list = (List) this.properties.get(key);
            if (list.size() > 1)
                throw Vertex.Exceptions.multiplePropertiesExistForProvidedKey(key);
            else
                return list.get(0);
        } else
            return VertexProperty.<V>empty();
    }

    @Override
    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        throw new UnsupportedOperationException("Detached vertices do not store edges: " + this);
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

    @Override
    public Vertex attach(final Vertex hostVertex) {
        if (hostVertex.equals(this))
            return hostVertex;
        else
            throw new IllegalStateException("The host vertex must be the detached vertex to attach: " + this);
    }

    @Override
    public Vertex attach(final Graph hostGraph) {
        return hostGraph.iterators().vertexIterator(this.id).next();
    }

    public static Vertex addTo(final Graph graph, final DetachedVertex detachedVertex) {
        final Vertex vertex = graph.addVertex(T.id, detachedVertex.id(), T.label, detachedVertex.label());
        detachedVertex.properties.entrySet().forEach(kv ->
                        kv.getValue().forEach(property -> {
                            final VertexProperty vertexProperty = (VertexProperty) property;
                            final List<Object> propsOnProps = new ArrayList<>();
                            vertexProperty.iterators().propertyIterator().forEachRemaining(h -> {
                                propsOnProps.add(h.key());
                                propsOnProps.add(h.value());
                            });
                            propsOnProps.add(T.id);
                            propsOnProps.add(vertexProperty.id());
                            vertex.property(kv.getKey(), property.value(), propsOnProps.toArray());
                        })
        );
        return vertex;
    }

    @Override
    public Vertex.Iterators iterators() {
        return this;
    }

    @Override
    public <V> Iterator<VertexProperty<V>> propertyIterator(final String... propertyKeys) {
        return (Iterator) super.propertyIterator(propertyKeys);
    }

    @Override
    public GraphTraversal<Vertex, Edge> edgeIterator(final Direction direction, final String... edgeLabels) {
        throw new UnsupportedOperationException("Detached vertices do not have edges");
    }

    @Override
    public GraphTraversal<Vertex, Vertex> vertexIterator(final Direction direction, final String... labels) {
        throw new UnsupportedOperationException("Detached vertices do not have edges and thus, adjacent vertices can not be accessed");
    }
}
