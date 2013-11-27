package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.StringFactory;
import com.tinkerpop.blueprints.util.ThingHelper;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class TinkerEdge extends TinkerElement implements Edge, Serializable {

    private final Map<String, Property<?, Edge>> properties = new HashMap<>();
    private final Vertex inVertex;
    private final Vertex outVertex;

    protected TinkerEdge(final String id, final Vertex outVertex, final String label, final Vertex inVertex, final TinkerGraph graph) {
        super(id, label, graph);
        this.outVertex = outVertex;
        this.inVertex = inVertex;
        this.graph.edgeIndex.autoUpdate(StringFactory.LABEL, this.label, null, this);
    }

    public Map<String, Property<?, Edge>> getProperties() {
        return new HashMap<>(this.properties);
    }

    public <V> Property<V, Edge> getProperty(final String key) {
        final Property<V, Edge> property = (Property) this.properties.get(key);
        return null == property ? Property.empty() : property;
    }

    public <V> Property<V, Edge> setProperty(final String key, final V value) {
        ThingHelper.validateProperty(this, key, value);
        final Property<V, Edge> property = new TinkerProperty<>(key, value, (Edge) this);
        this.properties.put(key, property);
        this.graph.edgeIndex.autoUpdate(key, value, property.getValue(), this);
        return property;
    }

    public void removeProperty(final String key) {
        final Property<?, Edge> property = this.properties.remove(key);
        this.graph.edgeIndex.autoRemove(key, null == property ? null : property.getValue(), this);
    }

    public Set<String> getPropertyKeys() {
        return this.properties.keySet();
    }

    public Vertex getVertex(final Direction direction) throws IllegalArgumentException {
        if (direction.equals(Direction.IN))
            return this.inVertex;
        else if (direction.equals(Direction.OUT))
            return this.outVertex;
        else
            throw Element.Features.bothIsNotSupported();
    }

    public void remove() {
        final TinkerVertex outVertex = (TinkerVertex) this.getVertex(Direction.OUT);
        final TinkerVertex inVertex = (TinkerVertex) this.getVertex(Direction.IN);
        if (null != outVertex && null != outVertex.outEdges) {
            final Set<Edge> edges = outVertex.outEdges.get(this.getLabel());
            if (null != edges)
                edges.remove(this);
        }
        if (null != inVertex && null != inVertex.inEdges) {
            final Set<Edge> edges = inVertex.inEdges.get(this.getLabel());
            if (null != edges)
                edges.remove(this);
        }

        this.graph.edgeIndex.removeElement(this);
        this.graph.edges.remove(this.getId());
    }

    public String toString() {
        return StringFactory.edgeString(this);
    }
}
