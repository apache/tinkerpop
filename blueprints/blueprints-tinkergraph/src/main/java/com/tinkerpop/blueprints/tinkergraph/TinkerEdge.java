package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ElementHelper;
import com.tinkerpop.blueprints.util.StringFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class TinkerEdge extends TinkerElement implements Edge, Serializable {

    protected final Map<String, Edge.Property> properties = new HashMap<>();
    private final Vertex inVertex;
    private final Vertex outVertex;

    protected TinkerEdge(final String id, final Vertex outVertex, final String label, final Vertex inVertex, final TinkerGraph graph) {
        super(id, label, graph);
        this.outVertex = outVertex;
        this.inVertex = inVertex;
        this.graph.edgeIndex.autoUpdate(StringFactory.LABEL, this.label, null, this);
    }

    public Map<String, Edge.Property> getProperties() {
        return new HashMap<>(this.properties);
    }

    public <V> Edge.Property<V> getProperty(final String key) {
        final Edge.Property<V> property = this.properties.get(key);
        return null == property ? Edge.Property.empty() : property;
    }

    public <V> Edge.Property<V> setProperty(final String key, final V value) {
        ElementHelper.validateProperty(key, value);
        final Property<V> property = new Property<>(this, key, value);
        this.properties.put(key, property);
        this.graph.edgeIndex.autoUpdate(key, value, property.getValue(), this);
        return property;
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
            throw Element.Exceptions.bothIsNotSupported();
    }

    public void remove() {
        if (!this.graph.edges.containsKey(this.getId()))
            throw Element.Exceptions.elementHasAlreadyBeenRemovedOrDoesNotExist(Edge.class, this.getId());

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

    public class Property<V> implements Edge.Property<V> {

        private final TinkerEdge edge;
        private final String key;
        private final V value;

        public Property(final TinkerEdge edge, final String key, final V value) {
            this.edge = edge;
            this.key = key;
            this.value = value;

        }

        public Edge getEdge() {
            return this.edge;
        }

        public String getKey() {
            return this.key;
        }

        public V getValue() {
            return this.value;
        }

        public void remove() {
            this.edge.properties.remove(key);
            this.edge.graph.edgeIndex.autoRemove(this.key, this.isPresent() ? this.value : null, this.edge);
        }

        public boolean isPresent() {
            return null != this.value;
        }
    }
}
