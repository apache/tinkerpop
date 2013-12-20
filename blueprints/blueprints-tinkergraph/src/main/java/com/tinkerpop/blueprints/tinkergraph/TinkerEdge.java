package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Property;
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

    private final Vertex inVertex;
    private final Vertex outVertex;

    protected TinkerEdge(final String id, final Vertex outVertex, final String label, final Vertex inVertex, final TinkerGraph graph) {
        super(id, label, graph);
        this.outVertex = outVertex;
        this.inVertex = inVertex;
        this.graph.edgeIndex.autoUpdate(StringFactory.LABEL, this.label, null, this);
    }

    public <V> void setProperty(final String key, final V value) {
        ElementHelper.validateProperty(key, value);
        final TinkerEdge edge = this;
        final Property oldProperty = super.getProperty(key);
        this.properties.put(key, new TinkerProperty<V>(this, key, value) {
            public void remove() {
                edge.properties.remove(key);
            }

            public <E extends Element> E getElement() {
                return (E) edge;
            }
        });
        this.graph.edgeIndex.autoUpdate(key, value, oldProperty.isPresent() ? oldProperty.getValue() : null, this);
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
}
