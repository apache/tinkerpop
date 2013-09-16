package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;
import com.tinkerpop.blueprints.util.StringFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class TinkerVertex extends TinkerElement implements Vertex, Serializable {

    protected Map<String, Set<Edge>> outEdges = new HashMap<>();
    protected Map<String, Set<Edge>> inEdges = new HashMap<>();

    protected TinkerVertex(final String id, final TinkerGraph graph) {
        super(id, graph);
    }

    public VertexQuery query() {
        return new TinkerVertexQuery(this);
    }

    public String toString() {
        return StringFactory.vertexString(this);
    }

    public Edge addEdge(final String label, final Vertex vertex, final Property... properties) {
        final Edge edge = this.graph.addEdge(this, vertex, label, properties);
        for (final Property property : properties) {
            edge.setProperty(property.getKey(), property.getValue());
        }
        return edge;
    }

    public void remove() {
        this.query().direction(Direction.BOTH).edges().forEach(Edge::remove);
        graph.vertexIndex.removeElement(this);
        graph.vertices.remove(this.id);
    }

    protected void addOutEdge(final String label, final Edge edge) {
        Set<Edge> edges = this.outEdges.get(label);
        if (null == edges) {
            edges = new HashSet<Edge>();
            this.outEdges.put(label, edges);
        }
        edges.add(edge);
    }

    protected void addInEdge(final String label, final Edge edge) {
        Set<Edge> edges = this.inEdges.get(label);
        if (null == edges) {
            edges = new HashSet<Edge>();
            this.inEdges.put(label, edges);
        }
        edges.add(edge);
    }
}
