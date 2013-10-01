package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.query.VertexQuery;
import com.tinkerpop.blueprints.util.ElementHelper;
import com.tinkerpop.blueprints.util.StringFactory;

import java.io.Serializable;
import java.util.HashMap;
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

    public <T> Property<T, Vertex> getProperty(final String key) {
        final Property<T, Vertex> t = this.properties.get(key);
        return (null == t) ? Property.<T, Vertex>empty() : t;
    }

    public <T> Property<T, Vertex> setProperty(final String key, final T value) {
        ElementHelper.validateProperty(this, key, value);
        final Property<T, Vertex> oldValue = this.properties.put(key, new TinkerProperty<>(key, value, this));
        this.graph.vertexIndex.autoUpdate(key, value, oldValue, this);
        return oldValue;
    }

    public <T> Property<T, Vertex> removeProperty(final String key) {
        final Property<T, Vertex> oldValue = this.properties.remove(key);
        this.graph.vertexIndex.autoRemove(key, oldValue, this);
        return oldValue;
    }

    public VertexQuery query() {
        return new TinkerVertexQuery(this);
    }

    public String toString() {
        return StringFactory.vertexString(this);
    }

    public Edge addEdge(final String label, final Vertex vertex, final Property... properties) {
        return TinkerHelper.addEdge(this.graph, this, (TinkerVertex) vertex, label, properties);
    }

    public void remove() {
        this.query().direction(Direction.BOTH).edges().forEach(Edge::remove);
        graph.vertexIndex.removeElement(this);
        graph.vertices.remove(this.id);
    }
}
