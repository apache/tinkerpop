package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.computer.GraphComputer;
import com.tinkerpop.blueprints.query.VertexQuery;
import com.tinkerpop.blueprints.util.StringFactory;
import com.tinkerpop.blueprints.util.ThingHelper;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class TinkerVertex extends TinkerElement implements Vertex, Serializable {

    protected enum State {STANDARD, CENTRIC, ADJACENT}

    protected TinkerVertexMemory vertexMemory = null;
    protected final State state;
    protected String centricId;

    protected Map<String, Set<Edge>> outEdges = new HashMap<>();
    protected Map<String, Set<Edge>> inEdges = new HashMap<>();

    protected TinkerVertex(final String id, final TinkerGraph graph) {
        super(id, graph);
        this.state = State.STANDARD;
        this.centricId = id;
    }

    protected TinkerVertex(final TinkerVertex vertex, final State state, final String centricId, final TinkerVertexMemory vertexMemory) {
        super(vertex.id, vertex.graph);
        this.state = state;
        this.outEdges = vertex.outEdges;
        this.inEdges = vertex.inEdges;
        this.properties = vertex.properties;
        this.vertexMemory = vertexMemory;
        this.centricId = centricId;
    }

    public <T> Property<T, Vertex> getProperty(final String key) {
        if (State.STANDARD == this.state) {
            final Property<T, Vertex> property = this.properties.get(key);
            return (null == property) ? Property.empty() : property;
        } else if (State.CENTRIC == this.state) {
            if (this.vertexMemory.isComputeKey(key))
                return this.vertexMemory.getProperty(this, key);
            else {
                final Property<T, Vertex> property = this.properties.get(key);
                return (null == property) ? Property.empty() : property;
            }
        } else {
            throw GraphComputer.Features.adjacentVertexPropertiesCanNotBeRead();
        }
    }

    public <T> Property<T, Vertex> setProperty(final String key, final T value) {
        if (State.STANDARD == this.state) {
            ThingHelper.validateProperty(this, key, value);
            final Property<T, Vertex> property = new TinkerProperty<>(key, value, (Vertex) this);
            this.properties.put(key, property);
            this.graph.vertexIndex.autoUpdate(key, value, property.getValue(), this);
            return property;
        } else if (State.CENTRIC == this.state) {
            if (this.vertexMemory.isComputeKey(key))
                return this.vertexMemory.setProperty(this, key, value);
            else
                throw GraphComputer.Features.providedKeyIsNotAComputeKey(key);
        } else {
            throw GraphComputer.Features.adjacentVertexPropertiesCanNotBeWritten();
        }
    }

    public <T> Property<T, Vertex> removeProperty(final String key) {
        if (State.STANDARD == this.state) {
            final Property<T, Vertex> property = this.properties.remove(key);
            this.graph.vertexIndex.autoRemove(key, null == property ? null : property.getValue(), this);
            return null == property ? Property.empty() : property;
        } else if (State.CENTRIC == this.state) {
            if (this.vertexMemory.isComputeKey(key))
                return this.vertexMemory.removeProperty(this, key);
            else
                throw GraphComputer.Features.providedKeyIsNotAComputeKey(key);
        } else {
            throw GraphComputer.Features.adjacentVertexPropertiesCanNotBeWritten();
        }
    }

    public VertexQuery query() {
        return new TinkerVertexQuery(this, this.vertexMemory);
    }

    public String toString() {
        return StringFactory.vertexString(this);
    }

    public Edge addEdge(final String label, final Vertex vertex, final Property... properties) {
        return TinkerHelper.addEdge(this.graph, this, (TinkerVertex) vertex, label, properties);
    }

    public void remove() {
        this.query().direction(Direction.BOTH).edges().forEach(Edge::remove);
        this.properties.clear();
        graph.vertexIndex.removeElement(this);
        graph.vertices.remove(this.id);
    }

    public TinkerVertex createClone(final State state, final String centricId, final TinkerVertexMemory vertexMemory) {
        return new TinkerVertex(this, state, centricId, vertexMemory);
    }
}
