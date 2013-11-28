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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class TinkerVertex extends TinkerElement implements Vertex, Serializable {

    protected Map<String, List<Property<?, Vertex>>> properties = new HashMap<>();

    protected enum State {STANDARD, CENTRIC, ADJACENT}

    protected TinkerVertexMemory vertexMemory;
    protected final State state;
    protected String centricId;

    protected Map<String, Set<Edge>> outEdges = new HashMap<>();
    protected Map<String, Set<Edge>> inEdges = new HashMap<>();

    protected TinkerVertex(final String id, final String label, final TinkerGraph graph) {
        super(id, label, graph);
        this.state = State.STANDARD;
        this.centricId = id;
    }

    protected TinkerVertex(final TinkerVertex vertex, final State state, final String centricId, final TinkerVertexMemory vertexMemory) {
        super(vertex.id, vertex.label, vertex.graph);
        this.state = state;
        this.outEdges = vertex.outEdges;
        this.inEdges = vertex.inEdges;
        this.properties = vertex.properties;
        this.vertexMemory = vertexMemory;
        this.centricId = centricId;
    }

    public <V> Property<V, Vertex> addProperty(final String key, final V value) {
        final Property<V, Vertex> property = new TinkerProperty<V, Vertex>(key, value, this);
        final List<Property<?, Vertex>> list = this.properties.getOrDefault(key, new ArrayList<>());
        list.add(property);
        this.properties.put(key, list);
        return property;
    }

    public Set<String> getPropertyKeys() {
        return this.properties.keySet();
    }

    public <V> Iterable<Property<V, Vertex>> getProperties(final String key) {
        return (Iterable) this.properties.getOrDefault(key, Collections.EMPTY_LIST);
    }

    public Map<String, Iterable<Property<?, Vertex>>> getProperties() {
        return (Map) new HashMap<>(this.properties);
    }

    public <V> Property<V, Vertex> getProperty(final String key) {
        if (State.STANDARD == this.state) {
            final List<Property<V, Vertex>> list = (List) this.properties.getOrDefault(key, Collections.EMPTY_LIST);
            if (list.size() == 0) return Property.empty();
            else if (list.size() > 1) throw Vertex.Features.propertyKeyReferencesMultipleProperties(key);
            else return list.get(0);
        } else if (State.CENTRIC == this.state) {
            if (this.vertexMemory.isComputeKey(key))
                return this.vertexMemory.getProperty(this, key);
            else {
                final List<Property<V, Vertex>> list = (List) this.properties.getOrDefault(key, Collections.EMPTY_LIST);
                if (list.size() == 0) return Property.empty();
                else if (list.size() > 1) throw Vertex.Features.propertyKeyReferencesMultipleProperties(key);
                else return list.get(0);
            }
        } else {
            throw GraphComputer.Features.adjacentVertexPropertiesCanNotBeRead();
        }
    }

    public <T> Property<T, Vertex> setProperty(final String key, final T value) {
        if (State.STANDARD == this.state) {
            ThingHelper.validateProperty(this, key, value);
            final Property<T, Vertex> property = new TinkerProperty<T, Vertex>(key, value, this);
            this.properties.put(key, (List) Arrays.asList(property));
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

    protected void removeProperty(final String key) {
        if (State.STANDARD == this.state) {
            this.properties.remove(key).stream().forEach(p -> this.graph.vertexIndex.autoRemove(key, p.getValue(), this));
        } else if (State.CENTRIC == this.state) {
            if (this.vertexMemory.isComputeKey(key))
                this.vertexMemory.removeProperty(this, key);
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
