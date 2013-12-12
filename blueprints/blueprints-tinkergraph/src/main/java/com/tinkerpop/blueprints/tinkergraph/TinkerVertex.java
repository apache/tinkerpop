package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.computer.GraphComputer;
import com.tinkerpop.blueprints.query.VertexQuery;
import com.tinkerpop.blueprints.util.ElementHelper;
import com.tinkerpop.blueprints.util.StringFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class TinkerVertex extends TinkerElement implements Vertex, Serializable {

    protected Map<String, List<Vertex.Property>> properties = new HashMap<>();

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

    public <V> Vertex.Property<V> addProperty(final String key, final V value) {
        final Vertex.Property<V> property = new Property<>(this, key, value);
        final List<Vertex.Property> list = this.properties.getOrDefault(key, new ArrayList<>());
        list.add(property);
        this.properties.put(key, list);
        return property;
    }

    public Set<String> getPropertyKeys() {
        return this.properties.keySet();
    }

    public <V> Iterable<Vertex.Property<V>> getProperties(final String key) {
        return (Iterable) this.properties.getOrDefault(key, Collections.EMPTY_LIST);
    }

    public Map<String, Iterable<Vertex.Property>> getProperties() {
        return (Map) new HashMap<>(this.properties);
    }

    public <V> Vertex.Property<V> getProperty(final String key) {
        if (State.STANDARD == this.state) {
            final List<Vertex.Property<V>> list = (List) this.properties.getOrDefault(key, Collections.EMPTY_LIST);
            if (list.size() == 0) return Vertex.Property.empty();
            else if (list.size() > 1) throw Vertex.Features.propertyKeyReferencesMultipleProperties(key);
            else return list.get(0);
        } else if (State.CENTRIC == this.state) {
            if (this.vertexMemory.isComputeKey(key))
                return this.vertexMemory.getProperty(this, key);
            else {
                final List<Vertex.Property<V>> list = (List) this.properties.getOrDefault(key, Collections.EMPTY_LIST);
                if (list.size() == 0) return Vertex.Property.empty();
                else if (list.size() > 1) throw Vertex.Features.propertyKeyReferencesMultipleProperties(key);
                else return list.get(0);
            }
        } else {
            throw GraphComputer.Features.adjacentVertexPropertiesCanNotBeRead();
        }
    }

    public <V> Vertex.Property<V> setProperty(final String key, final V value) {
        if (State.STANDARD == this.state) {
            ElementHelper.validateProperty(key, value);
            final Vertex.Property<V> property = new Property<>(this, key, value);
            final List<Vertex.Property> list = new ArrayList<>();
            list.add(property);
            this.properties.put(key, list);
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

    protected void removeProperty(final Vertex.Property property) {
        if (State.STANDARD == this.state) {
            new ArrayList<>(this.properties.get(property.getKey())).forEach(p -> this.properties.remove(p.getKey()));
            this.graph.vertexIndex.autoRemove(property.getKey(), property.getValue(), this);
        } else if (State.CENTRIC == this.state) {
            if (this.vertexMemory.isComputeKey(property.getKey()))
                this.vertexMemory.getProperty(this, property.getKey()).remove();
            else
                throw GraphComputer.Features.providedKeyIsNotAComputeKey(property.getKey());
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

    public Edge addEdge(final String label, final Vertex vertex, final Object... keyValues) {
        return TinkerHelper.addEdge(this.graph, this, (TinkerVertex) vertex, label, keyValues);
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

    public class Property<V> implements Vertex.Property<V> {

        private Map<String, com.tinkerpop.blueprints.Property> properties = new HashMap<>();
        private final TinkerVertex vertex;
        private final String key;
        private final V value;

        public Property(final TinkerVertex vertex, final String key, final V value) {
            this.vertex = vertex;
            this.key = key;
            this.value = value;

        }

        public boolean isPresent() {
            return null != this.value;
        }

        public Vertex getVertex() {
            return this.vertex;
        }

        public String getKey() {
            return this.key;
        }

        public V getValue() {
            return this.value;
        }

        public void remove() {
            this.vertex.removeProperty(this);
        }

        public Set<String> getPropertyKeys() {
            return this.properties.keySet();
        }

        public Map<String, com.tinkerpop.blueprints.Property> getProperties() {
            return new HashMap<>(this.properties);
        }

        public <V2> com.tinkerpop.blueprints.Property<V2> getProperty(final String key) {
            return this.properties.get(key);
        }

        public <V2> com.tinkerpop.blueprints.Property<V2> setProperty(final String key, final V2 value) {
            final com.tinkerpop.blueprints.Property<V2> property = new com.tinkerpop.blueprints.Property<V2>() {
                public boolean isPresent() {
                    return null != value;
                }

                public void remove() {
                    properties.remove(key);
                }

                public V2 getValue() {
                    if (!isPresent())
                        throw Property.Features.propertyDoesNotExist();
                    return value;
                }

                public String getKey() {
                    if (!isPresent())
                        throw Property.Features.propertyDoesNotExist();
                    return key;
                }


            };
            this.properties.put(key, property);
            return property;
        }
    }
}
