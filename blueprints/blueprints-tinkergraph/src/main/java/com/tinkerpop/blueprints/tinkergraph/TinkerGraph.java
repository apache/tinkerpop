package com.tinkerpop.blueprints.tinkergraph;


import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Transaction;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.computer.GraphComputer;
import com.tinkerpop.blueprints.query.GraphQuery;
import com.tinkerpop.blueprints.util.ElementHelper;
import com.tinkerpop.blueprints.util.StringFactory;
import org.apache.commons.configuration.Configuration;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * An in-memory, reference implementation of the property graph interfaces provided by Blueprints.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class TinkerGraph implements Graph, Serializable {

    protected Long currentId = -1l;
    protected Map<String, Vertex> vertices = new HashMap<>();
    protected Map<String, Edge> edges = new HashMap<>();
    protected Map<String, Graph.Property> properties = new HashMap<>();

    protected TinkerIndex<TinkerVertex> vertexIndex = new TinkerIndex<>(this, TinkerVertex.class);
    protected TinkerIndex<TinkerEdge> edgeIndex = new TinkerIndex<>(this, TinkerEdge.class);

    /**
     * All Graph implementations are to be constructed through the open() method and therefore Graph implementations
     * should maintain a private or protected constructor.  This rule is enforced by the Blueprints Test suite.
     */
    private TinkerGraph() {
    }

    /**
     * If a Graph implementation does not require a configuration (or perhaps has a default configuration) it can
     * choose to implement a zero argument open() method.  This is not a requirement and is not enforced by the
     * Blueprints test suite.
     */
    public static TinkerGraph open() {
        return open(Optional.empty());
    }

    /**
     * All graphs require that this method be overridden from the Graph interface.  It is enforced by the Blueprints
     * test suite.
     */
    public static <G extends Graph> G open(final Optional<Configuration> configuration) {
        return (G) new TinkerGraph();
    }

    ////////////// BLUEPRINTS API METHODS //////////////////

    public Vertex addVertex(final Object... keyValues) {
        Objects.requireNonNull(properties);
        Object idString = ElementHelper.getIdValue(keyValues);
        String label = ElementHelper.getLabelValue(keyValues);

        if (null != idString) {
            if (this.vertices.containsKey(idString.toString()))
                throw Features.vertexWithIdAlreadyExists(idString);
        } else {
            idString = TinkerHelper.getNextId(this);
        }

        final Vertex vertex = new TinkerVertex(idString.toString(), null == label ? Property.Key.DEFAULT_LABEL.toString() : label, this);
        this.vertices.put(vertex.getId().toString(), vertex);
        ElementHelper.attachKeyValues(vertex, keyValues);
        return vertex;

    }

    public GraphQuery query() {
        return new TinkerGraphQuery(this);
    }

    public GraphComputer compute() {
        return new TinkerGraphComputer(this);
    }

    public Map<String, Graph.Property> getProperties() {
        return new HashMap<>(this.properties);
    }

    public Set<String> getPropertyKeys() {
        return this.properties.keySet();
    }

    public <V> Graph.Property<V> getProperty(final String key) {
        final Graph.Property<V> property = this.properties.get(key);
        return (null == property) ? Graph.Property.empty() : property;
    }

    public <V> Graph.Property<V> setProperty(final String key, final V value) {
        ElementHelper.validateProperty(key, value);
        final Graph.Property<V> property = this.properties.put(key, new Property<>(this, key, value));
        return null == property ? Graph.Property.empty() : property;
    }

    protected void removeProperty(final String key) {
        this.properties.remove(key);
    }

    public String toString() {
        return StringFactory.graphString(this, "vertices:" + this.vertices.size() + " edges:" + this.edges.size());
    }

    public void clear() {
        this.vertices.clear();
        this.edges.clear();
        this.properties.clear();
        this.currentId = 0l;
        this.vertexIndex = new TinkerIndex<>(this, TinkerVertex.class);
        this.edgeIndex = new TinkerIndex<>(this, TinkerEdge.class);
    }

    public void close() {

    }

    public Transaction tx() {
        throw Graph.Features.transactionsNotSupported();
    }


    public Features getFeatures() {
        return new Graph.Features() {
        };
    }

    public class Property<V> implements Graph.Property<V> {
        private final TinkerGraph graph;
        private final String key;
        private final V value;

        public Property(final TinkerGraph graph, final String key, final V value) {
            this.graph = graph;
            this.key = key;
            this.value = value;
        }

        public Graph getGraph() {
            return this.graph;
        }

        public String getKey() {
            return this.key;
        }

        public V getValue() {
            return this.value;
        }

        public void remove() {
            this.graph.properties.remove(key);
        }

        public boolean isPresent() {
            return null != this.value;
        }
    }

    ///////////// GRAPH SPECIFIC INDEXING METHODS ///////////////

    public <E extends Element> void createIndex(final String key, final Class<E> elementClass) {
        if (Vertex.class.isAssignableFrom(elementClass)) {
            this.vertexIndex.createKeyIndex(key);
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            this.edgeIndex.createKeyIndex(key);
        } else {
            throw new IllegalArgumentException("Class is not indexable: " + elementClass);
        }
    }

    public <E extends Element> void dropIndex(final String key, final Class<E> elementClass) {
        if (Vertex.class.isAssignableFrom(elementClass)) {
            this.vertexIndex.dropKeyIndex(key);
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            this.edgeIndex.dropKeyIndex(key);
        } else {
            throw new IllegalArgumentException("Class is not indexable: " + elementClass);
        }
    }

    public <E extends Element> Set<String> getIndexedKeys(final Class<E> elementClass) {
        if (Vertex.class.isAssignableFrom(elementClass)) {
            return this.vertexIndex.getIndexedKeys();
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            return this.edgeIndex.getIndexedKeys();
        } else {
            throw new IllegalArgumentException("Class is not indexable: " + elementClass);
        }
    }
}
