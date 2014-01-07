package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Strategy;
import com.tinkerpop.blueprints.Transaction;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.computer.GraphComputer;
import com.tinkerpop.blueprints.query.GraphQuery;
import com.tinkerpop.blueprints.strategy.GraphStrategy;
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
    protected Map<String, Object> annotations = new HashMap<>();

    protected TinkerIndex<TinkerVertex> vertexIndex = new TinkerIndex<>(this, TinkerVertex.class);
    protected TinkerIndex<TinkerEdge> edgeIndex = new TinkerIndex<>(this, TinkerEdge.class);

    protected final Strategy strategy = new Strategy.Simple();

    /**
     * All Graph implementations are to be constructed through the open() method and therefore Graph implementations
     * should maintain a private or protected constructor.  This rule is enforced by the Blueprints Test suite.
     */
    private TinkerGraph(final Optional<GraphStrategy> strategy) {
        this.strategy.set(strategy);
    }

    /**
     * If a Graph implementation does not require a configuration (or perhaps has a default configuration) it can
     * choose to implement a zero argument open() method.  This is not a requirement and is not enforced by the
     * Blueprints test suite.
     */
    public static TinkerGraph open() {
        return open(Optional.empty(), Optional.empty());
    }

    /**
     * This is an optional constructor for TinkerGraph.  It is not enforced by the Blueprints Test Suite.
     */
    public static TinkerGraph open(final Optional<Configuration> configuration) {
        return open(configuration, Optional.empty());
    }

    /**
     * All graphs require that this method be overridden from the Graph interface.  It is enforced by the Blueprints
     * test suite.
     */
    public static <G extends Graph> G open(final Optional<Configuration> configuration, final Optional<GraphStrategy> strategy) {
        return (G) new TinkerGraph(strategy);
    }

    ////////////// BLUEPRINTS API METHODS //////////////////

    public Vertex addVertex(final Object... keyValues) {
        // apply the PreAddVertex strategy if present. apply strategies prior to the parameter validation in case
        // the strategy does something good (or bad) to the keyValues that the implementation does not like.
        //final Object[] strategizedKeyValues = strategy.ifPresent(s->s.getPreAddVertex().apply(keyValues), keyValues);

        return strategy.ifPresent(s->s.getWrapAddVertex().apply(this::internalAddVertex), this::internalAddVertex).apply(keyValues);

        // apply the PostAddVertex strategy if present
        //return strategy.ifPresent(s->s.getPostAddVertex().apply(vertex), vertex);
    }

    protected Vertex internalAddVertex(final Object... keyValues) {
        Objects.requireNonNull(keyValues);
        Object idString = ElementHelper.getIdValue(keyValues).orElse(null);
        final String label = ElementHelper.getLabelValue(keyValues).orElse(null);

        if (null != idString) {
            if (this.vertices.containsKey(idString.toString()))
                throw Exceptions.vertexWithIdAlreadyExists(idString);
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

    public Strategy strategy() {
        return this.strategy;
    }

    public <V> void setAnnotation(final String key, final V value) {
        this.annotations.put(key, value);
    }

    public <V> Optional<V> getAnnotation(final String key) {
        return Optional.ofNullable((V) this.annotations.get(key));
    }

    public String toString() {
        return StringFactory.graphString(this, "vertices:" + this.vertices.size() + " edges:" + this.edges.size());
    }

    public void clear() {
        this.vertices.clear();
        this.edges.clear();
        this.annotations.clear();
        this.currentId = 0l;
        this.vertexIndex = new TinkerIndex<>(this, TinkerVertex.class);
        this.edgeIndex = new TinkerIndex<>(this, TinkerEdge.class);
    }

    public void close() {

    }

    public Transaction tx() {
        throw Graph.Exceptions.transactionsNotSupported();
    }


    public Features getFeatures() {
        return new TinkerGraphFeatures();
    }

    public static class TinkerGraphFeatures implements Graph.Features {
        @Override
        public GraphFeatures graph() {
            return new GraphFeatures() {
                @Override
                public boolean supportsTransactions() {
                    return false;
                }
            };
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
