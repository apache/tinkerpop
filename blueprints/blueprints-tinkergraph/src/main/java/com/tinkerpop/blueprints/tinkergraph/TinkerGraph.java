package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Annotations;
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
    protected Annotations annotations = new TinkerAnnotations();

    protected TinkerIndex<TinkerVertex> vertexIndex = new TinkerIndex<>(this, TinkerVertex.class);
    protected TinkerIndex<TinkerEdge> edgeIndex = new TinkerIndex<>(this, TinkerEdge.class);

    protected final Strategy strategy = new Strategy.Simple();
    private final Strategy.Context<Graph> graphContext = new Strategy.Context<Graph>(this, this);

    /**
     * Internally create a new {@link TinkerGraph} instance.
     * <p/>
     * <b>Reference Implementation Help:</b> All Graph implementations are to be constructed through the open() method
     * and therefore {@link Graph} implementations should maintain a private or protected constructor.  This rule is
     * enforced by the Blueprints Test suite.
     */
    private TinkerGraph(final Optional<GraphStrategy> strategy) {
        this.strategy.set(strategy);
    }

    /**
     * Open a new {@link TinkerGraph} instance.
     * <p/>
     * <b>Reference Implementation Help:</b> If a {@link Graph } implementation does not require a
     * {@link Configuration} (or perhaps has a default configuration) it can choose to implement a zero argument
     * open() method. This is an optional constructor method for TinkerGraph. It is not enforced by the Blueprints
     * Test Suite.
     */
    public static TinkerGraph open() {
        return open(Optional.empty(), Optional.empty());
    }

    /**
     * Open a new {@link TinkerGraph} instance.
     * <p/>
     * <b>Reference Implementation Help:</b> This is an optional constructor method for {@link TinkerGraph}. It is not
     * enforced by the Blueprints Test Suite.
     */
    public static TinkerGraph open(final Optional<Configuration> configuration) {
        return open(configuration, Optional.empty());
    }

    /**
     * Open a new {@link TinkerGraph} instance.
     * <p/>
     * <b>Reference Implementation Help:</b> This method is the one use by the
     * {@link com.tinkerpop.blueprints.util.GraphFactory} to instantiate {@link Graph} instances.  This method must
     * be overridden for the Blueprint Test Suite to pass.
     *
     * @param configuration the configuration for the instance
     * @param strategy      a strategy to apply
     * @param <G>           the {@link Graph} instance
     * @return a newly opened {@link Graph}
     */
    public static <G extends Graph> G open(final Optional<Configuration> configuration, final Optional<GraphStrategy> strategy) {
        return (G) new TinkerGraph(strategy);
    }

    ////////////// BLUEPRINTS API METHODS //////////////////

    public Vertex addVertex(final Object... keyValues) {
        // The first argument to compose() gets the GraphStrategy to use and provides it the Context of the addVertex
        // call. The second argument to compose() is the TinkerGraph implementation of addVertex as a lambda where
        // the argument refer to the arguments to addVertex. Note that arguments passes through the GraphStrategy
        // implementations first so at this point the values within them may not be the same as they originally were.
        // The composed function must then be applied with the arguments originally passed to addVertex.
        return strategy.compose(
                s -> s.getAddVertexStrategy(graphContext),
                (kvs) -> {
                    Objects.requireNonNull(kvs);
                    Object idString = ElementHelper.getIdValue(kvs).orElse(null);
                    final String label = ElementHelper.getLabelValue(kvs).orElse(null);

                    if (null != idString) {
                        if (this.vertices.containsKey(idString.toString()))
                            throw Exceptions.vertexWithIdAlreadyExists(idString);
                    } else {
                        idString = TinkerHelper.getNextId(this);
                    }

                    final Vertex vertex = new TinkerVertex(idString.toString(), null == label ? Property.Key.DEFAULT_LABEL.toString() : label, this);
                    this.vertices.put(vertex.getId().toString(), vertex);
                    ElementHelper.attachKeyValues(vertex, kvs);
                    return vertex;
                }).apply(keyValues);
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

    public Annotations annotations() {
        return this.annotations;
    }

    public String toString() {
        return StringFactory.graphString(this, "vertices:" + this.vertices.size() + " edges:" + this.edges.size());
    }

    public void clear() {
        this.vertices.clear();
        this.edges.clear();
        this.annotations = new TinkerAnnotations();
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

                @Override
                public boolean supportsPersistence() {
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
