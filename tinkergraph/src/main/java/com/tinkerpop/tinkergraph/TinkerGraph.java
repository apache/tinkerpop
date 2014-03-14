package com.tinkerpop.tinkergraph;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Transaction;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.MemoryHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.tinkergraph.process.graph.map.TinkerGraphStep;
import com.tinkerpop.tinkergraph.process.graph.util.optimizers.TinkerGraphStepOptimizer;
import org.apache.commons.configuration.Configuration;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
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
    protected TinkerGraphMemory graphMemory = new TinkerGraphMemory(this);
    protected TinkerElementMemory elementMemory;

    protected boolean usesElementMemory = false;

    protected TinkerIndex<TinkerVertex> vertexIndex = new TinkerIndex<>(this, TinkerVertex.class);
    protected TinkerIndex<TinkerEdge> edgeIndex = new TinkerIndex<>(this, TinkerEdge.class);

    /**
     * An empty private constructor that initializes {@link TinkerGraph} with no {@link com.tinkerpop.gremlin.structure.strategy.GraphStrategy}.  Primarily
     * used for purposes of serialization issues.
     */
    private TinkerGraph() {
    }

    /**
     * Open a new {@link TinkerGraph} instance.
     * <p/>
     * <b>Reference Implementation Help:</b> If a {@link com.tinkerpop.gremlin.structure.Graph } implementation does not require a
     * {@link org.apache.commons.configuration.Configuration} (or perhaps has a default configuration) it can choose to implement a zero argument
     * open() method. This is an optional constructor method for TinkerGraph. It is not enforced by the Blueprints
     * Test Suite.
     */
    public static TinkerGraph open() {
        return open(Optional.empty());
    }

    /**
     * Open a new {@link TinkerGraph} instance.
     * <p/>
     * <b>Reference Implementation Help:</b> This method is the one use by the
     * {@link com.tinkerpop.gremlin.structure.util.GraphFactory} to instantiate {@link com.tinkerpop.gremlin.structure.Graph} instances.  This method must
     * be overridden for the Blueprint Test Suite to pass.
     *
     * @param configuration the configuration for the instance
     * @param <G>           the {@link com.tinkerpop.gremlin.structure.Graph} instance
     * @return a newly opened {@link com.tinkerpop.gremlin.structure.Graph}
     */
    public static <G extends Graph> G open(final Optional<Configuration> configuration) {
        return (G) new TinkerGraph();
    }

    ////////////// BLUEPRINTS API METHODS //////////////////

    public Vertex v(final Object id) {
        final Vertex vertex = this.vertices.get(id.toString());
        if (null == vertex)
            throw new NoSuchElementException();
        else
            return vertex;
    }

    public Edge e(final Object id) {
        final Edge edge = this.edges.get(id.toString());
        if (null == edge)
            throw new NoSuchElementException();
        else
            return edge;
    }

    public GraphTraversal<Vertex, Vertex> V() {
        final GraphTraversal traversal = new DefaultGraphTraversal<Object, Vertex>() {
            public GraphTraversal submit(final TraversalEngine engine) {
                if (engine instanceof GraphComputer)
                    this.optimizers().unregister(TinkerGraphStepOptimizer.class);
                return super.submit(engine);
            }
        };
        traversal.memory().set(Traversal.Memory.Variable.hidden("g"), this);    // TODO: is this good?
        traversal.optimizers().register(new TinkerGraphStepOptimizer());
        traversal.addStep(new TinkerGraphStep(traversal, Vertex.class, this));
        return traversal;
    }

    public GraphTraversal<Edge, Edge> E() {
        final GraphTraversal traversal = new DefaultGraphTraversal<Object, Edge>() {
            public GraphTraversal submit(final TraversalEngine engine) {
                if (engine instanceof GraphComputer)
                    this.optimizers().unregister(TinkerGraphStepOptimizer.class);
                return super.submit(engine);
            }
        };
        traversal.optimizers().register(new TinkerGraphStepOptimizer());
        traversal.addStep(new TinkerGraphStep(traversal, Edge.class, this));
        return traversal;
    }

    public Vertex addVertex(final Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        Object idString = ElementHelper.getIdValue(keyValues).orElse(null);
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Element.DEFAULT_LABEL);

        if (null != idString) {
            if (this.vertices.containsKey(idString.toString()))
                throw Exceptions.vertexWithIdAlreadyExists(idString);
        } else {
            idString = TinkerHelper.getNextId(this);
        }

        final Vertex vertex = new TinkerVertex(idString.toString(), label, this);
        this.vertices.put(vertex.getId().toString(), vertex);
        ElementHelper.attachProperties(vertex, keyValues);
        return vertex;
    }

    public GraphComputer compute() {
        return new TinkerGraphComputer(this);
    }

    public <M extends Memory> M memory() {
        return (M) this.graphMemory;
    }

    public String toString() {
        return StringFactory.graphString(this, "vertices:" + this.vertices.size() + " edges:" + this.edges.size());
    }

    public void clear() {
        this.vertices.clear();
        this.edges.clear();
        this.graphMemory = new TinkerGraphMemory(this);
        this.currentId = 0l;
        this.vertexIndex = new TinkerIndex<>(this, TinkerVertex.class);
        this.edgeIndex = new TinkerIndex<>(this, TinkerEdge.class);
    }

    public void close() {

    }

    public Transaction tx() {
        throw Exceptions.transactionsNotSupported();
    }


    public Features getFeatures() {
        return new TinkerGraphFeatures();
    }

    public static class TinkerGraphFeatures implements Features {
        @Override
        public GraphFeatures graph() {
            return new GraphFeatures() {
                @Override
                public boolean supportsTransactions() {
                    return false;
                }

                @Override
                public boolean supportsPersistence() {
                    // todo: temporary.........
                    return false;
                }

                @Override
                public boolean supportsThreadedTransactions() {
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
