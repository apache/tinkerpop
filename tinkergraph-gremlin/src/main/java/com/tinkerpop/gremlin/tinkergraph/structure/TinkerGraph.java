package com.tinkerpop.gremlin.tinkergraph.structure;

import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.util.GraphComputerHelper;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Transaction;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.tinkergraph.process.computer.TinkerGraphComputer;
import com.tinkerpop.gremlin.tinkergraph.process.computer.TinkerGraphView;
import com.tinkerpop.gremlin.tinkergraph.process.graph.strategy.TinkerElementStepStrategy;
import com.tinkerpop.gremlin.tinkergraph.process.graph.strategy.TinkerGraphStepStrategy;
import com.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * An in-sideEffects, reference implementation of the property graph interfaces provided by Gremlin3.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_PERFORMANCE)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_COMPUTER)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_PROCESS_COMPUTER)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_ENVIRONMENT)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_ENVIRONMENT_INTEGRATE)
public class TinkerGraph implements Graph, Graph.Iterators {

    static {
        try {
            TraversalStrategies.GlobalCache.registerStrategies(TinkerGraph.class, TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone().addStrategies(TinkerGraphStepStrategy.instance()));
            TraversalStrategies.GlobalCache.registerStrategies(TinkerVertex.class, TraversalStrategies.GlobalCache.getStrategies(Vertex.class).clone().addStrategies(TinkerElementStepStrategy.instance()));
            TraversalStrategies.GlobalCache.registerStrategies(TinkerEdge.class, TraversalStrategies.GlobalCache.getStrategies(Edge.class).clone().addStrategies(TinkerElementStepStrategy.instance()));
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    private static final Configuration EMPTY_CONFIGURATION = new BaseConfiguration() {{
        this.setProperty(Graph.GRAPH, TinkerGraph.class.getName());
    }};

    protected Long currentId = -1l;
    protected Map<Object, Vertex> vertices = new HashMap<>();
    protected Map<Object, Edge> edges = new HashMap<>();
    protected TinkerGraphVariables variables = new TinkerGraphVariables();
    protected TinkerGraphView graphView = null;

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
     * open() method. This is an optional constructor method for TinkerGraph. It is not enforced by the Gremlin
     * Test Suite.
     */
    public static TinkerGraph open() {
        return open(null);
    }

    /**
     * Open a new {@link TinkerGraph} instance.
     * <p/>
     * <b>Reference Implementation Help:</b> This method is the one use by the
     * {@link com.tinkerpop.gremlin.structure.util.GraphFactory} to instantiate
     * {@link com.tinkerpop.gremlin.structure.Graph} instances.  This method must be overridden for the Blueprint Test
     * Suite to pass. Implementers have latitude in terms of how exceptions are handled within this method.  Such
     * exceptions will be considered implementation specific by the test suite as all test generate graph instances
     * by way of {@link com.tinkerpop.gremlin.structure.util.GraphFactory}. As such, the exceptions get generalized
     * behind that facade and since {@link com.tinkerpop.gremlin.structure.util.GraphFactory} is the preferred method
     * to opening graphs it will be consistent at that level.
     *
     * @param configuration the configuration for the instance
     * @return a newly opened {@link com.tinkerpop.gremlin.structure.Graph}
     */
    public static TinkerGraph open(final Configuration configuration) {
        return new TinkerGraph();
    }

    ////////////// STRUCTURE API METHODS //////////////////

    @Override
    public Vertex addVertex(final Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);

        if (null != idValue) {
            if (this.vertices.containsKey(idValue))
                throw Exceptions.vertexWithIdAlreadyExists(idValue);
        } else {
            idValue = TinkerHelper.getNextId(this);
        }

        final Vertex vertex = new TinkerVertex(idValue, label, this);
        this.vertices.put(vertex.id(), vertex);
        ElementHelper.attachProperties(vertex, keyValues);
        return vertex;
    }

    @Override
    public GraphComputer compute(final Class... graphComputerClass) {
        GraphComputerHelper.validateComputeArguments(graphComputerClass);
        if (graphComputerClass.length == 0 || graphComputerClass[0].equals(TinkerGraphComputer.class))
            return new TinkerGraphComputer(this);
        else
            throw Graph.Exceptions.graphDoesNotSupportProvidedGraphComputer(graphComputerClass[0]);
    }


    @Override
    public Variables variables() {
        return this.variables;
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, "vertices:" + this.vertices.size() + " edges:" + this.edges.size());
    }

    public void clear() {
        this.vertices.clear();
        this.edges.clear();
        this.variables = new TinkerGraphVariables();
        this.currentId = 0l;
        this.vertexIndex = new TinkerIndex<>(this, TinkerVertex.class);
        this.edgeIndex = new TinkerIndex<>(this, TinkerEdge.class);
    }

    @Override
    public void close() {

    }

    @Override
    public Transaction tx() {
        throw Exceptions.transactionsNotSupported();
    }

    @Override
    public Configuration configuration() {
        return EMPTY_CONFIGURATION;
    }

    @Override
    public Iterators iterators() {
        return this;
    }

    @Override
    public Iterator<Vertex> vertexIterator(final Object... vertexIds) {
        if (0 == vertexIds.length) {
            return this.vertices.values().iterator();
        } else if (1 == vertexIds.length) {
            final Vertex vertex = this.vertices.get(vertexIds[0]);
            return null == vertex ? Collections.emptyIterator() : IteratorUtils.of(vertex);
        } else
            return Stream.of(vertexIds).filter(this.vertices::containsKey).map(this.vertices::get).iterator();
    }

    @Override
    public Iterator<Edge> edgeIterator(final Object... edgeIds) {
        if (0 == edgeIds.length) {
            return this.edges.values().iterator();
        } else if (1 == edgeIds.length) {
            final Edge edge = this.edges.get(edgeIds[0]);
            return null == edge ? Collections.emptyIterator() : IteratorUtils.of(edge);
        } else
            return Stream.of(edgeIds).filter(this.edges::containsKey).map(this.edges::get).iterator();
    }

    /**
     * Return TinkerGraph feature set.
     * <p/>
     * <b>Reference Implementation Help:</b> Implementers only need to implement features for which there are
     * negative or instance configured features.  By default, all {@link Features} return true.
     */
    @Override
    public Features features() {
        return new TinkerGraphFeatures();
    }

    public static class TinkerGraphFeatures implements Features {
        @Override
        public GraphFeatures graph() {
            return new TinkerGraphGraphFeatures();
        }

        @Override
        public EdgeFeatures edge() {
            return new TinkerGraphEdgeFeatures();
        }

        @Override
        public VertexFeatures vertex() {
            return new TinkerGraphVertexFeatures();
        }

        @Override
        public String toString() {
            return StringFactory.featureString(this);
        }
    }

    public static class TinkerGraphVertexFeatures implements Features.VertexFeatures {
        @Override
        public boolean supportsCustomIds() {
            return false;
        }
    }

    public static class TinkerGraphEdgeFeatures implements Features.EdgeFeatures {
        @Override
        public boolean supportsCustomIds() {
            return false;
        }
    }

    public static class TinkerGraphGraphFeatures implements Features.GraphFeatures {
        @Override
        public boolean supportsTransactions() {
            return false;
        }

        @Override
        public boolean supportsPersistence() {
            return false;
        }

        @Override
        public boolean supportsThreadedTransactions() {
            return false;
        }
    }

    ///////////// GRAPH SPECIFIC INDEXING METHODS ///////////////

    /**
     * Create an index for said element class ({@link Vertex} or {@link Edge}) and said property key.
     * Whenever an element has the specified key mutated, the index is updated.
     * When the index is created, all existing elements are indexed to ensure that they are captured by the index.
     *
     * @param key          the property key to index
     * @param elementClass the element class to index
     * @param <E>          The type of the element class
     */
    public <E extends Element> void createIndex(final String key, final Class<E> elementClass) {
        if (Vertex.class.isAssignableFrom(elementClass)) {
            this.vertexIndex.createKeyIndex(key);
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            this.edgeIndex.createKeyIndex(key);
        } else {
            throw new IllegalArgumentException("Class is not indexable: " + elementClass);
        }
    }

    /**
     * Drop the index for the specified element class ({@link Vertex} or {@link Edge}) and key.
     *
     * @param key          the property key to stop indexing
     * @param elementClass the element class of the index to drop
     * @param <E>          The type of the element class
     */
    public <E extends Element> void dropIndex(final String key, final Class<E> elementClass) {
        if (Vertex.class.isAssignableFrom(elementClass)) {
            this.vertexIndex.dropKeyIndex(key);
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            this.edgeIndex.dropKeyIndex(key);
        } else {
            throw new IllegalArgumentException("Class is not indexable: " + elementClass);
        }
    }

    /**
     * Return all the keys currently being index for said element class  ({@link Vertex} or {@link Edge}).
     *
     * @param elementClass the element class to get the indexed keys for
     * @param <E>          The type of the element class
     * @return the set of keys currently being indexed
     */
    public <E extends Element> Set<String> getIndexedKeys(final Class<E> elementClass) {
        if (Vertex.class.isAssignableFrom(elementClass)) {
            return this.vertexIndex.getIndexedKeys();
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            return this.edgeIndex.getIndexedKeys();
        } else {
            throw new IllegalArgumentException("Class is not indexable: " + elementClass);
        }
    }

    /**
     * {@link TinkerGraphComputer} generates a view of the original graph. When the view is no longer needed, it can be dropped.
     */
    public void dropGraphView() {
        this.graphView = null;
    }
}
