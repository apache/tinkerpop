package com.tinkerpop.gremlin.structure.util.batch;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.AnnotatedValue;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Transaction;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.batch.cache.VertexCache;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * BatchGraph is a wrapper that enables batch loading of a large number of edges and vertices by chunking the entire
 * load into smaller batches and maintaining a memory-efficient vertex cache so that the entire transactional state can
 * be flushed after each chunk is loaded.
 * <br />
 * BatchGraph is ONLY meant for loading data and does not support any retrieval or removal operations.
 * That is, BatchGraph only supports the following methods:
 * - {@link #addVertex(Object...)} for adding vertices
 * - {@link Vertex#addEdge(String, com.tinkerpop.gremlin.structure.Vertex, Object...)} for adding edges
 * - {@link #v(Object)} to be used when adding edges
 * - Property getter, setter and removal methods for vertices and edges.
 * <br />
 * An important limitation of BatchGraph is that edge properties can only be set immediately after the edge has been added.
 * If other vertices or edges have been created in the meantime, setting, getting or removing properties will throw
 * exceptions. This is done to avoid caching of edges which would require a great amount of memory.
 * <br />
 * BatchGraph can also automatically set the provided element ids as properties on the respective element. Use
 * {@link Builder#vertexIdKey(java.util.Optional)} and {@link Builder#edgeIdKey(java.util.Optional)} to set the keys
 * for the vertex and edge properties respectively. This allows to make the loaded baseGraph compatible for later
 * operation with {@link com.tinkerpop.gremlin.structure.strategy.IdGraphStrategy}.
 *
 * @author Matthias Broecheler (http://www.matthiasb.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class BatchGraph<T extends Graph> implements Graph {
    /**
     * Default buffer size
     */
    public static final long DEFAULT_BUFFER_SIZE = 100000;

    private final T baseGraph;

    private final Optional<String> vertexIdKey;
    private final Optional<String> edgeIdKey;
    private final boolean loadingFromScratch;
    private final boolean baseSupportsSuppliedVertexId;
    private final boolean baseSupportsSuppliedEdgeId;

    private final VertexCache cache;

    private final long bufferSize;
    private long remainingBufferSize;

    private BatchEdge currentEdge = null;
    private Edge currentEdgeCached = null;

    private Object previousOutVertexId = null;

    private final BatchFeatures batchFeatures;

    private final Transaction batchTransaction;

    /**
     * Constructs a BatchGraph wrapping the provided baseGraph, using the specified buffer size and expecting vertex
     * ids of the specified IdType. Supplying vertex ids which do not match this type will throw exceptions.
     *
     * @param graph      Graph to be wrapped
     * @param type       Type of vertex id expected. This information is used to optimize the vertex cache
     *                   memory footprint.
     * @param bufferSize Defines the number of vertices and edges loaded before starting a new transaction. The
     *                   larger this value, the more memory is required but the faster the loading process.
     */
    private BatchGraph(final T graph, final VertexIdType type, final long bufferSize, final Optional<String> vertexIdKey,
                       final Optional<String> edgeIdKey, final  boolean loadingFromScratch) {
        this.baseGraph = graph;
        this.batchTransaction = new BatchTransaction();
        this.batchFeatures = new BatchFeatures(graph.getFeatures());
        this.bufferSize = bufferSize;
        this.cache = type.getVertexCache();
        this.remainingBufferSize = this.bufferSize;
        this.vertexIdKey = vertexIdKey;
        this.edgeIdKey = edgeIdKey;
        this.loadingFromScratch = loadingFromScratch;
        this.baseSupportsSuppliedEdgeId = this.baseGraph.getFeatures().edge().supportsUserSuppliedIds();
        this.baseSupportsSuppliedVertexId = this.baseGraph.getFeatures().vertex().supportsUserSuppliedIds();
    }

    private void nextElement() {
        currentEdge = null;
        currentEdgeCached = null;
        if (remainingBufferSize <= 0) {
            if (baseGraph.getFeatures().graph().supportsTransactions()) baseGraph.tx().commit();
            cache.newTransaction();
            remainingBufferSize = bufferSize;
        }
        remainingBufferSize--;
    }

    private Vertex retrieveFromCache(final Object externalID) {
        final Object internal = cache.getEntry(externalID);
        if (internal instanceof Vertex) {
            return (Vertex) internal;
        } else if (internal != null) { //its an internal id
            final Vertex v = baseGraph.v(internal);
            cache.set(v, externalID);
            return v;
        } else return null;
    }

    private Vertex getCachedVertex(final Object externalID) {
        final Vertex v = retrieveFromCache(externalID);
        if (v == null) throw new IllegalArgumentException("Vertex for given ID cannot be found: " + externalID);
        return v;
    }

    @Override
    public Vertex addVertex(final Object... keyValues) {
        final Object id = ElementHelper.getIdValue(keyValues).orElse(null);
        if (null == id) throw new IllegalArgumentException("Vertex id value cannot be null");
        if (retrieveFromCache(id) != null) throw new IllegalArgumentException("Vertex id already exists");
        nextElement();

        final Optional<Object[]> kvs = this.baseSupportsSuppliedVertexId ?
                Optional.ofNullable(keyValues) : ElementHelper.remove(Element.ID, keyValues);
        final Vertex v = kvs.isPresent() ? baseGraph.addVertex(kvs.get()) : baseGraph.addVertex();

        vertexIdKey.ifPresent(k -> v.setProperty(k, id));
        cache.set(v, id);

        return new BatchVertex(id);
    }

    /**
     * {@inheritDoc}
     * <br/>
     * If the input data are sorted, then out vertex will be repeated for several edges in a row.
     * In this case, bypass cache and instead immediately return a new vertex using the known id.
     * This gives a modest performance boost, especially when the cache is large or there are
     * on average many edges per vertex.
     */
    @Override
    public Vertex v(final Object id) {
        if ((previousOutVertexId != null) && (previousOutVertexId.equals(id)))
            return new BatchVertex(previousOutVertexId);
        else {
            Vertex v = retrieveFromCache(id);
            if (null == v) {
                if (loadingFromScratch) return null;
                else {
                    if (!this.baseSupportsSuppliedVertexId) {
                        assert vertexIdKey.isPresent();
                        final Iterator<Vertex> iter = baseGraph.V().has(vertexIdKey.get(), id);
                        if (!iter.hasNext()) return null;
                        v = iter.next();
                        if (iter.hasNext())
                            throw new IllegalArgumentException("There are multiple vertices with the provided id in the database: " + id);
                    } else {
                        v = baseGraph.v(id);
                        if (null == v) return null;
                    }
                    cache.set(v, id);
                }
            }
            return new BatchVertex(id);
        }
    }

    @Override
    public Edge e(final Object id) {
        throw retrievalNotSupported();
    }

    @Override
    public GraphTraversal<Vertex, Vertex> V() {
        throw retrievalNotSupported();
    }

    @Override
    public GraphTraversal<Edge, Edge> E() {
        throw retrievalNotSupported();
    }

    @Override
    public <T extends Traversal> T traversal(final Class<T> traversalClass) {
        throw retrievalNotSupported();
    }

    @Override
    public GraphComputer compute() {
        throw Exceptions.graphComputerNotSupported();
    }

    @Override
    public Transaction tx() {
        return this.batchTransaction;
    }

    @Override
    public <M extends Memory> M memory() {
        throw Exceptions.memoryNotSupported();
    }

    @Override
    public Features getFeatures() {
        return this.batchFeatures;
    }

    @Override
    public void close() throws Exception {
        baseGraph.close();

        // call reset after the close in case the close behavior fails
        reset();
    }

    private void reset() {
        currentEdge = null;
        currentEdgeCached = null;
        remainingBufferSize = 0;
    }

    private class BatchTransaction implements Transaction {
        private final boolean supportsTx;

        public BatchTransaction() {
            supportsTx = baseGraph.getFeatures().graph().supportsTransactions();
        }

        @Override
        public Transaction onClose(final Consumer<Transaction> consumer) {
            throw new UnsupportedOperationException("Transaction behavior cannot be altered in batch mode - set the behavior on the base graph");
        }

        @Override
        public Transaction onReadWrite(final Consumer<Transaction> consumer) {
            throw new UnsupportedOperationException("Transaction behavior cannot be altered in batch mode - set the behavior on the base graph");
        }

        @Override
        public void close() {
            if (supportsTx) baseGraph.tx().close();

            // call reset after the close in case the close behavior fails
            reset();
        }

        @Override
        public void readWrite() {
            if (supportsTx) baseGraph.tx().readWrite();
        }

        @Override
        public boolean isOpen() {
            return !supportsTx || baseGraph.tx().isOpen();
        }

        @Override
        public <G extends Graph> G create() {
            throw new UnsupportedOperationException("Cannot start threaded transaction during batch loading");
        }

        @Override
        public <G extends Graph, R> Workload<G, R> submit(final Function<G, R> work) {
            throw new UnsupportedOperationException("Cannot submit a workload during batch loading");
        }

        @Override
        public void rollback() {
            throw new UnsupportedOperationException("Cannot issue a rollback during batch loading");
        }

        @Override
        public void commit() {
            if (supportsTx) baseGraph.tx().commit();

            // call reset after the close in case the close behavior fails
            reset();
        }

        @Override
        public void open() {
            if (supportsTx) baseGraph.tx().open();
        }
    }

    private class BatchVertex implements Vertex {

        private final Object externalID;

        BatchVertex(final Object id) {
            if (id == null) throw new IllegalArgumentException("External id may not be null");
            externalID = id;
        }

        @Override
        public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
            if (!BatchVertex.class.isInstance(inVertex))
                throw new IllegalArgumentException("Given element was not created in this baseGraph");
            nextElement();

            final Vertex ov = getCachedVertex(externalID);
            final Vertex iv = getCachedVertex(inVertex.getId());

            previousOutVertexId = externalID;  //keep track of the previous out vertex id

            final Optional<Object> id = ElementHelper.getIdValue(keyValues);
            final Optional<Object[]> kvs = baseSupportsSuppliedEdgeId ?
                    Optional.ofNullable(keyValues) : ElementHelper.remove(Element.ID, keyValues);

            currentEdgeCached = kvs.isPresent() ? ov.addEdge(label, iv, kvs.get()) : ov.addEdge(label, iv);

            if (edgeIdKey.isPresent() && id.isPresent())
                currentEdgeCached.setProperty(edgeIdKey.get(), id.get());

            currentEdge = new BatchEdge();

            return currentEdge;
        }

        @Override
        public Object getId() {
            return this.externalID;
        }

        @Override
        public String getLabel() {
            return getCachedVertex(externalID).getLabel();
        }

        @Override
        public void remove() {
            throw removalNotSupported();
        }

        @Override
        public Set<String> getPropertyKeys() {
            return getCachedVertex(externalID).getPropertyKeys();
        }

        @Override
        public Map<String, Property> getProperties() {
            return getCachedVertex(externalID).getProperties();
        }

        @Override
        public <V> Property<V> getProperty(final String key) {
            return getCachedVertex(externalID).getProperty(key);
        }

        @Override
        public <V> void setProperty(final String key, final V value) {
            getCachedVertex(externalID).setProperty(key, value);
        }

        @Override
        public void setProperties(final Object... keyValues) {
            getCachedVertex(externalID).setProperties(keyValues);
        }

        @Override
        public <V> V getValue(final String key) throws NoSuchElementException {
            return getCachedVertex(externalID).getValue(key);
        }

        @Override
        public <E2> GraphTraversal<Vertex, AnnotatedValue<E2>> annotatedValues(final String propertyKey) {
            throw retrievalNotSupported();
        }

        @Override
        public <E2> GraphTraversal<Vertex, Property<E2>> property(final String propertyKey) {
            throw retrievalNotSupported();
        }

        @Override
        public <E2> GraphTraversal<Vertex, E2> value(final String propertyKey) {
            throw retrievalNotSupported();
        }

        @Override
        public GraphTraversal<Vertex, Vertex> with(final Object... variableValues) {
            throw retrievalNotSupported();
        }

        @Override
        public GraphTraversal<Vertex, Vertex> sideEffect(final Consumer<Holder<Vertex>> consumer) {
            throw retrievalNotSupported();
        }

        @Override
        public GraphTraversal<Vertex, Vertex> out(final int branchFactor, final String... labels) {
            throw retrievalNotSupported();
        }

        @Override
        public GraphTraversal<Vertex, Vertex> in(final int branchFactor, final String... labels) {
            throw retrievalNotSupported();
        }

        @Override
        public GraphTraversal<Vertex, Vertex> both(final int branchFactor, final String... labels) {
            throw retrievalNotSupported();
        }

        @Override
        public GraphTraversal<Vertex, Edge> outE(final int branchFactor, final String... labels) {
            throw retrievalNotSupported();
        }

        @Override
        public GraphTraversal<Vertex, Edge> inE(final int branchFactor, final String... labels) {
            throw retrievalNotSupported();
        }

        @Override
        public GraphTraversal<Vertex, Edge> bothE(final int branchFactor, final String... labels) {
            throw retrievalNotSupported();
        }

        @Override
        public GraphTraversal<Vertex, Vertex> out(final String... labels) {
            throw retrievalNotSupported();
        }

        @Override
        public GraphTraversal<Vertex, Vertex> in(final String... labels) {
            throw retrievalNotSupported();
        }

        @Override
        public GraphTraversal<Vertex, Vertex> both(final String... labels) {
            throw retrievalNotSupported();
        }

        @Override
        public GraphTraversal<Vertex, Edge> outE(final String... labels) {
            throw retrievalNotSupported();
        }

        @Override
        public GraphTraversal<Vertex, Edge> inE(final String... labels) {
            throw retrievalNotSupported();
        }

        @Override
        public GraphTraversal<Vertex, Edge> bothE(final String... labels) {
            throw retrievalNotSupported();
        }

        @Override
        public GraphTraversal<Vertex, Vertex> start() {
            throw retrievalNotSupported();
        }

        @Override
        public GraphTraversal<Vertex, Vertex> as(final String as) {
            throw retrievalNotSupported();
        }

        @Override
        public GraphTraversal<Vertex, Vertex> identity() {
            throw retrievalNotSupported();
        }
    }

    private class BatchEdge implements Edge {

        @Override
        public Vertex getVertex(final Direction direction) throws IllegalArgumentException {
            return getWrappedEdge().getVertex(direction);
        }

        @Override
        public Object getId() {
            return getWrappedEdge().getLabel();
        }

        @Override
        public String getLabel() {
            return getWrappedEdge().getLabel();
        }

        @Override
        public void remove() {
            throw removalNotSupported();
        }

        @Override
        public Map<String, Property> getProperties() {
            return getWrappedEdge().getProperties();
        }

        @Override
        public <V> Property<V> getProperty(final String key) {
            return getWrappedEdge().getProperty(key);
        }

        @Override
        public <V> void setProperty(final String key, final V value) {
            getWrappedEdge().setProperty(key, value);
        }

        @Override
        public Set<String> getPropertyKeys() {
            return getWrappedEdge().getPropertyKeys();
        }

        @Override
        public void setProperties(final Object... keyValues) {
            getWrappedEdge().setProperties(keyValues);
        }

        @Override
        public <V> V getValue(final String key) throws NoSuchElementException {
            return getWrappedEdge().getValue(key);
        }

        private Edge getWrappedEdge() {
            if (this != currentEdge) {
                throw new UnsupportedOperationException("This edge is no longer in scope");
            }
            return currentEdgeCached;
        }
    }

    private static UnsupportedOperationException retrievalNotSupported() {
        return new UnsupportedOperationException("Retrieval operations are not supported during batch loading");
    }

    private static UnsupportedOperationException removalNotSupported() {
        return new UnsupportedOperationException("Removal operations are not supported during batch loading");
    }

    @SuppressWarnings("UnusedDeclaration")
    public static class Builder<T extends Graph> {
        private final T graphToLoad;
        private boolean loadingFromScratch = true;
        private Optional<String> vertexIdKey = Optional.empty();
        private Optional<String> edgeIdKey = Optional.empty();
        private long bufferSize = DEFAULT_BUFFER_SIZE;
        private VertexIdType vertexIdType = VertexIdType.OBJECT;

        public Builder(final T g) {
            if (null == g) throw new IllegalArgumentException("Graph may not be null");
            if (g instanceof BatchGraph) throw new IllegalArgumentException("BatchGraph cannot wrap another BatchGraph instance");
            this.graphToLoad = g;
        }

        /**
         * Sets the key to be used when setting the vertex id as a property on the respective vertex.
         * If the key is null, then no property will be set.
         *
         * @param key Key to be used.
         */
        public Builder vertexIdKey(final Optional<String> key) {
            if (null == key) throw new IllegalArgumentException("Optional value for key cannot be null");
            if (!loadingFromScratch && !key.isPresent() && !graphToLoad.getFeatures().vertex().supportsUserSuppliedIds())
                throw new IllegalStateException("Cannot set vertex id key to null when not loading from scratch while ids are ignored");
            this.vertexIdKey = key;
            return this;
        }

        /**
         * Sets the key to be used when setting the edge id as a property on the respective edge.
         * If the key is null, then no property will be set.
         *
         * @param key Key to be used.
         */
        public Builder edgeIdKey(final Optional<String> key) {
            if (null == key) throw new IllegalArgumentException("Optional value for key cannot be null");
            this.edgeIdKey = key;
            return this;
        }

        public Builder bufferSize(long bufferSize) {
            if (bufferSize <= 0) throw new IllegalArgumentException("BufferSize must be positive");
            this.bufferSize = bufferSize;
            return this;
        }

        public Builder vertexIdType(final VertexIdType type) {
            if (null == type) throw new IllegalArgumentException("Type may not be null");
            this.vertexIdType = type;
            return this;
        }

        /**
         * Sets whether the graph loaded through this instance of {@link BatchGraph} is loaded from scratch
         * (i.e. the wrapped graph is initially empty) or whether graph is loaded incrementally into an
         * existing graph.
         * <p/>
         * In the former case, BatchGraph does not need to check for the existence of vertices with the wrapped
         * graph but only needs to consult its own cache which can be significantly faster. In the latter case,
         * the cache is checked first but an additional check against the wrapped graph may be necessary if
         * the vertex does not exist.
         * <p/>
         * By default, BatchGraph assumes that the data is loaded from scratch.
         * <p/>
         * When setting loading from scratch to false, a vertex id key must be specified first using
         * {@link Builder#vertexIdKey} - otherwise an exception is thrown.
         */
        public Builder loadFromScratch(final boolean fromScratch) {
            if (!fromScratch && !vertexIdKey.isPresent() && !graphToLoad.getFeatures().vertex().supportsUserSuppliedIds())
                throw new IllegalStateException("Vertex id key is required to query existing vertices in wrapped graph.");
            loadingFromScratch = fromScratch;
            return this;
        }

        public BatchGraph<T> build() {
            return new BatchGraph<>(graphToLoad, vertexIdType, bufferSize, vertexIdKey, edgeIdKey, loadingFromScratch);
        }
    }
}
