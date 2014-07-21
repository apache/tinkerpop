package com.tinkerpop.gremlin.neo4j.structure;

import com.tinkerpop.gremlin.neo4j.process.step.map.Neo4jGraphStep;
import com.tinkerpop.gremlin.neo4j.strategy.Neo4jGraphStepTraversalStrategy;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.*;
import com.tinkerpop.gremlin.structure.Transaction;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.structure.util.wrapped.WrappedGraph;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.graphdb.index.AutoIndexer;
import org.neo4j.graphdb.index.RelationshipAutoIndexer;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.core.NodeManager;

import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Pieter Martin
 */
public class Neo4jGraph implements Graph, WrappedGraph<GraphDatabaseService> {
    private GraphDatabaseService baseGraph;

    public static final String CONFIG_DIRECTORY = "gremlin.neo4j.directory";
    private static final String CONFIG_HA = "gremlin.neo4j.ha";
    private static final String CONFIG_CONF = "gremlin.neo4j.conf";

    private final Neo4jTransaction neo4jTransaction = new Neo4jTransaction();

    protected final TransactionManager transactionManager;
    private final ExecutionEngine cypher;

    private Neo4jGraph(final GraphDatabaseService baseGraph) {
        this.baseGraph = baseGraph;
        transactionManager = ((GraphDatabaseAPI) baseGraph).getDependencyResolver().resolveDependency(TransactionManager.class);
        cypher = new ExecutionEngine(baseGraph);
    }

    private Neo4jGraph(final Configuration configuration) {
        try {
            final String directory = configuration.getString(CONFIG_DIRECTORY);
            final Map neo4jSpecificConfig = ConfigurationConverter.getMap(configuration.subset(CONFIG_CONF));
            final boolean ha = configuration.getBoolean(CONFIG_HA, false);

            // if HA is enabled then use the correct factory to instantiate the GraphDatabaseService
            this.baseGraph = ha ?
                    new HighlyAvailableGraphDatabaseFactory().newHighlyAvailableDatabaseBuilder(directory).setConfig(neo4jSpecificConfig).newGraphDatabase() :
                    new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(directory).
                            setConfig(neo4jSpecificConfig).newGraphDatabase();
            transactionManager = ((GraphDatabaseAPI) baseGraph).getDependencyResolver().resolveDependency(TransactionManager.class);
            cypher = new ExecutionEngine(baseGraph);

        } catch (Exception e) {
            if (this.baseGraph != null)
                this.baseGraph.shutdown();
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /**
     * Open a new {@link Neo4jGraph} instance.
     *
     * @param configuration the configuration for the instance
     * @param <G>           the {@link com.tinkerpop.gremlin.structure.Graph} instance
     * @return a newly opened {@link com.tinkerpop.gremlin.structure.Graph}
     */
    public static <G extends Graph> G open(final Configuration configuration) {
        if (null == configuration) throw Graph.Exceptions.argumentCanNotBeNull("configuration");

        if (!configuration.containsKey(CONFIG_DIRECTORY))
            throw new IllegalArgumentException(String.format("Neo4j configuration requires that the %s be set", CONFIG_DIRECTORY));

        return (G) new Neo4jGraph(configuration);
    }

    /**
     * Construct a Neo4jGraph instance by specifying the directory to create the database in..
     */
    public static <G extends Graph> G open(final String directory) {
        final Configuration config = new BaseConfiguration();
        config.setProperty(CONFIG_DIRECTORY, directory);
        return open(config);
    }

    /**
     * Construct a Neo4jGraph instance using an existing Neo4j raw instance.
     */
    public static <G extends Graph> G open(final GraphDatabaseService rawGraph) {
        return (G) new Neo4jGraph(Optional.ofNullable(rawGraph).orElseThrow(() -> Graph.Exceptions.argumentCanNotBeNull("baseGraph")));
    }

    @Override
    public Vertex addVertex(final Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        if (ElementHelper.getIdValue(keyValues).isPresent())
            throw Vertex.Exceptions.userSuppliedIdsNotSupported();

        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);

        this.tx().readWrite();
        final Neo4jVertex vertex = new Neo4jVertex(this.baseGraph.createNode(DynamicLabel.label(label)), this);
        ElementHelper.attachProperties(vertex, keyValues);
        return vertex;
    }

    @Override
    public GraphTraversal<Vertex, Vertex> V() {
        this.tx().readWrite();
        final GraphTraversal traversal = new DefaultGraphTraversal<Object, Vertex>();
        traversal.strategies().register(new Neo4jGraphStepTraversalStrategy());
        traversal.addStep(new Neo4jGraphStep(traversal, Vertex.class, this));
        return traversal;
    }

    @Override
    public GraphTraversal<Edge, Edge> E() {
        this.tx().readWrite();
        final GraphTraversal traversal = new DefaultGraphTraversal<Object, Edge>();
        traversal.strategies().register(new Neo4jGraphStepTraversalStrategy());
        traversal.addStep(new Neo4jGraphStep(traversal, Edge.class, this));
        return traversal;
    }

    @Override
    public Vertex v(final Object id) {
        this.tx().readWrite();
        if (null == id) throw Graph.Exceptions.elementNotFound();

        try {
            return new Neo4jVertex(this.baseGraph.getNodeById(evaluateToLong(id)), this);
        } catch (NotFoundException e) {
            throw Graph.Exceptions.elementNotFound();
        } catch (NumberFormatException e) {
            throw Graph.Exceptions.elementNotFound();
        } catch (NotInTransactionException e) {     // todo: is this right?
            throw Graph.Exceptions.elementNotFound();
        }
    }

    @Override
    public Edge e(final Object id) {
        this.tx().readWrite();
        if (null == id) throw Graph.Exceptions.elementNotFound();

        try {
            return new Neo4jEdge(this.baseGraph.getRelationshipById(evaluateToLong(id)), this);
        } catch (NotFoundException e) {
            throw Graph.Exceptions.elementNotFound();
        } catch (NumberFormatException e) {
            throw Graph.Exceptions.elementNotFound();
        } catch (NotInTransactionException e) {     // todo: is this right?
            throw Graph.Exceptions.elementNotFound();
        }
    }

    @Override
    public <C extends GraphComputer> C compute(final Class<C>... graphComputerClass) {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public Transaction tx() {
        return this.neo4jTransaction;
    }

    @Override
    public <V extends Variables> V variables() {
        throw Graph.Exceptions.variablesNotSupported();
    }

    @Override
    public void close() throws Exception {
        // need to close any dangling transactions
        // todo: does this need to be done across threads to keep shutdown fast???
        this.tx().close();

        if (this.baseGraph != null)
            this.baseGraph.shutdown();
    }

    public String toString() {
        return StringFactory.graphString(this, baseGraph.toString());
    }

    public Features getFeatures() {
        return new Neo4jGraphFeatures();
    }

    public GraphDatabaseService getBaseGraph() {
        return this.baseGraph;
    }

    public Schema getSchema() {
        return this.baseGraph.schema();
    }

    public Iterator<Map<String, Object>> query(final String query, final Map<String, Object> params) {
        this.tx().readWrite();
        return cypher.execute(query, null == params ? Collections.<String, Object>emptyMap() : params).iterator();
    }

    private PropertyContainer getGraphProperties() {
        return ((GraphDatabaseAPI) this.baseGraph).getDependencyResolver().resolveDependency(NodeManager.class).getGraphProperties();
    }

    private static Long evaluateToLong(final Object id) throws NumberFormatException {
        Long longId;
        if (id instanceof Long)
            longId = (Long) id;
        else if (id instanceof Number)
            longId = ((Number) id).longValue();
        else
            longId = Double.valueOf(id.toString()).longValue();
        return longId;
    }

    class Neo4jTransaction implements Transaction {
        private Consumer<Transaction> readWriteConsumer;
        private Consumer<Transaction> closeConsumer;

        protected final ThreadLocal<org.neo4j.graphdb.Transaction> threadLocalTx = new ThreadLocal<org.neo4j.graphdb.Transaction>() {
            protected org.neo4j.graphdb.Transaction initialValue() {
                return null;
            }
        };

        public Neo4jTransaction() {
            // auto transaction behavior
            readWriteConsumer = READ_WRITE_BEHAVIOR.AUTO;

            // commit on close
            closeConsumer = CLOSE_BEHAVIOR.COMMIT;
        }

        @Override
        public void open() {
            if (isOpen())
                throw Transaction.Exceptions.transactionAlreadyOpen();
            else
                threadLocalTx.set(getBaseGraph().beginTx());
        }

        @Override
        public void commit() {
            if (!isOpen())
                return;

            try {
                threadLocalTx.get().success();
            } finally {
                threadLocalTx.get().close();
                threadLocalTx.remove();
            }
        }

        @Override
        public void rollback() {
            if (!isOpen())
                return;

            try {
                javax.transaction.Transaction t = transactionManager.getTransaction();
                if (null == t || t.getStatus() == Status.STATUS_ROLLEDBACK)
                    return;

                threadLocalTx.get().failure();
            } catch (SystemException e) {
                throw new RuntimeException(e); // todo: generalize and make consistent
            } finally {
                threadLocalTx.get().close();
                threadLocalTx.remove();
            }
        }

        @Override
        public <R> Workload<R> submit(final Function<Graph, R> work) {
            return new Workload<>(Neo4jGraph.this, work);
        }

        @Override
        public <G extends Graph> G create() {
            throw Transaction.Exceptions.threadedTransactionsNotSupported();
        }

        @Override
        public boolean isOpen() {
            return (threadLocalTx.get() != null);
        }

        @Override
        public void readWrite() {
            this.readWriteConsumer.accept(this);
        }

        @Override
        public void close() {
            this.closeConsumer.accept(this);
        }

        @Override
        public Transaction onReadWrite(final Consumer<Transaction> consumer) {
            this.readWriteConsumer = Optional.ofNullable(consumer).orElseThrow(Transaction.Exceptions::onReadWriteBehaviorCannotBeNull);
            return this;
        }

        @Override
        public Transaction onClose(final Consumer<Transaction> consumer) {
            this.closeConsumer = Optional.ofNullable(consumer).orElseThrow(Transaction.Exceptions::onCloseBehaviorCannotBeNull);
            return this;
        }
    }

    public static class Neo4jGraphFeatures implements Features {
        @Override
        public GraphFeatures graph() {
            return new GraphFeatures() {
                @Override
                public boolean supportsComputer() {
                    return false;
                }

                @Override
                public VariableFeatures memory() {
                    return new Neo4jVariableFeatures();
                }

                @Override
                public boolean supportsThreadedTransactions() {
                    return false;
                }

                /**
                 * Neo4j does not support transaction consistency across threads when iterating over {@code g.V/E}
                 * in the sense that a vertex added in one thread will appear in the iteration of vertices in a
                 * different thread even prior to transaction commit in the first thread.
                 */
                @Override
                public boolean supportsFullyIsolatedTransactions() {
                    return false;
                }
            };
        }

        @Override
        public VertexFeatures vertex() {
            return new Neo4jVertexFeatures();
        }

        @Override
        public EdgeFeatures edge() {
            return new Neo4jEdgeFeatures();
        }

        @Override
        public String toString() {
            return StringFactory.featureString(this);
        }

        public static class Neo4jVertexFeatures implements VertexFeatures {
            @Override
            public VertexAnnotationFeatures annotations() {
                return new Neo4jVertexAnnotationFeatures();
            }

            @Override
            public boolean supportsUserSuppliedIds() {
                return false;
            }

            @Override
            public VertexPropertyFeatures properties() {
                return new Neo4jVertexPropertyFeatures();
            }
        }

        public static class Neo4jEdgeFeatures implements EdgeFeatures {
            @Override
            public boolean supportsUserSuppliedIds() {
                return false;
            }

            @Override
            public EdgePropertyFeatures properties() {
                return new Neo4jEdgePropertyFeatures();
            }
        }

        public static class Neo4jVertexPropertyFeatures implements VertexPropertyFeatures {
            @Override
            public boolean supportsMapValues() {
                return false;
            }

            @Override
            public boolean supportsMixedListValues() {
                return false;
            }

            @Override
            public boolean supportsSerializableValues() {
                return false;
            }

            @Override
            public boolean supportsUniformListValues() {
                return false;
            }
        }

        public static class Neo4jEdgePropertyFeatures implements EdgePropertyFeatures {
            @Override
            public boolean supportsMapValues() {
                return false;
            }

            @Override
            public boolean supportsMixedListValues() {
                return false;
            }

            @Override
            public boolean supportsSerializableValues() {
                return false;
            }

            @Override
            public boolean supportsUniformListValues() {
                return false;
            }
        }

        public static class Neo4jVariableFeatures implements VariableFeatures {
            @Override
            public boolean supportsBooleanValues() {
                return false;
            }

            @Override
            public boolean supportsDoubleValues() {
                return false;
            }

            @Override
            public boolean supportsFloatValues() {
                return false;
            }

            @Override
            public boolean supportsIntegerValues() {
                return false;
            }

            @Override
            public boolean supportsLongValues() {
                return false;
            }

            @Override
            public boolean supportsMapValues() {
                return false;
            }

            @Override
            public boolean supportsMixedListValues() {
                return false;
            }

            @Override
            public boolean supportsByteValues() {
                return false;
            }

            @Override
            public boolean supportsBooleanArrayValues() {
                return false;
            }

            @Override
            public boolean supportsByteArrayValues() {
                return false;
            }

            @Override
            public boolean supportsDoubleArrayValues() {
                return false;
            }

            @Override
            public boolean supportsFloatArrayValues() {
                return false;
            }

            @Override
            public boolean supportsIntegerArrayValues() {
                return false;
            }

            @Override
            public boolean supportsLongArrayValues() {
                return false;
            }

            @Override
            public boolean supportsStringArrayValues() {
                return false;
            }

            @Override
            public boolean supportsSerializableValues() {
                return false;
            }

            @Override
            public boolean supportsStringValues() {
                return false;
            }

            @Override
            public boolean supportsUniformListValues() {
                return false;
            }
        }

        public static class Neo4jVertexAnnotationFeatures implements VertexAnnotationFeatures {
            @Override
            public boolean supportsBooleanValues() {
                return false;
            }

            @Override
            public boolean supportsDoubleValues() {
                return false;
            }

            @Override
            public boolean supportsFloatValues() {
                return false;
            }

            @Override
            public boolean supportsIntegerValues() {
                return false;
            }

            @Override
            public boolean supportsLongValues() {
                return false;
            }

            @Override
            public boolean supportsMapValues() {
                return false;
            }

            @Override
            public boolean supportsMixedListValues() {
                return false;
            }

            @Override
            public boolean supportsByteValues() {
                return false;
            }

            @Override
            public boolean supportsBooleanArrayValues() {
                return false;
            }

            @Override
            public boolean supportsByteArrayValues() {
                return false;
            }

            @Override
            public boolean supportsDoubleArrayValues() {
                return false;
            }

            @Override
            public boolean supportsFloatArrayValues() {
                return false;
            }

            @Override
            public boolean supportsIntegerArrayValues() {
                return false;
            }

            @Override
            public boolean supportsLongArrayValues() {
                return false;
            }

            @Override
            public boolean supportsStringArrayValues() {
                return false;
            }

            @Override
            public boolean supportsSerializableValues() {
                return false;
            }

            @Override
            public boolean supportsStringValues() {
                return false;
            }

            @Override
            public boolean supportsUniformListValues() {
                return false;
            }
        }
    }

    //neo4j indexing

    /**
     * Wait until an index comes online
     *
     * @param index the index that we want to wait for
     * @param duration duration to wait for the index to come online
     * @param unit TimeUnit of duration
     * @throws IllegalStateException if the index did not enter the ONLINE state
     *             within the given duration or if the index entered the FAILED
     *             state
     */
    public void awaitIndexOnline(IndexDefinition index, long duration, TimeUnit unit) throws ConstraintViolationException {
        this.tx().readWrite();
        Schema schema = getBaseGraph().schema();
        schema.awaitIndexOnline( index, duration, unit );
    }

    /**
     * Creates a labeled index on the given propertyKey
     *
     * @param label       The label for nodes to be indexed
     * @param propertyKey the property key to include in this index to be created.

     * @return the created {@link org.neo4j.graphdb.schema.IndexDefinition index}.
     * @throws org.neo4j.graphdb.ConstraintViolationException if creating this index would violate one or more constraints.
     */
    public IndexDefinition createLabeledIndex(String label, String propertyKey) throws ConstraintViolationException {
        this.tx().readWrite();
        Schema schema = getBaseGraph().schema();
        return schema.indexFor(DynamicLabel.label(label)).on(propertyKey).create();
    }

    /**
     * Drop the index for a label.
     *
     * @param label The label for nodes to be indexed
     */
    public void dropLabeledIndex(String label) {
        this.tx().readWrite();
        Label dynamicLabel = DynamicLabel.label(label);
        for (IndexDefinition indexDefinition : this.getBaseGraph().schema().getIndexes(dynamicLabel)) {
            indexDefinition.drop();
        }
    }

    /**
     * Start auto indexing a property.
     *
     * @param elementClass Index is on a Edge or Vertex
     * @param key          The property name to start auto indexing.
     */
    public void createLegacyIndex(Class<? extends Element> elementClass, String key) {
        this.tx().readWrite();
        if (Vertex.class.isAssignableFrom(elementClass)) {
            AutoIndexer<Node> nodeAutoIndexer = this.getBaseGraph().index().getNodeAutoIndexer();
            if (!nodeAutoIndexer.isEnabled()) {
                throw new IllegalStateException("Automatic indexing must be enabled at startup for legacy indexing to work on vertices!");
            }
            nodeAutoIndexer.startAutoIndexingProperty(key);
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            RelationshipAutoIndexer relationshipAutoIndexer = this.getBaseGraph().index().getRelationshipAutoIndexer();
            if (!relationshipAutoIndexer.isEnabled()) {
                throw new IllegalStateException("Automatic indexing must be enabled at startup for legacy indexing to work on edges!");
            }
            relationshipAutoIndexer.startAutoIndexingProperty(key);
        } else {
            throw new IllegalArgumentException("Class is not indexable: " + elementClass);
        }
    }

    /**
     * Stop auto indexing a property.
     *
     * @param elementClass Index is on a Edge or Vertex
     * @param key          The property key to start auto indexing.
     */
    public void dropLegacyIndex(Class<? extends Element> elementClass, String key) {
        this.tx().readWrite();
        if (Vertex.class.isAssignableFrom(elementClass)) {
            AutoIndexer<Node> nodeAutoIndexer = this.getBaseGraph().index().getNodeAutoIndexer();
            if (!nodeAutoIndexer.isEnabled()) {
                throw new IllegalStateException("Automatic indexing must be enabled at startup for legacy indexing to work on vertices!");
            }
            nodeAutoIndexer.stopAutoIndexingProperty(key);
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            RelationshipAutoIndexer relationshipAutoIndexer = this.getBaseGraph().index().getRelationshipAutoIndexer();
            if (!relationshipAutoIndexer.isEnabled()) {
                throw new IllegalStateException("Automatic indexing must be enabled at startup for legacy indexing to work on edges!");
            }
            relationshipAutoIndexer.stopAutoIndexingProperty(key);
        } else {
            throw new IllegalArgumentException("Class is not indexable: " + elementClass);
        }
    }

    /**
     * Imposes a uniqueness constraint for the given property, such that
     * there can be at most one node, having the given label, for any set value of that property key.
     *
     * @param label       The label this constraint is for.
     * @param propertyKey The property key to start auto indexing.
     * @return Definition of the constraint.
     */
    public ConstraintDefinition createUniqueConstraint(String label, String propertyKey) {
        this.tx().readWrite();
        Schema schema = this.getBaseGraph().schema();
        return schema.constraintFor(DynamicLabel.label(label)).assertPropertyIsUnique(propertyKey).create();
    }

    /**
     * Drops all constraints association with the label
     * @param label the label to get constraints for
     */
    public void dropConstraint(String label) {
        this.tx().readWrite();
        Schema schema = this.getBaseGraph().schema();
        for (ConstraintDefinition cd : schema.getConstraints(DynamicLabel.label(label))) {
            cd.drop();
        }
    }

    /**
     * @return all {@link IndexDefinition indexes} in this database.
     */
    public Iterable<IndexDefinition> getLabeledIndexes() {
        this.tx().readWrite();
        Schema schema = this.getBaseGraph().schema();
        return schema.getIndexes();
    }

    /**
     * Returns the set of property names that are currently monitored for auto
     * indexing. If this auto indexer is set to ignore properties, the result
     * is the empty set.
     *
     * @return An immutable set of the auto indexed property names, possibly
     *         empty.
     */
    public <E extends Element> Set<String> getIndexedKeys(final Class<E> elementClass) {
        this.tx().readWrite();
        if (Vertex.class.isAssignableFrom(elementClass)) {
            final AutoIndexer indexer = this.getBaseGraph().index().getNodeAutoIndexer();
            return indexer.getAutoIndexedProperties();
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            final AutoIndexer indexer = this.getBaseGraph().index().getRelationshipAutoIndexer();
            return indexer.getAutoIndexedProperties();
        } else {
            throw new IllegalArgumentException("Class is not indexable: " + elementClass);
        }
    }

}
