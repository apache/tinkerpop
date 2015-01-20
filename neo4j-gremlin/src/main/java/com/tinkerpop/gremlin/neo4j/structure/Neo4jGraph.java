package com.tinkerpop.gremlin.neo4j.structure;

import com.tinkerpop.gremlin.neo4j.process.graph.Neo4jGraphTraversal;
import com.tinkerpop.gremlin.neo4j.process.graph.step.sideEffect.Neo4jGraphStep;
import com.tinkerpop.gremlin.neo4j.process.graph.step.util.Neo4jCypherIterator;
import com.tinkerpop.gremlin.neo4j.process.graph.strategy.Neo4jGraphStepStrategy;
import com.tinkerpop.gremlin.neo4j.process.graph.util.DefaultNeo4jGraphTraversal;
import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Transaction;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.structure.util.wrapped.WrappedGraph;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.tooling.GlobalGraphOperations;

import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Pieter Martin
 */
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
public class Neo4jGraph implements Graph, Graph.Iterators, WrappedGraph<GraphDatabaseService> {

    static {
        try {
            TraversalStrategies.GlobalCache.registerStrategies(Neo4jGraph.class, TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone().addStrategies(Neo4jGraphStepStrategy.instance()));
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    private static final Configuration EMPTY_CONFIGURATION = new BaseConfiguration() {{
        this.setProperty(Graph.GRAPH, Neo4jGraph.class.getName());
    }};


    private GraphDatabaseService baseGraph;
    private BaseConfiguration configuration = new BaseConfiguration();

    public static final String CONFIG_DIRECTORY = "gremlin.neo4j.directory";
    public static final String CONFIG_HA = "gremlin.neo4j.ha";
    public static final String CONFIG_CONF = "gremlin.neo4j.conf";
    public static final String CONFIG_META_PROPERTIES = "gremlin.neo4j.metaProperties";
    public static final String CONFIG_MULTI_PROPERTIES = "gremlin.neo4j.multiProperties";
    public static final String CONFIG_CHECK_ELEMENTS_IN_TRANSACTION = "gremlin.neo4j.checkElementsInTransaction";

    private final Neo4jTransaction neo4jTransaction = new Neo4jTransaction();
    private final Neo4jGraphVariables neo4jGraphVariables;

    protected final boolean supportsMetaProperties;
    protected final boolean supportsMultiProperties;
    protected boolean checkElementsInTransaction = false;

    protected final TransactionManager transactionManager;
    protected final ExecutionEngine cypher;

    private Neo4jGraph(final GraphDatabaseService baseGraph) {
        this.configuration.copy(EMPTY_CONFIGURATION);
        this.baseGraph = baseGraph;
        this.transactionManager = ((GraphDatabaseAPI) baseGraph).getDependencyResolver().resolveDependency(TransactionManager.class);
        this.cypher = new ExecutionEngine(this.baseGraph);
        this.neo4jGraphVariables = new Neo4jGraphVariables(this);

        ///////////
        final Optional<Boolean> metaProperties = this.neo4jGraphVariables.get(Hidden.hide(CONFIG_META_PROPERTIES));
        if (metaProperties.isPresent()) {
            this.supportsMetaProperties = metaProperties.get();
        } else {
            this.supportsMetaProperties = false;
            this.neo4jGraphVariables.set(Hidden.hide(CONFIG_META_PROPERTIES), false);
        }
        final Optional<Boolean> multiProperties = this.neo4jGraphVariables.get(Hidden.hide(CONFIG_MULTI_PROPERTIES));
        if (multiProperties.isPresent()) {
            this.supportsMultiProperties = multiProperties.get();
        } else {
            this.supportsMultiProperties = false;
            this.neo4jGraphVariables.set(Hidden.hide(CONFIG_MULTI_PROPERTIES), false);
        }
        if ((this.supportsMetaProperties && !this.supportsMultiProperties) || (!this.supportsMetaProperties && this.supportsMultiProperties)) {
            tx().rollback();
            throw new UnsupportedOperationException("Neo4jGraph currently requires either both meta- and multi-properties activated or neither activated");
        }
        final Optional<Boolean> elementsInTransaction = this.neo4jGraphVariables.get(Hidden.hide(CONFIG_CHECK_ELEMENTS_IN_TRANSACTION));
        if (elementsInTransaction.isPresent()) {
            this.checkElementsInTransaction = elementsInTransaction.get();
        } else {
            this.checkElementsInTransaction = false;
            this.neo4jGraphVariables.set(Hidden.hide(CONFIG_CHECK_ELEMENTS_IN_TRANSACTION), false);
        }
        tx().commit();
        ///////////
    }

    private Neo4jGraph(final Configuration configuration) {
        try {
            this.configuration.copy(EMPTY_CONFIGURATION);
            this.configuration.copy(configuration);
            final String directory = this.configuration.getString(CONFIG_DIRECTORY);
            final Map neo4jSpecificConfig = ConfigurationConverter.getMap(this.configuration.subset(CONFIG_CONF));
            final boolean ha = this.configuration.getBoolean(CONFIG_HA, false);
            // if HA is enabled then use the correct factory to instantiate the GraphDatabaseService
            this.baseGraph = ha ?
                    new HighlyAvailableGraphDatabaseFactory().newHighlyAvailableDatabaseBuilder(directory).setConfig(neo4jSpecificConfig).newGraphDatabase() :
                    new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(directory).
                            setConfig(neo4jSpecificConfig).newGraphDatabase();
            this.transactionManager = ((GraphDatabaseAPI) this.baseGraph).getDependencyResolver().resolveDependency(TransactionManager.class);
            this.cypher = new ExecutionEngine(this.baseGraph);
            this.neo4jGraphVariables = new Neo4jGraphVariables(this);
            ///////////
            if (!this.neo4jGraphVariables.get(Hidden.hide(CONFIG_META_PROPERTIES)).isPresent())
                this.neo4jGraphVariables.set(Hidden.hide(CONFIG_META_PROPERTIES), this.configuration.getBoolean(CONFIG_META_PROPERTIES, false));
            // TODO: Logger saying the configuration properties are ignored if already in Graph.Variables
            if (!this.neo4jGraphVariables.get(Hidden.hide(CONFIG_MULTI_PROPERTIES)).isPresent())
                this.neo4jGraphVariables.set(Hidden.hide(CONFIG_MULTI_PROPERTIES), this.configuration.getBoolean(CONFIG_MULTI_PROPERTIES, false));
            // TODO: Logger saying the configuration properties are ignored if already in Graph.Variables
            this.supportsMetaProperties = this.neo4jGraphVariables.<Boolean>get(Hidden.hide(CONFIG_META_PROPERTIES)).get();
            this.supportsMultiProperties = this.neo4jGraphVariables.<Boolean>get(Hidden.hide(CONFIG_MULTI_PROPERTIES)).get();
            if ((this.supportsMetaProperties && !this.supportsMultiProperties) || (!this.supportsMetaProperties && this.supportsMultiProperties)) {
                tx().rollback();
                throw new UnsupportedOperationException("Neo4jGraph currently requires either both meta- and multi-properties activated or neither activated");
            }
            //
            // TODO: Logger saying the configuration properties are ignored if already in Graph.Variables
            if (!this.neo4jGraphVariables.get(Hidden.hide(CONFIG_CHECK_ELEMENTS_IN_TRANSACTION)).isPresent())
                this.neo4jGraphVariables.set(Hidden.hide(CONFIG_CHECK_ELEMENTS_IN_TRANSACTION), this.configuration.getBoolean(CONFIG_CHECK_ELEMENTS_IN_TRANSACTION, false));
            this.checkElementsInTransaction = this.neo4jGraphVariables.<Boolean>get(Hidden.hide(CONFIG_CHECK_ELEMENTS_IN_TRANSACTION)).get();
            tx().commit();
            ///////////
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
     * @return a newly opened {@link com.tinkerpop.gremlin.structure.Graph}
     */
    public static Neo4jGraph open(final Configuration configuration) {
        if (null == configuration) throw Graph.Exceptions.argumentCanNotBeNull("configuration");
        if (!configuration.containsKey(CONFIG_DIRECTORY))
            throw new IllegalArgumentException(String.format("Neo4j configuration requires that the %s be set", CONFIG_DIRECTORY));

        return new Neo4jGraph(configuration);
    }

    /**
     * Construct a Neo4jGraph instance by specifying the directory to create the database in..
     */
    public static Neo4jGraph open(final String directory) {
        final Configuration config = new BaseConfiguration();
        config.setProperty(CONFIG_DIRECTORY, directory);
        return open(config);
    }

    /**
     * Construct a Neo4jGraph instance using an existing Neo4j raw instance.
     */
    public static Neo4jGraph open(final GraphDatabaseService baseGraph) {
        return new Neo4jGraph(Optional.ofNullable(baseGraph).orElseThrow(() -> Graph.Exceptions.argumentCanNotBeNull("baseGraph")));
    }

    @Override
    public Vertex addVertex(final Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        if (ElementHelper.getIdValue(keyValues).isPresent())
            throw Vertex.Exceptions.userSuppliedIdsNotSupported();

        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);

        this.tx().readWrite();
        final Neo4jVertex vertex = new Neo4jVertex(this.baseGraph.createNode(Neo4jHelper.makeLabels(label)), this);
        ElementHelper.attachProperties(vertex, keyValues);
        return vertex;
    }

    @Override
    public Neo4jGraphTraversal<Vertex, Vertex> V(final Object... vertexIds) {
        this.tx().readWrite();
        final Neo4jGraphTraversal<Vertex, Vertex> traversal = new DefaultNeo4jGraphTraversal<>(Neo4jGraph.class, this);
        traversal.addStep(new Neo4jGraphStep<>(traversal, this, Vertex.class, vertexIds));
        return traversal;
    }

    @Override
    public Neo4jGraphTraversal<Edge, Edge> E(final Object... edgeIds) {
        this.tx().readWrite();
        final Neo4jGraphTraversal<Edge, Edge> traversal = new DefaultNeo4jGraphTraversal<>(Neo4jGraph.class, this);
        traversal.addStep(new Neo4jGraphStep<>(traversal, this, Edge.class, edgeIds));
        return traversal;
    }

    @Override
    public GraphComputer compute(final Class... graphComputerClass) {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public Transaction tx() {
        return this.neo4jTransaction;
    }

    @Override
    public Variables variables() {
        return this.neo4jGraphVariables;
    }

    @Override
    public Configuration configuration() {
        return this.configuration;
    }

    @Override
    public Iterators iterators() {
        return this;
    }

    @Override
    public Iterator<Vertex> vertexIterator(final Object... vertexIds) {
        this.tx().readWrite();
        if (0 == vertexIds.length) {
            return StreamFactory.stream(GlobalGraphOperations.at(this.getBaseGraph()).getAllNodes())
                    .filter(node -> !this.checkElementsInTransaction || !Neo4jHelper.isDeleted(node))
                    .filter(node -> !node.hasLabel(Neo4jVertexProperty.VERTEX_PROPERTY_LABEL))
                    .map(node -> (Vertex) new Neo4jVertex(node, this)).iterator();
        } else {
            return Stream.of(vertexIds)
                    .filter(id -> id instanceof Number)
                    .flatMap(id -> {
                        try {
                            return Stream.of((Vertex) new Neo4jVertex(this.getBaseGraph().getNodeById(((Number) id).longValue()), this));
                        } catch (final NotFoundException e) {
                            return Stream.empty();
                        }
                    }).iterator();
        }
    }

    @Override
    public Iterator<Edge> edgeIterator(final Object... edgeIds) {
        this.tx().readWrite();
        if (0 == edgeIds.length) {
            return StreamFactory.stream(GlobalGraphOperations.at(this.getBaseGraph()).getAllRelationships())
                    .filter(relationship -> !this.checkElementsInTransaction || !Neo4jHelper.isDeleted(relationship))
                    .filter(relationship -> !relationship.getType().name().startsWith(Neo4jVertexProperty.VERTEX_PROPERTY_PREFIX))
                    .map(relationship -> (Edge) new Neo4jEdge(relationship, this)).iterator();
        } else {
            return Stream.of(edgeIds)
                    .filter(id -> id instanceof Number)
                    .flatMap(id -> {
                        try {
                            return Stream.of((Edge) new Neo4jEdge(this.getBaseGraph().getRelationshipById(((Number) id).longValue()), this));
                        } catch (final NotFoundException e) {
                            return Stream.empty();
                        }
                    }).iterator();
        }

    }

    /**
     * This implementation of {@code close} will also close the current transaction on the the thread, but it
     * is up to the caller to deal with dangling transactions in other threads prior to calling this method.
     */
    @Override
    public void close() throws Exception {
        this.tx().close();
        if (this.baseGraph != null) this.baseGraph.shutdown();
    }

    public String toString() {
        return StringFactory.graphString(this, baseGraph.toString());
    }

    @Override
    public Features features() {
        return new Neo4jGraphFeatures();
    }

    @Override
    public GraphDatabaseService getBaseGraph() {
        return this.baseGraph;
    }

    /**
     * Provides access to Neo4j's schema system for managing indices.
     *
     * @return Neo4j's schema/index manager.
     */
    public Schema getSchema() {
        return this.baseGraph.schema();
    }

    /**
     * Neo4j's transactions are not consistent between the graph and the graph
     * indices. Moreover, global graph operations are not consistent. For
     * example, if a vertex is removed and then an index is queried in the same
     * transaction, the removed vertex can be returned. This method allows the
     * developer to turn on/off a Neo4jGraph 'hack' that ensures transactional
     * consistency. The default behavior for Neo4jGraph is {@code true}.
     *
     * @param checkElementsInTransaction check whether an element is in the transaction between
     *                                   returning it
     */
    public void checkElementsInTransaction(final boolean checkElementsInTransaction) {
        this.checkElementsInTransaction = checkElementsInTransaction;
    }

    /**
     * Execute the Cypher query and get the result set as a {@link com.tinkerpop.gremlin.neo4j.process.graph.Neo4jGraphTraversal}.
     *
     * @param query the Cypher query to execute
     * @return a fluent Gremlin traversal
     */
    public <S, E> Neo4jGraphTraversal<S, E> cypher(final String query) {
        return cypher(query, Collections.emptyMap());
    }

    /**
     * Execute the Cypher query with provided parameters and get the result set as a {@link com.tinkerpop.gremlin.neo4j.process.graph.Neo4jGraphTraversal}.
     *
     * @param query      the Cypher query to execute
     * @param parameters the parameters of the Cypher query
     * @return a fluent Gremlin traversal
     */
    public <S, E> Neo4jGraphTraversal<S, E> cypher(final String query, final Map<String, Object> parameters) {
        this.tx().readWrite();
        final Neo4jGraphTraversal<S, E> traversal = new DefaultNeo4jGraphTraversal<>(Neo4jGraph.class, this);
        traversal.addStep(new StartStep(traversal, new Neo4jCypherIterator<S>((ResourceIterator) this.cypher.execute(query, parameters).iterator(), this)));
        return traversal;
    }

    class Neo4jTransaction implements Transaction {
        private Consumer<Transaction> readWriteConsumer;
        private Consumer<Transaction> closeConsumer;

        protected final ThreadLocal<org.neo4j.graphdb.Transaction> threadLocalTx = ThreadLocal.withInitial(() -> null);

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
            readWriteConsumer.accept(this);

            try {
                threadLocalTx.get().success();
            } finally {
                threadLocalTx.get().close();
                threadLocalTx.remove();
            }
        }

        @Override
        public void rollback() {
            readWriteConsumer.accept(this);

            try {
                javax.transaction.Transaction t = transactionManager.getTransaction();
                if (null == t || t.getStatus() == Status.STATUS_ROLLEDBACK)
                    return;

                threadLocalTx.get().failure();
            } catch (SystemException e) {
                throw new RuntimeException(e);
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
            readWriteConsumer.accept(this);
        }

        @Override
        public void close() {
            closeConsumer.accept(this);
        }

        @Override
        public Transaction onReadWrite(final Consumer<Transaction> consumer) {
            readWriteConsumer = Optional.ofNullable(consumer).orElseThrow(Transaction.Exceptions::onReadWriteBehaviorCannotBeNull);
            return this;
        }

        @Override
        public Transaction onClose(final Consumer<Transaction> consumer) {
            closeConsumer = Optional.ofNullable(consumer).orElseThrow(Transaction.Exceptions::onCloseBehaviorCannotBeNull);
            return this;
        }
    }

    public class Neo4jGraphFeatures implements Features {
        @Override
        public GraphFeatures graph() {
            return new GraphFeatures() {
                @Override
                public boolean supportsComputer() {
                    return false;
                }

                @Override
                public VariableFeatures variables() {
                    return new Neo4jGraphVariables.Neo4jVariableFeatures();
                }

                @Override
                public boolean supportsThreadedTransactions() {
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

        public class Neo4jVertexFeatures extends Neo4jElementFeatures implements VertexFeatures {
            @Override
            public VertexPropertyFeatures properties() {
                return new Neo4jVertexPropertyFeatures();
            }

            @Override
            public boolean supportsMetaProperties() {
                return Neo4jGraph.this.supportsMetaProperties;
            }

            @Override
            public boolean supportsMultiProperties() {
                return Neo4jGraph.this.supportsMultiProperties;
            }
        }

        public class Neo4jEdgeFeatures extends Neo4jElementFeatures implements EdgeFeatures {
            @Override
            public EdgePropertyFeatures properties() {
                return new Neo4jEdgePropertyFeatures();
            }
        }

        public class Neo4jElementFeatures implements ElementFeatures {
            @Override
            public boolean supportsUserSuppliedIds() {
                return false;
            }

            @Override
            public boolean supportsStringIds() {
                return false;
            }

            @Override
            public boolean supportsUuidIds() {
                return false;
            }

            @Override
            public boolean supportsAnyIds() {
                return false;
            }

            @Override
            public boolean supportsCustomIds() {
                return false;
            }
        }

        public class Neo4jVertexPropertyFeatures implements VertexPropertyFeatures {
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

            @Override
            public boolean supportsUserSuppliedIds() {
                return false;
            }
        }

        public class Neo4jEdgePropertyFeatures implements EdgePropertyFeatures {
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
    }
}
