package com.tinkerpop.gremlin.neo4j.structure;

import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Transaction;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.commons.configuration.Configuration;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.management.TransactionManager;

import java.util.Optional;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Neo4jGraph implements Graph {
    private GraphDatabaseService rawGraph;
    private static final String INDEXED_KEYS_POSTFIX = ":indexed_keys";

    protected final ThreadLocal<org.neo4j.graphdb.Transaction> tx = new ThreadLocal<org.neo4j.graphdb.Transaction>() {
        protected org.neo4j.graphdb.Transaction initialValue() {
            return null;
        }
    };

    protected final ThreadLocal<Boolean> checkElementsInTransaction = new ThreadLocal<Boolean>() {
        protected Boolean initialValue() {
            return false;
        }
    };

    private final TransactionManager transactionManager;
    private final ExecutionEngine cypher;

    public Neo4jGraph(final GraphDatabaseService rawGraph) {
        this.rawGraph = rawGraph;
        transactionManager = ((GraphDatabaseAPI) rawGraph).getDependencyResolver().resolveDependency(TransactionManager.class);
        cypher = new ExecutionEngine(rawGraph);

        // todo: indices were established in init
        // init();
    }

    private Neo4jGraph(final Configuration configuration) {
        try {
            final String directory = configuration.getString("directory");
            final GraphDatabaseBuilder builder = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(directory);

            /* todo: convert to map to pass to config ... ConfigurationConverter
            if (null != configuration)
                this.rawGraph = builder.setConfig(configuration).newGraphDatabase();
            else
            */

            this.rawGraph = builder.newGraphDatabase();

            transactionManager = ((GraphDatabaseAPI) rawGraph).getDependencyResolver().resolveDependency(TransactionManager.class);
            cypher = new ExecutionEngine(rawGraph);

            // todo: indices were established in init
            // init();

        } catch (Exception e) {
            if (this.rawGraph != null)
                this.rawGraph.shutdown();
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
    public static <G extends Graph> G open(final Optional<Configuration> configuration) {
        // todo: check null on configuration and validate what's passed in
        return (G) new Neo4jGraph(configuration.get());
    }


    @Override
    public Vertex addVertex(final Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);

        // todo: throw since id is not assignable...check features
        Object idString = ElementHelper.getIdValue(keyValues).orElse(null);

        final String label = ElementHelper.getLabelValue(keyValues).orElse(null);

        this.autoStartTransaction(true);
        return new Neo4jVertex(this.rawGraph.createNode(DynamicLabel.label(label)), this);
    }

    @Override
    public GraphTraversal<Vertex, Vertex> V() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public GraphTraversal<Edge, Edge> E() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public GraphComputer compute() {
        throw Graph.Exceptions.graphComputerNotSupported(); // todo: fix later
    }

    @Override
    public Transaction tx() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public <M extends Memory> M memory() {
        throw Graph.Exceptions.memoryNotSupported(); // todo: fix later
    }

    @Override
    public void close() throws Exception {
        // todo: how should transactions be treated on close here.  in tp2, we did this prior to shutdown...
        /*
        try {
            this.commit();
        } catch (TransactionFailureException e) {
            logger.warning("Failure on shutdown "+e.getMessage());
            // TODO: inspect why certain transactions fail
        }
         */

        if (this.rawGraph != null)
            this.rawGraph.shutdown();
    }

    public String toString() {
        return StringFactory.graphString(this, rawGraph.toString());
    }

    public Features getFeatures() {
        return new Neo4jGraphFeatures();
    }

    public static class Neo4jGraphFeatures implements Features {
        @Override
        public GraphFeatures graph() {
            return new GraphFeatures() {
                @Override
                public boolean supportsMemory() {
                    return false;    // todo: temporary...doesn't neo4j support graph properties
                }

                @Override
                public boolean supportsComputer() {
                    return false;  // todo: temporary...
                }
            };
        }
    }

    /**
     * Neo4j's transactions are not consistent between the graph and the graph
     * indices. Moreover, global graph operations are not consistent. For
     * example, if a vertex is removed and then an index is queried in the same
     * transaction, the removed vertex can be returned. This method allows the
     * developer to turn on/off a Neo4j2Graph 'hack' that ensures transactional
     * consistency. The default behavior for Neo4j2Graph is to use Neo4j's native
     * behavior which ensures speed at the expensive of consistency. Note that
     * this boolean switch is local to the current thread (i.e. a ThreadLocal
     * variable).
     *
     * @param checkElementsInTransaction check whether an element is in the transaction between
     *                                   returning it
     */
    public void setCheckElementsInTransaction(final boolean checkElementsInTransaction) {
        this.checkElementsInTransaction.set(checkElementsInTransaction);
    }

    // The forWrite flag is true when the autoStartTransaction method is
    // called before any operation which will modify the graph in any way. It
    // is not used in this simple implementation but is required in subclasses
    // which enforce transaction rules. Now that Neo4j reads also require a
    // transaction to be open it is otherwise impossible to tell the difference
    // between the beginning of a write operation and the beginning of a read
    // operation.
    public void autoStartTransaction(boolean forWrite) {
        if (tx.get() == null)
            tx.set(this.rawGraph.beginTx());
    }

    public GraphDatabaseService getRawGraph() {
        return this.rawGraph;
    }

    protected boolean checkElementsInTransaction() {
        if (this.tx.get() == null) {
            return false;
        } else {
            return this.checkElementsInTransaction.get();
        }
    }
}
