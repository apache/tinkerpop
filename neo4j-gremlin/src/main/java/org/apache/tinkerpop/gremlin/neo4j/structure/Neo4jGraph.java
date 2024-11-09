/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.neo4j.structure;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ConfigurationConverter;
import org.apache.tinkerpop.gremlin.neo4j.process.traversal.step.sideEffect.CypherStartStep;
import org.apache.tinkerpop.gremlin.neo4j.process.traversal.strategy.optimization.Neo4jGraphStepStrategy;
import org.apache.tinkerpop.gremlin.neo4j.process.util.Neo4jCypherIterator;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.AbstractThreadLocalTransaction;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.TransactionException;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.neo4j.tinkerpop.api.Neo4jFactory;
import org.neo4j.tinkerpop.api.Neo4jGraphAPI;
import org.neo4j.tinkerpop.api.Neo4jTx;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Pieter Martin
 * @deprecated See: https://tinkerpop.apache.org/docs/3.5.7/reference/#neo4j-gremlin
 */
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_INTEGRATE)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_LIMITED_STANDARD)
@Graph.OptIn("org.apache.tinkerpop.gremlin.neo4j.NativeNeo4jSuite")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.TransactionMultiThreadedTest",
        method = "*",
        reason = "Some scenarios are supported by Neo4jGraph")
@Deprecated
public final class Neo4jGraph implements Graph, WrappedGraph<Neo4jGraphAPI> {

    static {
        TraversalStrategies.GlobalCache.registerStrategies(Neo4jGraph.class, TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone().addStrategies(Neo4jGraphStepStrategy.instance()));
    }

    private static final Configuration EMPTY_CONFIGURATION = new BaseConfiguration() {{
        this.setProperty(Graph.GRAPH, Neo4jGraph.class.getName());
    }};

    protected Features features = new Neo4jGraphFeatures();

    protected Neo4jGraphAPI baseGraph;
    protected BaseConfiguration configuration = new BaseConfiguration();

    public static final String CONFIG_DIRECTORY = "gremlin.neo4j.directory";
    public static final String CONFIG_CONF = "gremlin.neo4j.conf";

    private final Neo4jTransaction neo4jTransaction = new Neo4jTransaction();
    private Neo4jGraphVariables neo4jGraphVariables;

    private void initialize(final Neo4jGraphAPI baseGraph, final Configuration configuration) {
        this.configuration.copy(configuration);
        this.baseGraph = baseGraph;
        this.neo4jGraphVariables = new Neo4jGraphVariables(this);
    }

    protected Neo4jGraph(final Neo4jGraphAPI baseGraph, final Configuration configuration) {
        this.initialize(baseGraph, configuration);
    }

    protected Neo4jGraph(final Configuration configuration) {
        this.configuration.copy(configuration);
        final String directory = this.configuration.getString(CONFIG_DIRECTORY);
        final Map neo4jSpecificConfig = ConfigurationConverter.getMap(this.configuration.subset(CONFIG_CONF));
        this.baseGraph = Neo4jFactory.Builder.open(directory, neo4jSpecificConfig);
        this.initialize(this.baseGraph, configuration);
    }

    /**
     * Open a new {@link Neo4jGraph} instance.
     *
     * @param configuration the configuration for the instance
     * @return a newly opened {@link Graph}
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
    public static Neo4jGraph open(final Neo4jGraphAPI baseGraph) {
        return new Neo4jGraph(baseGraph, EMPTY_CONFIGURATION);
    }

    @Override
    public Vertex addVertex(final Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        if (ElementHelper.getIdValue(keyValues).isPresent())
            throw Vertex.Exceptions.userSuppliedIdsNotSupported();
        this.tx().readWrite();
        final Neo4jVertex vertex = new Neo4jVertex(this.baseGraph.createNode(ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL).split(Neo4jVertex.LABEL_DELIMINATOR)), this);
        ElementHelper.attachProperties(vertex, keyValues);
        return vertex;
    }

    @Override
    public Iterator<Vertex> vertices(final Object... vertexIds) {
        this.tx().readWrite();
        if (0 == vertexIds.length) {
            return IteratorUtils.stream(this.getBaseGraph().allNodes())
                    .map(node -> (Vertex) new Neo4jVertex(node, this)).iterator();
        } else {
            return Stream.of(vertexIds)
                    .map(id -> {
                        if (id instanceof Number)
                            return ((Number) id).longValue();
                        else if (id instanceof String)
                            return Long.valueOf(id.toString());
                        else if (id instanceof Vertex && ((Vertex) id).id() instanceof Number)
                            return ((Number) ((Vertex) id).id()).longValue();
                        else if (null == id)
                            return null;
                        else
                            throw new IllegalArgumentException("Unknown vertex id type: " + id);
                    })
                    .flatMap(id -> {
                        // can't have a null id so just filter
                        if (null == id) return Stream.empty();
                        try {
                            return Stream.of(this.baseGraph.getNodeById(id));
                        } catch (final RuntimeException e) {
                            if (Neo4jHelper.isNotFound(e)) return Stream.empty();
                            throw e;
                        }
                    })
                    .map(node -> (Vertex) new Neo4jVertex(node, this)).iterator();
        }
    }

    @Override
    public Iterator<Edge> edges(final Object... edgeIds) {
        this.tx().readWrite();
        if (0 == edgeIds.length) {
            return IteratorUtils.stream(this.getBaseGraph().allRelationships())
                    .map(relationship -> (Edge) new Neo4jEdge(relationship, this)).iterator();
        } else {
            return Stream.of(edgeIds)
                    .map(id -> {
                        if (id instanceof Number)
                            return ((Number) id).longValue();
                        else if (id instanceof String)
                            return Long.valueOf(id.toString());
                        else if (id instanceof Edge && ((Edge) id).id() instanceof Number)
                            return ((Number) ((Edge) id).id()).longValue();
                        else if (null == id)
                            return null;
                        else
                            throw new IllegalArgumentException("Unknown edge id type: " + id);
                    })
                    .flatMap(id -> {
                        // can't have a null id so just filter
                        if (null == id) return Stream.empty();
                        try {
                            return Stream.of(this.baseGraph.getRelationshipById(id));
                        } catch (final RuntimeException e) {
                            if (Neo4jHelper.isNotFound(e)) return Stream.empty();
                            throw e;
                        }
                    })
                    .map(relationship -> (Edge) new Neo4jEdge(relationship, this)).iterator();
        }
    }

    @Override
    public <C extends GraphComputer> C compute(final Class<C> graphComputerClass) {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public GraphComputer compute() {
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

    /**
     * This implementation of {@code close} will also close the current transaction on the thread, but it
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
        return features;
    }

    @Override
    public Neo4jGraphAPI getBaseGraph() {
        return this.baseGraph;
    }

    /**
     * Execute the Cypher query and get the result set as a {@link GraphTraversal}.
     *
     * @param query the Cypher query to execute
     * @return a fluent Gremlin traversal
     */
    public <S, E> GraphTraversal<S, E> cypher(final String query) {
        return cypher(query, Collections.emptyMap());
    }

    /**
     * Execute the Cypher query with provided parameters and get the result set as a {@link GraphTraversal}.
     *
     * @param query      the Cypher query to execute
     * @param parameters the parameters of the Cypher query
     * @return a fluent Gremlin traversal
     */
    public <S, E> GraphTraversal<S, E> cypher(final String query, final Map<String, Object> parameters) {
        this.tx().readWrite();
        final GraphTraversal.Admin<S, E> traversal = new DefaultGraphTraversal<>(this);
        traversal.addStep(new CypherStartStep(traversal, query, new Neo4jCypherIterator<>((Iterator) this.baseGraph.execute(query, parameters), this)));
        return traversal;
    }

    class Neo4jTransaction extends AbstractThreadLocalTransaction {

        protected final ThreadLocal<Neo4jTx> threadLocalTx = ThreadLocal.withInitial(() -> null);

        public Neo4jTransaction() {
            super(Neo4jGraph.this);
        }

        @Override
        public void doOpen() {
            threadLocalTx.set(getBaseGraph().tx());
        }

        @Override
        public void doCommit() throws TransactionException {
            try (Neo4jTx tx = threadLocalTx.get()) {
                tx.success();
            } catch (Exception ex) {
                throw new TransactionException(ex);
            } finally {
                threadLocalTx.remove();
            }
        }

        @Override
        public void doRollback() throws TransactionException {
            try (Neo4jTx tx = threadLocalTx.get()) {
                tx.failure();
            } catch (Exception e) {
                throw new TransactionException(e);
            } finally {
                threadLocalTx.remove();
            }
        }

        @Override
        public boolean isOpen() {
            return (threadLocalTx.get() != null);
        }
    }

    public class Neo4jGraphFeatures implements Features {
        protected GraphFeatures graphFeatures = new Neo4jGraphGraphFeatures();
        protected VertexFeatures vertexFeatures = new Neo4jVertexFeatures();
        protected EdgeFeatures edgeFeatures = new Neo4jEdgeFeatures();

        @Override
        public GraphFeatures graph() {
            return graphFeatures;
        }

        @Override
        public VertexFeatures vertex() {
            return vertexFeatures;
        }

        @Override
        public EdgeFeatures edge() {
            return edgeFeatures;
        }

        @Override
        public String toString() {
            return StringFactory.featureString(this);
        }

        public class Neo4jGraphGraphFeatures implements GraphFeatures {

            private VariableFeatures variableFeatures = new Neo4jGraphVariables.Neo4jVariableFeatures();

            Neo4jGraphGraphFeatures() {
            }

            @Override
            public boolean supportsConcurrentAccess() {
                return false;
            }

            @Override
            public boolean supportsComputer() {
                return false;
            }

            @Override
            public VariableFeatures variables() {
                return variableFeatures;
            }

            @Override
            public boolean supportsThreadedTransactions() {
                return false;
            }
        }

        public class Neo4jVertexFeatures extends Neo4jElementFeatures implements VertexFeatures {

            private final VertexPropertyFeatures vertexPropertyFeatures = new Neo4jVertexPropertyFeatures();

            protected Neo4jVertexFeatures() {
            }

            @Override
            public VertexPropertyFeatures properties() {
                return vertexPropertyFeatures;
            }

            @Override
            public boolean supportsMetaProperties() {
                return false;
            }

            @Override
            public boolean supportsMultiProperties() {
                return false;
            }

            @Override
            public boolean supportsUserSuppliedIds() {
                return false;
            }

            @Override
            public VertexProperty.Cardinality getCardinality(final String key) {
                return VertexProperty.Cardinality.single;
            }
        }

        public class Neo4jEdgeFeatures extends Neo4jElementFeatures implements EdgeFeatures {

            private final EdgePropertyFeatures edgePropertyFeatures = new Neo4jEdgePropertyFeatures();

            Neo4jEdgeFeatures() {
            }

            @Override
            public EdgePropertyFeatures properties() {
                return edgePropertyFeatures;
            }
        }

        public class Neo4jElementFeatures implements ElementFeatures {

            Neo4jElementFeatures() {
            }

            @Override
            public boolean supportsNullPropertyValues() {
                return false;
            }

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

            Neo4jVertexPropertyFeatures() {
            }

            @Override
            public boolean supportsNullPropertyValues() {
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

            @Override
            public boolean supportsAnyIds() {
                return false;
            }
        }

        public class Neo4jEdgePropertyFeatures implements EdgePropertyFeatures {

            Neo4jEdgePropertyFeatures() {
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