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
package org.apache.tinkerpop.gremlin.process.computer.bulkloading;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.AbstractVertexProgramBuilder;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.javatuples.Pair;
import org.javatuples.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class BulkLoaderVertexProgram implements VertexProgram<Tuple> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BulkLoaderVertexProgram.class);

    public static final String BULK_LOADER_VERTEX_PROGRAM_CFG_PREFIX = "gremlin.bulkLoading";
    private static final String GRAPH_CFG_KEY = "graph";
    private static final String BULK_LOADER_CFG_KEY = "loader";
    private static final String INTERMEDIATE_BATCH_SIZE_CFG_KEY = "intermediateBatchSize";
    public static final String BULK_LOADER_VERTEX_ID_CFG_KEY = "vertexIdProperty";
    public static final String DEFAULT_BULK_LOADER_VERTEX_ID = "bulkLoader.vertex.id";

    private final MessageScope messageScope;
    private final Set<String> elementComputeKeys;
    private Configuration configuration;
    private BulkLoader bulkLoader;
    private Graph graph;
    private GraphTraversalSource g;
    private Long intermediateBatchSize;
    private AtomicLong counter = new AtomicLong();

    private BulkLoaderVertexProgram() {
        messageScope = MessageScope.Local.of(__::inE);
        elementComputeKeys = new HashSet<>();
    }

    private Configuration getGraphConfiguration() {
        final Configuration config = configuration.subset(GRAPH_CFG_KEY);
        config.setProperty(Graph.GRAPH, config.getString("class"));
        config.clearProperty("class");
        return config;
    }

    private BulkLoader createBulkLoader() {
        final BulkLoader loader;
        final Configuration config = configuration.subset(BULK_LOADER_CFG_KEY);
        if (config.containsKey("class")) {
            final String className = config.getString("class");
            config.clearProperty("class");
            try {
                final Class<?> bulkLoaderClass = Class.forName(className);
                loader = (BulkLoader) bulkLoaderClass.getConstructor().newInstance();
            } catch (ClassNotFoundException e) {
                LOGGER.error("Unable to find custom bulk loader class: {}", className);
                throw new IllegalStateException(e);
            } catch (Exception e) {
                LOGGER.error("Unable to create an instance of the given bulk loader class: {}", className);
                throw new IllegalStateException(e);
            }
        } else {
            loader = new DefaultBulkLoader();
        }
        loader.configure(config);
        return loader;
    }

    private void commit(final boolean force) {
        if (!force && (intermediateBatchSize == 0L || counter.incrementAndGet() % intermediateBatchSize != 0)) return;
        if (null != graph) {
            if (graph.features().graph().supportsTransactions()) {
                LOGGER.info("Committing transaction on Graph instance: {}", graph);
                try {
                    graph.tx().commit(); // TODO will Giraph/MR restart the program and re-run execute if this fails?
                    LOGGER.debug("Committed transaction on Graph instance: {}", graph);
                    counter.set(0L);
                } catch (Exception e) {
                    LOGGER.error("Failed to commit transaction on Graph instance: {}", graph);
                    graph.tx().rollback();
                    throw e;
                }
            }
            try {
                graph.close();
                LOGGER.info("Closed Graph instance: {}", graph);
                graph = null;
            } catch (Exception e) {
                LOGGER.warn("Failed to close Graph instance", e);
            }
        }
    }

    @Override
    public void setup(final Memory memory) {

    }

    @Override
    public void loadState(final Graph graph, final Configuration config) {
        configuration = new BaseConfiguration();
        if (config != null) {
            ConfigurationUtils.copy(config, configuration);
        }
        if (!configuration.subset(BULK_LOADER_CFG_KEY).containsKey(BULK_LOADER_VERTEX_ID_CFG_KEY)) {
            configuration.addProperty(
                    String.join(".", BULK_LOADER_CFG_KEY, BULK_LOADER_VERTEX_ID_CFG_KEY),
                    DEFAULT_BULK_LOADER_VERTEX_ID);
        }
        intermediateBatchSize = configuration.getLong(INTERMEDIATE_BATCH_SIZE_CFG_KEY, 0L);
        final String bulkLoaderVertexIdProperty = configuration.subset(BULK_LOADER_CFG_KEY)
                .getString(BULK_LOADER_VERTEX_ID_CFG_KEY);
        if (!elementComputeKeys.contains(bulkLoaderVertexIdProperty)) {
            synchronized (elementComputeKeys) {
                elementComputeKeys.add(bulkLoaderVertexIdProperty);
            }
        }
    }

    @Override
    public void storeState(final Configuration config) {
        VertexProgram.super.storeState(config);
        if (configuration != null) {
            ConfigurationUtils.copy(configuration, config);
        }
    }

    @Override
    public void workerIterationStart(final Memory memory) {
        if (null == graph) {
            graph = GraphFactory.open(getGraphConfiguration());
            LOGGER.info("Opened Graph instance: {}", graph);
            try {
                if (!graph.features().graph().supportsConcurrentAccess()) {
                    throw new IllegalStateException("The given graph instance does not allow concurrent access.");
                }
                g = graph.traversal();
                bulkLoader = createBulkLoader();
            } catch (Exception e) {
                try {
                    graph.close();
                } catch (Exception e2) {
                    LOGGER.warn("Failed to close Graph instance", e2);
                }
                throw e;
            }
        } else {
            LOGGER.warn("Leaked Graph instance: {}", graph);
        }
    }

    @Override
    public void workerIterationEnd(final Memory memory) {
        this.commit(true);
    }

    @Override
    public void execute(final Vertex sourceVertex, final Messenger<Tuple> messenger, final Memory memory) {
        try {
            executeInternal(sourceVertex, messenger, memory);
        } catch (Exception e) {
            if (graph.features().graph().supportsTransactions()) {
                graph.tx().rollback();
            }
            throw e;
        }
    }

    private void executeInternal(final Vertex sourceVertex, final Messenger<Tuple> messenger, final Memory memory) {
        if (memory.isInitialIteration()) {
            // get or create the vertex
            final Vertex targetVertex = bulkLoader.getOrCreateVertex(sourceVertex, graph, g);
            // write all the properties of the vertex to the newly created vertex
            final Iterator<VertexProperty<Object>> vpi = sourceVertex.properties();
            while (vpi.hasNext()) {
                bulkLoader.getOrCreateVertexProperty(vpi.next(), targetVertex, graph, g);
            }
            this.commit(false);
            if (!bulkLoader.useUserSuppliedIds()) {
                // create an id pair and send it to all the vertex's incoming adjacent vertices
                sourceVertex.property(bulkLoader.getVertexIdProperty(), targetVertex.id());
                messenger.sendMessage(messageScope, Pair.with(sourceVertex.id(), targetVertex.id()));
            }
        } else {
            if (bulkLoader.useUserSuppliedIds()) {
                final Vertex outV = bulkLoader.getVertex(sourceVertex, graph, g);
                sourceVertex.edges(Direction.OUT).forEachRemaining(edge -> {
                    final Vertex inV = bulkLoader.getVertex(edge.inVertex(), graph, g);
                    bulkLoader.getOrCreateEdge(edge, outV, inV, graph, g);
                });
            } else {
                // create an id map and populate it with all the incoming messages
                final Map<Object, Object> idPairs = new HashMap<>();
                final Iterator<Tuple> idi = messenger.receiveMessages();
                while (idi.hasNext()) {
                    final Tuple idPair = idi.next();
                    idPairs.put(idPair.getValue(0), idPair.getValue(1));
                }
                // get the vertex given the dummy id property
                final Long outVId = sourceVertex.value(bulkLoader.getVertexIdProperty());
                final Vertex outV = bulkLoader.getVertexById(outVId, graph, g);
                // for all the incoming edges of the vertex, get the incoming adjacent vertex and write the edge and its properties
                sourceVertex.edges(Direction.OUT).forEachRemaining(edge -> {
                    final Object inVId = idPairs.get(edge.inVertex().id());
                    final Vertex inV = bulkLoader.getVertexById(inVId, graph, g);
                    bulkLoader.getOrCreateEdge(edge, outV, inV, graph, g);
                    this.commit(false);
                });
            }
        }
    }

    @Override
    public boolean terminate(final Memory memory) {
        return memory.getIteration() >= 1;
    }

    @Override
    public Set<String> getElementComputeKeys() {
        return elementComputeKeys;
    }

    @Override
    public Set<MessageScope> getMessageScopes(final Memory memory) {
        return Collections.singleton(messageScope);
    }

    @Override
    public VertexProgram<Tuple> clone() {
        return this;
    }

    @Override
    public GraphComputer.ResultGraph getPreferredResultGraph() {
        return GraphComputer.ResultGraph.NEW;
    }

    @Override
    public GraphComputer.Persist getPreferredPersist() {
        return GraphComputer.Persist.EDGES;
    }

    @Override
    public String toString() {
        return StringFactory.vertexProgramString(this, "");
    }

    public static Builder build() {
        return new Builder();
    }

    public static class Builder extends AbstractVertexProgramBuilder<Builder> {

        private Builder() {
            super(BulkLoaderVertexProgram.class);
        }

        @SuppressWarnings("unchecked")
        @Override
        public BulkLoaderVertexProgram create(final Graph graph) {
            ConfigurationUtils.append(graph.configuration().subset(BULK_LOADER_VERTEX_PROGRAM_CFG_PREFIX), configuration);
            return (BulkLoaderVertexProgram) VertexProgram.createVertexProgram(graph, this.configuration);
        }
    }
}
