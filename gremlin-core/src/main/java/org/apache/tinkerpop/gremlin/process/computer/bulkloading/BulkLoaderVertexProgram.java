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
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.AbstractVertexProgramBuilder;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.MutationListener;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategy;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
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

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class BulkLoaderVertexProgram implements VertexProgram<Tuple> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BulkLoaderVertexProgram.class);

    public static final String BULK_LOADER_VERTEX_PROGRAM_CFG_PREFIX = "gremlin.bulkLoaderVertexProgram";
    public static final String BULK_LOADER_CLASS_CFG_KEY = String.join(".", BULK_LOADER_VERTEX_PROGRAM_CFG_PREFIX, "class");
    public static final String BULK_LOADER_VERTEX_ID_CFG_KEY = String.join(".", BULK_LOADER_VERTEX_PROGRAM_CFG_PREFIX, "vertexIdProperty");
    public static final String INTERMEDIATE_BATCH_SIZE_CFG_KEY = String.join(".", BULK_LOADER_VERTEX_PROGRAM_CFG_PREFIX, "intermediateBatchSize");
    public static final String KEEP_ORIGINAL_IDS_CFG_KEY = String.join(".", BULK_LOADER_VERTEX_PROGRAM_CFG_PREFIX, "keepOriginalIds");
    public static final String USER_SUPPLIED_IDS_CFG_KEY = String.join(".", BULK_LOADER_VERTEX_PROGRAM_CFG_PREFIX, "userSuppliedIds");
    public static final String WRITE_GRAPH_CFG_KEY = String.join(".", BULK_LOADER_VERTEX_PROGRAM_CFG_PREFIX, "writeGraph");
    public static final String DEFAULT_BULK_LOADER_VERTEX_ID = "bulkLoader.vertex.id";

    private final MessageScope messageScope;
    private final Set<String> elementComputeKeys;
    private Configuration configuration;
    private BulkLoader bulkLoader;
    private Graph graph;
    private GraphTraversalSource g;
    private long intermediateBatchSize;

    private BulkLoadingListener listener;

    private BulkLoaderVertexProgram() {
        messageScope = MessageScope.Local.of(__::inE);
        elementComputeKeys = new HashSet<>();
    }

    private BulkLoader createBulkLoader() {
        final BulkLoader loader;
        final Configuration config = configuration.subset(BULK_LOADER_VERTEX_PROGRAM_CFG_PREFIX);
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
            loader = new IncrementalBulkLoader();
        }
        loader.configure(configuration);
        return loader;
    }

    /**
     * Eventually commits the current transaction and closes the current graph instance. commit() will be called
     * if close is set true, otherwise it will only be called if the intermediate batch size is set and reached.
     *
     * @param close Whether to close the current graph instance after calling commit() or not.
     */
    private void commit(final boolean close) {
        if (!close && (intermediateBatchSize == 0L || listener.mutations() < intermediateBatchSize))
            return;
        if (null != graph) {
            if (graph.features().graph().supportsTransactions()) {
                LOGGER.info("Committing transaction on Graph instance: {} [{} mutations]", graph, listener.mutations());
                try {
                    graph.tx().commit();
                    LOGGER.debug("Committed transaction on Graph instance: {}", graph);
                    listener.resetCounter();
                } catch (Exception e) {
                    LOGGER.error("Failed to commit transaction on Graph instance: {}", graph);
                    graph.tx().rollback();
                    listener.resetCounter();
                    throw e;
                }
            }
            if (close) {
                try {
                    graph.close();
                    LOGGER.info("Closed Graph instance: {}", graph);
                    graph = null;
                } catch (Exception e) {
                    LOGGER.warn("Failed to close Graph instance", e);
                }
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
        intermediateBatchSize = configuration.getLong(INTERMEDIATE_BATCH_SIZE_CFG_KEY, 0L);
        elementComputeKeys.add(configuration.getString(BULK_LOADER_VERTEX_ID_CFG_KEY, DEFAULT_BULK_LOADER_VERTEX_ID));
        bulkLoader = createBulkLoader();
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
            graph = GraphFactory.open(configuration.subset(WRITE_GRAPH_CFG_KEY));
            LOGGER.info("Opened Graph instance: {}", graph);
            try {
                listener = new BulkLoadingListener();
                g = GraphTraversalSource.build().with(EventStrategy.build().addListener(listener).create()).create(graph);
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
            this.listener.resetStats();
            // get or create the vertex
            final Vertex targetVertex = bulkLoader.getOrCreateVertex(sourceVertex, graph, g);
            // write all the properties of the vertex to the newly created vertex
            final Iterator<VertexProperty<Object>> vpi = sourceVertex.properties();
            if (this.listener.isNewVertex()) {
                vpi.forEachRemaining(vp -> bulkLoader.createVertexProperty(vp, targetVertex, graph, g));
            } else {
                vpi.forEachRemaining(vp -> bulkLoader.getOrCreateVertexProperty(vp, targetVertex, graph, g));
            }
            this.commit(false);
            if (!bulkLoader.useUserSuppliedIds()) {
                // create an id pair and send it to all the vertex's incoming adjacent vertices
                sourceVertex.property(bulkLoader.getVertexIdProperty(), targetVertex.id());
                messenger.sendMessage(messageScope, Pair.with(sourceVertex.id(), targetVertex.id()));
            }
        } else if (memory.getIteration() == 1) {
            if (bulkLoader.useUserSuppliedIds()) {
                final Vertex outV = bulkLoader.getVertex(sourceVertex, graph, g);
                final boolean incremental = outV.edges(Direction.OUT).hasNext();
                sourceVertex.edges(Direction.OUT).forEachRemaining(edge -> {
                    final Vertex inV = bulkLoader.getVertex(edge.inVertex(), graph, g);
                    if (incremental) {
                        bulkLoader.getOrCreateEdge(edge, outV, inV, graph, g);
                    } else {
                        bulkLoader.createEdge(edge, outV, inV, graph, g);
                    }
                    this.commit(false);
                });
            } else {
                // create an id map and populate it with all the incoming messages
                final Map<Object, Object> idPairs = new HashMap<>();
                final Iterator<Tuple> idi = messenger.receiveMessages();
                while (idi.hasNext()) {
                    final Tuple idPair = idi.next();
                    idPairs.put(idPair.getValue(0), idPair.getValue(1));
                }
                // get the vertex with given the dummy id property
                final Object outVId = sourceVertex.value(bulkLoader.getVertexIdProperty());
                final Vertex outV = bulkLoader.getVertexById(outVId, graph, g);
                // for all the incoming edges of the vertex, get the incoming adjacent vertex and write the edge and its properties
                sourceVertex.edges(Direction.OUT).forEachRemaining(edge -> {
                    final Object inVId = idPairs.get(edge.inVertex().id());
                    final Vertex inV = bulkLoader.getVertexById(inVId, graph, g);
                    bulkLoader.getOrCreateEdge(edge, outV, inV, graph, g);
                    this.commit(false);
                });
            }
        } else if (memory.getIteration() == 2) {
            final Object vertexId = sourceVertex.value(bulkLoader.getVertexIdProperty());
            bulkLoader.getVertexById(vertexId, graph, g)
                    .property(bulkLoader.getVertexIdProperty()).remove();
            this.commit(false);
        }
    }

    @Override
    public boolean terminate(final Memory memory) {
        switch (memory.getIteration()) {
            case 1:
                return bulkLoader.keepOriginalIds();
            case 2:
                return true;
        }
        return false;
    }

    @Override
    public Set<String> getElementComputeKeys() {
        return elementComputeKeys;
    }

    @Override
    public Set<MessageScope> getMessageScopes(final Memory memory) {
        return Collections.singleton(messageScope);
    }

    @SuppressWarnings({"CloneDoesntDeclareCloneNotSupportedException", "CloneDoesntCallSuperClone"})
    @Override
    public VertexProgram<Tuple> clone() {
        return this;
    }

    @Override
    public GraphComputer.ResultGraph getPreferredResultGraph() {
        return GraphComputer.ResultGraph.ORIGINAL;
    }

    @Override
    public GraphComputer.Persist getPreferredPersist() {
        return GraphComputer.Persist.NOTHING;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        if (bulkLoader != null) {
            sb.append("bulkLoader=").append(bulkLoader.getClass().getSimpleName()).append(",");
            sb.append("vertexIdProperty=").append(bulkLoader.getVertexIdProperty()).append(",");
            sb.append("userSuppliedIds=").append(bulkLoader.useUserSuppliedIds()).append(",");
            sb.append("keepOriginalIds=").append(bulkLoader.keepOriginalIds()).append(",");
        } else {
            sb.append("bulkLoader=").append(bulkLoader).append(",");
        }
        sb.append("batchSize=").append(intermediateBatchSize);
        return StringFactory.vertexProgramString(this, sb.toString());
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
            return (BulkLoaderVertexProgram) VertexProgram.createVertexProgram(graph, configuration);
        }

        private void setGraphConfigurationProperty(final String key, final Object value) {
            configuration.setProperty(String.join(".", WRITE_GRAPH_CFG_KEY, key), value);
        }

        /**
         * Sets the class name of the BulkLoader implementation to be used.
         */
        public Builder bulkLoader(final String className) {
            configuration.setProperty(BULK_LOADER_CLASS_CFG_KEY, className);
            return this;
        }

        /**
         * Sets the class of the BulkLoader implementation to be used.
         */
        public Builder bulkLoader(final Class<? extends BulkLoader> clazz) {
            return bulkLoader(clazz.getCanonicalName());
        }

        /**
         * Sets the name of the property that is used to store the original vertex identifiers in the target graph.
         */
        public Builder vertexIdProperty(final String name) {
            configuration.setProperty(BULK_LOADER_VERTEX_ID_CFG_KEY, name);
            return this;
        }

        /**
         * Specifies whether user supplied identifiers should be used when the bulk loader creates vertices in the
         * target graph.
         */
        public Builder userSuppliedIds(final boolean useUserSuppliedIds) {
            configuration.setProperty(USER_SUPPLIED_IDS_CFG_KEY, useUserSuppliedIds);
            return this;
        }

        /**
         * Specifies whether the original vertex identifiers should be kept in the target graph or not. In case of false
         * BulkLoaderVertexProgram will add another iteration to remove the properties and it won't be possible to use
         * the data for further incremental bulk loads.
         */
        public Builder keepOriginalIds(final boolean keepOriginalIds) {
            configuration.setProperty(KEEP_ORIGINAL_IDS_CFG_KEY, keepOriginalIds);
            return this;
        }

        /**
         * The batch size for a single transaction (number of vertices in the vertex loading stage; number of edges in
         * the edge loading stage).
         */
        public Builder intermediateBatchSize(final int batchSize) {
            configuration.setProperty(INTERMEDIATE_BATCH_SIZE_CFG_KEY, batchSize);
            return this;
        }

        /**
         * A configuration for the target graph that can be passed to GraphFactory.open().
         */
        public Builder writeGraph(final String configurationFile) throws ConfigurationException {
            return writeGraph(new PropertiesConfiguration(configurationFile));
        }

        /**
         * A configuration for the target graph that can be passed to GraphFactory.open().
         */
        public Builder writeGraph(final Configuration configuration) {
            configuration.getKeys().forEachRemaining(key -> setGraphConfigurationProperty(key, configuration.getProperty(key)));
            return this;
        }
    }

    @Override
    public Features getFeatures() {
        return new Features() {
            @Override
            public boolean requiresLocalMessageScopes() {
                return true;
            }

            @Override
            public boolean requiresVertexPropertyAddition() {
                return true;
            }
        };
    }

    static class BulkLoadingListener implements MutationListener {

        private long counter;
        private boolean isNewVertex;

        public BulkLoadingListener() {
            this.counter = 0L;
            this.isNewVertex = false;
        }

        public boolean isNewVertex() {
            return this.isNewVertex;
        }

        public long mutations() {
            return this.counter;
        }

        public void resetStats() {
            this.isNewVertex = false;
        }

        public void resetCounter() {
            this.counter = 0L;
        }

        @Override
        public void vertexAdded(final Vertex vertex) {
            this.isNewVertex = true;
            this.counter++;
        }

        @Override
        public void vertexRemoved(final Vertex vertex) {
            this.counter++;
        }

        @Override
        public void vertexPropertyChanged(final Vertex element, final Property oldValue, final Object setValue,
                                          final Object... vertexPropertyKeyValues) {
            this.counter++;
        }

        @Override
        public void vertexPropertyRemoved(final VertexProperty vertexProperty) {
            this.counter++;
        }

        @Override
        public void edgeAdded(final Edge edge) {
            this.counter++;
        }

        @Override
        public void edgeRemoved(final Edge edge) {
            this.counter++;
        }

        @Override
        public void edgePropertyChanged(final Edge element, final Property oldValue, final Object setValue) {
            this.counter++;
        }

        @Override
        public void edgePropertyRemoved(final Edge element, final Property property) {
            this.counter++;
        }

        @Override
        public void vertexPropertyPropertyChanged(final VertexProperty element, final Property oldValue, final Object setValue) {
            this.counter++;
        }

        @Override
        public void vertexPropertyPropertyRemoved(final VertexProperty element, final Property property) {
            this.counter++;
        }
    }
}
