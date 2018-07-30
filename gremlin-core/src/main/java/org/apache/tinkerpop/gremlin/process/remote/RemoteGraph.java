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
package org.apache.tinkerpop.gremlin.process.remote;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.remote.traversal.strategy.decoration.RemoteStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.Iterator;

/**
 * A {@code ServerGraph} represents a proxy by which traversals spawned from this graph are expected over a
 * {@link RemoteConnection}. This is not a full {@link Graph} implementation in the sense that the most of the methods
 * will throw an {@link UnsupportedOperationException}.  This implementation can only be used for spawning remote
 * traversal instances.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @deprecated As for release 3.2.2, replaced by {@link TraversalSource#withRemote(RemoteConnection)} or its overloads.
 */
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_COMPUTER)
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ConnectedComponentTest",
        method = "*",
        reason = "https://issues.apache.org/jira/browse/TINKERPOP-1976")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.CoreTraversalTest",
        method = "*",
        reason = "The test suite does not support profiling or lambdas and for groovy tests: 'Could not locate method: GraphTraversalSource.withStrategies([{traversalCategory=interface org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy$DecorationStrategy}])'")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProgramTest",
        method = "*",
        reason = "RemoteGraph retrieves detached vertices that can't be attached to a remote OLAP graph")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategyProcessTest",
        method = "*",
        reason = "RemoteGraph does not support ElementIdStrategy at this time - it requires a lambda in construction which is not serializable")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategyProcessTest",
        method = "*",
        reason = "RemoteGraph does not support EventStrategy at this time - some of its members are not serializable")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.PartitionStrategyProcessTest",
        method = "*",
        reason = "RemoteGraph does not support PartitionStrategy at this time")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.computer.ranking.pagerank.PageRankVertexProgramTest",
        method = "*",
        reason = "RemoteGraph does not support direct Graph.compute() access")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.computer.bulkloading.BulkLoaderVertexProgramTest",
        method = "*",
        reason = "RemoteGraph does not support direct Graph.compute() access")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.computer.bulkdumping.BulkDumperVertexProgramTest",
        method = "*",
        reason = "RemoteGraph does not support direct Graph.compute() access")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.computer.clone.CloneVertexProgramTest",
        method = "*",
        reason = "RemoteGraph does not support direct Graph.compute() access")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.computer.GraphComputerTest",
        method = "*",
        reason = "RemoteGraph does not support direct Graph.compute() access")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.TraversalInterruptionTest",
        method = "*",
        reason = "The interruption model in the test can't guarantee interruption at the right time with RemoteGraph.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.TraversalInterruptionComputerTest",
        method = "*",
        reason = "The interruption model in the test can't guarantee interruption at the right time with RemoteGraph.")
@Deprecated
public class RemoteGraph implements Graph {

    private final RemoteConnection connection;

    public static final String GREMLIN_REMOTE_GRAPH_REMOTE_CONNECTION_CLASS = "gremlin.remoteGraph.remoteConnectionClass";

    static {
        TraversalStrategies.GlobalCache.registerStrategies(RemoteGraph.class, TraversalStrategies.GlobalCache.getStrategies(EmptyGraph.class).clone().addStrategies(RemoteStrategy.instance()));
    }

    private RemoteGraph(final RemoteConnection connection) {
        this.connection = connection;
    }

    /**
     * Creates a new {@link RemoteGraph} instance using the specified configuration, which allows {@link RemoteGraph}
     * to be compliant with {@link GraphFactory}. Expects key for {@link #GREMLIN_REMOTE_GRAPH_REMOTE_CONNECTION_CLASS}
     * as well as any configuration required by the underlying {@link RemoteConnection} which will be instantiated.
     * Note that the {@code Configuration} object is passed down without change to the creation of the
     * {@link RemoteConnection} instance.
     */
    public static RemoteGraph open(final Configuration conf) {
        if (!conf.containsKey(GREMLIN_REMOTE_GRAPH_REMOTE_CONNECTION_CLASS))
            throw new IllegalArgumentException("Configuration must contain the '" + GREMLIN_REMOTE_GRAPH_REMOTE_CONNECTION_CLASS + "' key");

        final RemoteConnection remoteConnection;
        try {
            final Class<? extends RemoteConnection> clazz = Class.forName(conf.getString(GREMLIN_REMOTE_GRAPH_REMOTE_CONNECTION_CLASS)).asSubclass(RemoteConnection.class);
            final Constructor<? extends RemoteConnection> ctor = clazz.getConstructor(Configuration.class);
            remoteConnection = ctor.newInstance(conf);
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }

        return new RemoteGraph(remoteConnection);
    }

    public static RemoteGraph open(final String configFile) throws Exception {
        return open(new PropertiesConfiguration(configFile));
    }

    /**
     * Creates a new {@link RemoteGraph} instance. {@link RemoteGraph} will attempt to call the
     * {@link RemoteConnection#close()} method when the {@link #close()} method is called on this class.
     *
     * @param connection the {@link RemoteConnection} instance to use
     *                   {@link RemoteConnection}
     */
    public static RemoteGraph open(final RemoteConnection connection) {
        return new RemoteGraph(connection);
    }

    public RemoteConnection getConnection() {
        return connection;
    }

    /**
     * Closes the underlying {@link RemoteConnection}.
     */
    @Override
    public void close() throws Exception {
        connection.close();
    }

    @Override
    public Vertex addVertex(final Object... keyValues) {
        throw new UnsupportedOperationException(String.format("RemoteGraph is a proxy to %s - this method is not supported", connection));
    }

    @Override
    public <C extends GraphComputer> C compute(final Class<C> graphComputerClass) throws IllegalArgumentException {
        throw new UnsupportedOperationException(String.format("RemoteGraph is a proxy to %s - this method is not supported", connection));
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        throw new UnsupportedOperationException(String.format("RemoteGraph is a proxy to %s - this method is not supported", connection));
    }

    /**
     * This method returns an empty {@link Iterator} - it is not meant to be called directly.
     */
    @Override
    public Iterator<Vertex> vertices(final Object... vertexIds) {
        return Collections.emptyIterator();
    }

    /**
     * This method returns an empty {@link Iterator} - it is not meant to be called directly.
     */
    @Override
    public Iterator<Edge> edges(final Object... edgeIds) {
        return Collections.emptyIterator();
    }

    @Override
    public Transaction tx() {
        throw new UnsupportedOperationException(String.format("RemoteGraph is a proxy to %s - this method is not supported", connection));
    }

    @Override
    public Variables variables() {
        throw new UnsupportedOperationException(String.format("RemoteGraph is a proxy to %s - this method is not supported", connection));
    }

    @Override
    public Configuration configuration() {
        throw new UnsupportedOperationException(String.format("RemoteGraph is a proxy to %s - this method is not supported", connection));
    }

    @Override
    public Features features() {
        return RemoteFeatures.INSTANCE;
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, connection.toString());
    }

    public static class RemoteFeatures implements Features {
        static RemoteFeatures INSTANCE = new RemoteFeatures();

        private RemoteFeatures() {
        }

        @Override
        public GraphFeatures graph() {
            return RemoteGraphFeatures.INSTANCE;
        }
    }

    public static class RemoteGraphFeatures implements Features.GraphFeatures {

        static RemoteGraphFeatures INSTANCE = new RemoteGraphFeatures();

        private RemoteGraphFeatures() {
        }

        @Override
        public boolean supportsTransactions() {
            return false;
        }

        @Override
        public boolean supportsThreadedTransactions() {
            return false;
        }
    }
}
