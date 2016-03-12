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
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.remote.traversal.strategy.RemoteStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
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
 */
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
public class RemoteGraph implements Graph {

    private final RemoteConnection connection;
    private final Class<? extends Graph> graphClass;

    public static final String GREMLIN_SERVERGRAPH_SERVER_CONNECTION_CLASS = "gremlin.servergraph.serverConnectionClass";
    public static final String GREMLIN_SERVERGRAPH_GRAPH_CLASS = "gremlin.servergraph.graphClass";

    private RemoteGraph(final RemoteConnection connection, final Class<? extends Graph> graphClass) {
        this.connection = connection;
        this.graphClass = graphClass;TraversalStrategies.GlobalCache.registerStrategies(
                RemoteGraph.class, TraversalStrategies.GlobalCache.getStrategies(EmptyGraph.class).clone().addStrategies(RemoteStrategy.instance()));
    }

    /**
     * Creates a new {@link RemoteGraph} instance using the specified configuration, which allows {@link RemoteGraph}
     * to be compliant with {@link GraphFactory}. Expects keys for the {@link #GREMLIN_SERVERGRAPH_GRAPH_CLASS} and
     * {@link #GREMLIN_SERVERGRAPH_SERVER_CONNECTION_CLASS} as well as any configuration required by the underlying
     * {@link RemoteConnection} which will be instantiated. Note that the {@code Configuration} object is passed down
     * without change to the creation of the {@link RemoteConnection} instance.
     */
    public static RemoteGraph open(final Configuration conf) {
        if (!conf.containsKey(GREMLIN_SERVERGRAPH_GRAPH_CLASS))
            throw new IllegalArgumentException("Configuration must contain the '" + GREMLIN_SERVERGRAPH_GRAPH_CLASS +"' key");

        if (!conf.containsKey(GREMLIN_SERVERGRAPH_SERVER_CONNECTION_CLASS))
            throw new IllegalArgumentException("Configuration must contain the '" + GREMLIN_SERVERGRAPH_SERVER_CONNECTION_CLASS +"' key");

        final RemoteConnection remoteConnection;
        try {
            final Class<? extends RemoteConnection> clazz = Class.forName(conf.getString(GREMLIN_SERVERGRAPH_SERVER_CONNECTION_CLASS)).asSubclass(RemoteConnection.class);
            final Constructor<? extends RemoteConnection> ctor = clazz.getConstructor(Configuration.class);
            remoteConnection = ctor.newInstance(conf);
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }

        final Class<? extends Graph> graphClazz;
        try {
            graphClazz = Class.forName(conf.getString(GREMLIN_SERVERGRAPH_GRAPH_CLASS)).asSubclass(Graph.class);
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }

        return new RemoteGraph(remoteConnection, graphClazz);
    }

    /**
     * Creates a new {@link RemoteGraph} instance. {@link RemoteGraph} will attempt to call the
     * {@link RemoteConnection#close()} method when the {@link #close()} method is called on this class.
     *
     * @param connection the {@link RemoteConnection} instance to use
     * @param graphClass the {@link Graph} class expected to be executed on the other side of the
     * {@link RemoteConnection}
     */
    public static RemoteGraph open(final RemoteConnection connection, final Class<? extends Graph> graphClass) {
        return new RemoteGraph(connection, graphClass);
    }

    public RemoteConnection getConnection() {
        return connection;
    }

    public Class<? extends Graph> getGraphClass() {
        return graphClass;
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
        throw new UnsupportedOperationException(String.format("ServerGraph is a proxy to %s - this method is not supported", connection));
    }

    @Override
    public <C extends GraphComputer> C compute(final Class<C> graphComputerClass) throws IllegalArgumentException {
        throw new UnsupportedOperationException(String.format("ServerGraph is a proxy to %s - this method is not supported", connection));
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        throw new UnsupportedOperationException(String.format("ServerGraph is a proxy to %s - this method is not supported", connection));
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
        throw new UnsupportedOperationException(String.format("ServerGraph is a proxy to %s - this method is not supported", connection));
    }

    @Override
    public Variables variables() {
        throw new UnsupportedOperationException(String.format("ServerGraph is a proxy to %s - this method is not supported", connection));
    }

    @Override
    public Configuration configuration() {
        throw new UnsupportedOperationException(String.format("ServerGraph is a proxy to %s - this method is not supported", connection));
    }
}
