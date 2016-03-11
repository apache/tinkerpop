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
package org.apache.tinkerpop.gremlin.process.server;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.server.traversal.strategy.ServerStrategy;
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
 * {@link ServerConnection}. This is not a full {@link Graph} implementation in the sense that the most of the methods
 * will throw an {@link UnsupportedOperationException}.  This implementation can only be used for spawning remote
 * traversal instances.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ServerGraph implements Graph {

    private final ServerConnection connection;
    private final Class<? extends Graph> graphClass;

    public static final String GREMLIN_SERVERGRAPH_SERVER_CONNECTION_CLASS = "gremlin.servergraph.serverConnectionClass";
    public static final String GREMLIN_SERVERGRAPH_GRAPH_CLASS = "gremlin.servergraph.graphClass";

    private ServerGraph(final ServerConnection connection, final Class<? extends Graph> graphClass) {
        this.connection = connection;
        this.graphClass = graphClass;TraversalStrategies.GlobalCache.registerStrategies(
                ServerGraph.class, TraversalStrategies.GlobalCache.getStrategies(EmptyGraph.class).clone().addStrategies(ServerStrategy.instance()));
    }

    /**
     * Creates a new {@link ServerGraph} instance using the specified configuration, which allows {@link ServerGraph}
     * to be compliant with {@link GraphFactory}. Expects keys for the {@link #GREMLIN_SERVERGRAPH_GRAPH_CLASS} and
     * {@link #GREMLIN_SERVERGRAPH_SERVER_CONNECTION_CLASS} as well as any configuration required by the underlying
     * {@link ServerConnection} which will be instantiated. Note that the {@code Configuration} object is passed down
     * without change to the creation of the {@link ServerConnection} instance.
     */
    public static ServerGraph open(final Configuration conf) {
        if (!conf.containsKey(GREMLIN_SERVERGRAPH_GRAPH_CLASS))
            throw new IllegalArgumentException("Configuration must contain the '" + GREMLIN_SERVERGRAPH_GRAPH_CLASS +"' key");

        if (!conf.containsKey(GREMLIN_SERVERGRAPH_SERVER_CONNECTION_CLASS))
            throw new IllegalArgumentException("Configuration must contain the '" + GREMLIN_SERVERGRAPH_SERVER_CONNECTION_CLASS +"' key");

        final ServerConnection serverConnection;
        try {
            final Class<? extends ServerConnection> clazz = Class.forName(conf.getString(GREMLIN_SERVERGRAPH_SERVER_CONNECTION_CLASS)).asSubclass(ServerConnection.class);
            final Constructor<? extends ServerConnection> ctor = clazz.getConstructor(Configuration.class);
            serverConnection = ctor.newInstance(conf);
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }

        final Class<? extends Graph> graphClazz;
        try {
            graphClazz = Class.forName(conf.getString(GREMLIN_SERVERGRAPH_GRAPH_CLASS)).asSubclass(Graph.class);
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }

        return new ServerGraph(serverConnection, graphClazz);
    }

    /**
     * Creates a new {@link ServerGraph} instance. {@link ServerGraph} will attempt to call the
     * {@link ServerConnection#close()} method when the {@link #close()} method is called on this class.
     *
     * @param connection the {@link ServerConnection} instance to use
     * @param graphClass the {@link Graph} class expected to be executed on the other side of the
     * {@link ServerConnection}
     */
    public static ServerGraph open(final ServerConnection connection, final Class<? extends Graph> graphClass) {
        return new ServerGraph(connection, graphClass);
    }

    public ServerConnection getConnection() {
        return connection;
    }

    public Class<? extends Graph> getGraphClass() {
        return graphClass;
    }

    /**
     * Closes the underlying {@link ServerConnection}.
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
