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
package org.apache.tinkerpop.gremlin.driver;

import org.apache.tinkerpop.gremlin.driver.exception.ConnectionException;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * A {@code Client} is constructed from a {@link Cluster} and represents a way to send messages to Gremlin Server.
 * This class itself is a base class as there are different implementations that provide differing kinds of
 * functionality.  See the implementations for specifics on their individual usage.
 * <p/>
 * The {@code Client} is designed to be re-used and shared across threads.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class Client {

    private static final Logger logger = LoggerFactory.getLogger(Client.class);

    protected final Cluster cluster;
    protected volatile boolean initialized;

    Client(final Cluster cluster) {
        this.cluster = cluster;
    }

    /**
     * Makes any final changes to the builder and returns the constructed {@link RequestMessage}.  Implementers
     * may choose to override this message to append data to the request before sending.  By default, this method
     * will simply call the {@link org.apache.tinkerpop.gremlin.driver.message.RequestMessage.Builder#create()} and return
     * the {@link RequestMessage}.
     */
    public RequestMessage buildMessage(final RequestMessage.Builder builder) {
        return builder.create();
    }

    /**
     * Called in the {@link #init} method.
     */
    protected abstract void initializeImplementation();

    /**
     * Chooses a {@link Connection} to write the message to.
     */
    protected abstract Connection chooseConnection(final RequestMessage msg) throws TimeoutException, ConnectionException;

    /**
     * Asynchronous close of the {@code Client}.
     */
    public abstract CompletableFuture<Void> closeAsync();

    /**
     * Create a new {@code Client} that aliases the specified {@link Graph} or {@link TraversalSource} name on the
     * server to a variable called "g" for the context of the requests made through that {@code Client}.
     *
     * @param graphOrTraversalSource rebinds the specified global Gremlin Server variable to "g"
     * @deprecated As of release 3.1.0, replaced by {@link #alias(String)}
     */
    @Deprecated
    public abstract Client rebind(final String graphOrTraversalSource);


    /**
     * Create a new {@code Client} that aliases the specified {@link Graph} or {@link TraversalSource} name on the
     * server to a variable called "g" for the context of the requests made through that {@code Client}.
     *
     * @param graphOrTraversalSource rebinds the specified global Gremlin Server variable to "g"
     */
    public abstract Client alias(final String graphOrTraversalSource);

    /**
     * Initializes the client which typically means that a connection is established to the server.  Depending on the
     * implementation and configuration this blocking call may take some time.  This method will be called
     * automatically if it is not called directly and multiple calls will not have effect.
     */
    public synchronized Client init() {
        if (initialized)
            return this;

        logger.debug("Initializing client on cluster [{}]", cluster);

        cluster.init();
        initializeImplementation();

        initialized = true;
        return this;
    }

    /**
     * Submits a Gremlin script to the server and returns a {@link ResultSet} once the write of the request is
     * complete.
     *
     * @param gremlin the gremlin script to execute
     */
    public ResultSet submit(final String gremlin) {
        return submit(gremlin, null);
    }

    /**
     * Submits a Gremlin script and bound parameters to the server and returns a {@link ResultSet} once the write of
     * the request is complete.  If a script is to be executed repeatedly with slightly different arguments, prefer
     * this method to concatenating a Gremlin script from dynamically produced strings and sending it to
     * {@link #submit(String)}.  Parameterized scripts will perform better.
     *
     * @param gremlin the gremlin script to execute
     * @param parameters a map of parameters that will be bound to the script on execution
     */
    public ResultSet submit(final String gremlin, final Map<String, Object> parameters) {
        try {
            return submitAsync(gremlin, parameters).get();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * The asynchronous version of {@link #submit(String)} where the returned future will complete when the
     * write of the request completes.
     *
     * @param gremlin the gremlin script to execute
     */
    public CompletableFuture<ResultSet> submitAsync(final String gremlin) {
        return submitAsync(gremlin, null);
    }

    /**
     * The asynchronous version of {@link #submit(String, Map)}} where the returned future will complete when the
     * write of the request completes.
     *
     * @param gremlin the gremlin script to execute
     * @param parameters a map of parameters that will be bound to the script on execution
     */
    public CompletableFuture<ResultSet> submitAsync(final String gremlin, final Map<String, Object> parameters) {
        final RequestMessage.Builder request = RequestMessage.build(Tokens.OPS_EVAL)
                .add(Tokens.ARGS_GREMLIN, gremlin)
                .add(Tokens.ARGS_BATCH_SIZE, cluster.connectionPoolSettings().resultIterationBatchSize);

        Optional.ofNullable(parameters).ifPresent(params -> request.addArg(Tokens.ARGS_BINDINGS, parameters));

        return submitAsync(buildMessage(request));
    }

    /**
     * A low-level method that allows the submission of a manually constructed {@link RequestMessage}.
     */
    public CompletableFuture<ResultSet> submitAsync(final RequestMessage msg) {
        if (!initialized)
            init();

        final CompletableFuture<ResultSet> future = new CompletableFuture<>();
        Connection connection = null;
        try {
            // the connection is returned to the pool once the response has been completed...see Connection.write()
            // the connection may be returned to the pool with the host being marked as "unavailable"
            connection = chooseConnection(msg);
            connection.write(msg, future);
            return future;
        } catch (TimeoutException toe) {
            // there was a timeout borrowing a connection
            throw new RuntimeException(toe);
        } catch (ConnectionException ce) {
            throw new RuntimeException(ce);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        } finally {
            if (logger.isDebugEnabled())
                logger.debug("Submitted {} to - {}", msg, null == connection ? "connection not initialized" : connection.toString());
        }
    }

    /**
     * Closes the client by making a synchronous call to {@link #closeAsync()}.
     */
    public void close() {
        closeAsync().join();
    }

    /**
     * A {@code Client} implementation that does not operate in a session.  Requests are sent to multiple servers
     * given a {@link LoadBalancingStrategy}.  Transactions are automatically committed
     * (or rolled-back on error) after each request.
     */
    public final static class ClusteredClient extends Client {

        private ConcurrentMap<Host, ConnectionPool> hostConnectionPools = new ConcurrentHashMap<>();

        ClusteredClient(final Cluster cluster) {
            super(cluster);
        }

        /**
         * Submits a Gremlin script to the server and returns a {@link ResultSet} once the write of the request is
         * complete.
         *
         * @param gremlin the gremlin script to execute
         */
        public ResultSet submit(final String gremlin, final String graphOrTraversalSource) {
            return submit(gremlin, graphOrTraversalSource, null);
        }

        /**
         * Submits a Gremlin script and bound parameters to the server and returns a {@link ResultSet} once the write of
         * the request is complete.  If a script is to be executed repeatedly with slightly different arguments, prefer
         * this method to concatenating a Gremlin script from dynamically produced strings and sending it to
         * {@link #submit(String)}.  Parameterized scripts will perform better.
         *
         * @param gremlin the gremlin script to execute
         * @param parameters a map of parameters that will be bound to the script on execution
         * @param graphOrTraversalSource rebinds the specified global Gremlin Server variable to "g"
         */
        public ResultSet submit(final String gremlin, final String graphOrTraversalSource, final Map<String, Object> parameters) {
            try {
                return submitAsync(gremlin, graphOrTraversalSource, parameters).get();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        /**
         * The asynchronous version of {@link #submit(String, Map)}} where the returned future will complete when the
         * write of the request completes.
         *
         * @param gremlin the gremlin script to execute
         * @param parameters a map of parameters that will be bound to the script on execution
         * @param graphOrTraversalSource rebinds the specified global Gremlin Server variable to "g"
         */
        public CompletableFuture<ResultSet> submitAsync(final String gremlin, final String graphOrTraversalSource,
                                                        final Map<String, Object> parameters) {
            final RequestMessage.Builder request = RequestMessage.build(Tokens.OPS_EVAL)
                    .add(Tokens.ARGS_GREMLIN, gremlin)
                    .add(Tokens.ARGS_BATCH_SIZE, cluster.connectionPoolSettings().resultIterationBatchSize);

            Optional.ofNullable(parameters).ifPresent(params -> request.addArg(Tokens.ARGS_BINDINGS, parameters));

            if (graphOrTraversalSource != null && !graphOrTraversalSource.isEmpty())
                request.addArg(Tokens.ARGS_ALIASES, makeRebindings(graphOrTraversalSource));

            return submitAsync(buildMessage(request));
        }

        /**
         * The asynchronous version of {@link #submit(String, Map)}} where the returned future will complete when the
         * write of the request completes.
         *
         * @param gremlin the gremlin script to execute
         * @param parameters a map of parameters that will be bound to the script on execution
         * @param aliases aliases the specified global Gremlin Server variable some other name that then be used in the
         *                script where the key is the alias name and the value represents the global variable on the
         *                server
         */
        public CompletableFuture<ResultSet> submitAsync(final String gremlin, final Map<String,String> aliases,
                                                        final Map<String, Object> parameters) {
            final RequestMessage.Builder request = RequestMessage.build(Tokens.OPS_EVAL)
                    .add(Tokens.ARGS_GREMLIN, gremlin)
                    .add(Tokens.ARGS_BATCH_SIZE, cluster.connectionPoolSettings().resultIterationBatchSize);

            Optional.ofNullable(parameters).ifPresent(params -> request.addArg(Tokens.ARGS_BINDINGS, parameters));

            if (aliases != null && !aliases.isEmpty())
                request.addArg(Tokens.ARGS_ALIASES, aliases);

            return submitAsync(buildMessage(request));
        }

        /**
         * {@inheritDoc}
         */
        @Override
        @Deprecated
        public Client rebind(final String graphOrTraversalSource) {
            return alias(graphOrTraversalSource);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Client alias(String graphOrTraversalSource) {
            return new AliasClusteredClient(this, graphOrTraversalSource);
        }

        /**
         * Creates a {@code Client} that supplies the specified set of aliases, thus allowing the user to re-name
         * one or more globally defined {@link Graph} or {@link TraversalSource} server bindings for the context of
         * the created {@code Client}.
         */
        @Deprecated
        public Client rebind(final Map<String,String> rebindings) {
            return alias(rebindings);
        }

        /**
         * Creates a {@code Client} that supplies the specified set of aliases, thus allowing the user to re-name
         * one or more globally defined {@link Graph} or {@link TraversalSource} server bindings for the context of
         * the created {@code Client}.
         */
        public Client alias(final Map<String,String> aliases) {
            return new AliasClusteredClient(this, aliases);
        }

        /**
         * Uses a {@link LoadBalancingStrategy} to choose the best {@link Host} and then selects the best connection
         * from that host's connection pool.
         */
        @Override
        protected Connection chooseConnection(final RequestMessage msg) throws TimeoutException, ConnectionException {
            final Iterator<Host> possibleHosts = this.cluster.loadBalancingStrategy().select(msg);
            if (!possibleHosts.hasNext()) throw new TimeoutException("Timed out waiting for an available host.");

            final Host bestHost = this.cluster.loadBalancingStrategy().select(msg).next();
            final ConnectionPool pool = hostConnectionPools.get(bestHost);
            return pool.borrowConnection(cluster.connectionPoolSettings().maxWaitForConnection, TimeUnit.MILLISECONDS);
        }

        /**
         * Initializes the connection pools on all hosts.
         */
        @Override
        protected void initializeImplementation() {
            cluster.allHosts().forEach(host -> {
                try {
                    // hosts that don't initialize connection pools will come up as a dead host
                    hostConnectionPools.put(host, new ConnectionPool(host, this));

                    // added a new host to the cluster so let the load-balancer know
                    this.cluster.loadBalancingStrategy().onNew(host);
                } catch (Exception ex) {
                    // catch connection errors and prevent them from failing the creation
                    logger.warn("Could not initialize connection pool for {} - will try later", host);
                }
            });
        }

        /**
         * Closes all the connection pools on all hosts.
         */
        @Override
        public CompletableFuture<Void> closeAsync() {
            final CompletableFuture[] poolCloseFutures = new CompletableFuture[hostConnectionPools.size()];
            hostConnectionPools.values().stream().map(ConnectionPool::closeAsync).collect(Collectors.toList()).toArray(poolCloseFutures);
            return CompletableFuture.allOf(poolCloseFutures);
        }

        private Map<String,String> makeRebindings(final String graphOrTraversalSource) {
            final Map<String,String> rebindings = new HashMap<>();
            rebindings.put("g", graphOrTraversalSource);
            return rebindings;
        }
    }

    /**
     * Uses a {@link org.apache.tinkerpop.gremlin.driver.Client.ClusteredClient} that rebinds requests to a
     * specified {@link Graph} or {@link TraversalSource} instances on the server-side.
     */
    public final static class AliasClusteredClient extends ReboundClusteredClient {
        public AliasClusteredClient(ClusteredClient clusteredClient, String graphOrTraversalSource) {
            super(clusteredClient, graphOrTraversalSource);
        }

        public AliasClusteredClient(ClusteredClient clusteredClient, Map<String, String> rebindings) {
            super(clusteredClient, rebindings);
        }
    }

    /**
     * Uses a {@link org.apache.tinkerpop.gremlin.driver.Client.ClusteredClient} that rebinds requests to a
     * specified {@link Graph} or {@link TraversalSource} instances on the server-side.
     *
     * @deprecated As of release 3.1.1-incubating, replaced by {@link AliasClusteredClient}.
     */
    @Deprecated
    public static class ReboundClusteredClient extends Client {
        private final ClusteredClient clusteredClient;
        private final Map<String,String> aliases = new HashMap<>();
        final CompletableFuture<Void> close = new CompletableFuture<>();

        ReboundClusteredClient(final ClusteredClient clusteredClient, final String graphOrTraversalSource) {
            super(clusteredClient.cluster);
            this.clusteredClient = clusteredClient;
            aliases.put("g", graphOrTraversalSource);
        }

        ReboundClusteredClient(final ClusteredClient clusteredClient, final Map<String,String> rebindings) {
            super(clusteredClient.cluster);
            this.clusteredClient = clusteredClient;
            this.aliases.putAll(rebindings);
        }

        @Override
        public synchronized Client init() {
            if (close.isDone()) throw new IllegalStateException("Client is closed");

            // the underlying client may not have been init'd
            clusteredClient.init();

            return this;
        }

        @Override
        public RequestMessage buildMessage(final RequestMessage.Builder builder) {
            if (close.isDone()) throw new IllegalStateException("Client is closed");
            if (!aliases.isEmpty())
                builder.addArg(Tokens.ARGS_ALIASES, aliases);

            return builder.create();
        }

        @Override
        protected void initializeImplementation() {
            // no init required
            if (close.isDone()) throw new IllegalStateException("Client is closed");
        }

        /**
         * Delegates to the underlying {@link org.apache.tinkerpop.gremlin.driver.Client.ClusteredClient}.
         */
        @Override
        protected Connection chooseConnection(final RequestMessage msg) throws TimeoutException, ConnectionException {
            if (close.isDone()) throw new IllegalStateException("Client is closed");
            return clusteredClient.chooseConnection(msg);
        }

        /**
         * Prevents messages from being sent from this {@code Client}. Note that calling this method does not call
         * close on the {@code Client} that created it.
         */
        @Override
        public CompletableFuture<Void> closeAsync() {
            close.complete(null);
            return close;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        @Deprecated
        public Client rebind(final String graphOrTraversalSource) {
            return alias(graphOrTraversalSource);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Client alias(String graphOrTraversalSource) {
            if (close.isDone()) throw new IllegalStateException("Client is closed");
            return new AliasClusteredClient(clusteredClient, graphOrTraversalSource);
        }
    }

    /**
     * A {@code Client} implementation that operates in the context of a session.  Requests are sent to a single
     * server, where each request is bound to the same thread with the same set of bindings across requests.
     * Transaction are not automatically committed. It is up the client to issue commit/rollback commands.
     */
    public final static class SessionedClient extends Client {
        private final String sessionId;

        private ConnectionPool connectionPool;

        SessionedClient(final Cluster cluster, final String sessionId) {
            super(cluster);
            this.sessionId = sessionId;
        }

        String getSessionId() {
            return sessionId;
        }

        /**
         * The sessioned client does not support this feature.
         *
         * @throws UnsupportedOperationException
         * @deprecated As of release 3.1.0, replaced by {@link #alias(String)}
         */
        @Deprecated
        @Override
        public Client rebind(final String graphOrTraversalSourceName){
            throw new UnsupportedOperationException("Sessioned client does not support aliasing");
        }

        /**
         * The sessioned client does not support this feature.
         *
         * @throws UnsupportedOperationException
         */
        @Override
        public Client alias(String graphOrTraversalSource) {
            throw new UnsupportedOperationException("Sessioned client does not support aliasing");
        }

        /**
         * Adds the {@link Tokens#ARGS_SESSION} value to every {@link RequestMessage}.
         */
        @Override
        public RequestMessage buildMessage(final RequestMessage.Builder builder) {
            builder.processor("session");
            builder.addArg(Tokens.ARGS_SESSION, sessionId);
            return builder.create();
        }

        /**
         * Since the session is bound to a single host, simply borrow a connection from that pool.
         */
        @Override
        protected Connection chooseConnection(final RequestMessage msg) throws TimeoutException, ConnectionException {
            return connectionPool.borrowConnection(cluster.connectionPoolSettings().maxWaitForConnection, TimeUnit.MILLISECONDS);
        }

        /**
         * Randomly choose an available {@link Host} to bind the session too and initialize the {@link ConnectionPool}.
         */
        @Override
        protected void initializeImplementation() {
            // chooses an available host at random
            final List<Host> hosts = cluster.allHosts()
                    .stream().filter(Host::isAvailable).collect(Collectors.toList());
            Collections.shuffle(hosts);
            final Host host = hosts.get(0);
            connectionPool = new ConnectionPool(host, this, Optional.of(1), Optional.of(1));
        }

        /**
         * Close the bound {@link ConnectionPool}.
         */
        @Override
        public CompletableFuture<Void> closeAsync() {
            return connectionPool.closeAsync();
        }
    }
}
