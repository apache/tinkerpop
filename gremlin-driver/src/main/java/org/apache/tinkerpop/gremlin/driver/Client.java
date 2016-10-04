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
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
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
    protected final Client.Settings settings;

    Client(final Cluster cluster, final Client.Settings settings) {
        this.cluster = cluster;
        this.settings = settings;
    }

    /**
     * Makes any final changes to the builder and returns the constructed {@link RequestMessage}.  Implementers
     * may choose to override this message to append data to the request before sending.  By default, this method
     * will simply return the {@code builder} passed in by the caller.
     */
    public RequestMessage.Builder buildMessage(final RequestMessage.Builder builder) {
        return builder;
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
    public Client rebind(final String graphOrTraversalSource) {
        return alias(graphOrTraversalSource);
    }

    /**
     * Create a new {@code Client} that aliases the specified {@link Graph} or {@link TraversalSource} name on the
     * server to a variable called "g" for the context of the requests made through that {@code Client}.
     *
     * @param graphOrTraversalSource rebinds the specified global Gremlin Server variable to "g"
     */
    public Client alias(final String graphOrTraversalSource) {
        return alias(makeDefaultAliasMap(graphOrTraversalSource));
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
        return new AliasClusteredClient(this, aliases, settings);
    }

    /**
     * Submit a {@link Traversal} to the server for remote execution.
     */
    public ResultSet submit(final Traversal traversal) {
        try {
            return submitAsync(traversal).get();
        } catch (UnsupportedOperationException uoe) {
            throw uoe;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * An asynchronous version of {@link #submit(Traversal)}.
     */
    public CompletableFuture<ResultSet> submitAsync(final Traversal traversal) {
        throw new UnsupportedOperationException("This implementation does not support Traversal submission - use a sessionless Client created with from the alias() method");
    }

    /**
     * Submit a {@link Bytecode} to the server for remote execution.
     */
    public ResultSet submit(final Bytecode bytecode) {
        try {
            return submitAsync(bytecode).get();
        } catch (UnsupportedOperationException uoe) {
            throw uoe;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * An asynchronous version of {@link #submit(Traversal)}.
     */
    public CompletableFuture<ResultSet> submitAsync(final Bytecode bytecode) {
        throw new UnsupportedOperationException("This implementation does not support Traversal submission - use a sessionless Client created with from the alias() method");
    }

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

        return submitAsync(buildMessage(request).create());
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
            request.addArg(Tokens.ARGS_ALIASES, makeDefaultAliasMap(graphOrTraversalSource));

        return submitAsync(buildMessage(request).create());
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

        return submitAsync(buildMessage(request).create());
    }

    /**
     * A low-level method that allows the submission of a manually constructed {@link RequestMessage}.
     */
    public CompletableFuture<ResultSet> submitAsync(final RequestMessage msg) {
        if (isClosing()) throw new IllegalStateException("Client has been closed");

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

    public abstract boolean isClosing();

    /**
     * Closes the client by making a synchronous call to {@link #closeAsync()}.
     */
    public void close() {
        closeAsync().join();
    }

    /**
     * Gets the {@link Client.Settings}.
     */
    public Settings getSettings() {
        return settings;
    }

    /**
     * Gets the {@link Cluster} that spawned this {@code Client}.
     */
    public Cluster getCluster() {
        return cluster;
    }

    protected Map<String,String> makeDefaultAliasMap(final String graphOrTraversalSource) {
        final Map<String,String> aliases = new HashMap<>();
        aliases.put("g", graphOrTraversalSource);
        return aliases;
    }

    /**
     * A {@code Client} implementation that does not operate in a session.  Requests are sent to multiple servers
     * given a {@link LoadBalancingStrategy}.  Transactions are automatically committed
     * (or rolled-back on error) after each request.
     */
    public final static class ClusteredClient extends Client {

        private ConcurrentMap<Host, ConnectionPool> hostConnectionPools = new ConcurrentHashMap<>();
        private final AtomicReference<CompletableFuture<Void>> closing = new AtomicReference<>(null);

        ClusteredClient(final Cluster cluster, final Client.Settings settings) {
            super(cluster, settings);
        }

        @Override
        public boolean isClosing() {
            return closing.get() != null;
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
                request.addArg(Tokens.ARGS_ALIASES, makeDefaultAliasMap(graphOrTraversalSource));

            return submitAsync(buildMessage(request).create());
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

            return submitAsync(buildMessage(request).create());
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
        public Client alias(final String graphOrTraversalSource) {
            final Map<String,String> aliases = new HashMap<>();
            aliases.put("g", graphOrTraversalSource);
            return alias(aliases);
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
         * {@inheritDoc}
         */
        @Override
        public Client alias(final Map<String,String> aliases) {
            return new AliasClusteredClient(this, aliases, settings);
        }

        /**
         * Uses a {@link LoadBalancingStrategy} to choose the best {@link Host} and then selects the best connection
         * from that host's connection pool.
         */
        @Override
        protected Connection chooseConnection(final RequestMessage msg) throws TimeoutException, ConnectionException {
            final Iterator<Host> possibleHosts;
            if (msg.optionalArgs(Tokens.ARGS_HOST).isPresent()) {
                // TODO: not sure what should be done if unavailable - select new host and re-submit traversal?
                final Host host = (Host) msg.getArgs().get(Tokens.ARGS_HOST);
                msg.getArgs().remove(Tokens.ARGS_HOST);
                possibleHosts = IteratorUtils.of(host);
            } else {
                possibleHosts = this.cluster.loadBalancingStrategy().select(msg);
            }

            // you can get no possible hosts in more than a few situations. perhaps the servers are just all down.
            // or perhaps the client is not configured properly (disables ssl when ssl is enabled on the server).
            if (!possibleHosts.hasNext())
                throw new TimeoutException("Timed out while waiting for an available host - check the client configuration and connectivity to the server if this message persists");

            final Host bestHost = possibleHosts.next();
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
        public synchronized CompletableFuture<Void> closeAsync() {
            if (closing.get() != null)
                return closing.get();

            final CompletableFuture[] poolCloseFutures = new CompletableFuture[hostConnectionPools.size()];
            hostConnectionPools.values().stream().map(ConnectionPool::closeAsync).collect(Collectors.toList()).toArray(poolCloseFutures);
            closing.set(CompletableFuture.allOf(poolCloseFutures));
            return closing.get();
        }
    }

    /**
     * Uses a {@link org.apache.tinkerpop.gremlin.driver.Client.ClusteredClient} that rebinds requests to a
     * specified {@link Graph} or {@link TraversalSource} instances on the server-side.
     */
    public final static class AliasClusteredClient extends ReboundClusteredClient {
        public AliasClusteredClient(final Client client, final Map<String, String> rebindings,
                                    final Client.Settings settings) {
            super(client, rebindings, settings);
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
        private final Client client;
        private final Map<String,String> aliases = new HashMap<>();
        final CompletableFuture<Void> close = new CompletableFuture<>();

        ReboundClusteredClient(final Client client, final Map<String,String> rebindings,
                               final Client.Settings settings) {
            super(client.cluster, settings);
            this.client = client;
            this.aliases.putAll(rebindings);
        }

        @Override
        public CompletableFuture<ResultSet> submitAsync(final Bytecode bytecode) {
            try {
                return submitAsync(buildMessage(RequestMessage.build(Tokens.OPS_BYTECODE)
                        .processor("traversal").addArg(Tokens.ARGS_GREMLIN, bytecode)).create());
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public CompletableFuture<ResultSet> submitAsync(final RequestMessage msg) {
            final RequestMessage.Builder builder = RequestMessage.from(msg);
            if (!aliases.isEmpty())
                builder.addArg(Tokens.ARGS_ALIASES, aliases);

            return super.submitAsync(builder.create());
        }

        @Override
        public CompletableFuture<ResultSet> submitAsync(final Traversal traversal) {
            return submitAsync(traversal.asAdmin().getBytecode());
        }

        @Override
        public synchronized Client init() {
            if (close.isDone()) throw new IllegalStateException("Client is closed");

            // the underlying client may not have been init'd
            client.init();

            return this;
        }

        @Override
        public RequestMessage.Builder buildMessage(final RequestMessage.Builder builder) {
            if (close.isDone()) throw new IllegalStateException("Client is closed");
            if (!aliases.isEmpty())
                builder.addArg(Tokens.ARGS_ALIASES, aliases);

            return client.buildMessage(builder);
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
            return client.chooseConnection(msg);
        }

        /**
         * Prevents messages from being sent from this {@code Client}. Note that calling this method does not call
         * close on the {@code Client} that created it.
         */
        @Override
        public synchronized CompletableFuture<Void> closeAsync() {
            close.complete(null);
            return close;
        }

        @Override
        public boolean isClosing() {
            return close.isDone();
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
        public Client alias(final Map<String, String> aliases) {
            if (close.isDone()) throw new IllegalStateException("Client is closed");
            return new AliasClusteredClient(client, aliases, settings);
        }
    }

    /**
     * A {@code Client} implementation that operates in the context of a session.  Requests are sent to a single
     * server, where each request is bound to the same thread with the same set of bindings across requests.
     * Transaction are not automatically committed. It is up the client to issue commit/rollback commands.
     */
    public final static class SessionedClient extends Client {
        private final String sessionId;
        private final boolean manageTransactions;

        private ConnectionPool connectionPool;

        private final AtomicReference<CompletableFuture<Void>> closing = new AtomicReference<>(null);

        SessionedClient(final Cluster cluster, final Client.Settings settings) {
            super(cluster, settings);
            this.sessionId = settings.getSession().get().sessionId;
            this.manageTransactions = settings.getSession().get().manageTransactions;
        }

        String getSessionId() {
            return sessionId;
        }

        /**
         * Adds the {@link Tokens#ARGS_SESSION} value to every {@link RequestMessage}.
         */
        @Override
        public RequestMessage.Builder buildMessage(final RequestMessage.Builder builder) {
            builder.processor("session");
            builder.addArg(Tokens.ARGS_SESSION, sessionId);
            builder.addArg(Tokens.ARGS_MANAGE_TRANSACTION, manageTransactions);
            return builder;
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
            if (hosts.isEmpty()) throw new IllegalStateException("No available host in the cluster");
            Collections.shuffle(hosts);
            final Host host = hosts.get(0);
            connectionPool = new ConnectionPool(host, this, Optional.of(1), Optional.of(1));
        }

        @Override
        public boolean isClosing() {
            return closing.get() != null;
        }

        /**
         * Close the bound {@link ConnectionPool}.
         */
        @Override
        public synchronized CompletableFuture<Void> closeAsync() {
            if (closing.get() != null)
                return closing.get();

            final CompletableFuture<Void> connectionPoolClose = connectionPool.closeAsync();
            closing.set(connectionPoolClose);
            return connectionPoolClose;
        }
    }

    /**
     * Settings given to {@link Cluster#connect(Settings)} that configures how a {@link Client} will behave.
     */
    public static class Settings {
        private final Optional<SessionSettings> session;

        private Settings(final Builder builder) {
            this.session = builder.session;
        }

        public static Builder build() {
            return new Builder();
        }

        /**
         * Determines if the {@link Client} is to be constructed with a session. If the value is present, then a
         * session is expected.
         */
        public Optional<SessionSettings> getSession() {
            return session;
        }

        public static class Builder {
            private Optional<SessionSettings> session = Optional.empty();

            private Builder() {}

            /**
             * Enables a session. By default this will create a random session name and configure transactions to be
             * unmanaged. This method will override settings provided by calls to the other overloads of
             * {@code useSession}.
             */
            public Builder useSession(final boolean enabled) {
                session = enabled ? Optional.of(SessionSettings.build().create()) : Optional.empty();
                return this;
            }

            /**
             * Enables a session. By default this will create a session with the provided name and configure
             * transactions to be unmanaged. This method will override settings provided by calls to the other
             * overloads of {@code useSession}.
             */
            public Builder useSession(final String sessionId) {
                session = sessionId != null && !sessionId.isEmpty() ?
                        Optional.of(SessionSettings.build().sessionId(sessionId).create()) : Optional.empty();
                return this;
            }

            /**
             * Enables a session. This method will override settings provided by calls to the other overloads of
             * {@code useSession}.
             */
            public Builder useSession(final SessionSettings settings) {
                session = Optional.ofNullable(settings);
                return this;
            }

            public Settings create() {
                return new Settings(this);
            }

        }
    }

    /**
     * Settings for a {@link Client} that involve a session.
     */
    public static class SessionSettings {
        private final boolean manageTransactions;
        private final String sessionId;

        private SessionSettings(final Builder builder) {
            manageTransactions = builder.manageTransactions;
            sessionId = builder.sessionId;
        }

        /**
         * If enabled, transactions will be "managed" such that each request will represent a complete transaction.
         */
        public boolean manageTransactions() {
            return manageTransactions;
        }

        /**
         * Provides the identifier of the session.
         */
        public String getSessionId() {
            return sessionId;
        }

        public static SessionSettings.Builder build() {
            return new SessionSettings.Builder();
        }

        public static class Builder {
            private boolean manageTransactions = false;
            private String sessionId = UUID.randomUUID().toString();

            private Builder() {}

            /**
             * If enabled, transactions will be "managed" such that each request will represent a complete transaction.
             * By default this value is {@code false}.
             */
            public Builder manageTransactions(final boolean manage) {
                manageTransactions = manage;
                return this;
            }

            /**
             * Provides the identifier of the session. This value cannot be null or empty. By default it is set to
             * a random {@code UUID}.
             */
            public Builder sessionId(final String sessionId) {
                if (null == sessionId || sessionId.isEmpty())
                    throw new IllegalArgumentException("sessionId cannot be null or empty");
                this.sessionId = sessionId;
                return this;
            }

            public SessionSettings create() {
                return new SessionSettings(this);
            }
        }
    }
}
