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

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.tinkerpop.gremlin.driver.exception.ConnectionException;
import org.apache.tinkerpop.gremlin.driver.exception.NoHostAvailableException;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
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
    protected Throwable initializationFailure = null;

    Client(final Cluster cluster, final Client.Settings settings) {
        this.cluster = cluster;
        this.settings = settings;
    }

    /**
     * Makes any initial changes to the builder and returns the constructed {@link RequestMessage}.  Implementers
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
     */
    public Client alias(final String graphOrTraversalSource) {
        return alias(makeDefaultAliasMap(graphOrTraversalSource));
    }

    /**
     * Creates a {@code Client} that supplies the specified set of aliases, thus allowing the user to re-name
     * one or more globally defined {@link Graph} or {@link TraversalSource} server bindings for the context of
     * the created {@code Client}.
     */
    public Client alias(final Map<String, String> aliases) {
        return new AliasClusteredClient(this, aliases, settings);
    }

    /**
     * Submit a {@link Traversal} to the server for remote execution.Results are returned as {@link Traverser}
     * instances and are therefore bulked, meaning that to properly iterate the contents of the result each
     * {@link Traverser#bulk()} must be examined to determine the number of times that object should be presented in
     * iteration.
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
     * An asynchronous version of {@link #submit(Traversal)}. Results are returned as {@link Traverser} instances and
     * are therefore bulked, meaning that to properly iterate the contents of the result each {@link Traverser#bulk()}
     * must be examined to determine the number of times that object should be presented in iteration.
     */
    public CompletableFuture<ResultSet> submitAsync(final Traversal traversal) {
        throw new UnsupportedOperationException("This implementation does not support Traversal submission - use a sessionless Client created with from the alias() method");
    }

    /**
     * Submit a {@link Bytecode} to the server for remote execution. Results are returned as {@link Traverser}
     * instances and are therefore bulked, meaning that to properly iterate the contents of the result each
     * {@link Traverser#bulk()} must be examined to determine the number of times that object should be presented in
     * iteration.
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
     * A version of {@link #submit(Bytecode)} which provides the ability to set per-request options.
     *
     * @param bytecode request in the form of gremlin {@link Bytecode}
     * @param options  for the request
     * @see #submit(Bytecode)
     */
    public ResultSet submit(final Bytecode bytecode, final RequestOptions options) {
        try {
            return submitAsync(bytecode, options).get();
        } catch (UnsupportedOperationException uoe) {
            throw uoe;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * An asynchronous version of {@link #submit(Traversal)}. Results are returned as {@link Traverser} instances and
     * are therefore bulked, meaning that to properly iterate the contents of the result each {@link Traverser#bulk()}
     * must be examined to determine the number of times that object should be presented in iteration.
     */
    public CompletableFuture<ResultSet> submitAsync(final Bytecode bytecode) {
        throw new UnsupportedOperationException("This implementation does not support Traversal submission - use a sessionless Client created with from the alias() method");
    }

    /**
     * A version of {@link #submit(Bytecode)} which provides the ability to set per-request options.
     *
     * @param bytecode request in the form of gremlin {@link Bytecode}
     * @param options  for the request
     * @see #submitAsync(Bytecode)
     */
    public CompletableFuture<ResultSet> submitAsync(final Bytecode bytecode, final RequestOptions options) {
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

        // throw an error if no host is available even after initialization is complete.
        if (cluster.availableHosts().isEmpty()) {
            if (this.initializationFailure != null) {
                throw new NoHostAvailableException(this.initializationFailure);
            } else {
                throw new NoHostAvailableException();
            }
        }

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
        return submit(gremlin, RequestOptions.EMPTY);
    }

    /**
     * Submits a Gremlin script and bound parameters to the server and returns a {@link ResultSet} once the write of
     * the request is complete.  If a script is to be executed repeatedly with slightly different arguments, prefer
     * this method to concatenating a Gremlin script from dynamically produced strings and sending it to
     * {@link #submit(String)}.  Parameterized scripts will perform better.
     *
     * @param gremlin    the gremlin script to execute
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
     * Submits a Gremlin script to the server and returns a {@link ResultSet} once the write of the request is
     * complete.
     *
     * @param gremlin the gremlin script to execute
     * @param options for the request
     */
    public ResultSet submit(final String gremlin, final RequestOptions options) {
        try {
            return submitAsync(gremlin, options).get();
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
        return submitAsync(gremlin, RequestOptions.build().create());
    }

    /**
     * The asynchronous version of {@link #submit(String, Map)}} where the returned future will complete when the
     * write of the request completes.
     *
     * @param gremlin    the gremlin script to execute
     * @param parameters a map of parameters that will be bound to the script on execution
     */
    public CompletableFuture<ResultSet> submitAsync(final String gremlin, final Map<String, Object> parameters) {
        final RequestOptions.Builder options = RequestOptions.build();
        if (parameters != null && !parameters.isEmpty()) {
            parameters.forEach(options::addParameter);
        }

        return submitAsync(gremlin, options.create());
    }

    /**
     * The asynchronous version of {@link #submit(String, Map)}} where the returned future will complete when the
     * write of the request completes.
     *
     * @param gremlin                the gremlin script to execute
     * @param parameters             a map of parameters that will be bound to the script on execution
     * @param graphOrTraversalSource rebinds the specified global Gremlin Server variable to "g"
     * @deprecated As of release 3.4.0, replaced by {@link #submitAsync(String, RequestOptions)}.
     */
    @Deprecated
    public CompletableFuture<ResultSet> submitAsync(final String gremlin, final String graphOrTraversalSource,
                                                    final Map<String, Object> parameters) {
        Map<String, String> aliases = null;
        if (graphOrTraversalSource != null && !graphOrTraversalSource.isEmpty()) {
            aliases = makeDefaultAliasMap(graphOrTraversalSource);
        }

        return submitAsync(gremlin, aliases, parameters);
    }

    /**
     * The asynchronous version of {@link #submit(String, Map)}} where the returned future will complete when the
     * write of the request completes.
     *
     * @param gremlin    the gremlin script to execute
     * @param parameters a map of parameters that will be bound to the script on execution
     * @param aliases    aliases the specified global Gremlin Server variable some other name that then be used in the
     *                   script where the key is the alias name and the value represents the global variable on the
     *                   server
     * @deprecated As of release 3.4.0, replaced by {@link #submitAsync(String, RequestOptions)}.
     */
    @Deprecated
    public CompletableFuture<ResultSet> submitAsync(final String gremlin, final Map<String, String> aliases,
                                                    final Map<String, Object> parameters) {
        final RequestOptions.Builder options = RequestOptions.build();
        if (aliases != null && !aliases.isEmpty()) {
            aliases.forEach(options::addAlias);
        }

        if (parameters != null && !parameters.isEmpty()) {
            parameters.forEach(options::addParameter);
        }

        options.batchSize(cluster.connectionPoolSettings().resultIterationBatchSize);

        return submitAsync(gremlin, options.create());
    }

    /**
     * The asynchronous version of {@link #submit(String, RequestOptions)}} where the returned future will complete when the
     * write of the request completes.
     *
     * @param gremlin the gremlin script to execute
     * @param options the options to supply for this request
     */
    public CompletableFuture<ResultSet> submitAsync(final String gremlin, final RequestOptions options) {
        final int batchSize = options.getBatchSize().orElse(cluster.connectionPoolSettings().resultIterationBatchSize);

        // need to call buildMessage() right away to get client specific configurations, that way request specific
        // ones can override as needed
        final RequestMessage.Builder request = buildMessage(RequestMessage.build(Tokens.OPS_EVAL))
                .add(Tokens.ARGS_GREMLIN, gremlin)
                .add(Tokens.ARGS_BATCH_SIZE, batchSize);

        // apply settings if they were made available
        options.getTimeout().ifPresent(timeout -> request.add(Tokens.ARGS_SCRIPT_EVAL_TIMEOUT, timeout));
        options.getParameters().ifPresent(params -> request.addArg(Tokens.ARGS_BINDINGS, params));
        options.getAliases().ifPresent(aliases -> request.addArg(Tokens.ARGS_ALIASES, aliases));
        options.getOverrideRequestId().ifPresent(request::overrideRequestId);
        options.getUserAgent().ifPresent(userAgent -> request.addArg(Tokens.ARGS_USER_AGENT, userAgent));

        return submitAsync(request.create());
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

    protected Map<String, String> makeDefaultAliasMap(final String graphOrTraversalSource) {
        final Map<String, String> aliases = new HashMap<>();
        aliases.put("g", graphOrTraversalSource);
        return aliases;
    }

    /**
     * A {@code Client} implementation that does not operate in a session.  Requests are sent to multiple servers
     * given a {@link LoadBalancingStrategy}.  Transactions are automatically committed
     * (or rolled-back on error) after each request.
     */
    public final static class ClusteredClient extends Client {

        protected ConcurrentMap<Host, ConnectionPool> hostConnectionPools = new ConcurrentHashMap<>();
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
         * @param gremlin                the gremlin script to execute
         * @param parameters             a map of parameters that will be bound to the script on execution
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
         * {@inheritDoc}
         */
        @Override
        public Client alias(final String graphOrTraversalSource) {
            final Map<String, String> aliases = new HashMap<>();
            aliases.put("g", graphOrTraversalSource);
            return alias(aliases);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Client alias(final Map<String, String> aliases) {
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
            if (!possibleHosts.hasNext()) {
                if (this.initializationFailure != null) {
                    throw new NoHostAvailableException(this.initializationFailure);
                } else {
                    throw new NoHostAvailableException();
                }
            }

            final Host bestHost = possibleHosts.next();
            final ConnectionPool pool = hostConnectionPools.get(bestHost);
            return pool.borrowConnection(cluster.connectionPoolSettings().maxWaitForConnection, TimeUnit.MILLISECONDS);
        }

        /**
         * Initializes the connection pools on all hosts.
         */
        @Override
        protected void initializeImplementation() {
            // use a special executor here to initialize the Host instances as the worker thread pool may be
            // insufficiently sized for this task and the parallel initialization of the ConnectionPool. if too small
            // tasks may be schedule in such a way as to produce a deadlock: TINKERPOP-2550
            //
            // the cost of this single threaded executor here should be fairly small because it is only used once at
            // initialization and shutdown. since users will typically construct a Client once for the life of their
            // application there shouldn't be tons of thread pools being created and destroyed.
            final BasicThreadFactory threadFactory = new BasicThreadFactory.Builder().namingPattern("gremlin-driver-initializer").build();
            final ExecutorService hostExecutor = Executors.newSingleThreadExecutor(threadFactory);

            try {
                CompletableFuture.allOf(cluster.allHosts().stream()
                        .map(host -> CompletableFuture.runAsync(() -> initializeConnectionSetupForHost.accept(host), hostExecutor))
                        .toArray(CompletableFuture[]::new))
                        .join();
            } catch (CompletionException ex) {
                this.initializationFailure = ExceptionUtils.getRootCause(ex) != null ? ExceptionUtils.getRootCause(ex) : ex;
                logger.error("", this.initializationFailure);
            } finally {
                hostExecutor.shutdown();
            }
        }

        /**
         * Closes all the connection pools on all hosts.
         */
        @Override
        public synchronized CompletableFuture<Void> closeAsync() {
            if (closing.get() != null)
                return closing.get();

            final CompletableFuture<Void> allPoolsClosedFuture =
                    CompletableFuture.allOf(hostConnectionPools.values().stream()
                            .map(ConnectionPool::closeAsync)
                            .toArray(CompletableFuture[]::new));

            closing.set(allPoolsClosedFuture);
            return closing.get();
        }

        private Consumer<Host> initializeConnectionSetupForHost = host -> {
            try {
                // hosts that don't initialize connection pools will come up as a dead host
                hostConnectionPools.put(host, new ConnectionPool(host, ClusteredClient.this));

                // added a new host to the cluster so let the load-balancer know
                ClusteredClient.this.cluster.loadBalancingStrategy().onNew(host);
            } catch (RuntimeException ex) {
                final String errMsg = "Could not initialize client for " + host;
                throw new RuntimeException(errMsg, ex);
            }
        };
    }

    /**
     * Uses a {@link Client.ClusteredClient} that rebinds requests to a specified {@link Graph} or
     * {@link TraversalSource} instances on the server-side.
     */
    public static class AliasClusteredClient extends Client {
        private final Client client;
        private final Map<String, String> aliases = new HashMap<>();
        final CompletableFuture<Void> close = new CompletableFuture<>();

        AliasClusteredClient(final Client client, final Map<String, String> aliases, final Client.Settings settings) {
            super(client.cluster, settings);
            this.client = client;
            this.aliases.putAll(aliases);
        }

        @Override
        public CompletableFuture<ResultSet> submitAsync(final Bytecode bytecode) {
            return submitAsync(bytecode, RequestOptions.EMPTY);
        }

        @Override
        public CompletableFuture<ResultSet> submitAsync(final Bytecode bytecode, final RequestOptions options) {
            try {
                // need to call buildMessage() right away to get client specific configurations, that way request specific
                // ones can override as needed
                final RequestMessage.Builder request = buildMessage(RequestMessage.build(Tokens.OPS_BYTECODE)
                        .processor("traversal")
                        .addArg(Tokens.ARGS_GREMLIN, bytecode));

                // apply settings if they were made available
                options.getBatchSize().ifPresent(batchSize -> request.add(Tokens.ARGS_BATCH_SIZE, batchSize));
                options.getTimeout().ifPresent(timeout -> request.add(Tokens.ARGS_SCRIPT_EVAL_TIMEOUT, timeout));
                options.getOverrideRequestId().ifPresent(request::overrideRequestId);
                options.getUserAgent().ifPresent(userAgent -> request.add(Tokens.ARGS_USER_AGENT, userAgent));

                return submitAsync(request.create());
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public CompletableFuture<ResultSet> submitAsync(final RequestMessage msg) {
            final RequestMessage.Builder builder = RequestMessage.from(msg);

            // only add aliases which aren't already present. if they are present then they represent request level
            // overrides which should be mucked with
            if (!aliases.isEmpty()) {
                final Map original = (Map) msg.getArgs().getOrDefault(Tokens.ARGS_ALIASES, Collections.emptyMap());
                aliases.forEach((k, v) -> {
                    if (!original.containsKey(k))
                        builder.addArg(Tokens.ARGS_ALIASES, aliases);
                });
            }

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
         * Delegates to the underlying {@link Client.ClusteredClient}.
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

            try {
                connectionPool = new ConnectionPool(host, this, Optional.of(1), Optional.of(1));
            } catch (RuntimeException ex) {
                logger.error("Could not initialize client for {}", host, ex);
                this.initializationFailure = ex;
            }
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

            // the connection pool may not have been initialized if requests weren't sent across it. in those cases
            // we just need to return a pre-completed future
            final CompletableFuture<Void> connectionPoolClose = null == connectionPool ?
                    CompletableFuture.completedFuture(null) : connectionPool.closeAsync();
            closing.set(connectionPoolClose);
            return connectionPoolClose;
        }
    }

    /**
     * Settings given to {@link Cluster#connect(Client.Settings)} that configures how a {@link Client} will behave.
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

            private Builder() {
            }

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
        private final boolean forceClosed;

        private SessionSettings(final Builder builder) {
            manageTransactions = builder.manageTransactions;
            sessionId = builder.sessionId;
            forceClosed = builder.forceClosed;
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

        /**
         * Determines if the session will be force closed. See {@link Builder#forceClosed(boolean)} for more details
         * on what that means.
         */
        public boolean isForceClosed() {
            return forceClosed;
        }

        public static SessionSettings.Builder build() {
            return new SessionSettings.Builder();
        }

        public static class Builder {
            private boolean manageTransactions = false;
            private String sessionId = UUID.randomUUID().toString();
            private boolean forceClosed = false;

            private Builder() {
            }

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

            /**
             * Determines if the session should be force closed when the client is closed. Force closing will not
             * attempt to close open transactions from existing running jobs and leave it to the underlying graph to
             * decided how to proceed with those orphaned transactions. Setting this to {@code true} tends to lead to
             * faster close operation which can be desirable if Gremlin Server has a long session timeout and a long
             * script evaluation timeout as attempts to close long run jobs can occur more rapidly. By default, this
             * value is {@code false}.
             */
            public Builder forceClosed(final boolean forced) {
                this.forceClosed = forced;
                return this;
            }

            public SessionSettings create() {
                return new SessionSettings(this);
            }
        }
    }
}
