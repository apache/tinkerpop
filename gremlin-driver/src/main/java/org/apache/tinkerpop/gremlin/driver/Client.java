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

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.driver.exception.ConnectionException;
import org.apache.tinkerpop.gremlin.driver.exception.NoHostAvailableException;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
    public static final String TOO_MANY_IN_FLIGHT_REQUESTS = "Number of active requests (%s) exceeds pool size (%s). " +
            "Consider increasing the value for maxConnectionPoolSize.";

    protected final Cluster cluster;
    protected volatile boolean initialized;

    private static final Random random = new Random();

    Client(final Cluster cluster) {
        this.cluster = cluster;
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
        return new AliasClusteredClient(this, graphOrTraversalSource);
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
        } catch (RuntimeException re) {
            throw re;
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
        } catch (RuntimeException re) {
            throw re;
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
        final RequestOptions.Builder options = RequestOptions.build();
        options.addG(graphOrTraversalSource);

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
        final RequestMessage.Builder request = buildMessage(RequestMessage.build(gremlin))
                .addChunkSize(batchSize);

        // apply settings if they were made available
        options.getTimeout().ifPresent(timeout -> request.addTimeoutMillis(timeout));
        options.getParameters().ifPresent(params -> request.addBindings(params));
        options.getG().ifPresent(g -> request.addG(g));
        options.getLanguage().ifPresent(lang -> request.addLanguage(lang));
        options.getMaterializeProperties().ifPresent(mp -> request.addMaterializeProperties(mp));
        options.getBulkResults().ifPresent(bulked -> request.addBulkResults(Boolean.parseBoolean(bulked)));

        return submitAsync(request.create());
    }

    /**
     * A low-level method that allows the submission of a manually constructed {@link RequestMessage}.
     */
    public CompletableFuture<ResultSet> submitAsync(final RequestMessage msg) {
        if (isClosing()) throw new IllegalStateException("Client is closed");

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
        } catch (RuntimeException re) {
            throw re;
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
     * Gets the {@link Cluster} that spawned this {@code Client}.
     */
    public Cluster getCluster() {
        return cluster;
    }

    /**
     * A {@code Client} implementation.  Requests are sent to multiple servers given a {@link LoadBalancingStrategy}.
     * Transactions are automatically committed (or rolled-back on error) after each request.
     */
    public final static class ClusteredClient extends Client {

        ConcurrentMap<Host, ConnectionPool> hostConnectionPools = new ConcurrentHashMap<>();
        private final AtomicReference<CompletableFuture<Void>> closing = new AtomicReference<>(null);
        private Throwable initializationFailure = null;

        ClusteredClient(final Cluster cluster) {
            super(cluster);
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
            } catch (RuntimeException re) {
                throw re;
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Client alias(final String graphOrTraversalSource) {
            return new AliasClusteredClient(this, graphOrTraversalSource);
        }

        /**
         * Uses a {@link LoadBalancingStrategy} to choose the best {@link Host} and then selects the best connection
         * from that host's connection pool.
         */
        @Override
        protected Connection chooseConnection(final RequestMessage msg) throws TimeoutException, ConnectionException {
            final Iterator<Host> possibleHosts = this.cluster.loadBalancingStrategy().select(msg);

            // try a random host if none are marked available. maybe it will reconnect in the meantime. better than
            // going straight to a fast NoHostAvailableException as was the case in versions 3.5.4 and earlier
            final Host bestHost = possibleHosts.hasNext() ? possibleHosts.next() : chooseRandomHost();
            final ConnectionPool pool = hostConnectionPools.get(bestHost);
            return pool.borrowConnection(cluster.connectionPoolSettings().maxWaitForConnection, TimeUnit.MILLISECONDS);
        }

        private Host chooseRandomHost() {
            final List<Host> hosts = new ArrayList<>(cluster.allHosts());
            final int ix = random.nextInt(hosts.size());
            return hosts.get(ix);
        }

        /**
         * Initializes the connection pools on all hosts.
         */
        @Override
        protected void initializeImplementation() {
            try {
                CompletableFuture.allOf(cluster.allHosts().stream()
                                .map(host -> CompletableFuture.runAsync(
                                        () -> initializeConnectionSetupForHost.accept(host), cluster.hostScheduler()))
                                .toArray(CompletableFuture[]::new))
                        .join();
            } catch (CompletionException ex) {
                logger.error("Initialization failed", ex);
                this.initializationFailure = ex;
            }

            // throw an error if there is no host available after initializing connection pool.
            if (cluster.availableHosts().isEmpty())
                throwNoHostAvailableException();

            // try to re-initiate any unavailable hosts in the background.
            final List<Host> unavailableHosts = cluster.allHosts()
                    .stream().filter(host -> !host.isAvailable()).collect(Collectors.toList());
            if (!unavailableHosts.isEmpty()) {
                handleUnavailableHosts(unavailableHosts);
            }
        }

        private void throwNoHostAvailableException() {
            final Throwable rootCause = ExceptionUtils.getRootCause(initializationFailure);
            // allow the certain exceptions to propagate as a cause
            if (rootCause instanceof SSLException || rootCause instanceof ConnectException) {
                throw new NoHostAvailableException(initializationFailure);
            } else {
                throw new NoHostAvailableException();
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

        private final Consumer<Host> initializeConnectionSetupForHost = host -> {
            try {
                // hosts that don't initialize connection pools will come up as a dead host.
                hostConnectionPools.put(host, new ConnectionPool(host, ClusteredClient.this));

                // hosts are not marked as available at cluster initialization, and are made available here instead.
                host.makeAvailable();

                // added a new host to the cluster so let the load-balancer know.
                ClusteredClient.this.cluster.loadBalancingStrategy().onNew(host);
            } catch (RuntimeException ex) {
                final String errMsg = "Could not initialize client for " + host;
                logger.error(errMsg);
                throw ex;
            }
        };

        private void handleUnavailableHosts(final List<Host> unavailableHosts) {
            // start the re-initialization attempt for each of the unavailable hosts through Host.makeUnavailable().
            for (Host host : unavailableHosts) {
                final CompletableFuture<Void> f = CompletableFuture.runAsync(
                        () -> host.makeUnavailable(this::tryReInitializeHost), cluster.hostScheduler());
                f.exceptionally(t -> {
                    logger.error("", (t.getCause() == null) ? t : t.getCause());
                    return null;
                });
            }
        }

        /**
         * Attempt to re-initialize the {@link Host} that was previously marked as unavailable.  This method gets called
         * as part of a schedule in {@link Host} to periodically try to re-initialize.
         */
        public boolean tryReInitializeHost(final Host host) {
            logger.debug("Trying to re-initiate host connection pool on {}", host);

            try {
                initializeConnectionSetupForHost.accept(host);
                return true;
            } catch (Exception ex) {
                logger.debug("Failed re-initialization attempt on {}", host, ex);
                return false;
            }
        }

    }

    /**
     * Uses a {@link Client.ClusteredClient} that rebinds requests to a specified {@link Graph} or
     * {@link TraversalSource} instances on the server-side.
     */
    public static class AliasClusteredClient extends Client {
        private final Client client;
        private final String graphOrTraversalSource;
        final CompletableFuture<Void> close = new CompletableFuture<>();

        AliasClusteredClient(final Client client, final String graphOrTraversalSource) {
            super(client.cluster);
            this.client = client;
            this.graphOrTraversalSource = graphOrTraversalSource;
        }

        @Override
        public CompletableFuture<ResultSet> submitAsync(final RequestMessage msg) {
            final RequestMessage.Builder builder = RequestMessage.from(msg);

            builder.addG(graphOrTraversalSource);

            return super.submitAsync(builder.create());
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
            builder.addG(graphOrTraversalSource);

            return client.buildMessage(builder);
        }

        @Override
        protected void initializeImplementation() {
            // no init required
            if (close.isDone()) {
                throw new IllegalStateException("Client is closed");
            } else if (cluster.availableHosts().isEmpty()) {
                throw new NoHostAvailableException();
            }
        }

        /**
         * Delegates to the underlying {@link Client.ClusteredClient}.
         */
        @Override
        protected Connection chooseConnection(final RequestMessage msg) throws TimeoutException, ConnectionException {
            if (close.isDone()) throw new IllegalStateException("Client is closed");
            return client.chooseConnection(msg);
        }

        @Override
        public void close() {
            client.close();
        }

        @Override
        public synchronized CompletableFuture<Void> closeAsync() {
            return client.closeAsync();
        }

        @Override
        public boolean isClosing() {
            return client.isClosing();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Client alias(final String graphOrTraversalSource) {
            if (close.isDone()) throw new IllegalStateException("Client is closed");
            return new AliasClusteredClient(client, graphOrTraversalSource);
        }
    }
}
