package com.tinkerpop.gremlin.driver;

import com.tinkerpop.gremlin.driver.exception.ConnectionException;
import com.tinkerpop.gremlin.driver.message.RequestMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
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
     * will simply call the {@link com.tinkerpop.gremlin.driver.message.RequestMessage.Builder#create()} and return
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
     * Asyncronous close of the {@code Client}.
     */
    public abstract CompletableFuture<Void> closeAsync();

    public synchronized Client init() {
        if (initialized)
            return this;

        logger.debug("Initializing client on cluster [{}]", cluster);

        cluster.init();
        initializeImplementation();

        initialized = true;
        return this;
    }

    public ResultSet submit(final String gremlin) {
        return submit(gremlin, null);
    }

    public ResultSet submit(final String gremlin, final Map<String, Object> parameters) {
        try {
            return submitAsync(gremlin, parameters).get();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public CompletableFuture<ResultSet> submitAsync(final String gremlin) {
        return submitAsync(gremlin, null);
    }

    public CompletableFuture<ResultSet> submitAsync(final String gremlin, final Map<String, Object> parameters) {
        final RequestMessage.Builder request = RequestMessage.build(Tokens.OPS_EVAL)
                .add(Tokens.ARGS_GREMLIN, gremlin)
                .add(Tokens.ARGS_BATCH_SIZE, cluster.connectionPoolSettings().resultIterationBatchSize);

        Optional.ofNullable(parameters).ifPresent(params -> request.addArg(Tokens.ARGS_BINDINGS, parameters));
        return submitAsync(buildMessage(request));
    }

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
            logger.debug("Submitted {} to - {}", msg, null == connection ? "connection not initialized" : connection.toString());
        }
    }

    public void close() {
        closeAsync().join();
    }

    /**
     * A {@code Client} implementation that does not operate in a session.  Requests are sent to multiple servers
     * given a {@link com.tinkerpop.gremlin.driver.LoadBalancingStrategy}.  Transactions are automatically committed
     * (or rolled-back on error) after each request.
     */
    public static class ClusteredClient extends Client {

        private ConcurrentMap<Host, ConnectionPool> hostConnectionPools = new ConcurrentHashMap<>();

        ClusteredClient(final Cluster cluster) {
            super(cluster);
        }

        @Override
        protected Connection chooseConnection(final RequestMessage msg) throws TimeoutException, ConnectionException {
            final Host bestHost = this.cluster.loadBalancingStrategy().select(msg).next();
            final ConnectionPool pool = hostConnectionPools.get(bestHost);
            return pool.borrowConnection(cluster.connectionPoolSettings().maxWaitForConnection, TimeUnit.MILLISECONDS);
        }

        @Override
        protected void initializeImplementation() {
            cluster.getClusterInfo().allHosts().forEach(host -> {
                try {
                    // hosts that don't initialize connection pools will come up as a dead host
                    hostConnectionPools.put(host, new ConnectionPool(host, cluster));

                    // added a new host to the cluster so let the load-balancer know
                    this.cluster.loadBalancingStrategy().onNew(host);
                } catch (Exception ex) {
                    // catch connection errors and prevent them from failing the creation
                    logger.warn("Could not initialize connection pool for {}", host);
                }
            });
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            final CompletableFuture[] poolCloseFutures = new CompletableFuture[hostConnectionPools.size()];
            hostConnectionPools.values().stream().map(ConnectionPool::closeAsync).collect(Collectors.toList()).toArray(poolCloseFutures);
            return CompletableFuture.allOf(poolCloseFutures);
        }
    }

    /**
     * A {@code Client} implementation that operates in the context of a session.  Requests are sent to a single
     * server, where each request is bound to the same thread with the same set of bindings across requests.
     * Transaction are not automatically committed. It is up the client to issue commit/rollback commands.
     */
    public static class SessionedClient extends Client {
        private final String sessionId;

        private ConnectionPool connectionPool;

        SessionedClient(final Cluster cluster, final String sessionId) {
            super(cluster);
            this.sessionId = sessionId;
        }

        @Override
        public RequestMessage buildMessage(final RequestMessage.Builder builder) {
            builder.processor("session");
            builder.addArg(Tokens.ARGS_SESSION, sessionId);
            return builder.create();
        }

        @Override
        protected Connection chooseConnection(final RequestMessage msg) throws TimeoutException, ConnectionException {
            return connectionPool.borrowConnection(cluster.connectionPoolSettings().maxWaitForConnection, TimeUnit.MILLISECONDS);
        }

        @Override
        protected void initializeImplementation() {
            // chooses an available host at random
            final List<Host> hosts = cluster.getClusterInfo().allHosts()
                    .stream().filter(Host::isAvailable).collect(Collectors.toList());
            Collections.shuffle(hosts);
            final Host host = hosts.get(0);
            connectionPool = new ConnectionPool(host, cluster);
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            return connectionPool.closeAsync();
        }
    }
}
