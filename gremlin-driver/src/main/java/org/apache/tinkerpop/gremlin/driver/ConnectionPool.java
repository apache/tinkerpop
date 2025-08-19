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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.tinkerpop.gremlin.driver.exception.ConnectionException;
import org.apache.tinkerpop.gremlin.util.ExceptionHelper;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.TimeUtil;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
final class ConnectionPool {
    private static final Logger logger = LoggerFactory.getLogger(ConnectionPool.class);

    public static final int MIN_POOL_SIZE = 2;
    public static final int MAX_POOL_SIZE = 8;
    public static final int MIN_SIMULTANEOUS_USAGE_PER_CONNECTION = 8;
    public static final int MAX_SIMULTANEOUS_USAGE_PER_CONNECTION = 16;
    // A small buffer in millis used for comparing if a connection was created within a certain amount of time.
    private static final int CONNECTION_SETUP_TIME_DELTA = 25;

    public final Host host;
    private final Cluster cluster;
    private final Client client;
    private final List<Connection> connections;
    private final AtomicInteger open;
    private final Set<Connection> bin = new CopyOnWriteArraySet<>();
    private final int minPoolSize;
    private final int maxPoolSize;
    private final int minSimultaneousUsagePerConnection;
    private final int maxSimultaneousUsagePerConnection;
    private final int minInProcess;
    private final String poolLabel;

    private final AtomicInteger scheduledForCreation = new AtomicInteger();
    private final AtomicReference<ConnectionResult> latestConnectionResult = new AtomicReference<>(new ConnectionResult());

    private final AtomicReference<CompletableFuture<Void>> closeFuture = new AtomicReference<>();

    private volatile int waiter = 0;
    private final Lock waitLock = new ReentrantLock(true);
    private final Condition hasAvailableConnection = waitLock.newCondition();
    ConnectionFactory connectionFactory;

    /**
     * The result of a connection attempt. A null value for failureCause means that the connection attempt was successful.
     */
    public class ConnectionResult {
        private long timeOfConnectionAttempt;
        private Throwable failureCause;

        public ConnectionResult() {}

        public Throwable getFailureCause() { return failureCause; }
        public long getTime() { return timeOfConnectionAttempt; }
        public void setFailureCause(Throwable cause) { failureCause = cause; }
        public void setTimeNow() { timeOfConnectionAttempt = System.currentTimeMillis(); }
    }

    public ConnectionPool(final Host host, final Client client) {
        this(host, client, Optional.empty(), Optional.empty());
    }

    public ConnectionPool(final Host host, final Client client, final Optional<Integer> overrideMinPoolSize,
                          final Optional<Integer> overrideMaxPoolSize) {
        this(host, client, overrideMinPoolSize, overrideMaxPoolSize, new ConnectionFactory.DefaultConnectionFactory());
    }

    ConnectionPool(final Host host, final Client client, final Optional<Integer> overrideMinPoolSize,
                          final Optional<Integer> overrideMaxPoolSize, final ConnectionFactory connectionFactory) {
        this.host = host;
        this.client = client;
        this.cluster = client.cluster;
        this.connectionFactory = connectionFactory;
        poolLabel = "Connection Pool {host=" + host + "}";

        final Settings.ConnectionPoolSettings settings = settings();
        this.minPoolSize = overrideMinPoolSize.orElse(settings.minSize);
        this.maxPoolSize = overrideMaxPoolSize.orElse(settings.maxSize);
        this.minSimultaneousUsagePerConnection = settings.minSimultaneousUsagePerConnection;
        this.maxSimultaneousUsagePerConnection = settings.maxSimultaneousUsagePerConnection;
        this.minInProcess = settings.minInProcessPerConnection;

        this.connections = new CopyOnWriteArrayList<>();
        this.open = new AtomicInteger();

        try {
            final List<CompletableFuture<Void>> connectionCreationFutures = new ArrayList<>();
            for (int i = 0; i < minPoolSize; i++) {
                connectionCreationFutures.add(CompletableFuture.runAsync(() -> {
                    final ConnectionResult result = new ConnectionResult();
                    try {
                        this.connections.add(connectionFactory.create(this));
                        this.open.incrementAndGet();
                    } catch (ConnectionException e) {
                        result.setFailureCause(e);
                        throw new CompletionException(e);
                    } finally {
                        result.setTimeNow();
                        if ((latestConnectionResult.get() == null) ||
                                (latestConnectionResult.get().getTime() < result.getTime())) {
                            latestConnectionResult.set(result);
                        }
                    }
                }, cluster.connectionScheduler()));
            }

            CompletableFuture.allOf(connectionCreationFutures.toArray(new CompletableFuture[0])).join();
        } catch (CancellationException ce) {
            logger.warn("Initialization of connections cancelled for {}", this.getPoolInfo(), ce);
            throw ce;
        } catch (CompletionException ce) {
            // Some connections might have been initialized, let's respect those as of 3.7.5
            if (connections.isEmpty()) {
                // Close the connection pool since we have zero connections
                this.closeAsync();

                final String errMsg = "Could not initialize " + minPoolSize + " (minPoolSize) connections in pool for "
                        + this.host + ". " +
                        " Successful connections=" + this.connections.size() +
                        ". Closing the connection pool.";

                Throwable cause;
                Throwable result = ce;

                if (null != (cause = result.getCause())) {
                    result = cause;
                }

                throw new CompletionException(errMsg, result);
            } else {
                // warn that the error may have the driver below the min pool size. expect recovery, but no point
                // going to NoHostAvailableException for potentially a single connection error.
                logger.warn("ConnectionPool for " + this.host + " initialized with " + connections.size() +
                                " expected minPoolSize was " + minPoolSize + " - will attempt to recover", ce);
            }
        }

        logger.info("Opening connection pool on {} with core size of {}", host, minPoolSize);
    }

    public Settings.ConnectionPoolSettings settings() {
        return cluster.connectionPoolSettings();
    }

    public Connection borrowConnection(final long timeout, final TimeUnit unit) throws TimeoutException, ConnectionException {
        logger.debug("Borrowing connection from pool on {} - timeout in {} {}", host, timeout, unit);

        if (isClosed()) throw new ConnectionException(host.getHostUri(), host.getAddress(), "Pool is shutdown");

        if (connections.isEmpty()) {
            logger.debug("Tried to borrow connection but the pool was empty for {} - scheduling pool creation and waiting for connection", host);
            for (int i = 0; i < minPoolSize; i++) {
                // If many connections are borrowed at the same time there needs to be a check to make sure no
                // additional ones get scheduled for creation
                if (scheduledForCreation.get() < minPoolSize) {
                    scheduledForCreation.incrementAndGet();
                    newConnection();
                }
            }

            return waitForConnection(timeout, unit);
        }

        // Get the least used valid connection
        final Connection leastUsedConn = getLeastUsedValidConnection();

        if (null == leastUsedConn) {
            if (isClosed())
                throw new ConnectionException(host.getHostUri(), host.getAddress(), "Pool is shutdown");
            logger.debug("Pool was initialized but a connection could not be selected earlier - waiting for connection on {}", host);
            return waitForConnection(timeout, unit);
        }

        if (logger.isDebugEnabled())
            logger.debug("Return least used {} on {}", leastUsedConn.getConnectionInfo(), host);
        return leastUsedConn;
    }

    public void returnConnection(final Connection connection) throws ConnectionException {
        logger.debug("Attempting to return {} on {}", connection, host);
        if (isClosed()) throw new ConnectionException(host.getHostUri(), host.getAddress(), "Pool is shutdown");

        final int borrowed = connection.borrowed.decrementAndGet();

        if (connection.isDead()) {
            logger.debug("Marking {} as dead", this.host);
            this.replaceConnection(connection);
        } else {
            if (bin.contains(connection) && borrowed == 0) {
                logger.debug("{} is already in the bin and it has no inflight requests so it is safe to close", connection);
                if (bin.remove(connection))
                    connection.closeAsync();
                return;
            }

            // destroy a connection that exceeds the minimum pool size - it does not have the right to live if it
            // isn't busy. replace a connection that has a low available in process count which likely means that
            // it's backing up with requests that might never have returned. consider the maxPoolSize in this condition
            // because if it is equal to 1 (which it is for a session) then there is no need to replace the connection
            // as it will be responsible for every single request. if neither of these scenarios are met then let the
            // world know the connection is available.
            final int poolSize = connections.size();
            final int availableInProcess = connection.availableInProcess();
            if (poolSize > minPoolSize && borrowed <= minSimultaneousUsagePerConnection) {
                if (logger.isDebugEnabled())
                    logger.debug("On {} pool size of {} > minPoolSize {} and borrowed of {} <= minSimultaneousUsagePerConnection {} so destroy {}",
                            host, poolSize, minPoolSize, borrowed, minSimultaneousUsagePerConnection, connection.getConnectionInfo());
                destroyConnection(connection);
            } else if (availableInProcess < minInProcess && maxPoolSize > 1) {
                if (logger.isDebugEnabled())
                    logger.debug("On {} availableInProcess {} < minInProcess {} so replace {}", host, availableInProcess, minInProcess, connection.getConnectionInfo());
                replaceConnection(connection);
            } else
                announceAvailableConnection();
        }
    }

    Client getClient() {
        return client;
    }

    Cluster getCluster() {
        return cluster;
    }

    public boolean isClosed() {
        return this.closeFuture.get() != null;
    }

    /**
     * Permanently kills the pool.
     */
    public synchronized CompletableFuture<Void> closeAsync() {
        if (closeFuture.get() != null) return closeFuture.get();

        logger.info("Signalled closing of connection pool on {} with core size of {}", host, minPoolSize);

        announceAllAvailableConnection();
        final CompletableFuture<Void> future = killAvailableConnections();
        closeFuture.set(future);

        return future;
    }

    /**
     * Required for testing
     */
    int numConnectionsWaitingToCleanup() {
        return bin.size();
    }

    /**
     * Calls close on connections in the pool gathering close futures from both active connections and ones in the
     * bin.
     */
    private CompletableFuture<Void> killAvailableConnections() {
        final List<CompletableFuture<Void>> futures = new ArrayList<>(connections.size() + bin.size());
        for (Connection connection : connections) {
            final CompletableFuture<Void> future = connection.closeAsync();
            future.thenRun(open::decrementAndGet);
            futures.add(future);
        }

        // Without the ones in the bin the close for the ConnectionPool won't account for their shutdown and could
        // lead to scenario where the bin connections stay open after the channel executor is closed which then
        // leads to close operation getting rejected in Connection.close() for channel.newPromise().
        for (Connection connection : bin) {
            futures.add(connection.closeAsync());
        }

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    /**
     * This method is not idempotent and should only be called once per connection.
     */
    void replaceConnection(final Connection connection) {
        logger.info("Replace {}", connection);

        // Do not replace connection if the conn pool is closing/closed.
        // Do not replace connection if it is already being replaced.
        if (connection.isBeingReplaced.getAndSet(true) || isClosed()) {
            return;
        }

        considerNewConnection();
        definitelyDestroyConnection(connection);
    }

    private void considerNewConnection() {
        logger.debug("Considering new connection on {} where pool size is {}", host, connections.size());
        while (true) {
            int inCreation = scheduledForCreation.get();

            logger.debug("There are {} connections scheduled for creation on {}", inCreation, host);

            // don't create more than one at a time
            if (inCreation >= 1)
                return;
            if (scheduledForCreation.compareAndSet(inCreation, inCreation + 1))
                break;
        }

        newConnection();
    }

    private void newConnection() {
        cluster.connectionScheduler().submit(() -> {
            // seems like this should be decremented first because if addConnectionIfUnderMaximum fails there is
            // nothing that wants to decrement this number and so it leaves things in a state where you could
            // newConnection() doesn't seem to get called at all because it believes connections are being currently
            // created. this seems to lead to situations where the client can never borrow a connection and it's as
            // though it can't reconnect at all. this was hard to test but it seemed to happen regularly after
            // introduced the ConnectionFactory that enabled a way to introduce connection jitters (i.e. failures in
            // creation of a Connection) at which point it seemed to happen with some regularity.
            scheduledForCreation.decrementAndGet();
            addConnectionIfUnderMaximum();
            return null;
        });
    }

    private boolean addConnectionIfUnderMaximum() {
        final int openCountToActOn;

        while (true) {
            final int opened = open.get();
            if (opened >= maxPoolSize)
                return false;

            if (open.compareAndSet(opened, opened + 1)) {
                openCountToActOn = opened;
                break;
            }
        }

        if (isClosed()) {
            open.decrementAndGet();
            return false;
        }

        final ConnectionResult result = new ConnectionResult();
        try {
            connections.add(connectionFactory.create(this));
        } catch (Exception ex) {
            open.decrementAndGet();
            logger.error(String.format(
                    "Connections[%s] were under maximum allowed[%s], but there was an error creating a new connection",
                            openCountToActOn, maxPoolSize),
                    ex);
            considerHostUnavailable();
            result.setFailureCause(ex);
            return false;
        } finally {
            result.setTimeNow();
            if (latestConnectionResult.get().getTime() < result.getTime()) {
                latestConnectionResult.set(result);
            }
        }

        announceAllAvailableConnection();
        return true;
    }

    private boolean destroyConnection(final Connection connection) {
        while (true) {
            final int opened = open.get();
            if (opened <= minPoolSize)
                return false;

            if (open.compareAndSet(opened, opened - 1))
                break;
        }

        definitelyDestroyConnection(connection);
        return true;
    }

    public void definitelyDestroyConnection(final Connection connection) {
        // only add to the bin for future removal if its not already there.
        if (!bin.contains(connection) && !connection.isClosing()) {
            bin.add(connection);
            connections.remove(connection);
            open.decrementAndGet();
        }

        // only close the connection for good once it is done being borrowed or when it is dead
        if (connection.isDead() || connection.borrowed.get() == 0) {
            if (bin.remove(connection)) {
                final CompletableFuture<Void> closeFuture = connection.closeAsync();
                closeFuture.whenComplete((v, t) -> {
                    logger.debug("Destroyed {}{}{}", connection.getConnectionInfo(), System.lineSeparator(), this.getPoolInfo());
                });
            }
        }
    }

    private Connection waitForConnection(final long timeout, final TimeUnit unit) throws TimeoutException, ConnectionException {
        long start = System.nanoTime();
        long remaining = timeout;
        long to = timeout;
        do {
            try {
                awaitAvailableConnection(remaining, unit);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                to = 0;
            }

            if (isClosed())
                throw new ConnectionException(host.getHostUri(), host.getAddress(), "Pool is shutdown");

            final Connection leastUsed = getLeastUsedValidConnection();

            if (leastUsed != null) {
                if (logger.isDebugEnabled())
                    logger.debug("Return least used {} on {} after waiting", leastUsed.getConnectionInfo(), host);
                return leastUsed;
            }

            remaining = to - TimeUtil.timeSince(start, unit);
            logger.debug("Continue to wait for connection on {} if {} > 0", host, remaining);
        } while (remaining > 0);

        final StringBuilder cause = new StringBuilder("Potential Cause: ");
        final ConnectionResult res = latestConnectionResult.get();
        // If a connection attempt failed within a request's maxWaitForConnection then it likely contributed to that
        // request getting a TimeoutException.
        if (((System.currentTimeMillis() - res.getTime()) < (cluster.getMaxWaitForConnection() + CONNECTION_SETUP_TIME_DELTA)) &&
                (res.getFailureCause() != null)) {
            // Assumes that the root cause will give better information about why the connection failed.
            cause.append(ExceptionHelper.getRootCause(res.getFailureCause()).getMessage());
        } else if (open.get() >= maxPoolSize) {
            cause.append(Client.TOO_MANY_IN_FLIGHT_REQUESTS);
        } else {
            cause.setLength(0);
        }
        // if we get to this point, we waited up to maxWaitForConnection from the pool and did not get a Connection.
        // this can either mean the pool is exhausted or the host is unavailable. for the former, this could mean
        // the connection pool and/or server is not sized correctly for the workload as connections are not freeing up
        // to keep up requests and for the latter it could mean anything from a network hiccup to the server simply
        // being down. it is critical that the caller be able to discern between these two. the following error
        // message written to the log should provide some insight into the state of the connection pool telling the
        // user if the pool was running at maximum.
        final String timeoutErrorMessage = String.format(
                "Timed-out (%s %s) waiting for connection on %s. %s%s%s",
                timeout, unit, host, cause.toString(), System.lineSeparator(), this.getPoolInfo());

        logger.error(timeoutErrorMessage);

        // if we timeout borrowing a connection that might mean the host is dead (or the timeout was super short).
        // either way supply a function to reconnect
        final TimeoutException timeoutException = new TimeoutException(timeoutErrorMessage);
        this.considerHostUnavailable();

        throw timeoutException;
    }

    /**
     * On a failure to get a {@link Connection} this method is called to determine if the {@link Host} should be
     * marked as unavailable and to establish a background reconnect operation.
     */
    public void considerHostUnavailable() {
        // if there is at least one available connection the host has to still be around (or is perhaps on its way out
        // but we'll stay optimistic in this check). no connections also means "unhealthy". unsure if there is an ok
        // "no connections" state we can get into. in any event if there are no connections then we'd just try to
        // immediately reconnect below anyway so perhaps that state isn't really something to worry about.
        final boolean maybeUnhealthy = connections.stream().allMatch(Connection::isDead);
        if (maybeUnhealthy) {
            // immediately fire off an attempt to reconnect because there are no active connections.
            host.tryReconnectingImmediately(this::tryReconnect);

            // let the load-balancer know that the host is acting poorly
            if (!host.isAvailable()) {
                // if the host is unavailable then we should release the connections
                connections.forEach(this::definitelyDestroyConnection);
                this.cluster.loadBalancingStrategy().onUnavailable(host);
            }
        }
    }

    /**
     * Attempt to reconnect to the {@link Host} that was previously marked as unavailable.  This method gets called
     * as part of a schedule in {@link Host} to periodically try to create working connections.
     */
    private boolean tryReconnect(final Host h) {
        logger.debug("Trying to re-establish connection on {}", h);

        Connection connection = null;
        try {
            // rather than rely on borrowConnection() infrastructure and the pool create a brand new Connection
            // instance solely for the purpose of this ping. this ensures that if the pool is overloaded that we
            // make an honest attempt at validating host health without failing over some timeout waiting for a
            // connection in the pool. not sure if we should try to keep this connection if it succeeds and if the
            // pool needs it. for now that seems like an unnecessary added bit of complexity for dealing with this
            // error state
            connection = connectionFactory.create(this);
            final RequestMessage ping = client.buildMessage(cluster.validationRequest()).create();
            final CompletableFuture<ResultSet> f = new CompletableFuture<>();
            connection.write(ping, f);
            f.get().all().get();

            // host is reconnected and a connection is now available
            this.cluster.loadBalancingStrategy().onAvailable(h);
            return true;
        } catch (Exception ex) {
            logger.error(String.format("Failed reconnect attempt on %s%s%s",
                            h, System.lineSeparator(), this.getPoolInfo()), ex);
            return false;
        } finally {
            if (connection != null) {
                connection.closeAsync();
            }
        }
    }

    private void announceAvailableConnection() {
        logger.debug("Announce connection available on {}", host);

        if (waiter == 0)
            return;

        waitLock.lock();
        try {
            hasAvailableConnection.signal();
        } finally {
            waitLock.unlock();
        }
    }

    /**
     * Get the least-used connection from the pool. Also triggers consideration of a new connection if the least-used
     * connection has hit the usage maximum or no valid connection could be retrieved from the pool.
     *
     * @return The least-used connection from the pool. Returns null if no valid connection could be retrieved from the
     * pool.
     */
    private synchronized Connection getLeastUsedValidConnection() {
        int minInFlight = Integer.MAX_VALUE;
        Connection leastBusy = null;
        for (Connection connection : connections) {
            final int inFlight = connection.borrowed.get();
            if (!connection.isDead() && inFlight < minInFlight && inFlight < maxSimultaneousUsagePerConnection) {
                minInFlight = inFlight;
                leastBusy = connection;
            }
        }

        if (leastBusy != null) {
            // Increment borrow count and consider making a new connection if least used connection hits usage maximum
            if (leastBusy.borrowed.incrementAndGet() >= maxSimultaneousUsagePerConnection
                    && connections.size() < maxPoolSize) {
                if (logger.isDebugEnabled())
                    logger.debug("Least used {} on {} reached maxSimultaneousUsagePerConnection but pool size {} < maxPoolSize - consider new connection",
                            leastBusy.getConnectionInfo(), host, connections.size());
                considerNewConnection();
            }
        } else if (connections.size() < maxPoolSize) {
            // A safeguard for scenarios where consideration of a new connection was somehow not triggered by an
            // existing connection hitting the usage maximum
            considerNewConnection();
        }

        return leastBusy;
    }

    private void awaitAvailableConnection(long timeout, TimeUnit unit) throws InterruptedException {
        logger.debug("Wait {} {} for an available connection on {} with {}", timeout, unit, host, Thread.currentThread());

        waitLock.lock();
        waiter++;
        try {
            hasAvailableConnection.await(timeout, unit);
        } finally {
            waiter--;
            waitLock.unlock();
        }
    }

    private void announceAllAvailableConnection() {
        if (waiter == 0)
            return;

        waitLock.lock();
        try {
            hasAvailableConnection.signalAll();
        } finally {
            waitLock.unlock();
        }
    }

    /**
     * Returns the set of Channel IDs maintained by the connection pool.
     * Currently, only used for testing.
     */
    Set<String> getConnectionIDs() {
        return connections.stream().map(Connection::getChannelId).collect(Collectors.toSet());
    }

    /**
     * Gets a message that describes the state of the connection pool.
     */
    public String getPoolInfo() {
        return getPoolInfo(null);
    }

    /**
     * Gets a message that describes the state of the connection pool.
     *
     * @param connectionToCallout the connection from the pool to identify more clearly in the message
     */
    public String getPoolInfo(final Connection connectionToCallout) {
        final StringBuilder sb = new StringBuilder("ConnectionPool (");
        sb.append(host.toString());
        sb.append(")");

        if (connections.isEmpty()) {
            sb.append("- no connections in pool");
        } else {
            final int connectionCount = connections.size();
            sb.append(System.lineSeparator());
            sb.append(String.format("Connection Pool Status (size=%s max=%s min=%s toCreate=%s bin=%s)",
                    connectionCount, maxPoolSize, minPoolSize, this.scheduledForCreation.get(), bin.size()));
            sb.append(System.lineSeparator());

            appendConnections(sb, connectionToCallout, (CopyOnWriteArrayList<Connection>) connections);
            sb.append(System.lineSeparator());
            sb.append("-- bin --");
            sb.append(System.lineSeparator());
            appendConnections(sb, connectionToCallout, new CopyOnWriteArrayList<>(bin));
        }

        return sb.toString().trim();
    }

    private void appendConnections(final StringBuilder sb, final Connection connectionToCallout,
                                   final CopyOnWriteArrayList<Connection> connections) {
        final Iterator<Connection> it = connections.iterator();
        while(it.hasNext()) {
            final Connection c = it.next();
            if (c.equals(connectionToCallout)) {
                sb.append("==> ");
            }
            else {
                sb.append("> ");
            }
            sb.append(c.getConnectionInfo(false));
            if (it.hasNext()) {
                sb.append(System.lineSeparator());
            }
        }
    }

    @Override
    public String toString() {
        return poolLabel;
    }
}
