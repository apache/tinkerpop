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
import org.apache.tinkerpop.gremlin.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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

    private final AtomicReference<CompletableFuture<Void>> closeFuture = new AtomicReference<>();

    private volatile int waiter = 0;
    private final Lock waitLock = new ReentrantLock(true);
    private final Condition hasAvailableConnection = waitLock.newCondition();

    public ConnectionPool(final Host host, final Client client) {
        this(host, client, Optional.empty(), Optional.empty());
    }

    public ConnectionPool(final Host host, final Client client, final Optional<Integer> overrideMinPoolSize,
                          final Optional<Integer> overrideMaxPoolSize) {
        this.host = host;
        this.client = client;
        this.cluster = client.cluster;
        poolLabel = String.format("Connection Pool {host=%s}", host);

        final Settings.ConnectionPoolSettings settings = settings();
        this.minPoolSize = overrideMinPoolSize.orElse(settings.minSize);
        this.maxPoolSize = overrideMaxPoolSize.orElse(settings.maxSize);
        this.minSimultaneousUsagePerConnection = settings.minSimultaneousUsagePerConnection;
        this.maxSimultaneousUsagePerConnection = settings.maxSimultaneousUsagePerConnection;
        this.minInProcess = settings.minInProcessPerConnection;

        this.connections = new CopyOnWriteArrayList<>();

        try {
            for (int i = 0; i < minPoolSize; i++)
                this.connections.add(new Connection(host.getHostUri(), this, settings.maxInProcessPerConnection));
        } catch (ConnectionException ce) {
            // ok if we don't get it initialized here - when a request is attempted in a connection from the
            // pool it will try to create new connections as needed.
            logger.warn("Could not initialize connections in pool for {} - pool size at {}", host, this.connections.size(), ce);
            considerHostUnavailable();
        }

        this.open = new AtomicInteger(connections.size());

        logger.info("Opening connection pool on {} with core size of {}", host, minPoolSize);
    }

    public Settings.ConnectionPoolSettings settings() {
        return cluster.connectionPoolSettings();
    }

    public Connection borrowConnection(final long timeout, final TimeUnit unit) throws TimeoutException, ConnectionException {
        logger.debug("Borrowing connection from pool on {} - timeout in {} {}", host, timeout, unit);

        if (isClosed())
            throw new ConnectionException(host.getHostUri(), host.getAddress(), "Pool is shutdown");

        final Connection leastUsedConn = selectLeastUsed();

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

        if (null == leastUsedConn) {
            if (isClosed())
                throw new ConnectionException(host.getHostUri(), host.getAddress(), "Pool is shutdown");
            logger.debug("Pool was initialized but a connection could not be selected earlier - waiting for connection on {}", host);
            return waitForConnection(timeout, unit);
        }

        // if the number borrowed on the least used connection exceeds the max allowed and the pool size is
        // not at maximum then consider opening a connection
        final int currentPoolSize = connections.size();
        if (leastUsedConn.borrowed.get() >= maxSimultaneousUsagePerConnection && currentPoolSize < maxPoolSize) {
            if (logger.isDebugEnabled())
                logger.debug("Least used {} on {} exceeds maxSimultaneousUsagePerConnection but pool size {} < maxPoolSize - consider new connection",
                        leastUsedConn.getConnectionInfo(), host, currentPoolSize);
            considerNewConnection();
        }

        while (true) {
            final int borrowed = leastUsedConn.borrowed.get();
            final int availableInProcess = leastUsedConn.availableInProcess();

            if (borrowed >= maxSimultaneousUsagePerConnection && leastUsedConn.availableInProcess() == 0) {
                logger.debug("Least used connection selected from pool for {} but borrowed [{}] >= availableInProcess [{}] - wait",
                        host, borrowed, availableInProcess);
                return waitForConnection(timeout, unit);
            }

            if (leastUsedConn.borrowed.compareAndSet(borrowed, borrowed + 1)) {
                if (logger.isDebugEnabled())
                    logger.debug("Return least used {} on {}", leastUsedConn.getConnectionInfo(), host);
                return leastUsedConn;
            }
        }
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
                logger.debug("{} is already in the bin and it has no in-flight requests so it is safe to close", connection);
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
        return closeFuture.get() != null;
    }

    /**
     * Permanently kills the pool.
     */
    public synchronized CompletableFuture<Void> closeAsync() {
        if (closeFuture.get() != null) return closeFuture.get();

        // marking conn pool to be closing...
        closeFuture.getAndSet(new CompletableFuture<>());
        logger.info("Signalled closing of connection pool on {} with core size of {}", host, minPoolSize);

        announceAllAvailableConnection();
        final CompletableFuture<Void> future = killAvailableConnections();
        logger.debug("Killed all connections in pool {}", getPoolInfo());

        closeFuture.getAndSet(future);

        return future;
    }

    /**
     * Required for testing
     */
    int numConnectionsWaitingToCleanup() {
        return bin.size();
    }

    private CompletableFuture<Void> killAvailableConnections() {
        final List<CompletableFuture<Void>> futures = new ArrayList<>(connections.size());
        for (Connection connection : connections) {
            final CompletableFuture<Void> future = connection.closeAsync();
            future.thenRun(open::decrementAndGet);
            futures.add(future);
        }

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
    }

    /**
     * This method is not idempotent and should only be called once per connection.
     */
    void replaceConnection(final Connection connection) {
        logger.info("Replace Connection ID:{}", connection.getChannelId());

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
        cluster.executor().submit(() -> {
            addConnectionIfUnderMaximum();
            scheduledForCreation.decrementAndGet();
            return null;
        });
    }

    private boolean addConnectionIfUnderMaximum() {
        while (true) {
            int opened = open.get();
            if (opened >= maxPoolSize)
                return false;

            if (open.compareAndSet(opened, opened + 1))
                break;
        }

        if (isClosed()) {
            open.decrementAndGet();
            return false;
        }

        try {
            connections.add(new Connection(host.getHostUri(), this, settings().maxInProcessPerConnection));
        } catch (ConnectionException ce) {
            logger.debug("Connections were under max, but there was an error creating the connection.", ce);
            open.decrementAndGet();
            considerHostUnavailable();
            return false;
        }

        announceAvailableConnection();
        return true;
    }

    private boolean destroyConnection(final Connection connection) {
        while (true) {
            int opened = open.get();
            if (opened <= minPoolSize)
                return false;

            if (open.compareAndSet(opened, opened - 1))
                break;
        }

        definitelyDestroyConnection(connection);
        return true;
    }

    private void definitelyDestroyConnection(final Connection connection) {
        // only add to the bin for future removal if its not already there.
        if (!bin.contains(connection) && !connection.isClosing()) {
            bin.add(connection);
            connections.remove(connection);
            open.decrementAndGet();
        }

        // only close the connection for good once it is done being borrowed or when it is dead
        if (connection.isDead() || connection.borrowed.get() == 0) {
            if(bin.remove(connection)) {
                connection.closeAsync();
                // TODO: Log the following message on completion of the future returned by closeAsync.
                logger.debug("{} destroyed", connection.getConnectionInfo());
            }
        }
    }

    private Connection waitForConnection(final long timeout, final TimeUnit unit) throws TimeoutException, ConnectionException {
        logger.debug("Start wait for connection");
        long start = System.nanoTime();
        long remaining = timeout;
        long to = timeout;
        do {
            try {
                awaitAvailableConnection(remaining, unit);
            } catch (InterruptedException e) {
                logger.debug("Await interrupted", e);
                Thread.currentThread().interrupt();
                to = 0;
            }

            logger.debug("wait for conn...isClosed {}", this.getPoolInfo());
            if (isClosed())
                throw new ConnectionException(host.getHostUri(), host.getAddress(), "Pool is shutdown");

            logger.debug("Selecting least used conn...");
            final Connection leastUsed = selectLeastUsed();
            logger.debug("Select least used conn");
            if (leastUsed != null) {
                while (true) {
                    final int inFlight = leastUsed.borrowed.get();
                    final int availableInProcess = leastUsed.availableInProcess();
                    if (inFlight >= availableInProcess) {
                        logger.debug("Least used {} on {} has requests borrowed [{}] >= availableInProcess [{}] - may timeout waiting for connection",
                                leastUsed, host, inFlight, availableInProcess);
                        break;
                    }

                    if (leastUsed.borrowed.compareAndSet(inFlight, inFlight + 1)) {
                        if (logger.isDebugEnabled())
                            logger.debug("Return least used {} on {} after waiting", leastUsed.getConnectionInfo(), host);
                        return leastUsed;
                    }
                }
            }

            remaining = to - TimeUtil.timeSince(start, unit);
            logger.debug("Continue to wait for connection on {} if {} > 0 on {}", host, remaining, getPoolInfo());
        } while (remaining > 0);

        logger.debug("Timed-out waiting for connection on {} - possibly unavailable", host);

        // if we timeout borrowing a connection that might mean the host is dead (or the timeout was super short).
        // either way supply a function to reconnect
        this.considerHostUnavailable();

        throw new TimeoutException("Timed-out waiting for connection on " + host + " - possibly unavailable");
    }

    public void considerHostUnavailable() {
        logger.debug("Considering host unavailable for pool {}", this.getPoolInfo());
        // called when a connection is "dead" due to a non-recoverable error.
        host.makeUnavailable(this::tryReconnect);

        // if the host is unavailable then we should release the connections
        connections.forEach(this::definitelyDestroyConnection);

        // let the load-balancer know that the host is acting poorly
        this.cluster.loadBalancingStrategy().onUnavailable(host);
    }

    /**
     * Attempt to reconnect to the {@link Host} that was previously marked as unavailable.  This method gets called
     * as part of a schedule in {@link Host} to periodically try to create working connections.
     */
    private boolean tryReconnect(final Host h) {
        logger.debug("Trying to re-establish connection on {}", h);

        Connection connection = null;
        try {
            connection = borrowConnection(cluster.connectionPoolSettings().maxWaitForConnection, TimeUnit.MILLISECONDS);
            final RequestMessage ping = client.buildMessage(cluster.validationRequest()).create();
            final CompletableFuture<ResultSet> f = new CompletableFuture<>();
            connection.write(ping, f);
            f.get().all().get();

            // host is reconnected and a connection is now available
            this.cluster.loadBalancingStrategy().onAvailable(h);
            return true;
        } catch (Exception ex) {
            logger.debug("Failed reconnect attempt on {}", h);
            if (connection != null) definitelyDestroyConnection(connection);
            return false;
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

    private Connection selectLeastUsed() {
        int minInFlight = Integer.MAX_VALUE;
        Connection leastBusy = null;
        for (Connection connection : connections) {
            final int inFlight = connection.borrowed.get();
            if (!connection.isDead() && inFlight < minInFlight) {
                minInFlight = inFlight;
                leastBusy = connection;
            }
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

    public String getPoolInfo() {
        final StringBuilder sb = new StringBuilder("ConnectionPool (");
        sb.append(host);
        sb.append(") - ");
        connections.forEach(c -> {
            sb.append(c);
            sb.append(",");
        });
        sb.append("isClosing=");
        sb.append(isClosed());
        return sb.toString().trim();
    }

    @Override
    public String toString() {
        return poolLabel;
    }
}
