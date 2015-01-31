package com.tinkerpop.gremlin.driver;

import com.tinkerpop.gremlin.driver.exception.ConnectionException;
import com.tinkerpop.gremlin.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
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

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class ConnectionPool {
    private static final Logger logger = LoggerFactory.getLogger(ConnectionPool.class);

    public static final int MIN_POOL_SIZE = 2;
    public static final int MAX_POOL_SIZE = 8;
    public static final int MIN_SIMULTANEOUS_REQUESTS_PER_CONNECTION = 8;
    public static final int MAX_SIMULTANEOUS_REQUESTS_PER_CONNECTION = 16;

    public final Host host;
    private final Cluster cluster;
    private final List<Connection> connections;
    private final AtomicInteger open;
    private final Set<Connection> bin = new CopyOnWriteArraySet<>();
    private final int minPoolSize;
    private final int maxPoolSize;
    private final int minSimultaneousRequestsPerConnection;
    private final int maxSimultaneousRequestsPerConnection;
    private final int minInProcess;

    private final AtomicInteger scheduledForCreation = new AtomicInteger();

    private final AtomicReference<CompletableFuture<Void>> closeFuture = new AtomicReference<>();

    private volatile int waiter = 0;
    private final Lock waitLock = new ReentrantLock(true);
    private final Condition hasAvailableConnection = waitLock.newCondition();

    public ConnectionPool(final Host host, final Cluster cluster) {
        this.host = host;
        this.cluster = cluster;

        final Settings.ConnectionPoolSettings settings = settings();
        this.minPoolSize = settings.minSize;
        this.maxPoolSize = settings.maxSize;
        this.minSimultaneousRequestsPerConnection = settings.minSimultaneousRequestsPerConnection;
        this.maxSimultaneousRequestsPerConnection = settings.maxSimultaneousRequestsPerConnection;
        this.minInProcess = settings.minInProcessPerConnection;

        final List<Connection> l = new ArrayList<>(minPoolSize);

        try {
            for (int i = 0; i < minPoolSize; i++)
                l.add(new Connection(host.getHostUri(), this, cluster, settings.maxInProcessPerConnection));
        } catch (ConnectionException ce) {
            // ok if we don't get it initialized here - when a request is attempted in a connection from the
            // pool it will try to create new connections as needed.
            logger.debug("Could not initialize connections in pool for {} - pool size at {}", host, l.size());
            considerUnavailable();
        }

        this.connections = new CopyOnWriteArrayList<>(l);
        this.open = new AtomicInteger(connections.size());

        logger.info("Opening connection pool on {} with core size of {}", host, minPoolSize);
    }

    public Settings.ConnectionPoolSettings settings() {
        return cluster.connectionPoolSettings();
    }

    public Connection borrowConnection(final long timeout, final TimeUnit unit) throws TimeoutException, ConnectionException {
        logger.debug("Borrowing connection from pool on {} - timeout in {} {}", host, timeout, unit);

        if (isClosed()) throw new ConnectionException(host.getHostUri(), host.getAddress(), "Pool is shutdown");

        final Connection leastUsedConn = selectLeastUsed();

        if (connections.isEmpty()) {
            logger.debug("Tried to borrow connection but the pool was empty for {} - scheduling pool creation and waiting for connection", host);
            for (int i = 0; i < minPoolSize; i++) {
                scheduledForCreation.incrementAndGet();
                newConnection();
            }

            return waitForConnection(timeout, unit);
        }

        if (null == leastUsedConn) {
            if (isClosed())
                throw new ConnectionException(host.getHostUri(), host.getAddress(), "Pool is shutdown");
            logger.debug("Pool was initialized but a connection could not be selected earlier - waiting for connection on {}", host);
            return waitForConnection(timeout, unit);
        }

        // if the number in flight on the least used connection exceeds the max allowed and the pool size is
        // not at maximum then consider opening a connection
        final int currentPoolSize = connections.size();
        if (leastUsedConn.inFlight.get() >= maxSimultaneousRequestsPerConnection && currentPoolSize < maxPoolSize) {
            logger.debug("Least used {} on {} exceeds maxSimultaneousRequestsPerConnection but pool size {} < maxPoolSize - consider new connection",
                    leastUsedConn, host, currentPoolSize);
            considerNewConnection();
        }

        while (true) {
            final int inFlight = leastUsedConn.inFlight.get();
            final int availableInProcess = leastUsedConn.availableInProcess();

            // if the number in flight starts to exceed what's available for this connection, then we need
            // to wait for a connection to become available.
            if (inFlight >= leastUsedConn.availableInProcess()) {
                logger.debug("Least used connection selected from pool for {} but inFlight [{}] >= availableInProcess [{}] - wait",
                        host, inFlight, availableInProcess);
                return waitForConnection(timeout, unit);
            }

            if (leastUsedConn.inFlight.compareAndSet(inFlight, inFlight + 1)) {
                logger.debug("Return least used {} on {}", leastUsedConn, host);
                return leastUsedConn;
            }
        }
    }

    public void returnConnection(final Connection connection) throws ConnectionException {
        logger.debug("Attempting to return {} on {}", connection, host);
        if (isClosed()) throw new ConnectionException(host.getHostUri(), host.getAddress(), "Pool is shutdown");

        int inFlight = connection.inFlight.decrementAndGet();
        if (connection.isDead()) {
            logger.debug("Marking {} as dead", this.host);
            considerUnavailable();
        } else {
            if (bin.contains(connection) && inFlight == 0) {
                logger.debug("{} is already in the bin and it has no inflight requests so it is safe to close", connection);
                if (bin.remove(connection))
                    connection.closeAsync();
                return;
            }

            // destroy a connection that exceeds the minimum pool size - it does not have the right to live if it
            // isn't busy. replace a connection that has a low available in process count which likely means that
            // it's backing up with requests that might never have returned. if neither of these scenarios are met
            // then let the world know the connection is available.
            final int poolSize = connections.size();
            final int availableInProcess = connection.availableInProcess();
            if (poolSize > minPoolSize && inFlight <= minSimultaneousRequestsPerConnection) {
                logger.debug("On {} pool size of {} > minPoolSize {} and inFlight of {} <= minSimultaneousRequestsPerConnection {} so destroy {}",
                        host, poolSize, minPoolSize, inFlight, minSimultaneousRequestsPerConnection, connection);
                destroyConnection(connection);
            } else if (connection.availableInProcess() < minInProcess) {
                logger.debug("On {} availableInProcess {} < minInProcess {} so replace {}", host, availableInProcess, minInProcess, connection);
                replaceConnection(connection);
            } else
                announceAvailableConnection();
        }
    }

    public boolean isClosed() {
        return closeFuture.get() != null;
    }

    /**
     * Permanently kills the pool.
     */
    public CompletableFuture<Void> closeAsync() {
        logger.info("Signalled closing of connection pool on {} with core size of {}", host, minPoolSize);

        CompletableFuture<Void> future = closeFuture.get();
        if (future != null)
            return future;

        announceAllAvailableConnection();
        future = CompletableFuture.allOf(killAvailableConnections());

        return closeFuture.compareAndSet(null, future) ? future : closeFuture.get();
    }

    public int opened() {
        return open.get();
    }

    private CompletableFuture[] killAvailableConnections() {
        final List<CompletableFuture<Void>> futures = new ArrayList<>(connections.size());
        for (Connection connection : connections) {
            final CompletableFuture<Void> future = connection.closeAsync();
            future.thenRunAsync(open::decrementAndGet);
            futures.add(future);
        }
        return futures.toArray(new CompletableFuture[futures.size()]);
    }

    private void replaceConnection(final Connection connection) {
        logger.debug("Replace {}", connection);

        open.decrementAndGet();
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
            connections.add(new Connection(host.getHostUri(), this, cluster, settings().maxInProcessPerConnection));
        } catch (ConnectionException ce) {
            logger.debug("Connections were under max, but there was an error creating the connection.", ce);
            considerUnavailable();
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
        bin.add(connection);
        connections.remove(connection);

        if (connection.inFlight.get() == 0 && bin.remove(connection))
            connection.closeAsync();

        logger.debug("{} destroyed", connection);
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

            final Connection leastUsed = selectLeastUsed();
            if (leastUsed != null) {
                while (true) {
                    final int inFlight = leastUsed.inFlight.get();
                    final int availableInProcess = leastUsed.availableInProcess();
                    if (inFlight >= availableInProcess) {
                        logger.debug("Least used {} on {} has requests inFlight [{}] >= availableInProcess [{}] - may timeout waiting for connection",
                                leastUsed, host, inFlight, availableInProcess);
                        break;
                    }

                    if (leastUsed.inFlight.compareAndSet(inFlight, inFlight + 1)) {
                        logger.debug("Return least used {} on {} after waiting", leastUsed, host);
                        return leastUsed;
                    }
                }
            }

            remaining = to - TimeUtil.timeSince(start, unit);
            logger.debug("Continue to wait for connection on {} if {} > 0", remaining);
        } while (remaining > 0);

        logger.debug("Timed-out waiting for connection on {} - possibly unavailable", host);

        // if we timeout borrowing a connection that might mean the host is dead (or the timeout was super short).
        // either way supply a function to reconnect
        this.considerUnavailable();

        throw new TimeoutException();
    }

    private void considerUnavailable() {
        // called when a connection is "dead" right now such that a "dead" connection means the host is basically
        // "dead".  that's probably ok for now, but this decision should likely be more flexible.
        host.makeUnavailable(this::tryReconnect);

        // let the load-balancer know that the host is acting poorly
        this.cluster.loadBalancingStrategy().onUnavailable(host);

    }

    private boolean tryReconnect(final Host h) {
        logger.debug("Trying to re-establish connection on {}", host);

        try {

            connections.add(new Connection(host.getHostUri(), this, cluster, settings().maxInProcessPerConnection));
            this.open.set(connections.size());

            // host is reconnected and a connection is now available
            this.cluster.loadBalancingStrategy().onAvailable(host);
            return true;
        } catch (Exception ex) {
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
            int inFlight = connection.inFlight.get();
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

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ConnectionPool (");
        sb.append(host);
        sb.append(") - ");
        connections.forEach(c -> {
            sb.append(c);
            sb.append(",");
        });
        return sb.toString().trim();
    }
}
