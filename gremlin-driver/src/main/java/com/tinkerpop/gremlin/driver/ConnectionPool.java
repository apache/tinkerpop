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
        for (int i = 0; i < minPoolSize; i++)
            l.add(new Connection(host.getWebSocketUri(), this, cluster, settings.maxInProcessPerConnection));

        this.connections = new CopyOnWriteArrayList<>(l);
        this.open = new AtomicInteger(connections.size());

        logger.info("Opening connection pool on {} with core size of {}", host, minPoolSize);
    }

    public Settings.ConnectionPoolSettings settings() {
        return cluster.connectionPoolSettings();
    }

    public Connection borrowConnection(final long timeout, final TimeUnit unit) throws TimeoutException, ConnectionException {
        if (isClosed()) throw new ConnectionException(host.getWebSocketUri(), host.getAddress(), "Pool is shutdown");

        final Connection leastUsedConn = selectLeastUsed();

        if (connections.isEmpty()) {
            for (int i = 0; i < minPoolSize; i++) {
                scheduledForCreation.incrementAndGet();
                newConnection();
            }

            return waitForConnection(timeout, unit);
        }

		if (null == leastUsedConn) {
			if (isClosed()) throw new ConnectionException(host.getWebSocketUri(), host.getAddress(), "Pool is shutdown");
			return waitForConnection(timeout, unit);
		}

        // if the number in flight on the least used connection exceeds the max allowed and the pool size is
        // not at maximum then consider opening a connection
        if (leastUsedConn.inFlight.get() >= maxSimultaneousRequestsPerConnection && connections.size() < maxPoolSize)
            considerNewConnection();

		while (true) {
			int inFlight = leastUsedConn.inFlight.get();

			// if the number in flight starts to exceed what's available for this connection, then we need
			// to wait for a connection to become available.
			if (inFlight >= leastUsedConn.availableInProcess()) {
				return waitForConnection(timeout, unit);
			}

			if (leastUsedConn.inFlight.compareAndSet(inFlight, inFlight + 1)) {
				return leastUsedConn;
			}
		}
    }

    public void returnConnection(final Connection connection) throws ConnectionException {
        if (isClosed()) throw new ConnectionException(host.getWebSocketUri(), host.getAddress(), "Pool is shutdown");

        int inFlight = connection.inFlight.decrementAndGet();
        if (connection.isDead()) {
            // a dead connection signifies a likely dead host - given that assumption close the pool.  we could likely
			// have a smarter and more configurable choice here, but for now this is ok.
			closeAsync();
        } else {
            if (bin.contains(connection) && inFlight == 0) {
                if (bin.remove(connection))
                    connection.closeAsync();
                return;
            }

            // destroy a connection that exceeds the minimum pool size - it does not have the right to live if it
            // isn't busy. replace a connection that has a low available in process count which likely means that
            // it's backing up with requests that might never have returned. if neither of these scenarios are met
            // then let the world know the connection is available.
            if (connections.size() > minPoolSize && inFlight <= minSimultaneousRequestsPerConnection)
                destroyConnection(connection);
            else if (connection.availableInProcess() < minInProcess)
                replaceConnection(connection);
            else
                announceAvailableConnection();
        }
    }

    public boolean isClosed() {
        return closeFuture.get() != null;
    }

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
        open.decrementAndGet();
        considerNewConnection();
        definitelyDestroyConnection(connection);
    }

    private void considerNewConnection() {
        while (true) {
            int inCreation = scheduledForCreation.get();

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
        while(true) {
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

        connections.add(new Connection(host.getWebSocketUri(), this, cluster, settings().maxInProcessPerConnection));
        announceAvailableConnection();
        return true;
    }


    private boolean destroyConnection(final Connection connection) {
        while(true) {
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

            if (isClosed()) throw new ConnectionException(host.getWebSocketUri(), host.getAddress(), "Pool is shutdown");

            final Connection leastUsed = selectLeastUsed();
            if (leastUsed != null) {
                while (true) {
                    int inFlight = leastUsed.inFlight.get();
                    if (inFlight >= leastUsed.availableInProcess())
                        break;

                    if (leastUsed.inFlight.compareAndSet(inFlight, inFlight + 1))
                        return leastUsed;
                }
            }

            remaining = to - TimeUtil.timeSince(start, unit);
        } while (remaining > 0);

        throw new TimeoutException();
    }

    private void announceAvailableConnection() {
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
            if (inFlight < minInFlight) {
                minInFlight = inFlight;
                leastBusy = connection;
            }
        }
        return leastBusy;
    }

    private void awaitAvailableConnection(long timeout, TimeUnit unit) throws InterruptedException {
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
        connections.forEach(c-> {
            sb.append(c);
            sb.append(",");
        });
        return sb.toString().trim();
    }
}
