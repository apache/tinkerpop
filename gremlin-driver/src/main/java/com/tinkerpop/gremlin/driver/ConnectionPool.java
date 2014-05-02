package com.tinkerpop.gremlin.driver;

import com.tinkerpop.gremlin.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ConnectionPool {
    private static final Logger logger = LoggerFactory.getLogger(ConnectionPool.class);

    // todo: configuration
    private static final int MIN_POOL_SIZE = 2;
    private static final int MAX_POOL_SIZE = 8;
    private static final int MIN_SIMULTANEOUS_REQUESTS_PER_CONNECTION = 8;
    private static final int MAX_SIMULTANEOUS_REQUESTS_PER_CONNECTION = 16;

    public final Host host;
    private final Cluster cluster;
    private final List<Connection> connections;
    private final AtomicInteger open;
    private final Set<Connection> bin = new CopyOnWriteArraySet<>();

    private final AtomicInteger scheduledForCreation = new AtomicInteger();

    private volatile int waiter = 0;
    private final Lock waitLock = new ReentrantLock(true);
    private final Condition hasAvailableConnection = waitLock.newCondition();


    public ConnectionPool(final Host host, final Cluster cluster) {
        this.host = host;
        this.cluster = cluster;

        final List<Connection> l = new ArrayList<>(MIN_POOL_SIZE);
        for (int i = 0; i < MIN_POOL_SIZE; i++)
            l.add(new Connection(host.getWebSocketUri(), this, cluster));

        this.connections = new CopyOnWriteArrayList<>(l);
        this.open = new AtomicInteger(connections.size());
    }

    public Connection borrowConnection(final long timeout, final TimeUnit unit) throws TimeoutException {
        // todo: check if pool is closed or not before allowing a borrow
        final Connection leastUsedConn = selectLeastUsed();

        if (connections.isEmpty()) {
            for (int i = 0; i < MIN_POOL_SIZE; i++) {
                scheduledForCreation.incrementAndGet();
                newConnection();
            }

            return waitForConnection(timeout, unit);
        }

        // if the number in flight on the least used connection exceeds the max allowed and the pool size is
        // not at maximum then consider opening a connection
        if (leastUsedConn.inFlight.get() >= MAX_SIMULTANEOUS_REQUESTS_PER_CONNECTION && connections.size() < MAX_POOL_SIZE)
            considerNewConnection();

        if (leastUsedConn == null) {
            // todo: check close status
            return waitForConnection(timeout, unit);
        } else {
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
    }

    public void returnConnection(final Connection connection) {
        // todo: check if pool is closed....

        int inFlight = connection.inFlight.decrementAndGet();

        if (connection.isDead()) {
            // todo: probably need to signal that the host is hurting - maybe try to reopen?
        } else {
            if (bin.contains(connection) && inFlight == 0) {
                if (bin.remove(connection))
                    close(connection);
                return;
            }

            // a connection that exceeds the minimum pool size does not have the right to live if it isn't busy
            if (connections.size() > MIN_POOL_SIZE && inFlight <= MIN_SIMULTANEOUS_REQUESTS_PER_CONNECTION) {
                //logger.info("destroy");
                destroyConnection(connection);
            }  else {
                //logger.info("available");
                announceAvailableConnection();
            }
        }
    }

    private void close(final Connection connection) {
        connection.close().exceptionally(t -> {
            logger.warn("Connection did not close properly", t);
            return null;
        });
    }

    // todo: need a replace in this case?  do we properly cleanup in-process requests on failure
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

        logger.debug("Creating new connection on busy pool to {}", host);
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
            if (opened >= MAX_POOL_SIZE)
                return false;

            if (open.compareAndSet(opened, opened + 1))
                break;
        }

        // todo: check closed

        connections.add(new Connection(host.getWebSocketUri(), this, cluster));
        announceAvailableConnection();
        return true;
    }


    private boolean destroyConnection(final Connection connection) {
        while(true) {
            int opened = open.get();
            if (opened <= MIN_POOL_SIZE)
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
            close(connection);
    }

    private Connection waitForConnection(final long timeout, final TimeUnit unit) throws TimeoutException {
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

            // todo: check pool closed

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
        // Quick check if it's worth signaling to avoid locking
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
        final StringBuilder sb = new StringBuilder("ConnectionPool - ");
        connections.forEach(c-> {
            sb.append(c);
            sb.append(",");
        });
        return sb.toString().trim();
    }
}
