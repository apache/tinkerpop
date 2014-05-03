package com.tinkerpop.gremlin.driver;

import com.tinkerpop.gremlin.driver.message.RequestMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Client {
    private static final Logger logger = LoggerFactory.getLogger(Client.class);

    private final Cluster cluster;
    private volatile boolean initialized;

    private ConcurrentMap<Host, ConnectionPool> hostConnectionPools = new ConcurrentHashMap<>();

    Client(final Cluster cluster) {
        this.cluster = cluster;
    }

    public synchronized Client init() {
        if (initialized)
            return this;

        cluster.init();
        cluster.getClusterInfo().allHosts().forEach(host -> hostConnectionPools.put(host, new ConnectionPool(host, cluster)));

        initialized = true;
        return this;
    }

    public ResultSet submit(final String gremlin) {
        try {
            return submitAsync(gremlin).get();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public CompletableFuture<ResultSet> submitAsync(final String gremlin) {
        if (!initialized)
            init();

        final CompletableFuture<ResultSet> future = new CompletableFuture<>();

        // todo: choose a host with some smarts - this is pretty whatever atm
        final ConnectionPool pool = hostConnectionPools.values().iterator().next();
        try {
            // the connection is returned to the pool once the response has been completed...see Connection.write()
            final Connection connection = pool.borrowConnection(300, TimeUnit.SECONDS);
            final RequestMessage request = RequestMessage.create("eval")
                    .add(Tokens.ARGS_GREMLIN, gremlin).build();
            connection.write(request, future);
            return future;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        } finally {
            if (logger.isDebugEnabled()) logger.debug("Submitted {} to - {}", gremlin, pool);
        }
    }
}
