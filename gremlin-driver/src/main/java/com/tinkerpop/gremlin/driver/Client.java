package com.tinkerpop.gremlin.driver;

import com.tinkerpop.gremlin.driver.message.RequestMessage;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Client {
    private final Cluster cluster;
    private volatile boolean initialized;

    // todo: each host gets a connection pool?
    private ConcurrentMap<Host, Connection> connections = new ConcurrentHashMap<>();

    Client(final Cluster cluster) {
        this.cluster = cluster;

    }

    public synchronized Client init() {
        if (initialized)
            return this;

        cluster.init();

        // todo: connection pooling

        cluster.getClusterInfo().allHosts().forEach(host -> connections.put(host, new Connection(host.getWebSocketUri(), cluster)));
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

        // todo: choose a connection smartly.
        final Connection connection  = connections.values().iterator().next();
        final RequestMessage request = RequestMessage.create("eval")
                .add(Tokens.ARGS_GREMLIN, gremlin).build();
        connection.write(request, future);

        return future;
    }
}
