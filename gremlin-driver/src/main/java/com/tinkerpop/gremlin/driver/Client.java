package com.tinkerpop.gremlin.driver;

import com.tinkerpop.gremlin.driver.message.RequestMessage;
import io.netty.channel.ChannelPromise;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Client {
    private final Cluster cluster;
    private volatile boolean initialized;

    // todo: each host gets a connection pool?
    private ConcurrentMap<Host, Connection> connections = new ConcurrentHashMap<>();

    public Client(final Cluster cluster) {
        this.cluster = cluster;
    }

    public synchronized Client init() throws Exception {
        if (initialized)
            return this;

        connections.put(new Host(new InetSocketAddress("localhost", 8182)), new Connection(URI.create("ws://localhost:8182/gremlin"), new Connection.Factory()));
        /*
        for (Host host : cluster.getClusterInfo().allHosts()) {
            // todo: build pool and use host to construct URI
        }
        */
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
        final CompletableFuture<ResultSet> future = new CompletableFuture<>();

        // todo: choose a connection smartly...get into async
        final Connection connection  = connections.values().iterator().next();
        final RequestMessage request = RequestMessage.create("eval")
                .add(Tokens.ARGS_GREMLIN, gremlin, Tokens.ARGS_ACCEPT, "application/json").build();
        connection.write(request, future);

        return future;
    }
}
