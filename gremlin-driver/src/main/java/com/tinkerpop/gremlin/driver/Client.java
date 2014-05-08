package com.tinkerpop.gremlin.driver;

import com.tinkerpop.gremlin.driver.message.RequestMessage;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.util.Serializer;
import com.tinkerpop.gremlin.util.function.SFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
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
        return submit(gremlin, (Map<String,Object>) null);
    }

    public ResultSet submit(final String gremlin, final Map<String, Object> parameters) {
        try {
            return submitAsync(gremlin, parameters).get();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public ResultSet submit(final SFunction<Graph, Traversal> traversal) {
        return submit("g", traversal);
    }

    public ResultSet submit(final String graph, final SFunction<Graph, Traversal> traversal) {
        try {
            return submitAsync(graph, traversal).get();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public CompletableFuture<ResultSet> submitAsync(final SFunction<Graph, Traversal> traversal) {
        return submitAsync("g", traversal);
    }

    public CompletableFuture<ResultSet> submitAsync(final String graph, final SFunction<Graph, Traversal> traversal) {
        try {
            byte[] bytes = Serializer.serializeObject(traversal);
            final RequestMessage.Builder request = RequestMessage.create(Tokens.OPS_TRAVERSE)
                    .add(Tokens.ARGS_GREMLIN, bytes)
                    .add(Tokens.ARGS_GRAPH_NAME, graph);
            return submitAsync(request.build());
        } catch (IOException ioe) {
            ioe.printStackTrace();
            throw new RuntimeException(ioe);
        }
    }

    public CompletableFuture<ResultSet> submitAsync(final String gremlin) {
        return submitAsync(gremlin, (Map<String,Object>) null);
    }

    public CompletableFuture<ResultSet> submitAsync(final String gremlin, final Map<String, Object> parameters) {
        final RequestMessage.Builder request = RequestMessage.create(Tokens.OPS_EVAL).add(Tokens.ARGS_GREMLIN, gremlin);
        Optional.ofNullable(parameters).ifPresent(params -> request.addArg(Tokens.ARGS_BINDINGS, parameters));
        return submitAsync(request.build());
    }

    public CompletableFuture<ResultSet> submitAsync(final RequestMessage msg) {
        if (!initialized)
            init();

        final CompletableFuture<ResultSet> future = new CompletableFuture<>();

        // todo: choose a host with some smarts - this is pretty whatever atm
        final ConnectionPool pool = hostConnectionPools.values().iterator().next();
        try {
            // the connection is returned to the pool once the response has been completed...see Connection.write()
            final Connection connection = pool.borrowConnection(300, TimeUnit.SECONDS);
            connection.write(msg, future);
            return future;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        } finally {
            if (logger.isDebugEnabled()) logger.debug("Submitted {} to - {}", msg, pool);
        }
    }
}
