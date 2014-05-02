package com.tinkerpop.gremlin.driver;

import com.tinkerpop.gremlin.driver.message.RequestMessage;
import com.tinkerpop.gremlin.driver.message.ResponseMessage;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;

import java.net.URI;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A single connection to a Gremlin Server instance.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class Connection {
    private final Channel channel;
    private final URI uri;
    private final ConcurrentMap<UUID, ResponseQueue> pending = new ConcurrentHashMap<>();
    private final Cluster cluster;
    private final ConnectionPool pool;

    // todo: configuration
    private static final int MAX_IN_PROCESS = 4;

    public final AtomicInteger inFlight = new AtomicInteger(0);
    private volatile boolean isDead = false;

    public Connection(final URI uri, final ConnectionPool pool, final Cluster cluster)  {
        this.uri = uri;
        this.cluster = cluster;
        this.pool = pool;

        final Bootstrap b = this.cluster.getFactory().createBootstrap();
        final String protocol = uri.getScheme();
        if (!"ws".equals(protocol))
            throw new IllegalArgumentException("Unsupported protocol: " + protocol);

        final ClientPipelineInitializer initializer = new ClientPipelineInitializer();
        b.channel(NioSocketChannel.class).handler(initializer);

        // todo: blocking
        try {
            channel = b.connect(uri.getHost(), uri.getPort()).sync().channel();
            initializer.handler.handshakeFuture().sync();
        } catch (InterruptedException ie) {
            ie.printStackTrace();
            throw new RuntimeException(ie);
        }
    }

    /**
     * A connection can only have so many things in process happening on it at once, where "in process" refers to
     * the number of in flight requests plus the number of pending responses.
     */
    public int availableInProcess() {
        return MAX_IN_PROCESS - pending.size();
    }

    public boolean isDead() {
        return isDead;
    }

    public ChannelPromise write(final RequestMessage requestMessage, final CompletableFuture<ResultSet> future) {
        // once there is a completed write, then create a holder for the result set and complete
        // the promise so that the client knows that that it can start checking for results.
        final Connection thisConnection = this;
        final ChannelPromise promise = channel.newPromise()
                .addListener(f -> {
                    final LinkedBlockingQueue<ResponseMessage> responseQueue = new LinkedBlockingQueue<>();
                    final CompletableFuture<Void> readCompleted = new CompletableFuture<>();
                    readCompleted.thenAcceptAsync(v -> thisConnection.returnToPool());
                    final ResponseQueue handler = new ResponseQueue(responseQueue, readCompleted);
                    pending.put(requestMessage.getRequestId(), handler);
                    final ResultSet resultSet = new ResultSet(handler);
                    future.complete(resultSet);
                });
        channel.writeAndFlush(requestMessage, promise);
        return promise;
    }

    public void returnToPool() {
        if (pool != null) pool.returnConnection(this);
    }

    public CompletableFuture<Void> close() {
        return CompletableFuture.runAsync(() ->  {
            channel.writeAndFlush(new CloseWebSocketFrame());
            try {
                channel.closeFuture().sync().get(120000, TimeUnit.MILLISECONDS);
            } catch (Exception ie) {
                throw new RuntimeException(ie);
            }
        });
    }

    @Override
    public String toString() {
        return "Connection{" +
                "inFlight=" + inFlight + "," +
                "pending=" + pending.size() +
                '}';
    }

    class ClientPipelineInitializer extends ChannelInitializer<SocketChannel> {

        // Connect with V13 (RFC 6455 aka HyBi-17). You can change it to V08 or V00.
        // If you change it to V00, ping is not supported and remember to change
        // HttpResponseDecoder to WebSocketHttpResponseDecoder in the pipeline.
        final Handler.WebSocketClientHandler handler;

        public ClientPipelineInitializer() {
            handler = new Handler.WebSocketClientHandler(
                          WebSocketClientHandshakerFactory.newHandshaker(
                              Connection.this.uri, WebSocketVersion.V13, null, false, HttpHeaders.EMPTY_HEADERS, 1280000));
        }

        @Override
        protected void initChannel(final SocketChannel socketChannel) throws Exception {
            final ChannelPipeline pipeline = socketChannel.pipeline();
            pipeline.addLast("http-codec", new HttpClientCodec());
            pipeline.addLast("aggregator", new HttpObjectAggregator(65536));
            pipeline.addLast("ws-handler", handler);
            pipeline.addLast("gremlin-encoder", new Handler.GremlinRequestEncoder(true, cluster.getSerializer()));
            pipeline.addLast("gremlin-decoder", new Handler.GremlinResponseDecoder(pending, cluster.getSerializer()));
        }
    }


}
