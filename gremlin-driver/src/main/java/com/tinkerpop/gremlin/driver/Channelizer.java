package com.tinkerpop.gremlin.driver;

import com.tinkerpop.gremlin.driver.handler.WebSocketGremlinRequestEncoder;
import com.tinkerpop.gremlin.driver.handler.NioGremlinRequestEncoder;
import com.tinkerpop.gremlin.driver.handler.NioGremlinResponseDecoder;
import com.tinkerpop.gremlin.driver.handler.WebSocketClientHandler;
import com.tinkerpop.gremlin.driver.handler.WebSocketGremlinResponseDecoder;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;

import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface Channelizer extends ChannelHandler {

    /**
     * Initializes the {@code Channelizer}. Called just after construction.
     */
    public void init(final Connection connection);

    /**
     * Called after the channel connects. The {@code Channelizer} may need to perform some functions, such as a
     * handshake.
     */
    public default void connected() {};

    abstract class AbstractChannelizer extends ChannelInitializer<SocketChannel> implements Channelizer {
        protected Connection connection;
        protected Cluster cluster;
        private ConcurrentMap<UUID, ResponseQueue> pending;

        protected static final String PIPELINE_GREMLIN_HANDLER = "gremlin-handler";

        public abstract void configure(final ChannelPipeline pipeline);
        public void finalize(final ChannelPipeline pipeline) {
            // do nothing
        }

        @Override
        public void init(final Connection connection) {
            this.connection = connection;
            this.cluster = connection.getCluster();
            this.pending = connection.getPending();
        }

        @Override
        protected void initChannel(final SocketChannel socketChannel) throws Exception {
            final ChannelPipeline pipeline = socketChannel.pipeline();

            configure(pipeline);
            pipeline.addLast(PIPELINE_GREMLIN_HANDLER, new Handler.GremlinResponseHandler(pending));
        }
    }

    class WebSocketChannelizer extends AbstractChannelizer {
        private WebSocketClientHandler handler;

        @Override
        public void configure(final ChannelPipeline pipeline) {
            final String protocol = connection.getUri().getScheme();
            if (!"ws".equals(protocol))
                throw new IllegalArgumentException("Unsupported protocol: " + protocol);

            final int maxContentLength = cluster.connectionPoolSettings().maxContentLength;
            handler = new WebSocketClientHandler(
                    WebSocketClientHandshakerFactory.newHandshaker(
                            connection.getUri(), WebSocketVersion.V13, null, false, HttpHeaders.EMPTY_HEADERS, maxContentLength));

            pipeline.addLast("http-codec", new HttpClientCodec());
            pipeline.addLast("aggregator", new HttpObjectAggregator(maxContentLength));
            pipeline.addLast("ws-handler", handler);
            pipeline.addLast("gremlin-encoder", new WebSocketGremlinRequestEncoder(true, cluster.getSerializer()));
            pipeline.addLast("gremlin-decoder", new WebSocketGremlinResponseDecoder(cluster.getSerializer()));
        }

        @Override
        public void connected() {
            try {
                handler.handshakeFuture().sync();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    class NioChannelizer extends AbstractChannelizer {
        @Override
        public void configure(ChannelPipeline pipeline) {
            pipeline.addLast("gremlin-decoder", new NioGremlinResponseDecoder(cluster.getSerializer()));
            pipeline.addLast("gremlin-encoder", new NioGremlinRequestEncoder(true, cluster.getSerializer()));
        }
    }
}
