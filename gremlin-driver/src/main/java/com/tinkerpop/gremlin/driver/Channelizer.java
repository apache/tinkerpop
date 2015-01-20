package com.tinkerpop.gremlin.driver;

import com.tinkerpop.gremlin.driver.handler.NioGremlinRequestEncoder;
import com.tinkerpop.gremlin.driver.handler.NioGremlinResponseDecoder;
import com.tinkerpop.gremlin.driver.handler.WebSocketClientHandler;
import com.tinkerpop.gremlin.driver.handler.WebSocketGremlinRequestEncoder;
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
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.util.SelfSignedCertificate;

import java.util.Optional;
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
    public default void connected() {
    }

    ;

    abstract class AbstractChannelizer extends ChannelInitializer<SocketChannel> implements Channelizer {
        protected Connection connection;
        protected Cluster cluster;
        private ConcurrentMap<UUID, ResponseQueue> pending;

        protected static final String PIPELINE_GREMLIN_HANDLER = "gremlin-handler";

        public boolean supportsSsl() {
            return cluster.connectionPoolSettings().enableSsl;
        }

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
            final Optional<SslContext> sslCtx;
            if (supportsSsl()) {
                try {
                    final SelfSignedCertificate ssc = new SelfSignedCertificate();
                    sslCtx = Optional.of(SslContext.newServerContext(ssc.certificate(), ssc.privateKey()));
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            } else {
                sslCtx = Optional.empty();
            }

            if (sslCtx.isPresent()) {
                pipeline.addLast(sslCtx.get().newHandler(socketChannel.alloc(), connection.getUri().getHost(), connection.getUri().getPort()));
            }

            configure(pipeline);
            pipeline.addLast(PIPELINE_GREMLIN_HANDLER, new Handler.GremlinResponseHandler(pending));
        }
    }

    class WebSocketChannelizer extends AbstractChannelizer {
        private WebSocketClientHandler handler;

        @Override
        public boolean supportsSsl() {
            final String scheme = connection.getUri().getScheme();
            return "wss".equalsIgnoreCase(scheme);
        }

        @Override
        public void configure(final ChannelPipeline pipeline) {
            final String scheme = connection.getUri().getScheme();
            if (!"ws".equalsIgnoreCase(scheme) && !"wss".equalsIgnoreCase(scheme))
                throw new IllegalStateException("Unsupported scheme (only ws: or wss: supported): " + scheme);

            if (!supportsSsl() && "wss".equalsIgnoreCase(scheme))
                throw new IllegalStateException("To use wss scheme ensure that enableSsl is set to true in configuration");

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
