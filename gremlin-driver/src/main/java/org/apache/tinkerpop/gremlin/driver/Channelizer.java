/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.driver;

import org.apache.tinkerpop.gremlin.driver.exception.ConnectionException;
import org.apache.tinkerpop.gremlin.driver.handler.WebSocketClientHandler;
import org.apache.tinkerpop.gremlin.driver.handler.WebSocketGremlinRequestEncoder;
import org.apache.tinkerpop.gremlin.driver.handler.WebSocketGremlinResponseDecoder;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.EmptyHttpHeaders;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.handler.codec.http.websocketx.extensions.compression.WebSocketClientCompressionHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.timeout.IdleStateHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.lang.Math.toIntExact;

/**
 * Client-side channel initializer interface.  It is responsible for constructing the Netty {@code ChannelPipeline}
 * used by the client to connect and send message to Gremlin Server.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface Channelizer extends ChannelHandler {

    /**
     * Initializes the {@code Channelizer}. Called just after construction.
     */
    public void init(final Connection connection);

    /**
     * Called on {@link Connection#closeAsync()} to perform an {@code Channelizer} specific functions.  Note that the
     * {@link Connection} already calls {@code Channel.close()} so there is no need to call that method here.
     * An implementation will typically use this method to send a {@code Channelizer} specific message to the
     * server to notify of shutdown coming from the client side (e.g. a "close" websocket frame).
     */
    public void close(final Channel channel);

    /**
     * Called after the channel connects. The {@code Channelizer} may need to perform some functions, such as a
     * handshake.
     */
    public default void connected() {
    }

    /**
     * Base implementation of the client side {@link Channelizer}.
     */
    abstract class AbstractChannelizer extends ChannelInitializer<SocketChannel> implements Channelizer {
        protected Connection connection;
        protected Cluster cluster;
        private ConcurrentMap<UUID, ResultQueue> pending;

        protected static final String PIPELINE_GREMLIN_SASL_HANDLER = "gremlin-sasl-handler";
        protected static final String PIPELINE_GREMLIN_HANDLER = "gremlin-handler";

        public boolean supportsSsl() {
            return cluster.connectionPoolSettings().enableSsl;
        }

        public abstract void configure(final ChannelPipeline pipeline);

        public void finalize(final ChannelPipeline pipeline) {
            // do nothing
        }

        @Override
        public void close(final Channel channel) {
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
                    sslCtx = Optional.of(cluster.createSSLContext());
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
            pipeline.addLast(PIPELINE_GREMLIN_SASL_HANDLER, new Handler.GremlinSaslAuthenticationHandler(cluster.authProperties()));
            pipeline.addLast(PIPELINE_GREMLIN_HANDLER, new Handler.GremlinResponseHandler(pending));
        }
    }

    /**
     * WebSocket {@link Channelizer} implementation.
     */
    public final class WebSocketChannelizer extends AbstractChannelizer {
        private static final Logger logger = LoggerFactory.getLogger(WebSocketChannelizer.class);
        private WebSocketClientHandler handler;

        private WebSocketGremlinRequestEncoder webSocketGremlinRequestEncoder;
        private WebSocketGremlinResponseDecoder webSocketGremlinResponseDecoder;

        @Override
        public void init(final Connection connection) {
            super.init(connection);
            webSocketGremlinRequestEncoder = new WebSocketGremlinRequestEncoder(true, cluster.getSerializer());
            webSocketGremlinResponseDecoder = new WebSocketGremlinResponseDecoder(cluster.getSerializer());
        }

        /**
         * Sends a {@code CloseWebSocketFrame} to the server for the specified channel.
         */
        @Override
        public void close(final Channel channel) {
            if (channel.isOpen()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Sending CloseWS frame to server from channel={}", channel.id().asShortText());
                }
                channel.writeAndFlush(new CloseWebSocketFrame());
            }
        }

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
                    new WebSocketClientHandler.InterceptedWebSocketClientHandshaker13(
                            connection.getUri(), WebSocketVersion.V13, null,true,
                            EmptyHttpHeaders.INSTANCE, maxContentLength, true, false, -1,
                            cluster.getHandshakeInterceptor()), cluster.getConnectionSetupTimeout());

            final int keepAliveInterval = toIntExact(TimeUnit.SECONDS.convert(
                    cluster.connectionPoolSettings().keepAliveInterval, TimeUnit.MILLISECONDS));

            pipeline.addLast("http-codec", new HttpClientCodec());
            pipeline.addLast("aggregator", new HttpObjectAggregator(maxContentLength));
            // Add compression extension for WebSocket defined in https://tools.ietf.org/html/rfc7692
            pipeline.addLast(WebSocketClientCompressionHandler.INSTANCE);
            pipeline.addLast("idle-state-Handler", new IdleStateHandler(0, keepAliveInterval, 0));
            pipeline.addLast("ws-handler", handler);
            pipeline.addLast("gremlin-encoder", webSocketGremlinRequestEncoder);
            pipeline.addLast("gremlin-decoder", webSocketGremlinResponseDecoder);
        }

        @Override
        public void connected() {
            try {
                // Block until the handshake is complete either successfully or with an error. The handshake future
                // will complete with a timeout exception after some time so it is guaranteed that this future will
                // complete.
                // If future completed with an exception more than likely, SSL is enabled on the server, but the client
                // forgot to enable it or perhaps the server is not configured for websockets.
                handler.handshakeFuture().sync();
            } catch (Exception ex) {
                String errMsg = "";
                if (ex instanceof TimeoutException) {
                    errMsg = "Timed out while waiting to complete the connection setup. Consider increasing the " +
                            "WebSocket handshake timeout duration.";
                } else {
                    errMsg = "Could not complete connection setup to the server. Ensure that SSL is correctly " +
                            "configured at both the client and the server. Ensure that client WebSocket handshake " +
                            "protocol matches the server. Ensure that the server is still reachable.";
                }
                throw new ConnectionException(connection.getUri(), errMsg, ex);
            }
        }
    }
}
