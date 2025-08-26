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
import org.apache.tinkerpop.gremlin.driver.handler.HttpGremlinRequestEncoder;
import org.apache.tinkerpop.gremlin.driver.handler.HttpGremlinResponseDecoder;
import org.apache.tinkerpop.gremlin.driver.handler.WebSocketClientHandler;
import org.apache.tinkerpop.gremlin.driver.handler.WebSocketGremlinRequestEncoder;
import org.apache.tinkerpop.gremlin.driver.handler.WebSocketGremlinResponseDecoder;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.handler.codec.http.websocketx.extensions.compression.WebSocketClientCompressionHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
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
     * Gets the scheme to use to construct the URL and by default uses HTTP.
     */
    public default String getScheme(final boolean sslEnabled) {
        return sslEnabled ? "https" : "http";
    }

    /**
     * Base implementation of the client side {@link Channelizer}.
     */
    abstract class AbstractChannelizer extends ChannelInitializer<SocketChannel> implements Channelizer {
        protected Connection connection;
        protected Cluster cluster;
        protected ConcurrentMap<UUID, ResultQueue> pending;

        protected static final String PIPELINE_GREMLIN_SASL_HANDLER = "gremlin-sasl-handler";
        protected static final String PIPELINE_GREMLIN_HANDLER = "gremlin-handler";
        public static final String PIPELINE_SSL_HANDLER = "gremlin-ssl-handler";

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
                final SslHandler sslHandler = sslCtx.get().newHandler(socketChannel.alloc(), connection.getUri().getHost(), connection.getUri().getPort());
                // TINKERPOP-2814. Remove the SSL handshake timeout so that handshakes that take longer than 10000ms
                // (Netty default) but less than connectionSetupTimeoutMillis can succeed. This means the SSL handshake
                // will instead be capped by connectionSetupTimeoutMillis.
                sslHandler.setHandshakeTimeoutMillis(0);
                pipeline.addLast(PIPELINE_SSL_HANDLER, sslHandler);
            }

            configure(pipeline);
            pipeline.addLast(PIPELINE_GREMLIN_SASL_HANDLER, new Handler.GremlinSaslAuthenticationHandler(cluster.authProperties()));
            pipeline.addLast(PIPELINE_GREMLIN_HANDLER, new Handler.GremlinResponseHandler(pending));
        }
    }

    /**
     * WebSocket {@link Channelizer} implementation.
     */
    class WebSocketChannelizer extends AbstractChannelizer {
        private static final Logger logger = LoggerFactory.getLogger(WebSocketChannelizer.class);
        private WebSocketClientHandler handler;

        private WebSocketGremlinRequestEncoder gremlinRequestEncoder;
        private WebSocketGremlinResponseDecoder gremlinResponseDecoder;

        @Override
        public void init(final Connection connection) {
            super.init(connection);
            gremlinRequestEncoder = new WebSocketGremlinRequestEncoder(true, cluster.getSerializer());
            gremlinResponseDecoder = new WebSocketGremlinResponseDecoder(cluster.getSerializer());
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
            final HttpHeaders httpHeaders = new DefaultHttpHeaders();
            if(connection.getCluster().isUserAgentOnConnectEnabled()) {
                httpHeaders.set(UserAgent.USER_AGENT_HEADER_NAME, UserAgent.USER_AGENT);
            }
            handler = new WebSocketClientHandler(
                    new WebSocketClientHandler.InterceptedWebSocketClientHandshaker13(
                            connection.getUri(), WebSocketVersion.V13, null, true,
                            httpHeaders, maxContentLength, true, false, -1,
                            cluster.getRequestInterceptor()), cluster.getConnectionSetupTimeout(), supportsSsl());

            final int keepAliveInterval = toIntExact(TimeUnit.SECONDS.convert(
                    cluster.connectionPoolSettings().keepAliveInterval, TimeUnit.MILLISECONDS));

            pipeline.addLast("http-codec", new HttpClientCodec());
            pipeline.addLast("aggregator", new HttpObjectAggregator(maxContentLength));
            if (connection.getCluster().enableCompression()) {
                // Add compression extension for WebSocket defined in https://tools.ietf.org/html/rfc7692
                pipeline.addLast(WebSocketClientCompressionHandler.INSTANCE);
            }
            pipeline.addLast("idle-state-Handler", new IdleStateHandler(0, keepAliveInterval, 0));
            pipeline.addLast("ws-handler", handler);
            pipeline.addLast("gremlin-encoder", gremlinRequestEncoder);
            pipeline.addLast("gremlin-decoder", gremlinResponseDecoder);
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

        @Override
        public String getScheme(boolean sslEnabled) {
            return sslEnabled ? "wss" : "ws";
        }
    }

    /**
     * Sends requests over the HTTP endpoint. Client functionality is governed by the limitations of the HTTP endpoint,
     * meaning that sessions are not available and as such {@code tx()} (i.e. transactions) are not available over this
     * channelizer. Only sessionless requests are possible. Some driver configuration options may not be relevant when
     * using HTTP, such as {@link Tokens#ARGS_BATCH_SIZE} since HTTP does not stream results back in that fashion.
     */
    class HttpChannelizer extends AbstractChannelizer {

        private HttpClientCodec handler;

        private HttpGremlinRequestEncoder gremlinRequestEncoder;
        private HttpGremlinResponseDecoder gremlinResponseDecoder;

        @Override
        public void init(final Connection connection) {
            super.init(connection);

            // server does not support sessions so this channerlizer can't support the SessionedClient
            if (connection.getClient() instanceof Client.SessionedClient)
                throw new IllegalStateException(String.format("Cannot use sessions or tx() with %s", HttpChannelizer.class.getSimpleName()));

            gremlinRequestEncoder = new HttpGremlinRequestEncoder(cluster.getSerializer(), cluster.getRequestInterceptor(), cluster.isUserAgentOnConnectEnabled());
            gremlinResponseDecoder = new HttpGremlinResponseDecoder(cluster.getSerializer());
        }

        @Override
        public void connected() {
            super.connected();
        }

        @Override
        public boolean supportsSsl() {
            final String scheme = connection.getUri().getScheme();
            return "https".equalsIgnoreCase(scheme);
        }

        @Override
        public void configure(final ChannelPipeline pipeline) {
            final String scheme = connection.getUri().getScheme();
            if (!"http".equalsIgnoreCase(scheme) && !"https".equalsIgnoreCase(scheme))
                throw new IllegalStateException("Unsupported scheme (only http: or https: supported): " + scheme);

            if (!supportsSsl() && "https".equalsIgnoreCase(scheme))
                throw new IllegalStateException("To use https scheme ensure that enableSsl is set to true in configuration");

            final int maxContentLength = cluster.connectionPoolSettings().maxContentLength;
            handler = new HttpClientCodec();

            pipeline.addLast("http-codec", handler);
            pipeline.addLast("aggregator", new HttpObjectAggregator(maxContentLength));
            pipeline.addLast("gremlin-encoder", gremlinRequestEncoder);
            pipeline.addLast("gremlin-decoder", gremlinResponseDecoder);
        }
    }
}
