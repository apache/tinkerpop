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

import io.netty.channel.Channel;
import io.netty.handler.codec.http.EmptyHttpHeaders;
import org.apache.tinkerpop.gremlin.driver.exception.ConnectionException;
import org.apache.tinkerpop.gremlin.driver.handler.WebSocketClientHandler;
import org.apache.tinkerpop.gremlin.driver.handler.WebSocketCloseHandler;
import org.apache.tinkerpop.gremlin.driver.handler.WebSocketGremlinRequestEncoder;
import org.apache.tinkerpop.gremlin.driver.handler.WebSocketGremlinResponseDecoder;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolHandler;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.timeout.IdleStateHandler;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
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

    public default void init(final ConnectionPool connectionPool) { throw new UnsupportedOperationException(); }

    public default void connected(final Channel ch) {
    }

    /**
     * Base implementation of the client side {@code Channelizer}.
     */
    abstract class AbstractChannelizer extends ChannelInitializer<SocketChannel> implements Channelizer {
        protected ConnectionPool connectionPool;
        protected Cluster cluster;
        protected Handler.GremlinResponseHandler gremlinResponseHandler;

        protected static final String PIPELINE_GREMLIN_SASL_HANDLER = "gremlin-sasl-handler";
        protected static final String PIPELINE_GREMLIN_HANDLER = "gremlin-handler";

        public boolean supportsSsl() {
            return cluster.connectionPoolSettings().enableSsl;
        }

        public abstract void configure(final ChannelPipeline pipeline);

        @Override
        public void init(final ConnectionPool connPool) {
            this.connectionPool = connPool;
            this.cluster = connPool.getCluster();
            this.gremlinResponseHandler = new Handler.GremlinResponseHandler();
        }

        @Override
        protected void initChannel(final SocketChannel socketChannel) {
            final ChannelPipeline pipeline = socketChannel.pipeline();
            final Optional<SslContext> sslCtxOpt;
            if (supportsSsl()) {
                try {
                    sslCtxOpt = Optional.of(cluster.createSSLContext());
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            } else {
                sslCtxOpt = Optional.empty();
            }

            sslCtxOpt.ifPresent((sslCtx) -> {
                pipeline.addLast(sslCtx.newHandler(socketChannel.alloc(),
                                                   connectionPool.getHost().getHostUri().getHost(),
                                                   connectionPool.getHost().getHostUri().getPort()));
            });

            configure(pipeline);
            pipeline.addLast(PIPELINE_GREMLIN_SASL_HANDLER, new Handler.GremlinSaslAuthenticationHandler(cluster.authProperties()));
            pipeline.addLast(PIPELINE_GREMLIN_HANDLER, gremlinResponseHandler);
        }
    }

    /**
     * WebSocket {@link Channelizer} implementation.
     */
    public final class WebSocketChannelizer extends AbstractChannelizer {

        private WebSocketGremlinRequestEncoder webSocketGremlinRequestEncoder;
        private WebSocketGremlinResponseDecoder webSocketGremlinResponseDecoder;

        @Override
        public void init(final ConnectionPool connpool) {
            super.init(connpool);
            webSocketGremlinRequestEncoder = new WebSocketGremlinRequestEncoder(true, cluster.getSerializer());
            webSocketGremlinResponseDecoder = new WebSocketGremlinResponseDecoder(cluster.getSerializer());
        }

        @Override
        public boolean supportsSsl() {
            final String scheme = connectionPool.getHost().getHostUri().getScheme();
            return "wss".equalsIgnoreCase(scheme);
        }

        @Override
        public void configure(final ChannelPipeline pipeline) {
            final String scheme = connectionPool.getHost().getHostUri().getScheme();
            if (!"ws".equalsIgnoreCase(scheme) && !"wss".equalsIgnoreCase(scheme))
                throw new IllegalStateException("Unsupported scheme (only ws: or wss: supported): " + scheme);

            if (!supportsSsl() && "wss".equalsIgnoreCase(scheme))
                throw new IllegalStateException("To use wss scheme ensure that enableSsl is set to true in configuration");

            final int maxContentLength = cluster.connectionPoolSettings().maxContentLength;

            final WebSocketClientHandshaker handshaker = WebSocketClientHandshakerFactory.newHandshaker(
                    connectionPool.getHost().getHostUri(), WebSocketVersion.V13, null, false,
                    EmptyHttpHeaders.INSTANCE, maxContentLength);
            final WebSocketClientProtocolHandler nettyWsHandler = new WebSocketClientProtocolHandler(
                    handshaker, true, false, 9000);
            final WebSocketClientHandler handler = new WebSocketClientHandler(connectionPool.getActiveChannels());

            final int keepAliveInterval = toIntExact(TimeUnit.SECONDS.convert(cluster.connectionPoolSettings().keepAliveInterval, TimeUnit.MILLISECONDS));
            pipeline.addLast("http-codec", new HttpClientCodec());
            pipeline.addLast("aggregator", new HttpObjectAggregator(maxContentLength));
            pipeline.addLast("netty-idle-state-Handler", new IdleStateHandler(0, keepAliveInterval, 0));
            pipeline.addLast("netty-ws-handler", nettyWsHandler);
            pipeline.addLast("ws-client-handler", handler);
            pipeline.addLast("ws-close-handler", new WebSocketCloseHandler());
            pipeline.addLast("gremlin-encoder", webSocketGremlinRequestEncoder);
            pipeline.addLast("gremlin-decoder", webSocketGremlinResponseDecoder);
        }


        @Override
        public void connected(final Channel ch) {
            try {
                // block for a few seconds - if the handshake takes longer than there's gotta be issues with that
                // server. more than likely, SSL is enabled on the server, but the client forgot to enable it or
                // perhaps the server is not configured for websockets.
                ((WebSocketClientHandler)(ch.pipeline().get("ws-client-handler"))).handshakeFuture().addListener( f -> {
                    if (!f.isSuccess()) {
                        throw new ConnectionException(connectionPool.getHost().getHostUri(),
                                                                           "Could not complete websocket handshake - ensure that client protocol matches server", f.cause());
                    }
                }).get(1500, TimeUnit.MILLISECONDS);
            } catch (ExecutionException ex) {
                throw new RuntimeException(ex.getCause());
            } catch (InterruptedException | TimeoutException ex) {
                // catching the InterruptedException will reset the interrupted flag. This is intentional.
                throw new RuntimeException(new ConnectionException(connectionPool.getHost().getHostUri(),
                                                                   "Timed out while performing websocket handshake - ensure that client protocol matches server", ex.getCause()));
            }
        }
    }
}
