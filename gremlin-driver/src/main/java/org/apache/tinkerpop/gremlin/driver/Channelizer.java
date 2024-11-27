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
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleStateHandler;
import org.apache.tinkerpop.gremlin.driver.exception.ConnectionException;
import org.apache.tinkerpop.gremlin.driver.handler.GremlinResponseHandler;
import org.apache.tinkerpop.gremlin.driver.handler.HttpContentDecompressionHandler;
import org.apache.tinkerpop.gremlin.driver.handler.HttpGremlinRequestEncoder;
import org.apache.tinkerpop.gremlin.driver.handler.HttpGremlinResponseDecoder;
import org.apache.tinkerpop.gremlin.driver.handler.IdleConnectionHandler;
import org.apache.tinkerpop.gremlin.driver.handler.InactiveChannelHandler;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static io.netty.handler.codec.http.HttpClientCodec.DEFAULT_FAIL_ON_MISSING_RESPONSE;
import static io.netty.handler.codec.http.HttpClientCodec.DEFAULT_PARSE_HTTP_AFTER_CONNECT_REQUEST;
import static io.netty.handler.codec.http.HttpObjectDecoder.DEFAULT_ALLOW_DUPLICATE_CONTENT_LENGTHS;
import static io.netty.handler.codec.http.HttpObjectDecoder.DEFAULT_INITIAL_BUFFER_SIZE;
import static io.netty.handler.codec.http.HttpObjectDecoder.DEFAULT_MAX_HEADER_SIZE;
import static io.netty.handler.codec.http.HttpObjectDecoder.DEFAULT_MAX_INITIAL_LINE_LENGTH;
import static io.netty.handler.codec.http.HttpObjectDecoder.DEFAULT_VALIDATE_HEADERS;

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
    void init(final Connection connection);

    /**
     * Called on {@link Connection#closeAsync()} to perform an {@code Channelizer} specific functions.  Note that the
     * {@link Connection} already calls {@code Channel.close()} so there is no need to call that method here.
     * An implementation will typically use this method to send a {@code Channelizer} specific message to the
     * server to notify of shutdown coming from the client side.
     */
    void close(final Channel channel);

    /**
     * Called after the channel connects. The {@code Channelizer} may need to perform some functions, such as a
     * handshake.
     */
    default void connected() {
    }

    /**
     * Gets the scheme to use to construct the URL and by default uses HTTP.
     */
    default String getScheme(final boolean sslEnabled) {
        return sslEnabled ? "https" : "http";
    }

    /**
     * Base implementation of the client side {@link Channelizer}.
     */
    abstract class AbstractChannelizer extends ChannelInitializer<SocketChannel> implements Channelizer {
        protected Connection connection;
        protected Cluster cluster;
        protected SslHandler sslHandler;
        private AtomicReference<ResultQueue> pending;

        protected static final String PIPELINE_GREMLIN_HANDLER = "gremlin-handler";
        protected static final String PIPELINE_SSL_HANDLER = "gremlin-ssl-handler";
        protected static final String PIPELINE_IDLE_STATE_HANDLER = "idle-state-handler";
        protected static final String PIPELINE_IDLE_CONNECTION_HANDLER = "idle-connection-handler";
        protected static final String PIPELINE_HTTP_CODEC = "http-codec";
        protected static final String PIPELINE_HTTP_AGGREGATOR = "http-aggregator";
        protected static final String PIPELINE_HTTP_ENCODER = "gremlin-encoder";
        protected static final String PIPELINE_HTTP_DECODER = "gremlin-decoder";
        protected static final String PIPELINE_HTTP_DECOMPRESSION_HANDLER = "http-decompression-handler";
        
        private static final String HANDSHAKE_ERROR = "Could not complete connection setup to the server. Ensure that SSL is correctly " +
                "configured at both the client and the server. Ensure that client http handshake " +
                "protocol matches the server. Ensure that the server is still reachable.";

        private static final InactiveChannelHandler inactiveChannelHandler = new InactiveChannelHandler();

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
        protected void initChannel(final SocketChannel socketChannel) {
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
                sslHandler = sslCtx.get().newHandler(socketChannel.alloc(), connection.getUri().getHost(), connection.getUri().getPort());
                // TINKERPOP-2814. Remove the SSL handshake timeout so that handshakes that take longer than 10000ms
                // (Netty default) but less than connectionSetupTimeoutMillis can succeed. This means the SSL handshake
                // will instead be capped by connectionSetupTimeoutMillis.
                sslHandler.setHandshakeTimeoutMillis(0);
                pipeline.addLast(PIPELINE_SSL_HANDLER, sslHandler);
            } else {
                pipeline.addLast(PIPELINE_SSL_HANDLER, inactiveChannelHandler);
            }

            configure(pipeline);
            pipeline.addLast(PIPELINE_GREMLIN_HANDLER, new GremlinResponseHandler(pending));
        }

        @Override
        public void connected() {
            if (supportsSsl()) {
                try {
                    // Block until the handshake is complete either successfully or with an error.
                    sslHandler.handshakeFuture().sync();
                } catch (Exception ex) {
                    throw new ConnectionException(connection.getUri(), HANDSHAKE_ERROR, ex);
                }
            }
        }
    }

    /**
     * Sends requests over the HTTP endpoint. Client functionality is governed by the limitations of the HTTP endpoint,
     * meaning that sessions are not available and as such {@code tx()} (i.e. transactions) are not available over this
     * channelizer. Only sessionless requests are possible.
     */
    final class HttpChannelizer extends AbstractChannelizer {
        /**
         * This response is used as a signal for determining if all content of the response has been read.
         */
        public static final ResponseMessage LAST_CONTENT_READ_RESPONSE =
                ResponseMessage.build().code(HttpResponseStatus.NO_CONTENT).result(Collections.emptyList()).create();

        private HttpGremlinRequestEncoder gremlinRequestEncoder;
        private HttpGremlinResponseDecoder gremlinResponseDecoder;

        private HttpContentDecompressionHandler httpCompressionDecoder;
        private IdleStateHandler idleStateHandler;
        private IdleConnectionHandler idleConnectionHandler;

        @Override
        public void init(final Connection connection) {
            super.init(connection);

            httpCompressionDecoder = new HttpContentDecompressionHandler();
            gremlinRequestEncoder = new HttpGremlinRequestEncoder(cluster.getSerializer(), cluster.getRequestInterceptors(),
                    cluster.isUserAgentOnConnectEnabled(), cluster.isBulkResultsEnabled(), connection.getUri());
            gremlinResponseDecoder = new HttpGremlinResponseDecoder(cluster.getSerializer());
            if (cluster.getIdleConnectionTimeout() > 0) {
                final int idleConnectionTimeout = (int) (cluster.getIdleConnectionTimeout() / 1000);
                idleStateHandler = new IdleStateHandler(idleConnectionTimeout, idleConnectionTimeout, 0);
                idleConnectionHandler = new IdleConnectionHandler();
            }
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

            if (cluster.getIdleConnectionTimeout() > 0) {
                // idle connection handling is enabled
                pipeline.addLast(PIPELINE_IDLE_STATE_HANDLER, idleStateHandler);
                pipeline.addLast(PIPELINE_IDLE_CONNECTION_HANDLER, idleConnectionHandler);
            }

            final HttpClientCodec handler = new HttpClientCodec(DEFAULT_MAX_INITIAL_LINE_LENGTH, DEFAULT_MAX_HEADER_SIZE,
                    1024 * 1024, DEFAULT_FAIL_ON_MISSING_RESPONSE,
                    DEFAULT_VALIDATE_HEADERS, DEFAULT_INITIAL_BUFFER_SIZE, DEFAULT_PARSE_HTTP_AFTER_CONNECT_REQUEST,
                    DEFAULT_ALLOW_DUPLICATE_CONTENT_LENGTHS, false);

            pipeline.addLast(PIPELINE_HTTP_CODEC, handler);
            pipeline.addLast(PIPELINE_HTTP_AGGREGATOR, new HttpObjectAggregator(cluster.getMaxResponseContentLength() > 0
                    ? (int) cluster.getMaxResponseContentLength() : Integer.MAX_VALUE));
            pipeline.addLast(PIPELINE_HTTP_ENCODER, gremlinRequestEncoder);
            pipeline.addLast(PIPELINE_HTTP_DECOMPRESSION_HANDLER, httpCompressionDecoder);
            pipeline.addLast(PIPELINE_HTTP_DECODER, gremlinResponseDecoder);
        }
    }
}
