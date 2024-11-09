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
package org.apache.tinkerpop.gremlin.driver.simple;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelOption;
import io.netty.handler.codec.http.EmptyHttpHeaders;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.handler.WebSocketClientHandler;
import org.apache.tinkerpop.gremlin.driver.handler.WebSocketGremlinRequestEncoder;
import org.apache.tinkerpop.gremlin.driver.handler.WebSocketGremlinResponseDecoder;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;

/**
 * A simple, non-thread safe Gremlin Server client using websockets.  Typical use is for testing and demonstration.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class WebSocketClient extends AbstractClient {
    private static final Logger logger = LoggerFactory.getLogger(WebSocketClient.class);
    private final Channel channel;

    public WebSocketClient() {
        this(URI.create("ws://localhost:8182/gremlin"));
    }

    public WebSocketClient(final URI uri) {
        super("ws-client-%d");
        final Bootstrap b = new Bootstrap().group(group);
        b.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

        final String protocol = uri.getScheme();
        if (!"ws".equalsIgnoreCase(protocol) && !"wss".equalsIgnoreCase(protocol))
            throw new IllegalArgumentException("Unsupported protocol: " + protocol);
        final String host = uri.getHost();
        final int port;
        if (uri.getPort() == -1) {
            if ("ws".equalsIgnoreCase(protocol)) {
                port = 80;
            } else if ("wss".equalsIgnoreCase(protocol)) {
                port = 443;
            } else {
                port = -1;
            }
        } else {
            port = uri.getPort();
        }

        try {
            final boolean ssl = "wss".equalsIgnoreCase(protocol);
            final SslContext sslCtx;
            if (ssl) {
                sslCtx = SslContextBuilder.forClient()
                        .trustManager(InsecureTrustManagerFactory.INSTANCE).build();
            } else {
                sslCtx = null;
            }
            final WebSocketClientHandler wsHandler = new WebSocketClientHandler(WebSocketClientHandshakerFactory.newHandshaker(
                    uri, WebSocketVersion.V13, null, true, EmptyHttpHeaders.INSTANCE, 65536), 10000, false);
            final MessageSerializer<GraphBinaryMapper> serializer = new GraphBinaryMessageSerializerV1();
            b.channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(final SocketChannel ch) {
                            final ChannelPipeline p = ch.pipeline();
                            if (sslCtx != null) {
                                p.addLast(sslCtx.newHandler(ch.alloc(), host, port));
                            }
                            p.addLast(
                                    new HttpClientCodec(),
                                    new HttpObjectAggregator(65536),
                                    wsHandler,
                                    new WebSocketGremlinRequestEncoder(true, serializer),
                                    new WebSocketGremlinResponseDecoder(serializer),
                                    callbackResponseHandler);
                        }
                    });

            channel = b.connect(uri.getHost(), uri.getPort()).sync().channel();
            wsHandler.handshakeFuture().get(30, TimeUnit.SECONDS);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void writeAndFlush(final RequestMessage requestMessage) throws Exception {
        channel.writeAndFlush(requestMessage);
    }

    @Override
    public void close() throws IOException {
        try {
            channel.close().get(30, TimeUnit.SECONDS);
        } catch (Exception ex) {
            logger.error("Failure closing simple WebSocketClient", ex);
        } finally {
            if (!group.shutdownGracefully().awaitUninterruptibly(30, TimeUnit.SECONDS)) {
                logger.error("Could not cleanly shutdown thread pool on WebSocketClient");
            }
        }
    }
}
